//! Long-running in-process driver for the `concurrency_tests` CM fixture.
//!
//! Layout:
//!   - engine task: drains `outgoing_execute` via `process_batch` every 100ms
//!     and dispatches each row through `WasmCmTransport` → `Runtime`.
//!   - signal ticker: pushes one `"increment"` signal every `--signal-interval-ms`
//!     into the reactor (which spawns a detached `increment` workflow each time).
//!   - read ticker: every `--read-interval-ms`, calls
//!     `rt.get_state(module_id, "counter")` to read the module's live
//!     in-memory counter directly and prints it.
//!   - Ctrl-C waiter: signals shutdown, drains the engine, prints final state.
//!
//! State is file-backed at `./data/{resonate,cm-kv}.db` by default; pass
//! `--ephemeral` for in-memory mode that starts at 0 each run.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use resonate::cm::{Runtime, RuntimeConfig};
use resonate::persistence::{persistence_sqlite::SqliteStorage, Storage};
use resonate::processing::processing_messages::process_batch;
use resonate::transport::cm::CmDispatcher;
use resonate::transport::transport_cm_wasm::WasmCmTransport;
use resonate::transport::{CmTransport, TransportDispatcher};
use tokio::sync::watch;

const CONCURRENCY_TESTS_WASM: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../resonate/tests/fixtures/cm/concurrency_tests.wasm"
));

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Long-running in-process CM signal + read demo", long_about = None)]
struct Args {
    /// How often to push an `"increment"` signal at the runtime.
    #[arg(long, default_value_t = 1000)]
    signal_interval_ms: u64,

    /// How often to dispatch a `read` workflow.
    #[arg(long, default_value_t = 2000)]
    read_interval_ms: u64,

    /// How often the engine task drains `outgoing_execute`.
    #[arg(long, default_value_t = 100)]
    engine_poll_ms: u64,

    /// Module id used for install / KV scoping / wasm-cm:// address.
    #[arg(long, default_value = "demo")]
    module_id: String,

    /// Directory holding the two .db files. Created if missing.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Use `:memory:` for both databases and skip the rehydrate path.
    /// Counter starts at 0 every run; nothing is written to disk.
    #[arg(long)]
    ephemeral: bool,
}

// 4 worker threads: 1 for the engine task, 1 for each ticker, 1 spare for the
// CM `module_loop` (which runs its own `Store::run_concurrent` task and uses
// `block_in_place` inside SQLite operations). With fewer threads the demo can
// stall under high tick rates because both workers end up parked in
// `block_in_place` waiting on the same `std::sync::Mutex` inside
// `SqliteStorage`. See `resonate/src/persistence/persistence_sqlite.rs:164`.
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let args = Args::parse();
    let started = Instant::now();

    // ── 1. Construct shared Arc<Storage> ─────────────────────────────────────
    let (resonate_db, kv_db) = if args.ephemeral {
        (":memory:".to_string(), ":memory:".to_string())
    } else {
        std::fs::create_dir_all(&args.data_dir).with_context(|| {
            format!("create data dir {}", args.data_dir.display())
        })?;
        (
            args.data_dir
                .join("resonate.db")
                .to_string_lossy()
                .into_owned(),
            args.data_dir
                .join("cm-kv.db")
                .to_string_lossy()
                .into_owned(),
        )
    };
    let sqlite = SqliteStorage::open(&resonate_db, 30_000)
        .with_context(|| format!("open SQLite at {resonate_db}"))?;
    let storage: Arc<Storage> = Arc::new(Storage::Sqlite(sqlite));

    // ── 2. Construct the CM Runtime sharing that Arc<Storage> ────────────────
    let rt_config = RuntimeConfig {
        resonate_db_path: resonate_db.clone(),
        kv_db_path: kv_db.clone(),
        ..Default::default()
    };
    let rt = Arc::new(
        Runtime::new_with_storage(rt_config, Arc::clone(&storage))
            .context("Runtime::new_with_storage")?,
    );

    println!(
        "→ runtime: resonate_db={resonate_db} kv_db={kv_db} ephemeral={}",
        args.ephemeral
    );

    // ── 3. Rehydrate + install if missing ────────────────────────────────────
    rt.rehydrate().await.context("Runtime::rehydrate")?;
    if rt.modules.contains_key(&args.module_id) {
        println!("→ rehydrated module: {}", args.module_id);
    } else {
        rt.install_module_from_bytes(
            &args.module_id,
            CONCURRENCY_TESTS_WASM.to_vec(),
            None,
        )
        .await
        .context("install_module_from_bytes")?;
        println!("→ installed module:  {}", args.module_id);
    }

    let counter_at_start = read_kv_counter(&rt, &args.module_id).await?;
    println!("→ counter at start: {counter_at_start}");

    // ── 4. Build the CM-only TransportDispatcher ─────────────────────────────
    let cm_dispatcher: Arc<dyn CmDispatcher> =
        Arc::clone(&rt) as Arc<dyn CmDispatcher>;
    let cm_transport: Arc<dyn CmTransport> =
        Arc::new(WasmCmTransport::new(cm_dispatcher));
    let dispatcher = Arc::new(TransportDispatcher::new(
        None,
        None,
        None,
        None,
        None,
        Some(cm_transport),
    ));

    // ── 5. Shutdown channel + 3 long-running tasks ───────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let engine_handle = tokio::spawn(engine_task(
        Arc::clone(&storage),
        Arc::clone(&dispatcher),
        Duration::from_millis(args.engine_poll_ms),
        shutdown_rx.clone(),
    ));

    let signal_handle = tokio::spawn(signal_ticker(
        Arc::clone(&rt),
        args.module_id.clone(),
        Duration::from_millis(args.signal_interval_ms),
        started,
        shutdown_rx.clone(),
    ));

    let read_handle = tokio::spawn(read_ticker(
        Arc::clone(&rt),
        args.module_id.clone(),
        Duration::from_millis(args.read_interval_ms),
        started,
        shutdown_rx.clone(),
    ));

    println!(
        "→ engine running (poll={}ms); signals every {}ms; reads every {}ms",
        args.engine_poll_ms, args.signal_interval_ms, args.read_interval_ms
    );
    println!("→ Ctrl-C to stop.");
    println!();

    // ── 6. Wait for Ctrl-C, then orderly shutdown ────────────────────────────
    tokio::signal::ctrl_c().await.context("ctrl_c handler")?;
    println!();
    println!("→ shutting down…");
    let _ = shutdown_tx.send(true);

    // Engine task exits cleanly on shutdown; tickers stop on their own next
    // shutdown poll. Abort if they hang past a short grace period.
    let _ = tokio::time::timeout(Duration::from_secs(2), engine_handle).await;
    signal_handle.abort();
    read_handle.abort();

    let counter_final = read_kv_counter(&rt, &args.module_id).await?;
    println!("→ final counter (kv direct): {counter_final}");
    println!("→ delta this run: {}", counter_final - counter_at_start);

    drop(rt);
    drop(dispatcher);
    drop(storage);
    Ok(())
}

// ── engine task ──────────────────────────────────────────────────────────────

/// Drain `outgoing_execute` on a fixed interval, dispatching each row through
/// the `TransportDispatcher`. This mirrors what
/// `processing_messages::message_processing_loop` does internally but bypasses
/// the `Server` wrapper so we can share an `Arc<Storage>` directly with the CM
/// runtime.
async fn engine_task(
    storage: Arc<Storage>,
    dispatcher: Arc<TransportDispatcher>,
    poll_interval: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                process_batch(&storage, &dispatcher, 100, "demo://local").await;
            }
        }
    }
}

// ── signal ticker ────────────────────────────────────────────────────────────

async fn signal_ticker(
    rt: Arc<Runtime>,
    module_id: String,
    interval: Duration,
    started: Instant,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    // First `tick()` returns immediately; skip it so we don't fire at T+0.
    tick.tick().await;
    let mut n: u64 = 0;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
            _ = tick.tick() => {
                n += 1;
                let t = started.elapsed().as_secs_f64();
                match rt.send_signal(&module_id, "sibling_increment".into(), Vec::new()).await {
                    Ok(()) => println!("[signal #{n:>3} pushed at T+{t:5.1}s]"),
                    Err(e) => eprintln!("[signal #{n:>3} ERROR at T+{t:5.1}s: {e}]"),
                }
            }
        }
    }
}

// ── read ticker ──────────────────────────────────────────────────────────────

async fn read_ticker(
    rt: Arc<Runtime>,
    module_id: String,
    interval: Duration,
    started: Instant,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.tick().await;
    let mut n: u64 = 0;
    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
            _ = tick.tick() => {
                n += 1;
                let t = started.elapsed().as_secs_f64();
                match rt.get_state(&module_id, "counter").await {
                    Ok(Some(bytes)) if bytes.len() == 8 => {
                        let counter = u64::from_le_bytes(bytes.try_into().unwrap());
                        println!("[read   #{n:>3} counter={counter:>4} observed at T+{t:5.1}s]");
                    }
                    Ok(Some(bytes)) => {
                        eprintln!("[read   #{n:>3} ERROR at T+{t:5.1}s: counter not 8 bytes: {}]", bytes.len());
                    }
                    Ok(None) => {
                        println!("[read   #{n:>3} counter=   0 observed at T+{t:5.1}s (no state)]");
                    }
                    Err(e) => {
                        eprintln!("[read   #{n:>3} ERROR at T+{t:5.1}s: {e}]");
                    }
                }
            }
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

async fn read_kv_counter(rt: &Arc<Runtime>, module_id: &str) -> Result<u64> {
    let bytes = rt
        .kv
        .get(module_id, "counter")
        .await
        .map_err(|e| anyhow::anyhow!("kv.get(counter): {e}"))?;
    match bytes {
        None => Ok(0),
        Some(b) => {
            anyhow::ensure!(b.len() == 8, "counter not 8 bytes: {}", b.len());
            Ok(u64::from_le_bytes(b.try_into().unwrap()))
        }
    }
}
