//! TiKV commit paths for one WAL append, measured (design §28 §7.3).
//!
//! ```text
//! cargo run --release -p loams-safekeeper --features tikv --example tikv_commit_modes -- \
//!     [--pd 127.0.0.1:19379] [--keyspace loams_pgwal] [--ops 1000] [--value 8192]
//! ```
//!
//! Every mode writes one `--value`-byte WAL chunk per operation, one
//! operation in flight. The modes run **interleaved**, one operation of each
//! per round, so a noisy neighbour or a slow fsync spell hits them all alike.
//! Each mode has its own random prefix and one key per operation, like the
//! WAL's chunk keys. The last row runs alone: the raw store's append with 8
//! in flight. Printed: p50, p90, p99, max (ms) and operations per second.

use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use loams_tikv::tikv_client::{Config, RawClient, TransactionClient, TransactionOptions};

fn arg(args: &[String], name: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

fn print(name: &str, lat: &mut [Duration], wall: Duration) {
    lat.sort();
    let n = lat.len();
    let q = |f: f64| lat[((n as f64 * f) as usize).min(n - 1)].as_secs_f64() * 1e3;
    println!(
        "| {name} | {:.2} | {:.2} | {:.2} | {:.2} | {:.0} |",
        q(0.5),
        q(0.9),
        q(0.99),
        q(1.0),
        n as f64 / wall.as_secs_f64()
    );
}

fn prefix(tag: &str) -> Vec<u8> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("bench/{tag}/{nanos}/").into_bytes()
}

fn key(p: &[u8], i: u64) -> Vec<u8> {
    let mut k = p.to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

type Op = Box<dyn Fn(u64) -> BoxFuture<'static, ()>>;

/// One fenced transactional append: read the head, put the chunk and the head.
fn txn_append(txn: TransactionClient, opts: TransactionOptions, value: Vec<u8>) -> Op {
    let p = prefix("txn");
    let head = key(&p, u64::MAX);
    Box::new(move |i| {
        let (txn, opts, value, p, head) = (
            txn.clone(),
            opts.clone(),
            value.clone(),
            p.clone(),
            head.clone(),
        );
        Box::pin(async move {
            let mut t = txn.begin_with_options(opts).await.expect("begin");
            let _ = t.get(head.clone()).await.expect("get");
            t.put(key(&p, i), value).await.expect("put");
            t.put(head, i.to_be_bytes().to_vec())
                .await
                .expect("put head");
            t.commit().await.expect("commit");
        })
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let pd = arg(&args, "--pd", "127.0.0.1:19379");
    let keyspace = arg(&args, "--keyspace", "loams_pgwal");
    let ops: u64 = arg(&args, "--ops", "1000").parse()?;
    let size: usize = arg(&args, "--value", "8192").parse()?;
    let warmup = 50;
    let config = Config::default()
        .with_keyspace(&keyspace)
        .with_timeout(Duration::from_secs(10));
    let txn = TransactionClient::new_with_config(vec![pd.clone()], config.clone()).await?;
    let raw = RawClient::new_with_config(vec![pd], config).await?;
    let atomic = raw.with_atomic_for_cas();
    let value = vec![0x5Au8; size];
    let head = key(&prefix("head"), 0);
    atomic
        .compare_and_swap(head.clone(), None::<Vec<u8>>, b"term 1".to_vec())
        .await?;

    let one_pc = TransactionOptions::new_optimistic()
        .use_async_commit()
        .try_one_pc();
    let mut modes: Vec<(&str, Op)> = vec![
        ("TSO only (the PD round trip of a start_ts)", {
            let txn = txn.clone();
            Box::new(move |_| {
                let txn = txn.clone();
                Box::pin(async move {
                    txn.current_timestamp().await.expect("tso");
                })
            })
        }),
        (
            "TxnKV 1PC + async commit: get head, put chunk + head (P4a store)",
            txn_append(txn.clone(), one_pc.clone(), value.clone()),
        ),
        (
            "TxnKV async commit, no 1PC: get head, put chunk + head",
            txn_append(
                txn.clone(),
                TransactionOptions::new_optimistic().use_async_commit(),
                value.clone(),
            ),
        ),
        (
            "TxnKV 2PC: get head, put chunk + head",
            txn_append(
                txn.clone(),
                TransactionOptions::new_optimistic(),
                value.clone(),
            ),
        ),
        ("TxnKV 1PC blind put (no fence)", {
            let (txn, value, p) = (txn.clone(), value.clone(), prefix("blind"));
            Box::new(move |i| {
                let (txn, value, p, opts) = (txn.clone(), value.clone(), p.clone(), one_pc.clone());
                Box::pin(async move {
                    let mut t = txn.begin_with_options(opts).await.expect("begin");
                    t.put(key(&p, i), value).await.expect("put");
                    t.commit().await.expect("commit");
                })
            })
        }),
        ("RawKV put (no fence)", {
            let (raw, value, p) = (raw.clone(), value.clone(), prefix("raw"));
            Box::new(move |i| {
                let (raw, value, p) = (raw.clone(), value.clone(), p.clone());
                Box::pin(async move {
                    raw.put(key(&p, i), value).await.expect("put");
                })
            })
        }),
        ("RawKV put, then get head (the raw store's append)", {
            let (raw, value, p, head) =
                (raw.clone(), value.clone(), prefix("fenced"), head.clone());
            Box::new(move |i| {
                let (raw, value, p, head) = (raw.clone(), value.clone(), p.clone(), head.clone());
                Box::pin(async move {
                    raw.batch_put(vec![(key(&p, i), value)]).await.expect("put");
                    let _ = raw.get(head).await.expect("get");
                })
            })
        }),
        ("RawKV get (the fence read alone)", {
            let (raw, head) = (raw.clone(), head.clone());
            Box::new(move |_| {
                let (raw, head) = (raw.clone(), head.clone());
                Box::pin(async move {
                    let _ = raw.get(head).await.expect("get");
                })
            })
        }),
        ("RawKV CAS (atomic mode: the per-election fence)", {
            let (atomic, p) = (atomic.clone(), prefix("cas"));
            Box::new(move |i| {
                let (atomic, p) = (atomic.clone(), p.clone());
                Box::pin(async move {
                    let _ = atomic
                        .compare_and_swap(key(&p, i), None::<Vec<u8>>, i.to_be_bytes().to_vec())
                        .await
                        .expect("cas");
                })
            })
        }),
    ];

    for i in 0..warmup {
        for (_, op) in &modes {
            op(u64::MAX - 1 - i).await;
        }
    }
    let mut lat: Vec<Vec<Duration>> = modes.iter().map(|_| Vec::new()).collect();
    let mut busy: Vec<Duration> = modes.iter().map(|_| Duration::ZERO).collect();
    for i in 0..ops {
        for (m, (_, op)) in modes.iter().enumerate() {
            let t0 = Instant::now();
            op(i).await;
            let d = t0.elapsed();
            lat[m].push(d);
            busy[m] += d;
        }
    }
    println!(
        "| mode ({size} B, {ops} ops, interleaved) | p50 ms | p90 ms | p99 ms | max ms | 1/mean ops/s (sequential; the pipelined row is wall-clock) |"
    );
    println!("|---|---|---|---|---|---|");
    for (m, (name, _)) in modes.drain(..).enumerate() {
        print(name, &mut lat[m], busy[m]);
    }

    // The raw store's append with 8 in flight (throughput and latency).
    let p = prefix("pipe");
    let mut lat = Vec::with_capacity(ops as usize);
    let t = Instant::now();
    let mut inflight = std::collections::VecDeque::new();
    for i in 0..ops {
        let (raw, k, v, h) = (raw.clone(), key(&p, i), value.clone(), head.clone());
        let t0 = Instant::now();
        inflight.push_back(tokio::spawn(async move {
            raw.put(k, v).await.expect("put");
            let _ = raw.get(h).await.expect("get");
            t0.elapsed()
        }));
        if inflight.len() >= 8
            && let Some(h) = inflight.pop_front()
        {
            lat.push(h.await?);
        }
    }
    for h in inflight {
        lat.push(h.await?);
    }
    print(
        "RawKV put + get head, 8 in flight (alone)",
        &mut lat,
        t.elapsed(),
    );
    Ok(())
}
