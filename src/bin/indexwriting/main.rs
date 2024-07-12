use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use concurrent_map::ConcurrentMap;
use dashmap::DashMap;
use log::info;
use solana_accounts_db::secondary_index::DashMapSecondaryIndexEntry;


pub fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    // dashmap size: 80.000.000 in 8.217780792s
    // let mut index: Arc<DashMap<u64, ()>> = Arc::new(DashMap::with_capacity(500_000_000));
    //  dashmap size: 80000000 in 12.900236292s
    // let mut index: ConcurrentMap<u64, (), 256, 1024> = ConcurrentMap::new();
    let started_at = Instant::now();
    let mut threads = Vec::new();
    for i in 0..16 {
        let mut btree = index.clone();
        let jh = thread::spawn(move || {
            for j in 0..5_000_000 {
                btree.insert(i * 10_000_000 + j, ());
            }
        });
        threads.push(jh);
    }

    for jh in threads {
        jh.join().unwrap();
    }

    let elpased = started_at.elapsed();
    info!("dashmap size: {} in {:?}", index.len(), elpased);
}