use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::thread::spawn;
use ahash::HashMapExt;
use std::time::{Duration, Instant};
use bloomfilter::Bloom;
use cap::Cap;
use log::{debug, trace, warn};
use {
    log::info,
    reqwest::blocking::Response,
    solana_accountsdb_reader::{
        append_vec::AppendVec,
        append_vec_iter,
        archived::ArchiveSnapshotExtractor,
        unpacked::UnpackedSnapshotExtractor,
        AppendVecIterator, ReadProgressTracking, SnapshotError, SnapshotExtractor, SnapshotResult,
    },
    std::{
        fs::File,
        path::Path,
        sync::Arc,
    },
};
use clap::Parser;
use concurrent_map::{ConcurrentMap, Minimum};
use const_env::from_env;
use fnv::{FnvHasher, FnvHashMap};
use itertools::Itertools;
use modular_bitfield::prelude::B31;
use qp_trie::Trie;
use serde::Serialize;
use sled::Tree;
use solana_accounts_db::account_info::{AccountInfo, StorageLocation};
use solana_accounts_db::accounts_db::{ACCOUNTS_DB_CONFIG_FOR_TESTING, AccountShrinkThreshold};
use solana_accounts_db::accounts_index::AccountSecondaryIndexes;
use solana_runtime::genesis_utils::create_genesis_config;
use solana_runtime::runtime_config::RuntimeConfig;
use solana_runtime::snapshot_archive_info::FullSnapshotArchiveInfo;
use solana_runtime::snapshot_bank_utils::bank_from_snapshot_archives;
use solana_sdk::native_token::sol_to_lamports;
use solana_sdk::pubkey::{Pubkey, PUBKEY_BYTES};
use solana_accountsdb_reader::parallel::AppendVecConsumer;

#[global_allocator]
pub static ALLOCATOR: Cap<std::alloc::System> = Cap::new(std::alloc::System, usize::max_value());

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}


#[from_env]
const WRITE_BATCH_SIZE: usize = 65536;

#[from_env]
const WRITE_BATCH_BUFFER_SIZE: usize = 16;

#[from_env]
const WRITE_BATCH_N_THREADS: usize = 4;

#[from_env]
const READ_N_THREADS: usize = 0;

//
//
//

// CLone is required by the ConcurrentMap -- TODO check why
#[derive(Clone, Serialize)]
struct AccountStuff {
    // TODO
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );


    let Args { snapshot_archive_path } = Args::parse();

    info!("WRITE_BATCH_SIZE: {}", WRITE_BATCH_SIZE);
    info!("WRITE_BATCH_BUFFER_SIZE: {}", WRITE_BATCH_BUFFER_SIZE);
    info!("WRITE_BATCH_N_THREADS: {}", WRITE_BATCH_N_THREADS);
    info!("READ_N_THREADS: {}", READ_N_THREADS);

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    info!("Reading snapshot from {:?} with {} bytes", archive_path, archive_path.metadata().unwrap().len());

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();
    info!("... opened snapshot archive");

    let started_at = Instant::now();
    let mut cnt_append_vecs = 0;
    // let mut alloc_trie_only = 0;

    let mut store_hashmapmap = HashMapMapStore::new();
    // let mut store_btree = ProgramPrefixBtree::new();
    let mut store_sled = SpaceJamMap::new();

    let mut program_account_size_map: HashMap<Pubkey, u64> = HashMap::with_capacity(10000);
    let mut flushes_per_program_account: HashMap<Pubkey, u64> = HashMap::with_capacity(10000);

    // let mut trie = Trie::new();
    let mut bloom = Bloom::new_for_fp_rate(1_000_000_000, 0.001);

    let (tx_sled_writer, rx) = crossbeam_channel::bounded::<Box<[(Pubkey, Vec<u8>)]>>(WRITE_BATCH_BUFFER_SIZE);

    for i in 0..WRITE_BATCH_N_THREADS {
        let sled_write_tree = store_sled.binned_stores.clone();
        let rx = rx.clone();
        spawn(move || {
            loop {
                match rx.recv() {
                    Ok(chunk) => {
                        for (bin, group) in &chunk.iter().group_by(|(account_key, _)| {
                            let bin = bin_from_pubkey(&account_key);
                            bin
                        }) {
                            let mut batch = sled::Batch::default();
                            for (account_key, value) in group {
                                // info!("writing {:?} to bin {}", account_key, bin);
                                batch.insert(&account_key.to_bytes(), value.as_slice());
                            }
                            // sled_write_tree[bin].insert(account_key.to_bytes(), value.as_slice()).unwrap();
                            sled_write_tree[bin].apply_batch(batch).unwrap();
                        }

                        // sled_write_tree.apply_batch(batch).unwrap();
                        trace!("wrote batch in thread {:?}", std::thread::current().id());
                    }
                    Err(_) => {
                        warn!("channel closed");
                        break;
                    }
                }
            }
        });
    }


    // read
    for i in 0..READ_N_THREADS {
        let sled_read_tree = store_sled.store.clone();
        spawn(move || {
            loop {
                let results = sled_read_tree.scan_prefix(&[i as u8]).map(|r| r.unwrap()).collect_vec();
                for (k, v) in results.iter().take(3) {
                    let key = Pubkey::new_from_array(k[0..PUBKEY_BYTES].try_into().unwrap());
                    // let value: AccountStuff = bincode::deserialize(&v).unwrap();
                    info!("read {:?}, total={}", key, results.len());
                }

                std::thread::sleep(Duration::from_millis(30));
            }
        });
    }


    let before = ALLOCATOR.allocated();
    // let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);
    const BUFFER_SIZE: u64 = 8 * 1024 * 1024;

    for snapshot_result in loader.iter() {
        if let Ok(append_vec) = snapshot_result {
            trace!("size: {:?}", append_vec.len());
            trace!("slot: {:?}", append_vec.slot());

            for handle in append_vec_iter(&append_vec) {
                {
                    cnt_append_vecs += 1;
                    if cnt_append_vecs % 100_000 == 0 {
                        info!("{} append vecs and {} in store after {:.3}s (speed {:.0}/s)",
                            cnt_append_vecs, store_sled.store.len(), started_at.elapsed().as_secs_f64(), cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64());
                    }
                    let stored = handle.access().unwrap();

                    let account_pubkey = stored.meta.pubkey;
                    let owner_pubkey = stored.account_meta.owner;
                    let account_size = stored.meta.data_len + 50; // approximation


                    // let uncompressed = stored.data.len();
                    // if (uncompressed > 100) {
                    //     let compressed = lz4_flex::block::compress(&stored.data).len();
                    //
                    //     info!("compress {}->{}: {:02}%", uncompressed, compressed, 100*compressed/uncompressed);
                    // }


                    program_account_size_map.entry(owner_pubkey)
                        .and_modify(|v| *v += account_size)
                        .or_insert(account_size);

                    let stuff = AccountStuff {
                    };



                    if cnt_append_vecs % 10_000 == 0 {
                        let flush_vec = program_account_size_map.iter().filter(|(_, &size)| size > BUFFER_SIZE)
                            .map(|(k,v)|k)
                            .cloned()
                            .collect_vec();
                        for pk in flush_vec {
                            let pk = pk.clone();
                            flushes_per_program_account.entry(pk)
                                .and_modify(|v| *v += 1)
                                .or_default();

                            program_account_size_map.remove(&pk);
                        }

                    }

                    if cnt_append_vecs % 500_000 == 0 {
                        let non_empty_buffers = program_account_size_map.len();
                        let topn = program_account_size_map.iter().sorted_by_key(|(_,&v)| v).rev().take(10).collect_vec();
                        for (owner, size) in topn {
                            let flushed_n = flushes_per_program_account.get(&owner).cloned().unwrap_or_default();

                            info!("- {:11}, {} flushes, program {}, nonempty={}", size, flushed_n, owner.to_string(), non_empty_buffers);

                        }
                    }

                    // let mut key_bytes = [0u8; PUBKEY_BYTES * 2];
                    // key_bytes[0..PUBKEY_BYTES].copy_from_slice(owner_pubkey.as_ref());
                    // key_bytes[PUBKEY_BYTES..].copy_from_slice(account_pubkey.as_ref());


                    // if batch.len() == WRITE_BATCH_SIZE {
                    //     info!("flush batch");
                    //     tx_sled_writer.send(batch.into_boxed_slice()).unwrap();
                    //
                    //     let n_buffered = tx_sled_writer.len();
                    //     if n_buffered > WRITE_BATCH_BUFFER_SIZE / 2 {
                    //         warn!("{} batches buffered", n_buffered);
                    //     }
                    //     batch = Vec::with_capacity(WRITE_BATCH_SIZE);
                    // }
                    //
                    // // TODO do not clone
                    // // batch.push((key_bytes.clone(), bincode::serialize(&stuff).unwrap()));
                    // batch.push((account_pubkey, bincode::serialize(&stored.data).unwrap()));


                    // let before_trie = ALLOCATOR.allocated();
                    // trie.insert(account_pubkey.to_bytes(), "");
                    // let after_trie = ALLOCATOR.allocated();
                    // alloc_trie_only += after_trie - before_trie;

                    // let stuff = AccountStuff {
                    // };
                    // store_hashmapmap.store(owner_pubkey, account_pubkey, stuff);


                    // bloom.set(&account_pubkey);


                }

                // if WRITE_BATCH_SORTED {
                //     batch.sort_by(|(a, _), (b, _)| a.cmp(b));
                //     trace!("sort batch of {} items", batch.len());
                // }
                //



                // spawn(move || {
                //     store_sled.store.append_batch(write_batch).unwrap();
                // });
                // store_sled.store.apply_batch(write_batch).unwrap();

            }



            // for handle in append_vec_iter(&append_vec) {
            //     cnt_append_vecs += 1;
            //     if cnt_append_vecs % 100_000 == 0 {
            //         info!("{} append vecs after {:.3}s (speed {:.0}/s)",
            //             cnt_append_vecs, started_at.elapsed().as_secs_f64(), cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64());
            //     }
            //     let stored = handle.access().unwrap();
            //     trace!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
            //
            //     // let stuff = AccountStuff {
            //     // };
            //     // store_hashmapmap.store(stored.meta.pubkey, stored.account_meta.owner, stuff);
            //     //
            //     // let stuff = AccountStuff {
            //     // };
            //     // store_btree.store(stored.meta.pubkey, stored.account_meta.owner, stuff);
            //
            //     let stuff = AccountStuff {
            //     };
            //     store_sled.store(stored.meta.pubkey, stored.account_meta.owner, stuff);
            //
            // }
        }
    } // -- END outer loop

    // final flush
    // if !batch.is_empty() {
    //     info!("final flush batch");
    //     tx_sled_writer.send(batch.into_boxed_slice()).unwrap();
    //
    //     batch = Vec::with_capacity(WRITE_BATCH_SIZE);
    // }


    // this should drop the trees helt by writer threads
    drop(tx_sled_writer);

    let after = ALLOCATOR.allocated();

    // [2024-07-03T17:12:40Z INFO  solana_accountsdb_reader] TOTAL HEAP allocated: 6878026235 (86.58/acc)
    // [2024-07-03T17:12:40Z INFO  solana_accountsdb_reader] HEAP FOR TRIES allocated: 6033728560 (75.95/acc)

    info!("BLOOM {}", bloom.bit_vec().count_ones());
    info!("BLOOM {} bytes", bloom.bit_vec().to_bytes().len());

    let mut cnt_false_positives = 0;
    for _ in 0..10000 {
        let key = Pubkey::new_unique();
        let key_bytes = key.to_bytes();
        let res = bloom.check(&key);
        if res {
            cnt_false_positives += 1;
        }
    }
    warn!("false positives : {}", cnt_false_positives);


    info!("TOTAL HEAP allocated: {}", after - before);
    // info!("TOTAL HEAP allocated: {} ({:.2}/acc)", after - before, (after - before) as f64 / trie.count() as f64);
    // info!("HEAP FOR TRIES allocated: {} ({:.2}/acc)", alloc_trie_only, alloc_trie_only as f64 / trie.count() as f64);

    store_hashmapmap.debug();
    // store_btree.debug();
    store_sled.debug();

    store_sled.shutdown();


    // find progtrams with many accounts
    store_hashmapmap.store.iter()
        .map(|(owner_pubkey, account_pks)| (owner_pubkey, account_pks.len()))
        .sorted_by_key(|(_, count)| *count).rev().take(30)
        .for_each(|(owner_pubkey, count)| {
        info!("owner {:?} has {} accounts", owner_pubkey, count);
    });

    Ok(())
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct MagicKey {
    owner_pubkey_part: u32,
    account_pubkey_part: u64,
}

impl Minimum for MagicKey {
    const MIN: Self = Self {
        owner_pubkey_part: 0,
        account_pubkey_part: 0,
    };
}

// note: Clone is only required for the ConcurrentMap -- TODO check why
#[derive(Clone)]
struct ExpandedKey {
    // pub owner_pubkey: Pubkey,
    // pub account_pubkey: Pubkey,
    // double hash
    pub account_double_hash32: u32,
}

impl MagicKey {
    fn from_pubkeys(owner_pubkey: Pubkey, account_pubkey: Pubkey) -> Self {
        let owner_pubkey_part = fnv32_pubkey(&owner_pubkey);
        let account_pubkey_part = fnv64_pubkey(&account_pubkey);
        Self {
            owner_pubkey_part,
            account_pubkey_part,
        }
    }

}

fn fnv32_pubkey(pubkey: &Pubkey) -> u32 {
    fnv64_pubkey(pubkey) as u32
}

fn fnv32_doublehash_pubkey(pubkey: &Pubkey) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(pubkey.as_ref());
    let owner_hash64 = hasher.finish();
    owner_hash64 as u32
}

fn fnv64_pubkey(pubkey: &Pubkey) -> u64 {
    let mut hasher = FnvHasher::default();
    hasher.write(pubkey.as_ref());
    let owner_hash64 = hasher.finish();
    owner_hash64
}

fn prefix_from_pubkey(pubkey: &Pubkey) -> u32 {
    const OFFSET: usize = 4;
    const INFIX_SIZE: usize = 4;
    u32::from_be_bytes(pubkey.as_ref()[OFFSET..OFFSET + INFIX_SIZE].try_into().unwrap())
}


struct HashMapMapStore {
    // owner/program pubkey -> account pubkey -> account stuff
    store: FnvHashMap<Pubkey, FnvHashMap<Pubkey, AccountStuff>>,
    writes: usize,
    overwrites: usize,
}

impl HashMapMapStore {
    fn new() -> Self {
        Self {
            store: FnvHashMap::new(),
            writes: 0,
            overwrites: 0,
        }
    }

    fn store(&mut self, account_pubkey: Pubkey, owner_pubkey: Pubkey, value: AccountStuff) {
        self.writes += 1;

        let replacement = self.store.entry(owner_pubkey)
            .or_insert_with(FnvHashMap::new) // TODO set default capacity
            .insert(account_pubkey, value);

        if replacement.is_some() {
            self.overwrites += 1;
        }
    }

    fn owner_count(&self) -> usize {
        self.store.len()
    }

    // note: this is slow
    fn total_count(&self) -> usize {
        self.store.values().map(|m| m.len()).sum()
    }

    fn debug(&self) {
        info!("HashMap, owners: {}, total: {}, writes: {}, overwrites: {}",
            self.owner_count(), self.total_count(), self.writes, self.overwrites);
    }
}

struct ProgramPrefixBtree {
    // TODO add account_key + owner_key to detect collisions
    store: ConcurrentMap<MagicKey, (ExpandedKey, AccountStuff), 256/*FANOUT*/, 1024/*LOCAL_GC_BUFFER_SIZE*/>,
    writes: usize,
    overwrites: usize,
    collisions: usize,
}

impl ProgramPrefixBtree {
    fn new() -> Self {
        Self {
            store: ConcurrentMap::new(),
            writes: 0,
            overwrites: 0,
            collisions: 0,
        }
    }

    fn store(&mut self, account_pubkey: Pubkey, owner_pubkey: Pubkey, value: AccountStuff) {
        self.writes += 1;

        let magic_key = MagicKey::from_pubkeys(owner_pubkey, account_pubkey);
        let account_double_hash32 = fnv32_doublehash_pubkey(&account_pubkey);
        let expanded_key = ExpandedKey {
            // owner_pubkey,
            // account_pubkey,
            account_double_hash32,
        };
        let replacement = self.store.insert(magic_key, (expanded_key, value));

        if let Some(prev_value) = replacement {
            self.overwrites += 1;

            // assert_eq!(prev_value.0.owner_pubkey, owner_pubkey, "owner pubkey fnv hash collision");
            // assert_eq!(prev_value.0.account_pubkey, account_pubkey, "account pubkey infix collision");


            // if prev_value.0.owner_pubkey != owner_pubkey {
            //     warn!("owner pubkey fnv hash collision ({} <-> {})", prev_value.0.owner_pubkey, owner_pubkey);
            // }
            // if prev_value.0.account_pubkey != account_pubkey {
            //     warn!("account pubkey hash collision ({} <-> {})", prev_value.0.account_pubkey, account_pubkey);
            // }


            // let coll1 = prev_value.0.account_pubkey == account_pubkey;
            let coll2 = prev_value.0.account_double_hash32 == account_double_hash32;
            // assert_eq!(coll1, coll2, "account collision checks disagree ({} <-> {})", prev_value.0.account_pubkey, account_pubkey);

            if coll2 {
                self.collisions += 1;
            }


        }
    }

    fn count(&self) -> usize {
        self.store.len()
    }

    fn debug(&self) {
        info!("BTreeMap, count: {}, writes: {}, overwrites: {}, collisions: {}",
            self.count(), self.writes, self.overwrites, self.collisions);
    }
}


// 13 -> 8k bins
const BIN_SIZE_LOG2: usize = 3;

#[inline]
fn bin_from_pubkey(pubkey: &Pubkey) -> usize {
    const BIN_SHIFT_BITS: usize = 24 - BIN_SIZE_LOG2;
    let as_ref = pubkey.as_ref();
    ((as_ref[0] as usize) << 16 | (as_ref[1] as usize) << 8 | (as_ref[2] as usize))
        >> BIN_SHIFT_BITS
}

#[test]
fn foo() {
    let pubkey = Pubkey::new_from_array([1u8; PUBKEY_BYTES]);
    println!("pubkey: {}", pubkey.to_string());

    let bin = bin_from_pubkey(&pubkey);

    println!("bin: {}", bin);

}

struct SpaceJamMap {
    db: sled::Db,
    // legacy
    store: Tree,
    binned_stores: Arc<[Tree]>,
}

impl SpaceJamMap {
    fn new() -> Self {
        let config = sled::Config::default()
            .path("/tmp/sled.db")
            .mode(sled::Mode::HighThroughput)
            .cache_capacity(512 * 1024 * 1024)
            .flush_every_ms(Some(5000))
            ;
        let db = config.open().unwrap();

        let binned_trees: Arc<[Tree]> = (0..(1 << BIN_SIZE_LOG2)).map(|bin| {
            db.open_tree(format!("accounts-{}", bin)).unwrap()
        }).collect();
        let legacy_accounts_tree = db.open_tree("accounts").unwrap();

        info!("sled initialized with {} bins", binned_trees.len());
        Self {
            db,
            store: legacy_accounts_tree,
            binned_stores: binned_trees,
        }
    }

    fn shutdown(&self) {
        self.store.flush().unwrap();
        self.db.flush().unwrap();
    }

    fn debug(&self) {
        info!("SledMap, count: {}", self.store.len());
        let somekey = Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap();
        // let left = self.store.get_lt(&somekey).unwrap()
        //     .map(|r| Pubkey::new_from_array(r.0[0..PUBKEY_BYTES].try_into().unwrap())).unwrap();
        // let right = self.store.get_gt(&somekey).unwrap()
        //     .map(|r| Pubkey::new_from_array(r.0[0..PUBKEY_BYTES].try_into().unwrap())).unwrap();
        // info!("left: {:?}, right: {:?}", left.to_string(), right.to_string());

        info!("db size: {}", self.db.size_on_disk().unwrap());

        let prefix = [12u8];
        let scan_matches = self.store.scan_prefix(&prefix).map(|r| r.unwrap()).count();
        info!("scanning matched {}", scan_matches);

    }

}


pub enum SupportedLoader {
    Unpacked(UnpackedSnapshotExtractor),
    ArchiveFile(ArchiveSnapshotExtractor<File>),
    ArchiveDownload(ArchiveSnapshotExtractor<Response>),
}

impl SupportedLoader {
    fn new(source: &str, progress_tracking: Box<dyn ReadProgressTracking>) -> anyhow::Result<Self> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Self::new_download(source)
        } else {
            Self::new_file(source.as_ref(), progress_tracking).map_err(Into::into)
        }
    }

    fn new_download(url: &str) -> anyhow::Result<Self> {
        let resp = reqwest::blocking::get(url)?;
        let loader = ArchiveSnapshotExtractor::from_reader(resp)?;
        info!("Streaming snapshot from HTTP");
        Ok(Self::ArchiveDownload(loader))
    }

    fn new_file(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> solana_accountsdb_reader::SnapshotResult<Self> {
        Ok(if path.is_dir() {
            info!("Reading unpacked snapshot");
            Self::Unpacked(UnpackedSnapshotExtractor::open(path, progress_tracking)?)
        } else {
            info!("Reading snapshot archive");
            Self::ArchiveFile(ArchiveSnapshotExtractor::open(path)?)
        })
    }
}

impl SnapshotExtractor for SupportedLoader {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        match self {
            SupportedLoader::Unpacked(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveFile(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveDownload(loader) => Box::new(loader.iter()),
        }
    }
}

struct SimpleLogConsumer {
}

#[async_trait::async_trait]
impl AppendVecConsumer for SimpleLogConsumer {
    async fn on_append_vec(&mut self, append_vec: AppendVec) -> anyhow::Result<()> {
        info!("size: {:?}", append_vec.len());
        info!("slot: {:?}", append_vec.slot());
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            info!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
        }
        Ok(())
    }
}


#[test]
fn test_magic_key() {
    let owner_pubkey = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let account_pubkey = Pubkey::from_str("12o5TrTwDfsACu7oCBjeBZ8oVDSfv2UW2cLvR8c47zdf").unwrap();
    let key = MagicKey::from_pubkeys(owner_pubkey, account_pubkey);
    assert_eq!(key.owner_pubkey_part, 6);
    assert_eq!(key.account_pubkey_part, 0);
}


#[test]
fn test_fnv_collision() {
    let key1 = Pubkey::from_str("9kS3ZoTfbREAzvT5i1BZy1qWYHyNVmAibiaDB12rTtci").unwrap();
    let key2 = Pubkey::from_str("HcYUjwg8guwoEKWoZsReFtGsisxaZBbYfJ8nk9pNQ9UJ").unwrap();
    assert_ne!(fnv32_pubkey(&key1), fnv32_pubkey(&key2));
}


