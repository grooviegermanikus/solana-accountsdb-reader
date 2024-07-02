use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
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
use fnv::FnvHasher;
use itertools::Itertools;
use modular_bitfield::prelude::B31;
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
const WRITE_BATCH_SORTED: bool = false;
#[from_env]
const WRITE_BATCH_SIZE: usize = 128;

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
    info!("WRITE_BATCH_SORTED: {}", WRITE_BATCH_SORTED);

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    info!("Reading snapshot from {:?} with {} bytes", archive_path, archive_path.metadata().unwrap().len());

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();
    info!("... opened snapshot archive");

    let started_at = Instant::now();
    let mut cnt_append_vecs = 0;
    let before = ALLOCATOR.allocated();

    // let mut store_hashmapmap = HashMapMapStore::new();
    // let mut store_btree = ProgramPrefixBtree::new();
    let mut store_sled = SpaceJamMap::new();
    for snapshot_result in loader.iter() {
        for append_vec in snapshot_result {
            trace!("size: {:?}", append_vec.len());
            trace!("slot: {:?}", append_vec.slot());

            for chunk in &append_vec_iter(&append_vec).chunks(WRITE_BATCH_SIZE) {

                // 60-128 items
                let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);
                for handle in chunk {
                    cnt_append_vecs += 1;
                    if cnt_append_vecs % 100_000 == 0 {
                        info!("{} append vecs after {:.3}s (speed {:.0}/s)",
                            cnt_append_vecs, started_at.elapsed().as_secs_f64(), cnt_append_vecs as f64 / started_at.elapsed().as_secs_f64());
                    }
                    let stored = handle.access().unwrap();
                    let account_pubkey = stored.meta.pubkey;
                    let owner_pubkey = stored.account_meta.owner;

                    let stuff = AccountStuff {
                    };

                    let mut key_bytes = [0u8; PUBKEY_BYTES * 2];
                    key_bytes[0..PUBKEY_BYTES].copy_from_slice(owner_pubkey.as_ref());
                    key_bytes[PUBKEY_BYTES..].copy_from_slice(account_pubkey.as_ref());

                    // TODO do not clone
                    batch.push((key_bytes.clone(), bincode::serialize(&stuff).unwrap()));
                    // write_batch.insert(key_bytes.as_ref(), bincode::serialize(&stuff).unwrap());

                }

                if WRITE_BATCH_SORTED {
                    batch.sort_by(|(a, _), (b, _)| a.cmp(b));
                    trace!("sort batch of {} items", batch.len());
                }


                let mut write_batch = sled::Batch::default();
                for (key, value) in batch {
                    write_batch.insert(&key, value);
                }

                store_sled.store.apply_batch(write_batch).unwrap();

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
    }

    let after = ALLOCATOR.allocated();

    // info!("HEAP allocated: {} ({:.2}/acc)", after - before, (after - before) as f64 / store_hashmapmap.total_count() as f64);

    // store_hashmapmap.debug();
    // store_btree.debug();
    store_sled.debug();

    store_sled.shutdown();


    // find progtrams with many accounts
    // store_hashmapmap.store.iter()
    //     .map(|(owner_pubkey, account_pks)| (owner_pubkey, account_pks.len()))
    //     .sorted_by_key(|(_, count)| *count).rev().take(30)
    //     .for_each(|(owner_pubkey, count)| {
    //     info!("owner {:?} has {} accounts", owner_pubkey, count);
    // });

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
    store: HashMap<Pubkey, HashMap<Pubkey, AccountStuff>>,
    writes: usize,
    overwrites: usize,
}

impl HashMapMapStore {
    fn new() -> Self {
        Self {
            store: HashMap::new(),
            writes: 0,
            overwrites: 0,
        }
    }

    fn store(&mut self, account_pubkey: Pubkey, owner_pubkey: Pubkey, value: AccountStuff) {
        self.writes += 1;

        let replacement = self.store.entry(owner_pubkey)
            .or_insert_with(HashMap::new) // TODO set default capacity
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

#[from_env]
const LEAF_FANOUT: usize = 32;
struct SpaceJamMap {
    db: sled::Db<LEAF_FANOUT>,
    store: Tree<LEAF_FANOUT>,
}

impl SpaceJamMap {
    fn new() -> Self {
        let config = sled::Config::default()
            .path("sled.db")
            // .mode(sled::Mode::HighThroughput)
            // compression features disabled cos of dependency mess
            // .use_compression(true)
            // .compression_factor(3)
            .cache_capacity_bytes(512 * 1024 * 1024)
            .flush_every_ms(Some(1000))
            ;
        let db = config.open().unwrap();
        let accounts_tree = db.open_tree("accounts").unwrap();
        // accounts_tree.insert("key", "value")?;
        Self {
            db,
            store: accounts_tree,
        }
    }

    // speed 38552/s on macbook
    fn store(&self, account_pubkey: Pubkey, owner_pubkey: Pubkey, value: AccountStuff) {
        let mut key_bytes = [0u8; PUBKEY_BYTES * 2];
        key_bytes[0..PUBKEY_BYTES].copy_from_slice(owner_pubkey.as_ref());
        key_bytes[PUBKEY_BYTES..].copy_from_slice(account_pubkey.as_ref());

        self.store.insert(key_bytes, bincode::serialize(&value).unwrap()).unwrap();
    }

    fn shutdown(&self) {
        self.store.flush().unwrap();
        self.db.flush().unwrap();
    }

    fn debug(&self) {
        info!("SledMap, count: {}", self.store.len());
        let somekey = Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap();
        let left = self.store.get_lt(&somekey).unwrap()
            .map(|r| Pubkey::new_from_array(r.0[0..PUBKEY_BYTES].try_into().unwrap())).unwrap();
        let right = self.store.get_gt(&somekey).unwrap()
            .map(|r| Pubkey::new_from_array(r.0[0..PUBKEY_BYTES].try_into().unwrap())).unwrap();
        info!("left: {:?}, right: {:?}", left.to_string(), right.to_string());

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


