use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::path::PathBuf;
use std::str::FromStr;
use cap::Cap;
use log::{trace, warn};
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
use fnv::FnvHasher;
use itertools::Itertools;
use modular_bitfield::prelude::B31;
use solana_accounts_db::account_info::{AccountInfo, StorageLocation};
use solana_sdk::pubkey::Pubkey;
use solana_accountsdb_reader::parallel::AppendVecConsumer;

#[global_allocator]
pub static ALLOCATOR: Cap<std::alloc::System> = Cap::new(std::alloc::System, usize::max_value());

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}


//
//
//

// CLone is required by the ConcurrentMap -- TODO check why
#[derive(Clone)]
struct AccountStuff {
    // TODO
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let Args { snapshot_archive_path } = Args::parse();

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    info!("Reading snapshot from {:?} with {} bytes", archive_path, archive_path.metadata().unwrap().len());

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();
    info!("... opened snapshot archive");

    // for vec in loader.iter() {
    //     let append_vec =  vec.unwrap();
    //     info!("size: {:?}", append_vec.len());
    //     for handle in append_vec_iter(&append_vec) {
    //         let stored = handle.access().unwrap();
    //         info!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
    //     }
    // }


    let before = ALLOCATOR.allocated();

    let mut store_hashmapmap = HashMapMapStore::new();
    let mut store_btree = ProgramPrefixBtree::new();
    for snapshot_result in loader.iter() {
        for append_vec in snapshot_result {
            trace!("size: {:?}", append_vec.len());
            trace!("slot: {:?}", append_vec.slot());
            for handle in append_vec_iter(&append_vec) {
                let stored = handle.access().unwrap();
                trace!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
                let stuff = AccountStuff {
                };
                store_hashmapmap.store(stored.meta.pubkey, stored.account_meta.owner, stuff);
                let stuff = AccountStuff {
                };
                store_btree.store(stored.meta.pubkey, stored.account_meta.owner, stuff);
            }
        }
    }

    let after = ALLOCATOR.allocated();

    info!("HEAP allocated: {} ({:.2}/acc)", after - before, (after - before) as f64 / store_hashmapmap.total_count() as f64);

    store_hashmapmap.debug();
    store_btree.debug();

    Ok(())
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct MagicKey {
    owner_pubkey_part: u32,
    account_pubkey_part: u32,
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
    pub owner_pubkey: Pubkey,
    pub account_pubkey: Pubkey,
}

impl MagicKey {
    fn from_pubkeys(owner_pubkey: Pubkey, account_pubkey: Pubkey) -> Self {
        let owner_pubkey_part = fnv32_pubkey(&owner_pubkey);
        let account_pubkey_part = fnv32_pubkey(&account_pubkey);
        Self {
            owner_pubkey_part,
            account_pubkey_part,
        }
    }

}

fn fnv32_pubkey(pubkey: &Pubkey) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(pubkey.as_ref());
    let owner_hash64 = hasher.finish();
    owner_hash64 as u32
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
    store: BTreeMap<MagicKey, (ExpandedKey, AccountStuff)>,
    writes: usize,
    overwrites: usize,
}

impl ProgramPrefixBtree {
    fn new() -> Self {
        Self {
            store: BTreeMap::new(),
            writes: 0,
            overwrites: 0,
        }
    }

    fn store(&mut self, account_pubkey: Pubkey, owner_pubkey: Pubkey, value: AccountStuff) {
        self.writes += 1;

        let magic_key = MagicKey::from_pubkeys(owner_pubkey, account_pubkey);
        let expanded_key = ExpandedKey {
            owner_pubkey,
            account_pubkey,
        };
        let replacement = self.store.insert(magic_key, (expanded_key, value));

        if let Some(prev_value) = replacement {
            self.overwrites += 1;

            assert_eq!(prev_value.0.owner_pubkey, owner_pubkey, "owner pubkey fnv hash collision");
            assert_eq!(prev_value.0.account_pubkey, account_pubkey, "account pubkey infix collision");

        }
    }

    fn count(&self) -> usize {
        self.store.len()
    }

    fn debug(&self) {
        info!("BTreeMap, count: {}, writes: {}, overwrites: {}",
            self.count(), self.writes, self.overwrites);
    }
}

struct SpaceJamMap {
    store: ConcurrentMap<MagicKey, (ExpandedKey, AccountStuff)>,
}

impl SpaceJamMap {
    fn new() -> Self {
        Self {
            store: ConcurrentMap::new(),
        }
    }

    fn store(&self, account_pubkey: Pubkey, owner_pubkey: Pubkey, value: AccountStuff) {
        let magic_key = MagicKey::from_pubkeys(owner_pubkey, account_pubkey);
        let expanded_key = ExpandedKey {
            owner_pubkey,
            account_pubkey,
        };
        self.store.insert(magic_key, (expanded_key, value));
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
    let key1 = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();
    let key2 = Pubkey::from_str("11111111111111111111111111111111").unwrap();
    assert_ne!(fnv32_pubkey(&key1), fnv32_pubkey(&key2));
}


