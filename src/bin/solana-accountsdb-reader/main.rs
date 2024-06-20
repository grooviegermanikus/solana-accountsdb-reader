use std::collections::HashMap;
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

    // for vec in loader.iter() {
    //     let append_vec =  vec.unwrap();
    //     info!("size: {:?}", append_vec.len());
    //     for handle in append_vec_iter(&append_vec) {
    //         let stored = handle.access().unwrap();
    //         info!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
    //     }
    // }


    let before = ALLOCATOR.allocated();

    let mut store = HashMapMapStore::new();
    for snapshot_result in loader.iter() {
        for append_vec in snapshot_result {
            info!("size: {:?}", append_vec.len());
            info!("slot: {:?}", append_vec.slot());
            for handle in append_vec_iter(&append_vec) {
                let stored = handle.access().unwrap();
                trace!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
                let stuff = AccountStuff {
                };
                store.store(stored.meta.pubkey, stuff);
            }
        }
    }

    let after = ALLOCATOR.allocated();

    info!("HEAP allocated: {} ({:.2}/acc)", after - before, (after - before) as f64 / store.size() as f64);

    store.debug();

    Ok(())
}

struct HashMapMapStore {
    store: HashMap<Pubkey, AccountStuff>,
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

    fn store(&mut self, key: Pubkey, value: AccountStuff) {
        self.writes += 1;
        let update = self.store.insert(key, value);
        if update.is_some() {
            self.overwrites += 1;
        }
    }

    fn size(&self) -> usize {
        self.store.len()
    }

    fn debug(&self) {
        info!("HashMap, entries: {}, writes: {}, overwrites: {}", self.size(), self.writes, self.overwrites);
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




