use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use log::warn;
use {
    log::info,
    reqwest::blocking::Response,
    solana_accountsdb_reader::{
        append_vec::AppendVec,
        append_vec_iter,
        archived::ArchiveSnapshotExtractor,
        parallel::{par_iter_append_vecs, AppendVecConsumer},
        unpacked::UnpackedSnapshotExtractor,
        AppendVecIterator, ReadProgressTracking, SnapshotError, SnapshotExtractor, SnapshotResult,
    },
    std::{
        fs::File,
        io::{IoSliceMut, Read},
        path::Path,
        sync::Arc,
    },
};
use clap::Parser;
use itertools::Itertools;
use marble::Config;
use solana_accounts_db::account_storage::meta::AccountMeta;
use solana_sdk::account::AccountSharedData;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let Args { snapshot_archive_path } = Args::parse();

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();

    let marble = Config {
        path: "heap".into(),
        zstd_compression_level: None,
        ..Config::default()
    }.open().unwrap();

    let filter = Pubkey::from_str("9MoKHTArqFYUWXCUzbU7xVnEQ2Z3n16dvgbn1MbtfHuq").unwrap();


    for vec in loader.iter() {
        let append_vec =  vec.unwrap();
        // info!("size: {:?}", append_vec.len());
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            let account_pubkey = stored.meta.pubkey;
            if account_pubkey != filter {
                continue;
            }

            let owner = stored.account_meta.owner;

            // see solana fn append_accounts
            let account_meta = AccountMeta {
                lamports: stored.account_meta.lamports,
                rent_epoch: stored.account_meta.rent_epoch,
                owner: owner,
                executable: stored.account_meta.executable,
            };

            let _account = stored.account_meta.clone();
            let prefix = u64::from_be_bytes(account_pubkey.as_ref()[0..8].try_into().unwrap());
            marble.write_batch([
                (prefix, Some(&stored.data))
            ]).unwrap();

        }
    }

    dbg!(marble.stats());

    Ok(())
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
            info!("account {:?}: {} at slot {}", stored.meta.pubkey, stored.account_meta.lamports, append_vec.slot());
        }
        Ok(())
    }
}
