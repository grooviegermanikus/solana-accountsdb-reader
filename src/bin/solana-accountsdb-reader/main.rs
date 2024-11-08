mod mint_reader;

use clap::Parser;
use itertools::Itertools;
use log::{debug, warn};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
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

    let mut loader: ArchiveSnapshotExtractor<File> =
        ArchiveSnapshotExtractor::open(&archive_path).unwrap();

    let mut accounts_per_slot: HashMap<Slot, u64> = HashMap::new();
    let mut updates: HashMap<Pubkey, Vec<Slot>> = HashMap::new();

    let mut writer_spl = csv::Writer::from_path("mints_spl_token.csv").unwrap();
    let mut writer_token2022 = csv::Writer::from_path("mints_token2022.csv").unwrap();

    for vec in loader.iter() {
        let append_vec = vec.unwrap();
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            let acc_pubkey = stored.meta.pubkey;
            let owner_pubkey = stored.account_meta.owner;


            let is_token2022 = ( owner_pubkey == spl_token_2022::ID) && mint_reader::is_token2022_mint(&stored.data, acc_pubkey);
            let is_spl_token = ( owner_pubkey == spl_token::ID ) && mint_reader::is_spltoken_mint(&stored.data, acc_pubkey);

            match (is_token2022, is_spl_token) {
                (true, true) => {
                    warn!(
                        "both token2022 and spl_token found for account {}", acc_pubkey);
                }
                (true, false) => {
                    writer_token2022
                        .write_record(&[acc_pubkey.to_string().as_str()])
                        .unwrap();
                }
                (false, true) => {
                    writer_spl
                        .write_record(&[acc_pubkey.to_string().as_str()])
                        .unwrap();
                }
                (false, false) => {
                    debug!("not a mint {}", acc_pubkey);
                }
            }
        }
    }

    writer_spl.flush().unwrap();
    writer_token2022.flush().unwrap();

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
