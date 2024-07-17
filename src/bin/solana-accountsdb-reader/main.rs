use std::collections::HashMap;
use std::mem;
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

    let mut accounts_per_slot: HashMap<Slot, u64> = HashMap::new();
    let mut updates: HashMap<Pubkey, Vec<Slot>> = HashMap::new();


    for vec in loader.iter() {
        let append_vec =  vec.unwrap();
        // info!("size: {:?}", append_vec.len());
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            let data = stored.data;
            let account_key = stored.meta.pubkey;
            let owner_key = stored.account_meta.owner;

            const THRESHOLD_SIZE: usize = 1000;
            const THRESHOLD_ZERO_TO_DATA_RATION: f32 = 0.2; // 20%


            if data.len() < 1000 {
                continue;
            }

            let trailing_zeros = num_of_trailing_zeros(&data);

            if trailing_zeros as f32 / data.len() as f32 > THRESHOLD_ZERO_TO_DATA_RATION {
                info!("account {:?} of program {:?} has {} trailing zeroes of total {}", account_key, owner_key, trailing_zeros, data.len());
            }

        }
    }


    Ok(())
}

fn num_of_trailing_zeros(buf: &[u8]) -> usize {
    const SCALE: usize = mem::size_of::<u128>() / mem::size_of::<u8>(); // 16
    let (prefix, aligned, suffix) = unsafe { buf.align_to::<u128>() };
    // println!("prefix: {}, aligned: {}, suffix: {}", prefix.len(), aligned.len(), suffix.len());

    let cnt_suffix = suffix.iter().rev().take_while(|&x| *x == 0).count();
    // println!("cnt_suffix: {}", cnt_suffix);
    if cnt_suffix < suffix.len() {
        return cnt_suffix;
    }

    let cnt_aligned = aligned.iter().rev().take_while(|&x| *x == 0).count();
    // println!("cnt_aligned: {}", cnt_aligned);
    if cnt_aligned < aligned.len() {
        // approximates to multiple of u128
        return cnt_aligned * SCALE + cnt_suffix;
    }
    let cnt_prefix = prefix.iter().rev().take_while(|&x| *x == 0).count();
    // println!("cnt_prefix: {}", cnt_prefix);

    cnt_prefix + cnt_aligned * SCALE + cnt_suffix
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
