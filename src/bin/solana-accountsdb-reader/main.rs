use std::cmp::Ordering;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use cap::Cap;
use {
    log::info,
    reqwest::blocking::Response,
    solana_accountsdb_reader::{
        append_vec::AppendVec,
        append_vec_iter,
        archived::ArchiveSnapshotExtractor,
        parallel::{AppendVecConsumer},
        unpacked::UnpackedSnapshotExtractor,
        AppendVecIterator, ReadProgressTracking, SnapshotExtractor,
    },
    std::{
        fs::File,
        path::Path,
        sync::Arc,
    },
};
use clap::Parser;
use indexmap::IndexMap;
use qp_trie::Trie;
use solana_sdk::hash::ParseHashError;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use fst::{IntoStreamer, Set, SetBuilder, Streamer};
use memmap2::MmapMut;
use solana_accountsdb_reader::parallel_io::pwrite_all;

#[global_allocator]
pub static ALLOCATOR: Cap<std::alloc::System> = Cap::new(std::alloc::System, usize::max_value());

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

    let mut index_map = IndexMap::<[u8; 32], ()>::new();
    let mut trie: Trie<[u8; 32], ()> = Trie::new();

    // let program_filter = Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap();

    info!("Reading the snapshot...");
    let started_at = Instant::now();

    let before = ALLOCATOR.allocated();

    for vec in loader.iter() {
        let append_vec =  vec.unwrap();
        // info!("size: {:?}", append_vec.len());
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            let account_key = stored.meta.pubkey;
            let program_key = stored.account_meta.owner;
            // if program_key != program_filter {
            //     continue;
            // }
            // trie.insert(account_key.to_bytes(), ());
            index_map.insert(account_key.to_bytes(), ());
        }
    }



    let after = ALLOCATOR.allocated();
    let elapsed = started_at.elapsed();

    info!("allocated {} bytes", after - before);

    info!("built trie size in {:.1}ms with {:?} entries",
        elapsed.as_millis(),
        trie.count());
    info!("rate {:.1} entries/sec",
        trie.count() as f64 / elapsed.as_millis() as f64 * 1000.0);

    info!("built indexmap size in {:.1}ms with {:?} entries",
        elapsed.as_millis(),
        index_map.len());
    info!("rate {:.1} entries/sec",
        index_map.len() as f64 / elapsed.as_millis() as f64 * 1000.0);


    let started_at = Instant::now();
    let prefix = [42, 12];
    let mut count = 0;
    for _pk in trie.iter_prefix(&prefix[..]) {
        // info!("- {:?}", pk);
        count += 1;
    }
    info!("iterated over trie with {} items in {:.1}ms", count, started_at.elapsed().as_millis());

    let started_at = Instant::now();
    index_map.sort_by(|a, b, x, y| a.cmp(x));
    let elapsed = started_at.elapsed();
    info!("sorted indexmap in {:.1}ms", elapsed.as_millis());

    let started_at = Instant::now();
    let mut fst_builder = SetBuilder::memory();
    fst_builder.extend_iter(index_map.keys().map(|pk| pk.as_ref())).unwrap();
    // index_map.keys().for_each(|pk| {
    //     fst_builder.insert(pk).unwrap();
    // });
    let fst_bytes = fst_builder.into_inner().unwrap();
    let elapsed = started_at.elapsed();
    info!("built fst size in {} bytes in {:.1}ms", fst_bytes.len(), elapsed.as_millis());


    let set = Set::new(fst_bytes).unwrap();
    info!("fst set size: {:?}", set.len());
    let mut stream = set.range().ge([42,00]).lt([42,00,20]).into_stream();
    while let Some(key) = stream.next() {
        info!("range match: {:?}", key);
    }

    // let blob = bincode::serialize(&trie).unwrap();
    // info!("serialized trie to {} bytes ({:.1}bytes/item)", blob.len(), blob.len() as f64 / trie.count() as f64);

    // let started_at = Instant::now();
    // let deser = bincode::deserialize::<Trie<[u8; 32], ()>>(&blob).unwrap();
    // let elapsed = started_at.elapsed();
    // info!("deserialized trie in {:.1}ms with {:?} entries",
    //     elapsed.as_millis(),
    //     deser.count());
    // for pk in deser.iter_prefix(&prefix[..]).take(5) {
    //     info!("- {:?}", pk);
    // }


    let FILE = "storage/indexmap.bin";
    let file = OpenOptions::new().write(true)
        .create(true)
        .truncate(true)
        .open(FILE)
        .unwrap();


    let started_at = Instant::now();
    let serialized = serde_pickle::to_vec(&index_map, Default::default()).unwrap();
    let mut mmap_file = MmapMut::map_anon(serialized.len())?;
    mmap_file.copy_from_slice(&serialized);
    mmap_file.flush().unwrap();
    info!("serialized indexmap to {} bytes ({:.1}bytes/item) took {:.1}ms",
        serialized.len(), serialized.len() as f64 / index_map.len() as f64,
        started_at.elapsed().as_millis());

    // let started_at = Instant::now();
    // let serialized = serde_pickle::to_vec(&index_map, Default::default()).unwrap();
    // pwrite_all(&file, &serialized, 0).unwrap();
    //
    // let file_size = fs::metadata(FILE).unwrap().len();
    // info!("serialized indexmap to {} bytes ({:.1}bytes/item) took {:.1}ms",
    //     file_size, file_size as f64 / index_map.len() as f64,
    //     started_at.elapsed().as_millis());


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
            info!("account {:?}: {}", stored.meta.pubkey, stored.account_meta.lamports);
        }
        Ok(())
    }
}
