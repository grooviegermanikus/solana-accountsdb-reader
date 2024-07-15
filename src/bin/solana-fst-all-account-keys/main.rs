use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use clap::Parser;
use fst::{IntoStreamer, Map, MapBuilder};
use memmap2::Mmap;
use solana_accountsdb_reader::{append_vec_iter, SnapshotExtractor};
use solana_accountsdb_reader::archived::ArchiveSnapshotExtractor;

/// load all account keys from the snapshot and build a FST index to disk

pub fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let Args { snapshot_archive_path } = Args::parse();

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();

    for vec in loader.iter() {
        let append_vec =  vec.unwrap();
        // info!("size: {:?}", append_vec.len());
        for handle in append_vec_iter(&append_vec) {
            let stored = handle.access().unwrap();
            let account_key = stored.meta.pubkey;
            let program_key = stored.account_meta.owner;
        }
    }

    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

fn build_index_fst_file() -> Result<(), Box<dyn std::error::Error>> {

    let mut wtr = io::BufWriter::new(File::create("map.fst")?);

// Create a builder that can be used to insert new key-value pairs.
    let mut build = MapBuilder::new(wtr)?;
    build.insert("bruce", 1).unwrap();
    build.insert("clarence", 2).unwrap();
    build.insert("stevie", 3).unwrap();

// Finish construction of the map and flush its contents to disk.
    build.finish()?;


    Ok(())
}

fn scan_it() -> Result<(), Box<dyn std::error::Error>>{

    // At this point, the map has been constructed. Now we'd like to search it.
    // This creates a memory map, which enables searching the map without loading
    // all of it into memory.
    let mmap = unsafe { Mmap::map(&File::open("map.fst")?)? };
    let map = Map::new(mmap)?;

    // Query for keys that are greater than or equal to clarence.
    let mut stream = map.range().ge("clarence").into_stream();

    Ok(())
}