use std::fs::{create_dir, create_dir_all, File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use std::vec::IntoIter;
use anyhow::bail;
use clap::Parser;
use fst::{Automaton, IntoStreamer, Map, MapBuilder, SetBuilder, Streamer};
use fst::automaton::{StartsWith, Str, Subsequence};
use futures::future::join_all;
use indexmap::IndexSet;
use indexmap::set::Iter;
use itertools::Itertools;
use log::{info, warn};
use rayon::prelude::*;
use memmap2::Mmap;
use solana_sdk::pubkey;
use solana_sdk::pubkey::{Pubkey, PUBKEY_BYTES};
use solana_accountsdb_reader::{append_vec_iter, SnapshotExtractor};
use solana_accountsdb_reader::archived::ArchiveSnapshotExtractor;

/// load all account keys from the snapshot and build a FST index to disk

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let Args { snapshot_archive_path, build_fst, scan_fst } = Args::parse();

    info!("build {}, scan {}", build_fst, scan_fst);
    if !build_fst && !scan_fst {
        return Err("nothing to do, specify --build-fst and/or --scan-fst".into()    );
    }

    if !build_fst && scan_fst {

        scan_it()?;

        return Ok(());
    }

    assert!(build_fst);

    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();
    info!(
        "Loading snapshot archive from {:?}", archive_path);

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();

    let storage = PathBuf::from("storage");
    if !storage.exists() {
        return Err("missing storage/ directory".into());
    }

    // TODO replace with external sorter (see merge.rs in fst project)

    let mut index_set: IndexSet<[u8; 32]> = IndexSet::with_capacity(1_000_000);


    let mut total_accounts: usize = 0;
    info!("Loading all account keys from the snapshot");
    let started_at = Instant::now();
    for vec in loader.iter() {
        let append_vec =  vec.unwrap();
        // info!("size: {:?}", append_vec.len());
        for account_keys_chunk in &append_vec_iter(&append_vec)
            .map(|ref handle| handle.access().unwrap().meta.pubkey.to_bytes())
            .chunks(1024 * 1024)
        {
            index_set.extend(account_keys_chunk);

        }
    }
    info!("loaded all {} account keys into indexsets in {:.1}ms",
        total_accounts,
        started_at.elapsed().as_millis());

    let started_at = Instant::now();
    // lk.sorted_by(|a, b| a.cmp(b));
    info!("Building FST index file {} ...", INDEX_FILE);
    build_index_fst_file(&INDEX_FILE, index_set.iter().sorted().into_iter()).unwrap();
    info!("... built FST index file {} in {}ms", INDEX_FILE, started_at.elapsed().as_millis());

    if scan_fst {
        scan_it()?;
    }

    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
    #[arg(long)]
    pub build_fst: bool,
    #[arg(long)]
    pub scan_fst: bool,
}


const INDEX_FILE: &'static str = "storage/all-account-pubkeys.fst";

fn build_index_fst_file(index_file: &str, pubkey_sorted: IntoIter<&[u8; 32]>) -> Result<(), Box<dyn std::error::Error>> {

    let fst_out_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(index_file)
        .unwrap();

    let wrt = io::BufWriter::with_capacity(16 * 1024 * 1024, fst_out_file);
    let mut build = SetBuilder::new(wrt)?;
    build.extend_iter(pubkey_sorted)?;

    build.finish()?;

    Ok(())
}

fn scan_it() -> Result<(), Box<dyn std::error::Error>>{

    let index_file = PathBuf::from_str(INDEX_FILE).expect("index file not");
    println!("Loading FST index from {:?}", index_file);

    // At this point, the map has been constructed. Now we'd like to search it.
    // This creates a memory map, which enables searching the map without loading
    // all of it into memory.
    let mmap = unsafe { Mmap::map(&File::open(index_file)?)? };
    let account_pubkey_index = fst::Set::new(mmap)?;

    for key in vec![
        Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap(),
        Pubkey::from_str("Bq6bppvF3VxigThHELdechhZmkjXZFHaAetpJkX3kC1P").unwrap(),
        Pubkey::from_str("Bq6bVvoQLJ149tm4RjmVgxd9gxj7P8SvCUPsnPrgzFhy").unwrap(),
    ] {
        let started_at = Instant::now();
        let found = account_pubkey_index.contains(&key.to_bytes());
        info!("lookup in {:.03}ms: {}", started_at.elapsed().as_secs_f64() * 1000.0, found);
    }

    let started_at = Instant::now();
    let prefix = Str::new("4M").starts_with();
    let mut range_match = account_pubkey_index.search(&prefix).into_stream();
    let mut count = 0;
    while let Some(key) = range_match.next() {
        let pubkey = Pubkey::try_from(key)?;
        count += 1;
        info!("range match: {:?}", pubkey);
    }
    info!("range search returned {} in {:.03}ms", count, started_at.elapsed().as_secs_f64() * 1000.0);

    Ok(())
}