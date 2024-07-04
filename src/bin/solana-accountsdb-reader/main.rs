use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::path::PathBuf;
use std::str::FromStr;
use ahash::HashMapExt;
use solana_sdk::blake3::Hash;
use std::time::Instant;
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
use std::{
    fs::OpenOptions, io::Result,
    os::unix::fs::OpenOptionsExt,
};

const BUFFER_COUNT: usize = 512;
const BUFFER_SIZE: usize = 4096 * 2048;

// `O_DIRECT` requires all reads and writes
// to be aligned to the block device's block
// size. 4096 might not be the best, or even
// a valid one, for yours!
#[repr(align(4096))]
struct AlignedBuffer([u8; BUFFER_SIZE]);



#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
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

    info!("Reading snapshot from {:?} with {} bytes", archive_path, archive_path.metadata().unwrap().len());

    let mut loader: ArchiveSnapshotExtractor<File> = ArchiveSnapshotExtractor::open(&archive_path).unwrap();
    info!("... opened snapshot archive");

    let started_at = Instant::now();

    let index = HashMap::<u64, usize, nohash::NoHashHasher<u64>>::new();

    // start the ring
    let ring = rio::new()?;


    // open output file, with `O_DIRECT` set
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open("bff")?;

    
    let mut buffers = [AlignedBuffer([0u8; BUFFER_SIZE]); BUFFER_COUNT];


    let mut buf_i = 0;
    let mut buf_o = 0;

    let mut completions = vec![];

    let mut cnt_append_vecs = 0u64;
    let mut len_append_vecs = 0;
    let mut rem_append_vecs = 0;

    for append_vec in loader.iter() {
        let append_vec = append_vec.unwrap();
        cnt_append_vecs += 1;
        len_append_vecs += append_vec.len();
        rem_append_vecs += append_vec.remaining_bytes();

        
        let mut vec_i = 0;
        let mut vec_o = 0;
        let acc_i = append_vec.get_account(vec_o);
        match acc_i {
            Some((acc, next)) => {

                let buf_on = buf_o + acc.stored_size;
                if buf_on > CHUNK_SIZE {
                    let write = ring.write_at(
                        &file,
                        &buffers[buf_i % BUFFER_COUNT].0,
                        buf_i * CHUNK_SIZE,
                    );
                    completions.reverse();
                    completions.push(write);
                    completions.reverse();

                    buf_i +=1 ;
                    buf_o = 0;
                    buf_on = acc.stored_size;

                    if completions.len() > BUFFER_COUNT {
                        let completion = completions.pop().unwrap();
                        completion.wait()?;
                    }
                }

                let buf = buffers[buf_i % BUFFER_COUNT].0;
                buf[buf_o..buf_on] = append_vec.get_slice(vec_o, acc.stored_size);

                let offset = buf_i * BUFFER_SIZE + buf_o;
                let key = acc_i.meta.pubkey;
                index[key] = offset;


                buf_o = buf_on;
                vec_i += 1;
                vec_o = next;
            }
            None => continue
        }
    }

    let write = ring.write_at(
        &file,
        &buffers[buf_i % BUFFER_COUNT].0,
        buf_i * CHUNK_SIZE,
    );
    completions.push(write);


    for c in completions.iter() {
        c.wait()?;
    }

    info!(
        "... read {} append vecs in {}s items stored:{} bytes remaining:{}",
        cnt_append_vecs,
        started_at.elapsed().as_secs_f64(),
        len_append_vecs,
        rem_append_vecs,
    );

    info!(
        "... wrote {buf_i} buffers of {BUFFER_SIZE} bytes total written:{}", (buf_i+1) * CHUNK_SIZE
    );

    info!(
        "... index contains {} keys", index.len()
    );

    Ok(())
}
