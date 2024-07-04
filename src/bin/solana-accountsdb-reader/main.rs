use clap::Parser;
use nohash_hasher::BuildNoHashHasher;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
use {
    log::info,
    solana_accountsdb_reader::{archived::ArchiveSnapshotExtractor, SnapshotExtractor},
    std::fs::File,
};

const BUFFER_COUNT: usize = 256;
const BUFFER_SIZE: usize = 4096*4096;

// `O_DIRECT` requires all reads and writes
// to be aligned to the block device's block
// size. 4096 might not be the best, or even
// a valid one, for yours!
#[repr(align(4096))]
#[derive(Copy, Clone)]
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

    let Args {
        snapshot_archive_path,
    } = Args::parse();
    let archive_path = PathBuf::from_str(snapshot_archive_path.as_str()).unwrap();

    info!(
        "Reading snapshot from {:?} with {} bytes",
        archive_path,
        archive_path.metadata().unwrap().len()
    );

    let mut loader: ArchiveSnapshotExtractor<File> =
        ArchiveSnapshotExtractor::open(&archive_path).unwrap();
    info!("... opened snapshot archive");

    let started_at = Instant::now();

    let mut index: HashMap<u64, usize, BuildNoHashHasher<u64>> =
        HashMap::with_capacity_and_hasher(1_000_000, BuildNoHashHasher::default());

    info!("... allocated index");

    // start the ring
    let ring = rio::new()?;

    info!("... started ring");

    // open output file, with `O_DIRECT` set
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open("bff")?;


    info!("... file open");

    let buffers = Box::new([AlignedBuffer([0u8; BUFFER_SIZE]); BUFFER_COUNT]);

    info!("... buffers allocated");


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

        let mut vec_o = 0;

        'vec: loop {
            let acc_i = append_vec.get_account(vec_o);
            match acc_i {
                Some((acc, next)) => {
                    let mut buf_on = buf_o + acc.stored_size;
                    if buf_on > BUFFER_SIZE {
                        let write = ring.write_at(
                            &file,
                            &buffers[buf_i % BUFFER_COUNT].0,
                            (buf_i * BUFFER_SIZE) as u64,
                        );
                        completions.reverse();
                        completions.push(write);
                        completions.reverse();

                        buf_i += 1;
                        buf_o = 0;
                        buf_on = acc.stored_size;

                        if completions.len() > BUFFER_COUNT {
                            let completion = completions.pop().unwrap();
                            completion.wait()?;
                        }
                    }

                    let mut buf = buffers[buf_i % BUFFER_COUNT].0;
                    buf[buf_o..buf_on]
                        .copy_from_slice(append_vec.get_slice(vec_o, acc.stored_size).unwrap().0);

                    let offset = buf_i * BUFFER_SIZE + buf_o;
                    let mut key = [0u8; 8];
                    key.copy_from_slice(&acc.meta.pubkey.to_bytes()[24..32]);
                    index.insert(u64::from_le_bytes(key), offset);

                    buf_o = buf_on;
                    vec_o = next;
                }
                None => break 'vec,
            }
        }
    }

    let write = ring.write_at(
        &file,
        &buffers[buf_i % BUFFER_COUNT].0,
        (buf_i * BUFFER_SIZE) as u64,
    );
    completions.push(write);

    for c in completions.into_iter() {
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
        "... wrote {} buffers of {BUFFER_SIZE} bytes total:{}",
        buf_i + 1,
        (buf_i + 1) * BUFFER_SIZE
    );

    info!("... index contains {} keys", index.len());

    Ok(())
}
