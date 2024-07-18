use clap::Parser;
use nohash_hasher::BuildNoHashHasher;
use rio::Completion;
use solana_sdk::pubkey::Pubkey;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
use {
    log::info,
    solana_accountsdb_reader::{archived::ArchiveSnapshotExtractor, SnapshotExtractor},
    std::fs::File,
};

const PAGE_SIZE: usize = 4096;
const BUFFER_COUNT: usize = 4;
const BUFFER_SIZE: usize = 2048 * PAGE_SIZE;

// `O_DIRECT` requires all reads and writes
// to be aligned to the block device's block
// size. 4096 might not be the best, or even
// a valid one, for yours!
#[repr(align(4096))] // cant use BLOCK_SIZE
#[derive(Copy, Clone)]
// TODO use MaybeUninit
struct AlignedBuffer([u8; BUFFER_SIZE]);

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    pub snapshot_archive_path: String,
}

fn pk2id32(pk: &Pubkey) -> u32 {
    let mut id = [0u8; 4];
    id.copy_from_slice(&pk.to_bytes()[24..28]);
    u32::from_le_bytes(id)
}

fn pk2id64(pk: &Pubkey) -> u64 {
    let mut id = [0u8; 8];
    id.copy_from_slice(&pk.to_bytes()[24..32]);
    u64::from_le_bytes(id)
    // TODO use this
    // u64::from_be_bytes(pubkey.as_ref()[0..PREFIX_SIZE].try_into().unwrap())
}

#[derive(Clone, Copy, Debug)]
pub struct FileOffset {
    pub offset: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct WeakAccountRef {
    pub program_id: u32,
    pub slot: u32,
    pub offset: usize,
}

impl WeakAccountRef {
    pub fn file_id(&self) -> u64 {
        (self.program_id as u64) << 32 | (self.slot as u64)
    }
}

pub struct Dummy {
}

impl Dummy {
    pub fn write(&mut self, bytes: &[u8]) -> anyhow::Result<usize> {
        Ok(bytes.len())
    }

    pub fn flush(&mut self) {
    }
}



// TODO: turn into real buffer file writer
struct AccountStreamFile<'a> {
    pub file: File,
    // consecutive list of equally sized buffers which map to the underlying file (BUFFER_SIZE * BUFFER_COUNT)
    pub buffers: Box<[AlignedBuffer; BUFFER_COUNT]>,
    // logical index of buffer-sized positions in file AND the index of the buffer to use (buffer_index % BUFFER_COUNT)
    // grows monotonically
    pub buffer_index: usize,
    // offset of the next write position the current buffer
    pub buffer_offset: usize,
    pub completions: VecDeque<RefCell<Option<Completion<'a, usize>>>>,
    pub ring: rio::Rio,
}

impl<'a> AccountStreamFile<'a> {
    // writes to buffer and eventually issues a uring submission write to the file
    // TODO wrap return value in FileOffset
    pub fn write(&'a mut self, bytes: &[u8]) -> anyhow::Result<usize>{
        let size = bytes.len();
        // TODO assert that completions deque does not grow
        // TODO handle case when size is larger than BUFFER_SIZE
        // TODO assert alignment
        let mut next_buffer_offset = self.buffer_offset + size;

        // if the next_buffer_offset exceeds past BUFFER_SIZE
        // immediately send of the current buffer to be written
        // and move the write-head to the beginning of the next buffer
        // in case all buffers are currently in use, wait for the first
        // one to finish writing
        if next_buffer_offset > BUFFER_SIZE {
            // FIXME guess we lose the data from buffer_offset until end of buffer
            let write_op = self.ring.write_at(
                &self.file,
                &self.buffers[self.buffer_index % BUFFER_COUNT].0,
                (self.buffer_index * BUFFER_SIZE) as u64,
            );
            // call submit all, becasue we won't wait for this completion for a while
            // self.ring.submit_all();

            self.completions.push_back(RefCell::new(Some(write_op)));


            self.buffer_index += 1;
            self.buffer_offset = 0;
            next_buffer_offset = size;

            if self.completions.len() > BUFFER_COUNT {
                let c = self.completions.pop_front().unwrap();
                c.into_inner()
                    .expect("completions should not be None") // TODO double-check
                    .wait()?;
            }
        } // -- END roll to next buffer

        let mut buf = self.buffers[self.buffer_index % BUFFER_COUNT].0;
        buf[self.buffer_offset..next_buffer_offset]
            .copy_from_slice(bytes);

        let file_offset = self.buffer_index * BUFFER_SIZE + self.buffer_offset;
        self.buffer_offset = next_buffer_offset;
        Ok(file_offset)
    }

    pub fn flush(& mut self) -> anyhow::Result<()> {
        let write_op = self.ring.write_at(
            &self.file,
            &self.buffers[self.buffer_index % BUFFER_COUNT].0,
            (self.buffer_index * BUFFER_SIZE) as u64,
        );

        self.buffer_index += 1;
        self.buffer_offset = 0;

        // queue last write and wait for all pending completions
        write_op.wait()?;
        for c in self.completions.drain(..) {
            c.into_inner().unwrap().wait()?;
        }

        Ok(())
    }
}

struct AccountDatabase<'a> {
    pub path_prefix: String,
    pub open_files: nohash_hasher::IntMap<u64, Box<AccountStreamFile<'a>>>,
}

impl AccountDatabase<'_> {
    pub fn new() -> Self {
        // TODO: clean / create directory 
        AccountDatabase {
            path_prefix: "./".to_string(),
            open_files: HashMap::with_capacity_and_hasher(100_000, BuildNoHashHasher::default()),
        }
    }
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

    // let mut gpa_index: HashMap<u32, Vec<WeakAccountRef>, BuildNoHashHasher<u32>> =
    // HashMap::with_capacity_and_hasher(100_000, BuildNoHashHasher::default());
    // TODO use IntMap<u64, ...>
    let mut pk_index: HashMap<u64, WeakAccountRef, BuildNoHashHasher<u64>> =
        HashMap::with_capacity_and_hasher(600_000_000, BuildNoHashHasher::default());

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

    let mut stream = AccountStreamFile {
        file,
        buffers: Box::new([AlignedBuffer([0u8; BUFFER_SIZE]); BUFFER_COUNT]),
        buffer_index: 0,
        buffer_offset: 0,
        completions: VecDeque::with_capacity(BUFFER_COUNT+1),
        ring
    };

    let mut dummy = Dummy {

    };


    info!("... file open");

    let mut cnt_append_vecs = 0u64;
    let mut len_append_vecs = 0;
    let mut rem_append_vecs = 0;

    for append_vec in loader.iter() {
        let append_vec = append_vec.unwrap();                    
        let slot = append_vec.slot() as u32;
        cnt_append_vecs += 1;
        len_append_vecs += append_vec.len();
        rem_append_vecs += append_vec.remaining_bytes();

        let mut vec_o = 0;

        'vec: loop {
            let acc_i = append_vec.get_account(vec_o);
            match acc_i {
                Some((acc, next)) => {
                
                    let bytes = append_vec.get_slice(vec_o, acc.stored_size);
                    let offset = {
                        // stream.write(bytes.unwrap().0)?
                        dummy.write(bytes.unwrap().0)?
                    };
                    let program_id = pk2id32(&acc.account_meta.owner);
                    let acc_ref = WeakAccountRef { offset, program_id, slot };
                    pk_index.insert(pk2id64(&acc.meta.pubkey), acc_ref.clone());
                    // gpa_index.insert(program_id, acc_ref);

                    vec_o = next;
                }
                None => break 'vec,
            }
        }
    }

    stream.flush()?;
    dummy.flush()?;

    info!(
        "... read {} append vecs in {}s items stored:{} bytes remaining:{}",
        cnt_append_vecs,
        started_at.elapsed().as_secs_f64(),
        len_append_vecs,
        rem_append_vecs,
    );


    info!(
        "... wrote {} buffers of {BUFFER_SIZE} bytes total:{}",
        0, //stream.buffer_index,
        0, //stream.buffer_index * BUFFER_SIZE
    );

    info!("... index contains {} keys", pk_index.len());

    Ok(())
}
