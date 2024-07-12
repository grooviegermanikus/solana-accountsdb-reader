use std::fs::File;
use std::io::{self, Read, Seek, Write};

use parking_lot::Mutex;

use super::{FileOffset, Result};

fn init_mu() -> Mutex<()> {
    Mutex::new(())
}

type MutexInit = fn() -> Mutex<()>;

static GLOBAL_FILE_LOCK: crate::Lazy<Mutex<()>, MutexInit> =
    crate::Lazy::new(init_mu);

pub fn pread_exact_or_eof(
    file: &File,
    mut buf: &mut [u8],
    offset: FileOffset,
) -> Result<usize> {
    let _lock = GLOBAL_FILE_LOCK.lock();

    let mut f = file.try_clone()?;

    let _ = f.seek(io::SeekFrom::Start(offset))?;

    let mut total = 0;
    while !buf.is_empty() {
        match f.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
}

pub fn pread_exact(
    file: &File,
    mut buf: &mut [u8],
    offset: FileOffset,
) -> Result<()> {
    let _lock = GLOBAL_FILE_LOCK.lock();

    let mut f = file.try_clone()?;

    let _ = f.seek(io::SeekFrom::Start(offset))?;

    while !buf.is_empty() {
        match f.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    if !buf.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        )
        .into())
    } else {
        Ok(())
    }
}

pub fn pwrite_all(
    file: &File,
    mut buf: &[u8],
    offset: FileOffset,
) -> Result<()> {
    let _lock = GLOBAL_FILE_LOCK.lock();

    let mut f = file.try_clone()?;

    let _ = f.seek(io::SeekFrom::Start(offset))?;

    while !buf.is_empty() {
        match f.write(buf) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                )
                .into());
            }
            Ok(n) => buf = &buf[n..],
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
