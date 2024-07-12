use std::fs::File;
use crate::parallel_io::parallel_io_unix::FileOffset;

#[cfg(any(all(not(unix), not(windows)), miri))]
mod parallel_io_polyfill;
#[cfg(all(unix, not(miri)))]
mod parallel_io_unix;
#[cfg(all(windows, not(miri)))]
mod parallel_io_windows;


#[cfg(any(all(not(unix), not(windows)), miri))]
pub use parallel_io_polyfill::{pread_exact, pread_exact_or_eof, pwrite_all};

#[cfg(all(unix, not(miri)))]
pub use parallel_io_unix::{pread_exact, pread_exact_or_eof, pwrite_all};

#[cfg(all(windows, not(miri)))]
pub use parallel_io_windows::{pread_exact, pread_exact_or_eof, pwrite_all};
