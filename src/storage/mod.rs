use thiserror::Error;

mod buffer;
mod clock;
mod disk;
mod page;

pub use buffer::BufferPoolManager;

const PAGE_SIZE: usize = 4096;
const DEFAULT_DB_FILE: &str = "naive.db";

/// `PageID` is used to fetch page from disk, it's
/// used internally as offset for disk.
pub type PageID = usize;
/// `FrameID` is used to fetch frame form memory,
/// aka buffer pool, it's used internally as off
/// -set in memory.
pub type FrameID = usize;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IOError: {0}")]
    IOError(#[from] std::io::Error),
    #[error("ReplacerError: {0}")]
    ReplacerError(String),
}
