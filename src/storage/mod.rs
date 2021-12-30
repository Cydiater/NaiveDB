use thiserror::Error;

mod buffer;
mod clock;
mod disk;
mod page;
mod slotted;

pub use buffer::{BufferPoolManager, BufferPoolManagerRef};

pub use page::{Page, PageRef};
pub use slotted::{KeyDataIter, SlottedPage, SlottedPageError};

pub const PAGE_SIZE: usize = 4096;
pub const DEFAULT_DB_FILE: &str = "naive.db";
pub const PAGE_ID_OF_ROOT_DATABASE_CATALOG: usize = 1;
pub const PAGE_ID_OF_METADATA: usize = 0;

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
    #[error("PageID Out of Bound: {0}")]
    PageIDOutOfBound(PageID),
    #[error("Free Pinned Page: {0}")]
    FreePinnedPage(PageID),
}
