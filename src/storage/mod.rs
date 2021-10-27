use thiserror::Error;

mod disk;
mod page;

const PAGE_SIZE: usize = 4096;
const DEFAULT_DB_FILE: &str = "naive.db";
pub type PageID = u64;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IOError: {0}")]
    IOError(#[from] std::io::Error),
}
