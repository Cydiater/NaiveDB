use crate::storage::{PageID, PAGE_SIZE};
use thiserror::Error;

/// DatabasePageBuffer is the root page of the whole DBMS storage.
/// We arrange the database as
///
///     |num_char: u32|PageID: u32|chars: [u8]|
///     |...|
///
/// Since we assume that there will not be much databases in our system, so to perform CURD on
/// databases, we can simply do brute force operation. We can mark num_char to 0 to imply the end.
///
#[derive(Clone)]
pub struct DatabasePageBuffer {
    buf: [u8; PAGE_SIZE],
}

#[allow(dead_code)]
impl DatabasePageBuffer {
    pub fn from_raw(raw: [u8; PAGE_SIZE]) -> Self {
        Self { buf: raw }
    }
    /// construct database page from a slice, we implement this
    /// mostly for test
    pub fn from_slice(slice: &[(PageID, String)]) -> Result<Self, PageError> {
        let mut buf: [u8; PAGE_SIZE] = [0u8; PAGE_SIZE];
        let mut offset = 0usize;
        for (page_id, db_name) in slice.iter() {
            let len = db_name.len();
            // num_char, u32
            buf[offset..(offset + 4)].copy_from_slice(&(len as u32).to_le_bytes());
            offset += 4;
            // page_id, u32
            buf[offset..(offset + 4)].copy_from_slice(&(*page_id as u32).to_le_bytes());
            offset += 4;
            // chars, [u8]
            buf[offset..(offset + len)].copy_from_slice(db_name.as_bytes());
            offset += len;
            if offset >= PAGE_SIZE {
                return Err(PageError::OutOfRange);
            }
        }
        Ok(Self { buf })
    }
    pub fn into_raw(self) -> [u8; PAGE_SIZE] {
        self.buf
    }
    pub fn as_raw(&self) -> &[u8; PAGE_SIZE] {
        &self.buf
    }
    pub fn as_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.buf
    }
    // insert an record for database
    pub fn insert(&mut self, _page_id: PageID, _db_name: String) -> Result<(), PageError> {
        todo!()
    }
    // search the page_id for a database name
    pub fn find(&self, _db_name: String) -> Option<PageID> {
        todo!()
    }
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Out of Range")]
    OutOfRange,
}
