use crate::storage::{PageID, PAGE_SIZE};
use std::convert::TryInto;
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
        // set end
        buf[offset..(offset + 4)].copy_from_slice(&[0u8; 4]);
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
    pub fn iter(&self) -> DatabasePageIter {
        DatabasePageIter {
            offset: 0,
            buf: &self.buf,
        }
    }
    /// insert an record for database
    pub fn insert(&mut self, page_id: PageID, db_name: String) -> Result<(), PageError> {
        let mut last = 0;
        for offset in self.iter() {
            last = offset;
        }
        let len = db_name.len();
        self.buf[last..last + 4].copy_from_slice(&(len as u32).to_le_bytes());
        self.buf[last + 4..last + 8].copy_from_slice(&(page_id as u32).to_le_bytes());
        self.buf[last + 8..last + 8 + len].copy_from_slice(db_name.as_bytes());
        Ok(())
    }
    /// search the page_id for a database name
    pub fn find(&self, db_name: String) -> Option<PageID> {
        for offset in self.iter() {
            let len = u32::from_le_bytes(self.buf[offset..offset + 4].try_into().unwrap());
            let this_db_name =
                String::from_utf8_lossy(&self.buf[offset + 8..offset + 8 + len as usize]);
            if db_name == this_db_name {
                let page_id =
                    u32::from_le_bytes(self.buf[offset + 4..offset + 8].try_into().unwrap())
                        as PageID;
                return Some(page_id);
            }
        }
        None
    }
}

pub struct DatabasePageIter<'a> {
    pub offset: usize,
    pub buf: &'a [u8],
}

impl Iterator for DatabasePageIter<'_> {
    /// we only care offset here
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let len = u32::from_le_bytes(self.buf[self.offset..self.offset + 4].try_into().unwrap());
        match len {
            0 => None,
            len => {
                let offset = self.offset;
                self.offset += 4 + 4 + len as usize;
                Some(offset)
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Out of Range")]
    OutOfRange,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_find() {
        let mut db_page = DatabasePageBuffer::from_slice(&[
            (0usize, "sample_0".to_string()),
            (1usize, "sample_1".to_string()),
            (2usize, "sample_2".to_string()),
        ])
        .unwrap();
        assert_eq!(db_page.find("sample_0".to_string()), Some(0usize));
        assert_eq!(db_page.find("sample_1".to_string()), Some(1usize));
        assert_eq!(db_page.find("sample_2".to_string()), Some(2usize));
        assert_eq!(db_page.find("sample_3".to_string()), None);
        db_page.insert(3usize, "sample_3".to_string()).unwrap();
        assert_eq!(db_page.find("sample_3".to_string()), Some(3usize));
    }
}
