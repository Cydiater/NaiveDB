use crate::storage::{
    BufferPoolManagerRef, PageID, PageRef, StorageError, PAGE_ID_OF_ROOT_DATABASE_CATALOG,
    PAGE_SIZE,
};
use std::convert::TryInto;
use thiserror::Error;

mod catalog_manager;

pub use catalog_manager::{CatalogManager, CatalogManagerRef};

impl Drop for Catalog {
    fn drop(&mut self) {
        let page_id = self.get_page_id();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

pub struct Catalog {
    bpm: BufferPoolManagerRef,
    page: PageRef,
}

pub struct CatalogIter {
    pub offset: usize,
    pub buf: PageRef,
    pub bpm: BufferPoolManagerRef,
}

fn len_page_id_name_at(buf: &[u8; PAGE_SIZE], offset: usize) -> (usize, PageID, String) {
    let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
    assert!(len < PAGE_SIZE);
    let page_id = u32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap()) as PageID;
    let name =
        String::from_utf8_lossy(buf[offset + 8..offset + 8 + len].try_into().unwrap()).to_string();
    (len, page_id, name)
}

impl Iterator for CatalogIter {
    type Item = (usize, PageID, String);

    fn next(&mut self) -> Option<Self::Item> {
        let (len, page_id, name) = len_page_id_name_at(&self.buf.borrow().buffer, self.offset);
        match len {
            0 => None,
            len => {
                self.offset += 4 + 4 + len as usize;
                Some((len, page_id, name))
            }
        }
    }
}

impl Catalog {
    pub fn get_page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }
    pub fn new_database_catalog(bpm: BufferPoolManagerRef) -> Catalog {
        let page = if bpm.borrow().num_pages().unwrap() > PAGE_ID_OF_ROOT_DATABASE_CATALOG {
            bpm.borrow_mut()
                .fetch(PAGE_ID_OF_ROOT_DATABASE_CATALOG)
                .unwrap()
        } else {
            let page = bpm.borrow_mut().alloc().unwrap();
            assert_eq!(
                page.borrow().page_id,
                Some(PAGE_ID_OF_ROOT_DATABASE_CATALOG)
            );
            page.borrow_mut().buffer[0..4].copy_from_slice(&0u32.to_le_bytes());
            page.borrow_mut().is_dirty = true;
            page
        };
        page.borrow_mut().is_dirty = true;
        Self { bpm, page }
    }
    pub fn new_with_page_id(bpm: BufferPoolManagerRef, page_id: PageID) -> Catalog {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        Self { bpm, page }
    }
    pub fn new_empty(bpm: BufferPoolManagerRef) -> Result<Catalog, CatalogError> {
        let page = bpm.borrow_mut().alloc().unwrap();
        page.borrow_mut().buffer[0..4].copy_from_slice(&(0u32.to_le_bytes()));
        page.borrow_mut().is_dirty = true;
        Ok(Self { bpm, page })
    }
    pub fn iter(&self) -> CatalogIter {
        CatalogIter {
            offset: 0,
            buf: self.page.clone(),
            bpm: self.bpm.clone(),
        }
    }
    pub fn insert(&mut self, page_id: PageID, name: String) -> Result<(), CatalogError> {
        let mut last = 0;
        for (len, _, _) in self.iter() {
            last += len + 4 + 4;
            if last >= PAGE_SIZE {
                return Err(CatalogError::OutOfRange);
            }
        }
        let len = name.len();
        {
            let buffer = &mut self.page.borrow_mut().buffer;
            buffer[last..last + 4].copy_from_slice(&(len as u32).to_le_bytes());
            buffer[last + 4..last + 8].copy_from_slice(&(page_id as u32).to_le_bytes());
            buffer[last + 8..last + 8 + len].copy_from_slice(name.as_bytes());
        }
        self.page.borrow_mut().is_dirty = true;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("Out of Range")]
    OutOfRange,
    #[error("BPM")]
    Storage(#[from] StorageError),
    #[error("Entry Not Found")]
    EntryNotFound,
    #[error("Not Using Database")]
    NotUsingDatabase,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BufferPoolManager;
    use itertools::Itertools;

    #[test]
    fn test_database_catalog() {
        let bpm = BufferPoolManager::new_random_shared(5);
        let mut db_catalog = Catalog::new_database_catalog(bpm.clone());
        db_catalog.insert(0, "sample_0".to_string()).unwrap();
        db_catalog.insert(1, "sample_1".to_string()).unwrap();
        db_catalog.insert(2, "sample_2".to_string()).unwrap();
        let res = db_catalog.iter().collect_vec();
        assert_eq!(
            res,
            vec![
                (8, 0, "sample_0".to_string()),
                (8, 1, "sample_1".to_string()),
                (8, 2, "sample_2".to_string()),
            ]
        );
        db_catalog.insert(3, "sample_3".to_string()).unwrap();
        let res = db_catalog.iter().collect_vec();
        assert_eq!(
            res,
            vec![
                (8, 0, "sample_0".to_string()),
                (8, 1, "sample_1".to_string()),
                (8, 2, "sample_2".to_string()),
                (8, 3, "sample_3".to_string()),
            ]
        );
        let _ = bpm.borrow_mut().clear();
    }
}
