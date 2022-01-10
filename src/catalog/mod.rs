use crate::storage::{
    BufferPoolManagerRef, KeyDataIter, PageID, PageRef, SlottedPage, SlottedPageError,
    StorageError, PAGE_ID_OF_ROOT_DATABASE_CATALOG,
};
use itertools::Itertools;
use log::info;
use thiserror::Error;

mod catalog_manager;

pub use catalog_manager::{CatalogManager, CatalogManagerRef};

impl Drop for Catalog {
    fn drop(&mut self) {
        let page_id = self.page_id();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

pub struct Catalog {
    bpm: BufferPoolManagerRef,
    page: PageRef,
}

type CatalogPage = SlottedPage<(), PageID>;

pub struct CatalogIter<'page> {
    key_data_iter: KeyDataIter<'page, PageID>,
}

impl<'page> CatalogIter<'page> {
    pub fn new(key_data_iter: KeyDataIter<'page, PageID>) -> Self {
        Self { key_data_iter }
    }
}

impl<'page> Iterator for CatalogIter<'page> {
    type Item = (&'page str, PageID);

    fn next(&mut self) -> Option<(&'page str, PageID)> {
        if let Some((k, v)) = self.key_data_iter.next() {
            Some((std::str::from_utf8(v).unwrap(), *k))
        } else {
            None
        }
    }
}

impl Catalog {
    pub fn new_for_database(bpm: BufferPoolManagerRef) -> Catalog {
        let page = if bpm.borrow().num_pages().unwrap() > PAGE_ID_OF_ROOT_DATABASE_CATALOG {
            bpm.borrow_mut()
                .fetch(PAGE_ID_OF_ROOT_DATABASE_CATALOG)
                .unwrap()
        } else {
            let page = bpm.borrow_mut().alloc().unwrap();
            {
                let mut page_mut = page.borrow_mut();
                let bytes = &mut page_mut.buffer;
                unsafe {
                    let slotted = &mut *(bytes.as_mut_ptr() as *mut CatalogPage);
                    slotted.reset(&());
                }
                page_mut.is_dirty = true;
            }
            page
        };
        Self { bpm, page }
    }
    pub fn open(bpm: BufferPoolManagerRef, page_id: PageID) -> Result<Catalog, CatalogError> {
        let page = bpm.borrow_mut().fetch(page_id)?;
        Ok(Self { bpm, page })
    }
    pub fn new(bpm: BufferPoolManagerRef) -> Result<Catalog, CatalogError> {
        let page = bpm.borrow_mut().alloc().unwrap();
        {
            let mut page_mut = page.borrow_mut();
            let bytes = &mut page_mut.buffer;
            unsafe {
                let slotted = &mut *(bytes.as_mut_ptr() as *mut CatalogPage);
                slotted.reset(&());
            }
            page_mut.is_dirty = true;
        }
        Ok(Self { bpm, page })
    }
    fn catalog_page(&self) -> &CatalogPage {
        unsafe { &*(self.page.borrow().buffer.as_ptr() as *const CatalogPage) }
    }
    fn catalog_page_mut(&mut self) -> &mut CatalogPage {
        self.page.borrow_mut().is_dirty = true;
        unsafe { &mut *(self.page.borrow_mut().buffer.as_ptr() as *mut CatalogPage) }
    }
    pub fn page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }
    pub fn remove(&mut self, name: &str) -> Result<(), CatalogError> {
        info!("catalog: remove {}", name);
        let catalog_page = self.catalog_page_mut();
        let page_id = catalog_page
            .key_data_iter()
            .find(|(_, v)| String::from_utf8((*v).to_vec()).unwrap() == name)
            .map(|(k, _)| *k)
            .ok_or(SlottedPageError::KeyNotFound)?;
        catalog_page.remove(&page_id)?;
        Ok(())
    }
    pub fn insert(&mut self, page_id: PageID, name: &str) -> Result<(), CatalogError> {
        info!("catalog: insert {}", name);
        let catalog_page = self.catalog_page_mut();
        catalog_page.insert(&page_id, name.as_bytes())?;
        Ok(())
    }
    pub fn page_id_of(&self, name: &str) -> Option<PageID> {
        let catalog_page = self.catalog_page();
        catalog_page
            .key_data_iter()
            .find(|(_, v)| String::from_utf8((*v).to_vec()).unwrap() == name)
            .map(|(k, _)| *k)
    }
    pub fn prefix_with(&self, prefix: &str) -> Vec<&str> {
        let catalog_page = self.catalog_page();
        catalog_page
            .key_data_iter()
            .map(|(_, v)| std::str::from_utf8(&*v).unwrap())
            .filter(|v| v.starts_with(prefix))
            .collect_vec()
    }
    pub fn iter(&self) -> CatalogIter {
        let catalog_page = self.catalog_page();
        CatalogIter::new(catalog_page.key_data_iter())
    }
}

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("Storage: {0}")]
    Storage(#[from] StorageError),
    #[error("CatalogPage: {0}")]
    CatalogPage(#[from] SlottedPageError),
    #[error("Not Using Database")]
    NotUsingDatabase,
    #[error("Entry Not Found")]
    EntryNotFound,
    #[error("Duplicated")]
    Duplicated,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BufferPoolManager;
    use std::fs::remove_file;

    #[test]
    fn test_database_catalog() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let mut db_catalog = Catalog::new_for_database(bpm);
            db_catalog.insert(0, "sample_0").unwrap();
            db_catalog.insert(1, "sample_1").unwrap();
            db_catalog.insert(2, "sample_2").unwrap();
            assert_eq!(db_catalog.page_id_of("sample_0").unwrap(), 0);
            assert_eq!(db_catalog.page_id_of("sample_1").unwrap(), 1);
            assert_eq!(db_catalog.page_id_of("sample_2").unwrap(), 2);
            db_catalog.insert(3, "sample_3").unwrap();
            assert_eq!(db_catalog.page_id_of("sample_3").unwrap(), 3);
            db_catalog.remove("sample_2").unwrap();
            assert_eq!(db_catalog.page_id_of("sample_2"), None);
            filename
        };
        remove_file(filename).unwrap();
    }
}
