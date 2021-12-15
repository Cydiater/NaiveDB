use super::*;
use crate::storage::page::PageRef;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use uuid::Uuid;

#[allow(dead_code)]
pub struct DiskManager {
    file: File,
    filename: String,
}

impl DiskManager {
    pub fn new_with_name(name: String) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name.clone())?;

        Ok(DiskManager {
            file,
            filename: name,
        })
    }
    pub fn new() -> Result<Self, StorageError> {
        Self::new_with_name(DEFAULT_DB_FILE.to_string())
    }
    #[allow(dead_code)]
    /// this is used for uni-test only
    pub fn new_random() -> Result<Self, StorageError> {
        let filename = format!("naive.test.{}.db", Uuid::new_v4());
        Self::new_with_name(filename)
    }
    #[allow(dead_code)]
    pub fn filename(&self) -> String {
        self.filename.clone()
    }
    pub fn clear(&mut self) -> Result<(), StorageError> {
        self.file.set_len(0).map_err(StorageError::IOError)
    }
    /// Fetch content from disk to memory, the actual data should be
    /// hold in the buffer pool, so we should NOT allocate memory in this
    /// stage.
    pub fn read(&mut self, page_id: PageID, page: PageRef) -> Result<(), StorageError> {
        let offset = (page_id as usize) * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        page.borrow_mut().page_id = Some(page_id);
        page.borrow_mut().is_dirty = false;
        self.file.read_exact(page.borrow_mut().buffer.as_mut())?;
        Ok(())
    }
    pub fn write(&mut self, page: PageRef) -> Result<(), StorageError> {
        let offset = page.borrow_mut().page_id.unwrap() * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.write_all(&page.borrow_mut().buffer)?;
        Ok(())
    }
    pub fn allocate(&mut self, page: PageRef) -> Result<(), StorageError> {
        let meta = self.file.metadata()?;
        let len = meta.len() as usize;
        assert_eq!(len % PAGE_SIZE, 0);
        self.file.set_len((len + PAGE_SIZE) as u64)?;
        let page_id = len / PAGE_SIZE;
        self.file.read_exact(page.borrow_mut().buffer.as_mut())?;
        page.borrow_mut().page_id = Some(page_id);
        page.borrow_mut().is_dirty = false;
        page.borrow_mut().pin_count = 1;
        Ok(())
    }
    pub fn num_pages(&self) -> Result<usize, StorageError> {
        let meta = self.file.metadata()?;
        let len = meta.len();
        assert_eq!(len % (PAGE_SIZE as u64), 0);
        Ok((len / (PAGE_SIZE as u64)) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::Page;
    use rand::Rng;
    use std::cell::RefCell;
    use std::fs::remove_file;
    use std::rc::Rc;

    #[test]
    fn create_write_read_test() {
        let filename = {
            // new a disk manager
            let mut disk_manager = DiskManager::new_random().unwrap();
            let filename = disk_manager.filename();
            // clear content
            disk_manager.clear().unwrap();
            // allocate three pages
            let page1 = Rc::new(RefCell::new(Page::new()));
            let page2 = Rc::new(RefCell::new(Page::new()));
            let page3 = Rc::new(RefCell::new(Page::new()));
            disk_manager.allocate(page1.clone()).unwrap();
            disk_manager.allocate(page2.clone()).unwrap();
            disk_manager.allocate(page3.clone()).unwrap();
            // since it's empty, page_id should increase from 0
            assert_eq!(page1.borrow().page_id.unwrap(), 0);
            assert_eq!(page2.borrow().page_id.unwrap(), 1);
            assert_eq!(page3.borrow().page_id.unwrap(), 2);
            // write random values
            let mut rng = rand::thread_rng();
            for i in 0..PAGE_SIZE {
                let p1 = rng.gen::<u8>();
                let p2 = rng.gen::<u8>();
                page1.borrow_mut().buffer.as_mut()[i] = p1;
                page2.borrow_mut().buffer.as_mut()[i] = p2;
                page3.borrow_mut().buffer.as_mut()[i] = p1 ^ p2;
            }
            // write back
            disk_manager.write(page1.clone()).unwrap();
            let id1 = page1.borrow_mut().page_id.unwrap();
            page1.borrow_mut().clear();
            disk_manager.write(page2.clone()).unwrap();
            let id2 = page2.borrow_mut().page_id.unwrap();
            page2.borrow_mut().clear();
            disk_manager.write(page3.clone()).unwrap();
            let id3 = page3.borrow_mut().page_id.unwrap();
            page3.borrow_mut().clear();
            // read again
            disk_manager.read(id1, page1.clone()).unwrap();
            disk_manager.read(id2, page2.clone()).unwrap();
            disk_manager.read(id3, page3.clone()).unwrap();
            // validate
            for i in 0..PAGE_SIZE {
                let p1 = page1.borrow_mut().buffer.as_mut()[i];
                let p2 = page2.borrow_mut().buffer.as_mut()[i];
                let p3 = page3.borrow_mut().buffer.as_mut()[i];
                assert_eq!(p1 ^ p2, p3);
            }
            filename
        };
        remove_file(filename).unwrap();
    }
}
