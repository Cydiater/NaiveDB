use super::*;
use crate::storage::page::PageRef;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

#[allow(dead_code)]
pub struct DiskManager {
    file: File,
}

#[allow(dead_code)]
impl DiskManager {
    pub fn new() -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(DEFAULT_DB_FILE)?;
        Ok(DiskManager { file })
    }
    pub fn erase() -> Result<(), StorageError> {
        remove_file(DEFAULT_DB_FILE).map_err(StorageError::IOError)
    }
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
        self.file.write_all(page.borrow_mut().buffer.as_mut())?;
        Ok(())
    }
    // TODO: support deallocate
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
        // clear the fs
        let _ = DiskManager::erase();
        // create disk manager
        let mut disk_manager = DiskManager::new().unwrap();
        // allocate three pages
        let page1 = Rc::new(RefCell::new(Page::new()));
        let page2 = Rc::new(RefCell::new(Page::new()));
        let page3 = Rc::new(RefCell::new(Page::new()));
        disk_manager.allocate(page1.clone()).unwrap();
        disk_manager.allocate(page2.clone()).unwrap();
        disk_manager.allocate(page3.clone()).unwrap();
        // write random values
        let mut rng = rand::thread_rng();
        for i in 0..PAGE_SIZE {
            let p1 = rng.gen::<u8>();
            let p2 = rng.gen::<u8>();
            page1.borrow_mut().buffer[i] = p1;
            page2.borrow_mut().buffer[i] = p2;
            page3.borrow_mut().buffer[i] = p1 ^ p2;
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
            let p1 = page1.borrow_mut().buffer[i];
            let p2 = page2.borrow_mut().buffer[i];
            let p3 = page3.borrow_mut().buffer[i];
            assert_eq!(p1 ^ p2, p3);
        }
        // clear
        remove_file(DEFAULT_DB_FILE).unwrap();
    }
}
