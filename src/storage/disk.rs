use super::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

#[allow(dead_code)]
pub struct DiskManager {
    file: File,
}

impl DiskManager {
    #[allow(dead_code)]
    pub fn new() -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(DEFAULT_DB_FILE)?;
        Ok(DiskManager { file })
    }
    #[allow(dead_code)]
    pub fn read(&mut self, page_id: PageID, page: &mut page::Page) -> Result<(), StorageError> {
        let offset = (page_id as usize) * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        page.page_id = page_id;
        page.is_dirty = false;
        self.file.read_exact(page.buffer.as_mut())?;
        Ok(())
    }
    #[allow(dead_code)]
    pub fn write(&mut self, page: &mut page::Page) -> Result<(), StorageError> {
        let offset = page.page_id * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.write_all(page.buffer.as_mut())?;
        Ok(())
    }
    // TODO: support deallocate
    #[allow(dead_code)]
    pub fn allocate(&mut self) -> Result<page::Page, StorageError> {
        let meta = self.file.metadata()?;
        let len = meta.len() as usize;
        assert_eq!(len % PAGE_SIZE, 0);
        self.file.set_len((len + PAGE_SIZE) as u64)?;
        let page_id = len / PAGE_SIZE;
        let mut buffer = [0u8; PAGE_SIZE];
        self.file.read_exact(buffer.as_mut())?;
        Ok(page::Page {
            page_id,
            is_dirty: false,
            pin_count: 0,
            buffer,
        })
    }
    #[allow(dead_code)]
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
    use rand::Rng;
    use std::fs::remove_file;

    #[test]
    fn create_write_read_test() {
        // clear the fs
        let _ = remove_file(DEFAULT_DB_FILE);
        // create disk manager
        let mut disk_manager = DiskManager::new().unwrap();
        // allocate three pages
        let mut page1 = disk_manager.allocate().unwrap();
        let mut page2 = disk_manager.allocate().unwrap();
        let mut page3 = disk_manager.allocate().unwrap();
        // write random values
        let mut rng = rand::thread_rng();
        for i in 0..PAGE_SIZE {
            let p1 = rng.gen::<u8>();
            let p2 = rng.gen::<u8>();
            page1.buffer[i] = p1;
            page2.buffer[i] = p2;
            page3.buffer[i] = p1 ^ p2;
        }
        // write back
        disk_manager.write(&mut page1).unwrap();
        let id1 = page1.page_id;
        page1.clear();
        disk_manager.write(&mut page2).unwrap();
        let id2 = page2.page_id;
        page2.clear();
        disk_manager.write(&mut page3).unwrap();
        let id3 = page3.page_id;
        page3.clear();
        // read again
        disk_manager.read(id1, &mut page1).unwrap();
        disk_manager.read(id2, &mut page2).unwrap();
        disk_manager.read(id3, &mut page3).unwrap();
        // validate
        for i in 0..PAGE_SIZE {
            let p1 = page1.buffer[i];
            let p2 = page2.buffer[i];
            let p3 = page3.buffer[i];
            assert_eq!(p1 ^ p2, p3);
        }
        // clear
        remove_file(DEFAULT_DB_FILE).unwrap();
    }
}
