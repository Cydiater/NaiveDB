use super::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

#[allow(dead_code)]
pub struct DiskManager {
    file: File,
}

impl DiskManager {
    #[allow(dead_code)]
    pub fn create() -> Result<Self, StorageError> {
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
        page.id = page_id;
        page.is_dirty = false;
        self.file.read_exact(&mut page.buffer)?;
        Ok(())
    }
    #[allow(dead_code)]
    pub fn write(&mut self, page: &page::Page) -> Result<(), StorageError> {
        let offset = (page.id as usize) * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        self.file.write_all(&page.buffer)?;
        Ok(())
    }
    #[allow(dead_code)]
    pub fn allocate(&mut self) -> Result<page::Page, StorageError> {
        let meta = self.file.metadata()?;
        let len = meta.len();
        assert_eq!(len % (PAGE_SIZE as u64), 0);
        self.file.set_len(len + PAGE_SIZE as u64)?;
        let id = len / (PAGE_SIZE as u64);
        let mut buffer = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buffer)?;
        Ok(page::Page {
            id,
            is_dirty: false,
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
