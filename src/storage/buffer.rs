use super::{FrameID, PageID, StorageError};
use crate::storage::clock::ClockReplacer;
use crate::storage::disk::DiskManager;
use crate::storage::page::Page;
use std::collections::HashMap;

#[allow(dead_code)]
pub struct BufferPoolManager {
    disk: DiskManager,
    replacer: ClockReplacer,
    buf: Vec<Page>,
    page_table: HashMap<PageID, FrameID>,
}

#[allow(dead_code)]
impl BufferPoolManager {
    pub fn new(size: usize) -> Self {
        Self {
            disk: DiskManager::new().unwrap(),
            replacer: ClockReplacer::new(size),
            buf: vec![Page::new(); size],
            page_table: HashMap::new(),
        }
    }

    pub fn fetch(&mut self, page_id: PageID) -> Result<&mut Page, StorageError> {
        // if we can find this page in buffer
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.replacer.pin(frame_id);
            let page = &mut self.buf[frame_id];
            page.pin_count += 1;
            return Ok(page);
        }
        // fetch from disk and put in buffer pool
        let frame_id = self.replacer.victim()?;
        let page = &mut self.buf[frame_id];
        // write back
        if page.is_dirty {
            self.disk.write(page)?;
        }
        // reset meta
        page.pin_count = 1;
        page.is_dirty = false;
        page.page_id = page_id;
        self.disk.read(page_id, page)?;
        Ok(page)
    }
}
