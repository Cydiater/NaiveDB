use super::{FrameID, PageID, StorageError};
use crate::storage::clock::ClockReplacer;
use crate::storage::disk::DiskManager;
use crate::storage::page::{Page, PageRef};
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

#[allow(dead_code)]
pub struct BufferPoolManager {
    disk: DiskManager,
    replacer: ClockReplacer,
    buf: Vec<PageRef>,
    page_table: HashMap<PageID, FrameID>,
}

#[allow(dead_code)]
impl BufferPoolManager {
    pub fn new(size: usize) -> Self {
        let buf = (0..size)
            .map(|_| Rc::new(RefCell::new(Page::new())))
            .collect_vec();
        Self {
            disk: DiskManager::new().unwrap(),
            replacer: ClockReplacer::new(size),
            buf,
            page_table: HashMap::new(),
        }
    }

    pub fn erase(&mut self) -> Result<(), StorageError> {
        DiskManager::erase()?;
        self.replacer.erase();
        self.page_table.clear();
        Ok(())
    }

    pub fn fetch(&mut self, page_id: PageID) -> Result<PageRef, StorageError> {
        // if we can find this page in buffer
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.replacer.pin(frame_id);
            let page = self.buf[frame_id].clone();
            page.borrow_mut().pin_count += 1;
            return Ok(page);
        }
        // fetch from disk and put in buffer pool
        let frame_id = self.replacer.victim()?;
        let page = self.buf[frame_id].clone();
        if let Some(this_page_id) = page.borrow().page_id {
            // write back
            if page.borrow_mut().is_dirty {
                self.disk.write(page.clone())?;
            }
            // erase from page_table
            self.page_table.remove(&this_page_id);
        }
        // reset meta
        page.borrow_mut().pin_count = 1;
        page.borrow_mut().is_dirty = false;
        page.borrow_mut().page_id = Some(page_id);
        self.disk.read(page_id, page.clone())?;
        Ok(page)
    }

    pub fn unpin(&mut self, page_id: PageID) -> Result<(), StorageError> {
        // assume we can find this page in buffer
        assert!(self.page_table.get(&page_id).is_some());
        // fetch frame_id
        let frame_id: FrameID = *self.page_table.get(&page_id).unwrap();
        // fetch page
        let page = self.buf[page_id].clone();
        // update pin count
        page.borrow_mut().pin_count -= 1;
        // ok to dump in replacer
        if page.borrow_mut().pin_count == 0 {
            self.replacer.unpin(frame_id);
        }
        Ok(())
    }

    pub fn alloc(&mut self) -> Result<PageRef, StorageError> {
        // ask replacer for a new frame_id
        let frame_id = self.replacer.victim()?;
        // fetch the page corresponding to the frame_id
        let page = self.buf[frame_id].clone();
        if let Some(this_page_id) = page.borrow().page_id {
            // write back
            if page.borrow_mut().is_dirty {
                self.disk.write(page.clone())?;
            }
            // remove from page_table
            self.page_table.remove(&this_page_id);
        }
        // ask disk for allocating page
        self.disk.allocate(page.clone())?;
        // update page table
        self.page_table
            .insert(page.borrow().page_id.unwrap(), frame_id);
        Ok(page)
    }

    // TODO impl deallocate
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{disk::DiskManager, PAGE_SIZE};
    use rand::Rng;

    #[test]
    fn write_read_test() {
        // clear the fs
        let _ = DiskManager::erase();
        // new a BPM
        let mut bpm = BufferPoolManager::new(5);
        // alloc 3 pages
        let page1 = bpm.alloc().unwrap();
        let page2 = bpm.alloc().unwrap();
        let page3 = bpm.alloc().unwrap();
        // write random values
        let mut rng = rand::thread_rng();
        for i in 0..PAGE_SIZE {
            let p1 = rng.gen::<u8>();
            let p2 = rng.gen::<u8>();
            page1.borrow_mut().buffer[i] = p1;
            page2.borrow_mut().buffer[i] = p2;
            page3.borrow_mut().buffer[i] = p1 ^ p2;
        }
        // save ids
        let page_id1 = page1.borrow().page_id.unwrap();
        let page_id2 = page2.borrow().page_id.unwrap();
        let page_id3 = page3.borrow().page_id.unwrap();
        // unpin
        bpm.unpin(page_id1).unwrap();
        bpm.unpin(page_id2).unwrap();
        bpm.unpin(page_id3).unwrap();
        // refetch, but in reverse order
        let page3 = bpm.fetch(page_id3).unwrap();
        let page2 = bpm.fetch(page_id2).unwrap();
        let page1 = bpm.fetch(page_id1).unwrap();
        // validate
        for i in 0..PAGE_SIZE {
            let p1 = page1.borrow().buffer[i];
            let p2 = page2.borrow().buffer[i];
            let p3 = page3.borrow().buffer[i];
            assert_eq!(p3, p1 ^ p2);
        }
    }
}
