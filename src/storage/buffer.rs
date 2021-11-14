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

pub type BufferPoolManagerRef = Rc<RefCell<BufferPoolManager>>;

impl Drop for BufferPoolManager {
    fn drop(&mut self) {
        for &frame_id in self.page_table.values() {
            if self.buf[frame_id].borrow().is_dirty {
                self.disk.write(self.buf[frame_id].clone()).unwrap();
            }
        }
    }
}

#[allow(dead_code)]
impl BufferPoolManager {
    pub fn new(size: usize) -> Self {
        Self::new_with_disk(size, DiskManager::new().unwrap())
    }
    pub fn new_random(size: usize) -> Self {
        Self::new_with_disk(size, DiskManager::new_random().unwrap())
    }
    pub fn new_with_name(size: usize, name: String) -> Self {
        Self::new_with_disk(size, DiskManager::new_with_name(name).unwrap())
    }
    pub fn new_with_disk(size: usize, disk: DiskManager) -> Self {
        let buf = (0..size)
            .map(|_| Rc::new(RefCell::new(Page::new())))
            .collect_vec();
        Self {
            disk,
            replacer: ClockReplacer::new(size),
            buf,
            page_table: HashMap::new(),
        }
    }
    pub fn new_shared(size: usize) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new(size)))
    }
    pub fn new_random_shared(size: usize) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new_random(size)))
    }
    pub fn filename(&self) -> String {
        self.disk.filename()
    }
    pub fn clear(&mut self) -> Result<(), StorageError> {
        self.disk.clear()
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
        // update page table
        self.page_table.insert(page_id, frame_id);
        Ok(page)
    }

    pub fn unpin(&mut self, page_id: PageID) -> Result<(), StorageError> {
        // assume we can find this page in buffer
        assert!(self.page_table.get(&page_id).is_some());
        // fetch frame_id
        let frame_id: FrameID = *self.page_table.get(&page_id).unwrap();
        // fetch page
        let page = self.buf[frame_id].clone();
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
        println!("alloc #{}", page.borrow().page_id.unwrap());
        Ok(page)
    }
    pub fn num_pages(&self) -> Result<usize, StorageError> {
        self.disk.num_pages()
    }
    // TODO impl deallocate
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PAGE_SIZE;
    use rand::Rng;
    use std::fs::remove_file;

    #[test]
    fn write_read_test() {
        let filename = {
            // new a BPM
            let mut bpm = BufferPoolManager::new_random(5);
            let filename = bpm.filename();
            // clear content
            bpm.clear().unwrap();
            // alloc 3 pages
            let page1 = bpm.alloc().unwrap();
            let page2 = bpm.alloc().unwrap();
            let page3 = bpm.alloc().unwrap();
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
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn alloc_fetch_then_unpin() {
        // alloc first
        let (page_id, filename) = {
            let mut bpm = BufferPoolManager::new_random(5);
            let filename = bpm.filename();
            let page = bpm.alloc().unwrap();
            let page_id = page.borrow().page_id.unwrap();
            (page_id, filename)
        };
        // fetch later
        let mut bpm = BufferPoolManager::new_with_name(5, filename.clone());
        let page = bpm.fetch(page_id).unwrap();
        // do unpin
        bpm.unpin(page_id).unwrap();
        assert_eq!(page.borrow().page_id, Some(page_id));
        // remove file
        remove_file(filename).unwrap();
    }
}
