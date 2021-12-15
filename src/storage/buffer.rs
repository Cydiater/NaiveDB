use super::{FrameID, PageID, StorageError};
use crate::storage::clock::ClockReplacer;
use crate::storage::disk::DiskManager;
use crate::storage::page::{Page, PageRef};
use crate::storage::PAGE_ID_OF_METADATA;
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::rc::Rc;

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

impl BufferPoolManager {
    pub fn get_page_id_of_first_free_page(&mut self) -> Option<PageID> {
        if self.num_pages().unwrap() == PAGE_ID_OF_METADATA {
            return None;
        }
        let meta_page = self.fetch(PAGE_ID_OF_METADATA).unwrap();
        let page_id =
            u32::from_le_bytes(meta_page.borrow().buffer[0..4].try_into().unwrap()) as PageID;
        self.unpin(PAGE_ID_OF_METADATA).unwrap();
        match page_id {
            0 => None,
            page_id => Some(page_id),
        }
    }
    pub fn set_page_id_of_first_free_page(&mut self, page_id: Option<PageID>) {
        let page_id = page_id.unwrap_or(0usize);
        let meta_page = self.fetch(PAGE_ID_OF_METADATA).unwrap();
        meta_page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id as u32).to_le_bytes());
        meta_page.borrow_mut().is_dirty = true;
        self.unpin(PAGE_ID_OF_METADATA).unwrap();
    }
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
        let mut bpm = Self {
            disk,
            replacer: ClockReplacer::new(size),
            buf,
            page_table: HashMap::new(),
        };
        if bpm.num_pages().unwrap() == PAGE_ID_OF_METADATA {
            let page = bpm.alloc().unwrap();
            page.borrow_mut().buffer[0..4].copy_from_slice(&0u32.to_le_bytes());
            page.borrow_mut().is_dirty = true;
            bpm.unpin(PAGE_ID_OF_METADATA).unwrap();
        }
        bpm
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
        if page_id >= self.num_pages()? {
            return Err(StorageError::PageIDOutOfBound(page_id));
        }
        // if we can find this page in buffer
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            let page = self.buf[frame_id].clone();
            self.replacer.pin(frame_id);
            page.borrow_mut().pin_count += 1;
            return Ok(page);
        }
        // fetch from disk and put in buffer pool
        let frame_id = self.replacer.victim()?;
        let page = self.buf[frame_id].clone();
        let this_page_id = page.borrow().page_id;
        if let Some(this_page_id) = this_page_id {
            // write back
            if page.borrow_mut().is_dirty {
                self.disk.write(page.clone())?;
            }
            // erase from page_table
            self.page_table.remove(&this_page_id);
        }
        // reset meta
        self.replacer.pin(frame_id);
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
        // if have free page
        let page = if let Some(page_id) = self.get_page_id_of_first_free_page() {
            // fetch to disk
            let page = self.fetch(page_id).unwrap();
            if page.borrow().pin_count != 1 {
                return Err(StorageError::FreePinnedPage(page_id));
            }
            // get page_id of next free page
            let page_id_of_next_free_page =
                u32::from_le_bytes(page.borrow().buffer[0..4].try_into().unwrap()) as PageID;
            let page_id_of_next_free_page = match page_id_of_next_free_page {
                0 => None,
                page_id => Some(page_id),
            };
            self.set_page_id_of_first_free_page(page_id_of_next_free_page);
            page
        } else {
            // ask replacer for a new frame_id
            let frame_id = self.replacer.victim()?;
            // fetch the page corresponding to the frame_id
            let page = self.buf[frame_id].clone();
            let this_page_id = page.borrow().page_id;
            if let Some(this_page_id) = this_page_id {
                // write back
                if page.borrow_mut().is_dirty {
                    self.disk.write(page.clone())?;
                }
                // remove from page_table
                self.page_table.remove(&this_page_id);
            }
            // ask disk for allocating page
            self.disk.allocate(page.clone())?;
            self.replacer.pin(frame_id);
            // update page table
            self.page_table
                .insert(page.borrow().page_id.unwrap(), frame_id);
            page
        };
        Ok(page)
    }
    pub fn free(&mut self, page_id: PageID) -> Result<(), StorageError> {
        let page = self.fetch(page_id).unwrap();
        let page_id_of_first_free_page = self.get_page_id_of_first_free_page();
        page.borrow_mut().buffer[0..4]
            .copy_from_slice(&(page_id_of_first_free_page.unwrap_or(0usize) as u32).to_le_bytes());
        page.borrow_mut().is_dirty = true;
        if page.borrow().pin_count != 1 {
            return Err(StorageError::FreePinnedPage(page.borrow().page_id.unwrap()));
        }
        let page_id = page.borrow().page_id.unwrap();
        self.unpin(page_id)?;
        self.set_page_id_of_first_free_page(Some(page_id));
        Ok(())
    }
    pub fn num_pages(&self) -> Result<usize, StorageError> {
        self.disk.num_pages()
    }
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
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            // alloc 3 pages
            let page1 = bpm.borrow_mut().alloc().unwrap();
            let page2 = bpm.borrow_mut().alloc().unwrap();
            let page3 = bpm.borrow_mut().alloc().unwrap();
            // since it's empty, page_id should increase from 0
            assert_eq!(page1.borrow().page_id.unwrap(), 1);
            assert_eq!(page2.borrow().page_id.unwrap(), 2);
            assert_eq!(page3.borrow().page_id.unwrap(), 3);
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
            bpm.borrow_mut().unpin(page_id1).unwrap();
            bpm.borrow_mut().unpin(page_id2).unwrap();
            bpm.borrow_mut().unpin(page_id3).unwrap();
            // refetch, but in reverse order
            let page3 = bpm.borrow_mut().fetch(page_id3).unwrap();
            let page2 = bpm.borrow_mut().fetch(page_id2).unwrap();
            let page1 = bpm.borrow_mut().fetch(page_id1).unwrap();
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

    #[test]
    fn free_test() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(10);
            let filename = bpm.borrow().filename();
            let mut page_ids = vec![];
            for _ in 0..1000 {
                let page = bpm.borrow_mut().alloc().unwrap();
                let page_id = page.borrow().page_id.unwrap();
                bpm.borrow_mut().unpin(page_id).unwrap();
                page_ids.push(page_id);
            }
            for page_id in page_ids {
                bpm.borrow_mut().free(page_id).unwrap();
            }
            let num_pages = bpm.borrow().num_pages().unwrap();
            for _ in 0..1000 {
                let page = bpm.borrow_mut().alloc().unwrap();
                let page_id = page.borrow().page_id.unwrap();
                bpm.borrow_mut().unpin(page_id).unwrap();
            }
            assert_eq!(num_pages, bpm.borrow().num_pages().unwrap());
            filename
        };
        remove_file(filename).unwrap()
    }

    #[test]
    fn stress_test() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(200);
            let filename = bpm.borrow().filename();
            for _ in 0..10000 {
                let page = bpm.borrow_mut().alloc().unwrap();
                let page_id = page.borrow().page_id.unwrap();
                bpm.borrow_mut().unpin(page_id).unwrap();
            }
            filename
        };
        remove_file(filename).unwrap();
    }
}
