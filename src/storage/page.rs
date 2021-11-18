use super::*;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub struct Page {
    pub page_id: Option<PageID>,
    pub is_dirty: bool,
    pub pin_count: usize,
    pub buffer: [u8; PAGE_SIZE],
}

pub type PageRef = Rc<RefCell<Page>>;

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

impl Page {
    pub fn new() -> Self {
        Page {
            page_id: None,
            is_dirty: false,
            pin_count: 0,
            buffer: [0; PAGE_SIZE],
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.page_id = None;
        self.is_dirty = false;
        self.buffer.fill(0);
    }
}
