use super::*;

#[derive(Clone)]
pub struct Page {
    pub page_id: PageID,
    pub is_dirty: bool,
    pub pin_count: usize,
    pub buffer: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new() -> Self {
        Page {
            page_id: 0,
            is_dirty: false,
            pin_count: 0,
            buffer: [0; PAGE_SIZE],
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.page_id = 0;
        self.buffer.fill(0);
        self.is_dirty = false;
    }
}
