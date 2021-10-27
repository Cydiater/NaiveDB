use super::*;

pub struct Page {
    pub id: PageID,
    pub is_dirty: bool,
    pub buffer: [u8; PAGE_SIZE],
}

impl Page {
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.id = 0;
        self.buffer.fill(0);
        self.is_dirty = false;
    }
}
