use super::*;

pub struct Page {
    pub id: PageID,
    pub is_dirty: bool,
    pub buffer: [u8; PAGE_SIZE],
}

// TODO impl reset mem
