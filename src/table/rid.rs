use crate::storage::PageID;
use std::convert::TryInto;

pub struct TupleID {
    page_id: PageID,
    offset: usize,
}

#[allow(dead_code)]
impl TupleID {
    pub fn new(page_id: PageID, offset: usize) -> Self {
        Self { page_id, offset }
    }
    pub fn from_le_bytes(buf: &[u8; 8]) -> Self {
        Self {
            page_id: u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize,
            offset: u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize,
        }
    }
    pub fn to_le_bytes(&self) -> [u8; 8] {
        let page_id = self.page_id as u32;
        let offset = self.offset as u32;
        [page_id.to_le_bytes(), offset.to_le_bytes()]
            .concat()
            .as_slice()
            .try_into()
            .unwrap()
    }
}
