use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::{DataType, Datum};
use std::convert::TryInto;

///
/// | num_child | page_id[0] | key_binary | page_id[1] | ... | page_id[n] |
///

#[allow(dead_code)]
pub struct InternalNode {
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
}

#[allow(dead_code)]
impl InternalNode {
    pub fn key_at(_idx: usize) {}

    pub fn find_child_page_id(&self, _key: Vec<Datum>) -> PageID {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let _num_child = u32::from_le_bytes(page.borrow().buffer[0..4].try_into().unwrap());
        let _index_key_size: usize = self
            .key_data_types
            .iter()
            .map(|d| d.size_as_index_key())
            .sum();
        todo!()
    }
}

#[allow(dead_code)]
pub struct LeafNode {
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
}

impl LeafNode {}
