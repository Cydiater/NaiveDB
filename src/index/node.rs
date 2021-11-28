use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::{DataType, Datum};
use std::convert::TryInto;

///
/// | num_child | page_id[0] | key_binary[0] | page_id[1] | ... | page_id[n] |
///

fn index_key_from_binary(
    bpm: BufferPoolManagerRef,
    data_types: &[DataType],
    bytes: &[u8],
) -> Vec<Datum> {
    let mut offset = 0;
    let mut datums = vec![];
    for data_type in data_types {
        let width = data_type.size_as_index_key();
        offset += data_type.size_as_index_key();
        datums.push(Datum::from_index_key_binary(
            bpm.clone(),
            *data_type,
            &bytes[offset..(offset + width)],
        ));
    }
    datums
}

#[allow(dead_code)]
pub struct InternalNode {
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
    key_size: usize,
}

#[allow(dead_code)]
impl InternalNode {
    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let start = std::mem::size_of::<u32>()
            + self.key_size * (idx - 1)
            + std::mem::size_of::<u32>() * idx;
        let end = start + self.key_size;
        let bytes = &page.borrow().buffer[start..end];
        index_key_from_binary(self.bpm.clone(), &self.key_data_types, bytes)
    }

    pub fn find_child_page_id(&self, _key: Vec<Datum>) -> PageID {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let _num_child = u32::from_le_bytes(page.borrow().buffer[0..4].try_into().unwrap());
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
