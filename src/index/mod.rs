use crate::datum::{DataType, Datum};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef};
use std::convert::TryInto;
use std::ops::Range;
use thiserror::Error;

#[allow(dead_code)]
pub type RecordID = (PageID, usize);

impl Drop for BPTIndex {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

///
/// | page_id_of_root | key_size | num_data_types | data_type[0] | ...
///

#[allow(dead_code)]
pub struct BPTIndex {
    page: PageRef,
    bpm: BufferPoolManagerRef,
}

mod internal;
mod leaf;
mod utils;

use leaf::LeafNode;

#[allow(dead_code)]
impl BPTIndex {
    const PAGE_ID_OF_ROOT: Range<usize> = 0..4;
    const KEY_SIZE: Range<usize> = 4..8;
    const NUM_DATA_TYPES: Range<usize> = 8..12;
    const SIZE_OF_META: usize = 12;

    const INLINED_LIMIT: usize = 256;

    pub fn get_page_id_of_root(&self) -> PageID {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::PAGE_ID_OF_ROOT]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn get_key_size(&self) -> usize {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::KEY_SIZE]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn get_num_data_types(&self) -> usize {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::NUM_DATA_TYPES]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn get_data_types(&self) -> Vec<DataType> {
        let num_data_types = self.get_num_data_types();
        let mut data_types = vec![];
        for idx in 0..num_data_types {
            let start = Self::SIZE_OF_META + idx * 5;
            let end = start + 5;
            let data_type =
                DataType::from_bytes(self.page.borrow().buffer[start..end].try_into().unwrap())
                    .unwrap();
            data_types.push(data_type)
        }
        data_types
    }

    /// 1. fetch the root node;
    /// 2. find the leaf node corresponding to the inserting key;
    /// 3. have enough space ? insert => done : split => 4
    /// 4. split, insert into parent => 3
    pub fn insert(&mut self, _key: Vec<Datum>, _rid: RecordID) -> Result<(), IndexError> {
        let page_id_of_current_node = self.get_page_id_of_root();
        let key_size = self.get_key_size();
        let data_types = self.get_data_types();
        let _leaf_node = loop {
            if let Ok(leaf_node) = LeafNode::open(
                self.bpm.clone(),
                data_types,
                key_size,
                key_size <= Self::INLINED_LIMIT,
                page_id_of_current_node,
            ) {
                break leaf_node;
            }
            todo!()
        };
        todo!()
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("open leaf page as internal index node")]
    NotInternalIndexNode,
    #[error("open leaf page as leaf index node")]
    NotLeafIndexNode,
}
