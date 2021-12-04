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

use internal::InternalNode;
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

    pub fn split_on_internal(&mut self, _internal_node: &mut InternalNode) {
        todo!()
    }

    pub fn set_root(&mut self, _key: &[Datum], _page_id_lhs: PageID, _page_id_rhs: PageID) {
        todo!()
    }

    pub fn split_on_leaf(&mut self, leaf_node: &mut LeafNode) {
        let lhs_node = leaf_node;
        let rhs_node = lhs_node.split();
        let _new_key = rhs_node.datums_at(0);
        let _new_value = rhs_node.get_page_id();
        let parent_page_id = lhs_node.get_parent_page_id();
        if parent_page_id.is_none() {
            todo!()
        }
        todo!()
    }

    /// 1. fetch the root node;
    /// 2. find the leaf node corresponding to the inserting key;
    /// 3. have enough space ? insert => done : split => 4
    /// 4. split, insert into parent => 3
    pub fn insert(&mut self, key: Vec<Datum>, _rid: RecordID) -> Result<(), IndexError> {
        let mut page_id_of_current_node = self.get_page_id_of_root();
        let key_size = self.get_key_size();
        let data_types = self.get_data_types();
        let mut leaf_node = loop {
            if let Ok(leaf_node) = LeafNode::open(
                self.bpm.clone(),
                data_types.clone(),
                key_size,
                key_size <= Self::INLINED_LIMIT,
                page_id_of_current_node,
            ) {
                break leaf_node;
            }
            let internal_node = InternalNode::open(
                self.bpm.clone(),
                data_types.clone(),
                key_size,
                key_size <= Self::INLINED_LIMIT,
                page_id_of_current_node,
            )?;
            let branch_idx = internal_node.index_of(key.as_slice());
            page_id_of_current_node = internal_node.value_at(branch_idx).unwrap()
        };
        if !leaf_node.ok_to_insert() {
            self.split_on_leaf(&mut leaf_node);
        }
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
