use crate::datum::Datum;
use crate::storage::{BufferPoolManagerRef, PageID, PageRef};
use crate::table::{Schema, SchemaRef};
use std::convert::TryInto;
use std::ops::Range;
use std::rc::Rc;
use thiserror::Error;

#[allow(dead_code)]
pub type RecordID = (PageID, usize);

enum IndexNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl IndexNode {
    pub fn get_parent_page_id(&self) -> Option<PageID> {
        match self {
            Self::Leaf(node) => node.get_parent_page_id(),
            Self::Internal(node) => node.get_parent_page_id(),
        }
    }
    pub fn get_page_id(&self) -> PageID {
        match self {
            Self::Leaf(node) => node.get_page_id(),
            Self::Internal(node) => node.get_page_id(),
        }
    }
    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        match self {
            Self::Leaf(node) => node.key_at(idx),
            Self::Internal(node) => node.key_at(idx),
        }
    }
    pub fn split(&mut self) -> Self {
        match self {
            Self::Leaf(node) => Self::Leaf(node.split()),
            Self::Internal(node) => Self::Internal(node.split()),
        }
    }
}

impl Drop for BPTIndex {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

///
/// Index Format:
///
///     | page_id_of_root | IndexSchema
///
/// IndexSchema here is used as the key schema, the page layout of index page is mostly same as
/// Slice.
///

#[allow(dead_code)]
pub struct BPTIndex {
    page: PageRef,
    bpm: BufferPoolManagerRef,
}

mod internal;
mod leaf;

use internal::InternalNode;
use leaf::LeafNode;

#[allow(dead_code)]
impl BPTIndex {
    const PAGE_ID_OF_ROOT: Range<usize> = 0..4;
    const SIZE_OF_META: usize = 4;

    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        let leaf_node = LeafNode::new(bpm.clone(), schema.clone());
        let page_id_of_root = leaf_node.get_page_id();
        // set page_id_of_root
        page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id_of_root as u32).to_le_bytes());
        // set schema
        let bytes = schema.to_bytes();
        let len = bytes.len();
        page.borrow_mut().buffer[4..4 + len].copy_from_slice(&bytes);
        page.borrow_mut().is_dirty = true;
        Self { bpm, page }
    }

    pub fn get_page_id_of_root(&self) -> PageID {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::PAGE_ID_OF_ROOT]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn set_page_id_of_root(&self, page_id: PageID) {
        self.page.borrow_mut().buffer[Self::PAGE_ID_OF_ROOT]
            .copy_from_slice(&(page_id as u32).to_le_bytes())
    }

    pub fn get_key_schema(&self) -> Schema {
        Schema::from_bytes(&self.page.borrow().buffer[Self::SIZE_OF_META..])
    }

    pub fn set_root(&mut self, key: &[Datum], page_id_lhs: PageID, page_id_rhs: PageID) {
        let root_node = InternalNode::new_root(
            self.bpm.clone(),
            Rc::new(self.get_key_schema()),
            key,
            page_id_lhs,
            page_id_rhs,
        );
        self.set_page_id_of_root(root_node.get_page_id());
    }

    fn split(&mut self, node: &mut IndexNode) {
        let lhs_node = node;
        let rhs_node = lhs_node.split();
        let new_key = rhs_node.key_at(0);
        let new_value = rhs_node.get_page_id();
        let parent_page_id = lhs_node.get_parent_page_id();
        if parent_page_id.is_none() {
            self.set_root(&new_key, lhs_node.get_page_id(), new_value);
            return;
        }
        let parent_page_id = parent_page_id.unwrap();
        let mut parent_node = InternalNode::open(
            self.bpm.clone(),
            Rc::new(self.get_key_schema()),
            parent_page_id,
        )
        .unwrap();
        if !parent_node.ok_to_insert(new_key.as_slice()) {
            self.split(&mut IndexNode::Internal(parent_node.clone()));
        }
        parent_node.insert(&new_key, new_value);
    }

    pub fn find_leaf(&self, key: &[Datum]) -> Option<LeafNode> {
        let mut page_id_of_current_node = self.get_page_id_of_root();
        let schema = Rc::new(self.get_key_schema());
        loop {
            if let Ok(leaf_node) =
                LeafNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
            {
                break Some(leaf_node);
            }
            let internal_node =
                InternalNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
                    .unwrap();
            let branch_idx = internal_node.index_of(key);
            page_id_of_current_node =
                if let Some(page_id_of_current_node) = internal_node.page_id_at(branch_idx) {
                    page_id_of_current_node
                } else {
                    break None;
                }
        }
    }

    /// 1. fetch the root node;
    /// 2. find the leaf node corresponding to the inserting key;
    /// 3. have enough space ? insert => done : split => 4
    /// 4. split, insert into parent => 3
    pub fn insert(&mut self, key: &[Datum], record_id: RecordID) -> Result<(), IndexError> {
        let mut leaf_node = if let Some(leaf_node) = self.find_leaf(key) {
            leaf_node
        } else {
            return Err(IndexError::KeyNotFound);
        };
        if !leaf_node.ok_to_insert(key) {
            self.split(&mut IndexNode::Leaf(leaf_node.clone()));
        }
        leaf_node.insert(key, record_id);
        Ok(())
    }

    pub fn find(&self, key: &[Datum]) -> Option<RecordID> {
        if let Some(leaf_node) = self.find_leaf(key) {
            leaf_node
                .index_of(key)
                .map(|idx| leaf_node.record_id_at(idx))
        } else {
            None
        }
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error("open leaf page as internal index node")]
    NotInternalIndexNode,
    #[error("open leaf page as leaf index node")]
    NotLeafIndexNode,
    #[error("can not find key in the index")]
    KeyNotFound,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::DataType;
    use crate::storage::BufferPoolManager;
    use std::fs::remove_file;

    #[test]
    fn test_insert_find() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(20);
            let filename = bpm.borrow().filename();
            let schema = Rc::new(Schema::from_slice(&[(
                DataType::new_int(false),
                "v1".to_string(),
            )]));
            let mut index = BPTIndex::new(bpm, schema);
            index.insert(&[Datum::Int(Some(0))], (0, 0)).unwrap();
            index.insert(&[Datum::Int(Some(1))], (1, 0)).unwrap();
            index.insert(&[Datum::Int(Some(2))], (2, 0)).unwrap();
            index.insert(&[Datum::Int(Some(3))], (3, 0)).unwrap();
            index.insert(&[Datum::Int(Some(4))], (4, 0)).unwrap();
            assert_eq!(index.find(&[Datum::Int(Some(2))]), Some((2, 0)));
            assert_eq!(index.find(&[Datum::Int(Some(4))]), Some((4, 0)));
            filename
        };
        remove_file(filename).unwrap();
    }
}
