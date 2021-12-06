use crate::datum::Datum;
use crate::storage::{BufferPoolManagerRef, PageID, PageRef};
use crate::table::Schema;
use std::convert::TryInto;
use std::ops::Range;
use std::rc::Rc;
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

    pub fn split_on_internal(&mut self, _internal_node: &mut InternalNode) {
        todo!()
    }

    pub fn split_on_leaf(&mut self, leaf_node: &mut LeafNode) {
        let lhs_node = leaf_node;
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
            self.split_on_internal(&mut parent_node);
        }
        parent_node.insert(&new_key, new_value);
    }

    /// 1. fetch the root node;
    /// 2. find the leaf node corresponding to the inserting key;
    /// 3. have enough space ? insert => done : split => 4
    /// 4. split, insert into parent => 3
    pub fn insert(&mut self, key: &[Datum], _rid: RecordID) -> Result<(), IndexError> {
        let mut page_id_of_current_node = self.get_page_id_of_root();
        let schema = Rc::new(self.get_key_schema());
        let mut leaf_node = loop {
            if let Ok(leaf_node) =
                LeafNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
            {
                break leaf_node;
            }
            let internal_node =
                InternalNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)?;
            let branch_idx = internal_node.index_of(key);
            page_id_of_current_node = internal_node.page_id_at(branch_idx).unwrap();
        };
        if !leaf_node.ok_to_insert(key) {
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
