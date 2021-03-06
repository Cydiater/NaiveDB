use crate::datum::Datum;
use crate::expr::{ColumnRefExpr, ExprImpl};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, SlottedPageError, PAGE_SIZE};
use crate::table::{Schema, SchemaRef};
use itertools::Itertools;
use std::convert::TryInto;
use std::ops::Range;
use std::rc::Rc;
use thiserror::Error;

pub type RecordID = (PageID, usize);

#[derive(Copy, Clone)]
pub struct IndexNodeMeta {
    pub is_leaf: bool,
    pub parent_page_id: Option<PageID>,
    pub next_page_id: Option<PageID>,
}

enum IndexNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl IndexNode {
    pub fn open(bpm: BufferPoolManagerRef, schema: SchemaRef, page_id: PageID) -> Self {
        if let Ok(leaf) = LeafNode::open(bpm.clone(), schema.clone(), page_id) {
            IndexNode::Leaf(leaf)
        } else {
            IndexNode::Internal(InternalNode::open(bpm, schema, page_id).unwrap())
        }
    }
    pub fn store_stat(&self) -> (usize, usize) {
        match self {
            Self::Leaf(node) => node.store_stat(),
            Self::Internal(node) => node.store_stat(),
        }
    }
    pub fn meta_mut(&mut self) -> &mut IndexNodeMeta {
        match self {
            Self::Leaf(node) => &mut node.meta_mut().common,
            Self::Internal(node) => &mut node.meta_mut().common,
        }
    }
    pub fn meta(&self) -> &IndexNodeMeta {
        match self {
            Self::Leaf(node) => &node.meta().common,
            Self::Internal(node) => &node.meta().common,
        }
    }
    pub fn page_id(&self) -> PageID {
        match self {
            Self::Leaf(node) => node.page_id(),
            Self::Internal(node) => node.page_id(),
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
    pub fn first_key(&self) -> Vec<Datum> {
        match self {
            Self::Internal(node) => node.key_at(0),
            Self::Leaf(node) => node.key_at(0),
        }
    }
    pub fn steal_from_right(
        &mut self,
        sibling: &mut Self,
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
    ) {
        match (self, sibling) {
            (IndexNode::Leaf(leaf), IndexNode::Leaf(sibling)) => {
                let key = sibling.key_at(0);
                let record_id = sibling.record_id_at(0);
                sibling.remove(&key).unwrap();
                leaf.append(&key, record_id).unwrap();
            }
            (IndexNode::Internal(internal), IndexNode::Internal(sibling)) => {
                let key = sibling.key_at(0);
                let page_id = sibling.page_id_at(0);
                sibling.remove(&key).unwrap();
                internal.append(&key, page_id).unwrap();
                let mut child = IndexNode::open(bpm, schema, page_id);
                child.meta_mut().parent_page_id = Some(internal.page_id());
            }
            _ => unreachable!(),
        }
    }
    pub fn steal_from_left(
        &mut self,
        sibling: &mut Self,
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
    ) {
        match (self, sibling) {
            (IndexNode::Leaf(leaf), IndexNode::Leaf(sibling)) => {
                let idx = sibling.len() - 1;
                let key = sibling.key_at(idx);
                let record_id = sibling.record_id_at(idx);
                sibling.remove(&key).unwrap();
                leaf.insert(&key, record_id).unwrap();
            }
            (IndexNode::Internal(internal), IndexNode::Internal(sibling)) => {
                let idx = sibling.len() - 1;
                let key = sibling.key_at(idx);
                let page_id = sibling.page_id_at(idx);
                sibling.remove(&key).unwrap();
                internal.insert(&key, page_id).unwrap();
                let mut child = IndexNode::open(bpm, schema, page_id);
                child.meta_mut().parent_page_id = Some(internal.page_id());
            }
            _ => unreachable!(),
        }
    }
    pub fn merge_in_back(
        &mut self,
        sibling: &mut Self,
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
    ) {
        self.meta_mut().next_page_id = sibling.meta().next_page_id;
        match (self, sibling) {
            (IndexNode::Leaf(leaf), IndexNode::Leaf(sibling)) => {
                for idx in 0..sibling.len() {
                    let key = sibling.key_at(idx);
                    let record_id = sibling.record_id_at(idx);
                    leaf.append(&key, record_id).unwrap();
                }
            }
            (IndexNode::Internal(internal), IndexNode::Internal(sibling)) => {
                assert_eq!(sibling.meta().leftmost, None);
                let len = sibling.len();
                for idx in 0..len {
                    let key = sibling.key_at(idx);
                    let page_id = sibling.page_id_at(idx);
                    internal.append(&key, page_id).unwrap();
                    let mut child = IndexNode::open(bpm.clone(), schema.clone(), page_id);
                    child.meta_mut().parent_page_id = Some(internal.page_id());
                }
            }
            _ => unreachable!(),
        }
    }

    #[allow(dead_code)]
    pub fn sanity_check(&self) {
        match self {
            IndexNode::Internal(n) => n.sanity_check(),
            IndexNode::Leaf(n) => n.sanity_check(),
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
///     | page_id_of_root | len_of_indexed_column | column_idx[0] | ... |
///
/// IndexSchema here is used as the key schema, the page layout of index page is mostly same as
/// Slice.
///

#[allow(dead_code)]
pub struct BPTIndex {
    page: PageRef,
    bpm: BufferPoolManagerRef,
    pub exprs: Vec<ExprImpl>,
}

mod internal;
mod leaf;

use internal::InternalNode;
use leaf::LeafNode;

pub struct IndexIter {
    leaf: LeafNode,
    bpm: BufferPoolManagerRef,
    idx: usize,
}

impl IndexIter {
    pub fn new(leaf: LeafNode, bpm: BufferPoolManagerRef, idx: usize) -> Self {
        Self { leaf, bpm, idx }
    }
}

impl Iterator for IndexIter {
    type Item = (Vec<Datum>, RecordID);

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.leaf.len();
        if self.idx == len {
            let next_page_id = self.leaf.meta().common.next_page_id;
            if let Some(next_page_id) = next_page_id {
                self.idx = 0;
                self.leaf =
                    LeafNode::open(self.bpm.clone(), self.leaf.schema.clone(), next_page_id)
                        .unwrap();
            } else {
                return None;
            }
        }
        let datums = self.leaf.key_at(self.idx);
        let record_id = self.leaf.record_id_at(self.idx);
        self.idx += 1;
        Some((datums, record_id))
    }
}

impl BPTIndex {
    const PAGE_ID_OF_ROOT: Range<usize> = 0..4;
    const LEN_OF_INDEXED_COLUMN_IDS: Range<usize> = 4..8;

    pub fn get_page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }

    pub fn erase(self) {
        todo!()
    }

    pub fn new(bpm: BufferPoolManagerRef, exprs: Vec<ExprImpl>) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        let schema = Rc::new(Schema::from_exprs(&exprs));
        let leaf_node = LeafNode::new(bpm.clone(), schema);
        let page_id_of_root = leaf_node.page_id();
        // set page_id_of_root
        page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id_of_root as u32).to_le_bytes());
        let indexed_columns_ids = exprs
            .iter()
            .map(|e| {
                if let ExprImpl::ColumnRef(cf) = e {
                    cf.as_idx()
                } else {
                    unreachable!()
                }
            })
            .collect_vec();
        let len_of_indexed_column_ids = indexed_columns_ids.len() as u32;
        page.borrow_mut().buffer[Self::LEN_OF_INDEXED_COLUMN_IDS]
            .copy_from_slice(&(len_of_indexed_column_ids.to_le_bytes()));
        let bytes: Vec<u8> = indexed_columns_ids
            .into_iter()
            .flat_map(|idx| (idx as u32).to_le_bytes())
            .collect_vec();
        let len = bytes.len();
        page.borrow_mut().buffer[8..8 + len].copy_from_slice(&bytes);
        page.borrow_mut().is_dirty = true;
        Self { bpm, page, exprs }
    }

    pub fn open(bpm: BufferPoolManagerRef, page_id: PageID, table_schema: &Schema) -> Self {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        let len_of_indexed_column_ids = u32::from_le_bytes(
            page.borrow().buffer[Self::LEN_OF_INDEXED_COLUMN_IDS]
                .try_into()
                .unwrap(),
        ) as usize;
        let exprs = (0..len_of_indexed_column_ids)
            .into_iter()
            .map(|idx| {
                let start = 8 + 4 * idx;
                let end = start + 4;
                let column_idx =
                    u32::from_le_bytes(page.borrow().buffer[start..end].try_into().unwrap())
                        as usize;
                ExprImpl::ColumnRef(ColumnRefExpr::new(
                    column_idx,
                    table_schema.columns[column_idx].data_type,
                    table_schema.columns[column_idx].desc.clone(),
                ))
            })
            .collect_vec();
        Self { bpm, page, exprs }
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
        Schema::from_exprs(&self.exprs)
    }

    pub fn set_root(&mut self, key: &[Datum], page_id_lhs: PageID, page_id_rhs: PageID) {
        let root_node = InternalNode::new_as_root(
            self.bpm.clone(),
            Rc::new(self.get_key_schema()),
            key,
            page_id_lhs,
            page_id_rhs,
        );
        self.set_page_id_of_root(root_node.page_id());
    }

    /// split an IndexNode, return the middle key and node at rhs
    fn split(&mut self, node: &mut IndexNode) -> (Vec<Datum>, IndexNode) {
        let mut rhs_node = node.split();
        let new_key = rhs_node.key_at(0);
        let new_value = rhs_node.page_id();
        let parent_page_id = node.meta().parent_page_id;
        if parent_page_id.is_none() {
            self.set_root(&new_key, node.page_id(), new_value);
            let page_id_of_root = self.get_page_id_of_root();
            node.meta_mut().parent_page_id = Some(page_id_of_root);
            rhs_node.meta_mut().parent_page_id = Some(page_id_of_root);
            return (new_key, rhs_node);
        }
        let parent_page_id = parent_page_id.unwrap();
        let mut parent_node = InternalNode::open(
            self.bpm.clone(),
            Rc::new(self.get_key_schema()),
            parent_page_id,
        )
        .unwrap();
        let parent_page_id = if parent_node.insert(new_key.as_slice(), new_value).is_err() {
            let (middle_key, rhs_node) = self.split(&mut IndexNode::Internal(parent_node.clone()));
            if new_key >= middle_key {
                if let IndexNode::Internal(mut rhs_node) = rhs_node {
                    rhs_node.insert(&new_key, new_value).unwrap();
                    rhs_node.page_id()
                } else {
                    unreachable!()
                }
            } else {
                parent_node.insert(&new_key, new_value).unwrap();
                parent_node.page_id()
            }
        } else {
            parent_node.page_id()
        };
        rhs_node.meta_mut().parent_page_id = Some(parent_page_id);
        (new_key, rhs_node)
    }

    fn find_leaf(&self, key: &[Datum]) -> Option<LeafNode> {
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
            if branch_idx == -1 {
                if let Some(page_id) = internal_node.meta().leftmost {
                    page_id_of_current_node = page_id;
                } else {
                    break None;
                }
            } else {
                page_id_of_current_node = internal_node.page_id_at(branch_idx as usize);
            }
        }
    }

    pub fn iter_start_from(&self, key: &[Datum]) -> Option<IndexIter> {
        let leaf = self.find_leaf(key);
        if let Some(leaf) = leaf {
            leaf.lower_bound(key)
                .map(|idx| IndexIter::new(leaf, self.bpm.clone(), idx))
        } else {
            None
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
        if leaf_node.index_of(key).is_some() {
            return Err(IndexError::Duplicated);
        }
        if leaf_node.insert(key, record_id).is_err() {
            let mut index_node = IndexNode::Leaf(leaf_node);
            let (middle_key, rhs_node) = self.split(&mut index_node);
            if key >= middle_key.as_slice() {
                if let IndexNode::Leaf(mut rhs_node) = rhs_node {
                    rhs_node.insert(key, record_id)?;
                }
            } else if let IndexNode::Leaf(mut node) = index_node {
                node.insert(key, record_id)?;
            }
        }
        Ok(())
    }

    pub fn last_key(&self) -> Vec<Datum> {
        let mut page_id_of_current_node = self.get_page_id_of_root();
        let schema = Rc::new(self.get_key_schema());
        let first_leaf = loop {
            if let Ok(leaf_node) =
                LeafNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
            {
                break leaf_node;
            }
            let internal_node =
                InternalNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
                    .unwrap();
            page_id_of_current_node = internal_node.page_id_at(internal_node.len() - 1)
        };
        first_leaf.key_at(first_leaf.len() - 1)
    }

    pub fn first_key(&self) -> Vec<Datum> {
        let mut page_id_of_current_node = self.get_page_id_of_root();
        let schema = Rc::new(self.get_key_schema());
        let first_leaf = loop {
            if let Ok(leaf_node) =
                LeafNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
            {
                break leaf_node;
            }
            let internal_node =
                InternalNode::open(self.bpm.clone(), schema.clone(), page_id_of_current_node)
                    .unwrap();
            page_id_of_current_node = if let Some(page_id) = internal_node.meta().leftmost {
                page_id
            } else {
                internal_node.page_id_at(0)
            }
        };
        first_leaf.key_at(0)
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

    fn balance(&mut self, node: &mut IndexNode) {
        // 0. no need to balance
        if node.store_stat().1 < PAGE_SIZE / 2 {
            return;
        }
        let parent_page_id = node.meta().parent_page_id;
        if parent_page_id.is_none() {
            // 1. root node
            if let IndexNode::Leaf(_) = node {
                // 1.1 root node is leaf, ignore
                return;
            } else if let IndexNode::Internal(node) = node {
                // 1.2 root node is internal
                if let Some(page_id_of_only_child) = node.candidate_child() {
                    // 1.2.1 only have one child, we can remove it
                    self.set_page_id_of_root(page_id_of_only_child);
                    let mut node = IndexNode::open(
                        self.bpm.clone(),
                        Rc::new(self.get_key_schema()),
                        page_id_of_only_child,
                    );
                    node.meta_mut().parent_page_id = None;
                    return;
                }
                return;
            } else {
                unreachable!();
            }
        }
        // 2. have parent
        let parent_page_id = parent_page_id.unwrap();
        let mut parent = InternalNode::open(
            self.bpm.clone(),
            Rc::new(self.get_key_schema()),
            parent_page_id,
        )
        .unwrap();
        let (p, q) = parent.siblings_of(node.page_id());
        if let Some((page_id_of_left_sibling, key)) = p {
            // 2.1 have left sibling, try balance to left
            let mut left_sibling = IndexNode::open(
                self.bpm.clone(),
                Rc::new(self.get_key_schema()),
                page_id_of_left_sibling,
            );
            // 2.1.2 ok to merge
            if left_sibling.store_stat().1 >= node.store_stat().0 {
                parent.remove(&key).unwrap();
                left_sibling.merge_in_back(node, self.bpm.clone(), Rc::new(self.get_key_schema()));
                self.balance(&mut IndexNode::Internal(parent));
                return;
            }
            // 2.1.3 steal left
            node.steal_from_left(
                &mut left_sibling,
                self.bpm.clone(),
                Rc::new(self.get_key_schema()),
            );
            let stolen_key = node.first_key();
            parent.update_key_with(&key, &stolen_key);
        } else if let Some((key, page_id_of_right_sibling)) = q {
            // 2.2 have right sibling, try balance to right
            let mut right_sibling = IndexNode::open(
                self.bpm.clone(),
                Rc::new(self.get_key_schema()),
                page_id_of_right_sibling,
            );
            // 2.1.2 ok to merge
            if node.store_stat().1 >= right_sibling.store_stat().0 {
                parent.remove(&key).unwrap();
                node.merge_in_back(
                    &mut right_sibling,
                    self.bpm.clone(),
                    Rc::new(self.get_key_schema()),
                );
                self.balance(&mut IndexNode::Internal(parent));
                return;
            }
            // 2.1.3 steal right
            node.steal_from_right(
                &mut right_sibling,
                self.bpm.clone(),
                Rc::new(self.get_key_schema()),
            );
            let stolen_key = right_sibling.first_key();
            parent.update_key_with(&key, &stolen_key);
        } else {
            unreachable!()
        }
    }

    pub fn remove(&mut self, key: &[Datum]) -> Result<(), IndexError> {
        let mut leaf_node = self.find_leaf(key).ok_or(IndexError::KeyNotFound)?;
        leaf_node.remove(key)?;
        self.balance(&mut IndexNode::Leaf(leaf_node));
        Ok(())
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
    #[error("node out of space")]
    NodeOutOfSpace,
    #[error("duplicated key")]
    Duplicated,
    #[error("Page Error: {0}")]
    PageError(#[from] SlottedPageError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::DataType;
    use crate::expr::ColumnRefExpr;
    use crate::storage::BufferPoolManager;
    use itertools::Itertools;
    use rand::Rng;
    use std::collections::HashSet;
    use std::fs::remove_file;

    #[test]
    fn test_insert_find_remove() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(20);
            let filename = bpm.borrow().filename();
            let exprs = vec![ExprImpl::ColumnRef(ColumnRefExpr::new(
                0,
                DataType::new_as_int(false),
                "v1".to_string(),
            ))];
            let mut index = BPTIndex::new(bpm, exprs);
            index.insert(&[Datum::Int(Some(0))], (0, 0)).unwrap();
            index.insert(&[Datum::Int(Some(1))], (1, 0)).unwrap();
            index.insert(&[Datum::Int(Some(2))], (2, 0)).unwrap();
            index.insert(&[Datum::Int(Some(3))], (3, 0)).unwrap();
            index.insert(&[Datum::Int(Some(4))], (4, 0)).unwrap();
            assert_eq!(index.find(&[Datum::Int(Some(2))]), Some((2, 0)));
            assert_eq!(index.find(&[Datum::Int(Some(4))]), Some((4, 0)));
            index.remove(&[Datum::Int(Some(0))]).unwrap();
            assert_eq!(index.find(&[Datum::Int(Some(0))]), None);
            assert_eq!(index.find(&[Datum::Int(Some(2))]), Some((2, 0)));
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn chaos_test() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(2000);
            let filename = bpm.borrow().filename();
            let exprs = vec![ExprImpl::ColumnRef(ColumnRefExpr::new(
                0,
                DataType::new_as_int(false),
                "v1".to_string(),
            ))];
            let mut index = BPTIndex::new(bpm, exprs);
            let mut set: HashSet<u16> = HashSet::new();
            let mut rng = rand::thread_rng();
            for _ in 0..100000 {
                let num: u16 = rng.gen();
                if set.contains(&num) {
                    set.remove(&num);
                    index.remove(&[Datum::Int(Some(num as i32))]).unwrap();
                } else {
                    set.insert(num);
                    index
                        .insert(
                            &[Datum::Int(Some(num as i32))],
                            (num as usize, num as usize),
                        )
                        .unwrap();
                }
            }
            for num in set.iter().sorted() {
                assert_eq!(
                    (*num as usize, *num as usize),
                    index.find(&[Datum::Int(Some(*num as i32))]).unwrap()
                );
            }
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_split_find_iter_remove() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(2000);
            let filename = bpm.borrow().filename();
            let exprs = vec![ExprImpl::ColumnRef(ColumnRefExpr::new(
                0,
                DataType::new_as_int(false),
                "v1".to_string(),
            ))];
            let mut index = BPTIndex::new(bpm, exprs);
            for idx in 0..40000usize {
                index
                    .insert(&[Datum::Int(Some(idx as i32))], (idx, idx))
                    .unwrap();
            }
            for idx in 0..40000usize {
                assert_eq!(
                    index.find(&[Datum::Int(Some(idx as i32))]).unwrap(),
                    (idx, idx)
                );
            }
            let res = index
                .iter_start_from(&[Datum::Int(Some(1000))])
                .unwrap()
                .take(100)
                .collect_vec();
            for (idx, res) in res.iter().enumerate() {
                assert_eq!(res.0, vec![Datum::Int(Some((idx + 1000) as i32))]);
            }
            assert_eq!(index.first_key(), vec![Datum::Int(Some(0))]);
            assert_eq!(index.last_key(), vec![Datum::Int(Some(39999))]);
            for idx in (0..40000usize).step_by(2) {
                index.remove(&[Datum::Int(Some(idx as i32))]).unwrap();
            }
            let res = index
                .iter_start_from(&[Datum::Int(Some(0))])
                .unwrap()
                .collect_vec();
            for (idx, res) in res.iter().enumerate() {
                assert_eq!(res.0, vec![Datum::Int(Some((idx * 2 + 1) as i32))]);
            }
            filename
        };
        remove_file(filename).unwrap();
    }
}
