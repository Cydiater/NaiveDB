use crate::datum::Datum;
use crate::index::{IndexError, RecordID};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, SlottedPage};
use crate::table::SchemaRef;
use itertools::Itertools;

///
/// LeafNode Format:
///
///     | Meta | offset[0] | record_id[0] | ...
///                                       ... | data[1] | data[0] |
///
/// Meta Format:
///
///     | is_leaf | parent_page_id | head | tail |
///

impl Drop for LeafNode {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

#[derive(Clone, Copy)]
pub struct LeafMeta {
    pub is_leaf: bool,
    pub parent_page_id: Option<PageID>,
    pub next_page_id: Option<PageID>,
}

type LeafPage = SlottedPage<LeafMeta, RecordID>;

#[derive(Clone)]
pub struct LeafNode {
    page: PageRef,
    bpm: BufferPoolManagerRef,
    pub schema: SchemaRef,
}

#[allow(dead_code)]
impl LeafNode {
    fn leaf_page(&self) -> &LeafPage {
        unsafe { &*(self.page.borrow().buffer.as_ptr() as *const LeafPage) }
    }

    fn leaf_page_mut(&mut self) -> &mut LeafPage {
        self.page.borrow_mut().is_dirty = true;
        unsafe { &mut *(self.page.borrow_mut().buffer.as_mut_ptr() as *mut LeafPage) }
    }

    pub fn page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }

    pub fn get_free_space(&self) -> usize {
        let leaf_page = self.leaf_page();
        leaf_page.get_free_space()
    }

    pub fn meta(&self) -> &LeafMeta {
        let leaf_page = self.leaf_page();
        leaf_page.meta()
    }

    pub fn meta_mut(&mut self) -> &mut LeafMeta {
        let leaf_page = self.leaf_page_mut();
        leaf_page.meta_mut()
    }

    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        unsafe {
            let slotted = &mut *(page.borrow_mut().buffer.as_mut_ptr() as *mut LeafPage);
            slotted.reset(&LeafMeta {
                is_leaf: true,
                parent_page_id: None,
                next_page_id: None,
            });
        }
        // mark dirty
        page.borrow_mut().is_dirty = true;
        Self { page, bpm, schema }
    }

    pub fn open(
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
        page_id: PageID,
    ) -> Result<Self, IndexError> {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        unsafe {
            let slotted = &*(page.borrow().buffer.as_ptr() as *const LeafPage);
            if !slotted.meta().is_leaf {
                return Err(IndexError::NotLeafIndexNode);
            }
        }
        Ok(Self { page, bpm, schema })
    }

    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        let leaf_page = self.leaf_page();
        Datum::from_bytes_and_schema(self.schema.as_ref(), leaf_page.data_at(idx))
    }

    pub fn record_id_at(&self, idx: usize) -> RecordID {
        let leaf_page = self.leaf_page();
        *leaf_page.key_at(idx)
    }

    pub fn len(&self) -> usize {
        let leaf_page = self.leaf_page();
        leaf_page.capacity()
    }

    pub fn lower_bound(&self, key: &[Datum]) -> Option<usize> {
        if self.len() == 0 {
            return None;
        }
        let mut left = 0;
        let mut right = self.len() - 1;
        let mut mid;
        while left + 1 < right {
            mid = (left + right) / 2;
            if self.key_at(mid).as_slice() < key {
                left = mid;
            } else {
                right = mid;
            }
        }
        if self.key_at(left).as_slice() >= key {
            Some(left)
        } else if self.key_at(right).as_slice() >= key {
            Some(right)
        } else {
            None
        }
    }

    pub fn index_of(&self, key: &[Datum]) -> Option<usize> {
        let lower_bound_idx = self.lower_bound(key);
        if let Some(idx) = lower_bound_idx {
            if self.key_at(idx) == key {
                Some(idx)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// split current node into two node, the lhs node have the front half while the rhs node have
    /// the back half, and they have the same parent_id
    pub fn split(&mut self) -> Self {
        let schema = self.schema.clone();
        let mut rhs = LeafNode::new(self.bpm.clone(), self.schema.clone());
        let leaf_page = self.leaf_page_mut();
        let tuple_and_record_id_set = leaf_page
            .key_data_iter()
            .map(|(record_id, bytes)| {
                (
                    *record_id,
                    Datum::from_bytes_and_schema(schema.as_ref(), bytes),
                )
            })
            .collect_vec();
        leaf_page.reset(&leaf_page.meta().clone());
        let len = tuple_and_record_id_set.len();
        let len_lhs = len / 2;
        // setup lhs node
        for tuple_and_record_id in tuple_and_record_id_set.iter().take(len_lhs) {
            leaf_page
                .insert(
                    &tuple_and_record_id.0,
                    &Datum::to_bytes_with_schema(&tuple_and_record_id.1, schema.as_ref()),
                )
                .unwrap();
        }
        // setup rhs node
        for tuple_and_record_id in tuple_and_record_id_set.iter().take(len).skip(len_lhs) {
            rhs.append(&tuple_and_record_id.1, tuple_and_record_id.0)
                .unwrap();
        }
        // set parent_page_id
        rhs.meta_mut().parent_page_id = leaf_page.meta().parent_page_id;
        rhs.meta_mut().next_page_id = leaf_page.meta().next_page_id;
        leaf_page.meta_mut().next_page_id = Some(rhs.page_id());
        self.page.borrow_mut().is_dirty = true;
        rhs
    }

    /// append to the end, the order should be preserved
    pub fn append(&mut self, key: &[Datum], record_id: RecordID) -> Result<(), IndexError> {
        self.insert_at(self.len(), key, record_id)
    }

    /// random insert
    pub fn insert(&mut self, key: &[Datum], record_id: RecordID) -> Result<(), IndexError> {
        let idx = self.lower_bound(key).unwrap_or_else(|| self.len());
        self.insert_at(idx, key, record_id)
    }

    /// insert at specific position
    pub fn insert_at(
        &mut self,
        idx: usize,
        key: &[Datum],
        record_id: RecordID,
    ) -> Result<(), IndexError> {
        let schema = self.schema.clone();
        let leaf_page = self.leaf_page_mut();
        leaf_page.move_backward(idx)?;
        leaf_page.insert_at(
            idx,
            &record_id,
            &Datum::to_bytes_with_schema(key, schema.as_ref()),
        )?;
        Ok(())
    }

    pub fn remove(&mut self, key: &[Datum]) -> Result<(), IndexError> {
        let idx = self.index_of(key).ok_or(IndexError::KeyNotFound)?;
        let leaf_page_mut = self.leaf_page_mut();
        leaf_page_mut.remove_at(idx)?;
        leaf_page_mut.move_forward(idx)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn sanity_check(&self) {
        let len = self.len();
        let mut last_key = None;
        for idx in 0..len {
            let key = self.key_at(idx);
            if let Some(last_key) = last_key {
                assert!(last_key < key);
            }
            last_key = Some(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::DataType;
    use crate::storage::BufferPoolManager;
    use crate::table::Schema;
    use std::fs::remove_file;
    use std::rc::Rc;

    #[test]
    fn test_append_remove() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(10);
            let filename = bpm.borrow().filename();
            let schema = Rc::new(Schema::from_slice(&[(
                DataType::new_int(false),
                "v1".to_string(),
            )]));
            let dummy_record_id = (0, 0);
            let mut node = LeafNode::new(bpm, schema);
            node.append(&[Datum::Int(Some(0))], dummy_record_id)
                .unwrap();
            node.append(&[Datum::Int(Some(1))], dummy_record_id)
                .unwrap();
            node.append(&[Datum::Int(Some(2))], dummy_record_id)
                .unwrap();
            assert_eq!(node.len(), 3);
            node.remove(&[Datum::Int(Some(0))]).unwrap();
            assert_eq!(node.key_at(0), [Datum::Int(Some(1))]);
            assert_eq!(node.key_at(1), [Datum::Int(Some(2))]);
            filename
        };
        remove_file(filename).unwrap()
    }
}
