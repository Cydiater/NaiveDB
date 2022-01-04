use crate::datum::Datum;
use crate::index::{IndexError, IndexNode, IndexNodeMeta};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, SlottedPage};
use crate::table::SchemaRef;
use itertools::Itertools;

impl Drop for InternalNode {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

#[derive(Clone)]
pub struct InternalNode {
    page: PageRef,
    bpm: BufferPoolManagerRef,
    schema: SchemaRef,
}

pub type Siblings = (Option<(PageID, Vec<Datum>)>, Option<(Vec<Datum>, PageID)>);

#[derive(Clone, Copy)]
pub struct InternalMeta {
    pub common: IndexNodeMeta,
    pub leftmost: Option<PageID>,
}

type InternalPage = SlottedPage<InternalMeta, PageID>;

impl InternalNode {
    fn internal_page(&self) -> &InternalPage {
        unsafe { &*(self.page.borrow().buffer.as_ptr() as *const InternalPage) }
    }

    fn internal_page_mut(&mut self) -> &mut InternalPage {
        self.page.borrow_mut().is_dirty = true;
        unsafe { &mut *(self.page.borrow_mut().buffer.as_mut_ptr() as *mut InternalPage) }
    }

    pub fn store_stat(&self) -> (usize, usize) {
        self.internal_page().store_stat()
    }

    pub fn meta(&self) -> &InternalMeta {
        let internal_page = self.internal_page();
        internal_page.meta()
    }

    pub fn page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }

    pub fn meta_mut(&mut self) -> &mut InternalMeta {
        self.internal_page_mut().meta_mut()
    }

    pub fn candidate_child(&self) -> Option<usize> {
        if self.len() == 0 {
            self.meta().leftmost
        } else if self.len() == 1 {
            if self.meta().leftmost.is_some() {
                None
            } else {
                Some(self.page_id_at(0))
            }
        } else {
            None
        }
    }

    pub fn new_as_root(
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
        key: &[Datum],
        page_id_lhs: PageID,
        page_id_rhs: PageID,
    ) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        unsafe {
            let buffer = &mut page.borrow_mut().buffer;
            let slotted = &mut *(buffer.as_mut_ptr() as *mut InternalPage);
            slotted.reset(&InternalMeta {
                common: IndexNodeMeta {
                    is_leaf: false,
                    parent_page_id: None,
                    next_page_id: None,
                },
                leftmost: Some(page_id_lhs),
            });
            let bytes = Datum::to_bytes_with_schema(key, schema.as_ref());
            slotted.append(&page_id_rhs, &bytes).unwrap();
        }
        page.borrow_mut().is_dirty = true;
        Self { page, bpm, schema }
    }

    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef, leftmost: Option<PageID>) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        unsafe {
            let buffer = &mut page.borrow_mut().buffer;
            let slotted = &mut *(buffer.as_mut_ptr() as *mut InternalPage);
            slotted.reset(&InternalMeta {
                common: IndexNodeMeta {
                    is_leaf: false,
                    parent_page_id: None,
                    next_page_id: None,
                },
                leftmost,
            });
        }
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
            let slotted = &*(page.borrow().buffer.as_ptr() as *const InternalPage);
            if slotted.meta().common.is_leaf {
                return Err(IndexError::NotInternalIndexNode);
            }
        }
        Ok(Self { page, bpm, schema })
    }

    pub fn len(&self) -> usize {
        self.internal_page().capacity()
    }

    pub fn page_id_at(&self, idx: usize) -> PageID {
        *self.internal_page().key_at(idx)
    }

    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        let internal_page = self.internal_page();
        Datum::from_bytes_and_schema(self.schema.as_ref(), internal_page.data_at(idx))
    }

    pub fn index_of(&self, key: &[Datum]) -> isize {
        let len = self.len();
        let mut left = 0;
        let mut right = len - 1;
        while left + 1 < right {
            let mid = (left + right) / 2;
            if key < &self.key_at(mid) {
                right = mid;
            } else {
                left = mid;
            }
        }
        if key >= &self.key_at(right) {
            right as isize
        } else if key >= &self.key_at(left) {
            left as isize
        } else {
            -1
        }
    }

    pub fn split(&mut self) -> Self {
        let schema = self.schema.clone();
        let bpm = self.bpm.clone();
        let internal_page = self.internal_page_mut();
        let key_page_id_set = internal_page
            .key_data_iter()
            .map(|(k, d)| (Datum::from_bytes_and_schema(schema.as_ref(), d), *k))
            .collect_vec();
        let meta = *internal_page.meta();
        internal_page.reset(&meta);
        let len = key_page_id_set.len();
        let len_lhs = len / 2;
        for (key, page_id) in key_page_id_set.iter().take(len_lhs) {
            internal_page
                .append(page_id, &Datum::to_bytes_with_schema(key, schema.as_ref()))
                .unwrap();
        }
        let mut rhs = InternalNode::new(bpm.clone(), schema.clone(), None);
        for (key, page_id) in key_page_id_set.into_iter().skip(len_lhs) {
            rhs.append(&key, page_id).unwrap();
            let mut child = IndexNode::open(bpm.clone(), schema.clone(), page_id);
            child.meta_mut().parent_page_id = Some(rhs.page_id());
        }
        rhs.meta_mut().common.parent_page_id = internal_page.meta().common.parent_page_id;
        rhs.meta_mut().common.next_page_id = internal_page.meta().common.next_page_id;
        internal_page.meta_mut().common.next_page_id = Some(rhs.page_id());
        rhs
    }

    pub fn siblings_of(&self, page_id: PageID) -> Siblings {
        if Some(page_id) == self.meta().leftmost {
            let right = if self.len() > 0 {
                Some((self.key_at(0), self.page_id_at(0)))
            } else {
                None
            };
            return (None, right);
        }
        let len = self.len();
        let idx = (0..len)
            .into_iter()
            .find(|idx| self.page_id_at(*idx) == page_id)
            .unwrap();
        let left = if idx > 0 {
            Some((self.page_id_at(idx - 1), self.key_at(idx)))
        } else {
            self.meta()
                .leftmost
                .map(|leftmost_page_id| (leftmost_page_id, self.key_at(idx)))
        };
        let right = if idx + 1 < len {
            Some((self.key_at(idx + 1), self.page_id_at(idx + 1)))
        } else {
            None
        };
        (left, right)
    }

    pub fn remove(&mut self, key: &[Datum]) -> Result<(), IndexError> {
        let idx = match self.index_of(key) {
            -1 => Err(IndexError::KeyNotFound),
            idx => {
                let idx = idx as usize;
                if self.key_at(idx) != key {
                    Err(IndexError::KeyNotFound)
                } else {
                    Ok(idx)
                }
            }
        }?;
        let internal_page = self.internal_page_mut();
        internal_page.remove_at(idx)?;
        internal_page.move_forward(idx + 1)?;
        Ok(())
    }

    /// append to the end, the order should be preserved
    pub fn append(&mut self, key: &[Datum], page_id: PageID) -> Result<(), IndexError> {
        let schema = self.schema.clone();
        let internal_page = self.internal_page_mut();
        internal_page.append(&page_id, &Datum::to_bytes_with_schema(key, schema.as_ref()))?;
        Ok(())
    }

    /// random insert
    pub fn insert(&mut self, key: &[Datum], page_id: PageID) -> Result<(), IndexError> {
        let idx = (self.index_of(key) + 1) as usize;
        let schema = self.schema.clone();
        let internal_page = self.internal_page_mut();
        if internal_page.store_stat().1
            < Datum::to_bytes_with_schema(key, &schema).len() + std::mem::size_of::<PageID>() + 16
        {
            return Err(IndexError::NodeOutOfSpace);
        }
        internal_page.move_backward(idx)?;
        internal_page.insert_at(idx, &page_id, &Datum::to_bytes_with_schema(key, &schema))?;
        Ok(())
    }

    pub fn update_key_with(&mut self, key: &[Datum], new_key: &[Datum]) {
        let idx = self.index_of(key) as usize;
        let page_id = self.page_id_at(idx);
        self.remove(key).unwrap();
        self.insert(new_key, page_id).unwrap();
    }

    #[allow(dead_code)]
    pub fn sanity_check(&self) {
        let len = self.len();
        let mut last_key = None;
        for idx in 0..len - 1 {
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
    use crate::datum::{DataType, Datum};
    use crate::storage::BufferPoolManager;
    use crate::table::Schema;
    use std::fs::remove_file;
    use std::rc::Rc;

    #[test]
    fn test_insert_find_internal() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(10);
            let filename = bpm.borrow().filename();
            let key_schema = Rc::new(Schema::from_slice(&[(
                DataType::new_int(false),
                "v1".to_string(),
            )]));
            let dummy_page_id = 10;
            let mut node = InternalNode::new_as_root(
                bpm,
                key_schema,
                &[Datum::Int(Some(1))],
                dummy_page_id,
                dummy_page_id,
            );
            node.insert(&[Datum::Int(Some(2))], dummy_page_id + 1)
                .unwrap();
            node.insert(&[Datum::Int(Some(4))], dummy_page_id + 2)
                .unwrap();
            node.insert(&[Datum::Int(Some(8))], dummy_page_id + 3)
                .unwrap();
            assert_eq!(node.index_of(&[Datum::Int(Some(5))]), 2);
            assert_eq!(node.index_of(&[Datum::Int(Some(-5))]), -1);
            node.remove(&[Datum::Int(Some(4))]).unwrap();
            assert_eq!(node.key_at(2), [Datum::Int(Some(8))]);
            filename
        };
        remove_file(filename).unwrap()
    }
}
