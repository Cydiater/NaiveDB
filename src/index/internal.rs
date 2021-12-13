use crate::datum::{DataType, Datum};
use crate::index::IndexError;
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::SchemaRef;
use std::convert::TryInto;
use std::ops::Range;

///
/// InternalNode Format:
///
///     | Meta | page_id[0] | offset[0] | ... | page_id[n - 1]    |
///                                       ... | data[1] | data[0] |
///
/// Meta Format:
///
///     | is_leaf | parent_page_id | next_page_id | head | tail |
///

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

impl InternalNode {
    const IS_LEAF: Range<usize> = 0..1;
    const PARENT_PAGE_ID: Range<usize> = 1..5;
    const NEXT_PAGE_ID: Range<usize> = 5..9;
    const HEAD: Range<usize> = 9..13;
    const TAIL: Range<usize> = 13..17;
    const SIZE_OF_META: usize = 17;

    pub fn new_root(
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
        key: &[Datum],
        page_id_lhs: PageID,
        page_id_rhs: PageID,
    ) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        {
            let buffer = &mut page.borrow_mut().buffer;
            // not leaf
            buffer[Self::IS_LEAF].copy_from_slice(&[0u8]);
            // no parent
            buffer[Self::PARENT_PAGE_ID].copy_from_slice(&[0u8; 4]);
            // set head
            buffer[Self::HEAD].copy_from_slice(&((Self::SIZE_OF_META + 12) as u32).to_le_bytes());
            // page_id
            buffer[Self::SIZE_OF_META..Self::SIZE_OF_META + 4]
                .copy_from_slice(&(page_id_lhs as u32).to_le_bytes());
            buffer[Self::SIZE_OF_META + 8..Self::SIZE_OF_META + 12]
                .copy_from_slice(&(page_id_rhs as u32).to_le_bytes());
            // key
            let bytes = Datum::to_bytes_with_schema(key, schema.clone());
            let end = PAGE_SIZE;
            let start = end - bytes.len();
            buffer[start..end].copy_from_slice(&bytes);
            buffer[Self::SIZE_OF_META + 4..Self::SIZE_OF_META + 8]
                .copy_from_slice(&(end as u32).to_le_bytes());
            // set tail
            buffer[Self::TAIL].copy_from_slice(&(start as u32).to_le_bytes());
            // set next_page_id
            buffer[Self::NEXT_PAGE_ID].copy_from_slice(&0u32.to_le_bytes());
        }
        page.borrow_mut().is_dirty = true;
        Self { page, bpm, schema }
    }

    pub fn get_next_page_id(&self) -> Option<usize> {
        let next_page_id = u32::from_le_bytes(
            self.page.borrow().buffer[Self::NEXT_PAGE_ID]
                .try_into()
                .unwrap(),
        ) as usize;
        if next_page_id == 0 {
            None
        } else {
            Some(next_page_id)
        }
    }

    pub fn set_next_page_id(&mut self, page_id: Option<PageID>) {
        let page_id = page_id.unwrap_or(0);
        self.page.borrow_mut().buffer[Self::NEXT_PAGE_ID]
            .copy_from_slice(&((page_id as u32).to_le_bytes()));
        self.page.borrow_mut().is_dirty = true;
    }

    pub fn new_single_child(
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
        page_id: Option<PageID>,
    ) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        {
            let buffer = &mut page.borrow_mut().buffer;
            // not leaf
            buffer[Self::IS_LEAF].copy_from_slice(&[0u8]);
            // no parent
            buffer[Self::PARENT_PAGE_ID].copy_from_slice(&[0u8; 4]);
            // set head
            buffer[Self::HEAD].copy_from_slice(&((Self::SIZE_OF_META + 4) as u32).to_le_bytes());
            // set tail
            buffer[Self::TAIL].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
            // page_id
            buffer[Self::SIZE_OF_META..Self::SIZE_OF_META + 4]
                .copy_from_slice(&(page_id.unwrap_or(0) as u32).to_le_bytes());
            // set next_page_id
            buffer[Self::NEXT_PAGE_ID].copy_from_slice(&0u32.to_le_bytes());
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
        if page.borrow().buffer[Self::IS_LEAF] != [0u8] {
            return Err(IndexError::NotInternalIndexNode);
        }
        Ok(Self { page, bpm, schema })
    }

    pub fn get_parent_page_id(&self) -> Option<PageID> {
        let parent_page_id = u32::from_le_bytes(
            self.page.borrow().buffer[Self::PARENT_PAGE_ID]
                .try_into()
                .unwrap(),
        ) as usize;
        if parent_page_id == 0 {
            None
        } else {
            Some(parent_page_id)
        }
    }

    pub fn get_page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }

    fn get_head(&self) -> usize {
        u32::from_le_bytes(self.page.borrow().buffer[Self::HEAD].try_into().unwrap()) as usize
    }

    fn get_tail(&self) -> usize {
        u32::from_le_bytes(self.page.borrow().buffer[Self::TAIL].try_into().unwrap()) as usize
    }

    fn set_head(&self, head: usize) {
        self.page.borrow_mut().buffer[Self::HEAD].copy_from_slice(&(head as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    fn set_tail(&self, tail: usize) {
        self.page.borrow_mut().buffer[Self::TAIL].copy_from_slice(&(tail as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    /// return the number of page_id
    pub fn len(&self) -> usize {
        let head = self.get_head();
        (head - Self::SIZE_OF_META + 4) / 8
    }

    pub fn set_parent_page_id(&self, page_id: Option<PageID>) {
        let page_id = page_id.unwrap_or(0);
        self.page.borrow_mut().buffer[Self::PARENT_PAGE_ID]
            .copy_from_slice(&((page_id as u32).to_le_bytes()));
        self.page.borrow_mut().is_dirty = true;
    }

    fn offset_at(&self, idx: usize) -> usize {
        let start = Self::SIZE_OF_META + idx * 8 + 4;
        let end = start + 4;
        let bytes: [u8; 4] = self.page.borrow().buffer[start..end].try_into().unwrap();
        u32::from_le_bytes(bytes) as usize
    }

    pub fn page_id_at(&self, idx: usize) -> Option<PageID> {
        let start = Self::SIZE_OF_META + idx * 8;
        let end = start + 4;
        let bytes: [u8; 4] = self.page.borrow().buffer[start..end].try_into().unwrap();
        match u32::from_le_bytes(bytes) as usize {
            0 => None,
            page_id => Some(page_id),
        }
    }

    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        assert!(idx < self.len() - 1);
        let base_offset = self.offset_at(idx);
        let bytes = &self.page.borrow().buffer[..base_offset];
        Datum::from_bytes_and_schema(self.schema.clone(), bytes)
    }

    /// return the index of child where this key belong
    pub fn index_of(&self, key: &[Datum]) -> usize {
        let num_child = self.len();
        // if we have only one child
        if num_child == 1 {
            return 0;
        }
        // the index here is about key
        let mut left = 0usize;
        let mut right = num_child as usize - 2;
        let mut mid;
        while left + 1 < right {
            mid = (left + right) / 2;
            if key < &self.key_at(mid) {
                right = mid;
            } else {
                left = mid;
            }
        }
        if key >= &self.key_at(right) {
            right + 1
        } else if key >= &self.key_at(left) {
            left + 1
        } else {
            0
        }
    }

    pub fn split(&mut self) -> Self {
        let len = self.len();
        let mut keys = vec![];
        let mut page_ids = vec![];
        // collect keys and page_ids
        for idx in 0..len - 1 {
            let key = self.key_at(idx);
            keys.push(key);
        }
        for idx in 0..len {
            let page_id = self.page_id_at(idx);
            page_ids.push(page_id);
        }
        // clear lhs
        self.set_head(Self::SIZE_OF_META + 4);
        self.set_tail(PAGE_SIZE);
        let len_lhs = (len - 1) / 2;
        self.set_left_most_page_id(page_ids.remove(0));
        for idx in 0..len_lhs {
            self.append(keys[idx].as_slice(), page_ids[idx].unwrap_or(0));
        }
        self.page.borrow_mut().is_dirty = true;
        // setup rhs
        let mut rhs = Self::new_single_child(self.bpm.clone(), self.schema.clone(), None);
        for idx in len_lhs..len - 1 {
            rhs.append(keys[idx].as_slice(), page_ids[idx].unwrap_or(0));
        }
        // set parent_page_id
        rhs.set_parent_page_id(self.get_parent_page_id());
        rhs.page.borrow_mut().is_dirty = true;
        // set next_page_id
        rhs.set_next_page_id(self.get_next_page_id());
        self.set_next_page_id(Some(rhs.get_page_id()));
        rhs
    }

    pub fn ok_to_insert(&self, datums: &[Datum]) -> bool {
        let bytes = Datum::to_bytes_with_schema(datums, self.schema.clone());
        let head = self.get_head();
        let tail = self.get_tail();
        head + 8 + bytes.len() <= tail
    }

    /// since you can't modify left-most page_id with insert
    pub fn set_left_most_page_id(&mut self, page_id: Option<PageID>) {
        self.page.borrow_mut().buffer[Self::SIZE_OF_META..Self::SIZE_OF_META + 4]
            .copy_from_slice(&(page_id.unwrap_or(0) as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    /// append to the end, the order should be preserved
    pub fn append(&mut self, key: &[Datum], page_id: PageID) {
        self.insert_at(self.len() - 1, key, page_id);
    }

    /// random insert
    pub fn insert(&mut self, key: &[Datum], page_id: PageID) {
        let idx = self.index_of(key);
        self.insert_at(idx, key, page_id);
    }

    pub fn insert_at(&mut self, idx: usize, key: &[Datum], page_id: PageID) {
        // | offset[idx - 1] | page_id[idx] | offset[idx] | ... | page_id[n - 1] |
        //                                start                                 end
        //                      =>
        // | offset[idx - 1] | page_id[idx] | new_offset | new_page_id | offset[idx]
        let start = Self::SIZE_OF_META + idx * 8 + 4;
        let end = Self::SIZE_OF_META + self.len() * 8 + 4;
        // move bytes at start..end 8 bytes backward
        self.page
            .borrow_mut()
            .buffer
            .copy_within(start..end, start + 8);
        // fill key
        let bytes = Datum::to_bytes_with_schema(key, self.schema.clone());
        let end = self.get_tail();
        let start = end - bytes.len();
        self.page.borrow_mut().buffer[start..end].copy_from_slice(&bytes);
        self.set_tail(start);
        let head = self.get_head();
        self.set_head(head + 8);
        // set offset
        let start = Self::SIZE_OF_META + idx * 8 + 4;
        self.page.borrow_mut().buffer[start..start + 4]
            .copy_from_slice(&(end as u32).to_le_bytes());
        // set page_id
        let start = Self::SIZE_OF_META + idx * 8 + 8;
        self.page.borrow_mut().buffer[start..start + 4]
            .copy_from_slice(&(page_id as u32).to_le_bytes());
        // mark dirty
        self.page.borrow_mut().is_dirty = true;
    }
}

#[allow(dead_code)]
pub struct LeafNode {
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
}

impl LeafNode {}

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
            let mut node = InternalNode::new_root(
                bpm,
                key_schema,
                &[Datum::Int(Some(1))],
                dummy_page_id,
                dummy_page_id,
            );
            node.insert(&[Datum::Int(Some(2))], dummy_page_id);
            node.insert(&[Datum::Int(Some(4))], dummy_page_id);
            node.insert(&[Datum::Int(Some(8))], dummy_page_id);
            assert_eq!(node.index_of(&[Datum::Int(Some(5))]), 3);
            assert_eq!(node.index_of(&[Datum::Int(Some(-5))]), 0);
            filename
        };
        remove_file(filename).unwrap()
    }
}
