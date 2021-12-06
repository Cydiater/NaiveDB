use crate::datum::Datum;
use crate::index::{IndexError, RecordID};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::SchemaRef;
use std::convert::TryInto;
use std::ops::Range;

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

#[allow(dead_code)]
#[derive(Clone)]
pub struct LeafNode {
    page: PageRef,
    bpm: BufferPoolManagerRef,
    schema: SchemaRef,
}

#[allow(dead_code)]
impl LeafNode {
    const IS_LEAF: Range<usize> = 0..1;
    const PARENT_PAGE_ID: Range<usize> = 1..5;
    const HEAD: Range<usize> = 5..9;
    const TAIL: Range<usize> = 9..13;
    const SIZE_OF_META: usize = 13;

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

    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        {
            let buffer = &mut page.borrow_mut().buffer;
            // set leaf
            buffer[Self::IS_LEAF].copy_from_slice(&[1u8]);
            // set parent_page_id as none
            buffer[Self::PARENT_PAGE_ID].copy_from_slice(&0u32.to_le_bytes());
            // set head = SIZE_OF_META
            buffer[Self::HEAD].copy_from_slice(&(Self::SIZE_OF_META as u32).to_le_bytes());
            // set tail = PAGE_SIZE
            buffer[Self::TAIL].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        }
        // mark dirty
        page.borrow_mut().is_dirty = true;
        Self { page, bpm, schema }
    }

    pub fn get_page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
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

    pub fn set_parent_page_id(&self, page_id: Option<PageID>) {
        let page_id = if let Some(page_id) = page_id {
            page_id
        } else {
            0
        };
        self.page.borrow_mut().buffer[Self::PARENT_PAGE_ID]
            .copy_from_slice(&(page_id.to_le_bytes()));
        self.page.borrow_mut().is_dirty = true;
    }

    pub fn open(
        bpm: BufferPoolManagerRef,
        schema: SchemaRef,
        page_id: PageID,
    ) -> Result<Self, IndexError> {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        if page.borrow().buffer[Self::IS_LEAF] == [1u8] {
            return Err(IndexError::NotLeafIndexNode);
        }
        Ok(Self { page, bpm, schema })
    }

    fn offset_at(&self, idx: usize) -> usize {
        let start = Self::SIZE_OF_META + idx * 12;
        let end = start + 4;
        let bytes: [u8; 4] = self.page.borrow().buffer[start..end].try_into().unwrap();
        u32::from_le_bytes(bytes) as usize
    }

    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        let base_offset = self.offset_at(idx);
        let bytes = &self.page.borrow().buffer[..base_offset];
        Datum::from_bytes_and_schema(self.schema.clone(), bytes)
    }

    pub fn record_id_at(&self, idx: usize) -> RecordID {
        let start = Self::SIZE_OF_META + idx * 12 + 4;
        let page_id = u32::from_le_bytes(
            self.page.borrow().buffer[start..start + 4]
                .try_into()
                .unwrap(),
        ) as PageID;
        let offset = u32::from_le_bytes(
            self.page.borrow().buffer[start + 4..start + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        (page_id, offset)
    }

    pub fn len(&self) -> usize {
        let head = self.get_head();
        (head - Self::SIZE_OF_META) / 12
    }

    /// find the first record with key greater than input
    pub fn lower_bound(&self, key: &[Datum]) -> Option<usize> {
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

    pub fn ok_to_insert(&self, datums: &[Datum]) -> bool {
        let bytes = Datum::to_bytes_with_schema(datums, self.schema.clone());
        let head = self.get_head();
        let tail = self.get_tail();
        head + 12 + bytes.len() <= tail
    }

    /// split current node into two node, the lhs node have the front half while the rhs node have
    /// the back half, and they have the same parent_id
    pub fn split(&mut self) -> Self {
        let len = self.len();
        // collect keys and record_ids
        let mut keys = vec![];
        let mut record_ids = vec![];
        for idx in 0..len {
            let key = self.key_at(idx);
            let record_id = self.record_id_at(idx);
            keys.push(key);
            record_ids.push(record_id);
        }
        // clear lhs
        self.set_head(Self::SIZE_OF_META);
        self.set_tail(PAGE_SIZE);
        let len_lhs = len / 2;
        // setup lhs node
        for idx in 0..len_lhs {
            self.append(keys[idx].as_slice(), record_ids[idx]);
        }
        // new rhs
        let rhs = LeafNode::new(self.bpm.clone(), self.schema.clone());
        for idx in len_lhs..len {
            self.append(keys[idx].as_slice(), record_ids[idx]);
        }
        // set parent_page_id
        rhs.set_parent_page_id(self.get_parent_page_id());
        rhs.page.borrow_mut().is_dirty = true;
        self.page.borrow_mut().is_dirty = true;
        rhs
    }

    /// append to the end, the order should be preserved
    pub fn append(&mut self, key: &[Datum], record_id: RecordID) {
        self.insert_at(self.len(), key, record_id);
    }

    /// random insert
    pub fn insert(&mut self, key: &[Datum], record_id: RecordID) {
        let idx = self.lower_bound(key).unwrap_or_else(|| self.len());
        self.insert_at(idx, key, record_id);
    }

    /// insert at specific position
    pub fn insert_at(&mut self, idx: usize, key: &[Datum], record_id: RecordID) {
        let start = Self::SIZE_OF_META + idx * 12;
        let end = Self::SIZE_OF_META + self.len() * 12;
        self.page
            .borrow_mut()
            .buffer
            .copy_within(start..end, start + 12);
        let bytes = Datum::to_bytes_with_schema(key, self.schema.clone());
        let end = self.get_tail();
        let start = end - bytes.len();
        self.page.borrow_mut().buffer[start..end].copy_from_slice(&bytes);
        self.set_tail(start);
        let head = self.get_head();
        self.set_head(head + 12);
        // set offset
        let start = Self::SIZE_OF_META + idx * 12;
        self.page.borrow_mut().buffer[start..start + 4]
            .copy_from_slice(&(end as u32).to_le_bytes());
        // set page_id
        let start = start + 4;
        self.page.borrow_mut().buffer[start..start + 4]
            .copy_from_slice(&(record_id.0 as u32).to_le_bytes());
        // set offset
        let start = start + 4;
        self.page.borrow_mut().buffer[start..start + 4]
            .copy_from_slice(&(record_id.1 as u32).to_le_bytes());
        // mark dirty
        self.page.borrow_mut().is_dirty = true;
    }
}
