use crate::datum::{DataType, Datum};
use crate::index::{
    utils::{datums_from_index_key, index_key_from_datums},
    IndexError, RecordID,
};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef};
use std::convert::TryInto;
use std::mem::size_of;
use std::ops::Range;

///
///
/// LeafNode Format:
///
///     | Meta | key[0] | rid[0] | ... | key[n - 1] | rid[n - 1] |
///
/// Meta Format:
///
///     | is_leaf | num_record | parent_page_id |
///
/// the value of page_id must larger than 0, if the field is 0, then we see this page_id as none,
/// when we insert new page_id, we must check that the value of page_id is not none
///
/// Note that the width of key is fixed
///
/// generate key datums used for index, there should be two
/// situations
///
///  - inlined: where total serialized byte length of key datums is less than 256 bytes,
///  we can just put the original byte representation of the datums.
///
///  - non-inlined: where the total length exceed 256 bytes, we just put the rid of tuple
///  and fetch the datums from buffer.
///
/// format of key binary:
///
///  - inlined: | data1 | data2 | ...
///
///  - non-inlined: | page_id | offset |
///
/// for data field of inlined case, we can simply to to_bytes func in datum, and assume they are
/// always not null value.
///

impl Drop for LeafNode {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

#[allow(dead_code)]
pub struct LeafNode {
    page: PageRef,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
    key_size: usize,
    is_inlined: bool,
}

#[allow(dead_code)]
impl LeafNode {
    const IS_LEAF: Range<usize> = 0..1;
    const NUM_RECORD: Range<usize> = 1..5;
    const PARENT_PAGE_ID: Range<usize> = 5..9;
    const SIZE_OF_META: usize = 9;

    /// key_size: the maximum size of key, we can not infer key_size from combined of
    /// key_data_types and is_inlined because for variable data_type, it can be inlined
    /// so we don't know the actually maximum size.
    pub fn new(
        bpm: BufferPoolManagerRef,
        key_data_types: Vec<DataType>,
        key_size: usize,
        is_inlined: bool,
    ) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        {
            let buffer = &mut page.borrow_mut().buffer;
            // set not leaf
            buffer[Self::IS_LEAF].copy_from_slice(&[0u8]);
            // set num_child as 0
            buffer[Self::NUM_RECORD].copy_from_slice(&0u32.to_le_bytes());
            // set parent_page_id as none
            buffer[Self::PARENT_PAGE_ID].copy_from_slice(&0u32.to_le_bytes());
        }
        // mark dirty
        page.borrow_mut().is_dirty = true;
        Self {
            page,
            bpm,
            key_data_types,
            key_size,
            is_inlined,
        }
    }

    pub fn open(
        bpm: BufferPoolManagerRef,
        key_data_types: Vec<DataType>,
        key_size: usize,
        is_inlined: bool,
        page_id: PageID,
    ) -> Result<Self, IndexError> {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        if page.borrow().buffer[Self::IS_LEAF] == [1u8] {
            return Err(IndexError::NotLeafIndexNode);
        }
        Ok(Self {
            page,
            bpm,
            key_data_types,
            key_size,
            is_inlined,
        })
    }

    fn get_num_record(&self) -> usize {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::NUM_RECORD]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_num_record(&self, num_child: usize) {
        self.page.borrow_mut().buffer[Self::NUM_RECORD]
            .copy_from_slice(&(num_child as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
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

    fn end(&self) -> usize {
        let num_child = self.get_num_record();
        self.offset_of_nth_value(num_child) + size_of::<RecordID>()
    }

    pub fn offset_of_nth_key(&self, idx: usize) -> usize {
        Self::SIZE_OF_META + idx * (self.key_size + size_of::<RecordID>())
    }

    pub fn offset_of_nth_value(&self, idx: usize) -> usize {
        self.offset_of_nth_key(idx) + self.key_size
    }

    pub fn datums_at(&self, idx: usize) -> Vec<Datum> {
        let start = self.offset_of_nth_key(idx);
        let end = start + self.key_size;
        let bytes = &self.page.borrow().buffer[start..end];
        datums_from_index_key(
            self.bpm.clone(),
            &self.key_data_types,
            bytes,
            self.is_inlined,
        )
    }

    pub fn value_at(&self, idx: usize) -> RecordID {
        let start = self.offset_of_nth_value(idx);
        let page_id = u32::from_le_bytes(
            self.page.borrow().buffer[start..start + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let offset = u32::from_le_bytes(
            self.page.borrow().buffer[start + 4..start + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        (page_id, offset)
    }

    /// find the first record with key greater than input
    pub fn lower_bound(&self, key: &[Datum]) -> Option<usize> {
        let num_record = self.get_num_record();
        let mut left = 0;
        let mut right = num_record - 1;
        let mut mid;
        while left + 1 < right {
            mid = (left + right) / 2;
            if self.datums_at(mid).as_slice() < key {
                left = mid;
            } else {
                right = mid;
            }
        }
        if self.datums_at(left).as_slice() >= key {
            Some(left)
        } else if self.datums_at(right).as_slice() >= key {
            Some(right)
        } else {
            None
        }
    }

    pub fn index_of(&self, key: &[Datum]) -> Option<usize> {
        let lower_bound_idx = self.lower_bound(key);
        if let Some(idx) = lower_bound_idx {
            if self.datums_at(idx) == key {
                Some(idx)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn insert(
        &mut self,
        key: &[Datum],
        _rid: &RecordID,
        record_id: RecordID,
    ) -> Result<(), IndexError> {
        let num_record = self.get_num_record();
        let lower_bound_idx = self.lower_bound(key).unwrap_or(num_record);
        let start = self.offset_of_nth_key(lower_bound_idx);
        let end = self.end();
        self.page
            .borrow_mut()
            .buffer
            .copy_within(start..end, start + self.key_size + size_of::<RecordID>());
        let end = start + self.key_size + size_of::<RecordID>();
        let mut bytes = index_key_from_datums(
            self.bpm.clone(),
            self.key_data_types.as_slice(),
            key,
            self.is_inlined,
        );
        bytes.extend_from_slice(&(record_id.0 as u32).to_le_bytes());
        bytes.extend_from_slice(&(record_id.1 as u32).to_le_bytes());
        self.page.borrow_mut().buffer[start..end].copy_from_slice(bytes.as_slice());
        let num_record = self.get_num_record();
        self.set_num_record(num_record + 1);
        self.page.borrow_mut().is_dirty = true;
        Ok(())
    }
}