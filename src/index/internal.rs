use crate::datum::{DataType, Datum};
use crate::index::{
    utils::{datums_from_index_key, index_key_from_datums},
    IndexError, RecordID,
};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef};
use std::convert::TryInto;
use std::ops::Range;

///
///
/// InternalNode Format:
///
///     | Meta | page_id[0] | key[0] | page_id[1] | ... | page_id[n] |
///
/// Meta Format:
///
///     | is_leaf | num_child | parent_page_id |
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

impl Drop for InternalNode {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

#[allow(dead_code)]
pub struct InternalNode {
    page: PageRef,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
    key_size: usize,
    is_inlined: bool,
}

#[allow(dead_code)]
impl InternalNode {
    const IS_LEAF: Range<usize> = 0..1;
    const NUM_CHILD: Range<usize> = 1..5;
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
            // set num_child as 1
            buffer[Self::NUM_CHILD].copy_from_slice(&1u32.to_le_bytes());
            // set parent_page_id as none
            buffer[Self::PARENT_PAGE_ID].copy_from_slice(&0u32.to_le_bytes());
            // set first child as none
            buffer[8..12].copy_from_slice(&0u32.to_le_bytes());
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
        if page.borrow().buffer[Self::IS_LEAF] == [0u8] {
            return Err(IndexError::NotInternalIndexNode);
        }
        Ok(Self {
            page,
            bpm,
            key_data_types,
            key_size,
            is_inlined,
        })
    }

    fn get_num_child(&self) -> usize {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::NUM_CHILD]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_num_child(&self, num_child: usize) {
        self.page.borrow_mut().buffer[Self::NUM_CHILD]
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
        let num_child = self.get_num_child();
        self.offset_of_nth_value(num_child) + std::mem::size_of::<u32>()
    }

    pub fn offset_of_nth_value(&self, idx: usize) -> usize {
        Self::SIZE_OF_META + idx * (self.key_size + std::mem::size_of::<u32>())
    }

    pub fn offset_of_nth_key(&self, idx: usize) -> usize {
        self.offset_of_nth_value(idx) + std::mem::size_of::<u32>()
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

    pub fn value_at(&self, idx: usize) -> Option<PageID> {
        let start = self.offset_of_nth_value(idx);
        let end = start + std::mem::size_of::<u32>();
        let page_id =
            u32::from_le_bytes(self.page.borrow().buffer[start..end].try_into().unwrap()) as usize;
        if page_id == 0 {
            None
        } else {
            Some(page_id)
        }
    }

    /// return the index of child where this key belong
    pub fn index_of(&self, key: &[Datum]) -> usize {
        let num_child = self.get_num_child();
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
            if key < self.datums_at(mid).as_slice() {
                right = mid;
            } else {
                left = mid;
            }
        }
        if key >= self.datums_at(right).as_slice() {
            right + 1
        } else if key >= self.datums_at(left).as_slice() {
            left + 1
        } else {
            0
        }
    }

    pub fn insert(
        &mut self,
        key: &[Datum],
        _rid: &RecordID,
        page_id: PageID,
    ) -> Result<(), IndexError> {
        let idx = self.index_of(key);
        let start = self.offset_of_nth_value(idx);
        let end = self.end();
        let delta = self.key_size + std::mem::size_of::<u32>();
        self.page
            .borrow_mut()
            .buffer
            .copy_within(start..end, start + delta);
        let end = start + std::mem::size_of::<u32>();
        if end > start {
            self.page.borrow_mut().buffer[start..end]
                .copy_from_slice(&(page_id as u32).to_le_bytes());
        }
        let start = end;
        let end = start + self.key_size;
        let bytes = index_key_from_datums(
            self.bpm.clone(),
            self.key_data_types.as_slice(),
            key,
            self.is_inlined,
        );
        self.page.borrow_mut().buffer[start..end].copy_from_slice(bytes.as_slice());
        let num_child = self.get_num_child();
        self.set_num_child(num_child + 1);
        self.page.borrow_mut().is_dirty = true;
        Ok(())
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
    use crate::table::{Schema, Slice};
    use itertools::Itertools;
    use std::fs::remove_file;
    use std::rc::Rc;

    #[test]
    fn test_insert_find_internal() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(10);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_varchar(false), "v2".to_string()),
            ]);
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            // insert (1, 'foo'), (2, 'bar'), (3, 'hello'), (4, 'world')
            slice
                .add(vec![
                    Datum::Int(Some(1)),
                    Datum::VarChar(Some("foo".to_string())),
                ])
                .unwrap();
            slice
                .add(vec![
                    Datum::Int(Some(2)),
                    Datum::VarChar(Some("bar".to_string())),
                ])
                .unwrap();
            slice
                .add(vec![
                    Datum::Int(Some(4)),
                    Datum::VarChar(Some("hello".to_string())),
                ])
                .unwrap();
            slice
                .add(vec![
                    Datum::Int(Some(8)),
                    Datum::VarChar(Some("world".to_string())),
                ])
                .unwrap();
            let rids = (0..4)
                .into_iter()
                .map(|idx| slice.record_id_at(idx))
                .collect_vec();
            let mut node =
                InternalNode::new(bpm.clone(), vec![(DataType::new_int(false))], 5, true);
            let dummy_page_id = 10;
            node.insert(&[Datum::Int(Some(1))], &rids[0], dummy_page_id)
                .unwrap();
            node.insert(&[Datum::Int(Some(2))], &rids[1], dummy_page_id)
                .unwrap();
            node.insert(&[Datum::Int(Some(4))], &rids[2], dummy_page_id)
                .unwrap();
            node.insert(&[Datum::Int(Some(8))], &rids[3], dummy_page_id)
                .unwrap();
            assert_eq!(node.index_of(&[Datum::Int(Some(5))]), 3);
            assert_eq!(node.index_of(&[Datum::Int(Some(-5))]), 0);
            filename
        };
        remove_file(filename).unwrap()
    }
}
