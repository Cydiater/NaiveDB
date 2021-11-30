use crate::index::IndexError;
use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::{DataType, Datum};
use std::convert::TryInto;

///
/// | num_child | parent_page_id | page_id[0] | key_binary[0] | page_id[1] | ... | page_id[n] |
///

fn index_key_from_binary(
    bpm: BufferPoolManagerRef,
    data_types: &[DataType],
    bytes: &[u8],
) -> Vec<Datum> {
    let mut offset = 0;
    let mut datums = vec![];
    for data_type in data_types {
        let width = data_type.size_as_index_key();
        offset += data_type.size_as_index_key();
        datums.push(Datum::from_index_key_binary(
            bpm.clone(),
            *data_type,
            bytes[offset..(offset + width)].to_vec(),
        ));
    }
    datums
}

#[allow(dead_code)]
pub struct InternalNode {
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
    key_size: usize,
}

#[allow(dead_code)]
impl InternalNode {
    fn header_size() -> usize {
        // num_child
        std::mem::size_of::<u32>()
        // parent_page_id
       + std::mem::size_of::<u32>()
    }

    fn get_num_child(&self) -> usize {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let page_id = page.borrow().page_id.unwrap();
        let num_child = u32::from_le_bytes(page.borrow().buffer[0..4].try_into().unwrap());
        self.bpm.borrow_mut().unpin(page_id).unwrap();
        num_child as usize
    }

    fn set_num_child(&self, num_child: usize) {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let page_id = page.borrow().page_id.unwrap();
        page.borrow_mut().buffer[0..4].copy_from_slice(&(num_child as u32).to_le_bytes());
        page.borrow_mut().is_dirty = true;
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }

    fn end(&self) -> usize {
        let num_child = self.get_num_child();
        self.offset_of_nth_value(num_child) + std::mem::size_of::<u32>()
    }

    pub fn offset_of_nth_key(&self, idx: usize) -> usize {
        Self::header_size() + (idx - 1) * (self.key_size + std::mem::size_of::<u32>())
    }

    pub fn offset_of_nth_value(&self, idx: usize) -> usize {
        self.offset_of_nth_key(idx) + std::mem::size_of::<u32>()
    }

    pub fn get_parent_page_id(&self) -> Option<PageID> {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let page_id = u32::from_le_bytes(page.borrow().buffer[4..8].try_into().unwrap()) as usize;
        self.bpm.borrow_mut().unpin(self.page_id).unwrap();
        if page_id == 0 {
            None
        } else {
            Some(page_id)
        }
    }

    pub fn set_parent_page_id(&self, page_id: Option<PageID>) {
        let page_id = page_id.unwrap_or(0);
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        page.borrow_mut().buffer[4..8].copy_from_slice(&page_id.to_le_bytes());
        page.borrow_mut().is_dirty = true;
        self.bpm.borrow_mut().unpin(self.page_id).unwrap();
    }

    pub fn key_at(&self, idx: usize) -> Vec<Datum> {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let start = self.offset_of_nth_key(idx);
        let end = start + self.key_size;
        let bytes = &page.borrow().buffer[start..end];
        index_key_from_binary(self.bpm.clone(), &self.key_data_types, bytes)
    }

    pub fn value_at(&self, idx: usize) -> PageID {
        let start = self.offset_of_nth_value(idx);
        let end = start + std::mem::size_of::<u32>();
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let page_id =
            u32::from_le_bytes(page.borrow().buffer[start..end].try_into().unwrap()) as usize;
        page_id
    }

    pub fn index_of(&self, key: &[Datum]) -> usize {
        let num_child = self.get_num_child();
        let mut left = 0usize;
        let mut right = num_child as usize;
        let mut mid;
        while left + 1 < right {
            mid = (left + right) / 2;
            if key < self.key_at(mid).as_slice() {
                right = mid;
            } else {
                left = mid;
            }
        }
        if key >= self.key_at(right).as_slice() {
            right
        } else if key >= self.key_at(left).as_slice() {
            left
        } else {
            0
        }
    }

    pub fn insert(
        &mut self,
        key: &[Datum],
        key_in_bytes: &[u8],
        page_id: PageID,
    ) -> Result<(), IndexError> {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let idx = self.index_of(key);
        let start = self.offset_of_nth_value(idx);
        let end = self.end();
        let delta = self.key_size + std::mem::size_of::<u32>();
        page.borrow_mut()
            .buffer
            .copy_within(start..end, start + delta);
        let end = start + std::mem::size_of::<u32>();
        page.borrow_mut().buffer[start..end].copy_from_slice(&(page_id as u32).to_le_bytes());
        let start = end;
        let end = start + self.key_size;
        page.borrow_mut().buffer[start..end].copy_from_slice(key_in_bytes);
        let num_child = self.get_num_child();
        self.set_num_child(num_child + 1);
        page.borrow_mut().is_dirty = true;
        self.bpm.borrow_mut().unpin(self.page_id).unwrap();
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
