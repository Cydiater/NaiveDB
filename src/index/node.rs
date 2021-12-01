use crate::datum::{DataType, Datum};
use crate::index::{IndexError, RecordID};
use crate::storage::{BufferPoolManagerRef, PageID};
use std::convert::TryInto;

///
/// | num_child | parent_page_id | page_id[0] | key[0] | page_id[1] | ... | page_id[n] |
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
fn datums_from_index_key(
    _bpm: BufferPoolManagerRef,
    data_types: &[DataType],
    bytes: &[u8],
    is_inlined: bool,
) -> Vec<Datum> {
    let mut datums = vec![];
    if is_inlined {
        let mut offset = 0usize;
        for data_type in data_types {
            let width = data_type.width_of_value().unwrap();
            let datum = Datum::from_bytes(data_type, bytes[offset..(offset + width)].to_vec());
            offset += width;
            datums.push(datum)
        }
        datums
    } else {
        todo!()
    }
}

#[allow(dead_code)]
pub struct InternalNode {
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    key_data_types: Vec<DataType>,
    key_size: usize,
    is_inlined: bool,
}

#[allow(dead_code)]
impl InternalNode {
    /// key_size: the maximum size of key, we can not infer key_size from combined of
    /// key_data_types and is_inlined because for variable data_type, it can be inlined
    /// so we don't know the actually maximum size.
    pub fn new_empty(
        bpm: BufferPoolManagerRef,
        key_data_types: Vec<DataType>,
        key_size: usize,
        is_inlined: bool,
    ) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        let page_id = page.borrow().page_id.unwrap();
        // set num_child as 1
        page.borrow_mut().buffer[0..4].copy_from_slice(&1u32.to_le_bytes());
        // set parent_page_id as none
        page.borrow_mut().buffer[4..8].copy_from_slice(&0u32.to_le_bytes());
        // set first child as none
        page.borrow_mut().buffer[8..12].copy_from_slice(&0u32.to_le_bytes());
        // mark dirty
        page.borrow_mut().is_dirty = true;
        // unpin
        bpm.borrow_mut().unpin(page_id).unwrap();
        Self {
            page_id,
            bpm,
            key_data_types,
            key_size,
            is_inlined,
        }
    }

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

    pub fn offset_of_nth_value(&self, idx: usize) -> usize {
        Self::header_size() + idx * (self.key_size + std::mem::size_of::<u32>())
    }

    pub fn offset_of_nth_key(&self, idx: usize) -> usize {
        self.offset_of_nth_value(idx) + std::mem::size_of::<u32>()
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

    pub fn datums_at(&self, idx: usize) -> Vec<Datum> {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let start = self.offset_of_nth_key(idx);
        let end = start + self.key_size;
        let bytes = &page.borrow().buffer[start..end];
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
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let page_id =
            u32::from_le_bytes(page.borrow().buffer[start..end].try_into().unwrap()) as usize;
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
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let idx = self.index_of(key);
        let start = self.offset_of_nth_value(idx);
        let end = self.end();
        let delta = self.key_size + std::mem::size_of::<u32>();
        page.borrow_mut()
            .buffer
            .copy_within(start..end, start + delta);
        let end = start + std::mem::size_of::<u32>();
        if end > start {
            page.borrow_mut().buffer[start..end].copy_from_slice(&(page_id as u32).to_le_bytes());
        }
        let start = end;
        let end = start + self.key_size;
        if self.is_inlined {
            let bytes =
                key.iter()
                    .zip(self.key_data_types.iter())
                    .fold(vec![], |mut bytes, (d, t)| {
                        bytes.extend_from_slice(d.clone().into_bytes(t).as_slice());
                        bytes
                    });
            page.borrow_mut().buffer[start..end].copy_from_slice(bytes.as_slice());
        } else {
            todo!()
        }
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
            let mut slice = Slice::new_empty(bpm.clone(), Rc::new(schema));
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
                InternalNode::new_empty(bpm.clone(), vec![(DataType::new_int(false))], 5, true);
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
