use crate::storage::PAGE_SIZE;
use itertools::Itertools;
use std::iter::Iterator;
use std::mem::{size_of, transmute};
use thiserror::Error;

///
/// SlottedPage Format:
///     
///     Meta | head | tail | Payload
///
/// Payload Format
///
///     | Slot[0] | Slot[1] | Slot[2] | .....
///
///     ..... | Data[2] | Data[1] | Data[0] |
///
/// Slot Format
///
///    | offset_start | offset_end | Key |
///

#[allow(dead_code)]
pub struct SlottedPage<Meta: Sized, Key: Sized>
where
    [(); PAGE_SIZE - size_of::<Meta>() - 48]:,
{
    meta: Meta,
    head: usize,
    tail: usize,
    bitmap: [u8; 32],
    bytes: [u8; PAGE_SIZE - size_of::<Meta>() - 48],
}

pub struct SlotIndexIter<'page> {
    idx: usize,
    bitmap: &'page [u8; 32],
    capacity: usize,
}

impl<'page> SlotIndexIter<'page> {
    pub fn new(bitmap: &'page [u8; 32], capacity: usize) -> Self {
        Self {
            idx: 0,
            bitmap,
            capacity,
        }
    }
}

impl<'page> Iterator for SlotIndexIter<'page> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.capacity {
            None
        } else {
            let byte_pos = self.idx / 8;
            let bit_pos = self.idx % 8;
            if self.bitmap[byte_pos] >> bit_pos == 0 {
                self.idx = (byte_pos + 1) * 8;
                self.next()
            } else if ((self.bitmap[byte_pos] >> bit_pos) & 1) == 1 {
                let idx = self.idx;
                self.idx += 1;
                Some(idx)
            } else {
                self.idx += 1;
                self.next()
            }
        }
    }
}

#[allow(dead_code)]
impl<Meta: Sized + Copy, Key: Sized + Copy + PartialEq> SlottedPage<Meta, Key>
where
    [(); PAGE_SIZE - size_of::<Meta>() - 48]:,
{
    fn capacity(&self) -> usize {
        self.head / (size_of::<Key>() + 16)
    }
    fn data_range_at(&self, idx: usize) -> Option<(usize, usize)> {
        let data_range_ptr = self.data_range_ptr_at(idx);
        unsafe {
            let start = *data_range_ptr.0;
            let end = *data_range_ptr.1;
            if start == end && end == 0 {
                None
            } else {
                Some((start, end))
            }
        }
    }
    fn data_range_ptr_at(&self, idx: usize) -> (*const usize, *const usize) {
        let offset = idx * (size_of::<Key>() + 16);
        unsafe {
            (
                transmute::<*const u8, *const usize>(self.bytes.as_ptr().add(offset)),
                transmute::<*const u8, *const usize>(self.bytes.as_ptr().add(offset + 8)),
            )
        }
    }
    fn data_range_mut_ptr_at(&mut self, idx: usize) -> (*mut usize, *mut usize) {
        let offset = idx * (size_of::<Key>() + 16);
        unsafe {
            (
                transmute::<*mut u8, *mut usize>(self.bytes.as_mut_ptr().add(offset)),
                transmute::<*mut u8, *mut usize>(self.bytes.as_mut_ptr().add(offset + 8)),
            )
        }
    }
    fn push_data(&mut self, data: &[u8]) -> Result<(usize, usize), SlottedPageError> {
        let start = self.tail - data.len();
        if start <= self.head {
            return Err(SlottedPageError::OutOfSpace);
        }
        let end = self.tail;
        self.tail = start;
        self.bytes[start..end].copy_from_slice(data);
        Ok((start, end))
    }
    fn find_first_empty_slot(&self) -> usize {
        for idx in 0..32 {
            if self.bitmap[idx] != 255 {
                for bit_idx in 0..8 {
                    if ((self.bitmap[idx] >> bit_idx) & 1) == 0 {
                        return idx * 8 + bit_idx;
                    }
                }
            }
        }
        self.head / (size_of::<Key>() + 16) + 1
    }
    pub fn reset(&mut self, meta: &Meta) {
        self.head = 0;
        self.tail = self.bytes.len();
        self.meta = *meta;
        self.bitmap.fill(0);
        self.bytes.fill(0);
    }
    fn key_ptr_at(&self, idx: usize) -> *const Key {
        let offset = idx * (size_of::<Key>() + 16);
        unsafe { transmute::<*const u8, *const Key>(self.bytes.as_ptr().add(offset + 16)) }
    }
    fn key_mut_ptr_at(&mut self, idx: usize) -> *mut Key {
        let offset = idx * (size_of::<Key>() + 16);
        unsafe { transmute::<*mut u8, *mut Key>(self.bytes.as_mut_ptr().add(offset + 16)) }
    }
    fn idx_iter(&self) -> SlotIndexIter {
        SlotIndexIter::new(&self.bitmap, self.capacity())
    }
    pub fn key_at(&self, idx: usize) -> &Key {
        unsafe { &*self.key_ptr_at(idx) }
    }
    pub fn data_at(&self, idx: usize) -> &[u8] {
        let data_range = self.data_range_ptr_at(idx);
        unsafe {
            std::slice::from_raw_parts(
                self.bytes.as_ptr().add(*data_range.0),
                *data_range.1 - *data_range.0,
            )
        }
    }
    pub fn index_of(&self, key: &Key) -> Option<usize> {
        self.idx_iter()
            .collect_vec()
            .into_iter()
            .find(|idx| self.key_at(*idx) == key)
    }
    pub fn insert_at(
        &mut self,
        idx: usize,
        key: &Key,
        data: &[u8],
    ) -> Result<(), SlottedPageError> {
        let (start, end) = self.push_data(data)?;
        let key_ptr = self.key_mut_ptr_at(idx);
        let data_range_ptr = self.data_range_mut_ptr_at(idx);
        unsafe {
            if *data_range_ptr.0 != 0 {
                return Err(SlottedPageError::InsertAtUsingSlot);
            }
            *key_ptr = *key;
            *data_range_ptr.0 = start;
            *data_range_ptr.1 = end;
        }
        self.bitmap[idx / 8] |= 1 << (idx % 8);
        Ok(())
    }
    pub fn remove_at(&mut self, idx: usize) -> Result<(), SlottedPageError> {
        let data_range = self
            .data_range_at(idx)
            .ok_or(SlottedPageError::SlotNotFound)?;
        let start = self.tail;
        let end = data_range.0;
        self.tail += data_range.1 - data_range.0;
        self.bytes.copy_within(start..end, self.tail);
        let data_range_mut_ptr = self.data_range_mut_ptr_at(idx);
        unsafe {
            *data_range_mut_ptr.0 = 0;
            *data_range_mut_ptr.1 = 0;
            self.bitmap[idx / 8] ^= 1 << (idx % 8);
            self.idx_iter().collect_vec().into_iter().for_each(|idx| {
                let data_range_mut_ptr = self.data_range_mut_ptr_at(idx);
                if *data_range_mut_ptr.0 < end {
                    *data_range_mut_ptr.0 += data_range.1 - data_range.0;
                    *data_range_mut_ptr.1 += data_range.1 - data_range.0;
                }
            })
        }
        Ok(())
    }
    pub fn remove(&mut self, key: &Key) -> Result<(), SlottedPageError> {
        let idx = self.index_of(key).ok_or(SlottedPageError::KeyNotFound)?;
        self.remove_at(idx)
    }
    pub fn insert(&mut self, key: &Key, data: &[u8]) -> Result<(), SlottedPageError> {
        let idx = self.find_first_empty_slot();
        if idx * (size_of::<Key>() + 16) >= self.head {
            self.head += size_of::<Key>() + 16;
            if self.head > self.tail {
                return Err(SlottedPageError::OutOfSpace);
            }
        }
        self.insert_at(idx, key, data)
    }
}

#[derive(Error, Debug)]
pub enum SlottedPageError {
    #[error("Out Of Space")]
    OutOfSpace,
    #[error("Insert At Using Slot")]
    InsertAtUsingSlot,
    #[error("Slot Not Found")]
    SlotNotFound,
    #[error("Key Not Found")]
    KeyNotFound,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PageID;

    #[test]
    fn basic() {
        #[allow(dead_code)]
        #[derive(Clone, Copy)]
        struct Meta {
            next_page_id: Option<PageID>,
        }
        #[derive(Clone, Copy, PartialEq, Debug)]
        struct Key {
            pub page_id: PageID,
        }
        let mut bytes = [0u8; PAGE_SIZE];
        let slotted = unsafe { &mut *(bytes.as_mut_ptr() as *mut SlottedPage<Meta, Key>) };
        slotted.reset(&Meta { next_page_id: None });
        slotted
            .insert(&Key { page_id: 0 }, &[1u8, 2u8, 3u8])
            .unwrap();
        slotted
            .insert(&Key { page_id: 1 }, &[1u8, 2u8, 3u8])
            .unwrap();
        slotted
            .insert(&Key { page_id: 2 }, &[1u8, 2u8, 3u8])
            .unwrap();
        assert_eq!(slotted.key_at(2), &Key { page_id: 2 });
        assert_eq!(slotted.data_at(2), &[1u8, 2u8, 3u8]);
        slotted.remove(&Key { page_id: 1 }).unwrap();
        slotted
            .insert(&Key { page_id: 3 }, &[1u8, 2u8, 3u8])
            .unwrap();
        assert_eq!(slotted.key_at(1), &Key { page_id: 3 });
    }
}
