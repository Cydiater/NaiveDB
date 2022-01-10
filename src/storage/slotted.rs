use crate::storage::PAGE_SIZE;
use itertools::Itertools;
use std::convert::TryInto;
use std::iter::Iterator;
use std::marker::PhantomData;
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

pub struct KeyDataIter<'page, Key> {
    idx_iter: SlotIndexIter<'page>,
    bytes: &'page [u8],
    _phantom: PhantomData<Key>,
}

impl<'page, Key> KeyDataIter<'page, Key> {
    pub fn new(idx_iter: SlotIndexIter<'page>, bytes: &'page [u8]) -> Self {
        Self {
            idx_iter,
            bytes,
            _phantom: PhantomData::<Key>,
        }
    }
}

impl<'page, Key: 'page + Sized> Iterator for KeyDataIter<'page, Key> {
    type Item = (&'page Key, &'page [u8]);

    fn next(&mut self) -> Option<(&'page Key, &'page [u8])> {
        if let Some(idx) = self.idx_iter.next() {
            let offset = idx * (size_of::<Key>() + 16);
            let start = usize::from_le_bytes(self.bytes[offset..offset + 8].try_into().unwrap());
            let end = usize::from_le_bytes(self.bytes[offset + 8..offset + 16].try_into().unwrap());
            unsafe {
                Some((
                    &*(self.bytes.as_ptr().add(offset + 16) as *const Key),
                    &self.bytes[start..end],
                ))
            }
        } else {
            None
        }
    }
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
    pub fn capacity(&self) -> usize {
        self.head / (size_of::<Key>() + 16)
    }
    pub fn store_stat(&self) -> (usize, usize) {
        let cap = self.bytes.len();
        let using = self.head + (cap - self.tail);
        (using, cap - using)
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
    pub fn meta(&self) -> &Meta {
        &self.meta
    }
    pub fn meta_mut(&mut self) -> &mut Meta {
        &mut self.meta
    }
    pub fn idx_iter(&self) -> SlotIndexIter {
        SlotIndexIter::new(&self.bitmap, self.capacity())
    }
    pub fn key_data_iter(&self) -> KeyDataIter<Key> {
        KeyDataIter::new(self.idx_iter(), &self.bytes)
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
    pub fn append(&mut self, key: &Key, data: &[u8]) -> Result<(), SlottedPageError> {
        let idx = self.capacity();
        if self.head + size_of::<Key>() + 16 > self.tail - data.len() {
            return Err(SlottedPageError::OutOfSpace);
        }
        self.head += size_of::<Key>() + 16;
        let rng = self.data_range_mut_ptr_at(idx);
        unsafe {
            *rng.0 = 0;
            *rng.1 = 0;
        }
        self.insert_at(idx, key, data)?;
        Ok(())
    }
    pub fn insert(&mut self, key: &Key, data: &[u8]) -> Result<usize, SlottedPageError> {
        let idx = self.find_first_empty_slot();
        if idx * (size_of::<Key>() + 16) >= self.head {
            self.head += size_of::<Key>() + 16;
            if self.head + data.len() > self.tail {
                return Err(SlottedPageError::OutOfSpace);
            }
        }
        self.insert_at(idx, key, data)?;
        Ok(idx)
    }
    pub fn move_backward(&mut self, start: usize) -> Result<(), SlottedPageError> {
        if self.head + size_of::<Key>() + 16 > self.tail {
            return Err(SlottedPageError::OutOfSpace);
        }
        let cap = self.capacity();
        for idx in (start + 1..cap + 1).rev() {
            {
                let last_range = self
                    .data_range_at(idx - 1)
                    .ok_or(SlottedPageError::SlotNotFound)?;
                let current_range_mut_ptr = self.data_range_mut_ptr_at(idx);
                unsafe {
                    *current_range_mut_ptr.0 = last_range.0;
                    *current_range_mut_ptr.1 = last_range.1;
                }
            }
            {
                let last_key = *self.key_at(idx - 1);
                let current_key_mut_ptr = self.key_mut_ptr_at(idx);
                unsafe {
                    *current_key_mut_ptr = last_key;
                }
            }
        }
        let range_mut_ptr = self.data_range_mut_ptr_at(start);
        unsafe {
            *range_mut_ptr.0 = 0;
            *range_mut_ptr.1 = 0;
        }
        self.head += size_of::<Key>() + 16;
        self.bitmap[start / 8] ^= 1 << (start % 8);
        let end = self.capacity() - 1;
        self.bitmap[end / 8] ^= 1 << (end % 8);
        Ok(())
    }
    pub fn move_forward(&mut self, start: usize) -> Result<(), SlottedPageError> {
        if self.data_range_at(start - 1).is_some() {
            return Err(SlottedPageError::InsertAtUsingSlot);
        }
        let cap = self.capacity();
        for idx in start - 1..cap - 1 {
            {
                let next_range = self
                    .data_range_at(idx + 1)
                    .ok_or(SlottedPageError::SlotNotFound)?;
                let current_range_mut_ptr = self.data_range_mut_ptr_at(idx);
                unsafe {
                    *current_range_mut_ptr.0 = next_range.0;
                    *current_range_mut_ptr.1 = next_range.1;
                }
            }
            {
                let next_key = *self.key_at(idx + 1);
                let current_key = self.key_mut_ptr_at(idx);
                unsafe {
                    *current_key = next_key;
                }
            }
        }
        self.head -= size_of::<Key>() + 16;
        let pos = start - 1;
        self.bitmap[pos / 8] ^= 1 << (pos % 8);
        let pos = self.capacity();
        self.bitmap[pos / 8] ^= 1 << (pos % 8);
        Ok(())
    }
    pub fn count(&self) -> usize {
        let mut cnt = 0;
        for byte in self.bitmap {
            cnt += byte.count_ones() as usize;
        }
        cnt
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
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use std::collections::HashMap;

    #[allow(dead_code)]
    #[derive(Clone, Copy)]
    struct Meta {
        next_page_id: Option<PageID>,
    }
    #[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
    struct Key {
        pub page_id: PageID,
    }

    #[test]
    fn basic() {
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
        slotted.move_backward(0).unwrap();
        assert_eq!(slotted.capacity(), 4);
        assert_eq!(slotted.key_at(1), &Key { page_id: 0 });
        slotted.move_forward(1).unwrap();
        assert_eq!(slotted.capacity(), 3);
        assert_eq!(slotted.key_at(1), &Key { page_id: 3 });
        slotted.move_backward(1).unwrap();
    }

    #[test]
    fn chaos() {
        let mut bytes = [0u8; PAGE_SIZE];
        let slotted = unsafe { &mut *(bytes.as_mut_ptr() as *mut SlottedPage<Meta, Key>) };
        slotted.reset(&Meta { next_page_id: None });
        let mut set: HashMap<PageID, String> = HashMap::new();
        let mut rng = rand::thread_rng();
        for _ in 0..100000 {
            let key = rng.gen::<usize>() % 300;
            let len = rng.gen::<usize>() % 8 + 1;
            let value: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect();
            if let std::collections::hash_map::Entry::Vacant(e) = set.entry(key) {
                if slotted
                    .insert(&Key { page_id: key }, value.as_bytes())
                    .is_ok()
                {
                    e.insert(value);
                }
            } else {
                slotted.remove(&Key { page_id: key }).unwrap();
                set.remove(&key);
            }
        }
        let key_data_from_set = set.into_iter().sorted().collect_vec();
        let key_data_from_slotted_page = slotted
            .key_data_iter()
            .map(|(key, data)| (key.page_id, String::from_utf8(data.to_vec()).unwrap()))
            .sorted()
            .collect_vec();
        assert_eq!(key_data_from_set, key_data_from_slotted_page)
    }
}
