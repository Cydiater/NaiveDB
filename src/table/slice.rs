use crate::datum::{DataType, Datum};
use crate::index::RecordID;
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::{Schema, SchemaRef, TableError};
use itertools::Itertools;
use prettytable::{Cell, Row, Table};
use std::convert::TryInto;
use std::fmt;
use std::ops::Range;
use std::rc::Rc;

///
/// Slice Format:
///
///     | Meta | offset1 | offset2 |  ......
///                                   ...... | data2 | data1 |
///
/// Meta Format:
///
///     | next_page_id | num_tuple | head | tail |
///
/// Note that:
///     
///     - next_page_id is None if the value is zero,
///
pub struct Slice {
    bpm: BufferPoolManagerRef,
    page: PageRef,
    pub schema: SchemaRef,
}

impl Drop for Slice {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

impl Slice {
    const NEXT_PAGE_ID: Range<usize> = 0..4;
    const NUM_TUPLE: Range<usize> = 4..8;
    const HEAD: Range<usize> = 8..12;
    const TAIL: Range<usize> = 12..16;
    /// SIZE_OF_META should equal to the end of last field
    const SIZE_OF_META: usize = 16;

    pub fn new_simple_message(
        bpm: BufferPoolManagerRef,
        header: String,
        message: String,
    ) -> Result<Self, TableError> {
        let schema = Schema::from_slice(&[(DataType::new_varchar(false), header)]);
        let mut slice = Self::new(bpm, Rc::new(schema));
        slice.add(&[Datum::VarChar(Some(message))])?;
        Ok(slice)
    }

    /// create a new empty slice
    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        {
            let buffer = &mut page.borrow_mut().buffer;
            // next_page_id = None
            buffer[Self::NEXT_PAGE_ID].copy_from_slice(&0u32.to_le_bytes());
            // num_tuple = 0
            buffer[Self::NUM_TUPLE].copy_from_slice(&0u32.to_le_bytes());
            // head = size_of_meta
            buffer[Self::HEAD].copy_from_slice(&(Self::SIZE_OF_META as u32).to_le_bytes());
            // tail = PAGE_SIZE;
            buffer[Self::TAIL].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        }
        // mark dirty
        page.borrow_mut().is_dirty = true;
        Self { page, bpm, schema }
    }

    /// open a slice with page_id
    pub fn open(bpm: BufferPoolManagerRef, schema: SchemaRef, page_id: PageID) -> Self {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        Self { page, bpm, schema }
    }

    pub fn get_page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }

    pub fn get_num_tuple(&self) -> usize {
        u32::from_le_bytes(
            self.page.borrow().buffer[Self::NUM_TUPLE]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn get_head(&self) -> usize {
        u32::from_le_bytes(self.page.borrow().buffer[Self::HEAD].try_into().unwrap()) as usize
    }

    pub fn get_tail(&self) -> usize {
        u32::from_le_bytes(self.page.borrow().buffer[Self::TAIL].try_into().unwrap()) as usize
    }

    pub fn get_free_size(&self) -> usize {
        self.get_tail() - self.get_head()
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

    pub fn set_head(&self, head: usize) {
        self.page.borrow_mut().buffer[Self::HEAD].copy_from_slice(&(head as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    pub fn set_tail(&self, tail: usize) {
        self.page.borrow_mut().buffer[Self::TAIL].copy_from_slice(&(tail as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    pub fn set_next_page_id(&self, next_page_id: Option<PageID>) {
        let next_page_id = next_page_id.unwrap_or(0);
        self.page.borrow_mut().buffer[Self::NEXT_PAGE_ID]
            .copy_from_slice(&(next_page_id as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    pub fn set_num_tuple(&self, num_tuple: usize) {
        self.page.borrow_mut().buffer[Self::NUM_TUPLE]
            .copy_from_slice(&(num_tuple as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }

    pub fn set_offset_at(&mut self, idx: usize, offset: usize) -> Result<(), TableError> {
        if idx >= self.get_num_tuple() {
            return Err(TableError::SliceIndexOutOfBound);
        }
        let start = Self::SIZE_OF_META + idx * std::mem::size_of::<u32>();
        let end = start + 4;
        self.page.borrow_mut().buffer[start..end].copy_from_slice(&(offset as u32).to_le_bytes());
        Ok(())
    }

    pub fn get_offset_at(&self, idx: usize) -> Result<usize, TableError> {
        if idx >= self.get_num_tuple() {
            return Err(TableError::SliceIndexOutOfBound);
        }
        let start = Self::SIZE_OF_META + idx * std::mem::size_of::<u32>();
        let end = start + 4;
        Ok(u32::from_le_bytes(self.page.borrow().buffer[start..end].try_into().unwrap()) as usize)
    }

    pub fn at(&self, idx: usize) -> Result<Option<Vec<Datum>>, TableError> {
        let base_offset = self.get_offset_at(idx)?;
        if base_offset == 0 {
            return Ok(None);
        }
        let bytes = &self.page.borrow().buffer[..base_offset];
        let datums = Datum::from_bytes_and_schema(self.schema.clone(), bytes);
        Ok(Some(datums))
    }

    pub fn ok_to_add(&self, datums: &[Datum]) -> bool {
        let delta_size: usize = datums
            .iter()
            .zip(self.schema.iter())
            .map(|(d, c)| d.size_of_bytes(&c.data_type))
            .sum();
        delta_size <= self.get_free_size()
    }

    pub fn remove(&mut self, idx: usize) -> Result<(), TableError> {
        let offset = self.get_offset_at(idx)?;
        if offset == 0 {
            return Err(TableError::AlreadyDeleted);
        }
        self.set_offset_at(idx, 0)?;
        Ok(())
    }

    pub fn add(&mut self, datums: &[Datum]) -> Result<RecordID, TableError> {
        // check schema
        if datums.len() != self.schema.len() {
            return Err(TableError::DatumSchemaNotMatch);
        }
        // check if ok to insert into the slice
        if !self.ok_to_add(datums) {
            return Err(TableError::SliceOutOfSpace);
        }
        let bytes = Datum::to_bytes_with_schema(datums, self.schema.clone());
        let end = self.get_tail();
        let start = end - bytes.len();
        self.page.borrow_mut().buffer[start..end].copy_from_slice(bytes.as_slice());
        self.set_tail(start);
        // move head
        let head = self.get_head();
        self.set_head(head + 4);
        // set offset
        self.page.borrow_mut().buffer[head..head + 4].copy_from_slice(&(end as u32).to_le_bytes());
        // increase num_tuple
        let num_tuple = self.get_num_tuple();
        self.set_num_tuple(num_tuple + 1);
        // mark dirty
        self.page.borrow_mut().is_dirty = true;
        Ok((self.get_page_id(), num_tuple))
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut table = Table::new();
        let header = self
            .schema
            .iter()
            .map(|c| Cell::new(c.desc.as_str()))
            .collect_vec();
        table.add_row(Row::new(header));
        for idx in 0..self.get_num_tuple() {
            let tuple = self.at(idx).unwrap();
            if let Some(tuple) = tuple {
                let tuple = tuple
                    .iter()
                    .map(|d| Cell::new(d.to_string().as_str()))
                    .collect_vec();
                table.add_row(Row::new(tuple));
            }
        }
        write!(f, "{}", table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::DataType;
    use crate::storage::BufferPoolManager;
    use crate::table::Schema;
    use std::fs::remove_file;

    #[test]
    fn test_simple_add_get() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_char(20, false), "v2".to_string()),
            ]);
            let tuple1 = vec![Datum::Int(Some(20)), Datum::Char(Some("hello".to_string()))];
            let tuple2 = vec![Datum::Int(Some(30)), Datum::Char(Some("world".to_string()))];
            let tuple3 = vec![Datum::Int(Some(40)), Datum::Char(Some("foo".to_string()))];
            let page_id = {
                let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
                slice.add(tuple1.as_slice()).unwrap();
                slice.add(tuple2.as_slice()).unwrap();
                assert_eq!(slice.at(0).unwrap().unwrap(), tuple1);
                assert_eq!(slice.at(1).unwrap().unwrap(), tuple2);
                slice.get_page_id()
            };
            // refetch
            let schema = Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_char(20, false), "v2".to_string()),
            ]);
            let mut slice = Slice::open(bpm, Rc::new(schema), page_id);
            slice.add(tuple3.as_slice()).unwrap();
            assert_eq!(slice.at(0).unwrap().unwrap(), tuple1);
            assert_eq!(slice.at(1).unwrap().unwrap(), tuple2);
            assert_eq!(slice.at(2).unwrap().unwrap(), tuple3);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_overflow() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(100);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[(DataType::new_int(false), "v1".to_string())]);
            let mut slice = Slice::new(bpm, Rc::new(schema));
            for i in 0..453 {
                slice.add(&[Datum::Int(Some(i))]).unwrap();
            }
            assert!(slice.add(&[Datum::Int(Some(0))]).is_err());
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_remove() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(100);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[(DataType::new_int(false), "v1".to_string())]);
            let mut slice = Slice::new(bpm, Rc::new(schema));
            slice.add(&[Datum::Int(Some(1))]).unwrap();
            slice.add(&[Datum::Int(Some(2))]).unwrap();
            slice.add(&[Datum::Int(Some(3))]).unwrap();
            slice.remove(1).unwrap();
            assert_eq!(slice.at(0).unwrap(), Some(vec![Datum::Int(Some(1))]));
            assert_eq!(slice.at(1).unwrap(), None);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_varchar() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Rc::new(Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_varchar(false), "v2".to_string()),
            ]));
            let mut slice = Slice::new(bpm, schema);
            let tuple1 = vec![
                Datum::Int(Some(20)),
                Datum::VarChar(Some("hello".to_string())),
            ];
            let tuple2 = vec![
                Datum::Int(Some(30)),
                Datum::VarChar(Some("world".to_string())),
            ];
            slice.add(tuple1.as_slice()).unwrap();
            slice.add(tuple2.as_slice()).unwrap();
            assert_eq!(slice.at(0).unwrap().unwrap(), tuple1);
            assert_eq!(slice.at(1).unwrap().unwrap(), tuple2);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_simple_message() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let slice = Slice::new_simple_message(bpm, "header".to_string(), "message".to_string())
                .unwrap();
            let tuple = slice.at(0).unwrap().unwrap();
            assert_eq!(tuple[0], Datum::VarChar(Some("message".to_string())));
            filename
        };
        remove_file(filename).unwrap();
    }
}
