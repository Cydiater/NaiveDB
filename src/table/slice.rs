use crate::datum::{DataType, Datum};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::{Schema, SchemaRef, TableError};
use itertools::Itertools;
use prettytable::{Cell, Row, Table};
use std::convert::TryInto;
use std::fmt;
use std::rc::Rc;

/// one slice is fitted precisely in one page,
/// we have multiple tuples in one slice. one Slice is organized as
///
///     | next_page_id | offset1: u32 | offset2: u32 |  ......
///                                                     ...... | data2 | data1 |
///
/// we mark offset = PAGE_SIZE as the end sign.
///
pub struct Slice {
    pub page_id: Option<PageID>,
    bpm: BufferPoolManagerRef,
    pub schema: SchemaRef,
    head: usize,
    tail: usize,
}

pub struct SliceIter {
    bpm: BufferPoolManagerRef,
    page: PageRef,
    idx: usize,
}

impl Iterator for SliceIter {
    /// start offset and end offset of a tuple data
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let end = u32::from_le_bytes(
            self.page.borrow().buffer[(self.idx + 1) * 4..(self.idx + 2) * 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let start = u32::from_le_bytes(
            self.page.borrow().buffer[(self.idx + 2) * 4..(self.idx + 3) * 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if start == 0 {
            None
        } else {
            self.idx += 1;
            Some((start, end))
        }
    }
}

impl Drop for SliceIter {
    fn drop(&mut self) {
        let page_id = self.page.borrow_mut().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

impl Slice {
    pub fn new_simple_message(
        bpm: BufferPoolManagerRef,
        header: String,
        message: String,
    ) -> Result<Self, TableError> {
        let schema = Schema::from_slice(&[(DataType::new_varchar(false), header)]);
        let mut slice = Self::new(bpm, Rc::new(schema));
        slice.add(vec![Datum::VarChar(Some(message))])?;
        Ok(slice)
    }

    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        Self {
            page_id: None,
            bpm,
            schema,
            head: 4usize,
            tail: PAGE_SIZE,
        }
    }

    pub fn remain(&self) -> usize {
        // 4 for next_page_id, 8 for end mark
        self.tail - self.head - 8 - 4
    }

    pub fn new_empty(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        let page_id = page.borrow().page_id.unwrap();
        // mark end next_page_id
        page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id as u32).to_le_bytes());
        // mark end tuple
        page.borrow_mut().buffer[4..8].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        page.borrow_mut().buffer[8..12].copy_from_slice(&(0u32).to_le_bytes());
        bpm.borrow_mut().unpin(page_id).unwrap();
        Self {
            page_id: Some(page_id),
            bpm,
            schema,
            head: 4usize,
            tail: PAGE_SIZE,
        }
    }

    pub fn get_next_page_id(&self) -> Option<PageID> {
        if let Some(page_id) = self.page_id {
            let page = self.bpm.borrow_mut().fetch(page_id).unwrap();
            let next_page_id =
                u32::from_le_bytes(page.borrow().buffer[0..4].try_into().unwrap()) as PageID;
            self.bpm.borrow_mut().unpin(page_id).unwrap();
            if next_page_id != page_id {
                Some(next_page_id)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn set_next_page_id(&mut self, page_id: PageID) -> Result<(), TableError> {
        if let Some(my_page_id) = self.page_id {
            let page = self.bpm.borrow_mut().fetch(my_page_id).unwrap();
            page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id as u32).to_le_bytes());
            Ok(())
        } else {
            Err(TableError::NoPageID)
        }
    }

    pub fn iter(&mut self) -> SliceIter {
        SliceIter {
            bpm: self.bpm.clone(),
            page: self.bpm.borrow_mut().fetch(self.page_id.unwrap()).unwrap(),
            idx: 0usize,
        }
    }

    pub fn attach(&mut self, page_id: PageID) {
        self.page_id = Some(page_id);
        let (head, tail) = self
            .iter()
            .fold((4, PAGE_SIZE), |(head, _), (tail, _)| (head + 4, tail));
        self.head = head;
        self.tail = tail;
    }

    pub fn push(&self, data: &[u8]) -> Result<usize, TableError> {
        // fetch page from bpm
        let page_id = self.page_id.unwrap();
        let page = self.bpm.borrow_mut().fetch(page_id).unwrap();
        let next_tail = self.tail - data.len();
        page.borrow_mut().buffer[next_tail..self.tail].copy_from_slice(data);
        // mark dirty
        page.borrow_mut().is_dirty = true;
        // unpin page
        self.bpm.borrow_mut().unpin(page_id)?;
        Ok(next_tail)
    }

    pub fn at(&self, idx: usize) -> Result<Vec<Datum>, TableError> {
        // fetch page
        let page = self.bpm.borrow_mut().fetch(self.page_id.unwrap())?;
        let end = u32::from_le_bytes(
            page.borrow().buffer
                [(idx + 1) * std::mem::size_of::<u32>()..(idx + 2) * std::mem::size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        assert!(end <= PAGE_SIZE);
        let mut tuple = Vec::<Datum>::new();
        for col in self.schema.iter() {
            let offset = end - col.offset;
            assert!(offset < PAGE_SIZE);
            let datum = if col.data_type.is_inlined() {
                let start = offset;
                let end = start + col.data_type.width_of_value().unwrap();
                let bytes = page.borrow().buffer[start..end].to_vec();
                Datum::from_bytes(col.data_type, bytes)
            } else {
                let start = u32::from_le_bytes(
                    page.borrow().buffer[offset..offset + 4].try_into().unwrap(),
                ) as usize;
                let end = u32::from_le_bytes(
                    page.borrow().buffer[offset + 4..offset + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let bytes = page.borrow().buffer[start..end].to_vec();
                Datum::from_bytes(col.data_type, bytes)
            };
            tuple.push(datum);
        }
        // unpin page
        self.bpm.borrow_mut().unpin(self.page_id.unwrap())?;
        Ok(tuple)
    }

    pub fn ok_to_add(&self, datums: &[Datum]) -> bool {
        let delta_size: usize = datums
            .iter()
            .zip(self.schema.iter())
            .map(|(d, c)| d.size_of_bytes(&c.data_type))
            .sum();
        delta_size <= self.remain()
    }

    pub fn add(&mut self, datums: Vec<Datum>) -> Result<(), TableError> {
        if datums.len() != self.schema.len() {
            return Err(TableError::DatumSchemaNotMatch);
        }
        // don't have page, allocate first
        let page = if self.page_id.is_none() {
            let page = self.bpm.borrow_mut().alloc().unwrap();
            // mark end
            page.borrow_mut().buffer[4..8].copy_from_slice(&(0u32).to_le_bytes());
            // fill page_id
            self.page_id = Some(page.borrow_mut().page_id.unwrap());
            // mark end slice
            page.borrow_mut().buffer[0..4]
                .copy_from_slice(&(self.page_id.unwrap() as u32).to_le_bytes());
            page
        } else {
            self.bpm.borrow_mut().fetch(self.page_id.unwrap()).unwrap()
        };
        // check if ok to insert into the slice
        if !self.ok_to_add(&datums) {
            return Err(TableError::SliceOutOfSpace);
        }
        let mut not_inlined_data = Vec::<(usize, DataType, Datum)>::new();
        let last_tail = self.tail;
        for (col, dat) in self.schema.iter().zip(datums.into_iter()) {
            self.tail = if dat.is_inlined() {
                self.push(dat.into_bytes(&col.data_type).as_slice())?
            } else {
                let tail = self.push(&[0u8; 8])?;
                not_inlined_data.push((tail, col.data_type, dat));
                tail
            };
        }
        // fill the external data
        for (offset, data_type, dat) in not_inlined_data {
            let end = self.tail;
            let tail = self.push(&dat.into_bytes(&data_type))?;
            let start = tail;
            self.tail = tail;
            page.borrow_mut().buffer[offset..offset + 4]
                .copy_from_slice(&(start as u32).to_le_bytes());
            page.borrow_mut().buffer[offset + 4..offset + 8]
                .copy_from_slice(&(end as u32).to_le_bytes());
        }
        let next_head = self.head + std::mem::size_of::<u32>();
        page.borrow_mut().buffer[self.head..next_head]
            .copy_from_slice(&(last_tail as u32).to_le_bytes());
        self.head = next_head;
        // mark tail
        page.borrow_mut().buffer[next_head..next_head + 4]
            .copy_from_slice(&(self.tail as u32).to_le_bytes());
        // mark next end
        page.borrow_mut().buffer[next_head + 4..next_head + 8]
            .copy_from_slice(&(0u32).to_le_bytes());
        // mark dirty
        page.borrow_mut().is_dirty = true;
        // unpin
        self.bpm.borrow_mut().unpin(self.page_id.unwrap())?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        (self.head - 4) / std::mem::size_of::<u32>()
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
        for idx in 0..self.len() {
            let tuple = self.at(idx).unwrap();
            let tuple = tuple
                .iter()
                .map(|d| Cell::new(d.to_string().as_str()))
                .collect_vec();
            table.add_row(Row::new(tuple));
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
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            let tuple1 = vec![Datum::Int(Some(20)), Datum::Char(Some("hello".to_string()))];
            let tuple2 = vec![Datum::Int(Some(30)), Datum::Char(Some("world".to_string()))];
            let tuple3 = vec![Datum::Int(Some(40)), Datum::Char(Some("foo".to_string()))];
            slice.add(tuple1.clone()).unwrap();
            slice.add(tuple2.clone()).unwrap();
            assert_eq!(slice.at(0).unwrap(), tuple1);
            assert_eq!(slice.at(1).unwrap(), tuple2);
            let page_id = slice.page_id.unwrap();
            // refetch
            let schema = Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_char(20, false), "v2".to_string()),
            ]);
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            slice.attach(page_id);
            slice.add(tuple3.clone()).unwrap();
            assert_eq!(slice.at(0).unwrap(), tuple1);
            assert_eq!(slice.at(1).unwrap(), tuple2);
            assert_eq!(slice.at(2).unwrap(), tuple3);
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
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            for i in 0..453 {
                slice.add(vec![Datum::Int(Some(i))]).unwrap();
            }
            assert!(slice.add(vec![Datum::Int(Some(0))]).is_err());
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_varchar() {
        let filename = {
            let bpm = BufferPoolManager::new_shared(5);
            let filename = bpm.borrow().filename();
            bpm.borrow_mut().clear().unwrap();
            let schema = Rc::new(Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_varchar(false), "v2".to_string()),
            ]));
            let mut slice = Slice::new(bpm.clone(), schema);
            let tuple1 = vec![
                Datum::Int(Some(20)),
                Datum::VarChar(Some("hello".to_string())),
            ];
            let tuple2 = vec![
                Datum::Int(Some(30)),
                Datum::VarChar(Some("world".to_string())),
            ];
            slice.add(tuple1.clone()).unwrap();
            slice.add(tuple2.clone()).unwrap();
            assert_eq!(slice.at(0).unwrap(), tuple1);
            assert_eq!(slice.at(1).unwrap(), tuple2);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_simple_message() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            bpm.borrow_mut().clear().unwrap();
            let slice =
                Slice::new_simple_message(bpm.clone(), "header".to_string(), "message".to_string())
                    .unwrap();
            let tuple = slice.at(0).unwrap();
            assert_eq!(tuple[0], Datum::VarChar(Some("message".to_string())));
            filename
        };
        remove_file(filename).unwrap();
    }
}
