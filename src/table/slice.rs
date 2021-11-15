use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::{DataType, Schema, SchemaRef, TableError};
use itertools::Itertools;
use pad::PadStr;
use prettytable::{Cell, Row, Table};
use std::convert::TryInto;
use std::fmt;
use std::rc::Rc;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum Datum {
    Int(i32),
    Char(String),
    VarChar(String),
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Int(d) => d.to_string(),
                Self::Char(s) => s.to_string(),
                Self::VarChar(s) => s.to_string(),
            }
        )
    }
}

#[allow(dead_code)]
/// one slice is fitted precisely in one page,
/// we have multiple tuples in one slice. one Slice is organized as
///
///     |next_page_id|offset1|offset2|......
///                      ......|data2|data1|
///
/// we mark offset = 0 as the end sign, there are two type of
/// columns, the inlined and un-inlined. for the inlined column, we just
/// put the original data in the page, for the un-inlined column, you should
/// put RecordID in to link the original data.
///
/// For a slice, if the next_page_id is equal to self's page_id, then we
/// assume that this slice is the end slice.
///
pub struct Slice {
    pub page_id: Option<PageID>,
    bpm: BufferPoolManagerRef,
    schema: SchemaRef,
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

#[allow(dead_code)]
impl Slice {
    pub fn new_simple_message(
        bpm: BufferPoolManagerRef,
        header: String,
        message: String,
    ) -> Result<Self, TableError> {
        let schema = Schema::from_slice(&[(DataType::VarChar, header)]);
        let mut slice = Self::new(bpm, Rc::new(schema));
        slice.add(&[Datum::VarChar(message)])?;
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
            page.borrow_mut().buffer[0..4].copy_from_slice(&page_id.to_le_bytes());
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
            match col.data_type {
                DataType::Int => {
                    tuple.push(Datum::Int(i32::from_le_bytes(
                        page.borrow().buffer[offset..offset + 4].try_into().unwrap(),
                    )));
                }
                DataType::Char(char_type) => {
                    tuple.push(Datum::Char(
                        String::from_utf8_lossy(
                            page.borrow().buffer[offset..offset + char_type.width]
                                .try_into()
                                .unwrap(),
                        )
                        .to_string()
                        .trim_end()
                        .to_string(),
                    ));
                }
                DataType::VarChar => {
                    let start = end
                        - u32::from_le_bytes(
                            page.borrow().buffer[offset..offset + 4].try_into().unwrap(),
                        ) as usize;
                    let end = end
                        - u32::from_le_bytes(
                            page.borrow().buffer[offset + 4..offset + 8]
                                .try_into()
                                .unwrap(),
                        ) as usize;
                    tuple.push(Datum::VarChar(
                        String::from_utf8_lossy(
                            page.borrow().buffer[start..end].try_into().unwrap(),
                        )
                        .to_string(),
                    ));
                }
            }
        }
        // unpin page
        self.bpm.borrow_mut().unpin(self.page_id.unwrap())?;
        Ok(tuple)
    }

    pub fn size_of(&self, datums: &[Datum]) -> usize {
        self.schema
            .iter()
            .zip(datums.iter())
            .fold(0usize, |size, (col, dat)| match (dat, col.data_type) {
                (Datum::Int(_), DataType::Int) => size + std::mem::size_of::<u32>(),
                (Datum::Char(_), DataType::Char(char_type)) => size + char_type.width,
                (Datum::VarChar(dat), DataType::VarChar) => size + dat.len(),
                _ => 0usize,
            })
    }

    pub fn add(&mut self, datums: &[Datum]) -> Result<(), TableError> {
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
        let size = self.size_of(datums);
        if self.head + 4 + 4 > self.tail - size {
            return Err(TableError::SliceOutOfSpace);
        }
        let mut not_inlined_indexes = Vec::<(usize, usize)>::new();
        let last_tail = self.tail;
        for (idx, (col, dat)) in self.schema.iter().zip(datums.iter()).enumerate() {
            let tail = match (dat, col.data_type) {
                (Datum::Int(dat), DataType::Int) => self.push(&dat.to_le_bytes())?,
                (Datum::Char(dat), DataType::Char(char_type)) => {
                    self.push(dat.with_exact_width(char_type.width).as_bytes())?
                }
                // put placeholder first, we fill offset and length later
                (Datum::VarChar(_), DataType::VarChar) => {
                    let tail = self.push(&[0u8; 8])?;
                    not_inlined_indexes.push((idx, tail));
                    tail
                }
                _ => {
                    return Err(TableError::DatumSchemaNotMatch);
                }
            };
            self.tail = tail;
        }
        // fill the external data
        for (idx, offset) in not_inlined_indexes {
            let end = last_tail - self.tail;
            let tail = match &datums[idx] {
                Datum::VarChar(dat) => self.push(dat.as_bytes())?,
                _ => {
                    return Err(TableError::DatumSchemaNotMatch);
                }
            };
            let start = last_tail - tail;
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
    use crate::storage::BufferPoolManager;
    use crate::table::{CharType, Column, DataType, Schema};
    use std::fs::remove_file;

    #[test]
    fn test_simple_add_get() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[
                (DataType::Int, "v1".to_string()),
                (DataType::Char(CharType::new(20)), "v2".to_string()),
            ]);
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            let tuple1 = vec![Datum::Int(20), Datum::Char("hello".to_string())];
            let tuple2 = vec![Datum::Int(30), Datum::Char("world".to_string())];
            let tuple3 = vec![Datum::Int(40), Datum::Char("foo".to_string())];
            slice.add(&tuple1).unwrap();
            slice.add(&tuple2).unwrap();
            assert_eq!(slice.at(0).unwrap(), tuple1);
            assert_eq!(slice.at(1).unwrap(), tuple2);
            let page_id = slice.page_id.unwrap();
            // refetch
            let schema = Schema::from_slice(&[
                (DataType::Int, "v1".to_string()),
                (DataType::Char(CharType::new(20)), "v2".to_string()),
            ]);
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            slice.attach(page_id);
            slice.add(&tuple3).unwrap();
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
            let schema = Schema::from_slice(&[(DataType::Int, "v1".to_string())]);
            let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
            // 4 + (4 + 4) * 511 = 4092
            for i in 0..511 {
                slice.add(&[Datum::Int(i)]).unwrap();
            }
            assert!(slice.add(&[Datum::Int(0)]).is_err());
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
            let columns = vec![
                Column::new(4, DataType::Int, "v1".to_string()),
                Column::new(12, DataType::VarChar, "v2".to_string()),
            ];
            let schema = Rc::new(Schema::new(columns));
            let mut slice = Slice::new(bpm.clone(), schema);
            let tuple1 = vec![Datum::Int(20), Datum::VarChar("hello".to_string())];
            let tuple2 = vec![Datum::Int(30), Datum::VarChar("world".to_string())];
            slice.add(&tuple1).unwrap();
            slice.add(&tuple2).unwrap();
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
            assert_eq!(tuple[0], Datum::VarChar("message".to_string()));
            filename
        };
        remove_file(filename).unwrap();
    }
}
