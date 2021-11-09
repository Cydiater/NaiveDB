use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::{Column, DataType, Schema, TableError};
use pad::PadStr;
use std::convert::TryInto;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum Datum {
    Int(i32),
    Char(String),
    VarChar(String),
}

#[allow(dead_code)]
/// one slice is fitted precisely in one page,
/// we have multiple tuples in one slice. One Slice is organized as
///
///     |offset1|offset2|......
///                      ......|data2|data1|
///
/// we mark offset = PAGE_SIZE as the end sign, there are two type of
/// columns, the inlined and un-inlined. for the inlined column, we just
/// put the original data in the page, for the un-inlined column, you should
/// put RecordID in to link the original data.
///
pub struct Slice {
    page_id: Option<PageID>,
    next_page_id: Option<PageID>,
    bpm: BufferPoolManagerRef,
    schema: Schema,
    head: usize,
    tail: usize,
}

#[allow(dead_code)]
pub struct SliceIter {
    bpm: BufferPoolManagerRef,
    page: PageRef,
    idx: usize,
}

impl Iterator for SliceIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = u32::from_le_bytes(
            self.page.borrow_mut().buffer[self.idx * 4..(self.idx + 1) * 4]
                .try_into()
                .unwrap(),
        ) as usize;
        if offset == PAGE_SIZE {
            None
        } else {
            Some(offset)
        }
    }
}

impl Drop for SliceIter {
    fn drop(&mut self) {
        self.bpm
            .borrow_mut()
            .unpin(self.page.borrow_mut().page_id.unwrap())
            .unwrap()
    }
}

#[allow(dead_code)]
impl Slice {
    pub fn new_simple_message(
        _bpm: BufferPoolManagerRef,
        header: String,
        _message: String,
    ) -> Self {
        let _schema = Schema::new(vec![Column::new(8, DataType::VarChar, header)]);
        todo!();
    }
    pub fn new(bpm: BufferPoolManagerRef, schema: Schema) -> Self {
        Self {
            page_id: None,
            next_page_id: None,
            bpm,
            schema,
            head: 0usize,
            tail: PAGE_SIZE,
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
        let page = self.bpm.borrow_mut().fetch(page_id).unwrap();
        self.page_id = Some(page.borrow().page_id.unwrap());
        let (head, tail) = self
            .iter()
            .fold((0, PAGE_SIZE), |(head, _), offset| (head + 4, offset));
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
    pub fn at(&mut self, idx: usize) -> Result<Vec<Datum>, TableError> {
        // fetch page
        let page = self.bpm.borrow_mut().fetch(self.page_id.unwrap())?;
        let end = u32::from_le_bytes(
            page.borrow().buffer
                [idx * std::mem::size_of::<u32>()..(idx + 1) * std::mem::size_of::<u32>()]
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
    pub fn add(&mut self, datums: &[Datum]) -> Result<(), TableError> {
        if datums.len() != self.schema.len() {
            return Err(TableError::DatumSchemaNotMatch);
        }
        // don't have page, allocate first
        let page = if self.page_id.is_none() {
            let page = self.bpm.borrow_mut().alloc().unwrap();
            // mark end
            page.borrow_mut().buffer[0..4].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
            self.page_id = Some(page.borrow_mut().page_id.unwrap());
            page
        } else {
            self.bpm.borrow_mut().fetch(self.page_id.unwrap()).unwrap()
        };
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
        self.bpm.borrow_mut().unpin(self.page_id.unwrap())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BufferPoolManager;
    use crate::table::{CharType, Column, DataType, Schema};

    #[test]
    fn test_simple_add_get() {
        let bpm = BufferPoolManager::new_shared(5);
        bpm.borrow_mut().clear().unwrap();
        let columns = vec![
            Column::new(4, DataType::Int, "v1".to_string()),
            Column::new(24, DataType::Char(CharType::new(20)), "v2".to_string()),
        ];
        let schema = Schema::new(columns);
        let mut slice = Slice::new(bpm.clone(), schema);
        let tuple1 = vec![Datum::Int(20), Datum::Char("hello".to_string())];
        let tuple2 = vec![Datum::Int(30), Datum::Char("world".to_string())];
        slice.add(&tuple1).unwrap();
        slice.add(&tuple2).unwrap();
        assert_eq!(slice.at(0).unwrap(), tuple1);
        assert_eq!(slice.at(1).unwrap(), tuple2);
    }

    #[test]
    fn test_varchar() {
        let bpm = BufferPoolManager::new_shared(5);
        bpm.borrow_mut().clear().unwrap();
        let columns = vec![
            Column::new(4, DataType::Int, "v1".to_string()),
            Column::new(12, DataType::VarChar, "v2".to_string()),
        ];
        let schema = Schema::new(columns);
        let mut slice = Slice::new(bpm.clone(), schema);
        let tuple1 = vec![Datum::Int(20), Datum::VarChar("hello".to_string())];
        let tuple2 = vec![Datum::Int(30), Datum::VarChar("world".to_string())];
        slice.add(&tuple1).unwrap();
        slice.add(&tuple2).unwrap();
        assert_eq!(slice.at(0).unwrap(), tuple1);
        assert_eq!(slice.at(1).unwrap(), tuple2);
    }
}
