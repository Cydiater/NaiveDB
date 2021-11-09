use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::{DataType, Schema, TableError};
use pad::PadStr;
use std::convert::TryInto;

#[allow(dead_code)]
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
    /// we do not modify head and tail here, this is to satisfy rust compiler borrow check
    pub fn push(&self, data: &[u8]) -> Result<(usize, usize), TableError> {
        assert!(self.page_id.is_some());
        // fetch page from bpm
        let page_id = self.page_id.unwrap();
        let page = self.bpm.borrow_mut().fetch(page_id).unwrap();
        let next_head = self.head + std::mem::size_of::<u32>();
        let next_tail = self.tail - data.len();
        if next_head >= next_tail {
            return Err(TableError::SliceOutOfSpace);
        }
        page.borrow_mut().buffer[self.head..next_head].copy_from_slice(&next_tail.to_le_bytes());
        page.borrow_mut().buffer[next_tail..self.tail].copy_from_slice(data);
        self.bpm.borrow_mut().unpin(page_id)?;
        Ok((next_head, next_tail))
    }
    pub fn push_external_data(&self, data: &[u8]) -> Result<usize, TableError> {
        // fetch page from bpm
        let page_id = self.page_id.unwrap();
        let page = self.bpm.borrow_mut().fetch(page_id).unwrap();
        let next_tail = self.tail - data.len();
        page.borrow_mut().buffer[next_tail..self.tail].copy_from_slice(data);
        self.bpm.borrow_mut().unpin(page_id)?;
        Ok(next_tail)
    }
    pub fn at(&mut self, idx: usize) -> Result<Vec<Datum>, TableError> {
        // fetch page
        let page = self.bpm.borrow_mut().fetch(self.page_id.unwrap())?;
        let start = u32::from_le_bytes(
            page.borrow().buffer[idx * 4..(idx + 1) * 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let mut tuple = Vec::<Datum>::new();
        for col in self.schema.iter() {
            let offset = start + col.offset;
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
                        .to_string(),
                    ));
                }
                DataType::VarChar => {
                    let start = u32::from_le_bytes(
                        page.borrow().buffer[offset..offset + 4].try_into().unwrap(),
                    ) as usize;
                    let end = u32::from_le_bytes(
                        page.borrow().buffer[offset + 4..offset + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    tuple.push(Datum::Char(
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
    pub fn add(&mut self, datums: Vec<Datum>) -> Result<(), TableError> {
        if datums.len() != self.schema.len() {
            return Err(TableError::DatumSchemaNotMatch);
        }
        // don't have page, allocate first
        let page = if self.page_id.is_none() {
            let page = self.bpm.borrow_mut().alloc().unwrap();
            // mark end
            page.borrow_mut().buffer[0..4].copy_from_slice(&PAGE_SIZE.to_le_bytes());
            self.page_id = Some(page.borrow_mut().page_id.unwrap());
            page
        } else {
            self.bpm.borrow_mut().fetch(self.page_id.unwrap()).unwrap()
        };
        let mut not_inlined_indexes = Vec::<(usize, usize)>::new();
        for (idx, (col, dat)) in self.schema.iter().zip(datums.iter()).enumerate() {
            let (head, tail) = match (dat, col.data_type) {
                (Datum::Int(dat), DataType::Int) => self.push(&dat.to_le_bytes())?,
                (Datum::Char(dat), DataType::Char(char_type)) => {
                    self.push(dat.with_exact_width(char_type.width).as_bytes())?
                }
                // put placeholder first, we fill offset and length later
                (Datum::VarChar(_), DataType::VarChar) => {
                    let (head, tail) = self.push(&[0u8; 8])?;
                    not_inlined_indexes.push((idx, tail));
                    (head, tail)
                }
                _ => {
                    return Err(TableError::DatumSchemaNotMatch);
                }
            };
            self.head = head;
            self.tail = tail;
        }
        // fill the external data
        for (idx, offset) in not_inlined_indexes {
            let end = self.tail;
            let start = match &datums[idx] {
                Datum::VarChar(dat) => self.push_external_data(dat.as_bytes())?,
                _ => {
                    return Err(TableError::DatumSchemaNotMatch);
                }
            };
            self.tail = start;
            page.borrow_mut().buffer[offset..offset + 4]
                .copy_from_slice(&(start as u32).to_le_bytes());
            page.borrow_mut().buffer[offset + 4..offset + 8]
                .copy_from_slice(&(end as u32).to_le_bytes());
        }
        self.bpm
            .borrow_mut()
            .unpin(page.borrow().page_id.unwrap())?;
        Ok(())
    }
}
