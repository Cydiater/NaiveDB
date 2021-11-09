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
/// we mark offset = PAGE_SIZE as the end sign
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
        Ok((next_head, next_tail))
    }
    pub fn add(&mut self, datums: Vec<Datum>) -> Result<(), TableError> {
        if datums.len() != self.schema.len() {
            return Err(TableError::DatumSchemaNotMatch);
        }
        // don't have page, allocate first
        if self.page_id.is_none() {
            let page = self.bpm.borrow_mut().alloc().unwrap();
            // mark end
            page.borrow_mut().buffer[0..4].copy_from_slice(&PAGE_SIZE.to_le_bytes());
            self.page_id = Some(page.borrow_mut().page_id.unwrap());
        }
        for (col, dat) in self.schema.iter().zip(datums.iter()) {
            let (head, tail) = match (dat, col.data_type) {
                (Datum::Int(dat), DataType::Int) => self.push(&dat.to_le_bytes())?,
                (Datum::Char(dat), DataType::Char(char_type)) => {
                    self.push(dat.with_exact_width(char_type.width).as_bytes())?
                }
                _ => {
                    return Err(TableError::DatumSchemaNotMatch);
                }
            };
            self.head = head;
            self.tail = tail;
        }
        Ok(())
    }
}
