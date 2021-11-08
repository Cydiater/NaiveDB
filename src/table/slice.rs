use crate::storage::{BufferPoolManagerRef, PageID, PageRef, PAGE_SIZE};
use crate::table::{DataType, Schema, TableError};
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
        let _page = self.bpm.borrow_mut().fetch(page_id).unwrap();
        todo!();
    }
    pub fn shrink_head_and_tail(
        &mut self,
        head_data: &[u8],
        tail_data: &[u8],
    ) -> Result<(), TableError> {
        assert!(self.page_id.is_some());
        // fetch page from bpm
        let page_id = self.page_id.unwrap();
        let page = self.bpm.borrow_mut().fetch(page_id).unwrap();
        let next_head = self.head + head_data.len();
        let next_tail = self.tail - tail_data.len();
        if next_head >= next_tail {
            return Err(TableError::SliceOutOfSpace);
        }
        page.borrow_mut().buffer[self.head..next_head].copy_from_slice(head_data);
        page.borrow_mut().buffer[next_tail..self.tail].copy_from_slice(tail_data);
        self.head = next_head;
        self.tail = next_tail;
        Ok(())
    }
    pub fn add(&mut self, datums: Vec<Datum>) -> Result<(), TableError> {
        if datums.len() != self.schema.len() {
            return Err(TableError::DatumSchemaNotMatch);
        }
        let page = self.bpm.borrow_mut().alloc().unwrap();
        self.page_id = Some(page.borrow_mut().page_id.unwrap());
        self.schema
            .iter()
            .zip(datums.iter())
            .for_each(|(col, dat)| match dat {
                Datum::Int(_) => {
                    if matches!(col.data_type, DataType::Int) {
                        todo!()
                    }
                }
                Datum::Char(_) => todo!(),
                _ => {}
            });
        todo!();
    }
}
