use crate::storage::{BufferPoolManagerRef, PageID, PAGE_SIZE};
use crate::table::{DataType, Schema, TableError};

#[allow(dead_code)]
pub enum Datum {
    Int(i32),
    Char(String),
    VarChar(String),
}

#[allow(dead_code)]
/// one slice is fitted precisely in one page,
/// we have multiple tuples in one slice.
pub struct Slice {
    page_id: Option<PageID>,
    next_page_id: Option<PageID>,
    bpm: BufferPoolManagerRef,
    schema: Schema,
    head: usize,
    tail: usize,
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
