use crate::storage::{BufferPoolManagerRef, PageID, StorageError};
use std::convert::TryInto;
use std::rc::Rc;
use thiserror::Error;

mod rid;
mod schema;
mod slice;
mod types;

pub use schema::{Column, Schema, SchemaRef};
pub use slice::{Datum, Slice};
pub use types::{CharType, DataType};

/// one table is fitted precisely in one page, which is organized as
///
///     | page_id_of_root_slice | col1 | col2 |...
///
/// each column have an desc, which is a string, and type id that describe
/// the type this column have.
///
///     | desc_len | chars_of_desc | type_id |
///
/// type_id:
///
///     | id: u8 | data: u32 |
///

#[allow(dead_code)]
pub struct Table {
    schema: Rc<Schema>,
    bpm: BufferPoolManagerRef,
}

#[allow(dead_code)]
pub struct TableIter {
    page_id: PageID,
    idx: usize,
}

impl Iterator for TableIter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[allow(dead_code)]
impl Table {
    /// open an exist table from disk
    pub fn open(page_id: PageID, bpm: BufferPoolManagerRef) -> Self {
        // fetch page from bpm
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        // reconstruct schema
        let mut offset = 4;
        let mut cols = vec![];
        loop {
            let desc_len =
                u32::from_le_bytes(page.borrow().buffer[offset..offset + 4].try_into().unwrap())
                    as usize;
            if desc_len == 0 {
                break;
            }
            offset += 4;
            let name = String::from_utf8(page.borrow().buffer[offset..offset + desc_len].to_vec())
                .unwrap();
            offset += desc_len;
            let dat =
                DataType::from_bytes(&page.borrow().buffer[offset..offset + 4].try_into().unwrap())
                    .unwrap();
            cols.push((dat, name));
        }
        let schema = Rc::new(Schema::from_slice(cols.as_slice()));
        Self { schema, bpm }
    }
    /// create an table
    pub fn new(schema: Schema, bpm: BufferPoolManagerRef) -> Self {
        let schema = Rc::new(schema);
        // alloc a page
        let page = bpm.borrow_mut().alloc().unwrap();
        let page_id = page.borrow().page_id.unwrap();
        // alloc slice page
        let mut slice = Slice::new(bpm.clone(), schema.clone());
        let slice_page = bpm.borrow_mut().alloc().unwrap();
        let page_id_of_root_slice = slice_page.borrow().page_id.unwrap();
        slice.attach(page_id_of_root_slice);
        bpm.borrow_mut().unpin(page_id_of_root_slice).unwrap();
        page.borrow_mut().buffer[0..4]
            .copy_from_slice(&(page_id_of_root_slice as u32).to_le_bytes());
        // fill schema
        let mut offset = 4;
        schema.iter().for_each(|col| {
            let desc_len = col.desc.len();
            page.borrow_mut().buffer[offset..offset + 4]
                .copy_from_slice(&(desc_len as u32).to_le_bytes());
            offset += 4;
            page.borrow_mut().buffer[offset..offset + desc_len]
                .copy_from_slice(col.desc.as_bytes());
            offset += desc_len;
            page.borrow_mut().buffer[offset..offset + 5].copy_from_slice(&col.data_type.as_bytes());
            offset += 5;
            page.borrow_mut().buffer[offset..offset + 4].copy_from_slice(&[0u8; 4]);
        });
        // unpin page
        bpm.borrow_mut().unpin(page_id).unwrap();
        Self { schema, bpm }
    }
    pub fn insert(_datums: &[Datum]) -> Result<(), TableError> {
        todo!()
    }
    pub fn iter() -> TableIter {
        todo!()
    }
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum TableError {
    #[error("datum not match with schema")]
    DatumSchemaNotMatch,
    #[error("slice out of space")]
    SliceOutOfSpace,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("PageID not assigned")]
    NoPageID,
}
