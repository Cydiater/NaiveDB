use crate::storage::{BufferPoolManagerRef, PageID, StorageError};
use std::convert::TryInto;
use std::rc::Rc;
use thiserror::Error;

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
///     | desc_len | chars_of_desc | type_id | nullable |
///
/// type_id:
///
///     | id: u8 | data: u32 |
///

#[allow(dead_code)]
pub struct Table {
    pub schema: Rc<Schema>,
    bpm: BufferPoolManagerRef,
    pub page_id: PageID,
}

#[allow(dead_code)]
pub struct TableIter {
    idx: usize,
    page_id: PageID,
    bpm: BufferPoolManagerRef,
    schema: SchemaRef,
}

impl Iterator for TableIter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut slice = Slice::new(self.bpm.clone(), self.schema.clone());
        slice.attach(self.page_id);
        if self.idx < slice.len() {
            let ret = Some(slice.at(self.idx).unwrap());
            self.idx += 1;
            ret
        } else if let Some(page_id_of_next_slice) = slice.get_next_page_id() {
            self.page_id = page_id_of_next_slice;
            self.idx = 1;
            slice.attach(self.page_id);
            Some(slice.at(0).unwrap())
        } else {
            None
        }
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
                DataType::from_bytes(&page.borrow().buffer[offset..offset + 5].try_into().unwrap())
                    .unwrap();
            offset += 5;
            let nullable = page.borrow().buffer[offset] != 0;
            offset += 1;
            cols.push((dat, name, nullable));
        }
        let schema = Rc::new(Schema::from_slice(cols.as_slice()));
        // unpin page
        bpm.borrow_mut().unpin(page_id).unwrap();
        Self {
            schema,
            bpm,
            page_id,
        }
    }
    /// create an table
    pub fn new(schema: SchemaRef, bpm: BufferPoolManagerRef) -> Self {
        // alloc a page
        let page = bpm.borrow_mut().alloc().unwrap();
        let page_id = page.borrow().page_id.unwrap();
        // alloc slice page
        let slice = Slice::new_empty(bpm.clone(), schema.clone());
        let page_id_of_root_slice = slice.page_id.unwrap();
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
            offset += 1;
            page.borrow_mut().buffer[offset] = col.nullable.into();
        });
        // mark dirty
        page.borrow_mut().is_dirty = true;
        // unpin page
        bpm.borrow_mut().unpin(page_id).unwrap();
        Self {
            schema,
            bpm,
            page_id,
        }
    }
    pub fn get_page_id_of_root_slice(&self) -> PageID {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        let page_id = u32::from_le_bytes(page.borrow().buffer[0..4].try_into().unwrap()) as usize;
        self.bpm.borrow_mut().unpin(self.page_id).unwrap();
        page_id
    }
    pub fn set_page_id_of_root_slice(&self, page_id: PageID) {
        let page = self.bpm.borrow_mut().fetch(self.page_id).unwrap();
        page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id as u32).to_le_bytes());
        self.bpm.borrow_mut().unpin(self.page_id).unwrap();
    }
    pub fn insert(&mut self, datums: &[Datum]) -> Result<(), TableError> {
        let page_id_of_root_slice = self.get_page_id_of_root_slice();
        let mut slice = Slice::new(self.bpm.clone(), self.schema.clone());
        slice.attach(page_id_of_root_slice);
        if slice.add(datums).is_ok() {
            Ok(())
        } else {
            let mut new_slice = Slice::new(self.bpm.clone(), self.schema.clone());
            new_slice.add(datums).unwrap();
            self.set_page_id_of_root_slice(new_slice.page_id.unwrap());
            new_slice.set_next_page_id(slice.page_id.unwrap()).unwrap();
            Ok(())
        }
    }
    pub fn iter(&self) -> TableIter {
        TableIter {
            idx: 0,
            page_id: self.get_page_id_of_root_slice(),
            bpm: self.bpm.clone(),
            schema: self.schema.clone(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BufferPoolManager;
    use itertools::Itertools;
    use std::cell::RefCell;
    use std::fs::remove_file;

    #[test]
    fn test_multiple_slice() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[(DataType::Int, "v1".to_string(), false)]);
            let mut table = Table::new(Rc::new(schema), bpm.clone());
            // insert
            for idx in 0..1000 {
                table.insert(&[Datum::Int(Some(idx))]).unwrap()
            }
            // validate
            table.iter().sorted().enumerate().for_each(|(idx, datums)| {
                assert_eq!(Datum::Int(Some(idx as i32)), datums[0]);
            });
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_create_open() {
        let (filename, page_id) = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[(DataType::VarChar, "v1".to_string(), false)]);
            let table = Table::new(Rc::new(schema), bpm.clone());
            (filename, table.page_id)
        };
        let filename = {
            let bpm = Rc::new(RefCell::new(BufferPoolManager::new_with_name(
                5,
                filename.clone(),
            )));
            let table = Table::open(page_id, bpm.clone());
            assert_eq!(table.schema.len(), 1);
            filename
        };
        remove_file(filename).unwrap();
    }
}
