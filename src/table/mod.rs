use crate::datum::{DataType, Datum};
use crate::storage::{BufferPoolManagerRef, PageID, PageRef, StorageError};
use itertools::Itertools;
use prettytable::{Cell, Row, Table as PrintTable};
use std::convert::TryInto;
use std::fmt;
use std::rc::Rc;
use thiserror::Error;

mod schema;
mod slice;

pub use schema::{Column, Schema, SchemaRef};
pub use slice::Slice;

///
/// Table Format:
///
///     | page_id_of_first_slice | Schema |
///
/// Schema Format:
///
///     | desc_len | desc | DataType |
///
/// DataType Format:
///
///     | data_type_id | related_data |
///

pub struct Table {
    pub schema: SchemaRef,
    page: PageRef,
    bpm: BufferPoolManagerRef,
}

impl Drop for Table {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap()
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut table = PrintTable::new();
        let header = self
            .schema
            .iter()
            .map(|c| Cell::new(c.desc.as_str()))
            .collect_vec();
        table.add_row(Row::new(header));
        self.iter().for_each(|tuple| {
            let tuple = tuple
                .iter()
                .map(|d| Cell::new(d.to_string().as_str()))
                .collect_vec();
            table.add_row(Row::new(tuple));
        });
        write!(f, "{}", table)
    }
}

pub struct TableIter {
    idx: usize,
    slice: Slice,
    bpm: BufferPoolManagerRef,
    pub schema: SchemaRef,
}

impl Iterator for TableIter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.slice.get_num_tuple() {
            let ret = Some(self.slice.at(self.idx).unwrap());
            self.idx += 1;
            ret
        } else if let Some(page_id_of_next_slice) = self.slice.get_next_page_id() {
            self.slice = Slice::open(self.bpm.clone(), self.schema.clone(), page_id_of_next_slice);
            self.idx = 1;
            Some(self.slice.at(0).unwrap())
        } else {
            None
        }
    }
}

#[allow(dead_code)]
impl Table {
    /// open a table with page_id
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
            cols.push((dat, name));
        }
        let schema = Rc::new(Schema::from_slice(cols.as_slice()));
        Self { schema, bpm, page }
    }
    /// create a table
    pub fn new(schema: SchemaRef, bpm: BufferPoolManagerRef) -> Self {
        // alloc table page
        let page = bpm.borrow_mut().alloc().unwrap();
        // alloc slice page
        let slice = Slice::new(bpm.clone(), schema.clone());
        let page_id_of_root_slice = slice.get_page_id();
        // set page_id_of_first_slice
        page.borrow_mut().buffer[0..4]
            .copy_from_slice(&(page_id_of_root_slice as u32).to_le_bytes());
        // set schema
        let mut offset = 4;
        schema.iter().for_each(|col| {
            let desc_len = col.desc.len();
            let buffer = &mut page.borrow_mut().buffer;
            buffer[offset..offset + 4].copy_from_slice(&(desc_len as u32).to_le_bytes());
            offset += 4;
            buffer[offset..offset + desc_len].copy_from_slice(col.desc.as_bytes());
            offset += desc_len;
            buffer[offset..offset + 5].copy_from_slice(&col.data_type.as_bytes());
            offset += 5;
            buffer[offset..offset + 4].copy_from_slice(&[0u8; 4]);
        });
        // mark dirty
        page.borrow_mut().is_dirty = true;
        Self { schema, bpm, page }
    }
    pub fn get_page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }
    pub fn get_page_id_of_first_slice(&self) -> PageID {
        u32::from_le_bytes(self.page.borrow().buffer[0..4].try_into().unwrap()) as usize
    }
    pub fn set_page_id_of_first_slice(&self, page_id: PageID) {
        self.page.borrow_mut().buffer[0..4].copy_from_slice(&(page_id as u32).to_le_bytes());
        self.page.borrow_mut().is_dirty = true;
    }
    pub fn insert(&mut self, datums: Vec<Datum>) -> Result<(), TableError> {
        let page_id_of_first_slice = self.get_page_id_of_first_slice();
        let mut slice = Slice::open(
            self.bpm.clone(),
            self.schema.clone(),
            page_id_of_first_slice,
        );
        if slice.ok_to_add(&datums) {
            slice.add(datums).unwrap();
            Ok(())
        } else {
            let mut new_slice = Slice::new(self.bpm.clone(), self.schema.clone());
            new_slice.add(datums).unwrap();
            self.set_page_id_of_first_slice(new_slice.get_page_id());
            new_slice.set_next_page_id(Some(slice.get_page_id()));
            Ok(())
        }
    }
    pub fn iter(&self) -> TableIter {
        let page_id_of_first_slice = self.get_page_id_of_first_slice();
        let slice = Slice::open(
            self.bpm.clone(),
            self.schema.clone(),
            page_id_of_first_slice,
        );
        TableIter {
            idx: 0,
            slice,
            bpm: self.bpm.clone(),
            schema: self.schema.clone(),
        }
    }
    pub fn from_slice(slices: Vec<Slice>, bpm: BufferPoolManagerRef) -> Self {
        let mut table = Table::new(slices[0].schema.clone(), bpm);
        slices.iter().for_each(|s| {
            let num_tuple = s.get_num_tuple();
            for idx in 0..num_tuple {
                let tuple = s.at(idx).unwrap();
                table.insert(tuple).unwrap();
            }
        });
        table
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
            let schema = Schema::from_slice(&[(DataType::new_int(false), "v1".to_string())]);
            let mut table = Table::new(Rc::new(schema), bpm.clone());
            // insert
            for idx in 0..1000 {
                table.insert(vec![Datum::Int(Some(idx))]).unwrap()
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
            let schema = Schema::from_slice(&[(DataType::new_varchar(false), "v1".to_string())]);
            let table = Table::new(Rc::new(schema), bpm.clone());
            (filename, table.get_page_id())
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
