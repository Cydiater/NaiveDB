use crate::datum::{DataType, Datum};
use crate::index::RecordID;
use crate::storage::{
    BufferPoolManagerRef, PageID, PageRef, SlottedPage, SlottedPageError, StorageError,
};
use itertools::Itertools;
use prettytable::{Cell, Row, Table as PrintTable};

use std::fmt;
use std::rc::Rc;
use thiserror::Error;

mod schema;
mod slice;

pub use schema::{Column, Schema, SchemaError, SchemaRef};
pub use slice::{Slice, SlotIter, TupleIter};

#[derive(Copy, Clone)]
pub struct TableMeta {
    pub page_id_of_first_slice: PageID,
    pub page_id_of_primary_index: Option<PageID>,
}

type TablePage = SlottedPage<TableMeta, ()>;

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
            .columns
            .iter()
            .map(|c| Cell::new(c.desc.as_str()))
            .collect_vec();
        table.add_row(Row::new(header));
        self.iter()
            .flat_map(|s| s.tuple_iter().collect_vec())
            .for_each(|tuple| {
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
    slice: Option<Slice>,
    bpm: BufferPoolManagerRef,
    schema: SchemaRef,
}

impl Iterator for TableIter {
    type Item = Slice;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(slice) = &self.slice {
            if let Some(next_page_id) = slice.meta().unwrap().next_page_id {
                let next_slice = Slice::open(self.bpm.clone(), self.schema.clone(), next_page_id);
                std::mem::replace(&mut self.slice, Some(next_slice))
            } else {
                std::mem::replace(&mut self.slice, None)
            }
        } else {
            None
        }
    }
}

impl Table {
    fn table_page(&self) -> &TablePage {
        unsafe { &*(self.page.borrow().buffer.as_ptr() as *const TablePage) }
    }
    fn table_page_mut(&mut self) -> &mut TablePage {
        self.page.borrow_mut().is_dirty = true;
        unsafe { &mut *(self.page.borrow_mut().buffer.as_mut_ptr() as *mut TablePage) }
    }
    pub fn meta(&self) -> &TableMeta {
        self.table_page().meta()
    }
    pub fn meta_mut(&mut self) -> &mut TableMeta {
        self.table_page_mut().meta_mut()
    }
    /// open a table with page_id
    pub fn open(page_id: PageID, bpm: BufferPoolManagerRef) -> Self {
        // fetch page from bpm
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        unsafe {
            let table_page = &*(page.borrow().buffer.as_ptr() as *const TablePage);
            let schema = Rc::new(Schema::from_bytes(table_page.data_at(0)));
            Self { schema, bpm, page }
        }
    }
    /// create a table
    pub fn new(schema: SchemaRef, bpm: BufferPoolManagerRef) -> Self {
        // alloc table page
        let page = bpm.borrow_mut().alloc().unwrap();
        // alloc slice page
        let slice = Slice::new(bpm.clone(), schema.clone());
        unsafe {
            let table_page = &mut *(page.borrow_mut().buffer.as_mut_ptr() as *mut TablePage);
            table_page.reset(&TableMeta {
                page_id_of_first_slice: slice.page_id(),
                page_id_of_primary_index: None,
            });
            table_page.append(&(), &schema.to_bytes()).unwrap();
        }
        page.borrow_mut().is_dirty = true;
        Self { schema, bpm, page }
    }
    pub fn set_schema(&mut self, schema: SchemaRef) {
        self.page.borrow_mut().is_dirty = true;
        let table_page_mut = self.table_page_mut();
        table_page_mut.remove_at(0).unwrap();
        table_page_mut
            .insert_at(0, &(), &schema.to_bytes())
            .unwrap();
        self.schema = schema;
    }
    pub fn page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }
    pub fn insert(&mut self, datums: Vec<Datum>) -> Result<RecordID, TableError> {
        let page_id_of_first_slice = self.meta().page_id_of_first_slice;
        let mut slice = Slice::open(
            self.bpm.clone(),
            self.schema.clone(),
            page_id_of_first_slice,
        );
        if let Ok(record_id) = slice.insert(&datums) {
            Ok(record_id)
        } else {
            let mut new_slice = Slice::new(self.bpm.clone(), self.schema.clone());
            self.meta_mut().page_id_of_first_slice = new_slice.page_id();
            new_slice.meta_mut()?.next_page_id = Some(slice.page_id());
            let record_id = new_slice.insert(&datums)?;
            Ok(record_id)
        }
    }
    pub fn iter(&self) -> TableIter {
        let page_id_of_first_slice = self.meta().page_id_of_first_slice;
        let slice = Slice::open(
            self.bpm.clone(),
            self.schema.clone(),
            page_id_of_first_slice,
        );
        TableIter {
            slice: Some(slice),
            bpm: self.bpm.clone(),
            schema: self.schema.clone(),
        }
    }
    pub fn tuple_at(&self, record_id: RecordID) -> Option<Vec<Datum>> {
        let slice = Slice::open(self.bpm.clone(), self.schema.clone(), record_id.0);
        Some(slice.tuple_at(record_id.1).unwrap())
    }
    pub fn set_ref_cnt_of(&mut self, record_id: RecordID, cnt: usize) -> Result<(), TableError> {
        let mut slice = Slice::open(self.bpm.clone(), self.schema.clone(), record_id.0);
        slice.set_ref_cnt_at(record_id.1, cnt)
    }
    pub fn ref_cnt_of(&self, record_id: RecordID) -> Result<usize, TableError> {
        let slice = Slice::open(self.bpm.clone(), self.schema.clone(), record_id.0);
        slice.ref_cnt_at(record_id.1)
    }
    pub fn from_slice(slices: Vec<Slice>, schema: SchemaRef, bpm: BufferPoolManagerRef) -> Self {
        let mut table = Table::new(schema, bpm);
        slices.iter().for_each(|s| {
            for tuple in s.tuple_iter() {
                table.insert(tuple).unwrap();
            }
        });
        table
    }
    pub fn into_slice(self) -> Vec<Slice> {
        let mut slices = vec![];
        let mut page_id = Some(self.meta().page_id_of_first_slice);
        while page_id.is_some() {
            let slice = Slice::open(self.bpm.clone(), self.schema.clone(), page_id.unwrap());
            let next_page_id = slice.meta().unwrap().next_page_id;
            slices.push(slice);
            page_id = next_page_id;
        }
        slices
    }
    pub fn remove(&mut self, record_id: RecordID) -> Result<(), TableError> {
        let mut slice = Slice::open(self.bpm.clone(), self.schema.clone(), record_id.0);
        if slice.ref_cnt_at(record_id.1)? > 0 {
            return Err(TableError::RemovingReferedTuple);
        }
        slice.remove_at(record_id.1)
    }
    pub fn erase(self) {
        let bpm = self.bpm.clone();
        let table_page_id = self.page.borrow().page_id.unwrap();
        let slice_page_ids = self
            .into_slice()
            .into_iter()
            .map(|s| s.page_id())
            .collect_vec();
        for page_id in slice_page_ids
            .into_iter()
            .chain(std::iter::once(table_page_id))
        {
            bpm.borrow_mut().free(page_id).unwrap();
        }
    }
}

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
    #[error("Slice Index Out of Bound")]
    SliceIndexOutOfBound,
    #[error("Delete Tuple That Already Deleted")]
    AlreadyDeleted,
    #[error("SlicePage: {0}")]
    SlicePage(#[from] SlottedPageError),
    #[error("Removing Refered Tuple")]
    RemovingReferedTuple,
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
            let schema =
                Schema::from_type_and_names(&[(DataType::new_as_int(false), "v1".to_string())]);
            let mut table = Table::new(Rc::new(schema), bpm);
            // insert
            for idx in 0..1000 {
                let _ = table.insert(vec![Datum::Int(Some(idx))]).unwrap();
            }
            // validate
            table
                .iter()
                .flat_map(|s| s.tuple_iter().collect_vec())
                .sorted()
                .enumerate()
                .for_each(|(idx, datums)| {
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
            let schema =
                Schema::from_type_and_names(&[(DataType::new_as_varchar(false), "v1".to_string())]);
            let table = Table::new(Rc::new(schema), bpm);
            (filename, table.page_id())
        };
        let filename = {
            let bpm = Rc::new(RefCell::new(BufferPoolManager::new_with_name(
                5,
                filename.clone(),
            )));
            let table = Table::open(page_id, bpm);
            assert_eq!(table.schema.columns.len(), 1);
            filename
        };
        remove_file(filename).unwrap();
    }
}
