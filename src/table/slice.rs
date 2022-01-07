use crate::datum::{DataType, Datum};
use crate::storage::{
    BufferPoolManagerRef, KeyDataIter, PageID, PageRef, SlotIndexIter, SlottedPage,
};
use crate::table::{Schema, SchemaRef, TableError};
use itertools::Itertools;
use prettytable::{Cell, Row, Table};
use std::fmt;
use std::rc::Rc;

///
/// Slice Format:
///
///     | Meta | offset1 | offset2 |  ......
///                                   ...... | data2 | data1 |
///
/// Meta Format:
///
///     | next_page_id | num_tuple | head | tail |
///
/// Note that:
///     
///     - next_page_id is None if the value is zero,
///
pub struct Slice {
    bpm: BufferPoolManagerRef,
    page: PageRef,
    pub schema: SchemaRef,
}

#[derive(Clone, Copy)]
pub struct SliceMeta {
    pub next_page_id: Option<PageID>,
}

type SlicePage = SlottedPage<SliceMeta, ()>;

pub struct TupleIter<'page> {
    key_data_iter: KeyDataIter<'page, ()>,
    pub next_page_id: Option<PageID>,
    schema: SchemaRef,
}

pub struct SlotIter<'page> {
    index_iter: SlotIndexIter<'page>,
}

impl<'page> TupleIter<'page> {
    pub fn new(
        key_data_iter: KeyDataIter<'page, ()>,
        next_page_id: Option<PageID>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            key_data_iter,
            next_page_id,
            schema,
        }
    }
}

impl<'page> SlotIter<'page> {
    pub fn new(index_iter: SlotIndexIter<'page>) -> Self {
        Self { index_iter }
    }
}

impl<'page> Iterator for SlotIter<'page> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        self.index_iter.next()
    }
}

impl<'page> Iterator for TupleIter<'page> {
    type Item = Vec<Datum>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((_, data)) = self.key_data_iter.next() {
            Some(Datum::from_bytes_and_schema(self.schema.as_ref(), data))
        } else {
            None
        }
    }
}

impl Drop for Slice {
    fn drop(&mut self) {
        let page_id = self.page.borrow().page_id.unwrap();
        self.bpm.borrow_mut().unpin(page_id).unwrap();
    }
}

impl Slice {
    pub fn new_as_message(
        bpm: BufferPoolManagerRef,
        header: &str,
        message: &str,
    ) -> Result<Self, TableError> {
        let schema = Schema::from_slice(&[(DataType::new_varchar(false), header.to_owned())]);
        let mut slice = Self::new(bpm, Rc::new(schema));
        slice.insert(&[Datum::VarChar(Some(message.to_owned()))])?;
        Ok(slice)
    }

    pub fn new_as_count(
        bpm: BufferPoolManagerRef,
        header: &str,
        cnt: usize,
    ) -> Result<Self, TableError> {
        let schema = Schema::from_slice(&[(DataType::new_int(false), header.to_owned())]);
        let mut slice = Self::new(bpm, Rc::new(schema));
        slice.insert(&[Datum::Int(Some(cnt as i32))])?;
        Ok(slice)
    }

    pub fn new(bpm: BufferPoolManagerRef, schema: SchemaRef) -> Self {
        let page = bpm.borrow_mut().alloc().unwrap();
        unsafe {
            let slotted = &mut *(page.borrow_mut().buffer.as_mut_ptr() as *mut SlicePage);
            slotted.reset(&SliceMeta { next_page_id: None });
        }
        // mark dirty
        page.borrow_mut().is_dirty = true;
        Self { page, bpm, schema }
    }

    pub fn open(bpm: BufferPoolManagerRef, schema: SchemaRef, page_id: PageID) -> Self {
        let page = bpm.borrow_mut().fetch(page_id).unwrap();
        Self { page, bpm, schema }
    }

    pub fn page_id(&self) -> PageID {
        self.page.borrow().page_id.unwrap()
    }

    fn slice_page(&self) -> &SlicePage {
        unsafe { &*(self.page.borrow().buffer.as_ptr() as *const SlicePage) }
    }

    fn slice_page_mut(&mut self) -> &mut SlicePage {
        self.page.borrow_mut().is_dirty = true;
        unsafe { &mut *(self.page.borrow_mut().buffer.as_mut_ptr() as *mut SlicePage) }
    }

    pub fn meta(&self) -> Result<&SliceMeta, TableError> {
        let slice_page = self.slice_page();
        Ok(slice_page.meta())
    }

    pub fn meta_mut(&mut self) -> Result<&mut SliceMeta, TableError> {
        let slice_page = self.slice_page_mut();
        Ok(slice_page.meta_mut())
    }

    pub fn insert(&mut self, tuple: &[Datum]) -> Result<(usize, usize), TableError> {
        let page_id = self.page_id();
        let schema = self.schema.clone();
        let slice_page = self.slice_page_mut();
        let slot_id =
            slice_page.insert(&(), &Datum::to_bytes_with_schema(tuple, schema.as_ref()))?;
        Ok((page_id, slot_id))
    }

    pub fn remove_at(&mut self, idx: usize) -> Result<(), TableError> {
        let slice_page = self.slice_page_mut();
        slice_page.remove_at(idx)?;
        Ok(())
    }

    pub fn tuple_at(&self, idx: usize) -> Result<Vec<Datum>, TableError> {
        let slice_page = self.slice_page();
        Ok(Datum::from_bytes_and_schema(
            self.schema.as_ref(),
            slice_page.data_at(idx),
        ))
    }

    pub fn tuple_iter(&self) -> TupleIter {
        let slice_page = self.slice_page();
        TupleIter::new(
            slice_page.key_data_iter(),
            slice_page.meta().next_page_id,
            self.schema.clone(),
        )
    }

    pub fn slot_iter(&self) -> SlotIter {
        let slice_page = self.slice_page();
        SlotIter::new(slice_page.idx_iter())
    }

    pub fn count(&self) -> usize {
        let slice_page = self.slice_page();
        slice_page.count()
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
        for tuple in self.tuple_iter() {
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
    use crate::datum::DataType;
    use crate::storage::BufferPoolManager;
    use crate::table::Schema;
    use std::fs::remove_file;

    #[test]
    fn test_simple_add_get() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_varchar(false), "v2".to_string()),
            ]);
            let tuple1 = vec![20.into(), "hello".into()];
            let tuple2 = vec![30.into(), "world".into()];
            let tuple3 = vec![40.into(), "foo".into()];
            let page_id = {
                let mut slice = Slice::new(bpm.clone(), Rc::new(schema));
                slice.insert(tuple1.as_slice()).unwrap();
                slice.insert(tuple2.as_slice()).unwrap();
                assert_eq!(slice.tuple_at(0).unwrap(), tuple1);
                assert_eq!(slice.tuple_at(1).unwrap(), tuple2);
                slice.page_id()
            };
            // refetch
            let schema = Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_varchar(false), "v2".to_string()),
            ]);
            let mut slice = Slice::open(bpm, Rc::new(schema), page_id);
            slice.insert(tuple3.as_slice()).unwrap();
            assert_eq!(slice.tuple_at(0).unwrap(), tuple1);
            assert_eq!(slice.tuple_at(1).unwrap(), tuple2);
            assert_eq!(slice.tuple_at(2).unwrap(), tuple3);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_remove() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(100);
            let filename = bpm.borrow().filename();
            let schema = Schema::from_slice(&[(DataType::new_int(false), "v1".to_string())]);
            let mut slice = Slice::new(bpm, Rc::new(schema));
            slice.insert(&[Datum::Int(Some(1))]).unwrap();
            slice.insert(&[Datum::Int(Some(2))]).unwrap();
            slice.insert(&[Datum::Int(Some(3))]).unwrap();
            slice.remove_at(1).unwrap();
            assert_eq!(slice.tuple_at(0).unwrap(), vec![Datum::Int(Some(1))]);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_varchar() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Rc::new(Schema::from_slice(&[
                (DataType::new_int(false), "v1".to_string()),
                (DataType::new_varchar(false), "v2".to_string()),
            ]));
            let mut slice = Slice::new(bpm, schema);
            let tuple1 = vec![
                Datum::Int(Some(20)),
                Datum::VarChar(Some("hello".to_string())),
            ];
            let tuple2 = vec![
                Datum::Int(Some(30)),
                Datum::VarChar(Some("world".to_string())),
            ];
            slice.insert(tuple1.as_slice()).unwrap();
            slice.insert(tuple2.as_slice()).unwrap();
            assert_eq!(slice.tuple_at(0).unwrap(), tuple1);
            assert_eq!(slice.tuple_at(1).unwrap(), tuple2);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_simple_message() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let slice = Slice::new_as_message(bpm, "header", "message").unwrap();
            let tuple = slice.tuple_at(0).unwrap();
            assert_eq!(tuple[0], Datum::VarChar(Some("message".to_string())));
            filename
        };
        remove_file(filename).unwrap();
    }
}
