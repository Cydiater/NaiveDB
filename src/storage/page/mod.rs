use super::*;
use std::cell::RefCell;
use std::rc::Rc;

mod db;

use db::DatabasePageBuffer;

#[allow(dead_code)]
#[derive(Clone)]
pub enum PageBuffer {
    /// just a continuous memory
    Raw([u8; PAGE_SIZE]),
    /// root page and the first page of storage,
    /// store information of databases
    DataBase(DatabasePageBuffer),
}

#[allow(dead_code)]
impl PageBuffer {
    pub fn into_raw(self) -> [u8; PAGE_SIZE] {
        match self {
            Self::Raw(raw) => raw,
            Self::DataBase(db) => db.into_raw(),
        }
    }
    pub fn as_raw(&self) -> &[u8; PAGE_SIZE] {
        match self {
            Self::Raw(raw) => raw,
            Self::DataBase(db) => db.as_raw(),
        }
    }
    pub fn as_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        match self {
            Self::Raw(raw) => raw,
            Self::DataBase(db) => db.as_mut(),
        }
    }
    pub fn into_database(self) -> Self {
        let buf = self.into_raw();
        PageBuffer::DataBase(DatabasePageBuffer::from_raw(buf))
    }
}

#[derive(Clone)]
pub struct Page {
    pub page_id: Option<PageID>,
    pub is_dirty: bool,
    pub pin_count: usize,
    pub buffer: PageBuffer,
}

pub type PageRef = Rc<RefCell<Page>>;

impl Page {
    pub fn new() -> Self {
        Page {
            page_id: None,
            is_dirty: false,
            pin_count: 0,
            buffer: PageBuffer::Raw([0; PAGE_SIZE]),
        }
    }

    #[allow(dead_code)]
    /// this method will not clear the content in the buffer
    pub fn clear(&mut self) {
        self.page_id = None;
        self.is_dirty = false;
    }
}
