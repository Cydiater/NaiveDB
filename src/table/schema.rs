use crate::table::DataType;
use std::slice::{Iter, IterMut};

pub struct Column {
    pub offset: usize,
    pub data_type: DataType,
    pub desc: String,
}

#[allow(dead_code)]
impl Column {
    pub fn new(offset: usize, data_type: DataType, desc: String) -> Self {
        Column {
            offset,
            data_type,
            desc,
        }
    }
    pub fn from_slice(_type_and_names: &[(DataType, String)]) -> Vec<Self> {
        todo!();
    }
}

pub struct Schema {
    columns: Vec<Column>,
}

#[allow(dead_code)]
impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
    pub fn len(&self) -> usize {
        self.columns.len()
    }
    pub fn iter(&self) -> Iter<Column> {
        self.columns.iter()
    }
    pub fn iter_mut(&mut self) -> IterMut<Column> {
        self.columns.iter_mut()
    }
}
