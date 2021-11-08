use crate::table::DataType;
use std::slice::Iter;

pub struct Column {
    pub offset: usize,
    pub data_type: DataType,
}

pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn len(&self) -> usize {
        self.columns.len()
    }
    pub fn iter(&self) -> Iter<Column> {
        self.columns.iter()
    }
}
