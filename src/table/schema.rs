use crate::table::DataType;
use itertools::Itertools;
use std::rc::Rc;
use std::slice::{Iter, IterMut};

#[derive(Debug, PartialEq)]
pub struct Column {
    pub offset: usize,
    pub data_type: DataType,
    pub desc: String,
}

pub type SchemaRef = Rc<Schema>;

impl Column {
    pub fn new(offset: usize, data_type: DataType, desc: String) -> Self {
        Column {
            offset,
            data_type,
            desc,
        }
    }
    pub fn from_slice(type_and_names: &[(DataType, String)]) -> Vec<Self> {
        let mut offset = 0;
        type_and_names
            .iter()
            .map(|(data_type, desc)| {
                offset += data_type.width().unwrap_or(8);
                Column::new(offset, *data_type, desc.clone())
            })
            .collect_vec()
    }
}

#[derive(Debug)]
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
    pub fn index_of(&self, field_name: String) -> Option<usize> {
        self.columns.iter().position(|c| c.desc == field_name)
    }
    pub fn from_slice(type_and_names: &[(DataType, String)]) -> Self {
        Schema::new(Column::from_slice(type_and_names))
    }
    pub fn iter(&self) -> Iter<Column> {
        self.columns.iter()
    }
    pub fn iter_mut(&mut self) -> IterMut<Column> {
        self.columns.iter_mut()
    }
    pub fn type_at(&self, idx: usize) -> DataType {
        self.columns[idx].data_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_from_slice() {
        let type_and_names = vec![
            (DataType::new_int(false), "v1".to_string()),
            (DataType::new_char(20, false), "v2".to_string()),
            (DataType::new_varchar(false), "v3".to_string()),
        ];
        let schema = Schema::from_slice(type_and_names.as_slice());
        let columns = schema.columns;
        assert_eq!(
            columns,
            vec![
                Column::new(4, DataType::new_int(false), "v1".to_string()),
                Column::new(
                    24,
                    DataType::new_char(20, false),
                    "v2".to_string(),
                ),
                Column::new(32, DataType::new_varchar(false), "v3".to_string()),
            ]
        );
    }
}
