use crate::expr::{ColumnRefExpr, ExprImpl};
use crate::table::DataType;
use itertools::Itertools;
use std::convert::TryInto;
use std::rc::Rc;
use std::slice::{Iter, IterMut};
use thiserror::Error;

///
/// Schema Format:
///
///     | num_column | Column[0] | Column[1] | ... |
///
/// Column Format:
///
///     | offset | len_desc | desc_content | DataType
///     | Primary / Foreign / None | len_of_column_ref_expr | ColumnRefExpr
///

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnConstraint {
    Normal,
    Primary,
    Foreign(ColumnRefExpr),
}

impl ColumnConstraint {
    pub fn size_in_bytes(&self) -> usize {
        match self {
            Self::Normal => 1,
            Self::Primary => 1,
            _ => todo!(),
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Normal => vec![0u8],
            Self::Primary => vec![1u8],
            Self::Foreign(_) => todo!(),
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        match bytes[0] {
            0u8 => Self::Normal,
            1u8 => Self::Primary,
            _ => todo!(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub offset: usize,
    pub data_type: DataType,
    pub desc: String,
    pub constraint: ColumnConstraint,
}

pub type SchemaRef = Rc<Schema>;

impl Column {
    pub fn new(offset: usize, data_type: DataType, desc: String) -> Self {
        Column {
            offset,
            data_type,
            desc,
            constraint: ColumnConstraint::Normal,
        }
    }
    pub fn from_slice(type_and_names: &[(DataType, String)]) -> Vec<Self> {
        let mut offset = 0;
        type_and_names
            .iter()
            .map(|(data_type, desc)| {
                offset += data_type.width_of_value().unwrap_or(8);
                Column::new(offset, *data_type, desc.clone())
            })
            .collect_vec()
    }
    pub fn size_in_bytes(&self) -> usize {
        4 + 4 + self.desc.len() + 5 + self.constraint.size_in_bytes()
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        let desc_len = self.desc.len();
        bytes.extend_from_slice(&(self.offset as u32).to_le_bytes());
        bytes.extend_from_slice(&(desc_len as u32).to_le_bytes());
        bytes.extend_from_slice(self.desc.as_bytes());
        bytes.extend_from_slice(&self.data_type.as_bytes());
        bytes.extend_from_slice(&self.constraint.to_bytes());
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let offset = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let desc_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let desc = String::from_utf8(bytes[8..8 + desc_len].to_vec()).unwrap();
        let data_type =
            DataType::from_bytes(bytes[8 + desc_len..8 + desc_len + 5].try_into().unwrap())
                .unwrap();
        let constraint = ColumnConstraint::from_bytes(&bytes[8 + desc_len + 5..]);
        Self {
            offset,
            desc,
            data_type,
            constraint,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
    pub fn set_primary(&mut self, column_name: String) -> Result<(), SchemaError> {
        let column: Option<&mut Column> = self.columns.iter_mut().find(|c| c.desc == column_name);
        if let Some(column) = column {
            column.constraint = ColumnConstraint::Primary;
            Ok(())
        } else {
            Err(SchemaError::ColumnNotFound)
        }
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
    pub fn to_vec(&self) -> Vec<(DataType, String)> {
        self.columns
            .iter()
            .map(|c| (c.data_type, c.desc.clone()))
            .collect_vec()
    }
    pub fn from_exprs(exprs: &[ExprImpl]) -> Self {
        let type_and_names = exprs
            .iter()
            .map(|e| (e.return_type(), e.name()))
            .collect_vec();
        Self::from_slice(&type_and_names)
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
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        // num_column
        bytes.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in self.columns.iter() {
            bytes.extend_from_slice(col.to_bytes().as_slice());
        }
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let num_column = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let mut columns = vec![];
        let mut offset = 4;
        for _ in 0..num_column {
            let start = offset;
            let column = Column::from_bytes(&bytes[start..]);
            offset += column.size_in_bytes();
            columns.push(column);
        }
        Self { columns }
    }
    pub fn primary_as_exprs(&self) -> Vec<ExprImpl> {
        self.iter()
            .enumerate()
            .filter_map(|(idx, c)| match c.constraint {
                ColumnConstraint::Primary => Some(ExprImpl::ColumnRef(ColumnRefExpr::new(
                    idx,
                    c.data_type,
                    c.desc.clone(),
                ))),
                _ => None,
            })
            .collect_vec()
    }
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("column not found")]
    ColumnNotFound,
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
                Column::new(5, DataType::new_int(false), "v1".to_string()),
                Column::new(26, DataType::new_char(20, false), "v2".to_string(),),
                Column::new(34, DataType::new_varchar(false), "v3".to_string()),
            ]
        );
    }

    #[test]
    fn test_to_from_bytes() {
        let type_and_names = vec![
            (DataType::new_int(false), "v1".to_string()),
            (DataType::new_char(20, false), "v2".to_string()),
            (DataType::new_varchar(false), "v3".to_string()),
        ];
        let mut schema = Schema::from_slice(type_and_names.as_slice());
        schema.set_primary("v1".to_string()).unwrap();
        schema.set_primary("v3".to_string()).unwrap();
        let bytes = schema.to_bytes();
        assert_eq!(Schema::from_bytes(&bytes), schema);
    }
}
