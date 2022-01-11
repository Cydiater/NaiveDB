use crate::expr::{ColumnRefExpr, ExprImpl};
use crate::table::DataType;
use itertools::Itertools;
use std::convert::TryInto;
use std::rc::Rc;
use thiserror::Error;

///
/// Schema Format:
///
///     | num_column | Column[0] | Column[1] | ... |
///     | len_unique | unique_payload |
///
/// Column Format:
///
///     | offset | len_desc | desc_content | DataType
///     | Primary / Foreign / None | page_id_of_ref_table | idx_of_ref_column
///

#[derive(Debug, PartialEq, Clone)]
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
    pub fn from_type_and_names(type_and_names: &[(DataType, String)]) -> Vec<Self> {
        let mut offset = 0;
        type_and_names
            .iter()
            .map(|(data_type, desc)| {
                offset += data_type.width_of_value().unwrap_or(8);
                Column::new(offset, *data_type, desc.clone())
            })
            .collect_vec()
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        vec![
            self.offset.to_le_bytes().to_vec(),
            self.data_type.to_bytes().to_vec(),
            self.desc.len().to_le_bytes().to_vec(),
            self.desc.as_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect_vec()
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let offset = usize::from_le_bytes(bytes[0..8].try_into().unwrap());
        let data_type = DataType::from_bytes(bytes[8..9].try_into().unwrap()).unwrap();
        let desc_len = usize::from_le_bytes(bytes[9..17].try_into().unwrap());
        let desc = String::from_utf8(bytes[17..17 + desc_len].to_vec()).unwrap();
        Self {
            offset,
            data_type,
            desc,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub unique: Vec<Vec<usize>>,
    pub primary: Vec<usize>,
    pub foreign: Vec<(usize, Vec<(usize, usize)>)>,
}

impl Schema {
    pub fn from_type_and_names(type_and_names: &[(DataType, String)]) -> Self {
        let columns = Column::from_type_and_names(type_and_names);
        Self {
            columns,
            unique: vec![],
            primary: vec![],
            foreign: vec![],
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        vec![
            self.columns.len().to_le_bytes().to_vec(),
            self.columns
                .iter()
                .map(|c| c.to_bytes())
                .flatten()
                .collect_vec(),
            self.unique.len().to_le_bytes().to_vec(),
            self.unique
                .iter()
                .map(|u| {
                    vec![
                        u.len().to_le_bytes().to_vec(),
                        u.iter()
                            .map(|n| n.to_le_bytes().to_vec())
                            .flatten()
                            .collect_vec(),
                    ]
                    .into_iter()
                    .flatten()
                    .collect_vec()
                })
                .flatten()
                .collect_vec(),
            self.primary.len().to_le_bytes().to_vec(),
            self.primary
                .iter()
                .map(|n| n.to_le_bytes())
                .flatten()
                .collect_vec(),
            self.foreign.len().to_le_bytes().to_vec(),
            self.foreign
                .iter()
                .map(|(ref_page_id, ref_vec)| {
                    vec![
                        ref_page_id.to_le_bytes().to_vec(),
                        ref_vec.len().to_le_bytes().to_vec(),
                        ref_vec
                            .iter()
                            .map(|(src, dst)| {
                                vec![src.to_le_bytes().to_vec(), dst.to_le_bytes().to_vec()]
                                    .into_iter()
                                    .flatten()
                                    .collect_vec()
                            })
                            .flatten()
                            .collect_vec(),
                    ]
                    .into_iter()
                    .flatten()
                    .collect_vec()
                })
                .flatten()
                .collect_vec(),
        ]
        .into_iter()
        .flatten()
        .collect_vec()
    }
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut offset = 0;
        let columns_len = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let mut columns = vec![];
        for _ in 0..columns_len {
            let column = Column::from_bytes(&bytes[offset..]);
            offset += 17 + column.desc.len();
            columns.push(column);
        }
        let mut unique = vec![];
        let unique_len = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        for _ in 0..unique_len {
            let len = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let mut u = vec![];
            for _ in 0..len {
                let idx = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                u.push(idx);
            }
            unique.push(u);
        }
        let mut primary = vec![];
        let primary_len = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        for _ in 0..primary_len {
            let p = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            primary.push(p);
            offset += 8;
        }
        let mut foreign = vec![];
        let foreign_len = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;
        for _ in 0..foreign_len {
            let page_id = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let vec_len = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let mut vec = vec![];
            for _ in 0..vec_len {
                let src = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                let dst = usize::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
                offset += 8;
                vec.push((src, dst));
            }
            foreign.push((page_id, vec));
        }
        Self {
            columns,
            unique,
            primary,
            foreign,
        }
    }
    pub fn project_by(&self, idxes: &[usize]) -> Vec<ExprImpl> {
        idxes
            .iter()
            .map(|idx| {
                ExprImpl::ColumnRef(ColumnRefExpr::new(
                    *idx,
                    self.columns[*idx].data_type,
                    self.columns[*idx].desc.clone(),
                ))
            })
            .collect_vec()
    }
    pub fn project_by_primary(&self) -> Vec<ExprImpl> {
        self.project_by(&self.primary)
    }
    pub fn from_exprs(exprs: &[ExprImpl]) -> Self {
        let type_and_names = exprs
            .iter()
            .map(|e| (e.return_type(), e.to_string()))
            .collect_vec();
        Self::from_type_and_names(&type_and_names)
    }
    pub fn to_type_and_names(&self) -> Vec<(DataType, String)> {
        self.columns
            .iter()
            .map(|c| (c.data_type, c.desc.clone()))
            .collect_vec()
    }
    pub fn index_by_column_name(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.desc == column_name)
            .map(|(idx, _)| idx)
    }
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Column Not Found")]
    ColumnNotFound,
    #[error("Duplicated Primary")]
    DuplicatedPrimary,
    #[error("Primary Not Found")]
    PrimaryNotFound,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_and_from_bytes() {
        let mut schema = Schema::from_type_and_names(&[
            (DataType::new_as_int(false), "v_int".into()),
            (DataType::new_as_varchar(true), "v_varchar".into()),
        ]);
        schema.primary = vec![0, 1];
        schema.unique.push(vec![1]);
        schema.foreign.push((1, vec![(1, 0), (0, 2)]));
        let bytes = schema.to_bytes();
        assert_eq!(Schema::from_bytes(&bytes), schema,);
    }
}
