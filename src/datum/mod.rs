use crate::table::Schema;
use chrono::{Datelike, NaiveDate};
use itertools::Itertools;
use ordered_float::NotNan;
use std::convert::{From, TryInto};
use std::fmt;
use std::ops::{Add, Div};

pub use types::DataType;

mod types;

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub enum Datum {
    Int(Option<i32>),
    VarChar(Option<String>),
    Bool(Option<bool>),
    Float(Option<NotNan<f32>>),
    Date(Option<NaiveDate>),
}

impl Add for Datum {
    type Output = Datum;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(Some(lhs)), Self::Int(Some(rhs))) => (lhs + rhs).into(),
            (Self::Float(Some(lhs)), Self::Float(Some(rhs))) => (lhs + rhs).into(),
            _ => todo!(),
        }
    }
}

impl Div<usize> for Datum {
    type Output = Datum;

    fn div(self, by: usize) -> Self {
        match self {
            Self::Int(Some(v)) => (v / (by as i32)).into(),
            Self::Float(Some(v)) => (v / (by as f32)).into(),
            _ => todo!(),
        }
    }
}

impl From<i32> for Datum {
    fn from(i: i32) -> Datum {
        Datum::Int(Some(i))
    }
}

impl From<&str> for Datum {
    fn from(s: &str) -> Datum {
        Datum::VarChar(Some(s.to_owned()))
    }
}

impl From<bool> for Datum {
    fn from(b: bool) -> Datum {
        Datum::Bool(Some(b))
    }
}

impl From<NotNan<f32>> for Datum {
    fn from(f: NotNan<f32>) -> Datum {
        Datum::Float(Some(f))
    }
}

impl From<f32> for Datum {
    fn from(f: f32) -> Datum {
        Datum::Float(Some(f.try_into().unwrap()))
    }
}

impl From<NaiveDate> for Datum {
    fn from(d: NaiveDate) -> Datum {
        Datum::Date(Some(d))
    }
}

impl Datum {
    pub fn byte_size_inlined(&self) -> usize {
        match self {
            Self::Int(_) => 5,
            Self::Float(_) => 5,
            Self::Bool(_) => 2,
            Self::Date(_) => 7,
            Self::VarChar(_) => 9,
        }
    }
    pub fn is_inlined(&self) -> bool {
        match self {
            Self::Int(_) | Self::Bool(_) | Self::Float(_) | Self::Date(_) => true,
            Self::VarChar(_) => false,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Int(v) => {
                if let Some(v) = v {
                    [vec![1u8], v.to_le_bytes().to_vec()]
                        .iter()
                        .flatten()
                        .cloned()
                        .collect_vec()
                } else {
                    vec![0u8; 5]
                }
            }
            Self::Float(v) => {
                if let Some(v) = v {
                    [vec![1u8], v.to_le_bytes().to_vec()]
                        .iter()
                        .flatten()
                        .cloned()
                        .collect_vec()
                } else {
                    vec![0u8; 5]
                }
            }
            Self::VarChar(v) => {
                if let Some(v) = v {
                    [
                        vec![1u8],
                        (v.len() as u32).to_le_bytes().to_vec(),
                        v.as_bytes().to_vec(),
                    ]
                    .iter()
                    .flatten()
                    .cloned()
                    .collect_vec()
                } else {
                    vec![0u8]
                }
            }
            Self::Date(v) => {
                if let Some(v) = v {
                    [
                        vec![1u8],
                        (v.year() as u32).to_le_bytes().to_vec(),
                        (v.month() as u8).to_le_bytes().to_vec(),
                        (v.day() as u8).to_le_bytes().to_vec(),
                    ]
                    .iter()
                    .flatten()
                    .cloned()
                    .collect_vec()
                } else {
                    vec![0u8; 7]
                }
            }
            _ => todo!(),
        }
    }
    pub fn bytes_from_tuple(datums: &[Datum]) -> Vec<u8> {
        let mut bytes_fragment = vec![];
        let mut not_inlined_data = Vec::<(usize, &Datum)>::new();
        let mut offset = 0;
        // collect bytes fragments
        for dat in datums {
            if dat.is_inlined() {
                let bytes = dat.to_bytes();
                offset += bytes.len();
                bytes_fragment.push(bytes);
            } else {
                bytes_fragment.push(vec![0u8; 8]);
                not_inlined_data.push((bytes_fragment.len() - 1, dat));
                offset += 8;
            };
        }
        for (idx, dat) in not_inlined_data {
            let bytes = dat.to_bytes();
            let end = offset;
            offset += bytes.len();
            let start = offset;
            bytes_fragment.push(bytes);
            let mut offset_bytes = vec![];
            offset_bytes.extend_from_slice(&(start as u32).to_le_bytes());
            offset_bytes.extend_from_slice(&(end as u32).to_le_bytes());
            bytes_fragment[idx] = offset_bytes;
        }
        bytes_fragment.iter().rev().flatten().cloned().collect_vec()
    }
    pub fn tuple_from_bytes_with_schema(bytes: &[u8], schema: &Schema) -> Vec<Datum> {
        let base_offset = bytes.len();
        let mut datums = vec![];
        for col in schema.columns.iter() {
            let offset = base_offset - col.offset;
            let datum = if col.data_type.is_inlined() {
                let start = offset;
                let end = start + col.data_type.width_of_value().unwrap();
                let bytes = bytes[start..end].to_vec();
                Datum::from_bytes_with_type(&bytes, &col.data_type)
            } else {
                let start = base_offset
                    - u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                let end = base_offset
                    - u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap())
                        as usize;
                let bytes = bytes[start..end].to_vec();
                Datum::from_bytes_with_type(&bytes, &col.data_type)
            };
            datums.push(datum);
        }
        datums
    }
    pub fn from_bytes_with_type(bytes: &[u8], data_type: &DataType) -> Self {
        match data_type {
            DataType::Int(_) => {
                if bytes[0] == 0 {
                    Datum::Int(None)
                } else {
                    Datum::Int(Some(i32::from_le_bytes(bytes[1..5].try_into().unwrap())))
                }
            }
            DataType::VarChar(_) => {
                if bytes[0] == 0 {
                    Datum::VarChar(None)
                } else {
                    let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
                    Datum::VarChar(Some(
                        String::from_utf8(bytes[5..5 + len].try_into().unwrap()).unwrap(),
                    ))
                }
            }
            DataType::Bool(_) => {
                if bytes[0] == 0 {
                    Datum::Bool(None)
                } else {
                    Datum::Bool(Some(bytes[1] != 0))
                }
            }
            DataType::Float(_) => {
                if bytes[0] == 0 {
                    Datum::Float(None)
                } else {
                    Datum::Float(Some(
                        f32::from_le_bytes(bytes[1..5].try_into().unwrap())
                            .try_into()
                            .unwrap(),
                    ))
                }
            }
            DataType::Date(_) => {
                if bytes[0] == 0 {
                    Datum::Date(None)
                } else {
                    Datum::Date(Some(NaiveDate::from_ymd(
                        u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as i32,
                        u8::from_le_bytes(bytes[5..6].try_into().unwrap()) as u32,
                        u8::from_le_bytes(bytes[6..7].try_into().unwrap()) as u32,
                    )))
                }
            }
        }
    }
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Int(Some(d)) => d.to_string(),
                Self::VarChar(Some(s)) => s.to_string(),
                Self::Bool(Some(s)) => s.to_string(),
                Self::Date(Some(d)) => d.to_string(),
                Self::Float(Some(f)) => f.to_string(),
                _ => String::from("NULL"),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::DataType;
    use crate::table::Schema;
    use std::rc::Rc;

    #[test]
    fn test_from_to_bytes_with_schema() {
        let schema = Schema::from_type_and_names(&[
            (DataType::new_as_int(false), "v1".to_string()),
            (DataType::new_as_varchar(false), "v2".to_string()),
        ]);
        let schema = Rc::new(schema);
        let datums = vec![Datum::Int(Some(1)), Datum::VarChar(Some("foo".to_string()))];
        let bytes = Datum::bytes_from_tuple(&datums);
        let datums_to_check = Datum::tuple_from_bytes_with_schema(bytes.as_slice(), &schema);
        assert_eq!(datums, datums_to_check);
    }
}
