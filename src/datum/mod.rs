use crate::table::SchemaRef;
use pad::PadStr;
use std::convert::TryInto;
use std::fmt;

pub use types::DataType;

mod types;

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub enum Datum {
    Int(Option<i32>),
    Char(Option<String>),
    VarChar(Option<String>),
    Bool(Option<bool>),
}

impl Datum {
    pub fn size_of_bytes(&self, data_type: &DataType) -> usize {
        match (self, data_type) {
            (Self::Int(_), DataType::Int(_)) => 5,
            (Self::Char(_), DataType::Char(t)) => t.width + 1,
            (Self::VarChar(_), DataType::VarChar(_)) => 9,
            _ => todo!(),
        }
    }
    pub fn is_inlined(&self) -> bool {
        match self {
            Self::Int(_) => true,
            Self::Char(_) => true,
            Self::Bool(_) => true,
            Self::VarChar(_) => false,
        }
    }
    pub fn to_bytes(&self, data_type: &DataType) -> Vec<u8> {
        match (self, data_type) {
            (Self::Int(v), DataType::Int(_)) => {
                if let Some(v) = v {
                    let mut bytes = vec![1];
                    bytes.extend_from_slice(&v.to_le_bytes());
                    bytes
                } else {
                    vec![0u8; 5]
                }
            }
            (Self::Char(v), DataType::Char(t)) => {
                if let Some(v) = v {
                    let mut bytes = vec![1];
                    bytes.extend_from_slice(v.with_exact_width(t.width).as_bytes());
                    bytes
                } else {
                    vec![0u8; t.width + 1]
                }
            }
            (Self::VarChar(v), DataType::VarChar(_)) => {
                if let Some(v) = v {
                    let mut bytes = vec![1];
                    bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(v.as_bytes());
                    bytes
                } else {
                    vec![0u8]
                }
            }
            _ => todo!(),
        }
    }
    pub fn to_bytes_with_schema(datums: &[Datum], schema: SchemaRef) -> Vec<u8> {
        let mut bytes_fragment = vec![];
        let mut not_inlined_data = Vec::<(usize, DataType, &Datum)>::new();
        let mut offset = 0;
        // collect bytes fragments
        for (col, dat) in schema.iter().zip(datums) {
            if dat.is_inlined() {
                let bytes = dat.to_bytes(&col.data_type);
                offset += bytes.len();
                bytes_fragment.push(bytes);
            } else {
                bytes_fragment.push(vec![0u8; 8]);
                not_inlined_data.push((bytes_fragment.len() - 1, col.data_type, dat));
                offset += 8;
            };
        }
        for (idx, data_type, dat) in not_inlined_data {
            let bytes = dat.to_bytes(&data_type);
            let end = offset;
            offset += bytes.len();
            let start = offset;
            bytes_fragment.push(bytes);
            let mut offset_bytes = vec![];
            offset_bytes.extend_from_slice(&(start as u32).to_le_bytes());
            offset_bytes.extend_from_slice(&(end as u32).to_le_bytes());
            bytes_fragment[idx] = offset_bytes;
        }
        let bytes = bytes_fragment.iter().rev().fold(vec![], |mut bytes, f| {
            bytes.extend_from_slice(f.as_slice());
            bytes
        });
        bytes
    }
    pub fn from_bytes_and_schema(schema: SchemaRef, bytes: &[u8]) -> Vec<Datum> {
        let base_offset = bytes.len();
        let mut datums = vec![];
        for col in schema.iter() {
            let offset = base_offset - col.offset;
            let datum = if col.data_type.is_inlined() {
                let start = offset;
                let end = start + col.data_type.width_of_value().unwrap();
                let bytes = bytes[start..end].to_vec();
                Datum::from_bytes(&col.data_type, &bytes)
            } else {
                let start = base_offset
                    - u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                let end = base_offset
                    - u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap())
                        as usize;
                let bytes = bytes[start..end].to_vec();
                Datum::from_bytes(&col.data_type, &bytes)
            };
            datums.push(datum);
        }
        datums
    }
    pub fn from_bytes(data_type: &DataType, bytes: &[u8]) -> Self {
        match data_type {
            DataType::Int(_) => {
                if bytes[0] == 0 {
                    Datum::Int(None)
                } else {
                    Datum::Int(Some(i32::from_le_bytes(bytes[1..5].try_into().unwrap())))
                }
            }
            DataType::Char(char_type) => {
                if bytes[0] == 0 {
                    Datum::Char(None)
                } else {
                    Datum::Char(Some(
                        String::from_utf8(bytes[1..char_type.width + 1].try_into().unwrap())
                            .unwrap()
                            .trim_end()
                            .to_string(),
                    ))
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
                Self::Char(Some(s)) => s.to_string(),
                Self::VarChar(Some(s)) => s.to_string(),
                Self::Bool(Some(s)) => s.to_string(),
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
        let schema = Schema::from_slice(&[
            (DataType::new_int(false), "v1".to_string()),
            (DataType::new_varchar(false), "v2".to_string()),
            (DataType::new_char(20, false), "v3".to_string()),
        ]);
        let schema = Rc::new(schema);
        let datums = vec![
            Datum::Int(Some(1)),
            Datum::VarChar(Some("foo".to_string())),
            Datum::Char(Some("bar".to_string())),
        ];
        let bytes = Datum::to_bytes_with_schema(&datums, schema.clone());
        let datums_to_check = Datum::from_bytes_and_schema(schema.clone(), bytes.as_slice());
        assert_eq!(datums, datums_to_check);
    }
}
