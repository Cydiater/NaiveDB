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
    pub fn into_bytes(self, data_type: &DataType) -> Vec<u8> {
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
                    bytes.extend_from_slice(v.as_bytes());
                    bytes
                } else {
                    vec![0u8]
                }
            }
            _ => todo!(),
        }
    }
    pub fn from_bytes(data_type: DataType, bytes: Vec<u8>) -> Self {
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
                    Datum::VarChar(Some(
                        String::from_utf8(bytes[1..].try_into().unwrap()).unwrap(),
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
