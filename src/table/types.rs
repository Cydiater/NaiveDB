use std::convert::TryInto;
use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct CharType {
    pub width: usize,
}

#[allow(dead_code)]
impl CharType {
    pub fn new(width: usize) -> Self {
        Self { width }
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DataType {
    Int,
    Char(CharType),
    VarChar,
}

#[allow(dead_code)]
impl DataType {
    pub fn width(&self) -> Option<usize> {
        match self {
            Self::Int => Some(4),
            Self::Char(char_type) => Some(char_type.width),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> [u8; 5] {
        match self {
            Self::Int => [0; 5],
            Self::Char(char_type) => {
                let mut b = vec![1u8];
                b.extend_from_slice(&(char_type.width as u32).to_le_bytes());
                b.as_slice().try_into().unwrap()
            }
            Self::VarChar => [2u8, 0, 0, 0, 0],
        }
    }
    pub fn from_bytes(bytes: &[u8; 5]) -> Result<Self, DataTypeError> {
        match bytes[0] {
            0 => Ok(Self::Int),
            1 => Ok(Self::Char(CharType::new(
                u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize,
            ))),
            2 => Ok(Self::VarChar),
            _ => Err(DataTypeError::UndefinedDataType),
        }
    }
}

#[derive(Error, Debug)]
pub enum DataTypeError {
    #[error("undefine datatype")]
    UndefinedDataType,
}
