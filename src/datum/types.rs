use std::convert::TryInto;
use std::fmt;
use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct CharType {
    pub width: usize,
    pub nullable: bool,
}

impl CharType {
    pub fn new(width: usize, nullable: bool) -> Self {
        Self { width, nullable }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct IntType {
    pub nullable: bool,
}

impl IntType {
    pub fn new(nullable: bool) -> Self {
        Self { nullable }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct VarCharType {
    pub nullable: bool,
}

impl VarCharType {
    pub fn new(nullable: bool) -> Self {
        Self { nullable }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BoolType {
    pub nullable: bool,
}

impl BoolType {
    pub fn new(nullable: bool) -> Self {
        Self { nullable }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DataType {
    Int(IntType),
    Char(CharType),
    VarChar(VarCharType),
    Bool(BoolType),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Int(_) => "int".to_string(),
                Self::Char(char_type) => format!("char({})", char_type.width),
                Self::VarChar(_) => "varchar".to_string(),
                Self::Bool(_) => "bool".to_string(),
            }
        )
    }
}

impl DataType {
    pub fn new_int(nullable: bool) -> Self {
        Self::Int(IntType::new(nullable))
    }
    pub fn new_bool(nullable: bool) -> Self {
        Self::Bool(BoolType::new(nullable))
    }
    pub fn new_varchar(nullable: bool) -> Self {
        Self::VarChar(VarCharType::new(nullable))
    }
    pub fn new_char(width: usize, nullable: bool) -> Self {
        Self::Char(CharType::new(width, nullable))
    }
    pub fn width_of_value(&self) -> Option<usize> {
        match self {
            Self::Bool(_) => Some(2),
            Self::Int(_) => Some(5),
            Self::Char(char_type) => Some(char_type.width + 1),
            _ => None,
        }
    }
    pub fn nullable(&self) -> bool {
        match self {
            Self::Bool(bool_type) => bool_type.nullable,
            Self::Int(int_type) => int_type.nullable,
            Self::VarChar(varchar_type) => varchar_type.nullable,
            Self::Char(char_type) => char_type.nullable,
        }
    }
    pub fn is_inlined(&self) -> bool {
        match self {
            Self::Bool(_) => true,
            Self::Int(_) => true,
            Self::Char(_) => true,
            Self::VarChar(_) => false,
        }
    }
    pub fn as_bytes(&self) -> [u8; 5] {
        let mask = if self.nullable() { 128u8 } else { 0u8 };
        match self {
            Self::Int(_) => [mask, 0, 0, 0, 0],
            Self::Char(char_type) => {
                let mut b = vec![1u8 | mask];
                b.extend_from_slice(&(char_type.width as u32).to_le_bytes());
                b.as_slice().try_into().unwrap()
            }
            Self::VarChar(_) => [2u8 | mask, 0, 0, 0, 0],
            Self::Bool(_) => [3u8 | mask, 0, 0, 0, 0],
        }
    }
    pub fn from_bytes(bytes: &[u8; 5]) -> Result<Self, DataTypeError> {
        let type_id = bytes[0] & (127);
        let nullable = bytes[0] & 128 != 0;
        match type_id {
            0 => Ok(Self::Int(IntType::new(nullable))),
            1 => Ok(Self::Char(CharType::new(
                u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize,
                nullable,
            ))),
            2 => Ok(Self::VarChar(VarCharType::new(nullable))),
            3 => Ok(Self::Bool(BoolType::new(nullable))),
            _ => Err(DataTypeError::UndefinedDataType),
        }
    }
}

#[derive(Error, Debug)]
pub enum DataTypeError {
    #[error("undefine datatype")]
    UndefinedDataType,
}
