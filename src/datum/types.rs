use std::fmt;
use thiserror::Error;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DataType {
    Int(bool),
    VarChar(bool),
    Bool(bool),
    Date(bool),
    Float(bool),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Int(_) => "int",
                Self::VarChar(_) => "varchar",
                Self::Bool(_) => "bool",
                Self::Date(_) => "date",
                Self::Float(_) => "float",
            }
        )
    }
}

impl DataType {
    pub fn new_as_int(nullable: bool) -> Self {
        Self::Int(nullable)
    }
    pub fn new_as_bool(nullable: bool) -> Self {
        Self::Bool(nullable)
    }
    pub fn new_as_varchar(nullable: bool) -> Self {
        Self::VarChar(nullable)
    }
    pub fn new_as_date(nullable: bool) -> Self {
        Self::Date(nullable)
    }
    pub fn new_as_float(nullable: bool) -> Self {
        Self::Float(nullable)
    }
    pub fn width_of_value(&self) -> Option<usize> {
        match self {
            Self::Bool(_) => Some(2),
            Self::Int(_) => Some(5),
            Self::Float(_) => Some(5),
            Self::Date(_) => Some(1 + 4 + 1 + 1),
            _ => None,
        }
    }
    pub fn nullable(&self) -> bool {
        match self {
            Self::Int(nullable)
            | Self::Bool(nullable)
            | Self::VarChar(nullable)
            | Self::Date(nullable)
            | Self::Float(nullable) => *nullable,
        }
    }
    pub fn is_inlined(&self) -> bool {
        match self {
            Self::Bool(_) | Self::Int(_) | Self::Float(_) | Self::Date(_) => true,
            Self::VarChar(_) => false,
        }
    }
    pub fn to_bytes(&self) -> [u8; 1] {
        let mask = if self.nullable() { 128u8 } else { 0u8 };
        match self {
            Self::Int(_) => [mask],
            Self::VarChar(_) => [2u8 | mask],
            Self::Bool(_) => [3u8 | mask],
            Self::Float(_) => [4u8 | mask],
            Self::Date(_) => [5u8 | mask],
        }
    }
    pub fn from_bytes(bytes: &[u8; 1]) -> Result<Self, DataTypeError> {
        let type_id = bytes[0] & (127);
        let nullable = bytes[0] & 128 != 0;
        match type_id {
            0 => Ok(Self::new_as_int(nullable)),
            2 => Ok(Self::new_as_varchar(nullable)),
            3 => Ok(Self::new_as_bool(nullable)),
            4 => Ok(Self::new_as_float(nullable)),
            5 => Ok(Self::new_as_date(nullable)),
            _ => Err(DataTypeError::UndefinedDataType),
        }
    }
}

#[derive(Error, Debug)]
pub enum DataTypeError {
    #[error("undefine datatype")]
    UndefinedDataType,
}
