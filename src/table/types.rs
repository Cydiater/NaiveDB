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
}
