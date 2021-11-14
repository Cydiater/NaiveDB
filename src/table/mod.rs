use crate::storage::StorageError;
use thiserror::Error;

mod rid;
mod schema;
mod slice;
mod types;

pub use schema::{Column, Schema};
pub use slice::{Datum, Slice};
pub use types::{CharType, DataType};

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum TableError {
    #[error("datum not match with schema")]
    DatumSchemaNotMatch,
    #[error("slice out of space")]
    SliceOutOfSpace,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("PageID not assigned")]
    NoPageID,
}
