use thiserror::Error;

mod rid;
mod schema;
mod slice;
mod types;

pub use schema::Schema;
pub use slice::Slice;
pub use types::DataType;

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum TableError {
    #[error("datum not match with schema")]
    DatumSchemaNotMatch,
    #[error("slice out of space")]
    SliceOutOfSpace,
}
