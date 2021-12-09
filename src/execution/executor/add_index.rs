use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor};
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct AddIndexExecutor {}

impl Executor for AddIndexExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_varchar(false),
            "Index Field".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        todo!()
    }
}
