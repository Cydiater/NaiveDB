use crate::execution::{ExecutionError, Executor};
use crate::table::Slice;

pub struct ValuesExecutor {}

impl Executor for ValuesExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        todo!()
    }
}
