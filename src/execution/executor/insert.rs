use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::table::Slice;

#[allow(dead_code)]
pub struct InsertExecutor {
    table_name: String,
    child: Box<ExecutorImpl>,
}

impl InsertExecutor {
    pub fn new(table_name: String, child: Box<ExecutorImpl>) -> Self {
        Self { table_name, child }
    }
}

impl Executor for InsertExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        todo!()
    }
}
