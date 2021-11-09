use crate::execution::ExecutionError;
use crate::table::Slice;
use create_database::CreateDatabaseExecutor;

mod create_database;

pub trait Executor {
    fn execute() -> Result<Slice, ExecutionError>;
}

#[allow(dead_code)]
pub enum ExecutorImpl {
    CreateDatabase(CreateDatabaseExecutor),
}

impl ExecutorImpl {
    pub fn execute(&self) -> Result<Slice, ExecutionError> {
        todo!()
    }
}
