use crate::execution::ExecutionError;
use crate::table::Slice;

pub use create_database::CreateDatabaseExecutor;
pub use show_databases::ShowDatabasesExecutor;

mod create_database;
mod show_databases;

pub trait Executor {
    fn execute(&mut self) -> Result<Slice, ExecutionError>;
}

#[allow(dead_code)]
pub enum ExecutorImpl {
    CreateDatabase(CreateDatabaseExecutor),
    ShowDatabases(ShowDatabasesExecutor),
}

impl ExecutorImpl {
    pub fn execute(&mut self) -> Result<Slice, ExecutionError> {
        match self {
            Self::CreateDatabase(executor) => executor.execute(),
            Self::ShowDatabases(executor) => executor.execute(),
        }
    }
}
