use crate::execution::ExecutionError;
use crate::table::Slice;

pub use create_database::CreateDatabaseExecutor;
pub use create_table::CreateTableExecutor;
pub use show_databases::ShowDatabasesExecutor;
pub use use_database::UseDatabaseExecutor;
pub use values::ValuesExecutor;

mod create_database;
mod create_table;
mod show_databases;
mod use_database;
mod values;

pub trait Executor {
    fn execute(&mut self) -> Result<Slice, ExecutionError>;
}

#[allow(dead_code)]
pub enum ExecutorImpl {
    CreateDatabase(CreateDatabaseExecutor),
    ShowDatabases(ShowDatabasesExecutor),
    UseDatabase(UseDatabaseExecutor),
    CreateTable(CreateTableExecutor),
    Values(ValuesExecutor),
}

impl ExecutorImpl {
    pub fn execute(&mut self) -> Result<Slice, ExecutionError> {
        match self {
            Self::CreateDatabase(executor) => executor.execute(),
            Self::ShowDatabases(executor) => executor.execute(),
            Self::UseDatabase(executor) => executor.execute(),
            Self::CreateTable(executor) => executor.execute(),
            Self::Values(executor) => executor.execute(),
        }
    }
}
