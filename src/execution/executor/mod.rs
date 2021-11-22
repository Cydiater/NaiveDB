use crate::execution::ExecutionError;
use crate::table::Slice;

pub use create_database::CreateDatabaseExecutor;
pub use create_table::CreateTableExecutor;
pub use desc::DescExecutor;
pub use insert::InsertExecutor;
pub use show_databases::ShowDatabasesExecutor;
pub use use_database::UseDatabaseExecutor;
pub use values::ValuesExecutor;

mod create_database;
mod create_table;
mod desc;
mod insert;
mod show_databases;
mod use_database;
mod values;

pub trait Executor {
    fn execute(&mut self) -> Result<Slice, ExecutionError>;
}

pub enum ExecutorImpl {
    CreateDatabase(CreateDatabaseExecutor),
    ShowDatabases(ShowDatabasesExecutor),
    UseDatabase(UseDatabaseExecutor),
    CreateTable(CreateTableExecutor),
    Values(ValuesExecutor),
    Insert(InsertExecutor),
    Desc(DescExecutor),
}

impl ExecutorImpl {
    pub fn execute(&mut self) -> Result<Slice, ExecutionError> {
        match self {
            Self::CreateDatabase(executor) => executor.execute(),
            Self::ShowDatabases(executor) => executor.execute(),
            Self::UseDatabase(executor) => executor.execute(),
            Self::CreateTable(executor) => executor.execute(),
            Self::Values(executor) => executor.execute(),
            Self::Insert(executor) => executor.execute(),
            Self::Desc(executor) => executor.execute(),
        }
    }
}
