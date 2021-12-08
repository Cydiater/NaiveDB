use crate::execution::ExecutionError;
use crate::table::{SchemaRef, Slice};

pub use create_database::CreateDatabaseExecutor;
pub use create_table::CreateTableExecutor;
pub use desc::DescExecutor;
pub use filter::FilterExecutor;
pub use insert::InsertExecutor;
pub use project::ProjectExecutor;
pub use seq_scan::SeqScanExecutor;
pub use show_databases::ShowDatabasesExecutor;
pub use use_database::UseDatabaseExecutor;
pub use values::ValuesExecutor;

mod create_database;
mod create_table;
mod desc;
mod filter;
mod insert;
mod project;
mod seq_scan;
mod show_databases;
mod use_database;
mod values;

pub trait Executor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError>;
    fn schema(&self) -> SchemaRef;
}

#[allow(dead_code)]
pub enum ExecutorImpl {
    CreateDatabase(CreateDatabaseExecutor),
    ShowDatabases(ShowDatabasesExecutor),
    UseDatabase(UseDatabaseExecutor),
    CreateTable(CreateTableExecutor),
    Values(ValuesExecutor),
    Insert(InsertExecutor),
    Desc(DescExecutor),
    SeqScan(SeqScanExecutor),
    Project(ProjectExecutor),
    Filter(FilterExecutor),
}

impl ExecutorImpl {
    pub fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        match self {
            Self::CreateDatabase(executor) => executor.execute(),
            Self::ShowDatabases(executor) => executor.execute(),
            Self::UseDatabase(executor) => executor.execute(),
            Self::CreateTable(executor) => executor.execute(),
            Self::Values(executor) => executor.execute(),
            Self::Insert(executor) => executor.execute(),
            Self::Desc(executor) => executor.execute(),
            Self::SeqScan(executor) => executor.execute(),
            Self::Project(executor) => executor.execute(),
            Self::Filter(executor) => executor.execute(),
        }
    }
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::CreateDatabase(executor) => executor.schema(),
            Self::ShowDatabases(executor) => executor.schema(),
            Self::UseDatabase(executor) => executor.schema(),
            Self::CreateTable(executor) => executor.schema(),
            Self::Values(executor) => executor.schema(),
            Self::Insert(executor) => executor.schema(),
            Self::Desc(executor) => executor.schema(),
            Self::SeqScan(executor) => executor.schema(),
            Self::Project(executor) => executor.schema(),
            Self::Filter(executor) => executor.schema(),
        }
    }
}
