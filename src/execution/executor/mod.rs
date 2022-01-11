use crate::execution::ExecutionError;
use crate::table::{SchemaRef, Slice};

pub use agg::AggExecutor;
pub use alter::{AddForeignExecutor, AddIndexExecutor, AddPrimaryExecutor, AddUniqueExecutor};
pub use create_database::CreateDatabaseExecutor;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use desc::{DescExecutor, ShowTablesExecutor};
pub use drop::{
    DropDatabaseExecutor, DropForeignExecuor, DropIndexExecutor, DropPrimaryExecutor,
    DropTableExecutor,
};
pub use filter::FilterExecutor;
pub use index_scan::IndexScanExecutor;
pub use insert::InsertExecutor;
pub use load_from_file::LoadFromFileExecutor;
pub use nested_loop_join::NestedLoopJoinExecutor;
pub use project::ProjectExecutor;
pub use seq_scan::SeqScanExecutor;
pub use show_databases::ShowDatabasesExecutor;
pub use use_database::UseDatabaseExecutor;
pub use values::ValuesExecutor;

mod agg;
mod alter;
mod create_database;
mod create_table;
mod delete;
mod desc;
mod drop;
mod filter;
mod index_scan;
mod insert;
mod load_from_file;
mod nested_loop_join;
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
    ShowTables(ShowTablesExecutor),
    SeqScan(SeqScanExecutor),
    IndexScan(IndexScanExecutor),
    Project(ProjectExecutor),
    Filter(FilterExecutor),
    AddIndex(AddIndexExecutor),
    AddPrimary(AddPrimaryExecutor),
    AddUnique(AddUniqueExecutor),
    AddForeign(AddForeignExecutor),
    DropTable(DropTableExecutor),
    DropDatabase(DropDatabaseExecutor),
    DropPrimary(DropPrimaryExecutor),
    DropForeign(DropForeignExecuor),
    DropIndex(DropIndexExecutor),
    Delete(DeleteExecutor),
    NestedLoopJoin(NestedLoopJoinExecutor),
    LoadFromFile(LoadFromFileExecutor),
    Agg(AggExecutor),
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
            Self::AddIndex(executor) => executor.execute(),
            Self::AddPrimary(executor) => executor.execute(),
            Self::AddForeign(executor) => executor.execute(),
            Self::AddUnique(executor) => executor.execute(),
            Self::IndexScan(executor) => executor.execute(),
            Self::DropTable(executor) => executor.execute(),
            Self::DropDatabase(executor) => executor.execute(),
            Self::DropPrimary(executor) => executor.execute(),
            Self::DropForeign(executor) => executor.execute(),
            Self::DropIndex(executor) => executor.execute(),
            Self::Delete(executor) => executor.execute(),
            Self::NestedLoopJoin(executor) => executor.execute(),
            Self::LoadFromFile(executor) => executor.execute(),
            Self::Agg(executor) => executor.execute(),
            Self::ShowTables(executor) => executor.execute(),
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
            Self::AddIndex(executor) => executor.schema(),
            Self::AddPrimary(executor) => executor.schema(),
            Self::AddForeign(executor) => executor.schema(),
            Self::AddUnique(executor) => executor.schema(),
            Self::IndexScan(executor) => executor.schema(),
            Self::DropTable(executor) => executor.schema(),
            Self::DropDatabase(executor) => executor.schema(),
            Self::DropPrimary(executor) => executor.schema(),
            Self::DropForeign(executor) => executor.schema(),
            Self::DropIndex(executor) => executor.schema(),
            Self::Delete(executor) => executor.schema(),
            Self::NestedLoopJoin(executor) => executor.schema(),
            Self::LoadFromFile(executor) => executor.schema(),
            Self::Agg(executor) => executor.schema(),
            Self::ShowTables(executor) => executor.schema(),
        }
    }
}
