use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::index::BPTIndex;
use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Table, TableError};
use log::info;
use thiserror::Error;

mod executor;

pub use executor::*;

pub struct Engine {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
}

impl Engine {
    fn build(&self, plan: Plan) -> ExecutorImpl {
        info!("execute with plan {:#?}", plan);
        match plan {
            Plan::CreateDatabase(plan) => {
                ExecutorImpl::CreateDatabase(CreateDatabaseExecutor::new(
                    self.catalog.clone(),
                    self.bpm.clone(),
                    plan.database_name,
                ))
            }
            Plan::ShowDatabases => ExecutorImpl::ShowDatabases(ShowDatabasesExecutor::new(
                self.catalog.clone(),
                self.bpm.clone(),
            )),
            Plan::UseDatabase(plan) => ExecutorImpl::UseDatabase(UseDatabaseExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.database_name,
            )),
            Plan::CreateTable(plan) => ExecutorImpl::CreateTable(CreateTableExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.schema,
            )),
            Plan::Values(plan) => ExecutorImpl::Values(ValuesExecutor::new(
                plan.values,
                plan.schema,
                self.bpm.clone(),
            )),
            Plan::Insert(plan) => {
                let child = self.build(*plan.child);
                ExecutorImpl::Insert(InsertExecutor::new(
                    plan.table_name,
                    self.catalog.clone(),
                    Box::new(child),
                ))
            }
            Plan::Desc(plan) => ExecutorImpl::Desc(DescExecutor::new(
                plan.table_name,
                self.bpm.clone(),
                self.catalog.clone(),
            )),
            Plan::SeqScan(plan) => {
                let table = self
                    .catalog
                    .borrow_mut()
                    .find_table(plan.table_name)
                    .unwrap();
                let schema = table.schema.clone();
                let page_id = table.get_page_id_of_first_slice();
                ExecutorImpl::SeqScan(SeqScanExecutor::new(self.bpm.clone(), page_id, schema))
            }
            Plan::Project(plan) => {
                let child = self.build(*plan.child);
                ExecutorImpl::Project(ProjectExecutor::new(
                    plan.exprs,
                    Box::new(child),
                    self.bpm.clone(),
                ))
            }
            Plan::Filter(plan) => {
                let child = self.build(*plan.child);
                ExecutorImpl::Filter(FilterExecutor::new(
                    self.bpm.clone(),
                    Box::new(child),
                    plan.exprs,
                ))
            }
            Plan::AddIndex(plan) => ExecutorImpl::AddIndex(AddIndexExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.exprs,
            )),
            Plan::IndexScan(plan) => {
                let index = BPTIndex::open(self.bpm.clone(), plan.index_page_id);
                let begin_datums = plan.begin_datums.unwrap_or(index.first_key());
                let end_datums = plan.end_datums.unwrap_or(index.last_key());
                ExecutorImpl::IndexScan(IndexScanExecutor::new(
                    Table::open(plan.table_page_id, self.bpm.clone()),
                    index,
                    begin_datums,
                    end_datums,
                    self.bpm.clone(),
                ))
            }
        }
    }
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self { bpm, catalog }
    }
    pub fn execute(&mut self, plan: Plan) -> Result<Table, ExecutionError> {
        let mut executor = self.build(plan);
        let mut slices = vec![];
        while let Some(slice) = executor.execute()? {
            slices.push(slice);
        }
        let schema = executor.schema();
        Ok(Table::from_slice(slices, schema, self.bpm.clone()))
    }
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),
    #[error("TableError: {0}")]
    Table(#[from] TableError),
}
