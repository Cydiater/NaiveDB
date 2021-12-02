use crate::catalog::{CatalogError, CatalogManagerRef};
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
                ExecutorImpl::SeqScan(SeqScanExecutor::new(
                    self.bpm.clone(),
                    Some(page_id),
                    schema,
                ))
            }
            Plan::Project(plan) => {
                let child = self.build(*plan.child);
                ExecutorImpl::Project(ProjectExecutor::new(
                    plan.exprs,
                    Box::new(child),
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
        Ok(Table::from_slice(slices, self.bpm.clone()))
    }
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),
    #[error("TableError: {0}")]
    Table(#[from] TableError),
}
