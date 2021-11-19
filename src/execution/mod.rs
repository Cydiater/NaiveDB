use crate::catalog::{CatalogError, CatalogManager, CatalogManagerRef};
use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Slice, TableError};
use log::info;
use thiserror::Error;

mod executor;

pub use executor::{
    CreateDatabaseExecutor, CreateTableExecutor, Executor, ExecutorImpl, InsertExecutor,
    ShowDatabasesExecutor, UseDatabaseExecutor, ValuesExecutor,
};

pub struct Engine {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
}

impl Engine {
    fn build(&self, plan: Plan) -> ExecutorImpl {
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
            Plan::Insert(plan) => {
                let table = self
                    .catalog
                    .borrow()
                    .find_table(plan.table_name.clone())
                    .unwrap();
                let child = Box::new(ExecutorImpl::Values(ValuesExecutor::new(
                    plan.values,
                    table.schema,
                    self.bpm.clone(),
                )));
                ExecutorImpl::Insert(InsertExecutor::new(plan.table_name, child))
            }
        }
    }
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        let num_pages = bpm.borrow().num_pages().unwrap();
        info!("disk file have {} pages", num_pages);
        // allocate database catalog
        if num_pages == 0 {
            let page = bpm.borrow_mut().alloc().unwrap();
            let page_id = page.borrow().page_id.unwrap();
            // mark num of database to 0
            page.borrow_mut().buffer[0..4].copy_from_slice(&0u32.to_le_bytes());
            bpm.borrow_mut().unpin(page_id).unwrap();
        }
        Self {
            bpm: bpm.clone(),
            catalog: CatalogManager::new_shared(bpm),
        }
    }
    pub fn execute(&mut self, plan: Plan) -> Result<Slice, ExecutionError> {
        let mut executor = self.build(plan);
        executor.execute()
    }
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),
    #[error("TableError: {0}")]
    Table(#[from] TableError),
}
