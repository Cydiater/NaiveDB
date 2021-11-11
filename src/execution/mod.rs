use crate::catalog::{Catalog, CatalogError, CatalogRef};
use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Slice, TableError};
use log::info;
use thiserror::Error;

mod executor;

pub use executor::{CreateDatabaseExecutor, Executor, ExecutorImpl};

pub struct Engine {
    bpm: BufferPoolManagerRef,
    database_catalog: CatalogRef,
}

impl Engine {
    fn build(&self, plan: Plan) -> ExecutorImpl {
        match plan {
            Plan::CreateDatabase(plan) => {
                ExecutorImpl::CreateDatabase(CreateDatabaseExecutor::new(
                    self.database_catalog.clone(),
                    self.bpm.clone(),
                    plan.database_name,
                ))
            }
            _ => todo!(),
        }
    }
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        let num_pages = bpm.borrow().num_pages().unwrap();
        info!("disk file have {} pages", num_pages);
        // allocate database catalog
        if num_pages == 0 {
            let _ = bpm.borrow_mut().alloc().unwrap();
        }
        Self {
            bpm: bpm.clone(),
            database_catalog: Catalog::new_database_catalog(bpm),
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
