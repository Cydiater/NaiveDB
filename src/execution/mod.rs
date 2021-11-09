use crate::catalog::{Catalog, CatalogError};
use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;
use crate::table::Slice;
use thiserror::Error;

mod executor;

pub use executor::{Executor, ExecutorImpl};

#[allow(dead_code)]
pub struct Engine {
    bpm: BufferPoolManagerRef,
    database_catalog: Catalog,
}

impl Engine {
    fn build(&self) -> ExecutorImpl {
        todo!();
    }
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        let num_pages = bpm.borrow().num_pages().unwrap();
        // allocate database catalog
        if num_pages == 0 {
            let _ = bpm.borrow_mut().alloc().unwrap();
        }
        Self {
            bpm: bpm.clone(),
            database_catalog: Catalog::new_database_catalog(bpm),
        }
    }
    pub fn execute(&mut self, _plan: Plan) -> Result<Slice, ExecutionError> {
        let mut executor = self.build();
        executor.execute()
    }
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),
}
