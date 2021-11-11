use crate::catalog::CatalogRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::Slice;

pub struct CreateDatabaseExecutor {
    database_catalog: CatalogRef,
    bpm: BufferPoolManagerRef,
    db_name: String,
}

impl CreateDatabaseExecutor {
    pub fn new(database_catalog: CatalogRef, bpm: BufferPoolManagerRef, db_name: String) -> Self {
        Self {
            database_catalog,
            bpm,
            db_name,
        }
    }
}

impl Executor for CreateDatabaseExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        self.database_catalog
            .borrow_mut()
            .insert(0, self.db_name.clone())?;
        let res = Slice::new_simple_message(
            self.bpm.clone(),
            "database".to_string(),
            self.db_name.clone(),
        )?;
        Ok(res)
    }
}
