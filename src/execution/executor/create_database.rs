use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::Slice;

pub struct CreateDatabaseExecutor {
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    db_name: String,
}

impl CreateDatabaseExecutor {
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef, db_name: String) -> Self {
        Self {
            catalog,
            bpm,
            db_name,
        }
    }
}

impl Executor for CreateDatabaseExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        self.catalog
            .borrow_mut()
            .create_database(self.db_name.clone())
            .unwrap();
        let res = Slice::new_simple_message(
            self.bpm.clone(),
            "database".to_string(),
            self.db_name.clone(),
        )?;
        Ok(res)
    }
}
