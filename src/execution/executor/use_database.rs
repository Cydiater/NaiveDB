use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::Slice;

pub struct UseDatabaseExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    database_name: String,
}

impl Executor for UseDatabaseExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        self.catalog
            .borrow_mut()
            .use_database(self.database_name.clone())?;
        Ok(Slice::new_simple_message(
            self.bpm.clone(),
            "database".to_string(),
            self.database_name.clone(),
        )
        .unwrap())
    }
}

impl UseDatabaseExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        database_name: String,
    ) -> Self {
        Self {
            bpm,
            catalog,
            database_name,
        }
    }
}
