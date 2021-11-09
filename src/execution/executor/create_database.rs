use crate::catalog::CatalogRef;
use crate::execution::{ExecutionError, Executor};
use crate::table::Slice;

#[allow(dead_code)]
pub struct CreateDatabaseExecutor {
    database_catalog: CatalogRef,
    db_name: String,
}

#[allow(dead_code)]
impl CreateDatabaseExecutor {
    pub fn new(database_catalog: CatalogRef, db_name: String) -> Self {
        Self {
            database_catalog,
            db_name,
        }
    }
}

impl Executor for CreateDatabaseExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        self.database_catalog
            .borrow_mut()
            .insert(0, self.db_name.clone())?;
        todo!();
    }
}
