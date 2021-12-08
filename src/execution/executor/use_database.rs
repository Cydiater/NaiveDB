use crate::catalog::CatalogManagerRef;
use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct UseDatabaseExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    database_name: String,
    executed: bool,
}

impl Executor for UseDatabaseExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_varchar(false),
            "database".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            self.catalog
                .borrow_mut()
                .use_database(self.database_name.clone())?;
            self.executed = true;
            Ok(Some(
                Slice::new_simple_message(
                    self.bpm.clone(),
                    "database".to_string(),
                    self.database_name.clone(),
                )
                .unwrap(),
            ))
        } else {
            Ok(None)
        }
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
            executed: false,
        }
    }
}
