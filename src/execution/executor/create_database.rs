use crate::catalog::CatalogManagerRef;
use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct CreateDatabaseExecutor {
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    db_name: String,
    executed: bool,
}

impl CreateDatabaseExecutor {
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef, db_name: String) -> Self {
        Self {
            catalog,
            bpm,
            db_name,
            executed: false,
        }
    }
}

impl Executor for CreateDatabaseExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            "database".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            self.catalog.borrow_mut().create_database(&self.db_name)?;
            let res = Slice::new_as_message(self.bpm.clone(), "database", &self.db_name)?;
            self.executed = true;
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }
}
