use crate::catalog::CatalogManagerRef;
use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct ShowDatabasesExecutor {
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    executed: bool,
}

impl ShowDatabasesExecutor {
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self {
            catalog,
            bpm,
            executed: false,
        }
    }
}

impl Executor for ShowDatabasesExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_as_varchar(false),
            "database".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            let mut slice = Slice::new(self.bpm.clone(), self.schema());
            self.catalog.borrow().database_iter().for_each(|(name, _)| {
                slice
                    .insert(&[Datum::VarChar(Some(name.to_owned()))])
                    .unwrap();
            });
            self.executed = true;
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }
}
