use crate::catalog::CatalogRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{DataType, Datum, Schema, Slice};
use std::rc::Rc;

pub struct ShowDatabasesExecutor {
    database_catalog: CatalogRef,
    bpm: BufferPoolManagerRef,
}

impl ShowDatabasesExecutor {
    pub fn new(database_catalog: CatalogRef, bpm: BufferPoolManagerRef) -> Self {
        Self {
            database_catalog,
            bpm,
        }
    }
}

impl Executor for ShowDatabasesExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        let schema = Schema::from_slice(&[(DataType::VarChar, "database".to_string())]);
        let mut slice = Slice::new(self.bpm.clone(), Rc::new(schema));
        self.database_catalog
            .borrow()
            .iter()
            .for_each(|(_, _, name)| {
                slice.add(&[Datum::VarChar(name)]).unwrap();
            });
        Ok(slice)
    }
}
