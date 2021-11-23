use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{DataType, Datum, Schema, Slice};
use std::rc::Rc;

pub struct ShowDatabasesExecutor {
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
}

impl ShowDatabasesExecutor {
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self { catalog, bpm }
    }
}

impl Executor for ShowDatabasesExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let schema = Schema::from_slice(&[(DataType::VarChar, "database".to_string(), false)]);
        let mut slice = Slice::new(self.bpm.clone(), Rc::new(schema));
        self.catalog.borrow().iter().for_each(|(_, _, name)| {
            slice.add(&[Datum::VarChar(Some(name))]).unwrap();
        });
        Ok(Some(slice))
    }
}
