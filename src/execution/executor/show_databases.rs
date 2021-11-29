use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{DataType, Datum, Schema, Slice};
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
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            let schema =
                Schema::from_slice(&[(DataType::new_varchar(false), "database".to_string())]);
            let mut slice = Slice::new(self.bpm.clone(), Rc::new(schema));
            self.catalog.borrow().iter().for_each(|(_, _, name)| {
                slice.add(vec![Datum::VarChar(Some(name))]).unwrap();
            });
            self.executed = true;
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }
}
