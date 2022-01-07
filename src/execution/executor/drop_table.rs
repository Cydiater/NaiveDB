use crate::catalog::CatalogManagerRef;
use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct DropTableExecutor {
    table_name: String,
    executed: bool,
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
}

impl DropTableExecutor {
    pub fn new(table_name: String, catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self {
            table_name,
            executed: false,
            catalog,
            bpm,
        }
    }
}

impl Executor for DropTableExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_as_varchar(false),
            "table".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let table = self.catalog.borrow().find_table(&self.table_name)?;
        table.erase();
        self.catalog.borrow_mut().remove_table(&self.table_name)?;
        self.catalog
            .borrow_mut()
            .remove_indexes_by_table(&self.table_name)?;
        Ok(Some(Slice::new_as_message(
            self.bpm.clone(),
            "table",
            &self.table_name,
        )?))
    }
}
