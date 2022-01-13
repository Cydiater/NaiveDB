use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor};
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice, Table};
use log::info;
use std::rc::Rc;

pub struct CreateTableExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    table_name: String,
    schema: SchemaRef,
    executed: bool,
}

impl CreateTableExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        table_name: String,
        schema: Schema,
    ) -> Self {
        Self {
            bpm,
            catalog,
            table_name,
            schema: Rc::new(schema),
            executed: false,
        }
    }
}

impl Executor for CreateTableExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            "table".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            info!("create table, schema = {:?}", self.schema);
            let mut table = Table::new(self.schema.clone(), self.bpm.clone());
            let page_id = table.page_id();
            if self
                .catalog
                .borrow_mut()
                .find_table(&self.table_name)
                .is_ok()
            {
                return Err(ExecutionError::Catalog(CatalogError::Duplicated));
            }
            self.catalog
                .borrow_mut()
                .create_table(&self.table_name, page_id)?;
            let primary_as_exprs = self.schema.project_by_primary();
            if !primary_as_exprs.is_empty() {
                let index = BPTIndex::new(self.bpm.clone(), primary_as_exprs);
                let page_id = index.get_page_id();
                self.catalog.borrow_mut().add_index(
                    &self.table_name,
                    Rc::new(index.get_key_schema()),
                    page_id,
                )?;
                table.meta_mut().page_id_of_primary_index = Some(index.get_page_id());
            }
            for unique in &table.schema.unique {
                let exprs = table.schema.project_by(unique);
                let index = BPTIndex::new(self.bpm.clone(), exprs);
                let page_id = index.get_page_id();
                self.catalog.borrow_mut().add_index(
                    &self.table_name,
                    Rc::new(index.get_key_schema()),
                    page_id,
                )?;
            }
            self.executed = true;
            Ok(Some(
                Slice::new_as_message(self.bpm.clone(), "table", &self.table_name).unwrap(),
            ))
        } else {
            Ok(None)
        }
    }
}
