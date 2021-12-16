use crate::catalog::CatalogManagerRef;
use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{ColumnConstraint, Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct DescExecutor {
    table_name: String,
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    executed: bool,
}

impl DescExecutor {
    pub fn new(table_name: String, bpm: BufferPoolManagerRef, catalog: CatalogManagerRef) -> Self {
        Self {
            table_name,
            bpm,
            catalog,
            executed: false,
        }
    }
}

impl Executor for DescExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[
            (DataType::new_varchar(false), "Field".to_string()),
            (DataType::new_varchar(false), "Type".to_string()),
            (DataType::new_varchar(false), "Nullable".to_string()),
            (DataType::new_varchar(false), "Key".to_string()),
        ]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            let table = self
                .catalog
                .borrow_mut()
                .find_table(self.table_name.clone())?;
            let desc_schema = self.schema();
            let mut desc = Slice::new(self.bpm.clone(), desc_schema);
            table.schema.iter().for_each(|c| {
                desc.add(&[
                    Datum::VarChar(Some(c.desc.clone())),
                    Datum::VarChar(Some(c.data_type.to_string())),
                    Datum::VarChar(Some(if c.data_type.nullable() {
                        "Yes".to_string()
                    } else {
                        "No".to_string()
                    })),
                    Datum::VarChar(Some(match c.constraint {
                        ColumnConstraint::Normal => "Normal".to_string(),
                        ColumnConstraint::Primary => "Primary".to_string(),
                        _ => todo!(),
                    })),
                ])
                .unwrap();
            });
            self.executed = true;
            Ok(Some(desc))
        } else {
            Ok(None)
        }
    }
}
