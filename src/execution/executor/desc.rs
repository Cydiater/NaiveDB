use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{DataType, Datum, Schema, Slice};
use std::rc::Rc;

pub struct DescExecutor {
    table_name: String,
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
}

impl DescExecutor {
    pub fn new(table_name: String, bpm: BufferPoolManagerRef, catalog: CatalogManagerRef) -> Self {
        Self {
            table_name,
            bpm,
            catalog,
        }
    }
}

impl Executor for DescExecutor {
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        let table = self
            .catalog
            .borrow_mut()
            .find_table(self.table_name.clone())?;
        let desc_schema = Schema::from_slice(&[
            (DataType::VarChar, "Field".to_string(), false),
            (DataType::VarChar, "Type".to_string(), false),
            (DataType::VarChar, "Nullable".to_string(), false),
        ]);
        let mut desc = Slice::new_empty(self.bpm.clone(), Rc::new(desc_schema));
        table.schema.iter().for_each(|c| {
            desc.add(&[
                Datum::VarChar(Some(c.desc.clone())),
                Datum::VarChar(Some(c.data_type.to_string())),
                Datum::VarChar(Some(if c.nullable {
                    "Yes".to_string()
                } else {
                    "No".to_string()
                })),
            ])
            .unwrap();
        });
        Ok(desc)
    }
}
