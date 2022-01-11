use crate::catalog::CatalogManagerRef;
use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct DescExecutor {
    table_name: String,
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    executed: bool,
}

pub struct ShowTablesExecutor {
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

impl ShowTablesExecutor {
    pub fn new(bpm: BufferPoolManagerRef, catalog: CatalogManagerRef) -> Self {
        Self {
            bpm,
            catalog,
            executed: false,
        }
    }
}

impl Executor for ShowTablesExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            self.catalog.borrow().current_database().unwrap(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let mut slice = Slice::new(self.bpm.clone(), self.schema());
        let table_names = self.catalog.borrow().table_names()?;
        for table_name in table_names {
            slice.insert(&[table_name.as_str().into()])?;
        }
        Ok(Some(slice))
    }
}

impl Executor for DescExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[
            (DataType::new_as_varchar(false), "Field".into()),
            (DataType::new_as_varchar(false), "Type".into()),
            (DataType::new_as_varchar(false), "Nullable".into()),
        ]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            let table = self.catalog.borrow_mut().find_table(&self.table_name)?;
            let desc_schema = self.schema();
            let mut desc = Slice::new(self.bpm.clone(), desc_schema);
            table.schema.columns.iter().for_each(|c| {
                desc.insert(&[
                    Datum::VarChar(Some(c.desc.clone())),
                    Datum::VarChar(Some(c.data_type.to_string())),
                    Datum::VarChar(Some(if c.data_type.nullable() {
                        "Yes".to_string()
                    } else {
                        "No".to_string()
                    })),
                ])
                .unwrap();
            });
            for unique in &table.schema.unique {
                let mut msg = "Unique(".to_string();
                for u in unique {
                    msg += &table.schema.columns[*u].desc;
                    msg += ", ";
                }
                msg.pop();
                msg.pop();
                msg += ")";
                desc.insert(&[
                    msg.as_str().into(),
                    "N/A".into(),
                    "N/A".into(),
                    "N/A".into(),
                ])?;
            }
            if !table.schema.primary.is_empty() {
                let mut msg = "Primary(".to_string();
                for p in &table.schema.primary {
                    msg += &table.schema.columns[*p].desc;
                    msg += ", "
                }
                msg.pop();
                msg.pop();
                msg += ")";
                desc.insert(&[
                    msg.as_str().into(),
                    "N/A".into(),
                    "N/A".into(),
                    "N/A".into(),
                ])?;
            }
            self.executed = true;
            Ok(Some(desc))
        } else {
            Ok(None)
        }
    }
}
