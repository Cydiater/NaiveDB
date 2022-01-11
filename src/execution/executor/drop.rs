use crate::catalog::CatalogManagerRef;
use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor};
use crate::expr::ExprImpl;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use itertools::Itertools;
use std::rc::Rc;

pub struct DropTableExecutor {
    table_name: String,
    executed: bool,
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
}

pub struct DropDatabaseExecutor {
    database_name: String,
    executed: bool,
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
}

pub struct DropIndexExecutor {
    table_name: String,
    exprs: Vec<ExprImpl>,
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    executed: bool,
}

pub struct DropPrimaryExecutor {
    table_name: String,
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    executed: bool,
}

pub struct DropForeignExecuor {
    table_name: String,
    column_idxes: Vec<usize>,
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    executed: bool,
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

impl DropDatabaseExecutor {
    pub fn new(
        database_name: String,
        catalog: CatalogManagerRef,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            database_name,
            executed: false,
            bpm,
            catalog,
        }
    }
}

impl DropIndexExecutor {
    pub fn new(
        table_name: String,
        exprs: Vec<ExprImpl>,
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
    ) -> Self {
        Self {
            table_name,
            exprs,
            executed: false,
            bpm,
            catalog,
        }
    }
}

impl DropPrimaryExecutor {
    pub fn new(table_name: String, catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self {
            table_name,
            catalog,
            bpm,
            executed: false,
        }
    }
}

impl DropForeignExecuor {
    pub fn new(
        table_name: String,
        column_idxes: Vec<usize>,
        catalog: CatalogManagerRef,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            table_name,
            column_idxes,
            catalog,
            bpm,
            executed: false,
        }
    }
}

impl Executor for DropForeignExecuor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            "Drop Foreign".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        let mut table = self.catalog.borrow().find_table(&self.table_name)?;
        let mut schema = (*table.schema).clone();
        schema.foreign = schema
            .foreign
            .into_iter()
            .filter(|(_, src_and_dst)| {
                let src = src_and_dst.iter().map(|(s, _)| *s).collect_vec();
                src == self.column_idxes
            })
            .collect_vec();
        table.set_schema(Rc::new(schema));
        let output = Slice::new_as_message(self.bpm.clone(), "Drop Foreign", "Ok")?;
        Ok(Some(output))
    }
}

impl Executor for DropIndexExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            "Drop Index".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let schema = Schema::from_exprs(&self.exprs);
        self.catalog
            .borrow_mut()
            .drop_index(&self.table_name, Rc::new(schema))?;
        let slice = Slice::new_as_message(self.bpm.clone(), "Drop Index", "Ok")?;
        Ok(Some(slice))
    }
}

impl Executor for DropPrimaryExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            "Drop Primary".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let mut table = self.catalog.borrow().find_table(&self.table_name)?;
        let exprs = table.schema.project_by_primary();
        let schema = Schema::from_exprs(&exprs);
        self.catalog
            .borrow_mut()
            .drop_index(&self.table_name, Rc::new(schema))?;
        let mut schema = (*table.schema).clone();
        schema.primary = vec![];
        table.set_schema(Rc::new(schema));
        let slice = Slice::new_as_message(self.bpm.clone(), "Drop Primary", "Ok")?;
        Ok(Some(slice))
    }
}

impl Executor for DropDatabaseExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_varchar(false),
            "database".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        self.catalog
            .borrow_mut()
            .remove_database(&self.database_name)?;
        Ok(Some(
            Slice::new_as_message(self.bpm.clone(), "database", &self.database_name).unwrap(),
        ))
    }
}

impl Executor for DropTableExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
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
