use crate::catalog::CatalogManagerRef;
use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::expr::ExprImpl;
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use itertools::Itertools;
use std::rc::Rc;

pub struct AddIndexExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    table_name: String,
    exprs: Vec<ExprImpl>,
    executed: bool,
}

pub struct AddPrimaryExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    table_name: String,
    column_names: Vec<String>,
    executed: bool,
}

impl AddPrimaryExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        table_name: String,
        column_names: Vec<String>,
    ) -> Self {
        Self {
            bpm,
            catalog,
            table_name,
            column_names,
            executed: false,
        }
    }
}

pub struct AddUniqueExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    table_name: String,
    unique_set: Vec<usize>,
    executed: bool,
}

impl AddUniqueExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        table_name: String,
        unique_set: Vec<usize>,
    ) -> Self {
        Self {
            bpm,
            catalog,
            table_name,
            unique_set,
            executed: false,
        }
    }
}

pub struct AddForeignExecutor {
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
    table_name: String,
    column_names: Vec<String>,
    ref_table_name: String,
    ref_column_names: Vec<String>,
    executed: bool,
}

impl AddForeignExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        table_name: String,
        column_names: Vec<String>,
        ref_table_name: String,
        ref_column_names: Vec<String>,
    ) -> Self {
        Self {
            catalog,
            bpm,
            table_name,
            column_names,
            ref_table_name,
            ref_column_names,
            executed: false,
        }
    }
}

impl Executor for AddPrimaryExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_as_varchar(false),
            "Add Primary".to_owned(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        let mut table = self.catalog.borrow().find_table(&self.table_name)?;
        let mut schema = (*table.schema).clone();
        for column_name in self.column_names.clone() {
            schema.set_primary(&column_name).unwrap()
        }
        table.set_schema(Rc::new(schema));
        let exprs = table.schema.primary_as_exprs();
        let mut index = BPTIndex::new(self.bpm.clone(), exprs.iter().cloned().collect_vec());
        let slices = table.into_slice();
        for slice in slices {
            let rows = ExprImpl::batch_eval(&exprs, Some(&slice));
            for (idx, row) in rows.iter().enumerate() {
                let record_id = (slice.page_id(), idx);
                index.insert(row, record_id).unwrap();
            }
        }
        let page_id = index.get_page_id();
        self.catalog.borrow_mut().add_index(
            &self.table_name,
            Rc::new(Schema::from_exprs(&exprs)),
            page_id,
        )?;
        self.executed = true;
        Ok(Some(
            Slice::new_as_message(self.bpm.clone(), "Add Primary", "Ok").unwrap(),
        ))
    }
}

impl Executor for AddUniqueExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_as_varchar(false),
            "Add Unique".to_owned(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        let mut table = self.catalog.borrow().find_table(&self.table_name)?;
        let mut schema = (*table.schema).clone();
        schema.set_unique(self.unique_set.clone());
        table.set_schema(Rc::new(schema));
        let unique_sets = table.schema.unique_as_exprs();
        let exprs = unique_sets.last().unwrap();
        let mut index = BPTIndex::new(self.bpm.clone(), exprs.iter().cloned().collect_vec());
        let slices = table.into_slice();
        for slice in slices {
            let rows = ExprImpl::batch_eval(exprs, Some(&slice));
            for (idx, row) in rows.iter().enumerate() {
                let record_id = (slice.page_id(), idx);
                index.insert(row, record_id).unwrap();
            }
        }
        let page_id = index.get_page_id();
        self.catalog.borrow_mut().add_index(
            &self.table_name,
            Rc::new(Schema::from_exprs(exprs)),
            page_id,
        )?;
        self.executed = true;
        Ok(Some(
            Slice::new_as_message(self.bpm.clone(), "Add Unique", "Ok").unwrap(),
        ))
    }
}

impl Executor for AddForeignExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_as_varchar(false),
            "Add Foreign".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let mut table = self.catalog.borrow().find_table(&self.table_name)?;
        let mut schema = (*table.schema).clone();
        let ref_table = self.catalog.borrow().find_table(&self.ref_table_name)?;
        let page_id_of_ref_table = ref_table.get_page_id();
        for (column_name, ref_column_name) in self.column_names.iter().zip(&self.ref_column_names) {
            let idx = ref_table.schema.index_of(ref_column_name).unwrap();
            schema
                .set_foreign(column_name, page_id_of_ref_table, idx)
                .unwrap();
        }
        table.set_schema(Rc::new(schema));
        Ok(Some(
            Slice::new_as_message(self.bpm.clone(), "Add Foreign", "Ok").unwrap(),
        ))
    }
}

impl AddIndexExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        catalog: CatalogManagerRef,
        table_name: String,
        exprs: Vec<ExprImpl>,
    ) -> Self {
        AddIndexExecutor {
            bpm,
            catalog,
            table_name,
            exprs,
            executed: false,
        }
    }
}

impl Executor for AddIndexExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_as_int(false),
            "Number Of Indexed Tuple".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let table = self.catalog.borrow().find_table(&self.table_name)?;
        let mut index = BPTIndex::new(self.bpm.clone(), self.exprs.iter().cloned().collect_vec());
        let slices = table.into_slice();
        let mut indexed_cnt = 0;
        for slice in slices {
            let rows = ExprImpl::batch_eval(&self.exprs, Some(&slice));
            for (idx, row) in rows.iter().enumerate() {
                let record_id = (slice.page_id(), idx);
                index.insert(row, record_id).unwrap();
                indexed_cnt += 1;
            }
        }
        let page_id = index.get_page_id();
        self.catalog.borrow_mut().add_index(
            &self.table_name,
            Rc::new(Schema::from_exprs(&self.exprs)),
            page_id,
        )?;
        let mut msg = Slice::new(self.bpm.clone(), self.schema());
        msg.insert(&[Datum::Int(Some(indexed_cnt))])?;
        Ok(Some(msg))
    }
}
