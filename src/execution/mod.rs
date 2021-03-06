use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::datum::Datum;
use crate::index::{BPTIndex, IndexError};
use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaError, Table, TableError};
use itertools::Itertools;
use log::info;
use thiserror::Error;

mod executor;

pub use executor::*;

pub struct Engine {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
}

impl Engine {
    fn build(&self, plan: Plan) -> Result<ExecutorImpl, ExecutionError> {
        info!("execute with plan {:#?}", plan);
        match plan {
            Plan::CreateDatabase(plan) => {
                Ok(ExecutorImpl::CreateDatabase(CreateDatabaseExecutor::new(
                    self.catalog.clone(),
                    self.bpm.clone(),
                    plan.database_name,
                )))
            }
            Plan::ShowDatabases => Ok(ExecutorImpl::ShowDatabases(ShowDatabasesExecutor::new(
                self.catalog.clone(),
                self.bpm.clone(),
            ))),
            Plan::UseDatabase(plan) => Ok(ExecutorImpl::UseDatabase(UseDatabaseExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.database_name,
            ))),
            Plan::CreateTable(plan) => Ok(ExecutorImpl::CreateTable(CreateTableExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.schema,
            ))),
            Plan::Values(plan) => Ok(ExecutorImpl::Values(ValuesExecutor::new(
                plan.values,
                plan.schema,
                self.bpm.clone(),
            ))),
            Plan::Update(plan) => {
                let table = self.catalog.borrow().find_table(&plan.table_name)?;
                let child = self.build(*plan.child)?;
                Ok(ExecutorImpl::Update(UpdateExecutor::new(
                    plan.idx_with_values,
                    table.schema.clone(),
                    self.bpm.clone(),
                    child,
                )))
            }
            Plan::Insert(plan) => {
                let child = self.build(*plan.child)?;
                let table = self.catalog.borrow().find_table(&plan.table_name).unwrap();
                let indexes = self
                    .catalog
                    .borrow()
                    .find_indexes_by_table(&plan.table_name)
                    .unwrap();
                Ok(ExecutorImpl::Insert(InsertExecutor::new(
                    table,
                    indexes,
                    Box::new(child),
                    self.bpm.clone(),
                )))
            }
            Plan::Desc(plan) => Ok(ExecutorImpl::Desc(DescExecutor::new(
                plan.table_name,
                self.bpm.clone(),
                self.catalog.clone(),
            ))),
            Plan::SeqScan(plan) => {
                let table = self.catalog.borrow_mut().find_table(&plan.table_name)?;
                let schema = table.schema.clone();
                let page_id = table.meta().page_id_of_first_slice;
                Ok(ExecutorImpl::SeqScan(SeqScanExecutor::new(
                    self.bpm.clone(),
                    Some(page_id),
                    schema,
                    plan.with_record_id,
                )))
            }
            Plan::Project(plan) => {
                let child = self.build(*plan.child)?;
                Ok(ExecutorImpl::Project(ProjectExecutor::new(
                    plan.exprs,
                    Box::new(child),
                    self.bpm.clone(),
                )))
            }
            Plan::Filter(plan) => {
                let child = self.build(*plan.child)?;
                Ok(ExecutorImpl::Filter(FilterExecutor::new(
                    self.bpm.clone(),
                    Box::new(child),
                    plan.exprs,
                )))
            }
            Plan::AddIndex(plan) => Ok(ExecutorImpl::AddIndex(AddIndexExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.exprs,
            ))),
            Plan::AddPrimary(plan) => Ok(ExecutorImpl::AddPrimary(AddPrimaryExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.column_names,
            ))),
            Plan::AddUnique(plan) => Ok(ExecutorImpl::AddUnique(AddUniqueExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.unique_set,
            ))),
            Plan::AddForeign(plan) => Ok(ExecutorImpl::AddForeign(AddForeignExecutor::new(
                self.bpm.clone(),
                self.catalog.clone(),
                plan.table_name,
                plan.column_names,
                plan.ref_table_name,
                plan.ref_column_names,
            ))),
            Plan::IndexScan(plan) => {
                let table = Table::open(plan.table_page_id, self.bpm.clone());
                let index =
                    BPTIndex::open(self.bpm.clone(), plan.index_page_id, table.schema.as_ref());
                let begin_datums = plan.begin_datums.unwrap_or_else(|| index.first_key());
                let end_datums = plan.end_datums.unwrap_or_else(|| index.last_key());
                Ok(ExecutorImpl::IndexScan(IndexScanExecutor::new(
                    table,
                    index,
                    begin_datums,
                    end_datums,
                    self.bpm.clone(),
                    plan.with_record_id,
                )))
            }
            Plan::DropTable(plan) => Ok(ExecutorImpl::DropTable(DropTableExecutor::new(
                plan.table_name,
                self.catalog.clone(),
                self.bpm.clone(),
            ))),
            Plan::DropDatabase(plan) => Ok(ExecutorImpl::DropDatabase(DropDatabaseExecutor::new(
                plan.database_name,
                self.catalog.clone(),
                self.bpm.clone(),
            ))),
            Plan::Delete(plan) => {
                let child = self.build(*plan.child)?;
                let table = Table::open(plan.table_page_id, self.bpm.clone());
                let indexes = plan
                    .index_page_ids
                    .iter()
                    .map(|page_id| {
                        BPTIndex::open(self.bpm.clone(), *page_id, table.schema.as_ref())
                    })
                    .collect_vec();
                Ok(ExecutorImpl::Delete(DeleteExecutor::new(
                    Box::new(child),
                    indexes,
                    table,
                    self.bpm.clone(),
                )))
            }
            Plan::NestedLoopJoin(plan) => {
                let children = plan
                    .children
                    .into_iter()
                    .map(|c| self.build(c).unwrap())
                    .collect_vec();
                Ok(ExecutorImpl::NestedLoopJoin(NestedLoopJoinExecutor::new(
                    self.bpm.clone(),
                    children,
                    plan.schema,
                )))
            }
            Plan::LoadFromFile(plan) => Ok(ExecutorImpl::LoadFromFile(LoadFromFileExecutor::new(
                plan.schema.clone(),
                plan.file_name,
                self.bpm.clone(),
            ))),
            Plan::Agg(plan) => {
                let child = self.build(*plan.child)?;
                Ok(ExecutorImpl::Agg(AggExecutor::new(
                    plan.exprs_with_action,
                    plan.group_by_expr,
                    child,
                    self.bpm.clone(),
                )))
            }
            Plan::DropForeign(plan) => Ok(ExecutorImpl::DropForeign(DropForeignExecuor::new(
                plan.table_name,
                plan.column_idxes,
                self.catalog.clone(),
                self.bpm.clone(),
            ))),
            Plan::DropPrimary(plan) => Ok(ExecutorImpl::DropPrimary(DropPrimaryExecutor::new(
                plan.table_name,
                self.catalog.clone(),
                self.bpm.clone(),
            ))),
            Plan::DropIndex(plan) => Ok(ExecutorImpl::DropIndex(DropIndexExecutor::new(
                plan.table_name,
                plan.exprs,
                self.bpm.clone(),
                self.catalog.clone(),
            ))),
            Plan::ShowTables => {
                if self.catalog.borrow().current_database() == None {
                    return Err(ExecutionError::Catalog(CatalogError::NotUsingDatabase));
                }
                Ok(ExecutorImpl::ShowTables(ShowTablesExecutor::new(
                    self.bpm.clone(),
                    self.catalog.clone(),
                )))
            }
        }
    }
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self { bpm, catalog }
    }
    pub fn execute(&mut self, plan: Plan) -> Result<Table, ExecutionError> {
        let mut executor = self.build(plan)?;
        let mut slices = vec![];
        while let Some(slice) = executor.execute()? {
            slices.push(slice);
        }
        let schema = executor.schema();
        Ok(Table::from_slice(slices, schema, self.bpm.clone()))
    }
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),
    #[error("TableError: {0}")]
    Table(#[from] TableError),
    #[error("SchemaError: {0}")]
    Schema(#[from] SchemaError),
    #[error("IndexError: {0}")]
    Index(#[from] IndexError),
    #[error("Insert Duplicated Key: {0:?}")]
    InsertDuplicatedKey(Vec<Datum>),
}
