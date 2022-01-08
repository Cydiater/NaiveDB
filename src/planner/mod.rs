use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::parser::ast::Statement;
use log::info;
use thiserror::Error;

pub use agg::AggPlan;
pub use alter::{AddForeignPlan, AddIndexPlan, AddPrimaryPlan, AddUniquePlan};
pub use create_database::CreateDatabasePlan;
pub use create_table::CreateTablePlan;
pub use delete::DeletePlan;
pub use desc::DescPlan;
pub use drop::{DropDatabasePlan, DropTablePlan};
pub use filter::FilterPlan;
pub use insert::InsertPlan;
pub use load_from_file::LoadFromFilePlan;
pub use nested_loop_join::NestedLoopJoinPlan;
pub use scan::{IndexScanPlan, SeqScanPlan};
pub use select::ProjectPlan;
pub use use_database::UseDatabasePlan;
pub use values::ValuesPlan;

mod agg;
mod alter;
mod create_database;
mod create_table;
mod delete;
mod desc;
mod drop;
mod filter;
mod insert;
mod load_from_file;
mod nested_loop_join;
mod scan;
mod select;
mod use_database;
mod values;

#[derive(Debug)]
pub enum Plan {
    CreateDatabase(CreateDatabasePlan),
    ShowDatabases,
    ShowTables,
    UseDatabase(UseDatabasePlan),
    DropDatabase(DropDatabasePlan),
    CreateTable(CreateTablePlan),
    Values(ValuesPlan),
    Insert(InsertPlan),
    Desc(DescPlan),
    SeqScan(SeqScanPlan),
    Project(ProjectPlan),
    Filter(FilterPlan),
    AddIndex(AddIndexPlan),
    AddUnique(AddUniquePlan),
    AddPrimary(AddPrimaryPlan),
    AddForeign(AddForeignPlan),
    IndexScan(IndexScanPlan),
    DropTable(DropTablePlan),
    Delete(DeletePlan),
    NestedLoopJoin(NestedLoopJoinPlan),
    LoadFromFile(LoadFromFilePlan),
    Agg(AggPlan),
}

pub struct Planner {
    catalog: CatalogManagerRef,
}

impl Planner {
    pub fn new(catalog: CatalogManagerRef) -> Self {
        Self { catalog }
    }
    pub fn plan(&self, stmt: Statement) -> Result<Plan, PlanError> {
        info!("plan with statement {:#?}", stmt);
        match stmt {
            Statement::CreateDatabase(stmt) => self.plan_create_database(stmt),
            Statement::ShowDatabases => Ok(Plan::ShowDatabases),
            Statement::ShowTables => Ok(Plan::ShowTables),
            Statement::UseDatabase(stmt) => self.plan_use_database(stmt),
            Statement::CreateTable(stmt) => self.plan_create_table(stmt),
            Statement::Insert(stmt) => self.plan_insert(stmt),
            Statement::Desc(stmt) => self.plan_desc(stmt),
            Statement::Select(stmt) => self.plan_select(stmt),
            Statement::AddIndex(stmt) => self.plan_add_index(stmt),
            Statement::AddPrimary(stmt) => self.plan_add_primary(stmt),
            Statement::AddForeign(stmt) => self.plan_add_foreign(stmt),
            Statement::AddUnique(stmt) => self.plan_add_unique(stmt),
            Statement::DropTable(stmt) => self.plan_drop_table(stmt),
            Statement::DropDatabase(stmt) => self.plan_drop_database(stmt),
            Statement::Delete(stmt) => self.plan_delete(stmt),
            Statement::LoadFromFile(stmt) => self.plan_load_from_file(stmt),
        }
    }
}

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("CatalogError: {0}")]
    Catalog(#[from] CatalogError),
}

#[cfg(test)]
mod tests {
    use crate::catalog::CatalogManager;
    use crate::parser::ast::{CreateDatabaseStmt, Statement};
    use crate::planner::{Plan, Planner};
    use crate::storage::BufferPoolManager;
    use std::fs::remove_file;

    #[test]
    fn test_gen_create_database_plan() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let catalog = CatalogManager::new_shared(bpm.clone());
            let filename = bpm.borrow().filename();
            let planner = Planner::new(catalog);
            let stmt = Statement::CreateDatabase(CreateDatabaseStmt {
                database_name: "sample_database".to_string(),
            });
            let plan = planner.plan(stmt).unwrap();
            if let Plan::CreateDatabase(plan) = plan {
                assert_eq!(plan.database_name, "sample_database");
            } else {
                panic!("not create_database plan");
            }
            filename
        };
        remove_file(filename).unwrap();
    }
}
