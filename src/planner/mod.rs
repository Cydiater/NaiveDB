use crate::catalog::CatalogManagerRef;
use crate::parser::ast::Statement;
use crate::storage::BufferPoolManagerRef;
pub use create_database::CreateDatabasePlan;
pub use create_table::CreateTablePlan;
pub use insert::InsertPlan;
pub use use_database::UseDatabasePlan;
pub use values::ValuesPlan;

mod create_database;
mod create_table;
mod insert;
mod use_database;
mod values;

#[allow(dead_code)]
pub enum Plan {
    CreateDatabase(CreateDatabasePlan),
    ShowDatabases,
    UseDatabase(UseDatabasePlan),
    CreateTable(CreateTablePlan),
    Values(ValuesPlan),
    Insert(InsertPlan),
}

#[allow(dead_code)]
pub struct Planner {
    catalog: CatalogManagerRef,
    bpm: BufferPoolManagerRef,
}

impl Planner {
    pub fn new(catalog: CatalogManagerRef, bpm: BufferPoolManagerRef) -> Self {
        Self { catalog, bpm }
    }

    pub fn plan(&self, stmt: Statement) -> Plan {
        match stmt {
            Statement::CreateDatabase(stmt) => self.plan_create_database(stmt),
            Statement::ShowDatabases => Plan::ShowDatabases,
            Statement::UseDatabase(stmt) => self.plan_use_database(stmt),
            Statement::CreateTable(stmt) => self.plan_create_table(stmt),
            Statement::Insert(stmt) => self.plan_insert(stmt),
        }
    }
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
            let planner = Planner::new(catalog, bpm);
            let stmt = Statement::CreateDatabase(CreateDatabaseStmt {
                database_name: "sample_database".to_string(),
            });
            let plan = planner.plan(stmt);
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
