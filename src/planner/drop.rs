use crate::parser::ast::{DropDatabaseStmt, DropForeignKeyStmt, DropTableStmt};
use crate::planner::{Plan, PlanError, Planner};

#[derive(Debug)]
pub struct DropTablePlan {
    pub table_name: String,
}

#[derive(Debug)]
pub struct DropDatabasePlan {
    pub database_name: String,
}

#[derive(Debug)]
pub struct DropForeignKeyPlan {
    pub table_name: String,
    pub column_names: Vec<String>,
}

impl Planner {
    pub fn plan_drop_table(&self, stmt: DropTableStmt) -> Result<Plan, PlanError> {
        Ok(Plan::DropTable(DropTablePlan {
            table_name: stmt.table_name,
        }))
    }
    pub fn plan_drop_database(&self, stmt: DropDatabaseStmt) -> Result<Plan, PlanError> {
        Ok(Plan::DropDatabase(DropDatabasePlan {
            database_name: stmt.database_name,
        }))
    }
    #[allow(dead_code)]
    pub fn plan_drop_foreign_key(&self, _stmt: DropForeignKeyStmt) -> Result<Plan, PlanError> {
        todo!()
    }
}
