use crate::parser::ast::{DropDatabaseStmt, DropTableStmt};
use crate::planner::{Plan, PlanError, Planner};

#[derive(Debug)]
pub struct DropTablePlan {
    pub table_name: String,
}

#[derive(Debug)]
pub struct DropDatabasePlan {
    pub database_name: String,
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
}
