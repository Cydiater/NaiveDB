use super::{Plan, PlanError, Planner};
use crate::parser::ast::CreateDatabaseStmt;

#[derive(Debug)]
pub struct CreateDatabasePlan {
    pub database_name: String,
}

impl Planner {
    pub fn plan_create_database(&self, stmt: CreateDatabaseStmt) -> Result<Plan, PlanError> {
        Ok(Plan::CreateDatabase(CreateDatabasePlan {
            database_name: stmt.database_name,
        }))
    }
}
