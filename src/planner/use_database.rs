use super::{Plan, PlanError, Planner};
use crate::parser::ast::UseDatabaseStmt;

#[derive(Debug)]
pub struct UseDatabasePlan {
    pub database_name: String,
}

impl Planner {
    pub fn plan_use_database(&self, stmt: UseDatabaseStmt) -> Result<Plan, PlanError> {
        Ok(Plan::UseDatabase(UseDatabasePlan {
            database_name: stmt.database_name,
        }))
    }
}
