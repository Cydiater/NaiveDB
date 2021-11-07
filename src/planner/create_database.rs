use super::{Plan, Planner};
use crate::parser::ast::CreateDatabaseStmt;

pub struct CreateDatabasePlan {
    pub database_name: String,
}

impl Planner {
    pub fn plan_create_database(&self, stmt: CreateDatabaseStmt) -> Plan {
        Plan::CreateDatabase(CreateDatabasePlan {
            database_name: stmt.database_name,
        })
    }
}
