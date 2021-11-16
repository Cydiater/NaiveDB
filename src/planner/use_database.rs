use super::{Plan, Planner};
use crate::parser::ast::UseDatabaseStmt;

pub struct UseDatabasePlan {
    pub database_name: String,
}

impl Planner {
    pub fn plan_use_database(&self, stmt: UseDatabaseStmt) -> Plan {
        Plan::UseDatabase(UseDatabasePlan {
            database_name: stmt.database_name,
        })
    }
}
