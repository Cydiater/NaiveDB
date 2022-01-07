use crate::parser::ast::DescStmt;
use crate::planner::{Plan, PlanError, Planner};

#[derive(Debug)]
pub struct DescPlan {
    pub table_name: String,
}

impl Planner {
    pub fn plan_desc(&self, stmt: DescStmt) -> Result<Plan, PlanError> {
        Ok(Plan::Desc(DescPlan {
            table_name: stmt.table_name,
        }))
    }
}
