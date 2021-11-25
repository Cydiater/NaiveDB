use crate::parser::ast::DescStmt;
use crate::planner::{Plan, Planner};

#[derive(Debug)]
pub struct DescPlan {
    pub table_name: String,
}

impl Planner {
    pub fn plan_desc(&self, stmt: DescStmt) -> Plan {
        Plan::Desc(DescPlan {
            table_name: stmt.table_name,
        })
    }
}
