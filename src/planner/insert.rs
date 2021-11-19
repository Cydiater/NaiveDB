use crate::expr::ExprImpl;
use crate::parser::ast::InsertStmt;
use crate::planner::{Plan, Planner};

pub struct InsertPlan {
    pub table_name: String,
    pub values: Vec<Vec<ExprImpl>>,
}

impl Planner {
    pub fn plan_insert(&self, stmt: InsertStmt) -> Plan {
        Plan::Insert(InsertPlan {
            table_name: stmt.table_name,
            values: stmt.values,
        })
    }
}
