use crate::parser::ast::AddIndexStmt;
use crate::planner::{Plan, Planner};

#[derive(Debug)]
#[allow(dead_code)]
pub struct AddIndexPlan {
    table_name: String,
}

impl Planner {
    pub fn plan_add_index(&self, _stmt: AddIndexStmt) -> Plan {
        todo!()
    }
}
