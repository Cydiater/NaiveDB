use crate::parser::ast::DropTableStmt;
use crate::planner::{Plan, Planner};

#[derive(Debug)]
pub struct DropTablePlan {
    pub table_name: String,
}

impl Planner {
    pub fn plan_drop_table(&self, stmt: DropTableStmt) -> Plan {
        Plan::DropTable(DropTablePlan {
            table_name: stmt.table_name,
        })
    }
}
