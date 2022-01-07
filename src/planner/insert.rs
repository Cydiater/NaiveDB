use crate::parser::ast::InsertStmt;
use crate::planner::{Plan, PlanError, Planner};

#[derive(Debug)]
pub struct InsertPlan {
    pub table_name: String,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_insert(&self, stmt: InsertStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let child = Box::new(self.plan_values(stmt.values, table.schema.clone())?);
        Ok(Plan::Insert(InsertPlan {
            table_name: stmt.table_name,
            child,
        }))
    }
}
