use crate::parser::ast::InsertStmt;
use crate::planner::{Plan, PlanError, Planner};

#[derive(Debug)]
pub struct InsertPlan {
    pub table_name: String,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_insert_from_values(&self, stmt: InsertStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let child = self.plan_values(stmt.values, table.schema.clone())?;
        self.plan_insert(&stmt.table_name, child)
    }
    pub fn plan_insert(&self, table_name: &str, child: Plan) -> Result<Plan, PlanError> {
        Ok(Plan::Insert(InsertPlan {
            table_name: table_name.to_owned(),
            child: Box::new(child),
        }))
    }
}
