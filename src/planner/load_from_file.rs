use crate::parser::ast::LoadFromFileStmt;
use crate::planner::{InsertPlan, Plan, PlanError, Planner};
use crate::table::SchemaRef;

#[derive(Debug)]
pub struct LoadFromFilePlan {
    pub schema: SchemaRef,
    pub file_name: String,
}

impl Planner {
    pub fn plan_load_from_file(&self, stmt: LoadFromFileStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let load_plan = Plan::LoadFromFile(LoadFromFilePlan {
            schema: table.schema.clone(),
            file_name: stmt.file_name,
        });
        Ok(Plan::Insert(InsertPlan {
            table_name: stmt.table_name,
            child: Box::new(load_plan),
        }))
    }
}
