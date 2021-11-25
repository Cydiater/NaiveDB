use crate::parser::ast::{CreateTableStmt, Field};
use crate::planner::{Plan, Planner};
use crate::table::Schema;
use itertools::Itertools;

#[derive(Debug)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub schema: Schema,
}

impl Planner {
    pub fn plan_create_table(&self, stmt: CreateTableStmt) -> Plan {
        let slice = stmt
            .fields
            .iter()
            .map(|f| match f {
                Field::Normal(f) => (f.field_data_type, f.field_name.clone()),
            })
            .collect_vec();
        Plan::CreateTable(CreateTablePlan {
            table_name: stmt.table_name,
            schema: Schema::from_slice(slice.as_slice()),
        })
    }
}
