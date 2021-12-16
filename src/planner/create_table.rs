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
                Field::Normal(f) => Some((f.field_data_type, f.field_name.clone())),
                _ => None,
            })
            .flatten()
            .collect_vec();
        let mut schema = Schema::from_slice(&slice);
        let primary = stmt.fields.iter().find(|f| matches!(f, Field::Primary(_)));
        if let Some(Field::Primary(primary)) = primary {
            for column_name in primary.column_names.iter() {
                schema.set_primary(column_name.clone()).unwrap();
            }
        }
        Plan::CreateTable(CreateTablePlan {
            table_name: stmt.table_name,
            schema,
        })
    }
}
