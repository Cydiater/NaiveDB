use crate::parser::ast::{CreateTableStmt, Field};
use crate::planner::{Plan, PlanError, Planner};
use crate::table::{Schema, SchemaError};
use itertools::Itertools;

#[derive(Debug)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub schema: Schema,
}

impl Planner {
    pub fn plan_create_table(&self, stmt: CreateTableStmt) -> Result<Plan, PlanError> {
        let slice = stmt
            .fields
            .iter()
            .filter_map(|f| match f {
                Field::Normal(f) => Some((f.field_data_type, f.field_name.clone())),
                _ => None,
            })
            .collect_vec();
        let mut schema = Schema::from_type_and_names(&slice);
        // primary field
        let primary = stmt.fields.iter().find(|f| matches!(f, Field::Primary(_)));
        if let Some(Field::Primary(primary)) = primary {
            schema.primary = primary
                .column_names
                .iter()
                .map(|column_name| {
                    schema
                        .index_by_column_name(column_name)
                        .ok_or(SchemaError::ColumnNotFound)
                })
                .collect::<Result<_, _>>()?;
        }
        // foreign field
        for field in &stmt.fields {
            if let Field::Foreign(foreign) = field {
                let ref_table = self.catalog.borrow().find_table(&foreign.ref_table_name)?;
                let mut vec = vec![];
                for (column_name, ref_column_name) in foreign
                    .column_names
                    .iter()
                    .zip(foreign.ref_column_names.iter())
                {
                    let idx = schema
                        .index_by_column_name(column_name)
                        .ok_or(SchemaError::ColumnNotFound)?;
                    let ref_idx = ref_table
                        .schema
                        .index_by_column_name(&ref_column_name)
                        .ok_or(SchemaError::ColumnNotFound)?;
                    vec.push((idx, ref_idx))
                }
                schema.foreign.push((ref_table.get_page_id(), vec));
            }
        }
        // unique field
        for field in &stmt.fields {
            if let Field::Unique(unique) = field {
                let unique_set = unique
                    .column_names
                    .iter()
                    .map(|column_name| {
                        schema
                            .index_by_column_name(column_name)
                            .ok_or(SchemaError::ColumnNotFound)
                    })
                    .collect::<Result<_, _>>()?;
                schema.unique.push(unique_set);
            }
        }
        Ok(Plan::CreateTable(CreateTablePlan {
            table_name: stmt.table_name,
            schema,
        }))
    }
}
