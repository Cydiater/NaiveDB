use crate::datum::{DataType, Datum};
use crate::parser::ast::{ConstantValue, ExprNode, UpdateStmt};
use crate::planner::{Plan, PlanError, Planner};
use crate::table::SchemaError;

#[derive(Debug)]
pub struct UpdatePlan {
    pub table_name: String,
    pub idx_with_values: Vec<(usize, Datum)>,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_update(&self, stmt: UpdateStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let delete_plan = self.plan_delete(&stmt.table_name, &stmt.where_exprs)?;
        let idx_with_values: Vec<(usize, Datum)> = stmt
            .set_exprs
            .iter()
            .map(|e| match e {
                ExprNode::Binary(b) => match (b.lhs.as_ref(), b.rhs.as_ref()) {
                    (ExprNode::ColumnRef(column_ref), ExprNode::Constant(value)) => {
                        let idx = table
                            .schema
                            .index_by_column_name(&column_ref.column_name)
                            .ok_or(SchemaError::ColumnNotFound)?;
                        match (table.schema.columns[idx].data_type, &value.value) {
                            (DataType::Int(_), ConstantValue::Real(value)) => {
                                Ok((idx, (*value as i32).into()))
                            }
                            (DataType::VarChar(_), ConstantValue::String(value)) => {
                                Ok((idx, value.as_str().into()))
                            }
                            (DataType::Float(_), ConstantValue::Real(value)) => {
                                Ok((idx, (*value as f32).into()))
                            }
                            (DataType::Date(_), ConstantValue::Date(value)) => {
                                Ok((idx, (*value).into()))
                            }
                            _ => todo!(),
                        }
                    }
                    _ => todo!(),
                },
                _ => todo!(),
            })
            .collect::<Result<_, SchemaError>>()?;
        let update_plan = Plan::Update(UpdatePlan {
            table_name: stmt.table_name.clone(),
            idx_with_values,
            child: Box::new(delete_plan),
        });
        self.plan_insert(&stmt.table_name, update_plan)
    }
}
