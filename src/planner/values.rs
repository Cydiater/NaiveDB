use crate::expr::ExprImpl;
use crate::parser::ast::ExprNode;
use crate::planner::{Plan, PlanError, Planner};
use crate::table::{SchemaError, SchemaRef};

#[derive(Debug)]
pub struct ValuesPlan {
    pub values: Vec<Vec<ExprImpl>>,
    pub schema: SchemaRef,
}

impl Planner {
    pub fn plan_values(
        &self,
        values: Vec<Vec<ExprNode>>,
        schema: SchemaRef,
    ) -> Result<Plan, PlanError> {
        let values = values
            .into_iter()
            .map(|nodes| {
                if nodes.len() != schema.columns.len() {
                    Err(PlanError::Schema(SchemaError::NotMatch))
                } else {
                    nodes
                        .into_iter()
                        .zip(schema.columns.iter())
                        .map(|(node, col)| {
                            ExprImpl::from_ast(
                                &node,
                                self.catalog.clone(),
                                &schema,
                                Some(col.data_type),
                            )
                            .map_err(|e| e.into())
                        })
                        .collect::<Result<_, PlanError>>()
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(Plan::Values(ValuesPlan { values, schema }))
    }
}
