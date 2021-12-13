use crate::expr::ExprImpl;
use crate::parser::ast::ExprNode;
use crate::planner::{Plan, Planner};
use crate::table::SchemaRef;
use itertools::Itertools;

#[derive(Debug)]
pub struct ValuesPlan {
    pub values: Vec<Vec<ExprImpl>>,
    pub schema: SchemaRef,
}

impl Planner {
    pub fn plan_values(&self, values: Vec<Vec<ExprNode>>, schema: SchemaRef) -> Plan {
        let values = values
            .into_iter()
            .map(|nodes| {
                nodes
                    .into_iter()
                    .zip(schema.iter())
                    .map(|(node, col)| {
                        ExprImpl::from_ast(&node, self.catalog.clone(), None, Some(&col.data_type))
                            .unwrap()
                    })
                    .collect_vec()
            })
            .collect_vec();
        Plan::Values(ValuesPlan { values, schema })
    }
}
