use crate::expr::ExprImpl;
use crate::parser::ast::ExprNode;
use crate::planner::{Plan, Planner};
use crate::table::Schema;
use itertools::Itertools;

#[derive(Debug)]
pub struct FilterPlan {
    pub exprs: Vec<ExprImpl>,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_filter(&self, schema: &Schema, where_exprs: &[ExprNode], plan: Plan) -> Plan {
        let exprs = where_exprs
            .iter()
            .map(|node| ExprImpl::from_ast(node, self.catalog.clone(), schema, None).unwrap())
            .collect_vec();
        match exprs.is_empty() {
            true => plan,
            false => Plan::Filter(FilterPlan {
                exprs,
                child: Box::new(plan),
            }),
        }
    }
}
