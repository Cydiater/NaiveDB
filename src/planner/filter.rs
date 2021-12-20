use crate::expr::ExprImpl;
use crate::parser::ast::ExprNode;
use crate::planner::{Plan, Planner};
use itertools::Itertools;

#[derive(Debug)]
pub struct FilterPlan {
    pub exprs: Vec<ExprImpl>,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_filter(&self, table_name: &str, where_exprs: &[ExprNode], plan: Plan) -> Plan {
        let exprs = where_exprs
            .iter()
            .map(|node| {
                ExprImpl::from_ast(node, self.catalog.clone(), Some(table_name.to_string()), None)
                    .unwrap()
            })
            .collect_vec();
        Plan::Filter(FilterPlan {
            exprs,
            child: Box::new(plan),
        })
    }
}
