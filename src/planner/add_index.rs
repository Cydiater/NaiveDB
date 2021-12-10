use crate::expr::ExprImpl;
use crate::parser::ast::AddIndexStmt;
use crate::planner::{Plan, Planner};
use itertools::Itertools;

#[derive(Debug)]
pub struct AddIndexPlan {
    pub table_name: String,
    pub exprs: Vec<ExprImpl>,
}

impl Planner {
    pub fn plan_add_index(&self, stmt: AddIndexStmt) -> Plan {
        let table_name = stmt.table_name.clone();
        let exprs = stmt
            .exprs
            .into_iter()
            .map(|node| {
                ExprImpl::from_ast(node, self.catalog.clone(), Some(table_name.clone()), None)
                    .unwrap()
            })
            .collect_vec();
        Plan::AddIndex(AddIndexPlan {
            table_name: stmt.table_name,
            exprs,
        })
    }
}
