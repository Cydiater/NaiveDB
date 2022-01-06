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
        let table = self.catalog.borrow().find_table(&stmt.table_name).unwrap();
        let exprs = stmt
            .exprs
            .into_iter()
            .map(|node| {
                ExprImpl::from_ast(&node, self.catalog.clone(), table.schema.as_ref(), None)
                    .unwrap()
            })
            .collect_vec();
        Plan::AddIndex(AddIndexPlan {
            table_name: stmt.table_name,
            exprs,
        })
    }
}
