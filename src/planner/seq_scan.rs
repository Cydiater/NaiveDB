use crate::expr::ExprImpl;
use crate::parser::ast::{SelectStmt, Selectors};
use crate::planner::{Plan, Planner};
use itertools::Itertools;

pub struct SeqScanPlan {
    pub exprs: Vec<ExprImpl>,
    pub is_all: bool,
    pub table_name: String,
}

impl Planner {
    pub fn plan_seq_scan(&self, stmt: SelectStmt) -> Plan {
        match stmt.selectors {
            Selectors::All => Plan::SeqScan(SeqScanPlan {
                exprs: vec![],
                is_all: true,
                table_name: stmt.table_name,
            }),
            Selectors::Exprs(exprs) => {
                let table_name = stmt.table_name;
                let exprs = exprs
                    .into_iter()
                    .map(|node| {
                        ExprImpl::from_ast(node, self.catalog.clone(), Some(table_name.clone()))
                            .unwrap()
                    })
                    .collect_vec();
                Plan::SeqScan(SeqScanPlan {
                    exprs,
                    is_all: false,
                    table_name,
                })
            }
        }
    }
}
