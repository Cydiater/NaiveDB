use crate::expr::ExprImpl;
use crate::parser::ast::{SelectStmt, Selectors};
use crate::planner::{Plan, Planner};
use itertools::Itertools;

#[derive(Debug)]
pub struct SeqScanPlan {
    pub table_name: String,
}

#[derive(Debug)]
pub struct ProjectPlan {
    pub exprs: Vec<ExprImpl>,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_select(&self, stmt: SelectStmt) -> Plan {
        match stmt.selectors {
            Selectors::All => Plan::SeqScan(SeqScanPlan {
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
                let seq_scan = Plan::SeqScan(SeqScanPlan { table_name });
                Plan::Project(ProjectPlan {
                    exprs,
                    child: Box::new(seq_scan),
                })
            }
        }
    }
}
