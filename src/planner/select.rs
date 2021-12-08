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

#[derive(Debug)]
pub struct FilterPlan {
    pub exprs: Vec<ExprImpl>,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_select(&self, stmt: SelectStmt) -> Plan {
        // SeqScan
        let plan = Plan::SeqScan(SeqScanPlan {
            table_name: stmt.table_name.clone(),
        });
        // Filter
        let plan = if let Some(exprs) = stmt.where_exprs {
            let table_name = stmt.table_name.clone();
            let exprs = exprs
                .into_iter()
                .map(|node| {
                    ExprImpl::from_ast(node, self.catalog.clone(), Some(table_name.clone()), None)
                        .unwrap()
                })
                .collect_vec();
            Plan::Filter(FilterPlan {
                exprs,
                child: Box::new(plan),
            })
        } else {
            plan
        };
        // Project
        if let Selectors::Exprs(exprs) = stmt.selectors {
            let table_name = stmt.table_name;
            let exprs = exprs
                .into_iter()
                .map(|node| {
                    ExprImpl::from_ast(node, self.catalog.clone(), Some(table_name.clone()), None)
                        .unwrap()
                })
                .collect_vec();
            Plan::Project(ProjectPlan {
                exprs,
                child: Box::new(plan),
            })
        } else {
            plan
        }
    }
}
