use crate::datum::Datum;
use crate::expr::ExprImpl;
use crate::parser::ast::{SelectStmt, Selectors};
use crate::planner::{Plan, Planner};
use crate::storage::PageID;
use itertools::Itertools;

#[allow(dead_code)]
#[derive(Debug)]
pub struct IndexScanPlan {
    begin_datums: Option<Vec<Datum>>,
    end_datums: Option<Vec<Datum>>,
    table_page_id: PageID,
    index_page_id: PageID,
}

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
        // SeqScan || IndexScan
        let plan = if let Some(where_exprs) = &stmt.where_exprs {
            let table_name = stmt.table_name.clone();
            let indexes = self
                .catalog
                .borrow()
                .find_indexes_by_table(stmt.table_name.clone())
                .unwrap();
            let where_exprs = where_exprs
                .iter()
                .map(|node| {
                    ExprImpl::from_ast(node, self.catalog.clone(), Some(table_name.clone()), None)
                        .unwrap()
                })
                .collect_vec();
            let mut index_scan = None;
            for index in indexes {
                let index_exprs = index.get_exprs();
                let mut begin: Vec<Option<Datum>> = vec![None; index_exprs.len()];
                let mut end: Vec<Option<Datum>> = vec![None; index_exprs.len()];
                for (idx, index_expr) in index_exprs.iter().enumerate() {
                    for where_expr in &where_exprs {
                        if let ExprImpl::Binary(binary_expr) = where_expr {
                            let bound = binary_expr.get_bound(index_expr);
                            if let Some(d) = bound.0 {
                                begin[idx] = Some(d);
                            }
                            if let Some(d) = bound.1 {
                                end[idx] = Some(d);
                            }
                        }
                    }
                }
                let begin = if begin.iter().all(|b| matches!(b, Some(_))) {
                    Some(begin.into_iter().map(|b| b.unwrap()).collect_vec())
                } else {
                    None
                };
                let end = if end.iter().all(|b| matches!(b, Some(_))) {
                    Some(end.into_iter().map(|b| b.unwrap()).collect_vec())
                } else {
                    None
                };
                if begin.is_some() || end.is_some() {
                    index_scan = Some(Plan::IndexScan(IndexScanPlan {
                        begin_datums: begin,
                        end_datums: end,
                        table_page_id: self
                            .catalog
                            .borrow()
                            .find_table(table_name)
                            .unwrap()
                            .get_page_id(),
                        index_page_id: index.get_page_id(),
                    }));
                    break;
                }
            }
            if let Some(index_scan) = index_scan {
                index_scan
            } else {
                Plan::SeqScan(SeqScanPlan {
                    table_name: stmt.table_name.clone(),
                })
            }
        } else {
            Plan::SeqScan(SeqScanPlan {
                table_name: stmt.table_name.clone(),
            })
        };
        // Filter
        let plan = if let Some(exprs) = stmt.where_exprs {
            let table_name = stmt.table_name.clone();
            let exprs = exprs
                .into_iter()
                .map(|node| {
                    ExprImpl::from_ast(&node, self.catalog.clone(), Some(table_name.clone()), None)
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
                    ExprImpl::from_ast(&node, self.catalog.clone(), Some(table_name.clone()), None)
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
