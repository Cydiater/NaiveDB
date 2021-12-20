use crate::datum::Datum;
use crate::expr::ExprImpl;
use crate::parser::ast::ExprNode;
use crate::planner::{Plan, Planner};
use crate::storage::PageID;
use itertools::Itertools;

#[derive(Debug)]
pub struct IndexScanPlan {
    pub begin_datums: Option<Vec<Datum>>,
    pub end_datums: Option<Vec<Datum>>,
    pub table_page_id: PageID,
    pub index_page_id: PageID,
    pub with_record_id: bool,
}

#[derive(Debug)]
pub struct SeqScanPlan {
    pub table_name: String,
    pub with_record_id: bool,
}

impl Planner {
    pub fn plan_scan(
        &self,
        table_name: &str,
        where_exprs: &Option<Vec<ExprNode>>,
        with_record_id: bool,
    ) -> Plan {
        if let Some(where_exprs) = &where_exprs {
            let indexes = self
                .catalog
                .borrow()
                .find_indexes_by_table(table_name)
                .unwrap();
            let where_exprs = where_exprs
                .iter()
                .map(|node| {
                    ExprImpl::from_ast(
                        node,
                        self.catalog.clone(),
                        Some(table_name.to_owned()),
                        None,
                    )
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
                        with_record_id,
                    }));
                    break;
                }
            }
            if let Some(index_scan) = index_scan {
                index_scan
            } else {
                Plan::SeqScan(SeqScanPlan {
                    table_name: table_name.to_owned(),
                    with_record_id,
                })
            }
        } else {
            Plan::SeqScan(SeqScanPlan {
                table_name: table_name.to_owned(),
                with_record_id,
            })
        }
    }
}
