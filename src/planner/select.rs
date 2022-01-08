use crate::catalog::CatalogManagerRef;
use crate::expr::ExprImpl;
use crate::parser::ast::{ExprNode, SelectStmt, Selectors};
use crate::planner::{Plan, PlanError, Planner};
use crate::table::Schema;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug)]
pub struct ProjectPlan {
    pub exprs: Vec<ExprImpl>,
    pub child: Box<Plan>,
}

fn pair_table_name_with_filter(
    table_names: &[String],
    exprs: Vec<ExprNode>,
    catalog: CatalogManagerRef,
) -> (Vec<(String, Vec<ExprNode>)>, Vec<ExprNode>) {
    let mut overall_exprs = vec![];
    let mut table_name_with_exprs = table_names
        .iter()
        .map(|name| (name.clone(), vec![]))
        .collect_vec();
    let column_to_table: HashMap<_, _> = table_names
        .iter()
        .flat_map(|table_name| {
            let table = catalog.borrow().find_table(table_name).unwrap();
            table
                .schema
                .iter()
                .map(|col| (col.desc.to_owned(), table_name.to_owned()))
                .collect_vec()
                .into_iter()
        })
        .collect();
    for expr in exprs {
        match expr {
            ExprNode::Binary(mut expr) => match (expr.lhs.as_mut(), expr.rhs.as_mut()) {
                (ExprNode::ColumnRef(lhs), ExprNode::ColumnRef(rhs)) => {
                    let table_name_lhs = lhs
                        .table_name
                        .as_ref()
                        .unwrap_or_else(|| &column_to_table[&lhs.column_name])
                        .to_owned();
                    let table_name_rhs = rhs
                        .table_name
                        .as_ref()
                        .unwrap_or_else(|| &column_to_table[&rhs.column_name])
                        .to_owned();
                    lhs.table_name = None;
                    lhs.column_name = format!("{}.{}", table_name_lhs, lhs.column_name);
                    rhs.table_name = None;
                    rhs.column_name = format!("{}.{}", table_name_rhs, rhs.column_name);
                    overall_exprs.push(ExprNode::Binary(expr));
                }
                (ExprNode::ColumnRef(column_ref), _) | (_, ExprNode::ColumnRef(column_ref)) => {
                    let table_name = column_ref
                        .table_name
                        .as_ref()
                        .unwrap_or_else(|| &column_to_table[&column_ref.column_name]);
                    let (_, exprs) = table_name_with_exprs
                        .iter_mut()
                        .find(|(name, _)| name == table_name)
                        .unwrap();
                    exprs.push(ExprNode::Binary(expr));
                }
                _ => todo!(),
            },
            ExprNode::Like(expr) => match expr.child.as_ref() {
                ExprNode::ColumnRef(cf) => {
                    let table_name = cf
                        .table_name
                        .as_ref()
                        .unwrap_or_else(|| &column_to_table[&cf.column_name]);
                    let (_, exprs) = table_name_with_exprs
                        .iter_mut()
                        .find(|(name, _)| name == table_name)
                        .unwrap();
                    exprs.push(ExprNode::Like(expr));
                }
                _ => todo!(),
            },
            _ => todo!(),
        }
    }
    (
        table_name_with_exprs.into_iter().collect_vec(),
        overall_exprs,
    )
}

impl Planner {
    pub fn plan_select(&self, stmt: SelectStmt) -> Result<Plan, PlanError> {
        let (table_with_filter_expr, overall) =
            pair_table_name_with_filter(&stmt.table_names, stmt.where_exprs, self.catalog.clone());
        let scan_plans = table_with_filter_expr
            .into_iter()
            .map(|(table_name, exprs)| {
                let plan = self.plan_scan(&table_name, &exprs, false);
                let table = self.catalog.borrow().find_table(&table_name).unwrap();
                if !exprs.is_empty() {
                    self.plan_filter(&table.schema, &exprs, plan)
                } else {
                    plan
                }
            })
            .collect_vec();
        let use_table_name = stmt.table_names.len() > 1;
        let schema = Rc::new(Schema::from_slice(
            &stmt
                .table_names
                .iter()
                .flat_map(|table_name| {
                    let table = self.catalog.borrow().find_table(table_name).unwrap();
                    table
                        .schema
                        .to_vec()
                        .into_iter()
                        .map(|(data_type, column_name)| {
                            if use_table_name {
                                (data_type, format!("{}.{}", table_name, column_name))
                            } else {
                                (data_type, column_name)
                            }
                        })
                        .collect_vec()
                        .into_iter()
                })
                .collect_vec(),
        ));
        let join_plan = self.plan_nested_loop_join(scan_plans, schema.clone());
        let filter_plan = self.plan_filter(&schema, &overall, join_plan);
        match stmt.selectors {
            Selectors::Exprs(exprs) => {
                let exprs = exprs
                    .into_iter()
                    .map(|node| {
                        ExprImpl::from_ast(&node, self.catalog.clone(), &schema, None).unwrap()
                    })
                    .collect_vec();
                Ok(Plan::Project(ProjectPlan {
                    exprs,
                    child: Box::new(filter_plan),
                }))
            }
            Selectors::All => Ok(filter_plan),
            Selectors::Agg { action, target } => {
                Ok(self.plan_agg(&schema, action, target, filter_plan).unwrap())
            }
        }
    }
}
