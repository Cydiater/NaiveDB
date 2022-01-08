use crate::datum::DataType;
use crate::expr::{ConstantExpr, ExprImpl};
use crate::parser::ast::{AggAction, AggItem, AggTarget, ExprNode};
use crate::planner::{Plan, PlanError, Planner};
use crate::table::Schema;
use itertools::Itertools;

#[derive(Debug)]
pub struct AggPlan {
    pub exprs_with_action: Vec<(ExprImpl, AggAction)>,
    pub group_by_expr: Option<ExprImpl>,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_agg(
        &self,
        schema: &Schema,
        items: Vec<AggItem>,
        group_by_expr: Option<ExprNode>,
        child: Plan,
    ) -> Result<Plan, PlanError> {
        let exprs_with_action = items
            .into_iter()
            .map(|item| {
                let expr = match item.target {
                    AggTarget::All => {
                        ExprImpl::Constant(ConstantExpr::new(1.into(), DataType::new_as_int(false)))
                    }
                    AggTarget::Expr(expr) => {
                        ExprImpl::from_ast(&expr, self.catalog.clone(), schema, None).unwrap()
                    }
                };
                (expr, item.action)
            })
            .collect_vec();
        let group_by_expr = group_by_expr
            .as_ref()
            .map(|node| ExprImpl::from_ast(node, self.catalog.clone(), schema, None).unwrap());
        Ok(Plan::Agg(AggPlan {
            exprs_with_action,
            group_by_expr,
            child: Box::new(child),
        }))
    }
}
