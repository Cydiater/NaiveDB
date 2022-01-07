use crate::datum::DataType;
use crate::expr::{ConstantExpr, ExprImpl};
use crate::parser::ast::{AggAction, AggTarget};
use crate::planner::{Plan, PlanError, Planner};
use crate::table::Schema;

#[derive(Debug)]
pub struct AggPlan {
    pub expr: ExprImpl,
    pub action: AggAction,
    pub child: Box<Plan>,
}

impl Planner {
    pub fn plan_agg(
        &self,
        schema: &Schema,
        action: AggAction,
        target: AggTarget,
        child: Plan,
    ) -> Result<Plan, PlanError> {
        let expr = match target {
            AggTarget::All => {
                ExprImpl::Constant(ConstantExpr::new(1.into(), DataType::new_as_int(false)))
            }
            AggTarget::Expr(expr) => {
                ExprImpl::from_ast(&expr, self.catalog.clone(), schema, None).unwrap()
            }
        };
        Ok(Plan::Agg(AggPlan {
            expr,
            action,
            child: Box::new(child),
        }))
    }
}
