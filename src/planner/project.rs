use crate::expr::ExprImpl;
use crate::planner::Plan;

pub struct ProjectPlan {
    pub exprs: Vec<ExprImpl>,
    pub child: Box<Plan>,
}
