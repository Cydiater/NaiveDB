use crate::execution::ExecutorImpl;
use crate::expr::ExprImpl;

#[allow(dead_code)]
pub struct ProjectPlan {
    exprs: Vec<ExprImpl>,
    child: Box<ExecutorImpl>,
}
