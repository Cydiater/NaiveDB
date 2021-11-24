use crate::execution::ExecutorImpl;
use crate::expr::ExprImpl;

pub struct ProjectPlan {
    exprs: Vec<ExprImpl>,
    child: Box<ExecutorImpl>,
}
