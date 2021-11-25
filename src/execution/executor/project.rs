use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::table::{Datum, Slice};

#[allow(dead_code)]
pub struct ProjectExecutor {
    exprs: Vec<ExprImpl>,
    child: Box<ExecutorImpl>,
    buffer: Vec<Vec<Datum>>,
}

impl Executor for ProjectExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        todo!()
    }
}
