use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::table::Slice;
use itertools::Itertools;

pub struct ProjectExecutor {
    exprs: Vec<ExprImpl>,
    child: Box<ExecutorImpl>,
}

impl Executor for ProjectExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if let Some(input) = self.child.execute()? {
            let _columns = self
                .exprs
                .iter_mut()
                .map(|e| e.eval(Some(&input)))
                .collect_vec();
            todo!()
        } else {
            Ok(None)
        }
    }
}
