use crate::datum::Datum;
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice};
use itertools::Itertools;
use std::rc::Rc;

pub struct ProjectExecutor {
    exprs: Vec<ExprImpl>,
    child: Box<ExecutorImpl>,
    buffer: Vec<Vec<Datum>>,
    bpm: BufferPoolManagerRef,
}

impl ProjectExecutor {
    pub fn new(exprs: Vec<ExprImpl>, child: Box<ExecutorImpl>, bpm: BufferPoolManagerRef) -> Self {
        ProjectExecutor {
            exprs,
            child,
            buffer: vec![],
            bpm,
        }
    }
}

impl Executor for ProjectExecutor {
    fn schema(&self) -> SchemaRef {
        let type_and_names = self
            .exprs
            .iter()
            .map(|e| (e.return_type(), e.name()))
            .collect_vec();
        Rc::new(Schema::from_slice(type_and_names.as_slice()))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let schema = self.schema();
        let mut slice = Slice::new(self.bpm.clone(), schema);
        loop {
            if self.buffer.is_empty() {
                let from_child = self.child.execute()?;
                if let Some(from_child) = from_child {
                    let mut columns = self
                        .exprs
                        .iter_mut()
                        .map(|e| e.eval(Some(&from_child)))
                        .collect_vec();
                    let len = columns[0].len();
                    for _ in 0..len {
                        let datums = columns.iter_mut().map(|v| v.remove(0)).collect_vec();
                        self.buffer.push(datums);
                    }
                } else if slice.count() == 0 {
                    return Ok(None);
                } else {
                    return Ok(Some(slice));
                }
            }
            if slice.insert(&self.buffer[0]).is_ok() {
                self.buffer.remove(0);
            } else {
                break;
            }
        }
        Ok(Some(slice))
    }
}
