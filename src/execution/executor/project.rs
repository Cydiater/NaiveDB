use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Datum, Schema, Slice};
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
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let type_and_names = self
            .exprs
            .iter()
            .map(|e| (e.return_type(), e.name()))
            .collect_vec();
        let schema = Rc::new(Schema::from_slice(type_and_names.as_slice()));
        let mut slice = Slice::new_empty(self.bpm.clone(), schema);
        loop {
            if self.buffer.is_empty() {
                let from_child = self.child.execute()?;
                if let Some(from_child) = from_child {
                    let len = from_child.len();
                    for idx in 0..len {
                        self.buffer.push(from_child.at(idx).unwrap());
                    }
                } else if slice.len() == 0 {
                    return Ok(None);
                } else {
                    return Ok(Some(slice));
                }
            }
            if slice.ok_to_add(&self.buffer[0]) {
                slice.add(self.buffer.remove(0))?;
            } else {
                break;
            }
        }
        Ok(Some(slice))
    }
}
