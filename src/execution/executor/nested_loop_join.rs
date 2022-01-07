use super::{ExecutionError, Executor, ExecutorImpl};
use crate::datum::Datum;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice};
use itertools::Itertools;

pub struct NestedLoopJoinExecutor {
    schema: SchemaRef,
    children: Vec<ExecutorImpl>,
    buffer: Vec<Vec<Datum>>,
    bpm: BufferPoolManagerRef,
    initialized: bool,
}

impl NestedLoopJoinExecutor {
    pub fn new(bpm: BufferPoolManagerRef, children: Vec<ExecutorImpl>, schema: SchemaRef) -> Self {
        Self {
            schema,
            children,
            bpm,
            buffer: vec![],
            initialized: false,
        }
    }
}

impl Executor for NestedLoopJoinExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.initialized {
            let mut buffers = self
                .children
                .iter_mut()
                .map(|child| {
                    let mut buffer = vec![];
                    while let Some(slice) = child.execute().unwrap() {
                        buffer.extend(slice.tuple_iter().collect_vec());
                    }
                    buffer.into_iter()
                })
                .collect_vec();
            let join_iter = buffers.remove(0);
            self.buffer = buffers
                .into_iter()
                .fold(join_iter, |iter, buffer| {
                    iter.cartesian_product(buffer.into_iter())
                        .map(|(t0, t1)| [t0, t1].concat())
                        .collect_vec()
                        .into_iter()
                })
                .rev()
                .collect_vec();
            self.initialized = true;
        }
        let mut slice = Slice::new(self.bpm.clone(), self.schema.clone());
        while !self.buffer.is_empty() {
            if slice.insert(self.buffer.last().unwrap()).is_ok() {
                self.buffer.pop();
            } else {
                break;
            }
        }
        if slice.count() == 0 {
            Ok(None)
        } else {
            Ok(Some(slice))
        }
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
