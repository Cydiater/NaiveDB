use crate::datum::Datum;
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice};

pub struct UpdateExecutor {
    bpm: BufferPoolManagerRef,
    child: Box<ExecutorImpl>,
    column_idx_with_values: Vec<(usize, Datum)>,
    schema: SchemaRef,
    buffer: Vec<Vec<Datum>>,
}

impl UpdateExecutor {
    pub fn new(
        column_idx_with_values: Vec<(usize, Datum)>,
        schema: SchemaRef,
        bpm: BufferPoolManagerRef,
        child: ExecutorImpl,
    ) -> Self {
        Self {
            bpm,
            child: Box::new(child),
            column_idx_with_values,
            schema,
            buffer: vec![],
        }
    }
}

impl Executor for UpdateExecutor {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        while let Some(slice) = self.child.execute()? {
            for mut tuple in slice.tuple_iter() {
                for (idx, datum) in &self.column_idx_with_values {
                    tuple[*idx] = datum.clone();
                }
                self.buffer.push(tuple)
            }
        }
        let mut output = Slice::new(self.bpm.clone(), self.schema());
        while !self.buffer.is_empty() {
            if output.insert(self.buffer.last().unwrap()).is_ok() {
                self.buffer.pop();
            } else {
                break;
            }
        }
        if output.count() > 0 {
            Ok(Some(output))
        } else {
            Ok(None)
        }
    }
}
