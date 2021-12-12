use crate::execution::{ExecutionError, Executor};
use crate::index::BPTIndex;
use crate::table::{SchemaRef, Slice};
use std::rc::Rc;

pub struct IndexScanExecutor {
    index: BPTIndex,
}

impl Executor for IndexScanExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(self.index.get_key_schema())
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        todo!()
    }
}
