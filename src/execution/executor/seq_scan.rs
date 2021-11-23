use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor};
use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::Slice;

#[allow(dead_code)]
pub struct SeqScanExecutor {
    bpm: BufferPoolManagerRef,
    catalog: CatalogManagerRef,
    page_id: PageID,
}

impl Executor for SeqScanExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        todo!()
    }
}
