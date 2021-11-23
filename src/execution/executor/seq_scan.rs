use crate::execution::{ExecutionError, Executor};
use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::{SchemaRef, Slice};

pub struct SeqScanExecutor {
    bpm: BufferPoolManagerRef,
    page_id: Option<PageID>,
    schema: SchemaRef,
}

impl SeqScanExecutor {
    pub fn new(bpm: BufferPoolManagerRef, page_id: Option<PageID>, schema: SchemaRef) -> Self {
        Self {
            bpm,
            page_id,
            schema,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if let Some(page_id) = self.page_id {
            let mut slice = Slice::new(self.bpm.clone(), self.schema.clone());
            slice.attach(page_id);
            self.page_id = slice.get_next_page_id();
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }
}
