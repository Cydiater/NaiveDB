use crate::datum::Datum;
use crate::execution::{ExecutionError, Executor};
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice, Table};
use std::rc::Rc;

pub struct IndexScanExecutor {
    table: Table,
    index: BPTIndex,
    begin_datums: Vec<Datum>,
    include_begin: bool,
    end_datums: Vec<Datum>,
    include_end: bool,
    bpm: BufferPoolManagerRef,
}

impl Executor for IndexScanExecutor {
    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let mut _output = Slice::new(self.bpm.clone(), Rc::new(self.index.get_key_schema()));
        let mut iter = self.index.iter_start_from(&self.begin_datums).unwrap();
        if !self.include_begin {
            iter.next();
            self.include_begin = true;
        }
        for (datums, _record_id) in iter {
            if datums == self.end_datums && !self.include_end {
                break;
            }
            if datums > self.end_datums {
                break;
            }
        }
        todo!()
    }
}
