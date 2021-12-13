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
    end_datums: Vec<Datum>,
    bpm: BufferPoolManagerRef,
    done: bool,
}

impl Executor for IndexScanExecutor {
    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.done {
            return Ok(None);
        }
        let mut output = Slice::new(self.bpm.clone(), Rc::new(self.index.get_key_schema()));
        let iter = self.index.iter_start_from(&self.begin_datums).unwrap();
        for (datums, record_id) in iter {
            if datums > self.end_datums {
                break;
            }
            let datums = self.table.datums_from_record_id(record_id);
            if !output.ok_to_add(&datums) {
                self.begin_datums = datums;
                return Ok(Some(output));
            }
            output.add(&datums)?;
        }
        self.done = true;
        Ok(Some(output))
    }
}
