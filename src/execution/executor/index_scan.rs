use crate::datum::Datum;
use crate::execution::{ExecutionError, Executor};
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice, Table};

pub struct IndexScanExecutor {
    table: Table,
    index: BPTIndex,
    begin_datums: Vec<Datum>,
    end_datums: Vec<Datum>,
    bpm: BufferPoolManagerRef,
    done: bool,
}

impl IndexScanExecutor {
    pub fn new(
        table: Table,
        index: BPTIndex,
        begin_datums: Vec<Datum>,
        end_datums: Vec<Datum>,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            table,
            index,
            begin_datums,
            end_datums,
            bpm,
            done: false,
        }
    }
}

impl Executor for IndexScanExecutor {
    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.done {
            return Ok(None);
        }
        let mut output = Slice::new(self.bpm.clone(), self.table.schema.clone());
        let iter = self.index.iter_start_from(&self.begin_datums).unwrap();
        for (datums, record_id) in iter {
            if datums > self.end_datums {
                break;
            }
            let datums = self.table.datums_from_record_id(record_id).unwrap();
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
