use crate::execution::{ExecutionError, Executor};
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice, Table, Schema};
use crate::datum::{Datum, DataType};
use std::rc::Rc;

pub struct IndexScanExecutor {
    table: Table,
    index: BPTIndex,
    begin_datums: Vec<Datum>,
    end_datums: Vec<Datum>,
    bpm: BufferPoolManagerRef,
    done: bool,
    with_record_id: bool,
}

impl IndexScanExecutor {
    pub fn new(
        table: Table,
        index: BPTIndex,
        begin_datums: Vec<Datum>,
        end_datums: Vec<Datum>,
        bpm: BufferPoolManagerRef,
        with_record_id: bool,
    ) -> Self {
        Self {
            table,
            index,
            begin_datums,
            end_datums,
            bpm,
            done: false,
            with_record_id,
        }
    }
}

impl Executor for IndexScanExecutor {
    fn schema(&self) -> SchemaRef {
        if !self.with_record_id {
            self.table.schema.clone()
        } else {
            let mut type_and_names = self.table.schema.to_vec();
            type_and_names.push((DataType::new_int(false), "_page_id".to_string()));
            type_and_names.push((DataType::new_int(false), "_idx".to_string()));
            Rc::new(Schema::from_slice(&type_and_names))
        }
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.done {
            return Ok(None);
        }
        let mut output = Slice::new(self.bpm.clone(), self.schema());
        let iter = self.index.iter_start_from(&self.begin_datums).unwrap();
        for (datums, record_id) in iter {
            if datums > self.end_datums {
                break;
            }
            let mut datums = self.table.datums_from_record_id(record_id).unwrap();
            if self.with_record_id {
                datums.push(Datum::Int(Some(record_id.0 as i32)));
                datums.push(Datum::Int(Some(record_id.1 as i32)));
            }
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
