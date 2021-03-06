use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::storage::{BufferPoolManagerRef, PageID};
use crate::table::{Schema, SchemaRef, Slice};
use std::rc::Rc;

pub struct SeqScanExecutor {
    bpm: BufferPoolManagerRef,
    page_id: Option<PageID>,
    schema: SchemaRef,
    with_record_id: bool,
    buffer: Vec<Vec<Datum>>,
}

impl SeqScanExecutor {
    pub fn new(
        bpm: BufferPoolManagerRef,
        page_id: Option<PageID>,
        schema: SchemaRef,
        with_record_id: bool,
    ) -> Self {
        Self {
            bpm,
            page_id,
            schema,
            with_record_id,
            buffer: vec![],
        }
    }
}

impl Executor for SeqScanExecutor {
    fn schema(&self) -> SchemaRef {
        if !self.with_record_id {
            self.schema.clone()
        } else {
            let mut type_and_names = self.schema.to_type_and_names();
            type_and_names.push((DataType::new_as_int(false), "_page_id".to_string()));
            type_and_names.push((DataType::new_as_int(false), "_idx".to_string()));
            Rc::new(Schema::from_type_and_names(&type_and_names))
        }
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if let Some(page_id) = self.page_id {
            if !self.with_record_id {
                let slice = Slice::open(self.bpm.clone(), self.schema.clone(), page_id);
                self.page_id = slice.meta()?.next_page_id;
                Ok(Some(slice))
            } else {
                let mut slice = Slice::new(self.bpm.clone(), self.schema());
                loop {
                    if self.buffer.is_empty() {
                        if self.page_id.is_none() {
                            break;
                        }
                        let page_id = self.page_id.unwrap();
                        let source = Slice::open(self.bpm.clone(), self.schema.clone(), page_id);
                        for idx in source.slot_iter().collect::<Vec<_>>() {
                            let mut tuple = source.tuple_at(idx)?;
                            tuple.push(Datum::Int(Some(page_id as i32)));
                            tuple.push(Datum::Int(Some(idx as i32)));
                            self.buffer.push(tuple);
                        }
                        self.page_id = source.meta()?.next_page_id;
                    }
                    if slice.insert(&self.buffer[0]).is_ok() {
                        self.buffer.remove(0);
                    } else {
                        return Ok(Some(slice));
                    }
                }
                Ok(Some(slice))
            }
        } else {
            Ok(None)
        }
    }
}
