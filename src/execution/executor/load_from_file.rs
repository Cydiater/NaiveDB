use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor};
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice};
use chrono::NaiveDate;
use csv::{Reader, ReaderBuilder};
use itertools::Itertools;
use std::collections::VecDeque;
use std::fs::File;
use std::str::FromStr;

pub struct LoadFromFileExecutor {
    schema: SchemaRef,
    reader: Reader<File>,
    bpm: BufferPoolManagerRef,
    buffer: VecDeque<Vec<Datum>>,
}

impl LoadFromFileExecutor {
    pub fn new(schema: SchemaRef, file_name: String, bpm: BufferPoolManagerRef) -> Self {
        Self {
            schema,
            reader: ReaderBuilder::new()
                .has_headers(false)
                .from_path(file_name)
                .unwrap(),
            bpm,
            buffer: VecDeque::new(),
        }
    }
}

impl Executor for LoadFromFileExecutor {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let mut output = Slice::new(self.bpm.clone(), self.schema.clone());
        if self.buffer.is_empty() {
            for record in self.reader.records().take(1000) {
                let record = record.unwrap();
                let tuple: Vec<Datum> = record
                    .iter()
                    .zip(&self.schema.columns)
                    .map(|(data, col)| {
                        match col.data_type {
                        DataType::Int(_) => data.parse::<i32>().unwrap().into(),
                        DataType::Date(_) => NaiveDate::from_str(data).unwrap().into(),
                        DataType::Float(_) => f32::from_str(data).unwrap().into(),
                        DataType::VarChar(_) => data.into(),
                        DataType::Bool(_) => bool::from_str(data).unwrap().into(),

                    }})
                    .collect_vec();
                self.buffer.push_back(tuple);
            }
        }
        while !self.buffer.is_empty() {
            if output.insert(self.buffer.front().unwrap()).is_ok() {
                self.buffer.pop_front().unwrap();
            } else {
                break;
            }
        }
        if output.count() == 0 {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}
