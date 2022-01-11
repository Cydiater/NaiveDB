use crate::datum::DataType;
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::index::{BPTIndex, IndexError};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaError, SchemaRef, Slice, Table};
use itertools::Itertools;
use log::info;
use std::rc::Rc;

pub struct InsertExecutor {
    bpm: BufferPoolManagerRef,
    table: Table,
    indexes: Vec<BPTIndex>,
    child: Box<ExecutorImpl>,
    cnt: usize,
    executed: bool,
}

impl InsertExecutor {
    pub fn new(
        table: Table,
        indexes: Vec<BPTIndex>,
        child: Box<ExecutorImpl>,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            bpm,
            table,
            indexes,
            child,
            cnt: 0,
            executed: false,
        }
    }
}

impl Executor for InsertExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_int(false),
            "Inserted".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        while let Some(input) = self.child.execute()? {
            // stage-1: validate
            for (page_id, src_and_dst) in &self.table.schema.foreign {
                let mut foreign_table = Table::open(*page_id, self.bpm.clone());
                let page_id_of_index = foreign_table
                    .meta()
                    .page_id_of_primary_index
                    .ok_or(SchemaError::PrimaryNotFound)?;
                let foreign_index = BPTIndex::open(
                    self.bpm.clone(),
                    page_id_of_index,
                    foreign_table.schema.as_ref(),
                );
                let exprs = self
                    .table
                    .schema
                    .project_by(&src_and_dst.iter().map(|(src, _)| *src).collect_vec());
                let datums_from_slice = ExprImpl::batch_eval(&exprs, Some(&input));
                for datums in datums_from_slice {
                    let record_id = foreign_index.find(&datums).ok_or(IndexError::KeyNotFound)?;
                    let ref_cnt = foreign_table.ref_cnt_of(record_id)?;
                    foreign_table.set_ref_cnt_of(record_id, ref_cnt + 1)?;
                }
            }
            // stage-2: update
            let mut indexes_rows = vec![];
            for index in &mut self.indexes {
                let rows = ExprImpl::batch_eval(&index.exprs, Some(&input));
                indexes_rows.push(rows);
            }
            for tuple in input.tuple_iter() {
                info!("insert tuple {:?}", tuple);
                for (rows, index) in indexes_rows.iter_mut().zip(&mut self.indexes) {
                    if index.find(&rows[0]).is_some() {
                        return Err(ExecutionError::InsertDuplicatedKey(rows[0].clone()));
                    }
                }
                let record_id = self.table.insert(tuple)?;
                for (rows, index) in indexes_rows.iter_mut().zip(&mut self.indexes) {
                    index.insert(&rows.remove(0), record_id)?;
                }
                self.cnt += 1;
            }
        }
        Ok(Some(
            Slice::new_as_count(self.bpm.clone(), "Inserted", self.cnt).unwrap(),
        ))
    }
}
