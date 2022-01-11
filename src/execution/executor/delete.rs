use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::index::{BPTIndex, IndexError};
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaError, SchemaRef, Slice, Table};
use itertools::Itertools;
use std::rc::Rc;

pub struct DeleteExecutor {
    child: Box<ExecutorImpl>,
    indexes: Vec<BPTIndex>,
    table: Table,
    bpm: BufferPoolManagerRef,
    executed: bool,
}

impl DeleteExecutor {
    pub fn new(
        child: Box<ExecutorImpl>,
        indexes: Vec<BPTIndex>,
        table: Table,
        bpm: BufferPoolManagerRef,
    ) -> Self {
        Self {
            child,
            indexes,
            table,
            bpm,
            executed: false,
        }
    }
}

impl Executor for DeleteExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_type_and_names(&[(
            DataType::new_as_int(false),
            "Number Of Deleted Tuple".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let mut remove_cnt = 0;
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
                    foreign_table.set_ref_cnt_of(record_id, ref_cnt - 1)?;
                }
            }
            // stage-2: delete
            let mut indexes_rows = self
                .indexes
                .iter_mut()
                .map(|index| ExprImpl::batch_eval(&index.exprs, Some(&input)))
                .collect_vec();
            for idx in input.slot_iter() {
                let mut tuple = input.tuple_at(idx)?;
                let idx: i32 = tuple.pop().unwrap().into();
                let page_id: i32 = tuple.pop().unwrap().into();
                let record_id = (page_id as usize, idx as usize);
                self.table.remove(record_id)?;
                remove_cnt += 1;
                for (rows, index) in indexes_rows.iter_mut().zip(&mut self.indexes) {
                    index.remove(&rows.remove(0))?;
                }
            }
        }
        let mut msg = Slice::new(self.bpm.clone(), self.schema());
        msg.insert(&[Datum::Int(Some(remove_cnt as i32))])?;
        Ok(Some(msg))
    }
}
