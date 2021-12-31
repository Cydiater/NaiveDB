use crate::datum::{DataType, Datum};
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::index::BPTIndex;
use crate::storage::BufferPoolManagerRef;
use crate::table::{Schema, SchemaRef, Slice, Table};
use itertools::Itertools;
use std::rc::Rc;

pub struct DeleteExecutor {
    child: Box<ExecutorImpl>,
    indexes: Vec<BPTIndex>,
    table: Table,
    bpm: BufferPoolManagerRef,
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
        }
    }
}

impl Executor for DeleteExecutor {
    fn schema(&self) -> SchemaRef {
        Rc::new(Schema::from_slice(&[(
            DataType::new_int(false),
            "Number Of Deleted Tuple".to_string(),
        )]))
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let input = if let Some(input) = self.child.execute()? {
            input
        } else {
            return Ok(None);
        };
        let mut indexes_rows = self
            .indexes
            .iter_mut()
            .map(|index| ExprImpl::batch_eval(&mut index.get_exprs(), Some(&input)))
            .collect_vec();
        let mut remove_cnt = 0;
        for idx in input.slot_iter() {
            let mut tuple = input.tuple_at(idx)?;
            let idx = tuple.pop().unwrap();
            let idx = if let Datum::Int(Some(idx)) = idx {
                idx as usize
            } else {
                unreachable!()
            };
            let page_id = tuple.pop().unwrap();
            let page_id = if let Datum::Int(Some(page_id)) = page_id {
                page_id as usize
            } else {
                unreachable!()
            };
            let record_id = (page_id, idx);
            self.table.remove(record_id)?;
            remove_cnt += 1;
            for (rows, index) in indexes_rows.iter_mut().zip(&mut self.indexes) {
                index.remove(&rows.remove(0))?;
            }
        }
        let mut msg = Slice::new(self.bpm.clone(), self.schema());
        msg.insert(&[Datum::Int(Some(remove_cnt as i32))])?;
        Ok(Some(msg))
    }
}
