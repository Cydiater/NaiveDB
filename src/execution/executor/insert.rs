use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::index::BPTIndex;
use crate::table::{SchemaRef, Slice, Table};
use log::info;

pub struct InsertExecutor {
    table: Table,
    indexes: Vec<BPTIndex>,
    child: Box<ExecutorImpl>,
}

impl InsertExecutor {
    pub fn new(table: Table, indexes: Vec<BPTIndex>, child: Box<ExecutorImpl>) -> Self {
        Self {
            table,
            indexes,
            child,
        }
    }
}

impl Executor for InsertExecutor {
    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let input = self.child.execute()?;
        if let Some(input) = input {
            let len = input.get_num_tuple();
            let mut indexes_rows = vec![];
            for index in &mut self.indexes {
                let mut exprs = index.get_exprs();
                let rows = ExprImpl::batch_eval(&mut exprs, Some(&input));
                indexes_rows.push(rows);
            }
            for idx in 0..len {
                if let Some(tuple) = input.at(idx)? {
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
                }
            }
            Ok(Some(input))
        } else {
            Ok(None)
        }
    }
}
