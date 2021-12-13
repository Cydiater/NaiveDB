use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::index::BPTIndex;
use crate::table::{SchemaRef, Slice, Table};
use itertools::Itertools;
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
                let exprs = index.get_exprs();
                let rows = exprs.into_iter().map(|e| e.eval(Some(&input))).fold(
                    vec![vec![]; input.get_num_tuple()],
                    |rows, column| {
                        rows.into_iter()
                            .zip(column.into_iter())
                            .map(|(mut row, d)| {
                                row.push(d);
                                row
                            })
                            .collect_vec()
                    },
                );
                indexes_rows.push(rows);
            }
            for idx in 0..len {
                let tuple = input.at(idx)?;
                info!("insert tuple {:?}", tuple);
                let record_id = self.table.insert(tuple)?;
                for (rows, index) in indexes_rows.iter().zip(&mut self.indexes) {
                    index.insert(&rows[idx], record_id).unwrap();
                }
            }
            Ok(Some(input))
        } else {
            Ok(None)
        }
    }
}
