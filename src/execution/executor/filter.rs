use crate::datum::Datum;
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::expr::ExprImpl;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice};
use itertools::Itertools;
use std::collections::VecDeque;

pub struct FilterExecutor {
    child: Box<ExecutorImpl>,
    exprs: Vec<ExprImpl>,
    bpm: BufferPoolManagerRef,
    buffer: VecDeque<Vec<Datum>>,
}

impl FilterExecutor {
    pub fn new(bpm: BufferPoolManagerRef, child: Box<ExecutorImpl>, exprs: Vec<ExprImpl>) -> Self {
        Self {
            child,
            exprs,
            bpm,
            buffer: VecDeque::new(),
        }
    }
    fn filter_map(&self, slice: &Slice) -> Vec<bool> {
        let check_results = self.exprs.iter().map(|e| e.eval(Some(slice))).collect_vec();
        let len = check_results[0].len();
        let check_results = check_results.iter().fold(vec![true; len], |check, res| {
            check
                .iter()
                .zip(res.iter())
                .map(|(b, d)| {
                    if let Datum::Bool(Some(d)) = d {
                        b & d
                    } else {
                        unreachable!()
                    }
                })
                .collect_vec()
        });
        check_results
    }
}

impl Executor for FilterExecutor {
    fn schema(&self) -> SchemaRef {
        self.child.schema()
    }
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let mut output = Slice::new(self.bpm.clone(), self.schema());
        loop {
            if self.buffer.is_empty() {
                let input = self.child.execute()?;
                if let Some(slice) = input {
                    let mut tuples = vec![];
                    for idx in 0..slice.get_num_tuple() {
                        if let Some(tuple) = slice.at(idx)? {
                            tuples.push(tuple)
                        }
                    }
                    let filter_map = self.filter_map(&slice);
                    for (tuple, check) in tuples.into_iter().zip(filter_map) {
                        if !check {
                            continue;
                        }
                        self.buffer.push_back(tuple);
                    }
                } else if output.get_num_tuple() > 0 {
                    return Ok(Some(output));
                } else {
                    return Ok(None);
                }
            }
            if self.buffer.is_empty() {
                return Ok(None);
            }
            if output.ok_to_add(self.buffer.front().unwrap()) {
                output.add(self.buffer.pop_front().unwrap().as_slice())?;
            } else {
                break;
            }
        }
        Ok(Some(output))
    }
}
