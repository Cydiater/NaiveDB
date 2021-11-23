use crate::execution::{ExecutionError, Executor};
use crate::expr::ExprImpl;
use crate::storage::BufferPoolManagerRef;
use crate::table::{SchemaRef, Slice};
use itertools::Itertools;
use log::info;

#[allow(dead_code)]
pub struct ValuesExecutor {
    values: Vec<Vec<ExprImpl>>,
    schema: SchemaRef,
    bpm: BufferPoolManagerRef,
    executed: bool,
}

#[allow(dead_code)]
impl ValuesExecutor {
    pub fn new(values: Vec<Vec<ExprImpl>>, schema: SchemaRef, bpm: BufferPoolManagerRef) -> Self {
        Self {
            values,
            schema,
            bpm,
            executed: false,
        }
    }
}

impl Executor for ValuesExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        if !self.executed {
            let mut slice = Slice::new(self.bpm.clone(), self.schema.clone());
            for tuple in self.values.iter_mut() {
                let datums = tuple
                    .iter_mut()
                    .map(|e| e.eval(None).remove(0))
                    .collect_vec();
                info!("generate tuple {:?}", datums);
                slice.add(&datums)?;
            }
            self.executed = true;
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{ConstantExpr, ExprImpl};
    use crate::storage::BufferPoolManager;
    use crate::table::{DataType, Datum, Schema};
    use std::fs::remove_file;
    use std::rc::Rc;

    #[test]
    fn test_values() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let values = vec![vec![
                ExprImpl::Constant(ConstantExpr::new(Datum::Int(Some(1)))),
                ExprImpl::Constant(ConstantExpr::new(Datum::VarChar(Some(
                    "hello world".to_string(),
                )))),
            ]];
            let schema = Schema::from_slice(&[
                (DataType::Int, "v1".to_string(), false),
                (DataType::VarChar, "v2".to_string(), false),
            ]);
            let mut values_executor = ValuesExecutor::new(values, Rc::new(schema), bpm);
            let res = values_executor.execute().unwrap().unwrap();
            assert_eq!(
                res.at(0).unwrap(),
                [
                    Datum::Int(Some(1)),
                    Datum::VarChar(Some("hello world".to_string()))
                ]
                .to_vec(),
            );
            filename
        };
        remove_file(filename).unwrap();
    }
}