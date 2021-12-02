use crate::catalog::CatalogManagerRef;
use crate::execution::{ExecutionError, Executor, ExecutorImpl};
use crate::table::Slice;
use log::info;

#[allow(dead_code)]
pub struct InsertExecutor {
    table_name: String,
    catalog: CatalogManagerRef,
    child: Box<ExecutorImpl>,
}

impl InsertExecutor {
    pub fn new(table_name: String, catalog: CatalogManagerRef, child: Box<ExecutorImpl>) -> Self {
        Self {
            table_name,
            catalog,
            child,
        }
    }
}

impl Executor for InsertExecutor {
    fn execute(&mut self) -> Result<Option<Slice>, ExecutionError> {
        let input = self.child.execute()?;
        if let Some(input) = input {
            let mut table = self
                .catalog
                .borrow_mut()
                .find_table(self.table_name.clone())?;
            let len = input.get_num_tuple();
            for idx in 0..len {
                let tuple = input.at(idx)?;
                info!("insert tuple {:?}", tuple);
                table.insert(tuple)?;
            }
            Ok(Some(input))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::CatalogManager;
    use crate::datum::{DataType, Datum};
    use crate::execution::{ExecutorImpl, InsertExecutor, ValuesExecutor};
    use crate::expr::{ConstantExpr, ExprImpl};
    use crate::storage::BufferPoolManager;
    use crate::table::{Schema, Table};
    use std::fs::remove_file;
    use std::rc::Rc;

    #[test]
    fn test_insert() {
        let filename = {
            let bpm = BufferPoolManager::new_random_shared(5);
            let filename = bpm.borrow().filename();
            let schema = Rc::new(Schema::from_slice(&[(
                DataType::new_int(false),
                "v1".to_string(),
            )]));
            let catalog = CatalogManager::new_shared(bpm.clone());
            let values_executor = ExecutorImpl::Values(ValuesExecutor::new(
                vec![vec![ExprImpl::Constant(ConstantExpr::new(
                    Datum::Int(Some(123)),
                    DataType::new_int(false),
                ))]],
                schema.clone(),
                bpm.clone(),
            ));
            let table = Table::new(schema.clone(), bpm.clone());
            catalog
                .borrow_mut()
                .create_database("d".to_string())
                .unwrap();
            catalog.borrow_mut().use_database("d".to_string()).unwrap();
            catalog
                .borrow_mut()
                .create_table("t".to_string(), table.get_page_id())
                .unwrap();
            let mut insert_executor = ExecutorImpl::Insert(InsertExecutor::new(
                "t".to_string(),
                catalog.clone(),
                Box::new(values_executor),
            ));
            insert_executor.execute().unwrap();
            assert_eq!(table.iter().count(), 1);
            filename
        };
        remove_file(filename).unwrap();
    }
}
