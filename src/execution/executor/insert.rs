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
    fn execute(&mut self) -> Result<Slice, ExecutionError> {
        let input = self.child.execute()?;
        let mut table = self
            .catalog
            .borrow_mut()
            .find_table(self.table_name.clone())?;
        let len = input.len();
        for idx in 0..len {
            let tuple = input.at(idx)?;
            info!("insert tuple {:?}", tuple);
            table.insert(tuple.as_slice())?;
        }
        Ok(input)
    }
}
