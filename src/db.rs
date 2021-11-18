use crate::execution::Engine;
use crate::execution::ExecutionError;
use crate::parser::parse;
use crate::planner::Planner;
use crate::storage::BufferPoolManager;
use thiserror::Error;

pub struct NaiveDB {
    engine: Engine,
    planner: Planner,
}

impl NaiveDB {
    pub fn new() -> Self {
        let bpm = BufferPoolManager::new_shared(4096);
        Self {
            engine: Engine::new(bpm),
            planner: Planner::new(),
        }
    }
    pub fn run(&mut self, sql: &str) -> Result<String, NaiveDBError> {
        let statements = parse(sql)?;
        let mut res = "".to_string();
        for stmt in statements.into_iter() {
            let plan = self.planner.plan(stmt);
            let slice = self.engine.execute(plan)?;
            res += &slice.to_string();
        }
        Ok(res)
    }
}

#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("ParseError: {0}")]
    Parse(String),
    #[error("ExecutionError: {0}")]
    Execution(#[from] ExecutionError),
}
