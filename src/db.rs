use crate::execution::Engine;
use crate::execution::ExecutionError;
use crate::parser::parse;
use crate::planner::Planner;
use crate::storage::{BufferPoolManager, BufferPoolManagerRef};
use std::cell::RefCell;
use std::rc::Rc;
use thiserror::Error;

#[allow(dead_code)]
pub struct NaiveDB {
    bpm: BufferPoolManagerRef,
    engine: Engine,
    planner: Planner,
}

impl NaiveDB {
    pub fn new() -> Self {
        let bpm = Rc::new(RefCell::new(BufferPoolManager::new(4096)));
        Self {
            bpm: bpm.clone(),
            engine: Engine::new(bpm),
            planner: Planner::new(),
        }
    }
    pub fn run(&mut self, sql: &str) -> Result<(), NaiveDBError> {
        let statements = parse(sql)?;
        for stmt in statements.into_iter() {
            let plan = self.planner.plan(stmt);
            self.engine.execute(plan)?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("ParseError: {0}")]
    Parse(String),
    #[error("ExecutionError: {0}")]
    Execution(#[from] ExecutionError),
}
