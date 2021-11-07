use crate::execution::Engine;
use crate::parser::parse;
use crate::planner::Planner;
use thiserror::Error;

pub struct NaiveDB {
    engine: Engine,
    planner: Planner,
}

impl NaiveDB {
    pub fn run(&mut self, sql: &str) -> Result<(), NaiveDBError> {
        let statements = parse(sql)?;
        for stmt in statements.into_iter() {
            let plan = self.planner.plan(stmt);
            self.engine.execute(plan);
        }
        Ok(())
    }
}

impl NaiveDB {
    pub fn new() -> Self {
        Self {
            engine: Engine::new(4096),
            planner: Planner::new(),
        }
    }
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("ParseError: {0}")]
    Parse(String),
}
