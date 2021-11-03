use thiserror::Error;
use crate::parser::parse;

pub struct NaiveDB;

impl NaiveDB {
    pub fn run(&self, sql: &str) -> Result<(), NaiveDBError> {
        parse(sql)?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("ParseError: {0}")]
    Parse(String),
}
