use thiserror::Error;

pub struct NaiveDB;

impl NaiveDB {
    pub fn run(&self, _sql: &str) -> Result<(), NaiveDBError> {
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("{0}")]
    Parse(String),
}
