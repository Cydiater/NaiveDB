use crate::parser::parse;
use log::info;
use sqlparser::parser::ParserError;
use thiserror::Error;

pub struct NaiveDB;

impl NaiveDB {
    pub fn run(&self, sql: &str) -> Result<(), NaiveDBError> {
        let ast = match parse(sql) {
            Ok(ast) => ast,
            Err(parse_err) => return Err(NaiveDBError::Parse(parse_err)),
        };
        info!("Parse to AST {:?}", ast);
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("{0}")]
    Parse(#[from] ParserError),
}
