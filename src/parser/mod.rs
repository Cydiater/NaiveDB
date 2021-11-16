use crate::db::NaiveDBError;
use crate::sql::StatementsParser;
use ast::Statement;

pub mod ast;

pub fn parse(sql: &str) -> Result<Vec<Statement>, NaiveDBError> {
    let stmt_parser = StatementsParser::new();
    stmt_parser
        .parse(sql)
        .map_err(|e| NaiveDBError::Parse(e.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::sql;

    #[test]
    fn sql() {
        // create database
        assert!(sql::StatementsParser::new()
            .parse("create database sample;")
            .is_ok());
        assert!(sql::StatementsParser::new()
            .parse("create_database sample;")
            .is_err());
        // show database
        assert!(sql::StatementsParser::new()
            .parse("show databases;")
            .is_ok());
        // use database
        assert!(sql::StatementsParser::new().parse("use sample;").is_ok());
    }
}
