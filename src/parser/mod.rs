use crate::db::NaiveDBError;
use crate::sql::StatementParser;
use ast::Statement;

pub mod ast;

pub fn parse(sql: &str) -> Result<Statement, NaiveDBError> {
    let stmt_parser = StatementParser::new();
    stmt_parser
        .parse(sql)
        .map_err(|e| NaiveDBError::Parse(e.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::sql;

    #[test]
    fn test_database_sql() {
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

    #[test]
    fn test_expr() {
        // constant expr
        assert!(sql::ExprParser::new().parse("123").is_ok());
        assert!(sql::ExprParser::new().parse("'hello'").is_ok());
        assert!(sql::ExprParser::new().parse("222hh").is_err());
    }

    #[test]
    fn test_table_sql() {
        // create table
        assert!(sql::StatementsParser::new()
            .parse("create table sample(v1 int not null, v2 char(20) null);")
            .is_ok());
        // insert into
        assert!(sql::StatementsParser::new()
            .parse("insert into demo values (1, 2, 'hello'), (2, 3, 'world');")
            .is_ok());
        // desc
        assert!(sql::StatementsParser::new().parse("desc sample;").is_ok());
        // select from
        assert!(sql::StatementsParser::new()
            .parse("select * from t;")
            .is_ok());
        assert!(sql::StatementsParser::new()
            .parse("select v1, v2, v3 from t;")
            .is_ok());
        // where
        assert!(sql::StatementsParser::new()
            .parse("select * from t where v1 = 3;")
            .is_ok());
        // add index
        assert!(sql::StatementsParser::new()
            .parse("alter table t add index (v1, v2);")
            .is_ok());
        // drop table
        assert!(sql::StatementsParser::new().parse("drop table t;").is_ok());
    }
}
