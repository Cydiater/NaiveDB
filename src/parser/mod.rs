pub mod ast;

#[cfg(test)]
mod tests {
    use crate::sql;

    #[test]
    fn sql() {
        assert!(sql::StatementsParser::new().parse("create database sample").is_ok());
    }
}
