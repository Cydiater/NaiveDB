#[cfg(test)]
mod tests {
    use crate::sql;

    #[test]
    fn sql() {
        assert!(sql::TermParser::new().parse("22").is_ok());
        assert!(sql::TermParser::new().parse("(22)").is_ok());
        assert!(sql::TermParser::new().parse("((((22))))").is_ok());
        assert!(sql::TermParser::new().parse("((22)").is_err());
    }
}
