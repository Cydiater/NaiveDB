pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabase,
}

pub struct CreateDatabaseStmt {
    pub database_name: String,
}

pub enum Tok {
    CreateDatabase,
    ShowDatabase,
}
