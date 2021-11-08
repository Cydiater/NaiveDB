pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabases,
}

pub struct CreateDatabaseStmt {
    pub database_name: String,
}

pub enum Tok {
    CreateDatabase,
    ShowDatabases,
}
