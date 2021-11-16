pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabases,
    UseDatabase(UseDatabaseStmt),
}

pub struct CreateDatabaseStmt {
    pub database_name: String,
}

pub struct UseDatabaseStmt {
    pub database_name: String,
}

pub enum Tok {
    CreateDatabase,
    ShowDatabases,
    UseDatabase,
}
