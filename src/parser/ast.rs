pub enum Statement {
    CreateDatabase {
        database_name: String,
    }
}

pub enum Tok {
    CreateDatabase,
}
