pub enum Statement {
    CreateDatabase { database_name: String },
    ShowDatabase,
}

pub enum Tok {
    CreateDatabase,
    ShowDatabase,
}
