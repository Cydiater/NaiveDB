pub use crate::table::DataType;

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

pub enum Field {
    Normal(NormalField),
}

pub struct NormalField {
    pub field_name: String,
    pub field_data_type: DataType,
    pub nullable: bool,
}
