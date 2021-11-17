pub use crate::table::DataType;

#[derive(Debug)]
pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabases,
    UseDatabase(UseDatabaseStmt),
    CreateTable(CreateTableStmt),
}

#[derive(Debug)]
pub struct CreateDatabaseStmt {
    pub database_name: String,
}

#[derive(Debug)]
pub struct UseDatabaseStmt {
    pub database_name: String,
}

#[derive(Debug)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub fields: Vec<Field>,
}

#[derive(Debug)]
pub enum Field {
    Normal(NormalField),
}

#[derive(Debug)]
pub struct NormalField {
    pub field_name: String,
    pub field_data_type: DataType,
    pub nullable: bool,
}
