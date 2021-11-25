pub use crate::table::DataType;

#[derive(Debug)]
pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabases,
    UseDatabase(UseDatabaseStmt),
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Desc(DescStmt),
    Select(SelectStmt),
}

#[derive(Debug)]
pub enum ConstantValue {
    String(String),
    Int(i32),
    Bool(bool),
}

#[derive(Debug)]
pub struct ConstantExprNode {
    pub value: ConstantValue,
}

#[derive(Debug)]
pub struct ColumnRefExprNode {
    pub column_name: String,
}

#[derive(Debug)]
pub enum ExprNode {
    Constant(ConstantExprNode),
    ColumnRef(ColumnRefExprNode),
}

#[derive(Debug)]
pub enum Selectors {
    All,
    Exprs(Vec<ExprNode>),
}

#[derive(Debug)]
pub struct SelectStmt {
    pub table_name: String,
    pub selectors: Selectors,
}

#[derive(Debug)]
pub struct DescStmt {
    pub table_name: String,
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
pub struct InsertStmt {
    pub table_name: String,
    pub values: Vec<Vec<ExprNode>>,
}

#[derive(Debug)]
pub enum Field {
    Normal(NormalField),
}

#[derive(Debug)]
pub struct NormalField {
    pub field_name: String,
    pub field_data_type: DataType,
}
