use crate::datum::DataType;
use crate::expr::BinaryOp;

#[derive(Debug)]
pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabases,
    UseDatabase(UseDatabaseStmt),
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Desc(DescStmt),
    Select(SelectStmt),
    AddIndex(AddIndexStmt),
    DropTable(DropTableStmt),
    Delete(DeleteStmt),
}
#[derive(Debug)]
pub struct DropTableStmt {
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub enum ConstantValue {
    Null,
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
pub struct BinaryExprNode {
    pub lhs: Box<ExprNode>,
    pub rhs: Box<ExprNode>,
    pub op: BinaryOp,
}

#[derive(Debug)]
pub enum ExprNode {
    Constant(ConstantExprNode),
    ColumnRef(ColumnRefExprNode),
    Binary(BinaryExprNode),
}

#[derive(Debug)]
pub enum Selectors {
    All,
    Exprs(Vec<ExprNode>),
}

#[derive(Debug)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_exprs: Option<Vec<ExprNode>>,
}

#[derive(Debug)]
pub struct SelectStmt {
    pub table_name: String,
    pub selectors: Selectors,
    pub where_exprs: Option<Vec<ExprNode>>,
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
pub struct PrimaryField {
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub struct ForeignField {
    pub column_names: Vec<String>,
    pub ref_column_names: Vec<String>,
    pub ref_table_name: String,
}

#[derive(Debug)]
pub struct NormalField {
    pub field_name: String,
    pub field_data_type: DataType,
}

#[derive(Debug)]
pub enum Field {
    Normal(NormalField),
    Primary(PrimaryField),
    Foreign(ForeignField),
}

#[derive(Debug)]
pub struct AddIndexStmt {
    pub table_name: String,
    pub exprs: Vec<ExprNode>,
}
