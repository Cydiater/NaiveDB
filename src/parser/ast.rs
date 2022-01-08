use crate::datum::DataType;
use crate::expr::BinaryOp;
use chrono::NaiveDate;
use std::string::ToString;

#[derive(Debug)]
pub enum Statement {
    CreateDatabase(CreateDatabaseStmt),
    ShowDatabases,
    ShowTables,
    UseDatabase(UseDatabaseStmt),
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Desc(DescStmt),
    Select(SelectStmt),
    AddIndex(AddIndexStmt),
    AddPrimary(AddPrimaryStmt),
    AddForeign(AddForeignStmt),
    AddUnique(AddUniqueStmt),
    DropTable(DropTableStmt),
    Delete(DeleteStmt),
    LoadFromFile(LoadFromFileStmt),
    DropDatabase(DropDatabaseStmt),
}

#[derive(Debug, Clone)]
pub enum AggAction {
    Sum,
    Avg,
    Max,
    Cnt,
    No,
}

impl ToString for AggAction {
    fn to_string(&self) -> String {
        match self {
            Self::Sum => "sum".to_owned(),
            Self::Avg => "average".to_owned(),
            Self::Max => "max".to_owned(),
            Self::Cnt => "count".to_owned(),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub enum AggTarget {
    All,
    Expr(ExprNode),
}

#[derive(Debug)]
pub struct AggItem {
    pub action: AggAction,
    pub target: AggTarget,
}

#[derive(Debug)]
pub struct DropDatabaseStmt {
    pub database_name: String,
}

#[derive(Debug)]
pub struct DropTableStmt {
    pub table_name: String,
}

#[derive(Debug, Clone)]
pub enum ConstantValue {
    Null,
    String(String),
    Real(f64),
    Bool(bool),
    Date(NaiveDate),
}

#[derive(Debug)]
pub struct ConstantExprNode {
    pub value: ConstantValue,
}

#[derive(Debug)]
pub struct ColumnRefExprNode {
    pub table_name: Option<String>,
    pub column_name: String,
}

#[derive(Debug)]
pub struct BinaryExprNode {
    pub lhs: Box<ExprNode>,
    pub rhs: Box<ExprNode>,
    pub op: BinaryOp,
}

#[derive(Debug)]
pub struct LikeExprNode {
    pub child: Box<ExprNode>,
    pub pattern: String,
}

#[derive(Debug)]
pub enum ExprNode {
    Constant(ConstantExprNode),
    ColumnRef(ColumnRefExprNode),
    Binary(BinaryExprNode),
    Like(LikeExprNode),
}

impl ExprNode {
    pub fn ref_what_column(&self) -> Option<String> {
        match self {
            Self::Constant(_) => None,
            Self::Binary(b) => {
                if let Some(n) = b.lhs.ref_what_column() {
                    Some(n)
                } else {
                    b.rhs.ref_what_column()
                }
            }
            Self::ColumnRef(c) => Some(c.column_name.to_owned()),
            Self::Like(c) => c.child.ref_what_column(),
        }
    }
}

#[derive(Debug)]
pub enum Selectors {
    All,
    Exprs(Vec<ExprNode>),
    Agg(Vec<AggItem>),
}

#[derive(Debug)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_exprs: Vec<ExprNode>,
}

#[derive(Debug)]
pub struct SelectStmt {
    pub table_names: Vec<String>,
    pub selectors: Selectors,
    pub where_exprs: Vec<ExprNode>,
    pub group_by_expr: Option<ExprNode>,
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
pub struct LoadFromFileStmt {
    pub table_name: String,
    pub file_name: String,
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
pub struct UniqueField {
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub enum Field {
    Normal(NormalField),
    Primary(PrimaryField),
    Foreign(ForeignField),
    Unique(UniqueField),
}

#[derive(Debug)]
pub struct AddIndexStmt {
    pub table_name: String,
    pub exprs: Vec<ExprNode>,
}

#[derive(Debug)]
pub struct AddPrimaryStmt {
    pub table_name: String,
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub struct AddUniqueStmt {
    pub table_name: String,
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub struct AddForeignStmt {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub ref_table_name: String,
    pub ref_column_names: Vec<String>,
}
