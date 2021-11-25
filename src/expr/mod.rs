use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::parser::ast::{ConstantValue, ExprNode};
use crate::table::{DataType, Datum, Slice};
pub use column_ref::ColumnRefExpr;
pub use constant::ConstantExpr;
use thiserror::Error;

mod column_ref;
mod constant;

pub trait Expr {
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum>;
    fn return_type(&self) -> DataType;
    fn name(&self) -> String;
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ExprImpl {
    Constant(ConstantExpr),
    ColumnRef(ColumnRefExpr),
}

#[allow(dead_code)]
impl ExprImpl {
    pub fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum> {
        match self {
            ExprImpl::Constant(expr) => expr.eval(slice),
            ExprImpl::ColumnRef(expr) => expr.eval(slice),
        }
    }
    pub fn return_type(&self) -> DataType {
        match self {
            ExprImpl::Constant(expr) => expr.return_type(),
            ExprImpl::ColumnRef(expr) => expr.return_type(),
        }
    }
    pub fn name(&self) -> String {
        match self {
            ExprImpl::Constant(expr) => expr.name(),
            ExprImpl::ColumnRef(expr) => expr.name(),
        }
    }
    pub fn from_ast(
        node: ExprNode,
        catalog: CatalogManagerRef,
        table_name: Option<String>,
    ) -> Result<Self, ExprError> {
        match node {
            ExprNode::Constant(node) => Ok(match node.value {
                ConstantValue::Int(value) => ExprImpl::Constant(ConstantExpr::new(
                    Datum::Int(Some(value)),
                    DataType::new_int(false),
                )),
                ConstantValue::String(value) => ExprImpl::Constant(ConstantExpr::new(
                    Datum::VarChar(Some(value)),
                    DataType::new_varchar(false),
                )),
                ConstantValue::Bool(value) => ExprImpl::Constant(ConstantExpr::new(
                    Datum::Bool(Some(value)),
                    DataType::new_bool(false),
                )),
            }),
            ExprNode::ColumnRef(node) => {
                let table_name = table_name.unwrap();
                let table = catalog.borrow().find_table(table_name)?;
                let schema = table.schema;
                let idx = schema.index_of(node.column_name.clone()).unwrap();
                let return_type = schema.type_at(idx);
                Ok(ExprImpl::ColumnRef(ColumnRefExpr::new(
                    idx,
                    return_type,
                    node.column_name,
                )))
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Error, Debug)]
pub enum ExprError {
    #[error("TableNameNotFound")]
    TableNameNotFound,
    #[error("CatalogError: {0}")]
    CatalogError(#[from] CatalogError),
}
