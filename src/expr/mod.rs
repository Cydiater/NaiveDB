use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::datum::{DataType, Datum};
use crate::parser::ast::{ConstantValue, ExprNode};
use crate::table::Slice;
use thiserror::Error;

pub use binary::{BinaryExpr, BinaryOp};
pub use column_ref::ColumnRefExpr;
pub use constant::ConstantExpr;

mod binary;
mod column_ref;
mod constant;

pub trait Expr {
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum>;
    fn return_type(&self) -> DataType;
    fn name(&self) -> String;
}

#[derive(Debug)]
pub enum ExprImpl {
    Constant(ConstantExpr),
    ColumnRef(ColumnRefExpr),
    Binary(BinaryExpr),
}

impl ExprImpl {
    pub fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum> {
        match self {
            ExprImpl::Constant(expr) => expr.eval(slice),
            ExprImpl::ColumnRef(expr) => expr.eval(slice),
            ExprImpl::Binary(expr) => expr.eval(slice),
        }
    }
    pub fn return_type(&self) -> DataType {
        match self {
            ExprImpl::Constant(expr) => expr.return_type(),
            ExprImpl::ColumnRef(expr) => expr.return_type(),
            ExprImpl::Binary(expr) => expr.return_type(),
        }
    }
    pub fn name(&self) -> String {
        match self {
            ExprImpl::Constant(expr) => expr.name(),
            ExprImpl::ColumnRef(expr) => expr.name(),
            ExprImpl::Binary(expr) => expr.name(),
        }
    }
    pub fn from_ast(
        node: ExprNode,
        catalog: CatalogManagerRef,
        table_name: Option<String>,
        data_type_hint: Option<&DataType>,
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
                ConstantValue::Null => match data_type_hint.unwrap() {
                    DataType::Int(int_type) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Int(None),
                        DataType::Int(*int_type),
                    )),
                    DataType::Char(char_type) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Char(None),
                        DataType::Char(*char_type),
                    )),
                    DataType::VarChar(varchar_type) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::VarChar(None),
                        DataType::VarChar(*varchar_type),
                    )),
                    DataType::Bool(bool_type) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Bool(None),
                        DataType::Bool(*bool_type),
                    )),
                },
            }),
            ExprNode::ColumnRef(node) => {
                let table_name = table_name.unwrap();
                let table = catalog.borrow().find_table(table_name)?;
                let schema = table.schema.clone();
                let idx = schema.index_of(node.column_name.clone()).unwrap();
                let return_type = schema.type_at(idx);
                Ok(ExprImpl::ColumnRef(ColumnRefExpr::new(
                    idx,
                    return_type,
                    node.column_name,
                )))
            }
            ExprNode::Binary(node) => {
                let lhs = Self::from_ast(
                    *node.lhs,
                    catalog.clone(),
                    table_name.clone(),
                    data_type_hint,
                )?;
                let rhs = Self::from_ast(
                    *node.rhs,
                    catalog,
                    table_name,
                    data_type_hint,
                )?;
                Ok(ExprImpl::Binary(BinaryExpr::new(
                    Box::new(lhs),
                    Box::new(rhs),
                    node.op,
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
