use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::datum::{DataType, Datum};
use crate::parser::ast::{ConstantValue, ExprNode};
use crate::table::{Schema, SchemaError, Slice};
use itertools::Itertools;
use std::convert::TryInto;
use std::fmt;
use thiserror::Error;

pub use self::like::LikeExpr;
pub use binary::{BinaryExpr, BinaryOp};
pub use column_ref::ColumnRefExpr;
pub use constant::ConstantExpr;

mod binary;
mod column_ref;
mod constant;
mod like;

pub trait Expr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum>;
    fn return_type(&self) -> DataType;
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExprImpl {
    Constant(ConstantExpr),
    ColumnRef(ColumnRefExpr),
    Binary(BinaryExpr),
    Like(LikeExpr),
}

impl fmt::Display for ExprImpl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{}", expr.get_value()),
            Self::Like(expr) => write!(f, "{}", expr),
            Self::Binary(expr) => write!(f, "{}", expr),
            Self::ColumnRef(expr) => write!(f, "{}", expr.as_return_type_and_column_name().1),
        }
    }
}

impl ExprImpl {
    pub fn batch_eval(exprs: &[ExprImpl], slice: Option<&Slice>) -> Vec<Vec<Datum>> {
        exprs.iter().map(|e| e.eval(slice)).fold(
            vec![vec![]; slice.unwrap().count()],
            |rows, column| {
                rows.into_iter()
                    .zip(column.into_iter())
                    .map(|(mut row, d)| {
                        row.push(d);
                        row
                    })
                    .collect_vec()
            },
        )
    }
    pub fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        match self {
            ExprImpl::Constant(expr) => expr.eval(slice),
            ExprImpl::ColumnRef(expr) => expr.eval(slice),
            ExprImpl::Binary(expr) => expr.eval(slice),
            ExprImpl::Like(expr) => expr.eval(slice),
        }
    }
    pub fn return_type(&self) -> DataType {
        match self {
            ExprImpl::Constant(expr) => expr.return_type(),
            ExprImpl::ColumnRef(expr) => expr.return_type(),
            ExprImpl::Binary(expr) => expr.return_type(),
            ExprImpl::Like(expr) => expr.return_type(),
        }
    }
    pub fn from_ast(
        node: &ExprNode,
        catalog: CatalogManagerRef,
        schema: &Schema,
        return_type_hint: Option<DataType>,
    ) -> Result<Self, ExprError> {
        match node {
            ExprNode::Constant(node) => match &node.value {
                ConstantValue::Real(value) => match return_type_hint.unwrap() {
                    DataType::Int(_) => Ok(ExprImpl::Constant(ConstantExpr::new(
                        Datum::Int(Some(*value as i32)),
                        return_type_hint.unwrap(),
                    ))),
                    DataType::Float(_) => Ok(ExprImpl::Constant(ConstantExpr::new(
                        Datum::Float(Some((*value as f32).try_into().unwrap())),
                        return_type_hint.unwrap(),
                    ))),
                    _ => Err(ExprError::NotMatch),
                },
                ConstantValue::String(value) => Ok(ExprImpl::Constant(ConstantExpr::new(
                    value.as_str().into(),
                    return_type_hint.unwrap(),
                ))),
                ConstantValue::Bool(value) => Ok(ExprImpl::Constant(ConstantExpr::new(
                    Datum::Bool(Some(*value)),
                    return_type_hint.unwrap(),
                ))),
                ConstantValue::Date(value) => Ok(ExprImpl::Constant(ConstantExpr::new(
                    Datum::Date(Some(*value)),
                    return_type_hint.unwrap(),
                ))),
                ConstantValue::Null => Ok(match return_type_hint.unwrap() {
                    DataType::Int(_) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Int(None),
                        return_type_hint.unwrap(),
                    )),
                    DataType::VarChar(_) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::VarChar(None),
                        return_type_hint.unwrap(),
                    )),
                    DataType::Bool(_) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Bool(None),
                        return_type_hint.unwrap(),
                    )),
                    DataType::Date(_) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Bool(None),
                        return_type_hint.unwrap(),
                    )),
                    DataType::Float(_) => ExprImpl::Constant(ConstantExpr::new(
                        Datum::Float(None),
                        return_type_hint.unwrap(),
                    )),
                }),
            },
            ExprNode::ColumnRef(node) => {
                let idx = schema
                    .index_by_column_name(&node.column_name)
                    .ok_or(SchemaError::ColumnNotFound)?;
                let return_type = schema.columns[idx].data_type;
                Ok(ExprImpl::ColumnRef(ColumnRefExpr::new(
                    idx,
                    return_type,
                    node.column_name.clone(),
                )))
            }
            ExprNode::Binary(node) => {
                let lhs =
                    Self::from_ast(node.lhs.as_ref(), catalog.clone(), schema, return_type_hint)?;
                let rhs = Self::from_ast(node.rhs.as_ref(), catalog, schema, return_type_hint)?;
                Ok(ExprImpl::Binary(BinaryExpr::new(
                    Box::new(lhs),
                    Box::new(rhs),
                    node.op.clone(),
                )))
            }
            ExprNode::Like(node) => {
                let child = Self::from_ast(node.child.as_ref(), catalog, schema, return_type_hint)?;
                Ok(ExprImpl::Like(LikeExpr::new(
                    &node.pattern,
                    Box::new(child),
                )))
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum ExprError {
    #[error("TableNameNotFound")]
    TableNameNotFound,
    #[error("CatalogError: {0}")]
    CatalogError(#[from] CatalogError),
    #[error("SchemaError: {0}")]
    SchemaError(#[from] SchemaError),
    #[error("Not Match")]
    NotMatch,
}
