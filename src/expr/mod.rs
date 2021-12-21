use crate::catalog::{CatalogError, CatalogManagerRef};
use crate::datum::{DataType, Datum};
use crate::parser::ast::{ConstantValue, ExprNode};
use crate::table::Slice;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use binary::{BinaryExpr, BinaryOp};
pub use column_ref::ColumnRefExpr;
pub use constant::ConstantExpr;

mod binary;
mod column_ref;
mod constant;

pub trait Expr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum>;
    fn return_type(&self) -> DataType;
    fn name(&self) -> String;
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum ExprImpl {
    Constant(ConstantExpr),
    ColumnRef(ColumnRefExpr),
    Binary(BinaryExpr),
}

impl ExprImpl {
    pub fn batch_eval(exprs: &mut [ExprImpl], slice: Option<&Slice>) -> Vec<Vec<Datum>> {
        exprs.iter_mut().map(|e| e.eval(slice)).fold(
            vec![vec![]; slice.unwrap().get_num_tuple()],
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
        node: &ExprNode,
        catalog: CatalogManagerRef,
        table_name: Option<String>,
        data_type_hint: Option<&DataType>,
    ) -> Result<Self, ExprError> {
        match node {
            ExprNode::Constant(node) => Ok(match &node.value {
                ConstantValue::Int(value) => ExprImpl::Constant(ConstantExpr::new(
                    Datum::Int(Some(*value)),
                    DataType::new_int(false),
                )),
                ConstantValue::String(value) => ExprImpl::Constant(ConstantExpr::new(
                    Datum::VarChar(Some(value.clone())),
                    DataType::new_varchar(false),
                )),
                ConstantValue::Bool(value) => ExprImpl::Constant(ConstantExpr::new(
                    Datum::Bool(Some(*value)),
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
                let table = catalog.borrow().find_table(&table_name)?;
                let schema = table.schema.clone();
                let idx = schema.index_of(&node.column_name).unwrap();
                let return_type = schema.type_at(idx);
                Ok(ExprImpl::ColumnRef(ColumnRefExpr::new(
                    idx,
                    return_type,
                    node.column_name.clone(),
                )))
            }
            ExprNode::Binary(node) => {
                let lhs = Self::from_ast(
                    node.lhs.as_ref(),
                    catalog.clone(),
                    table_name.clone(),
                    data_type_hint,
                )?;
                let rhs = Self::from_ast(node.rhs.as_ref(), catalog, table_name, data_type_hint)?;
                Ok(ExprImpl::Binary(BinaryExpr::new(
                    Box::new(lhs),
                    Box::new(rhs),
                    node.op.clone(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde() {
        let expr = ExprImpl::Binary(BinaryExpr::new(
            Box::new(ExprImpl::ColumnRef(ColumnRefExpr::new(
                0,
                DataType::new_int(false),
                "v1".to_string(),
            ))),
            Box::new(ExprImpl::Constant(ConstantExpr::new(
                Datum::Int(Some(1)),
                DataType::new_int(false),
            ))),
            BinaryOp::Equal,
        ));
        let serialized = serde_json::to_string(&expr).unwrap();
        let deserialized: ExprImpl = serde_json::from_str(&serialized).unwrap();
        assert!(matches!(deserialized, ExprImpl::Binary(_)));
    }
}
