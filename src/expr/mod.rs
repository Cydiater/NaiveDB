use crate::catalog::CatalogManagerRef;
use crate::parser::ast::ExprNode;
use crate::table::{DataType, Datum, Slice};
pub use column_ref::ColumnRefExpr;
pub use constant::ConstantExpr;

mod column_ref;
mod constant;

pub trait Expr {
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum>;
    fn return_type(&self) -> DataType;
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
    pub fn from_ast(_node: ExprNode, _catalog: CatalogManagerRef) -> Self {
        todo!()
    }
}
