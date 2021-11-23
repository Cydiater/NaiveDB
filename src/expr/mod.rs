use crate::table::{Datum, Slice};
pub use column_ref::ColumnRefExpr;
pub use constant::ConstantExpr;

mod column_ref;
mod constant;

pub trait Expr {
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum>;
}

#[derive(Debug)]
pub enum ExprImpl {
    Constant(ConstantExpr),
    ColumnRef(ColumnRefExpr),
}

impl ExprImpl {
    pub fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum> {
        match self {
            ExprImpl::Constant(expr) => expr.eval(slice),
            ExprImpl::ColumnRef(expr) => expr.eval(slice),
        }
    }
}
