use crate::table::{Datum, Slice};
pub use constant::ConstantExpr;

mod constant;

pub trait Expr {
    fn eval(&mut self, slice: Option<&Slice>) -> Datum;
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ExprImpl {
    Constant(ConstantExpr),
}

impl ExprImpl {
    pub fn eval(&mut self, slice: Option<&Slice>) -> Datum {
        match self {
            ExprImpl::Constant(expr) => expr.eval(slice),
        }
    }
}
