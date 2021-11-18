use crate::table::{Datum, Slice};
use constant::ConstantExpr;

mod constant;

pub trait Expr {
    fn eval(&mut self, slice: Option<&Slice>) -> Datum;
}

#[allow(dead_code)]
pub enum ExprImpl {
    Constant(ConstantExpr),
}
