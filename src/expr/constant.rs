use crate::expr::Expr;
use crate::table::{Datum, Slice};

pub struct ConstantExpr {}

impl Expr for ConstantExpr {
    fn eval(&mut self, _slice: Option<&Slice>) -> Datum {
        todo!()
    }
}
