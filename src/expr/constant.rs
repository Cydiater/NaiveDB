use crate::expr::Expr;
use crate::table::{Datum, Slice};

#[allow(dead_code)]
pub struct ConstantExpr {
    value: Datum,
}

impl ConstantExpr {
    pub fn new(value: Datum) -> Self {
        Self { value }
    }
}

impl Expr for ConstantExpr {
    fn eval(&mut self, _slice: Option<&Slice>) -> Datum {
        todo!()
    }
}
