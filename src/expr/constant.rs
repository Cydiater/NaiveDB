use crate::expr::Expr;
use crate::table::{Datum, Slice};

#[allow(dead_code)]
#[derive(Debug)]
pub struct ConstantExpr {
    value: Datum,
}

impl ConstantExpr {
    pub fn new(value: Datum) -> Self {
        Self { value }
    }
}

impl Expr for ConstantExpr {
    fn eval(&mut self, _: Option<&Slice>) -> Datum {
        self.value.clone()
    }
}
