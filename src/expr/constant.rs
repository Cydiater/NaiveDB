use crate::expr::Expr;
use crate::table::{Datum, Slice};

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
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            vec![self.value.clone(); slice.len()]
        } else {
            vec![self.value.clone()]
        }
    }
}
