use crate::expr::Expr;
use crate::table::{DataType, Datum, Slice};

#[derive(Debug)]
pub struct ConstantExpr {
    value: Datum,
    return_type: DataType,
}

impl ConstantExpr {
    pub fn new(value: Datum, return_type: DataType) -> Self {
        Self { value, return_type }
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
    fn return_type(&self) -> DataType {
        self.return_type
    }
    fn name(&self) -> String {
        String::from("constant")
    }
}
