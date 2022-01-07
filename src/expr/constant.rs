use crate::datum::{DataType, Datum};
use crate::expr::Expr;
use crate::table::Slice;

#[derive(Debug, PartialEq)]
pub struct ConstantExpr {
    value: Datum,
    return_type: DataType,
}

impl ConstantExpr {
    pub fn new(value: Datum, return_type: DataType) -> Self {
        Self { value, return_type }
    }
    pub fn get_value(&self) -> Datum {
        self.value.clone()
    }
}

impl Expr for ConstantExpr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            vec![self.value.clone(); slice.count()]
        } else {
            vec![self.value.clone()]
        }
    }
    fn return_type(&self) -> DataType {
        self.return_type
    }
}
