use crate::datum::{DataType, Datum};
use crate::expr::Expr;
use crate::table::Slice;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            vec![self.value.clone(); slice.get_num_tuple()]
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
