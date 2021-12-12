use crate::datum::{DataType, Datum};
use crate::expr::Expr;
use crate::table::Slice;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnRefExpr {
    idx: usize,
    return_type: DataType,
    desc: String,
}

impl ColumnRefExpr {
    pub fn new(idx: usize, return_type: DataType, desc: String) -> Self {
        Self {
            idx,
            return_type,
            desc,
        }
    }
}

impl Expr for ColumnRefExpr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            let len = slice.get_num_tuple();
            let mut res = vec![];
            for idx in 0..len {
                res.push(slice.at(idx).unwrap().remove(self.idx));
            }
            res
        } else {
            vec![]
        }
    }
    fn return_type(&self) -> DataType {
        self.return_type
    }
    fn name(&self) -> String {
        self.desc.clone()
    }
}
