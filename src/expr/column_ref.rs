use crate::datum::{DataType, Datum};
use crate::expr::Expr;
use crate::table::Slice;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    pub fn idx(&self) -> usize {
        self.idx
    }
}

impl Expr for ColumnRefExpr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            let len = slice.get_num_tuple();
            let mut res = vec![];
            for idx in 0..len {
                if let Some(mut tuple) = slice.at(idx).unwrap() {
                    res.push(tuple.remove(self.idx));
                }
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
