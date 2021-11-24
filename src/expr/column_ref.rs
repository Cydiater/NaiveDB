use crate::expr::Expr;
use crate::table::{DataType, Datum, Slice};

#[derive(Debug)]
pub struct ColumnRefExpr {
    idx: usize,
    return_type: DataType,
}

impl ColumnRefExpr {
    pub fn new(idx: usize, return_type: DataType) -> Self {
        Self { idx, return_type }
    }
}

impl Expr for ColumnRefExpr {
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            let len = slice.len();
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
}
