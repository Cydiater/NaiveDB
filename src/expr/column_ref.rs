use crate::expr::Expr;
use crate::table::{Datum, Slice};

#[derive(Debug)]
pub struct ColumnRefExpr {
    field_name: String,
}

impl ColumnRefExpr {
    pub fn new(field_name: String) -> Self {
        Self { field_name }
    }
}

impl Expr for ColumnRefExpr {
    fn eval(&mut self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            let len = slice.len();
            let field_idx = slice.schema.index_of(self.field_name.clone()).unwrap();
            let mut res = vec![];
            for idx in 0..len {
                res.push(slice.at(idx).unwrap().remove(field_idx));
            }
            res
        } else {
            vec![]
        }
    }
}
