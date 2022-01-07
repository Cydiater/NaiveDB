use crate::datum::{DataType, Datum};
use crate::expr::Expr;
use crate::table::Slice;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ColumnRefExpr {
    idx: usize,
    return_type: DataType,
    column_name: String,
}

impl ColumnRefExpr {
    pub fn new(idx: usize, return_type: DataType, column_name: String) -> Self {
        Self {
            idx,
            return_type,
            column_name,
        }
    }
    pub fn idx(&self) -> usize {
        self.idx
    }
}

impl Expr for ColumnRefExpr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        if let Some(slice) = slice {
            slice
                .tuple_iter()
                .map(|mut tuple| tuple.remove(self.idx))
                .collect_vec()
        } else {
            vec![]
        }
    }
    fn return_type(&self) -> DataType {
        self.return_type
    }
    fn name(&self) -> String {
        self.column_name.clone()
    }
}
