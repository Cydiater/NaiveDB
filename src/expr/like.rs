use crate::datum::{DataType, Datum};
use crate::expr::{Expr, ExprImpl};
use crate::table::Slice;
use itertools::Itertools;
use like::Like;

#[derive(Debug, PartialEq, Clone)]
pub struct LikeExpr {
    child: Box<ExprImpl>,
    pattern: String,
}

impl LikeExpr {
    pub fn new(pattern: &str, child: Box<ExprImpl>) -> Self {
        Self {
            child,
            pattern: pattern.to_owned(),
        }
    }
}

impl Expr for LikeExpr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        let datums = self.child.eval(slice);
        datums
            .into_iter()
            .map(|d| match d {
                Datum::VarChar(Some(d)) => Like::<false>::like(d.as_str(), &self.pattern)
                    .unwrap()
                    .into(),
                _ => todo!(),
            })
            .collect_vec()
    }
    fn return_type(&self) -> DataType {
        DataType::new_as_bool(false)
    }
}
