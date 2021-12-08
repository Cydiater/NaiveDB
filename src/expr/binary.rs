use crate::datum::{DataType, Datum};
use crate::expr::{Expr, ExprImpl};
use crate::table::Slice;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Equal,
    LessThan,
    GreaterThan,
}

impl BinaryOp {
    pub fn gen_func(&self) -> fn(&Datum, &Datum) -> Datum {
        match self {
            Self::Equal => |l, r| {
                if l == r {
                    Datum::Bool(Some(true))
                } else {
                    Datum::Bool(Some(false))
                }
            },
            Self::LessThan => |l, r| {
                if l < r {
                    Datum::Bool(Some(true))
                } else {
                    Datum::Bool(Some(false))
                }
            },
            Self::GreaterThan => |l, r| {
                if l > r {
                    Datum::Bool(Some(true))
                } else {
                    Datum::Bool(Some(false))
                }
            },
        }
    }
}

#[derive(Debug)]
pub struct BinaryExpr {
    lhs: Box<ExprImpl>,
    rhs: Box<ExprImpl>,
    op: BinaryOp,
    desc: String,
}

impl BinaryExpr {
    pub fn new(lhs: Box<ExprImpl>, rhs: Box<ExprImpl>, op: BinaryOp) -> Self {
        Self {
            lhs,
            rhs,
            op,
            desc: "".to_string(),
        }
    }
}

impl Expr for BinaryExpr {
    fn eval(&self, slice: Option<&Slice>) -> Vec<Datum> {
        let datums_lhs = self.lhs.eval(slice);
        let datums_rhs = self.rhs.eval(slice);
        let func = self.op.gen_func();
        let datums = datums_lhs
            .iter()
            .zip(datums_rhs.iter())
            .map(|(l, r)| func(l, r))
            .collect_vec();
        datums
    }
    fn return_type(&self) -> DataType {
        match self.op {
            BinaryOp::Equal | BinaryOp::LessThan | BinaryOp::GreaterThan => {
                DataType::new_bool(false)
            }
        }
    }
    fn name(&self) -> String {
        self.desc.clone()
    }
}
