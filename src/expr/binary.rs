use crate::datum::{DataType, Datum};
use crate::expr::{Expr, ExprImpl};
use crate::table::Slice;
use itertools::Itertools;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
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

impl fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let lhs = self.lhs.to_string();
        let rhs = self.rhs.to_string();
        match self.op {
            BinaryOp::Equal => write!(f, "{} = {}", lhs, rhs),
            BinaryOp::LessThan => write!(f, "{} < {}", lhs, rhs),
            BinaryOp::GreaterThan => write!(f, "{} > {}", lhs, rhs),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
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
    pub fn get_bound(&self, expr: &ExprImpl) -> (Option<Datum>, Option<Datum>) {
        if expr == self.lhs.as_ref() {
            let datum = if let ExprImpl::Constant(c) = self.rhs.as_ref() {
                c.get_value()
            } else {
                return (None, None);
            };
            match self.op {
                BinaryOp::Equal => (Some(datum.clone()), Some(datum)),
                BinaryOp::LessThan => (None, Some(datum)),
                BinaryOp::GreaterThan => (Some(datum), None),
            }
        } else if expr == self.rhs.as_ref() {
            let datum = if let ExprImpl::Constant(c) = self.lhs.as_ref() {
                c.get_value()
            } else {
                return (None, None);
            };
            match self.op {
                BinaryOp::Equal => (Some(datum.clone()), Some(datum)),
                BinaryOp::LessThan => (Some(datum), None),
                BinaryOp::GreaterThan => (None, Some(datum)),
            }
        } else {
            (None, None)
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
                DataType::new_as_bool(false)
            }
        }
    }
}
