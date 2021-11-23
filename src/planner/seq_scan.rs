use crate::expr::ExprImpl;
use crate::parser::ast::{SelectStmt, Selectors};
use crate::planner::{Plan, Planner};

pub struct SeqScanPlan {
    pub exprs: Vec<ExprImpl>,
    pub is_all: bool,
    pub table_name: String,
}

impl Planner {
    pub fn plan_seq_scan(&self, stmt: SelectStmt) -> Plan {
        match stmt.selectors {
            Selectors::All => Plan::SeqScan(SeqScanPlan {
                exprs: vec![],
                is_all: true,
                table_name: stmt.table_name,
            }),
            Selectors::Exprs(exprs) => Plan::SeqScan(SeqScanPlan {
                exprs,
                is_all: false,
                table_name: stmt.table_name,
            }),
        }
    }
}
