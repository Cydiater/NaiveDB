use crate::expr::ExprImpl;
use crate::planner::{Plan, Planner};
use crate::table::SchemaRef;

pub struct ValuesPlan {
    pub values: Vec<Vec<ExprImpl>>,
    pub schema: SchemaRef,
}

impl Planner {
    pub fn plan_values(&self, values: Vec<Vec<ExprImpl>>, schema: SchemaRef) -> Plan {
        Plan::Values(ValuesPlan { values, schema })
    }
}
