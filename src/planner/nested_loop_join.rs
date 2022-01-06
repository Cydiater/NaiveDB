use super::{Plan, Planner};
use crate::table::SchemaRef;

#[derive(Debug)]
pub struct NestedLoopJoinPlan {
    pub childs: Vec<Plan>,
    pub schema: SchemaRef,
}

impl Planner {
    pub fn plan_nested_loop_join(&self, mut plans: Vec<Plan>, schema: SchemaRef) -> Plan {
        match plans.len() {
            1 => plans.remove(0),
            _ => Plan::NestedLoopJoin(NestedLoopJoinPlan {
                childs: plans,
                schema,
            }),
        }
    }
}
