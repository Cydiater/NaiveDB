use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;
use catalog::Catalog;

mod catalog;

pub struct Engine {
    pub bpm: BufferPoolManagerRef,
    pub catalog: Catalog,
}

impl Engine {
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        Self {
            bpm,
            catalog: Catalog {},
        }
    }
    pub fn execute(&mut self, _plan: Plan) {
        todo!()
    }
}
