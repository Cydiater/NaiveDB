use crate::planner::Plan;
use crate::storage::BufferPoolManager;
use catalog::Catalog;

mod catalog;

#[allow(dead_code)]
pub struct Engine {
    pub bpm: BufferPoolManager,
    pub catalog: Catalog,
}

impl Engine {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            bpm: BufferPoolManager::new(buffer_size),
            catalog: Catalog {},
        }
    }
    pub fn execute(&mut self, _plan: Plan) {
        todo!()
    }
}
