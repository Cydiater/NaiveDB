use crate::catalog::Catalog;
use crate::planner::Plan;
use crate::storage::BufferPoolManagerRef;

#[allow(dead_code)]
pub struct Engine {
    bpm: BufferPoolManagerRef,
    database_catalog: Catalog,
}

impl Engine {
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        let num_pages = bpm.borrow().num_pages().unwrap();
        // allocate database catalog
        if num_pages == 0 {
            let _ = bpm.borrow_mut().alloc().unwrap();
        }
        Self {
            bpm: bpm.clone(),
            database_catalog: Catalog::new_database_catalog(bpm),
        }
    }
    pub fn execute(&mut self, _plan: Plan) {
        todo!()
    }
}
