use crate::parser::ast::ExprNode;
use crate::planner::{Plan, PlanError, Planner};
use crate::storage::PageID;
use itertools::Itertools;

#[derive(Debug)]
pub struct DeletePlan {
    pub child: Box<Plan>,
    pub index_page_ids: Vec<PageID>,
    pub table_page_id: PageID,
}

impl Planner {
    pub fn plan_delete(
        &self,
        table_name: &str,
        where_exprs: &[ExprNode],
    ) -> Result<Plan, PlanError> {
        let plan = self.plan_scan(table_name, where_exprs, true);
        let table = self.catalog.borrow().find_table(table_name)?;
        let plan = self.plan_filter(table.schema.as_ref(), where_exprs, plan);
        let indexes = self
            .catalog
            .borrow()
            .find_indexes_by_table(table_name)
            .unwrap();
        let index_page_ids = indexes
            .into_iter()
            .map(|index| index.get_page_id())
            .collect_vec();
        Ok(Plan::Delete(DeletePlan {
            child: Box::new(plan),
            index_page_ids,
            table_page_id: self
                .catalog
                .borrow()
                .find_table(table_name)
                .unwrap()
                .page_id(),
        }))
    }
}
