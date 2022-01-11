use crate::catalog::CatalogError;
use crate::expr::ExprImpl;
use crate::parser::ast::{AddForeignStmt, AddIndexStmt, AddPrimaryStmt, AddUniqueStmt};
use crate::planner::{Plan, PlanError, Planner};
use itertools::Itertools;

#[derive(Debug)]
pub struct AddIndexPlan {
    pub table_name: String,
    pub exprs: Vec<ExprImpl>,
}

#[derive(Debug)]
pub struct AddUniquePlan {
    pub table_name: String,
    pub unique_set: Vec<usize>,
}

#[derive(Debug)]
pub struct AddPrimaryPlan {
    pub table_name: String,
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub struct AddForeignPlan {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub ref_table_name: String,
    pub ref_column_names: Vec<String>,
}

impl Planner {
    pub fn plan_add_index(&self, stmt: AddIndexStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name).unwrap();
        let exprs = stmt
            .exprs
            .into_iter()
            .map(|node| {
                ExprImpl::from_ast(&node, self.catalog.clone(), table.schema.as_ref(), None)
                    .unwrap()
            })
            .collect_vec();
        Ok(Plan::AddIndex(AddIndexPlan {
            table_name: stmt.table_name,
            exprs,
        }))
    }
    pub fn plan_add_unique(&self, stmt: AddUniqueStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let unique_set = stmt
            .column_names
            .iter()
            .map(|column_name| {
                table
                    .schema
                    .index_by_column_name(column_name)
                    .ok_or(CatalogError::EntryNotFound)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Plan::AddUnique(AddUniquePlan {
            table_name: stmt.table_name,
            unique_set,
        }))
    }
    pub fn plan_add_primary(&self, stmt: AddPrimaryStmt) -> Result<Plan, PlanError> {
        Ok(Plan::AddPrimary(AddPrimaryPlan {
            table_name: stmt.table_name,
            column_names: stmt.column_names,
        }))
    }
    pub fn plan_add_foreign(&self, stmt: AddForeignStmt) -> Result<Plan, PlanError> {
        Ok(Plan::AddForeign(AddForeignPlan {
            table_name: stmt.table_name,
            column_names: stmt.column_names,
            ref_table_name: stmt.ref_table_name,
            ref_column_names: stmt.ref_column_names,
        }))
    }
}
