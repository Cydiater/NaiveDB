use crate::expr::ExprImpl;
use crate::parser::ast::{
    DropDatabaseStmt, DropForeignStmt, DropIndexStmt, DropPrimaryStmt, DropTableStmt,
};
use crate::planner::{Plan, PlanError, Planner};
use crate::table::SchemaError;
use itertools::Itertools;

#[derive(Debug)]
pub struct DropTablePlan {
    pub table_name: String,
}

#[derive(Debug)]
pub struct DropDatabasePlan {
    pub database_name: String,
}

#[derive(Debug)]
pub struct DropIndexPlan {
    pub table_name: String,
    pub exprs: Vec<ExprImpl>,
}

#[derive(Debug)]
pub struct DropForeignPlan {
    pub table_name: String,
    pub column_idxes: Vec<usize>,
}

#[derive(Debug)]
pub struct DropPrimaryPlan {
    pub table_name: String,
}

impl Planner {
    pub fn plan_drop_table(&self, stmt: DropTableStmt) -> Result<Plan, PlanError> {
        Ok(Plan::DropTable(DropTablePlan {
            table_name: stmt.table_name,
        }))
    }
    pub fn plan_drop_database(&self, stmt: DropDatabaseStmt) -> Result<Plan, PlanError> {
        Ok(Plan::DropDatabase(DropDatabasePlan {
            database_name: stmt.database_name,
        }))
    }
    pub fn plan_drop_foreign(&self, stmt: DropForeignStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let column_idxes = stmt
            .column_names
            .iter()
            .map(|column_name| {
                table
                    .schema
                    .index_by_column_name(column_name)
                    .ok_or(SchemaError::ColumnNotFound)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Plan::DropForeign(DropForeignPlan {
            table_name: stmt.table_name,
            column_idxes,
        }))
    }
    pub fn plan_drop_index(&self, stmt: DropIndexStmt) -> Result<Plan, PlanError> {
        let table = self.catalog.borrow().find_table(&stmt.table_name)?;
        let exprs = stmt
            .exprs
            .into_iter()
            .map(|e| ExprImpl::from_ast(&e, self.catalog.clone(), &table.schema, None).unwrap())
            .collect_vec();
        Ok(Plan::DropIndex(DropIndexPlan {
            table_name: stmt.table_name,
            exprs,
        }))
    }
    pub fn plan_drop_primary(&self, stmt: DropPrimaryStmt) -> Result<Plan, PlanError> {
        Ok(Plan::DropPrimary(DropPrimaryPlan {
            table_name: stmt.table_name,
        }))
    }
}
