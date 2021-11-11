use crate::parser::ast::Statement;
pub use create_database::CreateDatabasePlan;

mod create_database;

pub enum Plan {
    CreateDatabase(CreateDatabasePlan),
    ShowDatabases,
}

pub struct Planner;

#[allow(dead_code)]
impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn plan(&self, stmt: Statement) -> Plan {
        match stmt {
            Statement::CreateDatabase(stmt) => self.plan_create_database(stmt),
            Statement::ShowDatabases => Plan::ShowDatabases,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::ast::{CreateDatabaseStmt, Statement};
    use crate::planner::{Plan, Planner};

    #[test]
    fn test_gen_create_database_plan() {
        let planner = Planner::new();
        let stmt = Statement::CreateDatabase(CreateDatabaseStmt {
            database_name: "sample_database".to_string(),
        });
        let plan = planner.plan(stmt);
        if let Plan::CreateDatabase(plan) = plan {
            assert_eq!(plan.database_name, "sample_database");
        } else {
            panic!("not create_database plan");
        }
    }
}
