use crate::catalog::CatalogManager;
use crate::execution::{Engine, ExecutionError};
use crate::parser::parse;
use crate::planner::Planner;
use crate::storage::{BufferPoolManager, BufferPoolManagerRef};
use crate::table::Table;
use std::cell::RefCell;
use std::rc::Rc;
use thiserror::Error;

pub struct NaiveDB {
    #[allow(dead_code)]
    bpm: BufferPoolManagerRef,
    engine: Engine,
    planner: Planner,
}

impl NaiveDB {
    #[allow(dead_code)]
    pub fn filename(&self) -> String {
        self.bpm.borrow().filename()
    }
    #[allow(dead_code)]
    pub fn new_random() -> Self {
        let bpm = BufferPoolManager::new_random_shared(4096);
        let catalog = CatalogManager::new_shared(bpm.clone());
        Self {
            bpm: bpm.clone(),
            engine: Engine::new(catalog.clone(), bpm),
            planner: Planner::new(catalog),
        }
    }
    #[allow(dead_code)]
    pub fn new_with_name(filename: String) -> Self {
        let bpm = Rc::new(RefCell::new(BufferPoolManager::new_with_name(
            4096, filename,
        )));
        let catalog = CatalogManager::new_shared(bpm.clone());
        Self {
            bpm: bpm.clone(),
            engine: Engine::new(catalog.clone(), bpm),
            planner: Planner::new(catalog),
        }
    }
    pub fn new() -> Self {
        let bpm = BufferPoolManager::new_shared(4096);
        let catalog = CatalogManager::new_shared(bpm.clone());
        Self {
            bpm: bpm.clone(),
            engine: Engine::new(catalog.clone(), bpm),
            planner: Planner::new(catalog),
        }
    }
    pub fn run(&mut self, sql: &str) -> Result<Table, NaiveDBError> {
        let stmt = parse(sql)?;
        let plan = self.planner.plan(stmt);
        let table = self.engine.execute(plan)?;
        Ok(table)
    }
}

#[derive(Error, Debug)]
pub enum NaiveDBError {
    #[error("ParseError: {0}")]
    Parse(String),
    #[error("ExecutionError: {0}")]
    Execution(#[from] ExecutionError),
}

#[cfg(test)]
mod tests {
    use crate::datum::Datum;
    use crate::db::NaiveDB;
    use itertools::Itertools;
    use std::fs::remove_file;

    #[test]
    fn test_insert_select() {
        let filename = {
            let mut db = NaiveDB::new_random();
            let filename = db.filename();
            db.run("create database d;").unwrap();
            db.run("use d;").unwrap();
            db.run("create table t (v1 int not null);").unwrap();
            db.run("insert into t values (1), (2), (3);").unwrap();
            let table = db.run("select * from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1))],
                    vec![Datum::Int(Some(2))],
                    vec![Datum::Int(Some(3))],
                ]
            );
            let table = db.run("select v1 from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1))],
                    vec![Datum::Int(Some(2))],
                    vec![Datum::Int(Some(3))],
                ]
            );
            let table = db.run("select v1 from t where v1 = 1;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(tuples, vec![vec![Datum::Int(Some(1))],]);
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_persistent() {
        let filename = {
            let mut db = NaiveDB::new_random();
            let filename = db.filename();
            db.run("create database d;").unwrap();
            db.run("use d;").unwrap();
            db.run("create table t (v1 int not null);").unwrap();
            db.run("insert into t values (1), (2), (3);").unwrap();
            let table = db.run("select * from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1))],
                    vec![Datum::Int(Some(2))],
                    vec![Datum::Int(Some(3))],
                ]
            );
            let table = db.run("select v1 from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1))],
                    vec![Datum::Int(Some(2))],
                    vec![Datum::Int(Some(3))],
                ]
            );
            filename
        };
        let filename = {
            let mut db = NaiveDB::new_with_name(filename.clone());
            db.run("use d;").unwrap();
            let table = db.run("select * from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1))],
                    vec![Datum::Int(Some(2))],
                    vec![Datum::Int(Some(3))],
                ]
            );
            filename
        };
        remove_file(filename).unwrap();
    }

    #[test]
    fn test_null() {
        let filename = {
            let mut db = NaiveDB::new_random();
            let filename = db.filename();
            db.run("create database d;").unwrap();
            db.run("use d;").unwrap();
            db.run("create table t (v1 int null, v2 varchar null);")
                .unwrap();
            db.run("insert into t values (1, 'foo'), (2, null), (null, 'bar');")
                .unwrap();
            let table = db.run("select * from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1)), Datum::VarChar(Some("foo".to_string()))],
                    vec![Datum::Int(Some(2)), Datum::VarChar(None)],
                    vec![Datum::Int(None), Datum::VarChar(Some("bar".to_string()))],
                ]
            );
            let table = db.run("select v1 from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(1))],
                    vec![Datum::Int(Some(2))],
                    vec![Datum::Int(None)],
                ]
            );
            filename
        };
        remove_file(filename).unwrap();
    }
}
