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
    use rand::Rng;
    use std::collections::HashSet;
    use std::fs::remove_file;

    #[test]
    fn chaos_test() {
        let filename = {
            let mut db = NaiveDB::new_random();
            let filename = db.filename();
            db.run("create database d;").unwrap();
            db.run("use d;").unwrap();
            db.run("create table t (v1 int not null, primary key (v1));")
                .unwrap();
            let mut set: HashSet<u16> = HashSet::new();
            let mut rng = rand::thread_rng();
            for _ in 0..500 {
                let num: u16 = rng.gen();
                if set.contains(&num) {
                    set.remove(&num);
                    db.run(format!("delete from t where v1 = {};", num).as_str())
                        .unwrap();
                } else {
                    set.insert(num);
                    db.run(format!("insert into t values ({});", num).as_str())
                        .unwrap();
                }
            }
            for num in set.iter().sorted() {
                assert_eq!(
                    db.run(format!("select * from t where v1 = {};", num).as_str())
                        .unwrap()
                        .iter()
                        .collect_vec(),
                    [[Datum::Int(Some(*num as i32))]],
                );
            }
            filename
        };
        remove_file(filename).unwrap()
    }

    #[test]
    fn index_test() {
        let filename = {
            let mut db = NaiveDB::new_random();
            let filename = db.filename();
            db.run("create database d;").unwrap();
            db.run("use d;").unwrap();
            db.run("create table t (v1 int not null, v2 varchar not null, primary key (v1, v2));")
                .unwrap();
            db.run("insert into t values (1, '1'), (2, '2'), (3, '3');")
                .unwrap();
            db.run("alter table t add index (v1);").unwrap();
            db.run("select * from t where v1 > 1;").unwrap();
            db.run("insert into t values (4, '4'), (5, '5'), (6, '6');")
                .unwrap();
            let table = db.run("select * from t where v1 > 1 and v1 < 6;").unwrap();
            let res = table.iter().collect_vec();
            assert_eq!(
                res,
                vec![
                    vec![Datum::Int(Some(2)), Datum::VarChar(Some("2".to_string()))],
                    vec![Datum::Int(Some(3)), Datum::VarChar(Some("3".to_string()))],
                    vec![Datum::Int(Some(4)), Datum::VarChar(Some("4".to_string()))],
                    vec![Datum::Int(Some(5)), Datum::VarChar(Some("5".to_string()))],
                ]
            );
            filename
        };
        remove_file(filename).unwrap()
    }

    #[test]
    fn basic_test() {
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
            db.run("alter table t add index (v1);").unwrap();
            db.run("desc t;").unwrap();
            db.run("drop table t;").unwrap();
            db.run("create table t (v1 int not null);").unwrap();
            db.run("insert into t values (4), (5), (6);").unwrap();
            let table = db.run("select * from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![
                    vec![Datum::Int(Some(4))],
                    vec![Datum::Int(Some(5))],
                    vec![Datum::Int(Some(6))],
                ]
            );
            db.run("delete from t where v1 = 5;").unwrap();
            let table = db.run("select * from t;").unwrap();
            let tuples = table.iter().collect_vec();
            assert_eq!(
                tuples,
                vec![vec![Datum::Int(Some(4))], vec![Datum::Int(Some(6))],]
            );
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
