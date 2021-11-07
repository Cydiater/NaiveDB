#![feature(vec_into_raw_parts)]

mod db;
mod parser;
mod planner;
mod storage;

use crate::db::NaiveDB;
use std::io::{self, Write};

#[macro_use]
extern crate lalrpop_util;
lalrpop_mod!(#[allow(clippy::all)] pub sql);

fn main() {
    env_logger::init();
    let db = NaiveDB {};
    loop {
        print!("navie_db > ");
        io::stdout().flush().unwrap();
        let mut sql = String::new();
        io::stdin().read_line(&mut sql).unwrap();
        match db.run(&sql) {
            Ok(()) => {
                todo!();
            }
            Err(err) => {
                println!("Error: {}", err.to_string());
            }
        }
    }
}
