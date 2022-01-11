#![feature(vec_into_raw_parts)]
#![feature(generic_const_exprs)]
#![feature(inherent_associated_types)]
#![allow(incomplete_features)]

mod catalog;
mod datum;
mod execution;
mod expr;
mod index;
mod parser;
mod planner;
mod storage;
mod table;

mod db;

use crate::db::NaiveDB;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use std::time::Instant;

#[macro_use]
extern crate lalrpop_util;
lalrpop_mod!(#[allow(clippy::all)] pub sql);

fn main() {
    env_logger::init();
    let mut db = NaiveDB::new();
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("naive_db > ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let start = Instant::now();
                match db.run(line.as_str()) {
                    Ok(res) => {
                        println!("{}", res);
                        println!("Elapsed Time: {:?}", start.elapsed())
                    }
                    Err(err) => {
                        println!("Error: {}", err);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
