#![feature(vec_into_raw_parts)]

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

#[macro_use]
extern crate lalrpop_util;
lalrpop_mod!(#[allow(clippy::all)] pub sql);

fn main() {
    env_logger::init();
    let mut db = NaiveDB::new();
    let mut rl = Editor::<()>::new();
    loop {
        print!("naive_db > ");
        let readline = rl.readline("naive_db > ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                match db.run(line.as_str()) {
                    Ok(res) => {
                        print!("{}", res);
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
