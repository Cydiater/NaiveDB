mod db;
mod parser;
mod storage;

use crate::db::NaiveDB;
use std::io::{self, Write};

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
