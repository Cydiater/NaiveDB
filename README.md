# NaiveDB
Yet another simple relational disk-oriented database written in rust.

## Architecture

TBD

## Parsing

In general, user type in SQL from command line, which we can not manipulate directly. To process the SQL, we need parse statement into AST. It's lucky that we have some ready-to-use packages. For this project, we use the crate [sqlparser](https://docs.rs/sqlparser/0.12.0/sqlparser/).

## Miscellaneous

### Log

You can enable logging to check the sanity of this database.

```sh
# INFO
RUST_LOG=info cargo run
# DEBUG
RUST_LOG=debug cargo run
```
