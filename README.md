# NaiveDB

<img src="./img/logo.jpg" height=200/>

Yet another simple relational disk-oriented database written in rust.

## Architecture

The NaiveDB is designed to be running in one single thread, so I do not need to worry about things like lock and concurrency control.

TBD

## Storage

The most basic job of our database is to manage the storage, including the user memory and disk. The smallest manageable unit of our storage will be  `Page`,  which is sized about 4 KB.

### Page

`Page` is the basic unit of our storage. In out implementation, each `Page` object will have 3 fields:

- `data` store the raw binary of the page, sized about 4KB
- `is_dirty` used by Buffer Pool Manager, to mark wheather this page is needed to write back to disk
- `page_id` the id of this page, from this id we can calculate the offset of this page in disk
- `pin_count` the number of times the database engine using this page

`PageRef` is essitially the `Rc<RefCell<Page>>`, we use this form to maintain the shared mutable refernece. For more information about shared mutable reference, please refer to https://doc.rust-lang.org/std/cell/index.html#introducing-mutability-inside-of-something-immutable.

### Buffer Pool Manager

Buffer Pool Manager(BPM) manage the page in memory. You can see this as the cache between the disk and our execution engine.

### Disk Manager

The job of `disk_manager` is to create, read and write data into the disk. We use the File System provided by our OS to achieve this. The `disk_manager` contains a `File`, which represent the disk space. We also should contain a Bitmap or Linked List to track the freed page to support the page resue, but for now we don't consider that.

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
