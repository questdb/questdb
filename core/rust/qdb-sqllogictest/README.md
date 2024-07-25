# SqlLogicTest QuestDB Rust JNI bindings

This project contains modified sources of [sqllogictest-rs](https://github.com/risinglightdb/sqllogictest-rs) and JNI bindings
to execute sqllogic tests format files as Java unit tests.

### Usage

The output of this project is used in Java unit tests, see `io.questdb.test.sqllogictest.parquet`.

### Mofiication

If you need to modify the sources of the project, you can run 

```
cargo build

# or

cargo build --release
```

that will output the shared library in `rust/qdb-sqllogictest/target/debug` or `rust/qdb-sqllogictest/target/release`
respectively. Java unit tests will probe
those directories and load directly from there if the file is the newest.

When finished, commit the changes and run `.github/workflows/rebuild_rust_test.yml` to commit the compiled binaries to
test resource directories at `src/test/resources/io/questdb/bin/<platforms>`