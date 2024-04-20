This Rust crate compiles to a dynamic library that is loaded
up as a JNI extension in Java by the `io.questdb.std.Qdb` class.

It is just a wrapper of the `qdb-core` crate, which is where all the
actual implementation is.
