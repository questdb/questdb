# Carrier-keyed local storage

This note explains `io.questdb.std.CarrierLocal` and
`io.questdb.mp.CarrierIdentity`.

## Why ThreadLocal is unsafe in a migrating fiber

`Fiber` uses a raw `jdk.internal.vm.Continuation`. A query can yield on
one worker and resume on another worker in the same Fiber-host pool.

`ThreadLocal.get()` resolves through `Thread.currentThread()`. HotSpot models
that call as the `_currentThread` intrinsic, and C2 may treat its value as
loop-invariant. User-space raw continuation code cannot use the boot-loader
only `@ChangesCurrentThread` annotation that protects JDK virtual threads.

A compiled query-fiber body can therefore retain carrier A's Java `Thread`
reference in a frozen frame and observe A's `ThreadLocal` map after carrier B
resumes it. If A concurrently uses the same entry, both carriers mutate
single-threaded state through one holder.

The worker loop itself is not a continuation. The hazard exists only for code
that runs inside a mounted fiber, but shared SQL, logging, exception, and
protocol helpers cannot safely assume that their caller is outside one.

## CarrierIdentity

`CarrierIdentity.bind()` assigns a process-wide integer to the current OS
thread. Worker threads and timer-shard threads bind on entry and unbind on
exit. Pool-local worker ids are unsuitable because different pools reuse the
same small ids.

`CarrierIdentity.current()` reads a Rust `thread_local!` slot through an FFI
critical downcall:

- `qdb_carrier_bind(int)` stores the id;
- `qdb_carrier_current()` reads it.

The opaque native call prevents C2 from replacing the current carrier with a
hoisted Java `Thread` reference. The Rust slot uses a const initializer, so
the normal read is a direct native TLS access without lazy initialization.

Both OSS and enterprise code must use the symbols from `libquestdbr`.
Independent `cdylib` files have independent native TLS slots.

If a future JDK treats the critical downcall as foldable, change it to a
non-critical downcall or JNI before relying on carrier-local state.

## CarrierLocal

`CarrierLocal.get()` uses the current carrier id to select a
`[carrierId][key]` entry. Each bound carrier owns one row, so a resumed query
reads the row of the worker that executes it now.

Unbound threads use a lazy Java `ThreadLocal` fallback. Bootstrap, test, and
shutdown threads normally take this path and do not migrate inside raw
continuations.

`CarrierIdentity.unbind()` clears the row before recycling its id. Values that
own native or closeable resources need an explicit thread-local cleaner;
clearing a row does not close arbitrary values.

## Required invariants

- Bind each carrier thread before it can run query-fiber or carrier-local code.
- Unbind only from that carrier's exit path.
- Never cache `CarrierIdentity.current()` or a carrier-local value across a
  suspension when the value represents carrier-confined mutable state.
- Use process-wide carrier ids, not pool worker ids.
- Release native resources explicitly before unbinding.

## Validation

Focused tests cover binding, id recycling, row isolation, and resuming a raw
continuation on a different carrier. HTTP/PG sleep and `wait_wal_table()`
integration tests exercise migration through production fibers.

The C2 failure depends on compilation and inlining shape, so a small
interpreter-only test cannot fully reproduce it. Logging-enabled concurrent
suspension stress remains the useful end-to-end guard.

## Files

- `core/rust/qdbr/src/carrier.rs`
- `core/src/main/java/io/questdb/mp/CarrierIdentity.java`
- `core/src/main/java/io/questdb/std/CarrierLocal.java`
- `core/src/main/java/io/questdb/mp/Worker.java`
- `core/src/main/java/io/questdb/mp/continuation/Fiber.java`
