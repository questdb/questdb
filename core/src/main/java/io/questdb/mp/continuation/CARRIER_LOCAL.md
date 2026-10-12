# Carrier-keyed and fiber-owned local storage

This note explains `io.questdb.mp.CarrierIdentity`,
`io.questdb.std.CarrierLocal` and `io.questdb.std.FiberLocal`.
`CarrierIdentity` answers which OS thread is executing. `CarrierLocal` keys
state by that carrier and suits state that describes the carrier.
`FiberLocal` keys mutable scratch by the running fiber, so a reference cached
before a yield stays valid after the fiber resumes on another carrier.

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

That guarantee covers only the lookup. It defeats the hoisted `Thread`
reference described above, but a reference cached in a local variable before
a yield still names the old carrier's value after the fiber resumes
elsewhere, while a second fiber on the old carrier obtains the same object.
Two threads then mutate one buffer.

Unbound threads use a lazy Java `ThreadLocal` fallback. Bootstrap, test, and
shutdown threads normally take this path and do not migrate inside raw
continuations.

`CarrierIdentity.unbind()` clears the row before recycling its id. Values that
own native or closeable resources need an explicit thread-local cleaner;
clearing a row does not close arbitrary values.

## FiberLocal

`FiberLocal` gives mutable scratch to the fiber whose code is running rather
than to the carrier. A fiber runs on one carrier at a time, so a reference
obtained before a yield still names the fiber's own object after it resumes
elsewhere, and two fibers never share a buffer.

Each `FiberLocal` takes a global slot index at construction. Every owner, a
carrier or a fiber, keeps one `ObjList<Object>` of slots. `CarrierIdentity`
selects the carrier's `Holder`, whose `current` list is the carrier's own list
while plain job code runs. Both mount sites, `Fiber.runMounted()` and
`Fiber.releaseRoleSwitchReadLock()`, call `FiberLocal.enter()` with the
fiber's list next to `scope.fiber = this` and `FiberLocal.exit()` in the same
`finally`, so `current` always names the table of the code that executes on
that carrier. `get()` is one carrier lookup, one holder read and one slot
read, with no hashing and no mount check; mount already answered.

Unbound threads keep a per-`Thread` holder, matching the `CarrierLocal`
fallback.

`FiberPool.onRetired()` frees the fiber's slots after `completeRetirement()`,
which is where a `Path` releases its native buffer. `removeAndFree()` frees
the current owner's slot; `Path.clearThreadLocals()` calls it on worker exit
as before. `CarrierIdentity.unbind()` drops the carrier's holder and, like a
cleared `CarrierLocal` row, closes nothing.

`FiberLocal` has no `set()`. A holder that installs a value from outside,
such as a cursor cached after construction, has no `FiberLocal` equivalent.

Slot indexes are never recycled. Declare `FiberLocal` as `static final` only;
an instance field would take a fresh index per instance and grow every
owner's table.

## Choosing between them

- Mutable scratch that fiber-executed code can reach, such as sinks, paths,
  lists, format compilers and preallocated exceptions: `FiberLocal`.
- State that describes the carrier itself (`Worker.CURRENT`,
  `SuspensionScope`, memory-tracker thread state, log records), per-carrier
  pools and striping, and any instance field: `CarrierLocal`.
- Heavy or native-backed scratch stays a `CarrierLocal` when its holding
  scope provably contains no checkpoint: fiber ownership multiplies retained
  scratch by live fibers per pool instead of by carriers.
- Writer, WAL and O3 code keeps its carrier-locals safely only because it
  contains no cooperative checkpoint. Adding one there reopens the migration
  hazard for every carrier-local that code touches.

## Required invariants

- Bind each carrier thread before it can run query-fiber or carrier-local code.
- Unbind only from that carrier's exit path.
- Never cache `CarrierIdentity.current()` or a carrier-local value across a
  suspension. Mutable scratch that fiber code can reach is therefore a
  `FiberLocal`, which removes the rule rather than relying on it.
- Declare `FiberLocal` as `static final`; slot indexes are never recycled.
- Use process-wide carrier ids, not pool worker ids.
- Release native resources explicitly before unbinding.

## Validation

Focused tests cover binding, id recycling, row isolation, and resuming a raw
continuation on a different carrier. HTTP/PG sleep and `wait_wal_table()`
integration tests exercise migration through production fibers.

The C2 failure depends on compilation and inlining shape, so a small
interpreter-only test cannot fully reproduce it. Logging-enabled concurrent
suspension stress remains the useful end-to-end guard.

`FiberLocalTest` covers the `FiberLocal` claims above: a nested `enter()`
restores the outer table, `removeAndFree()` frees only the mounted owner's
slot, `unbind()` releases the carrier's slots, an unbound thread keeps its
own slots, and a value travels with its fiber across carriers.

Performance: `FiberLocalBenchmark` measures `FiberLocal.get()` against
`CarrierLocal.get()` on a plain carrier and inside a mounted fiber.

## Files

- `core/rust/qdbr/src/carrier.rs`
- `core/src/main/java/io/questdb/mp/CarrierIdentity.java`
- `core/src/main/java/io/questdb/std/CarrierLocal.java`
- `core/src/main/java/io/questdb/std/FiberLocal.java`
- `core/src/main/java/io/questdb/mp/Worker.java`
- `core/src/main/java/io/questdb/mp/continuation/Fiber.java`
- `core/src/main/java/io/questdb/mp/continuation/FiberPool.java`
