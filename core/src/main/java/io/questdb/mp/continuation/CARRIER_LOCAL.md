# Carrier-keyed thread-local storage

Background and rationale for `io.questdb.std.CarrierLocal` and
`io.questdb.mp.CarrierIdentity`. Read this if you are touching either class,
the worker continuation machinery, or anything that stores per-thread state on
threads that run inside `Worker.loopBody`.

## Problem

QuestDB's worker pool runs each iteration of `Worker.loopBody` inside a raw
`jdk.internal.vm.Continuation`. Suspending functions (`TxnWaiter.suspend`,
`Worker.loopBody`'s handoff yield) call `Continuation.yield(SCOPE)`, freezing
the cont's frame state. The cont can later be remounted by a *different*
worker carrier via `Continuation.run()`.

`java.lang.ThreadLocal.get()` resolves through `Thread.currentThread()`,
which is the `_currentThread` HotSpot intrinsic. C2 models `_currentThread`
as a thread-pinned constant and is free to hoist its result out of loops via
LICM. Inside the C2-compiled `Worker.loopBody`, this hoisting moves the
`Thread` reference into the loop preheader, where it becomes part of the
cont's frozen frame state on yield.

The failure sequence:

1. Carrier A enters `loopBody`. Preheader caches `t = currentThread()` -> A's
   `Thread`.
2. A runs work. `tl.get()` resolves `t.threadLocals.getEntry(tl)` -> holder
   `H_A`.
3. A's loopBody yields. Cont frames freeze, including the cached `t`.
4. Carrier B remounts the cont. Frames thaw. Cached `t` is restored unchanged
   - still A's `Thread`.
5. The body resumes on B but every `tl.get()` still resolves through cached
   `t` to A's `threadLocals`, returning `H_A`.
6. Concurrently, A is running its own fresh cont and also hits `H_A`.
7. Both carriers read and write `H_A` without synchronization. Cross-carrier
   corruption.

The JDK's defense for its own continuation user (`VirtualThread`) is
`@ChangesCurrentThread` on `VirtualThread.runContinuation`, which tells C2
that `_currentThread` may change across the call and forces re-evaluation.
The annotation lives in `jdk.internal.vm.annotation` and is honored only on
boot-loader-loaded classes. User-space continuation consumers (such as
`WorkerContinuation`) cannot apply it.

For a full reproducible-failure analysis with experiment data, see the
investigation notes attached to the original incident
(`wait_wal_table` cross-carrier holder corruption).

## Solution

Replace `java.lang.ThreadLocal` with carrier-keyed storage that does not go
through `_currentThread`.

Two pieces:

- `io.questdb.mp.CarrierIdentity` - one integer per OS thread, stored in TLS
  on the native side. Read via an FFI critical downcall into Rust
  (`qdb_carrier_current` in `core/rust/qdbr/src/carrier.rs`). C2 cannot fold
  the downcall with hoisted `_currentThread` because the linker emits an
  opaque call site that the optimizer is not allowed to elide. Each call
  re-reads native TLS and reflects the actual carrier executing now.

- `io.questdb.std.CarrierLocal` - a `ThreadLocal`-shaped class whose `get()`
  uses `CarrierIdentity.current()` as an index into a `[carrierId][slot]`
  array. The array entries are never aliased across carriers because the
  carrier id is fresh on every access.

`Worker.run()` calls `CarrierIdentity.bind()` once before entering the cont
driver loop. `bind()` allocates a globally-unique id from a static counter
(pool-local `workerId` is *not* safe - every pool numbers from 0, so two
pools' worker 0 would alias the same row). Threads that never bind (test
runners, ServerMain bootstrap, shutdown hooks) fall through to a per-`Thread`
`java.lang.ThreadLocal`; they do not execute inside the cont scheduler, so
the hoist hazard does not apply to them.

## Why FFI rather than JNI

Both work for the hoist defense - the optimizer treats either as opaque.
FFI is the choice here because:

- A critical downcall (`Linker.Option.critical(false)`) skips the Java/native
  thread-state transition that JNI requires, dropping per-call overhead from
  ~5-10 ns to ~1-3 ns.
- The Rust crate `qdbr` is already a `cdylib` loaded into the process, so the
  symbols `qdb_carrier_bind` / `qdb_carrier_current` resolve through the
  existing `SymbolLookup.loaderLookup()` without a separate native lib.
- Logging is on a hot path. The full chain `LOG.x().$()...$()` issues at
  least two `current()` calls (open + close); shaving the per-call cost is
  worth the FFI plumbing.

If a future JDK ever marks critical downcalls as foldable / pure / leaf, this
module must move to a non-critical downcall (which forces a thread-state
transition C2 cannot cross) or back to JNI.

## Why `thread_local!` with `const` initializer in Rust

```rust
thread_local! {
    static CARRIER_ID: Cell<i32> = const { Cell::new(-1) };
}
```

The `const { ... }` form (Rust 1.59+) gives a const-initialized TLS slot
backed by `#[thread_local]`-equivalent codegen. Access is a single
TLS-relative `mov` from `fs:` / `gs:` - no lazy-init branch on first touch.
Without `const`, `thread_local!` adds a runtime init check via
`pthread_getspecific`, which is ~3-5 ns slower per call.

## Cross-cdylib placement

Both `qdbr` (OSS) and `qdb-ent` are independent `cdylib`s loaded into the
same process. Under the GLOBAL_DYNAMIC TLS model, a `thread_local!` in
`qdbr` is a *different storage slot* from a `thread_local!` in `qdb-ent`.
The carrier symbols therefore live in `qdbr` only. Enterprise code accessing
carrier identity goes through the same `CarrierIdentity` class, which
FFI-binds against `libquestdbr`.

## Diagnostics

The FFI primitive is observable - if a future C2 change starts hoisting the
downcall, the failure mode reappears as `ABANDONED LOG RECORD` markers in
the log or as the assertion `h.isLogRecordInProgress` in
`AbstractLogRecord.$()` firing. The assertion message prints both
`CarrierIdentity.current()` and `h.carrierId`; a mismatch indicates
cross-carrier holder aliasing.

To rule out hoisting on a particular JDK, run with
`-XX:+UnlockDiagnosticVMOptions -XX:+PrintInlining` and confirm
`CarrierIdentity.current` shows as a real call rather than an inlined leaf.

## Files

- `core/rust/qdbr/src/carrier.rs` - native side.
- `core/src/main/java/io/questdb/mp/CarrierIdentity.java` - FFI binding +
  global id allocator.
- `core/src/main/java/io/questdb/std/CarrierLocal.java` - the
  `ThreadLocal`-shaped API used by callers.
- `core/src/main/java/io/questdb/mp/Worker.java` - `bind()` call site.
