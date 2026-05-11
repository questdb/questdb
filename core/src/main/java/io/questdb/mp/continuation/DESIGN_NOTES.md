# Continuation runtime: design rationale

Background for race-shape and resource decisions that look suspicious in
review but are intentional. Read this before proposing "defensive" fixes
to the worker continuation, `TxnWaiter`, `TimerCont`, or `TimerShards`.

For the C2 hoist hazard and the `CarrierLocal` design, see
`CARRIER_LOCAL.md`. This document covers everything else.

## Continuation allocation per outer-driver iteration

`Worker.run()` allocates a fresh `WorkerContinuation` (which wraps a
`jdk.internal.vm.Continuation`) on every iteration of the outer driver
loop. Each `Continuation` carries a native stack chunk (typically
16-64KB).

This is unavoidable today. `jdk.internal.vm.Continuation` exposes no
`reset()` / `reuse()` operation; once a body returns or the cont becomes
`isDone()`, the object is single-use and must be discarded. Worker.run's
fresh-per-iteration policy is what lets a peer-mounted cont stay parked
inside a suspending function while the worker that created the outer
cont proceeds to the next iteration without aliasing.

GC pressure scales with yield frequency. On a workload with many
concurrent `wait_wal_table` / `sleep` parks, native stack chunks can land
in the G1 humongous-allocation path. If that ever becomes a measurable
hot spot, the answer is *not* to pool `WorkerContinuation` instances --
the JDK rejects reuse. The answer is either:

1. A future JDK exposing continuation reset (proposed in OpenJDK
   discussions but not landed), or
2. Coarser-grained suspending: have one long-lived cont per worker that
   yields on a multiplexed wakeup instead of a fresh cont per query.

Neither is in scope for this change. Document the cost in release notes
when relevant; do not work around it.

## Peer busy-spin in `Worker.mountForeignCont` is bounded

The spin loop in `mountForeignCont` looks unbounded. It is not. The peer
only spins when `cont.run()` throws `IllegalStateException`, which means
the cont is still mounted on its originating carrier. The spin exits on
any of:

- `cont.consumeParkRefused()` -- a phantom resume was enqueued before
  the body's `suspend()` returned false; the body marks parkRefused
  inside `abortContinuation`. The mark happens within a handful of
  instructions on the body's carrier after `suspend()` returns.
- `cont.isDone()` -- the originating body has unwound (e.g. on shutdown
  the breaker tripped or `isShuttingDown()` threw).
- `lifecycle != RUNNING` -- worker is halting; abandon the cont.

For a peer to have dequeued the cont in the first place,
`scheduleResume()` must have been issued. The only producers are
`tryFire` / `expire` / `shutdown`, all of which sit on a path where the
matching state transition (CAS to FIRED or CANCELLED) happens on or
before the body reaches the abort that sets parkRefused. The
peer-visible window between scheduleResume's enqueue and the body's
parkRefused write is bounded by program-order writes on the body's
carrier -- microseconds.

The "what if the carrier stays pinned forever" scenario does not
produce an unbounded spin: if the carrier never reaches
`abortContinuation`, no `scheduleResume` was issued either (the body is
still running, never tried to suspend), so the peer never dequeued the
cont.

## `TxnWaiter.reset()` re-registers without removing the prior entry

`reset()` calls `TimerShards.register(this)` on every wake cycle without
calling any cancel/remove on the prior heap entry. Two reasons this is
correct and one reason it is intentionally cheap-and-loose:

1. **SeqTxnTracker queue:** non-issue. `SeqTxnTracker.fireWaiters`
   dequeues the holder before calling `tryFire`. By the time the body
   wakes and `reset()` runs, the prior tracker holder is already gone.
   `registerWaiter` enqueues a fresh one. Queue size stays at 1 per
   waiter.

2. **Timer shard heap, `expire` wake path:** non-issue. `shard.take()`
   removed the entry from the heap before calling `expire()`. Next
   `reset()` registers a fresh entry. Heap size stays at 1.

3. **Timer shard heap, `tryFire` wake path:** the prior entry is still
   in the heap. `reset()` adds another. Heap can grow by 1 per
   `tryFire`-wake. In a burst-commit workload where the writer commits
   faster than `wakeIntervalMillis`, entries accumulate roughly
   proportionally to burst rate x wake interval.

Why this is fine:

- All entries for a given waiter share `TxnWaiter.registeredAtMillis`,
  which `reset()` mutates. `getDelay()` reads it live. The heap's
  notion of "this entry's deadline" therefore shifts forward every time
  `reset()` runs. **Stale entries do not fire at an old deadline.** They
  fire at the latest reset's deadline, alongside the freshly-registered
  entry, and the per-waiter CAS makes multi-fire harmless (first wins,
  rest are no-ops on the next `expire()`).

- Heap accumulation is self-limiting: as soon as the writer pauses for
  longer than `wakeIntervalMillis`, the shard thread starts draining
  entries one at a time, each pop firing the (already-PENDING-again)
  waiter and burning one no-op CAS until the heap is back to size 1.

The in-code comment in `reset()` says stale entries are "sorted by an
older deadline." That description is wrong about the mechanism --
`registeredAtMillis` is shared, so the entry's deadline shifts live --
but the conclusion (stale entries are harmless) holds.

Do not add a cancel-and-re-register cycle to `reset()` to "clean up"
the prior heap entry. The `DelayQueue` API has no O(log n) remove (only
O(n) linear scan), and the accumulation cost is already cheaper than
that scan in the burst case.

## `tryCancel()` in `WaitWalFunction.getBool`'s finally instead of `abortContinuation()`

The finally block calls `waiter.tryCancel()`, which CASes
`PENDING -> CANCELLED` but does not set `parkRefused`. The only path
that opens a window where `parkRefused` would be needed is:

1. The body is mid-`reset()`, between `CAS-to-PENDING` and
   `TimerShards.register`'s post-`shard.offer` `!running` check.
2. `SeqTxnTracker.fireWaiters` concurrently wins `tryFire` (body is now
   FIRED, scheduleResume issued).
3. `TimerShards.shutdown` concurrently flips `running` false.
4. The body's `register` then throws `CairoException.queryCancelled`
   on its post-check.
5. The finally's `tryCancel` CAS loses (state is FIRED).

Trigger: engine shutdown only. `TimerShards.register` has no other
throw path in current code. The peer that dequeued the phantom spins on
`Os.pause` until `cont.isDone()` flips true, which happens after the
body unwinds the five frames out of register / reset / getBool /
WorkerContinuation.run. Tens of microseconds, once, while the engine is
already shutting down. The pool's `lifecycle != RUNNING` transition
closes the peer from the other side shortly after.

Swapping `tryCancel` for `abortContinuation` would buy a single volatile
write of `parkRefused` to short-circuit that microsecond spin. Not worth
the cognitive load of explaining why a successful-cancel path uses the
abort vocabulary. Revisit if and only if a non-shutdown throw path
between `reset()` and `suspend()` is introduced.

## `SleepFunctionFactory` has no try/finally around `TimerCont.scheduleAfter`

Same race shape as the `WaitWalFunction` case above: if
`TimerShards.register` throws `queryCancelled` between
`shard.offer(t)` and the post-check, the caller in
`SleepFunctionFactory` unwinds without calling `t.abortContinuation()`.
The phantom (already-enqueued resume) gets dequeued by a peer, peer
spins on `IllegalStateException` until `cont.isDone()` flips.

Same trigger (engine shutdown only), same magnitude (microseconds, once).
A `try { ... } finally { if (!t.isDone()) t.abortContinuation(); }`
wrapper would close it, but the only event it defends against is
shutdown, where the cost is invisible against the pool tear-down.

If the cancel race shape changes -- e.g. `TimerShards.register` grows a
non-shutdown throw path, or `scheduleAfter` starts being called from
a non-cont-aware context -- revisit and add the try/finally.

## `WaitWalFunction` legacy polling fallback observes shutdown via the breaker

The pinned-carrier fallback loop in `WaitWalFunction.getBool`:

```
for (int i = 0; seqTxnTracker.getWriterTxn() < seqTxn; i++) {
    Os.sleep(1);
    executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
    ...
}
```

Does not check `cont.isShutdown()`. It does not need to. On engine
shutdown, the PG / HTTP server signal-close paths tear down client
connections; the next `statefulThrowExceptionIfTripped` probe (within
1ms) observes the broken FD and throws. The body unwinds, the worker
loopBody resumes, `WorkerPool.halt()` proceeds.

The breaker probe is the shutdown signal for the legacy loop. Do not
add a separate shutdown-flag check; it would duplicate the breaker's
job and create a second path to keep in sync.

## `getWaiterRegistrationCount()` atomic on `registerWaiter`

`SeqTxnTracker.registerWaiter()` does an `Unsafe.getAndAddLong` on
`waiterRegistrationCount`, which exists only for `@TestOnly`
synchronization in tests (tests poll the counter to detect that a
parked `wait_wal_table` body has reached the registration point, which
replaces fixed-duration `Os.sleep` synchronization that flakes on slow
CI).

`registerWaiter` is not on any data path. It runs once on the first
iteration of a user-issued `wait_wal_table` call and once per wake cycle
inside that call -- single-digit invocations per user query. The
uncontended atomic costs ~5-20 cycles on x86; surrounding work
(continuation alloc, yield syscall, scheduleResume) is several orders of
magnitude larger.

Do not move the increment behind a debug flag. The whole point is for
tests to observe the counter monotonically without flaky timing.

## `TimerShards.start()` is not currently thread-safe

`shutdown()` is `synchronized`; `start()` and `halt()` are not. The
`if (running) return;` check is a plain volatile read that races with
concurrent state transitions.

In current callers `start()` is invoked exactly once from the
`CairoEngine` constructor, before any other code can call
`shutdown()` or `halt()`. The race window cannot be reached.

If `TimerShards` is ever reused (e.g. an engine-restart scenario, or a
second `start()` after `halt()`), make `start()` `synchronized` (or
introduce a lifecycle CAS) and have it `join` any preceding threads
before launching new ones. As long as the contract stays "single-use,
single-threaded init," leave it alone -- the comment on the class needs
to make that contract explicit, though.

## Cleanup hook contract for closeable-valued `CarrierLocal`

`CarrierLocal.releaseRow(id)` drops the per-carrier map without
invoking `Misc.freeIfCloseable` on its values. This matches the legacy
`io.questdb.std.ThreadLocal` behavior on thread exit (which also did
not auto-close).

For closeable-valued `CarrierLocal`s -- the only current native-backed
case is `ColumnTypeConverter`'s `dstFixMemTL` / `dstVarMemTL` of
`MemoryCMARW` -- the worker pool's `assignThreadLocalCleaner` mechanism
runs an explicit free hook on worker exit. Every new closeable-valued
`CarrierLocal` MUST register a corresponding cleaner. The
`releaseRow` path is for the carrier-id row itself, not for the
values it indexed.

If you add a `CarrierLocal<T>` where `T` holds native memory or other
non-GC-managed resources, add an `assignThreadLocalCleaner` call in
every worker pool that runs code touching that local. There is no
catch-all; this is enforced by code review.
