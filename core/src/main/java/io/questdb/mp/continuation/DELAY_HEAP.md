# DelayHeap (replacement for `java.util.concurrent.DelayQueue`)

Why `TimerShards` uses `io.questdb.mp.continuation.DelayHeap` instead of
`java.util.concurrent.DelayQueue`. Read this if you are touching `DelayHeap`,
`TimerShards`, or any code that registers timer entries from inside a raw
`jdk.internal.vm.Continuation` body. Related reading: `CARRIER_LOCAL.md`
(sibling C2-hoist hazard).

## Problem

`TimerShards` is a per-shard `DelayQueue` of `DelayedFireable`s. The producers
call `TxnWaiter.reset` -> `TimerShards.register` -> `shard.offer(entry)` while
running inside the cont body of a SQL function (e.g., `wait_wal_table`). The
JDK's `DelayQueue.offer` internally does:

```java
public boolean offer(E e) {
    final ReentrantLock lock = this.lock;
    lock.lock();                       // owner = Thread.currentThread()
    try {
        q.offer(e);
        if (q.peek() == e) { leader = null; available.signal(); }
        return true;
    } finally {
        lock.unlock();                 // checks Thread.currentThread() == owner
    }
}
```

`ReentrantLock` tracks ownership via `Thread.currentThread()`, the
`_currentThread` HotSpot intrinsic - the exact intrinsic that C2 is free to
hoist via LICM (documented in `CARRIER_LOCAL.md`). Inside a raw
`jdk.internal.vm.Continuation` body, the carrier thread can disagree between
the `lock()` site and the `unlock()` site:

- C2 inlines `ReentrantLock.lock()` and `ReentrantLock.unlock()` into
  `DelayQueue.offer`, then reads `Thread.currentThread()` at each site
  independently. If the two reads see different carriers (because the JIT did
  not CSE them, or because something in between changed the carrier identity
  C2 saw), `tryRelease` fires.

The unlock fails with `IllegalMonitorStateException`. Because the throw
originates inside `finally`, the lock is **never released**. From that moment
on, every operation on that shard's `DelayQueue` blocks forever:

- The shard daemon's `shard.take()` blocks on `lock.lock()`.
- Every subsequent `register()` blocks on `lock.lock()`.
- `TimerShards.shutdown()` -> `shard.offer(PoisonSentinel.INSTANCE)` blocks
  on `lock.lock()`, so engine close hangs.

## Observed failure

Reproduced locally on `feat-productionise-wait-wal-table-func` running
`ServerMainWaitWalTableTest.testFuzzConcurrentWaitWalTable` against
JDK 25.0.2 on macOS / arm64. The producer-side throw inside the cont body:

```
java.lang.IllegalMonitorStateException
    at java.util.concurrent.locks.ReentrantLock$Sync.tryRelease(ReentrantLock.java:176)
    at java.util.concurrent.locks.AbstractQueuedSynchronizer.release(AbstractQueuedSynchronizer.java:1099)
    at java.util.concurrent.locks.ReentrantLock.unlock(ReentrantLock.java:495)
    at java.util.concurrent.DelayQueue.offer(DelayQueue.java:178)
    at io.questdb.mp.continuation.TimerShards.register(TimerShards.java:124)
    at io.questdb.mp.continuation.TxnWaiter.reset(TxnWaiter.java:171)
    at io.questdb.griffin.engine.functions.table.WaitWalFunction.getBool(WaitWalFunction.java:116)
    at io.questdb.cairo.sql.VirtualFunctionRecord.getBool(VirtualFunctionRecord.java:82)
    at io.questdb.cutlass.pgwire.PGPipelineEntry.outColTxtBool(PGPipelineEntry.java:2116)
    at io.questdb.cutlass.pgwire.PGPipelineEntry.outRecord(PGPipelineEntry.java:2610)
    at io.questdb.cutlass.pgwire.PGPipelineEntry.outCursor(PGPipelineEntry.java:2409)
    at io.questdb.cutlass.pgwire.PGPipelineEntry.outCursor(PGPipelineEntry.java:2386)
    at io.questdb.cutlass.pgwire.PGPipelineEntry.msgSync(PGPipelineEntry.java:811)
    at io.questdb.cutlass.pgwire.PGConnectionContext.syncPipeline(PGConnectionContext.java:1414)
    at io.questdb.cutlass.pgwire.PGConnectionContext.msgSync0(PGConnectionContext.java:1160)
    at io.questdb.cutlass.pgwire.PGConnectionContext.msgSync(PGConnectionContext.java:1156)
    at io.questdb.cutlass.pgwire.PGConnectionContext.parseMessage(PGConnectionContext.java:1266)
    at io.questdb.cutlass.pgwire.PGConnectionContext.handleClientOperation(PGConnectionContext.java:449)
    at io.questdb.cutlass.pgwire.PGServer$1.lambda$new$0(PGServer.java:102)
    at io.questdb.network.AbstractIODispatcher.processIOQueue(AbstractIODispatcher.java:222)
    at io.questdb.cutlass.pgwire.PGServer$1.run(PGServer.java:130)
    at io.questdb.mp.Worker.loopBody(Worker.java:289)
    at jdk.internal.vm.Continuation.run(Continuation.java:254)
    at io.questdb.mp.continuation.WorkerContinuation.run(WorkerContinuation.java:160)
    at io.questdb.mp.Worker.mountForeignCont(Worker.java:390)
    at io.questdb.mp.Worker.run(Worker.java:223)
```

The cascading hang at engine close, showing the shard's `ReentrantLock` is
permanently held by a ghost owner:

```
at java.base/jdk.internal.misc.Unsafe.park(Native Method)
at java.base/java.util.concurrent.locks.LockSupport.park(LockSupport.java:223)
at java.base/java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:790)
at java.base/java.util.concurrent.locks.AbstractQueuedSynchronizer.acquire(AbstractQueuedSynchronizer.java:1030)
at java.base/java.util.concurrent.locks.ReentrantLock$Sync.lock(ReentrantLock.java:154)
at java.base/java.util.concurrent.locks.ReentrantLock.lock(ReentrantLock.java:323)
at java.base/java.util.concurrent.DelayQueue.offer(DelayQueue.java:169)
at io.questdb/io.questdb.mp.continuation.TimerShards.shutdown(TimerShards.java:147)
at io.questdb/io.questdb.cairo.CairoEngine.signalClose(CairoEngine.java:1868)
at io.questdb/io.questdb.ServerMain.close(ServerMain.java:192)
```

The same failure also surfaced in CI as a 15-second timeout in
`testFuzzConcurrentWaitWalTable`'s `helpersDone.await` - the four parked
`wait_wal_table` continuations could not be fired because the shard daemon's
`shard.take()` and the test thread's `setSuspended -> fireWaiters` could no
longer touch the bricked `DelayQueue`. The waiters only unblocked when the
test's `@Test(timeout = 240_000)` killed the JVM.

## Why `ReentrantLock` is uniquely exposed

`ReentrantLock` is Java code. Its ownership check is a Java field, set and
read through `Thread.currentThread()` - a hoistable HotSpot intrinsic that
C2 can move out of loops via LICM. JDK's defense for its own continuation
client (`VirtualThread`) is the boot-classpath-only annotation
`@ChangesCurrentThread` on `VirtualThread.runContinuation`; user-space
continuation consumers cannot apply it. So `ReentrantLock` (and every
AQS-based primitive: `ReentrantReadWriteLock`, `StampedLock`, `Semaphore`,
`CountDownLatch`-on-wait, etc.) is unsafe to call from inside a raw
`Continuation` body.

`synchronized`, by contrast, is implemented in the JVM. `monitorenter` /
`monitorexit` are bytecodes; their ownership check uses the
`JavaThread*` pointer from HotSpot's runtime, not the Java
`Thread.currentThread()` call, and `monitorenter` / `monitorexit` act as
memory barriers that constrain C2's freedom to hoist surrounding identity
reads. As long as the body inside the synchronized region never calls
`Continuation.yield(SCOPE)`, the JVM cannot migrate the carrier between
`monitorenter` and `monitorexit`, and the check at exit cannot disagree
with the check at entry.

`DelayHeap` exploits this by using `synchronized` on each public method and
`Object.wait` / `Object.notify` for the consumer wakeup. No `ReentrantLock`,
no `Condition`, no `AbstractQueuedSynchronizer` - none of which would be
safe in a cont-body call path.

## Invariant

The single rule callers must keep:

> **Never call `Continuation.yield(SCOPE)` from within any method of
> `DelayHeap`.**

This is trivially true today: the bodies of `offer`, `take`, `size`,
`clear`, and `toArray` only perform heap operations on a private
`PriorityQueue` plus, at most, one `notify` or `wait` call - none of which
yield a raw continuation. If a future change adds a callback or invokes
something that may yield (a SQL function, a user lambda) from inside these
methods, the IMSE failure mode returns.

## Tests

`io.questdb.test.mp.DelayHeapTest` covers, in addition to plain
heap-ordering and blocking semantics:

- `testConcurrentProducersOrdering` - many producers stamp distinct
  deadlines under contention; the single consumer must see them pop in
  deadline order.
- `testConcurrentProducersSingleConsumer` - producer/consumer stress with
  a mix of already-expired and short-future entries; every entry must be
  consumed exactly once.
- `testTakeEarlierOfferWakesConsumer` - covers the `notify` invariant
  (consumer parked on a far-future head must wake when a sooner entry is
  inserted).

End-to-end coverage of the original failure case is in
`io.questdb.test.ServerMainWaitWalTableTest.testFuzzConcurrentWaitWalTable`.

## Files

- `core/src/main/java/io/questdb/mp/continuation/DelayHeap.java` - the heap.
- `core/src/main/java/io/questdb/mp/continuation/TimerShards.java` - the
  only consumer; one `DelayHeap<DelayedFireable>` per shard.
- `core/src/test/java/io/questdb/test/mp/DelayHeapTest.java` - tests.
- `core/src/main/java/io/questdb/mp/continuation/CARRIER_LOCAL.md` -
  related C2-hoist hazard for `ThreadLocal`.
