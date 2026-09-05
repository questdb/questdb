# Raw Continuation Carrier-Monitor Issue

## Summary

While introducing reusable raw Continuation, I found that mounting a Continuation from a carrier that already holds
an intrinsic monitor can permanently strand that monitor.

## Reproduction path

The failure requires this sequence:

1. A carrier enters a `synchronized` scope.
2. The carrier directly mounts a raw Continuation.
3. The Continuation yields.
4. Control returns to the carrier while the outer `synchronized` scope remains
   active.
5. The carrier no longer owns the monitor.
6. Exiting the `synchronized` scope does not make the monitor available.
7. Another thread attempting to acquire the monitor remains blocked
   indefinitely.

Immediately after the yield, `Thread.holdsLock()` changes from `true` to
`false`.

confirmed the behavior with:

- OpenJDK 25.0.2 using lightweight locking;
- OpenJDK 28 EA using its default locking implementation;
- OpenJDK 25 legacy locking as a control, where the problem does not occur.

## Cause

JEP 491 allows monitor ownership to follow an unmounted Continuation. When a
Continuation yields, HotSpot transfers the carrier's complete lightweight lock
stack into the Continuation's stack chunk.

This mechanism assumes that the carrier holds no intrinsic monitor before
mounting the Continuation. The JEP 491 implementation review states:

> "we currently assume carriers don't hold monitors while mounting virtual
> threads."

See [OpenJDK PR #21565](https://github.com/openjdk/jdk/pull/21565).

A raw Continuation mounted inside an outer `synchronized` scope violates this
assumption. HotSpot cannot distinguish the carrier's pre-existing monitor from
monitors acquired inside the Continuation, so it transfers both.

## Required avoidance

We must enforce this invariant:

> A raw Continuation may only mount from a scheduler-controlled clean carrier
> boundary with no intrinsic monitor held across the mount.

Consequently:

- The plain worker loop should remain the normal Continuation mount boundary.
- Code running inside lifecycle locks, callbacks, or `synchronized` scopes must
  not mount a Continuation directly.
- Such callers should reserve and enqueue the fiber, leave the locking scope,
  and let a worker mount it later.
- `!Fiber.isMounted()` alone does not prove that the carrier is clean.
- Replacing an individual `synchronized` block with another lock type does not
  provide a general solution.

Java provides no general API for checking whether the current carrier holds any
intrinsic monitor, so QuestDB must enforce this structurally through the
scheduler design.

## Minimal reproducer

```java
import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

public final class ContinuationCarrierLockRepro {
    private static final Object LOCK = new Object();
    private static final ContinuationScope SCOPE = new ContinuationScope("repro");

    public static void main(String[] args) throws Exception {
        final Continuation continuation = new Continuation(SCOPE, () -> Continuation.yield(SCOPE));

        synchronized (LOCK) {
            System.out.println("before yield: " + Thread.holdsLock(LOCK));
            continuation.run();
            System.out.println("after yield:  " + Thread.holdsLock(LOCK));
        }

        final Thread contender = Thread.ofPlatform().start(() -> {
            synchronized (LOCK) {
            }
        });
        contender.join(500);
        final boolean isBlocked = contender.isAlive();
        System.out.println("contender blocked after lexical exit: " + isBlocked);
        Runtime.getRuntime().halt(isBlocked ? 2 : 0);
    }
}
```

Compile with the JDK under test:

```bash
<JDK_HOME>/bin/javac \
  --add-exports java.base/jdk.internal.vm=ALL-UNNAMED \
  ContinuationCarrierLockRepro.java
```

Reproduce with OpenJDK 25 lightweight locking:

```bash
<JDK_HOME>/bin/java \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:LockingMode=2 \
  --add-exports java.base/jdk.internal.vm=ALL-UNNAMED \
  ContinuationCarrierLockRepro
```

Run the OpenJDK 25 legacy-locking control by changing `LockingMode` to `1`.
OpenJDK 28 no longer accepts the option; run the class with its default locking
implementation instead.

## Status

We reproduced the issue with official OpenJDK builds and prepared an upstream report.
