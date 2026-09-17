/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.wal.seq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableWriterPressureControlImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.continuation.FiberWaitCoordinator;
import io.questdb.mp.continuation.FiberWalWaitRegistration;
import io.questdb.mp.continuation.SourceRegistrationResult;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SeqTxnTrackerTest {
    private static final int F_APPLY = SeqTxnTracker.SUSPEND_FLAG_APPLY;
    private static final int F_APPLY_WRITE = SeqTxnTracker.SUSPEND_FLAG_APPLY | SeqTxnTracker.SUSPEND_FLAG_WRITE;
    // Every hard-suspend flavour + release, for looping over combos.
    private static final int[] FLAVOURS = {F_APPLY, F_APPLY_WRITE};
    private static final Log LOG = LogFactory.getLog(SeqTxnTrackerTest.class);
    private static final int P_DDL = SeqTxnTracker.SUSPEND_PRIORITY_DDL;
    private static final int P_REC = SeqTxnTracker.SUSPEND_PRIORITY_RECONCILE;

    @Test
    public void testSuspendAcquireReleaseOnUnlocked() {
        // Acquire then release, for each priority x flavour, over an unlocked table.
        for (int prio : new int[]{P_DDL, P_REC}) {
            SeqTxnTracker t = createSeqTracker();
            // release on an unlocked table is an allowed no-op
            assertTrue(t.trySetSuspend(prio, 0));
            assertSuspend(t, false, false);
            for (int flags : FLAVOURS) {
                assertTrue(t.trySetSuspend(prio, flags));
                assertSuspend(t, true, flags == F_APPLY_WRITE);
                assertTrue(t.trySetSuspend(prio, 0));
                assertSuspend(t, false, false);
            }
        }
    }

    @Test
    public void testSuspendSamePriorityUpgradeDowngrade() {
        // A holder may freely change its own flavour (apply <-> apply+write) and release.
        for (int prio : new int[]{P_DDL, P_REC}) {
            SeqTxnTracker t = createSeqTracker();
            assertTrue(t.trySetSuspend(prio, F_APPLY));
            assertSuspend(t, true, false);
            assertTrue(t.trySetSuspend(prio, F_APPLY_WRITE)); // upgrade
            assertSuspend(t, true, true);
            assertTrue(t.trySetSuspend(prio, F_APPLY)); // downgrade
            assertSuspend(t, true, false);
            assertTrue(t.trySetSuspend(prio, F_APPLY)); // idempotent re-acquire
            assertSuspend(t, true, false);
            assertTrue(t.trySetSuspend(prio, 0));
            assertSuspend(t, false, false);
        }
    }

    @Test
    public void testSuspendLowerCannotChangeHigher() {
        // Race A: while RECONCILE holds the lock, an operator (DDL priority) SUSPEND / RESUME /
        // change is refused and leaves the state untouched.
        for (int recFlags : FLAVOURS) {
            SeqTxnTracker t = createSeqTracker();
            assertTrue(t.trySetSuspend(P_REC, recFlags));
            boolean write = recFlags == F_APPLY_WRITE;
            assertSuspend(t, true, write);
            assertFalse(t.trySetSuspend(P_DDL, F_APPLY));       // SUSPEND WAL APPLY
            assertFalse(t.trySetSuspend(P_DDL, F_APPLY_WRITE)); // SUSPEND WAL APPLY AND WRITE
            assertFalse(t.trySetSuspend(P_DDL, 0));             // RESUME WAL
            assertSuspend(t, true, write);                      // unchanged
            assertTrue(t.trySetSuspend(P_REC, 0));              // reconcile releases its own
            assertSuspend(t, false, false);
        }
    }

    @Test
    public void testSuspendHigherPreemptsAndRestoresLower() {
        // Race B: RECONCILE preempts an operator suspend of ANY flavour and, on release, RESTORES
        // exactly that operator suspend (not a full resume). All 4 (ddl x rec) flavour combos.
        for (int ddlFlags : FLAVOURS) {
            for (int recFlags : FLAVOURS) {
                SeqTxnTracker t = createSeqTracker();
                assertTrue(t.trySetSuspend(P_DDL, ddlFlags));
                assertSuspend(t, true, ddlFlags == F_APPLY_WRITE);
                // reconcile preempts (takes over regardless of the operator's flavour)
                assertTrue(t.trySetSuspend(P_REC, recFlags));
                assertSuspend(t, true, recFlags == F_APPLY_WRITE);
                // reconcile releases -> the operator's original flavour is restored exactly
                assertTrue(t.trySetSuspend(P_REC, 0));
                assertSuspend(t, true, ddlFlags == F_APPLY_WRITE);
                // and the operator can now resume its own (restored) lock
                assertTrue(t.trySetSuspend(P_DDL, 0));
                assertSuspend(t, false, false);
            }
        }
    }

    @Test
    public void testSuspendReconcileOwnModifyKeepsPreempted() {
        // A reconcile that preempts an operator suspend and then changes its OWN flavour must keep
        // the preempted operator lock, and still restore it on release.
        SeqTxnTracker t = createSeqTracker();
        assertTrue(t.trySetSuspend(P_DDL, F_APPLY));       // operator apply-suspend
        assertTrue(t.trySetSuspend(P_REC, F_APPLY));       // reconcile preempts (saves DDL apply)
        assertSuspend(t, true, false);
        assertTrue(t.trySetSuspend(P_REC, F_APPLY_WRITE)); // reconcile upgrades its own lock
        assertSuspend(t, true, true);
        assertFalse(t.trySetSuspend(P_DDL, 0));            // operator still cannot interfere
        assertTrue(t.trySetSuspend(P_REC, 0));             // reconcile releases -> restore DDL apply
        assertSuspend(t, true, false);
        assertTrue(t.trySetSuspend(P_DDL, 0));
        assertSuspend(t, false, false);
    }

    @Test
    public void testSuspendReconcileCannotAcquireOverAnotherReconcile() {
        // RECONCILE TABLE must be mutually exclusive with itself: a second reconcile CANNOT take the
        // lock while the first still holds it. trySetSuspend deliberately lets a same-priority holder
        // modify its own lock (which is how an operator SUSPEND WAL -> RESUME WAL works, since DDL
        // carries no owner identity), so reconcile acquires via tryAcquireSuspend instead.
        for (int firstFlags : FLAVOURS) {
            SeqTxnTracker t = createSeqTracker();
            assertTrue(t.tryAcquireSuspend(P_REC, firstFlags));
            assertSuspend(t, true, firstFlags == F_APPLY_WRITE);
            for (int secondFlags : FLAVOURS) {
                assertFalse(t.tryAcquireSuspend(P_REC, secondFlags));
                assertSuspend(t, true, firstFlags == F_APPLY_WRITE); // first holder's lock untouched
            }
            // The holder itself may still modify and release its own lock through trySetSuspend.
            assertTrue(t.trySetSuspend(P_REC, 0));
            assertSuspend(t, false, false);
            // Released -> the next reconcile can acquire.
            assertTrue(t.tryAcquireSuspend(P_REC, firstFlags));
            assertSuspend(t, true, firstFlags == F_APPLY_WRITE);
        }
    }

    @Test
    public void testSuspendReconcileAcquirePreemptsAndRestoresDdl() {
        // tryAcquireSuspend keeps trySetSuspend's preempt-and-restore behaviour against a strictly
        // lower priority: reconcile takes an operator-suspended table over, and releasing restores
        // the operator's exact flavour rather than resuming the table.
        for (int ddlFlags : FLAVOURS) {
            for (int recFlags : FLAVOURS) {
                SeqTxnTracker t = createSeqTracker();
                assertTrue(t.trySetSuspend(P_DDL, ddlFlags));
                assertTrue(t.tryAcquireSuspend(P_REC, recFlags));
                assertSuspend(t, true, recFlags == F_APPLY_WRITE);
                assertTrue(t.trySetSuspend(P_REC, 0));
                assertSuspend(t, true, ddlFlags == F_APPLY_WRITE); // operator's suspend restored
                assertTrue(t.trySetSuspend(P_DDL, 0));
                assertSuspend(t, false, false);
            }
        }
    }

    @Test
    public void testSuspendReconcileOverUnlockedClearsFully() {
        // RECONCILE over an unlocked table has nothing to restore -> release clears fully.
        for (int recFlags : FLAVOURS) {
            SeqTxnTracker t = createSeqTracker();
            assertTrue(t.trySetSuspend(P_REC, recFlags));
            assertSuspend(t, true, recFlags == F_APPLY_WRITE);
            assertTrue(t.trySetSuspend(P_REC, 0));
            assertSuspend(t, false, false);
        }
    }

    @Test
    public void testSuspendReleaseDoesNotClearUnownedLowerPriorityLock() {
        // A release only ever clears the CALLER'S OWN lock. A reconcile-priority release while an
        // operator SUSPEND WAL is the active lock (nothing was preempted, so reconcile owns nothing)
        // must leave the operator's suspend alone. Reachable in production: clearReconcileQuiesce
        // fires a RECONCILE-priority release on every apply-failure path, and the tracker is
        // in-memory, so after a restart the reconcile lock is gone while an operator can have
        // re-suspended the table. Before the fix this silently RESUMED the operator's table.
        for (int ddlFlags : FLAVOURS) {
            SeqTxnTracker t = createSeqTracker();
            assertTrue(t.trySetSuspend(P_DDL, ddlFlags));
            assertSuspend(t, true, ddlFlags == F_APPLY_WRITE);
            assertFalse(t.trySetSuspend(P_REC, 0)); // not ours -> refused, state untouched
            assertSuspend(t, true, ddlFlags == F_APPLY_WRITE);
            // The operator can still release its own lock afterwards.
            assertTrue(t.trySetSuspend(P_DDL, 0));
            assertSuspend(t, false, false);
        }
    }

    @Test
    public void testConcurrentInitTxns() throws Exception {
        LOG.info().$("testConcurrentInitTxns").$();
        TestUtils.assertMemoryLeak(() -> {
            final int threads = 4;

            final SeqTxnTracker tracker = createSeqTracker();
            assertFalse(tracker.isInitialised());

            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(threads);
            final AtomicInteger successes = new AtomicInteger();
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        startBarrier.await();
                        if (tracker.initTxns(1, 2 + finalI, false)) {
                            successes.incrementAndGet();
                        }
                        doneLatch.countDown();
                    } catch (Throwable th) {
                        th.printStackTrace(System.out);
                        errors.incrementAndGet();
                    }
                }).start();
            }

            doneLatch.await();

            assertEquals(0, errors.get());
            assertEquals(threads, successes.get());

            assertEquals(1, tracker.getWriterTxn());
            assertEquals(1 + threads, tracker.getSeqTxn());
            assertFalse(tracker.isSuspended());
        });
    }

    @Test
    public void testConcurrentNotifyOnCheck() throws Exception {
        LOG.info().$("testConcurrentNotifyOnCheck").$();
        TestUtils.assertMemoryLeak(() -> {
            final int threads = 4;

            final SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 1, false);
            assertTrue(tracker.isInitialised());

            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(threads);
            final AtomicInteger successes = new AtomicInteger();
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        startBarrier.await();
                        if (tracker.notifyOnCheck(2 + finalI)) {
                            successes.incrementAndGet();
                        }
                        doneLatch.countDown();
                    } catch (Throwable th) {
                        th.printStackTrace(System.out);
                        errors.incrementAndGet();
                    }
                }).start();
            }

            doneLatch.await();

            assertEquals(0, errors.get());
            assertEquals(threads, successes.get());

            assertEquals(1, tracker.getWriterTxn());
            assertEquals(1 + threads, tracker.getSeqTxn());
            assertFalse(tracker.isSuspended());
        });
    }

    @Test
    public void testConcurrentNotifyOnCommit() throws Exception {
        LOG.info().$("testConcurrentNotifyOnCommit").$();
        TestUtils.assertMemoryLeak(() -> {
            final int threads = 4;

            final SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 1, false);
            assertTrue(tracker.isInitialised());

            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch doneLatch = new SOCountDownLatch(threads);
            final AtomicInteger successes = new AtomicInteger();
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threads; i++) {
                int finalI = i;
                new Thread(() -> {
                    try {
                        startBarrier.await();
                        if (tracker.notifyOnCommit(2 + finalI)) {
                            successes.incrementAndGet();
                        }
                        doneLatch.countDown();
                    } catch (Throwable th) {
                        th.printStackTrace(System.out);
                        errors.incrementAndGet();
                    }
                }).start();
            }

            doneLatch.await();

            assertEquals(0, errors.get());
            assertEquals(1, successes.get());

            assertEquals(1, tracker.getWriterTxn());
            assertEquals(1 + threads, tracker.getSeqTxn());
            assertFalse(tracker.isSuspended());
        });
    }

    @Test
    public void testMemoryPressureLevels() {
        final var pressureControl = createPressureControl();
        assertEquals("initial memory pressure level", 0, pressureControl.getMemoryPressureLevel());
        pressureControl.updateInflightPartitions(2);
        pressureControl.onOutOfMemory();
        assertEquals("memory pressure level after one OOM", 1, pressureControl.getMemoryPressureLevel());
        pressureControl.onOutOfMemory();
        assertEquals("memory pressure level after two OOMs", 2, pressureControl.getMemoryPressureLevel());
    }

    @Test
    public void testMemoryPressureRegulationEasesOffOnSuccess() {
        final var pressureControl = createPressureControl();
        int expectedParallelism = 16;
        pressureControl.updateInflightPartitions(expectedParallelism);
        pressureControl.onOutOfMemory();
        expectedParallelism /= 4;
        assertEquals(expectedParallelism, pressureControl.getMemoryPressureRegulationValue());
        expectedParallelism *= 4;
        int maxSuccessToEaseOff = 100;
        retryBlock:
        {
            for (int i = 0; i < maxSuccessToEaseOff; i++) {
                pressureControl.onEnoughMemory();
                if (pressureControl.getMemoryPressureRegulationValue() == expectedParallelism) {
                    break retryBlock;
                }
            }
            fail("Regulation did not ease off even after " + maxSuccessToEaseOff + " successes");
        }
    }

    @Test
    public void testMemoryPressureRegulationGivesUpEventually() {
        final var pressureControl = createPressureControl();
        int maxFailuresToGiveUp = 10;

        for (int i = 0; i < maxFailuresToGiveUp; i++) {
            pressureControl.onOutOfMemory();
            if (!pressureControl.isReadyToProcess()) {
                return;
            }
        }
        fail("Did not signal to give up even after " + maxFailuresToGiveUp + " failures");
    }

    @Test
    public void testMemoryPressureRegulationIntroducesBackoff() {
        var fixedClock = new MillisecondClock() {
            private long time = 0;

            public void advanceTimeBy(long millis) {
                time += millis;
            }

            @Override
            public long getTicks() {
                return time;
            }
        };

        CairoConfiguration configuration = getConfiguration(fixedClock);

        final var pressureControl = new TableWriterPressureControlImpl(configuration);

        pressureControl.onOutOfMemory();
        assertFalse(pressureControl.isReadyToProcess());

        fixedClock.advanceTimeBy(4000);
        assertTrue(pressureControl.isReadyToProcess());
    }

    @Test
    public void testMemoryPressureRegulationReducesParallelism() {
        final var tracker = createPressureControl();
        int expectedParallelism = 16;
        tracker.updateInflightPartitions(expectedParallelism);
        while (true) {
            tracker.onOutOfMemory();
            expectedParallelism /= 4;
            if (expectedParallelism < 1) {
                break;
            }
            tracker.updateInflightPartitions(expectedParallelism);
            assertEquals(expectedParallelism, tracker.getMemoryPressureRegulationValue());
        }
    }

    @Test
    public void testWaiterFiberFiresImmediatelyIfAlreadyMet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(10, 10, false);
            FiberTarget target = new FiberTarget();
            FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            long token = coordinator.beginBuild(1);
            FiberWalWaitRegistration registration = coordinator.acquireWal(token, 5);

            assertSame(SourceRegistrationResult.ACCEPTED, tracker.registerWaiter(registration));
            assertTrue(coordinator.seal(token));

            assertTrue(coordinator.isFired(token));
            assertEquals(FiberWaitCoordinator.REASON_WAL, target.reason);
        });
    }

    @Test
    public void testWaiterFiberUnlinksOnCancel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            FiberWaitCoordinator coordinator = new FiberWaitCoordinator(new FiberTarget());
            long token = coordinator.beginBuild(1);
            FiberWalWaitRegistration registration = coordinator.acquireWal(token, 10);

            assertSame(SourceRegistrationResult.ACCEPTED, tracker.registerWaiter(registration));
            assertTrue(registration.cancel());
            assertTrue(coordinator.abort(token));
            tracker.updateWriterTxns(10, 10);
            assertEquals(FiberWaitCoordinator.REASON_NONE, coordinator.consume(token));
        });
    }

    @Test
    public void testWaiterFiresOnDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            FiberTarget target = new FiberTarget();
            FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            long token = coordinator.beginBuild(1);
            FiberWalWaitRegistration registration = coordinator.acquireWal(token, 100);
            assertSame(SourceRegistrationResult.ACCEPTED, tracker.registerWaiter(registration));
            assertTrue(coordinator.seal(token));
            assertFalse(coordinator.isFired(token));

            tracker.notifyOnDrop();

            assertTrue(coordinator.isFired(token));
            assertEquals(FiberWaitCoordinator.REASON_WAL, target.reason);
        });
    }

    @Test
    public void testWaiterFiresOnSuspend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            FiberTarget target = new FiberTarget();
            FiberWaitCoordinator coordinator = new FiberWaitCoordinator(target);
            long token = coordinator.beginBuild(1);
            FiberWalWaitRegistration registration = coordinator.acquireWal(token, 100);
            assertSame(SourceRegistrationResult.ACCEPTED, tracker.registerWaiter(registration));
            assertTrue(coordinator.seal(token));
            assertFalse(coordinator.isFired(token));

            tracker.setSuspended(ErrorTag.NONE, "test");

            assertTrue(coordinator.isFired(token));
            assertEquals(FiberWaitCoordinator.REASON_WAL, target.reason);
        });
    }

    @Test
    public void testWaiterFiresOnWriterTxnAdvance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            FiberTarget target1 = new FiberTarget();
            FiberWaitCoordinator coordinator1 = new FiberWaitCoordinator(target1);
            long token1 = coordinator1.beginBuild(1);
            FiberWalWaitRegistration registration1 = coordinator1.acquireWal(token1, 3);
            assertSame(SourceRegistrationResult.ACCEPTED, tracker.registerWaiter(registration1));
            assertTrue(coordinator1.seal(token1));

            FiberTarget target2 = new FiberTarget();
            FiberWaitCoordinator coordinator2 = new FiberWaitCoordinator(target2);
            long token2 = coordinator2.beginBuild(1);
            FiberWalWaitRegistration registration2 = coordinator2.acquireWal(token2, 7);
            assertSame(SourceRegistrationResult.ACCEPTED, tracker.registerWaiter(registration2));
            assertTrue(coordinator2.seal(token2));

            assertFalse(coordinator1.isFired(token1));
            assertFalse(coordinator2.isFired(token2));

            tracker.updateWriterTxns(3, 3);
            assertTrue(coordinator1.isFired(token1));
            assertFalse(coordinator2.isFired(token2));

            tracker.updateWriterTxns(7, 7);
            assertTrue(coordinator2.isFired(token2));
            assertEquals(FiberWaitCoordinator.REASON_WAL, target1.reason);
            assertEquals(FiberWaitCoordinator.REASON_WAL, target2.reason);
        });
    }

    private static void assertSuspend(SeqTxnTracker t, boolean applySuspended, boolean writeSuspended) {
        assertEquals("apply-suspend", applySuspended, t.isHardSuspended());
        assertEquals("write-suspend", writeSuspended, t.isWriteSuspended());
    }

    private static final class FiberTarget implements FiberWaitCoordinator.Target {
        private int reason;

        @Override
        public void abortWait(long token) {
        }

        @Override
        public boolean fireWait(long token, int reason) {
            this.reason = reason;
            return true;
        }
    }

    @NotNull
    private static TableWriterPressureControlImpl createPressureControl() {
        CairoConfiguration configuration = getConfiguration(MillisecondClockImpl.INSTANCE);
        return new TableWriterPressureControlImpl(configuration);
    }

    @NotNull
    private static SeqTxnTracker createSeqTracker() {
        return new SeqTxnTracker(getConfiguration(MillisecondClockImpl.INSTANCE));
    }

    @NotNull
    private static CairoConfiguration getConfiguration(MillisecondClock instance) {
        return new DefaultCairoConfiguration(null) {
            @Override
            public @NotNull MillisecondClock getMillisecondClock() {
                return instance;
            }
        };
    }
}
