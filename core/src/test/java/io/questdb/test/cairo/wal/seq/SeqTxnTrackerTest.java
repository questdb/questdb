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
import io.questdb.cairo.wal.seq.TableWriterPressureControlImpl;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TxnWaiter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ContinuationResumeJob;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SqlContinuation;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SeqTxnTrackerTest {
    private static final Log LOG = LogFactory.getLog(SeqTxnTrackerTest.class);

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
    public void testWaiterCancelledIsSkippedByFire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TxnWaiter w = new TxnWaiter(10, dummyContinuation(), resumeJob);
            tracker.registerWaiter(w);
            assertTrue(w.tryCancel());
            assertTrue(w.isCancelled());
            // Advancing past target must not fire a cancelled waiter.
            tracker.updateWriterTxns(10, 10);
            assertTrue(w.isCancelled());
            assertFalse(w.isFired());
        });
    }

    @Test
    public void testWaiterFiresImmediatelyIfAlreadyMet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(10, 10, false);
            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TxnWaiter w = new TxnWaiter(5, dummyContinuation(), resumeJob);
            tracker.registerWaiter(w);
            assertTrue(w.isFired());
            assertFalse(w.isCancelled());
        });
    }

    @Test
    public void testWaiterFiresOnDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TxnWaiter w = new TxnWaiter(100, dummyContinuation(), resumeJob);
            tracker.registerWaiter(w);
            assertFalse(w.isFired());
            tracker.notifyOnDrop();
            assertTrue(w.isFired());
        });
    }

    @Test
    public void testWaiterFiresOnSuspend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TxnWaiter w = new TxnWaiter(100, dummyContinuation(), resumeJob);
            tracker.registerWaiter(w);
            assertFalse(w.isFired());
            tracker.setSuspended(ErrorTag.NONE, "test");
            assertTrue(w.isFired());
        });
    }

    @Test
    public void testWaiterFiresOnWriterTxnAdvance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SeqTxnTracker tracker = createSeqTracker();
            tracker.initTxns(1, 5, false);
            ContinuationResumeJob resumeJob = new ContinuationResumeJob();
            TxnWaiter w1 = new TxnWaiter(3, dummyContinuation(), resumeJob);
            TxnWaiter w2 = new TxnWaiter(7, dummyContinuation(), resumeJob);
            tracker.registerWaiter(w1);
            tracker.registerWaiter(w2);
            // w1.target(3) is already met by initTxns writerTxn=1... wait, initTxns sets
            // writerTxn=1, so w1.target(3) is NOT met yet. w2.target(7) also not met.
            assertFalse(w1.isFired());
            assertFalse(w2.isFired());

            tracker.updateWriterTxns(3, 3);
            // w1 fires (target 3 met), w2 stays (target 7 not met yet).
            assertTrue(w1.isFired());
            assertFalse(w2.isFired());

            tracker.updateWriterTxns(7, 7);
            assertTrue(w2.isFired());
        });
    }

    private static SqlContinuation dummyContinuation() {
        // A continuation whose body never runs in these tests; we only need a reference
        // that ContinuationResumeJob.enqueue() can stash. Tests verify the waiter state,
        // not the resume side.
        return new SqlContinuation(() -> {
        });
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
