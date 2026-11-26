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

import io.questdb.cairo.ErrorTag;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The suspended_tables gauge counts the tables that are suspended right now: a call that leaves the suspension
 * state as it was, or that reaches a dropped table, does not move it.
 */
public class SeqTxnTrackerSuspendedGaugeTest extends AbstractCairoTest {
    private static final String SUSPENDED_TABLES = "questdb_suspended_tables";

    @Test
    public void testApplyProgressUnsuspendsAndUncounts() throws Exception {
        final AtomicBoolean isFailing = new AtomicBoolean();
        assertMemoryLeak(failingColumnOpens(isFailing, new AtomicInteger()), () -> {
            final TableToken token = createTable();
            isFailing.set(true);
            execute("update t set x = 11");
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            assertSuspendedTables(1);

            // A notification queued before the suspension applies the table once the cause is gone.
            isFailing.set(false);
            engine.notifyWalTxnCommitted(token);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertSuspendedTables(0);
        });
    }

    @Test
    public void testApplySuspendsTwiceAfterStaleNotificationCountsOnce() throws Exception {
        final AtomicBoolean isFailing = new AtomicBoolean();
        final AtomicInteger faults = new AtomicInteger();
        assertMemoryLeak(failingColumnOpens(isFailing, faults), () -> {
            final TableToken token = createTable();
            isFailing.set(true);
            execute("update t set x = 11");
            // The commit queued one notification; a check pass before the apply queues a second one.
            try (CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine)) {
                checkJob.run();
            }
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertTrue("both notifications must reach the apply job, faults=" + faults.get(), faults.get() > 1);
            assertSuspendedTables(1);

            engine.notifyWalTxnCommitted(token);
            drainWalQueue();
            assertSuspendedTables(1);

            isFailing.set(false);
            execute("alter table t resume wal");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertSuspendedTables(0);
        });
    }

    @Test
    public void testConcurrentTransitionsKeepGaugeExact() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int rounds = 20;
            final int iterations = 500 + rnd.nextInt(1000);
            for (int round = 0; round < rounds; round++) {
                final SeqTxnTracker tracker = new SeqTxnTracker(configuration);
                if (rnd.nextBoolean()) {
                    tracker.initTxns(0, 0, false);
                }
                final boolean isDropping = rnd.nextBoolean();
                final int dropAt = rnd.nextInt(iterations);
                final long before = suspendedTables();
                final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
                final ObjList<Thread> threads = new ObjList<>();
                final int threadCount = isDropping ? 5 : 4;
                final CyclicBarrier barrier = new CyclicBarrier(threadCount);
                for (int t = 0; t < threadCount; t++) {
                    final int role = t;
                    threads.add(new Thread(() -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < iterations; i++) {
                                switch (role) {
                                    case 0, 1 -> tracker.setSuspended(ErrorTag.NONE, "suspended by " + role);
                                    case 2 -> tracker.setUnsuspended();
                                    case 3 -> tracker.updateWriterTxns(i + 1, i + 1);
                                    default -> {
                                        if (i == dropAt) {
                                            tracker.notifyOnDrop();
                                        }
                                    }
                                }
                                Os.pause();
                            }
                        } catch (Throwable th) {
                            failures.add(th);
                        }
                    }));
                    threads.getLast().start();
                }
                for (int t = 0; t < threadCount; t++) {
                    threads.getQuick(t).join();
                }
                if (!failures.isEmpty()) {
                    throw new AssertionError("a tracker thread failed", failures.peek());
                }
                Assert.assertEquals(
                        "round " + round + ", dropped=" + tracker.isDropped() + ", suspended=" + tracker.isSuspended(),
                        !tracker.isDropped() && tracker.isSuspended() ? 1 : 0,
                        suspendedTables() - before
                );
            }
        });
    }

    @Test
    public void testDropSuspendedTableUncounts() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("alter table t suspend wal");
            assertSuspendedTables(1);
            execute("drop table t");
            drainWalQueue();
            assertSuspendedTables(0);
        });
    }

    @Test
    public void testResumeOfRunningTableDoesNotUncount() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("alter table t resume wal");
            assertSuspendedTables(0);
            execute("alter table t resume wal from txn 2");
            assertSuspendedTables(0);
            execute("alter table t suspend wal");
            assertSuspendedTables(1);
            execute("alter table t resume wal");
            assertSuspendedTables(0);
        });
    }

    @Test
    public void testSuspendWalTwiceCountsOnce() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("alter table t suspend wal with 'DISK FULL', 'first'");
            assertSuspendedTables(1);
            execute("alter table t suspend wal with 'TOO MANY OPEN FILES', 'second'");
            assertSuspendedTables(1);
            // A second SUSPEND WAL still replaces the reason.
            assertQuery("select suspended, errorTag, errorMessage from wal_tables() where name = 't'")
                    .noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary()
                    .returns("suspended\terrorTag\terrorMessage\ntrue\tTOO MANY OPEN FILES\tsecond\n");
            execute("alter table t resume wal");
            assertSuspendedTables(0);
        });
    }

    @Test
    public void testTrackerDropUncountsAndIgnoresLaterTransitions() throws Exception {
        assertMemoryLeak(() -> {
            final SeqTxnTracker tracker = new SeqTxnTracker(configuration);
            final long before = suspendedTables();
            tracker.setSuspended(ErrorTag.NONE, "before the drop");
            Assert.assertEquals(1, suspendedTables() - before);
            tracker.notifyOnDrop();
            Assert.assertEquals(0, suspendedTables() - before);
            tracker.setSuspended(ErrorTag.NONE, "after the drop");
            tracker.setUnsuspended();
            tracker.setSuspended(ErrorTag.NONE, "after the drop");
            Assert.assertEquals(0, suspendedTables() - before);
        });
    }

    private static void assertSuspendedTables(long expected) {
        Assert.assertEquals(SUSPENDED_TABLES, expected, suspendedTables());
    }

    private static TableToken createTable() throws Exception {
        execute("create table t (x long, ts timestamp) timestamp(ts) partition by day wal");
        execute("insert into t values (1, '2024-01-01T00')");
        drainWalQueue();
        return engine.verifyTableName("t");
    }

    private static FilesFacade failingColumnOpens(AtomicBoolean isFailing, AtomicInteger faults) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // The UPDATE writes a new version of column x.
                if (isFailing.get() && Utf8s.containsAscii(name, "x.d.")) {
                    faults.incrementAndGet();
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };
    }

    private static long suspendedTables() {
        return TestUtils.getMetricValue(engine, SUSPENDED_TABLES);
    }
}
