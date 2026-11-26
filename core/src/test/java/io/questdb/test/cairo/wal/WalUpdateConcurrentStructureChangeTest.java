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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UPDATE statements race a thread that adds and drops columns of the same WAL table, and a thread that applies its
 * WAL, as clients of HTTP and PostgreSQL wire do: each statement retries while the table is busy. Every UPDATE adds 1
 * to every row, so every acknowledged UPDATE, and only those, must show in the rows, exactly once, and the table must
 * never suspend.
 */
public class WalUpdateConcurrentStructureChangeTest extends AbstractCairoTest {
    private static final int ROW_COUNT = 4;
    // the column thread changes a column once every that many finished UPDATEs, and when an UPDATE asks
    private static final int STATEMENTS_PER_COLUMN_CHANGE = 4;
    private static final int UPDATES_PER_THREAD = 25;
    private static final int UPDATE_THREAD_COUNT = 4;
    // whether the UPDATE that the calling thread executes waits, once it took its WAL writer, for a column change
    private static final ThreadLocal<Boolean> isWindowArmed = ThreadLocal.withInitial(() -> false);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // 32 pooled WAL writers per table rather than 320 keeps holding all of them cheap
        setProperty(PropertyKey.CAIRO_WAL_WRITER_POOL_MAX_SEGMENTS, 1);
        AbstractCairoTest.setUpStatic();
    }

    @After
    public void tearDownObserver() {
        OperationDispatcher.setPreApplyObserver(null);
    }

    /**
     * A thread at times holds every pooled WAL writer of the table, as long-lived ingestion connections do. The
     * statements that find none fail with "table busy", rather than change the table outside the WAL, and their
     * retries apply them once.
     */
    @Test
    public void testStatementsWithEveryWalWriterBusyApplyOnce() throws Exception {
        assertConcurrentStatementsApplyOnce(false, true);
    }

    /**
     * A third of the UPDATEs wait, after they took their WAL writer and before they are sequenced, for the column
     * thread to commit a change through another WAL writer: the window in which an UPDATE compiled against the old
     * columns used to be sequenced after the change, and failed to apply when the change dropped a column it reads.
     */
    @Test
    public void testUpdatesRacingColumnChangesApplyOnce() throws Exception {
        assertConcurrentStatementsApplyOnce(true, false);
    }

    private static void executeRetryingBusy(String sql, SqlExecutionContext context, SCSequence eventSubSeq, AtomicLong busyRefusals) throws SqlException {
        while (true) {
            try {
                engine.execute(sql, context, eventSubSeq);
                return;
            } catch (EntryUnavailableException e) {
                // every pooled WAL writer of the table is in use; HTTP retries the statement the same way
                TestUtils.assertContains(e.getFlyweightMessage(), "table busy [reason=");
                busyRefusals.incrementAndGet();
                Os.pause();
            }
        }
    }

    private void assertConcurrentStatementsApplyOnce(boolean isWindowForced, boolean isPoolHeld) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            execute("CREATE TABLE t (k INT, c LONG, ts TIMESTAMP, v0 INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO t (k, c, ts) VALUES
                        (1, 0, '2024-01-01'),
                        (2, 0, '2024-01-02'),
                        (3, 0, '2024-01-03'),
                        (4, 0, '2024-01-04')
                    """);
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");

            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            // the newest of the columns the column thread adds and drops, v<n>, which an UPDATE filters on
            final AtomicInteger liveColumn = new AtomicInteger();
            final AtomicLong appliedUpdates = new AtomicLong();
            final AtomicLong busyRefusals = new AtomicLong();
            final AtomicLong droppedColumnRefusals = new AtomicLong();
            final AtomicInteger runningUpdateThreads = new AtomicInteger(UPDATE_THREAD_COUNT);
            final AtomicBoolean isColumnThreadDone = new AtomicBoolean();
            final AtomicBoolean isApplyDone = new AtomicBoolean();
            final AtomicInteger windowsServed = new AtomicInteger();
            // an UPDATE that opens a window asks for a column change, and goes on once the change is committed
            final Semaphore windowRequests = new Semaphore(0);
            final Semaphore windowsClosed = new Semaphore(0);
            final int threadCount = UPDATE_THREAD_COUNT + 2 + (isPoolHeld ? 1 : 0);
            final CyclicBarrier start = new CyclicBarrier(threadCount);

            if (isWindowForced) {
                OperationDispatcher.setPreApplyObserver(() -> {
                    if (!isWindowArmed.get()) {
                        return;
                    }
                    // only the first attempt: the retry after a recompile goes through
                    isWindowArmed.set(false);
                    windowRequests.release();
                    try {
                        while (!windowsClosed.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                            if (isColumnThreadDone.get() || !errors.isEmpty()) {
                                return;
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                });
            }

            final ObjList<Thread> threads = new ObjList<>();
            for (int t = 0; t < UPDATE_THREAD_COUNT; t++) {
                final Rnd threadRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                threads.add(new Thread(() -> {
                    try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                        final SCSequence eventSubSeq = new SCSequence();
                        start.await();
                        for (int i = 0; i < UPDATES_PER_THREAD && errors.isEmpty(); i++) {
                            final boolean isOnLiveColumn = threadRnd.nextBoolean();
                            final String sql = isOnLiveColumn
                                    ? "UPDATE t SET c = c + 1 WHERE v" + liveColumn.get() + " IS NULL"
                                    : "UPDATE t SET c = c + 1";
                            isWindowArmed.set(isWindowForced && threadRnd.nextInt(3) == 0);
                            try {
                                executeRetryingBusy(sql, context, eventSubSeq, busyRefusals);
                                appliedUpdates.incrementAndGet();
                            } catch (SqlException e) {
                                // the column thread dropped the column before the statement compiled again
                                Assert.assertTrue(e.getFlyweightMessage().toString(), isOnLiveColumn);
                                TestUtils.assertContains(e.getFlyweightMessage(), "Invalid column: v");
                                droppedColumnRefusals.incrementAndGet();
                            } finally {
                                isWindowArmed.set(false);
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        runningUpdateThreads.decrementAndGet();
                        Path.clearThreadLocals();
                    }
                }));
            }

            // Adds v<n + 1>, then drops v<n> and makes UPDATEs filter on v<n + 1>, one change at a time, while UPDATEs run.
            // A change serves the windows opened before it starts, and otherwise waits for UPDATEs to finish: each
            // change makes every WAL writer of the table catch up on its next use.
            threads.add(new Thread(() -> {
                try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
                    final SCSequence eventSubSeq = new SCSequence();
                    start.await();
                    int changes = 0;
                    while (runningUpdateThreads.get() > 0 && errors.isEmpty()) {
                        final int windows = windowRequests.drainPermits();
                        if (windows == 0 && changes >= (appliedUpdates.get() + droppedColumnRefusals.get()) / STATEMENTS_PER_COLUMN_CHANGE) {
                            Os.pause();
                            continue;
                        }
                        final int n = liveColumn.get();
                        if ((changes & 1) == 0) {
                            executeRetryingBusy("ALTER TABLE t ADD COLUMN v" + (n + 1) + " INT", context, eventSubSeq, busyRefusals);
                        } else {
                            // the UPDATEs filter on v<n> until after it is gone
                            executeRetryingBusy("ALTER TABLE t DROP COLUMN v" + n, context, eventSubSeq, busyRefusals);
                            liveColumn.set(n + 1);
                        }
                        changes++;
                        windowsClosed.release(windows);
                        windowsServed.addAndGet(windows);
                    }
                    if ((changes & 1) == 1) {
                        // the last change added a column: leave one of them
                        executeRetryingBusy("ALTER TABLE t DROP COLUMN v" + liveColumn.get(), context, eventSubSeq, busyRefusals);
                        liveColumn.incrementAndGet();
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    isColumnThreadDone.set(true);
                    Path.clearThreadLocals();
                }
            }));

            threads.add(new Thread(() -> {
                try (
                        ApplyWal2TableJob applyJob = new ApplyWal2TableJob(engine, 0);
                        CheckWalTransactionsJob checkJob = new CheckWalTransactionsJob(engine)
                ) {
                    start.await();
                    while (!isApplyDone.get() && errors.isEmpty()) {
                        if (!applyJob.run() && !checkJob.run()) {
                            Os.pause();
                        }
                        if (engine.getTableSequencerAPI().isSuspended(token)) {
                            throw new AssertionError("the table suspended");
                        }
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                }
            }));

            if (isPoolHeld) {
                // holds every pooled WAL writer of the table until statements find none, a few times over; the first
                // time before the other threads start
                threads.add(new Thread(() -> {
                    final ObjList<WalWriter> walWriters = new ObjList<>();
                    try {
                        for (int round = 0; round < 2 && errors.isEmpty(); round++) {
                            while (true) {
                                try {
                                    walWriters.add(engine.getWalWriter(token));
                                } catch (EntryUnavailableException e) {
                                    break;
                                }
                            }
                            final long refusals = busyRefusals.get();
                            if (round == 0) {
                                start.await();
                            }
                            while (busyRefusals.get() < refusals + UPDATE_THREAD_COUNT && runningUpdateThreads.get() > 0 && errors.isEmpty()) {
                                Os.pause();
                            }
                            Misc.freeObjListAndClear(walWriters);
                            // lets statements through before the next round
                            final long applied = appliedUpdates.get();
                            while (appliedUpdates.get() < applied + UPDATE_THREAD_COUNT && runningUpdateThreads.get() > 0 && errors.isEmpty()) {
                                Os.pause();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        Misc.freeObjListAndClear(walWriters);
                        Path.clearThreadLocals();
                    }
                }));
            }

            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).start();
            }
            // the apply thread goes last, after every statement
            for (int i = 0, n = threads.size(); i < n; i++) {
                if (i != UPDATE_THREAD_COUNT + 1) {
                    threads.getQuick(i).join();
                }
            }
            isApplyDone.set(true);
            threads.getQuick(UPDATE_THREAD_COUNT + 1).join();
            for (Throwable error : errors) {
                throw new AssertionError(error);
            }

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            LOG.info().$("concurrent statements done [applied=").$(appliedUpdates.get())
                    .$(", droppedColumnRefusals=").$(droppedColumnRefusals.get())
                    .$(", busyRefusals=").$(busyRefusals.get())
                    .$(", windowsServed=").$(windowsServed.get())
                    .I$();
            Assert.assertEquals(UPDATE_THREAD_COUNT * UPDATES_PER_THREAD, appliedUpdates.get() + droppedColumnRefusals.get());
            if (isWindowForced) {
                Assert.assertTrue(windowsServed.get() > 0);
            }
            if (isPoolHeld) {
                Assert.assertTrue(busyRefusals.get() >= UPDATE_THREAD_COUNT);
            }
            // every acknowledged UPDATE added 1 to every row, once
            final long applied = appliedUpdates.get();
            assertQuery("SELECT min(c), max(c), count() FROM t").noLeakCheck().noRandomAccess().expectSize()
                    .returns("min\tmax\tcount\n" + applied + "\t" + applied + "\t" + ROW_COUNT + "\n");
            // the column thread's changes all applied, through the WAL: one live column of its own is left
            assertQuery("SELECT \"column\" FROM table_columns('t')").noLeakCheck().noRandomAccess()
                    .returns("column\nk\nc\nts\nv" + liveColumn.get() + "\n");
            // the table keeps taking UPDATEs and rows
            execute("UPDATE t SET c = c + 1");
            execute("INSERT INTO t (k, c, ts) VALUES (5, 0, '2024-01-05')");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT k, c FROM t").noLeakCheck().expectSize().returns(
                    "k\tc\n1\t" + (applied + 1) + "\n2\t" + (applied + 1) + "\n3\t" + (applied + 1) + "\n4\t" + (applied + 1) + "\n5\t0\n"
            );
        });
    }
}
