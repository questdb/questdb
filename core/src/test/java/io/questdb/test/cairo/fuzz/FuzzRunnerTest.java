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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.mp.MPSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.fuzz.FuzzAddCoveringIndexOperation;
import io.questdb.test.fuzz.FuzzDropIndexOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class FuzzRunnerTest extends AbstractCairoTest {
    // Ample budget: a couple of 100ms backoffs recover well within it even under a slow sleep, so the
    // retry-then-succeed tests never flake on timing.
    private static final long AMPLE_RETRY_TIMEOUT_MILLIS = 10_000;
    // Deliberately small: testReadTimeoutRethrownAfterDeadline spends the whole budget, so it must stay
    // sub-second.
    private static final long RETRY_TIMEOUT_MILLIS = 300;
    private final FuzzRunner fuzzer = new FuzzRunner();
    private final Rnd rnd = TestUtils.generateRandom(LOG);

    @Test
    public void testApplyLeavesColumnPurgeToPoolJob() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 0);
        assertMemoryLeak(() -> {
            final File staleFile = createStaleColumnVersion("t", false);
            // no job holds the purge log, as after a pool job failed to save a purge
            final FuzzRunner runner = newRunner();
            runner.setHasPoolColumnPurgeJob(true);
            runner.applyNonWal(new ObjList<>(), "t", new Rnd());
            Assert.assertTrue(staleFile.exists());

            runner.setHasPoolColumnPurgeJob(false);
            runner.applyNonWal(new ObjList<>(), "t", new Rnd());
            Assert.assertFalse(staleFile.exists());
            assertQuery("SELECT x FROM t").noLeakCheck().expectSize().returns("x\n2\n");
        });
    }

    @Test
    public void testApplyLeavesColumnPurgeToRunningJob() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 0);
        assertMemoryLeak(() -> {
            final File staleFile = createStaleColumnVersion("t", false);
            // the job of the test holds the purge log, as the job of a server does
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                newRunner().applyNonWal(new ObjList<>(), "t", new Rnd());
                Assert.assertTrue(staleFile.exists());
                purgeJob.run();
            }
            Assert.assertFalse(staleFile.exists());
            assertQuery("SELECT x FROM t").noLeakCheck().expectSize().returns("x\n2\n");
        });
    }

    @Test
    public void testApplyManyWalParallelDrainsAfterWritersDoneCallback() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("callback");
            fuzzer.withDb(engine, sqlExecutionContext);
            ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            ObjList<FuzzTransaction> tableTransactions = new ObjList<>();
            FuzzTransaction transaction = new FuzzTransaction();
            transaction.operationList.add((rnd, cairoEngine, writer, timestampIndex, excludedIntervals) -> {
                var row = writer.newRow(0);
                row.putSym(0, "ingest");
                row.append();
                return false;
            });
            tableTransactions.add(transaction);
            transactions.add(tableTransactions);
            AtomicInteger callbackCount = new AtomicInteger();

            fuzzer.applyManyWalParallel(transactions, new Rnd(), "callback", false, true, () -> {
                Assert.assertEquals(1, engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("callback")));
                callbackCount.incrementAndGet();
                // Model a background producer's final commit. Apply workers must still drain it.
                TestUtils.unchecked(() -> execute("INSERT INTO callback VALUES ('final', 'value', 1)"));
            });

            Assert.assertEquals(1, callbackCount.get());
            assertQuery("SELECT sym FROM callback ORDER BY sym").expectSize().returns("sym\nfinal\ningest\n");
        });
    }

    @Test
    public void testApplyManyWalParallelDrainsAfterWritersDoneCallbackFails() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("callback_failure");
            fuzzer.withDb(engine, sqlExecutionContext);
            ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            transactions.add(new ObjList<>());
            RuntimeException failure = new RuntimeException("callback failure");

            RuntimeException thrown = Assert.assertThrows(RuntimeException.class, () ->
                    fuzzer.applyManyWalParallel(transactions, new Rnd(), "callback_failure", false, true, () -> {
                        TestUtils.unchecked(() -> execute("INSERT INTO callback_failure VALUES ('final', 'value', 1)"));
                        throw failure;
                    })
            );

            Assert.assertSame(failure, thrown.getCause());
            assertQuery("SELECT sym FROM callback_failure").expectSize().returns("sym\nfinal\n");
        });
    }

    @Test
    public void testApplyNonWalFailsWhenWriterColumnVersionsAreNotOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithAddedColumn("t", false);
            final ObjList<FuzzTransaction> transactions = new ObjList<>();
            final FuzzTransaction transaction = new FuzzTransaction();
            // changes the cached records of the writer as a change that commit() does not write would
            transaction.operationList.add((rnd, cairoEngine, writer, timestampIndex, excludedIntervals) -> {
                changeCachedColumnTop(((TableWriter) writer).columnVersionReader().getCachedColumnVersionList());
                return false;
            });
            transactions.add(transaction);
            try {
                newRunner().applyNonWal(transactions, "t", new Rnd());
                Assert.fail("the check must find the writer's records missing from _cv");
            } catch (AssertionError e) {
                TestUtils.assertContains(e.getMessage(), "the writer's _cv records are not on disk [table=t, writer=");
            }
        });
    }

    @Test
    public void testApplyNonWalKeepsColumnPurgeOffWhilePlantedFailurePending() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 0);
        final FuzzRunner runner = newRunner();
        final FailureFileFacade ff = runner.getFileFacade();
        assertMemoryLeak(ff, () -> {
            final File staleFile = createStaleColumnVersion("t", false);
            // no file operation of the run reaches the planted call, so the failure stays pending
            ff.setToFailAfter(Integer.MAX_VALUE);
            runner.applyNonWal(new ObjList<>(), "t", new Rnd());
            Assert.assertTrue(staleFile.exists());

            // applyNonWal clears the failure before it returns
            runner.applyNonWal(new ObjList<>(), "t", new Rnd());
            Assert.assertFalse(staleFile.exists());
            assertQuery("SELECT x FROM t").noLeakCheck().expectSize().returns("x\n2\n");
        });
    }

    @Test
    public void testApplyNonWalPurgesColumnVersionsAfterEachTransaction() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 0);
        assertMemoryLeak(() -> {
            final File staleFile = createStaleColumnVersion("t", false);
            final ObjList<FuzzTransaction> transactions = new ObjList<>();
            transactions.add(rolledBackTransaction(() -> Assert.assertTrue(staleFile.exists())));
            transactions.add(rolledBackTransaction(() -> Assert.assertFalse(staleFile.exists())));
            // the first draw of the reload Rnd is false, so applyNonWal purges and reloads after every transaction
            Assert.assertFalse(new Rnd().nextBoolean());

            newRunner().applyNonWal(transactions, "t", new Rnd());

            assertQuery("SELECT x FROM t").noLeakCheck().expectSize().returns("x\n2\n");
        });
    }

    @Test
    public void testApplyNonWalRecoversFromFailureAtEveryCallOfReopen() throws Exception {
        // A pooled reader that fails to go passive in close() is closed and dropped from the pool. Fail each file
        // call in turn, from the point the runner's readers hold an old txn to the end of the reopen that closes them.
        final FuzzRunner runner = newRunner();
        final FailureFileFacade ff = runner.getFileFacade();
        final AtomicInteger expelledReaders = new AtomicInteger();
        assertMemoryLeak(ff, () -> {
            engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_READER && event == PoolListener.EV_OUT_OF_POOL_CLOSE) {
                    expelledReaders.incrementAndGet();
                }
            });
            try {
                final AtomicBoolean isFired = new AtomicBoolean();
                int failAfter = 0;
                do {
                    failAfter++;
                    execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    execute("INSERT INTO t VALUES (0, '1970-01-01')");
                    runner.applyNonWal(reopenAfterReadersFallBehind(ff, failAfter, isFired), "t", newLaggingReloadRnd());
                    assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n5\n");
                    execute("DROP TABLE t");
                } while (isFired.get());
            } finally {
                engine.setPoolListener(null);
            }
            Assert.assertTrue("no failure closed a reader", expelledReaders.get() > 0);
        });
    }

    @Test
    public void testApplyPurgesColumnVersionsBeforeReturning() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 0);
        assertMemoryLeak(() -> {
            // with no transaction to apply, each apply still purges what the queue holds before it returns
            final File nonWalFile = createStaleColumnVersion("non_wal", false);
            newRunner().applyNonWal(new ObjList<>(), "non_wal", new Rnd());
            Assert.assertFalse(nonWalFile.exists());

            final File walFile = createStaleColumnVersion("wal", true);
            newRunner().applyWal(new ObjList<>(), "wal", 1, new Rnd());
            Assert.assertFalse(walFile.exists());

            final File parallelFile = createStaleColumnVersion("parallel", true);
            final ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            transactions.add(new ObjList<>());
            newRunner().applyManyWalParallel(transactions, new Rnd(), "parallel", false, true);
            Assert.assertFalse(parallelFile.exists());

            for (String tableName : new String[]{"non_wal", "wal", "parallel"}) {
                assertQuery("SELECT x FROM " + tableName).noLeakCheck().expectSize().returns("x\n2\n");
            }
        });
    }

    @Test
    public void testApplyWalDrainTakesNoDrawFromApplyRnd() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t VALUES (0, '2022-02-20T00:00:00Z')");
            execute("INSERT INTO t VALUES (1, '2022-02-21T00:00:00Z')");
            execute("INSERT INTO t VALUES (2, '2022-02-22T00:00:00Z')");
            final Rnd applyRnd = new Rnd(7, 11);

            newRunner().applyWal(new ObjList<>(), "t", 1, applyRnd);

            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            // no transactions: applyToWal() draws nothing, so the drains must leave applyRnd at its seeds
            Assert.assertEquals(new Rnd(7, 11).nextLong(), applyRnd.nextLong());
        });
    }

    @Test
    public void testApplyWalFailsWhenPooledWriterColumnVersionsAreNotOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithAddedColumn("t", true);
            final FuzzRunner runner = newRunner();
            // the drain checks the writer the WAL apply leaves in the pool, which agrees with _cv
            runner.applyWal(new ObjList<>(), "t", 1, new Rnd());
            try (TableWriter writer = getWriter("t")) {
                changeCachedColumnTop(writer.columnVersionReader().getCachedColumnVersionList());
            }
            try {
                runner.applyWal(new ObjList<>(), "t", 1, new Rnd());
                Assert.fail("the check must find the pooled writer's records missing from _cv");
            } catch (AssertionError e) {
                TestUtils.assertContains(e.getMessage(), "the writer's _cv records are not on disk [table=t, writer=");
            }
        });
    }

    @Test
    public void testApplyWalSameSeedsDifferentDrainLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE t2 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t1 VALUES (0, '2022-02-20T00:00:00Z')");
            for (int i = 0; i < 20; i++) {
                execute("INSERT INTO t2 VALUES (" + i + ", '2022-02-20T00:00:" + (10 + i) + "Z')");
            }
            final FuzzRunner runner = newRunner();
            final Rnd rnd1 = new Rnd(3, 5);
            final Rnd rnd2 = new Rnd(3, 5);

            runner.applyWal(new ObjList<>(), "t1", 1, rnd1);
            runner.applyWal(new ObjList<>(), "t2", 1, rnd2);

            assertQuery("SELECT count() FROM t1").noLeakCheck().noRandomAccess().expectSize().returns("count\n1\n");
            assertQuery("SELECT count() FROM t2").noLeakCheck().noRandomAccess().expectSize().returns("count\n20\n");
            // a 1-commit and a 20-commit backlog drain in a different number of loop iterations
            Assert.assertEquals(rnd1.nextLong(), rnd2.nextLong());
        });
    }

    @Test
    public void testApplyWalWithDropIndexDrainTakesOneDrawPerTransaction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (c2 SYMBOL, ts TIMESTAMP, sym2 SYMBOL INDEX TYPE BITMAP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT 'k' || (x % 5), dateadd('m', x::INT * 7, '2022-02-26T00:00:00Z'::TIMESTAMP), 'S' || (x % 11) FROM long_sequence(400)");
            final IntList include = new IntList();
            include.add(2);
            final FuzzTransaction coverTransaction = new FuzzTransaction();
            coverTransaction.operationList.add(new FuzzAddCoveringIndexOperation(0, include, "POSTING"));
            coverTransaction.rollback = true;
            final FuzzTransaction dropTransaction = new FuzzTransaction();
            dropTransaction.operationList.add(new FuzzDropIndexOperation(2, "POSTING"));
            dropTransaction.rollback = true;
            final ObjList<FuzzTransaction> transactions = new ObjList<>();
            transactions.add(coverTransaction);
            transactions.add(dropTransaction);
            final Rnd applyRnd = new Rnd(13, 17);

            newRunner().applyWal(transactions, "t", 1, applyRnd);

            assertQuery("SELECT count() FROM t").noLeakCheck().noRandomAccess().expectSize().returns("count\n400\n");
            // applyToWal() picks a WAL writer for each of the 2 transactions; the drain before the drop
            // index op and the drains around executeDropIndexOps() must take nothing more
            final Rnd expected = new Rnd(13, 17);
            expected.nextPositiveInt();
            expected.nextPositiveInt();
            Assert.assertEquals(expected.nextLong(), applyRnd.nextLong());
        });
    }

    @Test
    public void testAssertStringColDensityUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("density");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Transaction read timeout [src=density]");

            retryingFuzzer.assertStringColDensity("density");

            assertRetryRouting(retryingFuzzer, 1, 0, 0);
            Assert.assertEquals(2, retryingFuzzer.readTimeoutAttempts.get()); // injected timeout, then the real open
        });
    }

    @Test
    public void testCancelledDuringRecreateWaitReturnsNull() {
        AtomicInteger attempts = new AtomicInteger();
        String reader = fuzzer.openWithRetries(
                () -> {
                    attempts.incrementAndGet();
                    throw CairoException.critical(0).put("table does not exist [table=x]");
                },
                true,
                () -> attempts.get() >= 3,
                RETRY_TIMEOUT_MILLIS
        );
        // The recreate never lands, so without the cancellation check the open would wait forever.
        Assert.assertNull(reader);
        Assert.assertEquals(3, attempts.get());
    }

    @Test
    public void testDrainWalQueueUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("drain");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Transaction read timeout [src=drain]");
            ObjList<FuzzTransaction> transactions = new ObjList<>();

            retryingFuzzer.applyWal(transactions, "drain", 1, rnd);

            assertRetryRouting(retryingFuzzer, 2, 2, 0);
            Assert.assertEquals(2, retryingFuzzer.readTimeoutAttempts.get());
        });
    }

    @Test
    public void testGenerateSymbolsUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("symbols");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Column Version read timeout [src=symbols]");

            String[] symbols = retryingFuzzer.generateSymbols(rnd, 3, 4, "symbols");

            Assert.assertEquals(3, symbols.length);
            assertRetryRouting(retryingFuzzer, 1, 0, 0);
            Assert.assertEquals(2, retryingFuzzer.readTimeoutAttempts.get());
        });
    }

    @Test
    public void testGenerateTransactionsUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("generate");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Metadata read timeout [src=generate]");
            ObjList<FuzzTransaction> transactions = retryingFuzzer.generateTransactions("generate", rnd, 0, 1);

            try {
                assertRetryRouting(retryingFuzzer, 1, 0, 0);
                Assert.assertEquals(2, retryingFuzzer.readTimeoutAttempts.get());
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testLatePurgeWorkerFailureReachesCallerAfterJoin() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("late_error");
            CountDownLatch purgeEntered = new CountDownLatch(1);
            CountDownLatch releasePurge = new CountDownLatch(1);
            AtomicReference<BooleanSupplier> isRunOver = new AtomicReference<>();
            AtomicReference<Thread> purgeThread = new AtomicReference<>();
            AtomicReference<Throwable> workerFailure = new AtomicReference<>();
            AtomicReference<Throwable> callerFailure = new AtomicReference<>();
            RuntimeException sentinel = new RuntimeException("late purge worker failure");
            FuzzRunner failingFuzzer = new FuzzRunner() {
                @Override
                <T> @Nullable T openWithRetries(
                        Supplier<T> open,
                        boolean isTableRecreateTolerated,
                        @Nullable BooleanSupplier isCancelled,
                        long retryTimeoutMillis
                ) {
                    purgeThread.set(Thread.currentThread());
                    isRunOver.set(isCancelled);
                    purgeEntered.countDown();
                    try {
                        Assert.assertNotNull("purge reader must support cancellation", isCancelled);
                        Assert.assertTrue("purge release timed out", releasePurge.await(30, TimeUnit.SECONDS));
                    } catch (Throwable e) {
                        workerFailure.set(e);
                        throw new AssertionError("purge worker coordination failed", e);
                    }
                    throw sentinel;
                }
            };
            failingFuzzer.withDb(engine, sqlExecutionContext);
            // Empty transactions let every writer finish without introducing other worker failures.
            ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            transactions.add(new ObjList<>());
            Thread caller = new Thread(() -> {
                try {
                    failingFuzzer.applyManyWalParallel(transactions, rnd, "late_error", false, true);
                } catch (Throwable e) {
                    callerFailure.set(e);
                } finally {
                    Path.clearThreadLocals();
                }
            }, "late-purge-error-caller");
            try {
                caller.start();
                Assert.assertTrue("purge worker did not enter", purgeEntered.await(10, TimeUnit.SECONDS));
                // Cancellation alone is too early: the old ordering could still inspect errors next.
                // With the purge worker held on the latch, the caller must stay in worker joins after
                // writer completion. Observe that stable phase before allowing the sentinel to escape.
                TestUtils.assertEventually(() -> {
                    Assert.assertNull("purge worker coordination failed", workerFailure.get());
                    Assert.assertNotNull(isRunOver.get());
                    Assert.assertTrue("writers have not finished", isRunOver.get().getAsBoolean());
                    boolean hasJoinFrame = false;
                    boolean hasApplyFrame = false;
                    for (StackTraceElement frame : caller.getStackTrace()) {
                        hasJoinFrame |= Thread.class.getName().equals(frame.getClassName())
                                && "join".equals(frame.getMethodName());
                        hasApplyFrame |= FuzzRunner.class.getName().equals(frame.getClassName())
                                && "applyManyWalParallel".equals(frame.getMethodName());
                    }
                    Assert.assertTrue("caller did not reach worker joins", hasJoinFrame && hasApplyFrame);
                }, 10);
            } finally {
                releasePurge.countDown();
                try {
                    caller.join(10_000);
                } finally {
                    Thread purge = purgeThread.get();
                    if (purge != null) {
                        purge.join(10_000);
                        Assert.assertFalse("purge worker did not terminate", purge.isAlive());
                    }
                }
                Assert.assertFalse("caller did not terminate", caller.isAlive());
            }
            if (workerFailure.get() != null) {
                throw new AssertionError("purge worker coordination failed", workerFailure.get());
            }
            Assert.assertNotNull("late worker failure did not reach caller", callerFailure.get());
            Assert.assertSame(sentinel, callerFailure.get().getCause());
        });
    }

    @Test
    public void testPoolPostingSealJobReopensLogAfterFault() throws Exception {
        // The PostingSealPurgeJob of a worker pool meets a fault planted on its log as it logs an entry: the open of the
        // first partition of the log fails. It purges without the log, reopens the log in the same run and writes the
        // entry back, then completes it there.
        final FuzzRunner runner = newRunner();
        final FailureFileFacade ff = runner.getFileFacade();
        final String logTableName = configuration.getSystemTableNamePrefix() + "posting_seal_purge_log";
        ff.watchTable(logTableName);
        assertMemoryLeak(ff, () -> {
            final TableToken token = createTable(new TableModel(configuration, "ps", PartitionBy.NONE).col("c", ColumnType.INT).timestamp("ts"));
            final long sealTxn = writeAndSealTwice(token, "c");
            final WorkerPool pool = new TestWorkerPool(1);
            final PostingSealPurgeJob job = new PostingSealPurgeJob(engine);
            pool.assign(job);
            pool.freeOnExit(job);
            ff.armTableFailure(logTableName, FailureFileFacade.TableFailureKind.PARTITION_OPEN, PostingSealPurgeJob.class.getName(), "persistReadyTasksDirect");
            pool.start(LOG);
            try (Path path = partitionPathOf(token)) {
                final int pLen = path.size();
                publishPostingSealPurgeTask(token, "c", sealTxn);
                TestUtils.assertEventually(() -> {
                    Assert.assertFalse(configuration.getFilesFacade().exists(
                            PostingIndexUtils.valueFileName(path.trimTo(pLen), "c", TableUtils.COLUMN_NAME_TXN_NONE, sealTxn)
                    ));
                    assertQuery("SELECT column_name, seal_txn, completed != null done FROM '" + logTableName + "'")
                            .noLeakCheck()
                            .expectSize()
                            .returns("column_name\tseal_txn\tdone\nc\t" + sealTxn + "\ttrue\n");
                });
            } finally {
                pool.halt();
            }
            Assert.assertEquals(1, ff.getTableFailuresFired(logTableName, FailureFileFacade.TableFailureKind.PARTITION_OPEN));
            Assert.assertEquals(0, ff.getTableFailuresFired(logTableName, FailureFileFacade.TableFailureKind.PAGE_ALLOCATE));
        });
    }

    @Test
    public void testPurgeLogFaultShapesFire() throws Exception {
        // At chance 1, each run of the column purge job of an apply plants a fault of a random kind on each purge log.
        // Each transaction sets the clock to a month of its own, queues a purge for each purge job and runs the posting
        // seal purge job, as a worker pool does in a fuzz run, and applyNonWal runs the column purge job after it. A
        // job writes the purge to a new partition of its log, or reopens its log writer, which opens the last
        // partition, before it writes the purge back: either way it opens partition files and allocates their pages,
        // so each fault fires in the next run of its job, of either kind. Two of the 42 faults do not fire: the column
        // purge job logs nothing in the run after the last transaction, and the next fault replaces the one planted on
        // the posting seal purge log after the last transaction.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 0);
        final FuzzRunner runner = newRunner();
        runner.setPurgeLogFaultProbability(1, 42L, 7L);
        assertMemoryLeak(runner.getFileFacade(), () -> {
            final long firstMonth = MicrosTimestampDriver.floor("2024-01-01");
            setCurrentMicros(firstMonth);
            final File staleFile = createStaleColumnVersion("t", false);
            final TableToken goneToken = new TableToken("gone", "gone", null, 123, false, false, false);
            try (PostingSealPurgeJob postingSealPurgeJob = new PostingSealPurgeJob(engine)) {
                final ObjList<FuzzTransaction> transactions = new ObjList<>();
                for (int i = 0; i < 20; i++) {
                    final int month = i;
                    transactions.add(rolledBackTransaction(() -> {
                        setCurrentMicros(Micros.addMonths(firstMonth, month));
                        publishColumnPurgeTask(goneToken);
                        publishPostingSealPurgeTask(goneToken, "c", 1);
                        postingSealPurgeJob.run();
                    }));
                }
                // the first draw of the reload Rnd is false, so applyNonWal runs the column purge job after every transaction
                Assert.assertFalse(new Rnd().nextBoolean());
                runner.applyNonWal(transactions, "t", new Rnd());
                // the posting seal purge job meets the fault planted last in the next month
                setCurrentMicros(Micros.addMonths(firstMonth, 20));
                publishPostingSealPurgeTask(goneToken, "c", 1);
                postingSealPurgeJob.run();
            }
            for (boolean isPostingSealLog : new boolean[]{false, true}) {
                final String logName = isPostingSealLog ? "posting seal purge log " : "column purge log ";
                int plantedCount = 0;
                int firedCount = 0;
                for (FailureFileFacade.TableFailureKind kind : FailureFileFacade.TableFailureKind.values()) {
                    final int kindFiredCount = runner.getFiredPurgeLogFaultCount(isPostingSealLog, kind);
                    Assert.assertTrue(logName + kind, kindFiredCount > 0);
                    plantedCount += runner.getPlantedPurgeLogFaultCount(isPostingSealLog, kind);
                    firedCount += kindFiredCount;
                }
                Assert.assertEquals(logName, 21, plantedCount);
                Assert.assertEquals(logName, 20, firedCount);
            }
            // the faults of the logs leave the purges and the table intact
            Assert.assertFalse(staleFile.exists());
            assertQuery("SELECT x FROM t").noLeakCheck().expectSize().returns("x\n2\n");
        });
    }

    @Test
    public void testPurgePartitionReadersUseRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("purge");
            // No injected timeout on this route: the writers finish at once on an empty transaction list,
            // so the cancellation predicate can legitimately end the wait instead of retrying it.
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(0, "");
            ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            transactions.add(new ObjList<>());

            retryingFuzzer.applyManyWalParallel(transactions, rnd, "purge", false, true);

            assertRetryRouting(retryingFuzzer, 2, 2, 2);
        });
    }

    @Test
    public void testReadTimeoutRethrownAfterDeadline() {
        AtomicInteger attempts = new AtomicInteger();
        long startNanos = System.nanoTime();
        CairoException e = Assert.assertThrows(CairoException.class, () -> fuzzer.openWithRetries(
                () -> {
                    attempts.incrementAndGet();
                    throw CairoException.critical(0).put("Transaction read timeout [src=reader, timeout=1000ms]");
                },
                false,
                null,
                RETRY_TIMEOUT_MILLIS
        ));
        TestUtils.assertContains(e.getFlyweightMessage(), "read timeout");
        // The budget is spent end to end, not restarted per attempt.
        Assert.assertTrue((System.nanoTime() - startNanos) >= RETRY_TIMEOUT_MILLIS * 1_000_000L);
        Assert.assertTrue("attempts: " + attempts.get(), attempts.get() > 1 && attempts.get() <= 20);
    }

    @Test
    public void testReadTimeoutRetriedThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        String reader = fuzzer.openWithRetries(
                () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw CairoException.critical(0).put("Metadata read timeout [src=metadata, timeout=1000ms]");
                    }
                    return "reader";
                },
                false,
                null,
                AMPLE_RETRY_TIMEOUT_MILLIS
        );
        Assert.assertEquals("reader", reader);
        Assert.assertEquals(3, attempts.get());
    }

    @Test
    public void testRunFuzzSameSeedsDifferentApplyTimeQuota() throws Exception {
        assertMemoryLeak(() -> {
            // a zero quota stops each apply job run after one commit, so the drains loop once per commit;
            // the large quota applies the backlog in one run. The seed Rnd must end in the same state.
            setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
            final Rnd rnd1 = new Rnd(19, 23);
            newFuzzRunner().runFuzz("quota_zero", rnd1);

            setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 600_000);
            final Rnd rnd2 = new Rnd(19, 23);
            newFuzzRunner().runFuzz("quota_large", rnd2);

            Assert.assertEquals(rnd1.nextLong(), rnd2.nextLong());
        });
    }

    @Test
    public void testTableRecreateNotToleratedFailsFast() {
        AtomicInteger attempts = new AtomicInteger();
        CairoException e = Assert.assertThrows(CairoException.class, () -> fuzzer.openWithRetries(
                () -> {
                    attempts.incrementAndGet();
                    throw CairoException.critical(0).put("table does not exist [table=x]");
                },
                false,
                null,
                RETRY_TIMEOUT_MILLIS
        ));
        TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist");
        Assert.assertEquals(1, attempts.get()); // no retry: the first attempt rethrows
    }

    @Test
    public void testTableRecreateToleratedIsWaitedOut() {
        AtomicInteger attempts = new AtomicInteger();
        String reader = fuzzer.openWithRetries(
                () -> {
                    int n = attempts.incrementAndGet();
                    if (n == 1) {
                        throw CairoException.critical(0).put("table name is reserved [table=x]");
                    }
                    if (n == 2) {
                        throw EntryLockedException.instance("locked");
                    }
                    return "reader";
                },
                true,
                null,
                RETRY_TIMEOUT_MILLIS
        );
        Assert.assertEquals("reader", reader);
        Assert.assertEquals(3, attempts.get());
    }

    @Test
    public void testUnrelatedCairoExceptionAlwaysRethrown() {
        AtomicInteger attempts = new AtomicInteger();
        // Even with recreate tolerated, anything outside the read-timeout / recreate families surfaces at once.
        CairoException e = Assert.assertThrows(CairoException.class, () -> fuzzer.openWithRetries(
                () -> {
                    attempts.incrementAndGet();
                    throw CairoException.critical(0).put("some other failure");
                },
                true,
                null,
                RETRY_TIMEOUT_MILLIS
        ));
        TestUtils.assertContains(e.getFlyweightMessage(), "some other failure");
        Assert.assertEquals(1, attempts.get());
    }

    private static void assertRetryRouting(
            RetryInjectingFuzzRunner fuzzer,
            int expectedOpenCalls,
            int expectedRecreateTolerantCalls,
            int expectedCancellableCalls
    ) {
        Assert.assertEquals(expectedOpenCalls, fuzzer.openCalls.get());
        Assert.assertEquals(expectedRecreateTolerantCalls, fuzzer.recreateTolerantCalls.get());
        Assert.assertEquals(expectedCancellableCalls, fuzzer.cancellableCalls.get());
    }

    // moves the column top of the last record, the one ADD COLUMN gave the partition it was added at
    private static void changeCachedColumnTop(LongList records) {
        final int columnTopIndex = records.size() - ColumnVersionReader.BLOCK_SIZE + ColumnVersionReader.COLUMN_TOP_OFFSET;
        Assert.assertNotEquals(ColumnVersionReader.COL_TOP_DEFAULT_PARTITION, records.getQuick(records.size() - ColumnVersionReader.BLOCK_SIZE));
        records.setQuick(columnTopIndex, records.getQuick(columnTopIndex) + 1);
    }

    // A reader pins the first version of x while UPDATE replaces it, so the writer queues its purge instead of
    // purging it, and no job of the test runs that purge.
    private static File createStaleColumnVersion(String tableName, boolean isWal) throws SqlException {
        execute("CREATE TABLE " + tableName + " (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + tableName + " VALUES (1, '2022-02-24T00:00:00Z')");
        drainWalQueue();
        final File[] partitions = new File(configuration.getDbRoot(), engine.verifyTableName(tableName).getDirName())
                .listFiles((dir, name) -> name.startsWith("2022-02-24"));
        Assert.assertNotNull(partitions);
        Assert.assertEquals(1, partitions.length);
        final File staleFile = new File(partitions[0], "x.d");
        try (TableReader ignore = getReader(tableName)) {
            execute("UPDATE " + tableName + " SET x = 2");
            drainWalQueue();
        }
        releaseSavePointHolds(tableName);
        engine.releaseInactive();
        Assert.assertTrue(staleFile.exists());
        return staleFile;
    }

    // the default partition of a non-partitioned table, created
    private static Path partitionPathOf(TableToken token) {
        final Path path = new Path().of(configuration.getDbRoot()).concat(token.getDirName());
        TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP_MICRO, PartitionBy.NONE, 0L, -1L);
        configuration.getFilesFacade().mkdirs(path.slash(), configuration.getMkDirMode());
        return path;
    }

    // queues the purge of a column version of a table, which the column purge job completes at once when the table does
    // not exist
    private static void publishColumnPurgeTask(TableToken token) {
        final MPSequence pubSeq = engine.getMessageBus().getColumnPurgePubSeq();
        long cursor;
        while ((cursor = pubSeq.next()) == -2) {
            Os.pause();
        }
        Assert.assertTrue("purge queue must accept the task", cursor >= 0);
        try {
            final ColumnPurgeTask task = engine.getMessageBus().getColumnPurgeQueue().get(cursor);
            task.of(token, "c", token.getTableId(), 0, ColumnType.INT, IndexType.NONE, ColumnType.TIMESTAMP_MICRO, PartitionBy.NONE, 1);
            task.appendColumnInfo(1, 0, -1);
        } finally {
            pubSeq.done(cursor);
        }
    }

    // queues the purge of a superseded sealed generation of the POSTING index of colName
    private static void publishPostingSealPurgeTask(TableToken token, String colName, long sealTxn) {
        final MPSequence pubSeq = engine.getMessageBus().getPostingSealPurgePubSeq();
        long cursor;
        while ((cursor = pubSeq.next()) == -2) {
            Os.pause();
        }
        Assert.assertTrue("purge queue must accept the task", cursor >= 0);
        try {
            engine.getMessageBus().getPostingSealPurgeQueue().get(cursor).of(
                    token, colName, TableUtils.COLUMN_NAME_TXN_NONE, sealTxn, 0L, -1L, PartitionBy.NONE, ColumnType.TIMESTAMP_MICRO, 0L, 1L
            );
        } finally {
            pubSeq.done(cursor);
        }
    }

    // a transaction that runs the step of the test when it applies, and rolls back
    private static FuzzTransaction rolledBackTransaction(Runnable step) {
        final FuzzTransaction transaction = new FuzzTransaction();
        transaction.operationList.add((rnd, cairoEngine, writer, timestampIndex, excludedIntervals) -> {
            step.run();
            return false;
        });
        transaction.rollback = true;
        return transaction;
    }

    // writes a POSTING index of colName in the default partition and seals it twice, returning the superseded sealTxn
    private static long writeAndSealTwice(TableToken token, String colName) {
        try (Path path = partitionPathOf(token)) {
            final int pLen = path.size();
            final long supersededSealTxn;
            try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, colName, TableUtils.COLUMN_NAME_TXN_NONE)) {
                for (int i = 0; i < 8; i++) {
                    writer.add(i % 4, i);
                }
                writer.setMaxValue(7);
                writer.commit();
                writer.seal();
                supersededSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(
                        configuration.getFilesFacade(),
                        PostingIndexUtils.keyFileName(path.trimTo(pLen), colName, TableUtils.COLUMN_NAME_TXN_NONE)
                );
                path.trimTo(pLen);
                for (int i = 8; i < 16; i++) {
                    writer.add(i % 4, i);
                }
                writer.setMaxValue(15);
                writer.commit();
                writer.seal();
            }
            return supersededSealTxn;
        }
    }

    private void createTableWithAddedColumn(String tableName, boolean isWal) throws Exception {
        execute("CREATE TABLE " + tableName + " (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + tableName + " VALUES (1, '2024-01-01T00:00:00Z')");
        // a column top record for 2024-01-01, which holds a row without y
        execute("ALTER TABLE " + tableName + " ADD COLUMN y INT");
        drainWalQueue();
    }

    private void createWalTable(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (sym SYMBOL, str STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private FuzzRunner newFuzzRunner() {
        final FuzzRunner runner = newRunner();
        // no planted IO failure: its retry lands on a thread-dependent call, so the run could not replay
        runner.setFuzzCounts(false, 2_000, 30, 10, 10, 20, 100, 3, -1, 0);
        runner.setFuzzProbabilities(
                0.01, 0.2, 0.1, 0.05, 0.05, 0.02, 0.05, 0.0, 1.0, 0.01, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        );
        return runner;
    }

    private RetryInjectingFuzzRunner newFuzzer(int injectedCall, String timeoutMessage) {
        RetryInjectingFuzzRunner retryingFuzzer = new RetryInjectingFuzzRunner(injectedCall, timeoutMessage);
        retryingFuzzer.withDb(engine, sqlExecutionContext);
        return retryingFuzzer;
    }

    // reload probability 0.25: the readers keep their txn after the first transaction and reload after every later one
    private static Rnd newLaggingReloadRnd() {
        return new Rnd() {
            private int doubleDraws;

            @Override
            public boolean nextBoolean() {
                return true;
            }

            @Override
            public double nextDouble() {
                return doubleDraws++ == 0 ? 0.9 : 0.0;
            }
        };
    }

    private FuzzRunner newRunner() {
        final FuzzRunner runner = new FuzzRunner();
        runner.withDb(engine, sqlExecutionContext);
        return runner;
    }

    // The first transaction adds partitions and reads its own txn, so closing the runner's readers of the older txn
    // extends their _txn mapping, then arms the failure. The second reopens the table. The last one reports whether
    // the failure fired before it and disarms it.
    private static ObjList<FuzzTransaction> reopenAfterReadersFallBehind(FailureFileFacade ff, int failAfter, AtomicBoolean isFired) {
        final ObjList<FuzzTransaction> transactions = new ObjList<>();
        final FuzzTransaction growTransaction = new FuzzTransaction();
        // a failure in the commit after the operation runs the transaction again
        final AtomicBoolean isGrown = new AtomicBoolean();
        growTransaction.operationList.add((rnd, cairoEngine, writer, timestampIndex, excludedIntervals) -> {
            if (isGrown.compareAndSet(false, true)) {
                for (int day = 1; day <= 4; day++) {
                    final TableWriter.Row row = writer.newRow(day * Micros.DAY_MICROS);
                    row.putInt(0, day);
                    row.append();
                }
                writer.commit();
                try (TableReader reader = cairoEngine.getReader("t")) {
                    Assert.assertEquals(5, reader.size());
                }
                ff.setToFailAfterOnCurrentThread(failAfter);
            }
            return false;
        });
        transactions.add(growTransaction);
        final FuzzTransaction reopenTransaction = new FuzzTransaction();
        reopenTransaction.reopenTable = true;
        transactions.add(reopenTransaction);
        final FuzzTransaction endTransaction = new FuzzTransaction();
        endTransaction.operationList.add((rnd, cairoEngine, writer, timestampIndex, excludedIntervals) -> {
            isFired.set(ff.getOsCallsCount() <= 0);
            ff.setToFailAfterOnCurrentThread(Integer.MAX_VALUE);
            return false;
        });
        transactions.add(endTransaction);
        return transactions;
    }

    // Counts every open the harness routes through the retry helper, along with the policy each caller
    // picked, and fails the injectedCall-th open with a read timeout. A caller that opens a table
    // resource without the helper never reaches it, so its route assertion fails.
    private static class RetryInjectingFuzzRunner extends FuzzRunner {
        private final AtomicInteger cancellableCalls = new AtomicInteger();
        private final int injectedCall;
        private final AtomicInteger openCalls = new AtomicInteger();
        private final AtomicInteger readTimeoutAttempts = new AtomicInteger();
        private final AtomicInteger recreateTolerantCalls = new AtomicInteger();
        private final String timeoutMessage;

        private RetryInjectingFuzzRunner(int injectedCall, String timeoutMessage) {
            this.injectedCall = injectedCall;
            this.timeoutMessage = timeoutMessage;
        }

        @Override
        public ObjList<FuzzTransaction> generateSet(
                Rnd rnd,
                TableRecordMetadata sequencerMetadata,
                TableMetadata tableMetadata,
                long start,
                long end,
                String tableName
        ) {
            return new ObjList<>();
        }

        @Override
        <T> @Nullable T openWithRetries(
                Supplier<T> open,
                boolean isTableRecreateTolerated,
                @Nullable BooleanSupplier isCancelled,
                long retryTimeoutMillis
        ) {
            int call = openCalls.incrementAndGet();
            if (isTableRecreateTolerated) {
                recreateTolerantCalls.incrementAndGet();
            }
            if (isCancelled != null) {
                cancellableCalls.incrementAndGet();
            }
            if (call == injectedCall) {
                return super.openWithRetries(
                        () -> {
                            if (readTimeoutAttempts.incrementAndGet() == 1) {
                                throw CairoException.critical(0).put(timeoutMessage);
                            }
                            return open.get();
                        },
                        isTableRecreateTolerated,
                        isCancelled,
                        retryTimeoutMillis
                );
            }
            return super.openWithRetries(open, isTableRecreateTolerated, isCancelled, retryTimeoutMillis);
        }
    }
}
