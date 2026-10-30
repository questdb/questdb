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
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzAddCoveringIndexOperation;
import io.questdb.test.fuzz.FuzzDropIndexOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
            // index op and the drains around executeIndexOps() must take nothing more
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

            retryingFuzzer.applyWal(transactions, "drain", 1, new Rnd());

            assertRetryRouting(retryingFuzzer, 4, 4, 0);
            Assert.assertEquals(2, retryingFuzzer.readTimeoutAttempts.get());
        });
    }

    @Test
    public void testGenerateSymbolsUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("symbols");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Column Version read timeout [src=symbols]");

            String[] symbols = retryingFuzzer.generateSymbols(new Rnd(), 3, 4, "symbols");

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
            ObjList<FuzzTransaction> transactions = retryingFuzzer.generateTransactions("generate", new Rnd(), 0, 1);

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
                    failingFuzzer.applyManyWalParallel(transactions, new Rnd(), "late_error", false, true);
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
    public void testPurgePartitionReadersUseRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("purge");
            // No injected timeout on this route: the writers finish at once on an empty transaction list,
            // so the cancellation predicate can legitimately end the wait instead of retrying it.
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(0, "");
            ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            transactions.add(new ObjList<>());

            retryingFuzzer.applyManyWalParallel(transactions, new Rnd(), "purge", false, true);

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

    private FuzzRunner newRunner() {
        final FuzzRunner runner = new FuzzRunner();
        runner.withDb(engine, sqlExecutionContext);
        return runner;
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
