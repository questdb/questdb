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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class FuzzRunnerTest extends AbstractCairoTest {

    private final FuzzRunner fuzzer = new FuzzRunner();

    @Test
    public void testAssertStringColDensityUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("density");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Transaction read timeout [src=density]");

            retryingFuzzer.assertStringColDensity("density");

            assertRetryRouting(retryingFuzzer, 1, 0);
        });
    }

    @Test
    public void testDrainWalQueueUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("drain");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Transaction read timeout [src=drain]");
            ObjList<FuzzTransaction> transactions = new ObjList<>();

            retryingFuzzer.applyWal(transactions, "drain", 1, new Rnd());

            assertRetryRouting(retryingFuzzer, 4, 4);
        });
    }

    @Test
    public void testGenerateSymbolsUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("symbols");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Column Version read timeout [src=symbols]");

            String[] symbols = retryingFuzzer.generateSymbols(new Rnd(), 3, 4, "symbols");

            Assert.assertEquals(3, symbols.length);
            assertRetryRouting(retryingFuzzer, 1, 0);
        });
    }

    @Test
    public void testGenerateTransactionsUsesRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("generate");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Metadata read timeout [src=generate]");
            ObjList<FuzzTransaction> transactions = retryingFuzzer.generateTransactions("generate", new Rnd(), 0, 1);

            try {
                assertRetryRouting(retryingFuzzer, 1, 0);
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testPurgePartitionReadersUseRetries() throws Exception {
        assertMemoryLeak(() -> {
            createWalTable("purge");
            RetryInjectingFuzzRunner retryingFuzzer = newFuzzer(1, "Metadata read timeout [src=purge]");
            ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            transactions.add(new ObjList<>());

            retryingFuzzer.applyManyWalParallel(transactions, new Rnd(), "purge", false, true);

            assertRetryRouting(retryingFuzzer, 2, 2);
        });
    }

    @Test
    public void testReadTimeoutRethrownAfterRetriesExhausted() {
        AtomicInteger attempts = new AtomicInteger();
        try {
            // Transaction, not Metadata: the retry must cover the whole read-timeout family, not just
            // the Metadata variant the pre-broadening code matched.
            fuzzer.openWithRetries(() -> {
                attempts.incrementAndGet();
                throw CairoException.critical(0).put("Transaction read timeout [src=reader]");
            }, false);
            Assert.fail("read timeout should surface once retries are exhausted");
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "read timeout"));
        }
        Assert.assertEquals(11, attempts.get()); // 1 initial attempt + 10 retries
    }

    @Test
    public void testReadTimeoutRetriedThenSucceeds() {
        AtomicInteger attempts = new AtomicInteger();
        String reader = fuzzer.openWithRetries(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw CairoException.critical(0).put("Metadata read timeout [src=metadata]");
            }
            return "reader";
        }, false);
        Assert.assertEquals("reader", reader);
        Assert.assertEquals(3, attempts.get());
    }

    @Test
    public void testTableRecreateNotToleratedFailsFast() {
        AtomicInteger attempts = new AtomicInteger();
        try {
            fuzzer.openWithRetries(() -> {
                attempts.incrementAndGet();
                throw CairoException.critical(0).put("table does not exist [table=x]");
            }, false);
            Assert.fail("a dropped table should surface immediately when recreate is not tolerated");
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "table does not exist"));
        }
        Assert.assertEquals(1, attempts.get()); // no retry: the first attempt rethrows
    }

    @Test
    public void testTableRecreateToleratedIsWaitedOut() {
        AtomicInteger attempts = new AtomicInteger();
        String reader = fuzzer.openWithRetries(() -> {
            int n = attempts.incrementAndGet();
            if (n == 1) {
                throw CairoException.critical(0).put("table name is reserved [table=x]");
            }
            if (n == 2) {
                throw EntryLockedException.instance("locked");
            }
            return "reader";
        }, true);
        Assert.assertEquals("reader", reader);
        Assert.assertEquals(3, attempts.get());
    }

    @Test
    public void testUnrelatedCairoExceptionAlwaysRethrown() {
        AtomicInteger attempts = new AtomicInteger();
        try {
            // Even with recreate tolerated, anything outside the read-timeout / recreate families surfaces at once.
            fuzzer.openWithRetries(() -> {
                attempts.incrementAndGet();
                throw CairoException.critical(0).put("some other failure");
            }, true);
            Assert.fail("an unrelated error should surface immediately");
        } catch (CairoException e) {
            Assert.assertTrue(Chars.contains(e.getFlyweightMessage(), "some other failure"));
        }
        Assert.assertEquals(1, attempts.get());
    }

    private static void assertRetryRouting(RetryInjectingFuzzRunner fuzzer, int expectedOpenCalls, int expectedToleratedRecreateCalls) {
        Assert.assertEquals(expectedOpenCalls, fuzzer.openCalls.get());
        Assert.assertEquals(2, fuzzer.readTimeoutAttempts.get());
        Assert.assertEquals(expectedToleratedRecreateCalls, fuzzer.toleratedRecreateCalls.get());
    }

    private void createWalTable(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (sym SYMBOL, str STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private RetryInjectingFuzzRunner newFuzzer(int injectedCall, String timeoutMessage) {
        RetryInjectingFuzzRunner retryingFuzzer = new RetryInjectingFuzzRunner(injectedCall, timeoutMessage);
        retryingFuzzer.withDb(engine, sqlExecutionContext);
        return retryingFuzzer;
    }

    private static class RetryInjectingFuzzRunner extends FuzzRunner {
        private final int injectedCall;
        private final AtomicInteger openCalls = new AtomicInteger();
        private final AtomicInteger readTimeoutAttempts = new AtomicInteger();
        private final String timeoutMessage;
        private final AtomicInteger toleratedRecreateCalls = new AtomicInteger();

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
        <T> T openWithRetries(Supplier<T> open, boolean tolerateTableRecreate) {
            int call = openCalls.incrementAndGet();
            if (tolerateTableRecreate) {
                toleratedRecreateCalls.incrementAndGet();
            }
            if (call == injectedCall) {
                return super.openWithRetries(() -> {
                    if (readTimeoutAttempts.incrementAndGet() == 1) {
                        throw CairoException.critical(0).put(timeoutMessage);
                    }
                    return open.get();
                }, tolerateTableRecreate);
            }
            return super.openWithRetries(open, tolerateTableRecreate);
        }
    }
}
