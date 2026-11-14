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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pins the armed window of {@link FuzzRunner#applyWalWithIoFailure}: the failure it plants lands on
 * the WAL apply of the table under test and never on the table writer open. Runs under both commit
 * modes CI runs and with every symbol index type, the default that the fuzz table's indexed symbols
 * take when the runner draws no TYPE clause: both change the calls a writer open makes. Also pins, under
 * nosync, that it commits through the WAL writers it is given, so that a failure can hit each call of
 * an apply of two WAL writers' segments.
 */
@RunWith(Parameterized.class)
public class FuzzWalIoFailureTest extends AbstractFuzzTest {
    private static final long DAY = MicrosTimestampDriver.floor("2022-02-24");
    // initial rows: one per second from 2022-02-24T00:00:00 (FuzzRunner.createInitialTable)
    private static final int INITIAL_ROW_COUNT = 20;
    private static final String[] SYMBOLS = {"a", "b", "c"};
    private final String commitMode;
    private final String symbolIndexType;

    public FuzzWalIoFailureTest(String commitMode, String symbolIndexType) {
        this.commitMode = commitMode;
        this.symbolIndexType = symbolIndexType;
    }

    @Parameterized.Parameters(name = "{0} {1}")
    public static Collection<Object[]> data() {
        final ArrayList<Object[]> data = new ArrayList<>();
        for (String commitMode : new String[]{"nosync", "adaptive"}) {
            for (String symbolIndexType : new String[]{"BITMAP", "POSTING", "POSTING DELTA", "POSTING EF"}) {
                data.add(new Object[]{commitMode, symbolIndexType});
            }
        }
        return data;
    }

    @Test
    public void testFailureOnApplyFromTwoWalWriters() throws Exception {
        // The calls on the two WAL writers' segments do not change with the commit mode, and the fsyncs
        // of adaptive make the sweep of ~240 applies four times slower
        Assume.assumeTrue("the sweep runs under nosync only", "nosync".equals(commitMode));
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, symbolIndexType);
        fuzzer.setFuzzCounts(false, 1, 1, 5, 5, 5, INITIAL_ROW_COUNT, 1, -1, 0);
        assertMemoryLeak(fuzzer.getFileFacade(), () -> {
            final TableToken tableToken = fuzzer.createInitialTableWal("w", INITIAL_ROW_COUNT);
            drainWalQueue();
            fuzzer.createInitialTableNonWal("nw", null);

            // The first apply can make fewer calls than the later ones, so the second one counts the
            // calls to sweep
            applyRowPerWalWriter(tableToken, INITIAL_ROW_COUNT, Integer.MAX_VALUE);
            final long applyCalls = applyRowPerWalWriter(tableToken, INITIAL_ROW_COUNT + 2, Integer.MAX_VALUE);
            Assert.assertEquals("IO failures", 0, fuzzer.getIoFailuresObserved());
            Assert.assertTrue("the apply made no file system call", applyCalls > 0);

            // For each failAfterCalls up to the calls of that apply: the failure fires once, the table
            // resumes and keeps every row, and assertMemoryLeak finds no fd or mapping left behind
            for (int k = 1; k <= applyCalls; k++) {
                applyRowPerWalWriter(tableToken, INITIAL_ROW_COUNT + 2 + 2 * k, k);
                Assert.assertEquals("IO failures after failAfterCalls=" + k, k, fuzzer.getIoFailuresObserved());
                assertQuery("SELECT count() FROM w")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" + (INITIAL_ROW_COUNT + 4 + 2 * k) + "\n");
            }
            assertSqlCursors("nw", "w");
        });
    }

    @Test
    public void testFailureSkipsWriterOpen() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, symbolIndexType);
        fuzzer.setFuzzCounts(false, 1, 1, 5, 5, 5, INITIAL_ROW_COUNT, 1, -1, 0);
        assertMemoryLeak(fuzzer.getFileFacade(), () -> {
            final TableToken tableToken = fuzzer.createInitialTableWal("w", INITIAL_ROW_COUNT);
            drainWalQueue();
            fuzzer.createInitialTableNonWal("nw", null);
            final FailureFileFacade ff = fuzzer.getFileFacade();
            final Rnd rnd = new Rnd();

            // the file system calls of one writer open
            engine.releaseInactive();
            ff.clearFailures();
            ff.setToFailAfter(Integer.MAX_VALUE);
            try (TableWriter ignored = engine.getWriter(tableToken, "count the calls of a writer open")) {
                // close() returns the writer to the pool
            }
            final int writerOpenCalls = Integer.MAX_VALUE - ff.getOsCallsCount();
            ff.clearFailures();
            Assert.assertTrue("writer open made no file system call", writerOpenCalls > 0);

            // drainWalQueue releases the writer at random, so the apply can start without one: the
            // harness opens it before it arms the failure
            final AtomicInteger writerOpenCount = new AtomicInteger();
            final AtomicInteger armedWriterOpenCount = new AtomicInteger();
            final PoolListener previousListener = engine.getPoolListener();
            engine.setPoolListener((factoryType, thread, token, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_CREATE && tableToken.equals(token)) {
                    writerOpenCount.incrementAndGet();
                    if (ff.getOsCallsCount() > 0) {
                        armedWriterOpenCount.incrementAndGet();
                    }
                }
            });
            try {
                engine.releaseInactive();
                applyRow(rnd, INITIAL_ROW_COUNT, Integer.MAX_VALUE);
            } finally {
                engine.setPoolListener(previousListener);
            }
            Assert.assertEquals("writer opens", 1, writerOpenCount.get());
            Assert.assertEquals("writer opens while the failure is armed", 0, armedWriterOpenCount.get());
            Assert.assertEquals("IO failures", 0, fuzzer.getIoFailuresObserved());

            // For each failAfterCalls up to the calls of one writer open, with the writer released
            // first: the failure fires once, the table resumes and keeps every row, and
            // assertMemoryLeak finds no fd or mapping left behind
            for (int k = 1; k <= writerOpenCalls; k++) {
                engine.releaseInactive();
                applyRow(rnd, INITIAL_ROW_COUNT + k, k);
                Assert.assertEquals("IO failures after failAfterCalls=" + k, k, fuzzer.getIoFailuresObserved());
                assertQuery("SELECT count() FROM w")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("count\n" + (INITIAL_ROW_COUNT + 1 + k) + "\n");
            }
            assertSqlCursors("nw", "w");
        });
    }

    // Applies one row, second seconds after 2022-02-24T00:00:00, to the non-WAL oracle and to the WAL
    // table, failing the WAL apply at its failAfterCalls-th file system call.
    private void applyRow(Rnd rnd, int second, int failAfterCalls) {
        final ObjList<FuzzTransaction> transactions = new ObjList<>();
        try {
            final FuzzTransaction transaction = new FuzzTransaction();
            transaction.operationList.add(new FuzzInsertOperation(rnd.nextLong(), rnd.nextLong(), DAY + second * Micros.SECOND_MICROS, 0, 0, 0, 5, SYMBOLS));
            transactions.add(transaction);
            fuzzer.applyNonWal(transactions, "nw", rnd);
            fuzzer.applyWalWithIoFailure(transactions, "w", 1, rnd, failAfterCalls);
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
    }

    // Applies the same two rows, second and second + 1 seconds after 2022-02-24T00:00:00, to the non-WAL
    // oracle and, one row per WAL writer, to the WAL table, failing the WAL apply at its failAfterCalls-th
    // file system call. Asserts that the two WAL writers committed one row each and that the table is
    // not suspended. Returns the calls the armed apply made.
    private long applyRowPerWalWriter(TableToken tableToken, int second, int failAfterCalls) throws Exception {
        final long lastSeqTxn = engine.getTableSequencerAPI().lastTxn(tableToken);
        final ObjList<FuzzTransaction> transactions = new ObjList<>();
        final long calls;
        try {
            // the same rows each time: other values would change the calls an apply makes
            final Rnd rnd = new Rnd();
            for (int i = 0; i < 2; i++) {
                final FuzzTransaction transaction = new FuzzTransaction();
                transaction.operationList.add(new FuzzInsertOperation(rnd.nextLong(), rnd.nextLong(), DAY + (second + i) * Micros.SECOND_MICROS, 0, 0, 0, 5, SYMBOLS));
                transactions.add(transaction);
            }
            fuzzer.applyNonWal(transactions, "nw", rnd);
            // applyToWal picks each transaction's WAL writer with the apply Rnd: with these seeds, the
            // first transaction goes to the second writer and the second one to the first
            calls = fuzzer.applyWalWithIoFailure(transactions, "w", 2, new Rnd(1, 2), failAfterCalls);
        } finally {
            Misc.freeObjListAndClear(transactions);
        }
        Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        assertQuery("SELECT count() commits, count_distinct(walId) wal_writers FROM wal_transactions('w') WHERE sequencerTxn > " + lastSeqTxn)
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("commits\twal_writers\n2\t2\n");
        return calls;
    }
}
