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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.fuzz.FuzzDropCreateTableOperation;
import io.questdb.test.fuzz.FuzzDropPartitionOperation;
import io.questdb.test.fuzz.FuzzSetTtlOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.fuzz.FuzzTruncateTableOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MatViewFuzzTest extends AbstractFuzzTest {
    static final String FUZZ_INVALIDATION_REASON = "fuzz invalidation";
    // Two hours of fuzz data that NOT BETWEEN reads and view queries exclude.
    private static final String NOT_BETWEEN_HI = "'2022-02-24T20:00:00.000000Z'";
    private static final String NOT_BETWEEN_LO = "'2022-02-24T18:00:00.000000Z'";
    // The days of testViewAndBaseOperations: 2024-01-01 and the five after it.
    private static final int OPS_DAY_COUNT = 6;
    private static final long OPS_DAY_LO = 1_704_067_200_000_000L;
    // The share of the generic fuzzers' data commits that replace a range of the base, see setFuzzParams().
    private static final double REPLACE_INSERT_PROB = 0.1;
    private static final long SPIN_LOCK_TIMEOUT = 100_000_000;
    // Month starts that follow the end of a year, of February in a common and a leap year, and of a 30-day month.
    private static final ObjList<String> monthStarts = new ObjList<>(
            "2022-01-01T00:00:00.000000Z",
            "2022-03-01T00:00:00.000000Z",
            "2024-03-01T00:00:00.000000Z",
            "2022-05-01T00:00:00.000000Z"
    );
    private static final String[] timestampTypes = new String[]{"timestamp", "timestamp_ns"};
    // None of these time zones changes its UTC offset within two weeks of the monthStarts.
    private static final ObjList<String> timeZones = new ObjList<>("Europe/Berlin", "Asia/Kolkata", "America/Sao_Paulo");

    @Override
    public void setUp() {
        super.setUp();
        // Timer refresh tests mess with the clock, so set the spin timeout
        // to a large value to avoid false positive errors.
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
        spinLockTimeout = SPIN_LOCK_TIMEOUT;
        final Rnd rnd = TestUtils.generateRandom(LOG);
        node1.setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, rnd.nextInt(10000));
    }

    @Test
    public void test2LevelDependencyView() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final String mv2Name = testName.getMethodName() + "_mv2";
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by 1h";
        final String view2Sql = "select min(min) as min, max(max) as max, ts from " + mvName + " sample by 2h";
        final Rnd rnd = generateRandom(LOG);
        testMvFuzz(rnd, tableName, mvName, viewSql, mv2Name, view2Sql);
    }

    @Test
    public void testBaseTableCanHaveColumnsAdded() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0.2, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testFullRefreshAfterUnsupportedOperations() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            fuzzer.setFuzzCounts(
                    rnd.nextBoolean(),
                    rnd.nextInt(2_000_000),
                    rnd.nextInt(1000),
                    rnd.nextInt(3),
                    rnd.nextInt(5),
                    rnd.nextInt(1000),
                    rnd.nextInt(1_000_000),
                    5 + rnd.nextInt(10)
            );

            // Don't rename/drop/change type for existing columns to keep the mat view query valid
            fuzzer.setFuzzProbabilities(
                    0.0,
                    0.0,
                    0.0,
                    0.3,
                    0.3,
                    0.0,
                    0.0,
                    0.0,
                    1,
                    0.0,
                    0.3,
                    0.3,
                    0.0,
                    0.3,
                    0.0,
                    0.0
            );

            setFuzzProperties(rnd);

            final String testTableName = getTestName();
            final int tableCount = 1 + rnd.nextInt(3);

            final AtomicBoolean stop = new AtomicBoolean();
            final ObjList<Thread> refreshJobs = new ObjList<>();

            final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            final ObjList<String> viewSqls = new ObjList<>();

            try {
                refreshJobs.add(startRefreshJob(0, stop, rnd));

                for (int i = 0; i < tableCount; i++) {
                    final String tableNameBase = testTableName + "_" + i;
                    final String tableNameMv = tableNameBase + "_mv";
                    final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableNameBase + " sample by 1h";
                    final ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, tableNameMv, viewSql);
                    fuzzTransactions.add(transactions);
                    viewSqls.add(viewSql);
                }

                // Can help to reduce memory consumption.
                engine.releaseInactive();
                fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
            } finally {
                stopAndJoinJobs(stop, refreshJobs);
            }

            drainWalQueue();
            fuzzer.checkNoSuspendedTables();

            drainWalAndMatViewQueues();
            fuzzer.checkNoSuspendedTables();

            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";

                execute("refresh materialized view '" + mvName + "' full;");
                drainWalAndMatViewQueues();

                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                assertQuery("select count() from materialized_views where view_name = '" + mvName + "' and view_status <> 'invalid';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            viewSql,
                            mvName,
                            LOG
                    );
                }
            }
        });
    }

    @Test
    public void testFuzzParamsGenerateReplaceCommits() throws Exception {
        // The generic fuzzers write replace commits into their views' bases, which can empty base partitions under the
        // views' rows, and assertViewMatchesBase() accepts the rows the views keep there. The seed is fixed, so a
        // failure reproduces.
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd(42, 42);
            setFuzzParams(rnd, 1_000, 1_000, 0, 0);
            fuzzer.createInitialTableWal("base", "timestamp");
            final ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions("base", rnd);
            try {
                final LongList removedRanges = collectRemovedRanges(transactions, new LongList());
                int replaceCommits = 0;
                for (int i = 0, n = transactions.size(); i < n; i++) {
                    if (transactions.getQuick(i).hasReplaceRange()) {
                        replaceCommits++;
                    }
                }
                Assert.assertTrue("no replace commit in " + transactions.size() + " transactions", replaceCommits > 0);
                Assert.assertEquals(2 * replaceCommits, removedRanges.size());
            } finally {
                Misc.freeObjListAndClear(transactions);
            }
        });
    }

    @Test
    public void testInvalidate() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            // TRUNCATE no longer invalidates the view, which keeps the rows the base lost
            setFuzzParams(rnd, 0, 0.5);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testManyTablesPeriodView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runPeriodMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testManyTablesRefreshJobRace() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 1_000, 10_000, 0, 0.0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), false);
        });
    }

    @Test
    public void testManyTablesTimerView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runTimerMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testManyTablesView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), true);
        });
    }

    @Test
    public void testMultipleQueryExecutionsPerRefresh() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTShiftBack() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTShiftForward() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTWithOffset() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin' with offset '00:15'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshSmallSamplingInterval() throws Exception {
        final Rnd rnd = generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int secs = 1 + rnd.nextInt(30);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + secs + "s";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testOneView() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = generateRandom(LOG);
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testOneView_randomSampleByUnit() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = generateRandom(LOG);
        final char[] units = {'y', 'M', 'w', 'd', 'h', 'm'};
        final char unit = units[rnd.nextInt(units.length)];
        final int interval = 1 + rnd.nextInt(3);
        final String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableName + " sample by " + interval + unit + randomAlignment(rnd);
        // The first batch of rows crosses monthStart, and later batches start near it, where the offset or
        // the time zone moves the start of a month or year bucket away from midnight on the 1st.
        final long monthStart = MicrosFormatUtils.parseUTCTimestamp(monthStarts.getQuick(rnd.nextInt(monthStarts.size())));
        final long start = monthStart - 4 * Micros.DAY_MICROS - 7 * Micros.HOUR_MICROS;
        testMvFuzz(rnd, tableName, start, monthStart, mvName, viewSql);
    }

    @Test
    public void testPeriodRefreshConcurrent() throws Exception {
        final TestMicroClock testClock = new TestMicroClock(MicrosTimestampDriver.floor("2000-01-01T00:00:00.000000Z"));
        testMicrosClock = testClock;

        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            execute(
                    "create table base_price (" +
                            "  sym varchar, price long, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            final String viewQuery = "select sym, sum(price) as sum_price, ts from base_price sample by 1m";
            execute("create materialized view price_1h refresh period(length 5m) as " + viewQuery);

            final int iterations = 100;
            final AtomicInteger errorCounter = new AtomicInteger();
            final AtomicBoolean writesFinished = new AtomicBoolean();
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            final Thread writer = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    for (int i = 0; i < iterations; i++) {
                        executionContext.setNowAndFixClock(testClock.micros.get(), ColumnType.TIMESTAMP_MICRO);
                        execute(
                                "insert into base_price values ('gbpusd', 1317, dateadd('m', -3, now()))," +
                                        "('gbpusd', 1318, dateadd('m', -2, now()))," +
                                        "('gbpusd', 1319, dateadd('m', -1, now()))," +
                                        "('gbpusd', 1320, now())," +
                                        "('gbpusd', 1321, dateadd('m', 1, now()))," +
                                        "('gbpusd', 1322, dateadd('m', 2, now()))," +
                                        "('gbpusd', 1323, dateadd('m', 3, now()))",
                                executionContext
                        );
                        drainWalQueue();
                        testClock.micros.addAndGet(rnd.nextInt(10) * Micros.MINUTE_MICROS);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                    writesFinished.set(true);
                }
            });
            writer.start();

            while (!writesFinished.get()) {
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
            }

            writer.join();

            // do a big jump forward in time to make sure that all rows are in complete periods
            testClock.micros.addAndGet(Micros.HOUR_MICROS);
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();

            Assert.assertEquals(0, errorCounter.get());

            final StringSink sinkB = new StringSink();
            printSql(viewQuery, sink);
            printSql("price_1h", sinkB);
            TestUtils.assertEquals(sink, sinkB);
        });
    }

    @Test
    public void testResumeFullAndBaseReplacement() throws Exception {
        // Random base inserts, base replacements (DROP and CREATE under the same name), forced invalidations and
        // restarts of the view states, with RESUME and FULL recovering the view. Oracle: a twin view with the same
        // SQL, rebuilt with FULL at each check, which equals the SQL over the current base. While the view stays
        // valid with no RESUME in between, its incremental refreshes alone must match the twin; after a RESUME,
        // which jumps over the rows the base received while the view was invalid, a RANGE over the base's days
        // must. Every base, the replacements included, has rows in each of the same days, so the partition clip
        // keeps no view row the twin lacks.
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            final String viewSql = "SELECT ts, sym, last(price) AS price, sum(qty) AS qty FROM base SAMPLE BY 1h";
            final long dayLo = MicrosFormatUtils.parseTimestamp("2024-01-01T00:00:00.000000Z");
            final int dayCount = 4;
            createResumeFuzzBase(rnd, dayLo, dayCount);
            execute("CREATE MATERIALIZED VIEW mv AS (" + viewSql + ") PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW twin REFRESH MANUAL DEFERRED AS (" + viewSql + ") PARTITION BY DAY");
            drainWalAndMatViewQueues();

            boolean isGapAccepted = false;
            final int steps = 20 + rnd.nextInt(20);
            for (int step = 0; step < steps; step++) {
                final int op = rnd.nextInt(10);
                if (op < 5) {
                    insertResumeFuzzRows(rnd, dayLo, dayCount, 1 + rnd.nextInt(5));
                } else if (op == 5) {
                    LOG.info().$("fuzz: replacing base table").$();
                    execute("DROP TABLE base");
                    createResumeFuzzBase(rnd, dayLo, dayCount);
                } else if (op == 6) {
                    LOG.info().$("fuzz: restarting view states").$();
                    engine.getMatViewStateStore().clear();
                    engine.hydrateMatViewStateStore();
                } else if (op == 7) {
                    engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), "fuzz invalidation");
                } else if (op == 8) {
                    drainWalQueue();
                }
                if (rnd.nextBoolean()) {
                    drainWalAndMatViewQueues();
                }
                if (step % 5 == 4 || step == steps - 1) {
                    isGapAccepted = checkResumeFuzzViews(rnd, dayLo, dayCount, isGapAccepted);
                }
            }
        });
    }

    @Test
    public void testSelfJoinQuery() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = generateRandom(LOG);
        final int mins = 1 + rnd.nextInt(60);
        final String viewSql = "select first(t2.c2) as first, last(t2.c2) as last, t1.ts from  " + tableName + " t1 asof join " + tableName + " t2 sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testSingleTablePeriodView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runPeriodMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testSingleTableTimerView() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runTimerMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testStressSqlRecompilation() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES, 1);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testStressWalPurgeJob() throws Exception {
        // Here we generate many WAL segments and run WalPurgeJob frequently.
        // The goal is to make sure WalPurgeJob doesn't delete WAL-E files used by MatViewRefreshJob.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 10);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            // Smaller workload on slow CI runners (Mac, Windows): fewer fuzz rows, initial rows, and tables.
            // Segment rollover stays at 10 rows and the purge interval at 10, so the test still
            // generates many WAL segments and runs WalPurgeJob frequently to exercise the race.
            final boolean isLinux = Os.isLinux();
            setFuzzParams(rnd, isLinux ? 10_000 : 3_000, isLinux ? 100_000 : 30_000, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), isLinux ? 4 : 2, true);
        });
    }

    @Test
    public void testUpdateRefreshIntervalsConcurrent() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "10ms");

        final TestMicroClock testClock = new TestMicroClock(parseFloorPartialTimestamp("2000-01-01T00:00:00.000000Z"));
        testMicrosClock = testClock;

        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);

            execute(
                    "create table base_price (" +
                            "  sym varchar, price long, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            final String viewQuery = "select sym, sum(price) as sum_price, ts from base_price sample by 1m";
            execute("create materialized view price_1h refresh manual as " + viewQuery);
            execute("insert into base_price values ('gbpusd', 42, '2000-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            final int iterations = 100;
            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final AtomicBoolean writesFinished = new AtomicBoolean();
            final AtomicInteger errorCounter = new AtomicInteger();
            final MatViewTimerJob timerJob = new MatViewTimerJob(engine);

            final Thread writer = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    startBarrier.await();
                    for (int i = 0; i < iterations; i++) {
                        executionContext.setNowAndFixClock(testClock.micros.get(), ColumnType.TIMESTAMP_MICRO);
                        execute(
                                "insert into base_price values ('gbpusd', 41, dateadd('m', -1, now()))," +
                                        "('gbpusd', 42, now())," +
                                        "('gbpusd', 43, dateadd('m', 1, now()))",
                                executionContext
                        );
                        drainWalQueue();
                        drainMatViewTimerQueue(timerJob);
                        testClock.micros.addAndGet(rnd.nextInt(10) * Micros.MINUTE_MICROS);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                    writesFinished.set(true);
                }
            });
            writer.start();

            final Thread refresher = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    startBarrier.await();
                    for (int i = 0; i < iterations; i++) {
                        execute("refresh materialized view price_1h incremental;", executionContext);
                        drainMatViewQueue(engine);
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                }
            });
            refresher.start();

            while (!writesFinished.get()) {
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues();
            }

            refresher.join();
            writer.join();

            execute("refresh materialized view price_1h incremental;");
            drainMatViewTimerQueue(timerJob);
            drainWalAndMatViewQueues();

            Assert.assertEquals(0, errorCounter.get());

            final StringSink sinkB = new StringSink();
            printSql(viewQuery, sink);
            printSql("price_1h", sinkB);
            TestUtils.assertEquals(sink, sinkB);
        });
    }

    @Test
    public void testViewAndBaseOperations() throws Exception {
        // Ingestion mixed with base partition operations (DROP, DETACH and ATTACH PARTITION), TRUNCATE, UPDATE, TTL,
        // base replacements, view ADD and DROP COLUMN, view DROP PARTITION, ALTER MATERIALIZED VIEW ... AS, RANGE,
        // FULL and RESUME, forced invalidations, backlog updates, WAL purges and restarts of the view states, among
        // them restarts right after a base replacement followed by FULL or RESUME. The WAL apply and the refresh job
        // run at random points in between. The view is IMMEDIATE, so its own refreshes must keep up. Oracle: a twin
        // view with every column the view's SQL can produce, created and fully refreshed at each check, which the view
        // must match outside the base days the fuzzer removed and the view days it dropped, see MatViewFuzzOracle.
        // The budget: questdb.fuzz.matview.rounds independent rounds of questdb.fuzz.matview.steps operations.
        final int rounds = Integer.getInteger("questdb.fuzz.matview.rounds", 1);
        final int steps = Integer.getInteger("questdb.fuzz.matview.steps", -1);
        // short WAL segments, so that the WAL purges between the operations find closed segments to purge
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 4);
        assertMemoryLeak(() -> {
            final Rnd rnd = generateRandom(LOG);
            for (int round = 0; round < rounds; round++) {
                LOG.info().$("fuzz: round ").$(round).$();
                runViewAndBaseOperations(rnd, steps > 0 ? steps : 40 + rnd.nextInt(40));
            }
        });
    }

    @Test
    public void testViewSqlGeneratorEmitsBothShapes() {
        // runMvFuzz must keep generating views with and without their own NOT BETWEEN on the
        // designated timestamp. The seed is fixed, so a failure reproduces.
        final Rnd rnd = new Rnd(42, 42);
        final String plainSql = "select min(c3) as min, max(c3) as max, ts from t sample by 1h";
        final String notBetweenSql = "select min(c3) as min, max(c3) as max, ts from t " +
                "where ts not between '2022-02-24T18:00:00.000000Z' and '2022-02-24T20:00:00.000000Z' sample by 1h";
        int plainCount = 0;
        int notBetweenCount = 0;
        for (int i = 0; i < 100; i++) {
            final String viewSql = generateViewSql(rnd, "t");
            if (plainSql.equals(viewSql)) {
                plainCount++;
            } else if (notBetweenSql.equals(viewSql)) {
                notBetweenCount++;
            } else {
                Assert.fail("unexpected view SQL: " + viewSql);
            }
        }
        Assert.assertTrue("no view without NOT BETWEEN in 100 draws", plainCount > 0);
        Assert.assertTrue("no view with NOT BETWEEN in 100 draws", notBetweenCount > 0);
    }

    // Asserts the view against its twin, rebuilt from the current base with FULL, and returns whether a gap that only
    // a RANGE fills may remain in the view: a RESUME jumps over the base changes made while the view was invalid.
    private static boolean checkResumeFuzzViews(Rnd rnd, long dayLo, int dayCount, boolean isGapAccepted) throws Exception {
        drainResumeFuzzQueues();
        final io.questdb.cairo.mv.MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
        if (state.isInvalid()) {
            if (rnd.nextBoolean()) {
                LOG.info().$("fuzz: resuming view").$();
                execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                isGapAccepted = true;
            } else {
                LOG.info().$("fuzz: rebuilding view with FULL").$();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                isGapAccepted = false;
            }
            drainResumeFuzzQueues();
        }
        Assert.assertFalse(state.isInvalid());
        if (isGapAccepted) {
            final StringSink sink = new StringSink();
            sink.put("REFRESH MATERIALIZED VIEW mv RANGE FROM '");
            MicrosFormatUtils.appendDateTimeUSec(sink, dayLo);
            sink.put("' TO '");
            MicrosFormatUtils.appendDateTimeUSec(sink, dayLo + dayCount * Micros.DAY_MICROS);
            sink.put('\'');
            execute(sink);
            drainResumeFuzzQueues();
        }
        execute("REFRESH MATERIALIZED VIEW twin FULL");
        drainResumeFuzzQueues();
        Assert.assertFalse(state.isInvalid());
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "twin ORDER BY ts, sym", "mv ORDER BY ts, sym", LOG);
        }
        return false;
    }

    // A base with rows in each of its days, and at the first and the last microsecond of its days, so a replacement
    // covers the same days as the base it replaces, and a RANGE over the days, which reads from the base's minimum to
    // its maximum timestamp, recomputes all of them.
    private static void createResumeFuzzBase(Rnd rnd, long dayLo, int dayCount) throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertResumeFuzzRow(rnd, dayLo);
        for (int d = 0; d < dayCount; d++) {
            insertResumeFuzzRow(rnd, dayLo + d * Micros.DAY_MICROS + rnd.nextLong(Micros.DAY_MICROS));
        }
        insertResumeFuzzRow(rnd, dayLo + dayCount * Micros.DAY_MICROS - 1);
    }

    // FULL on an invalid view waits for the view's own WAL, and the WAL apply job redelivers it, so the queues drain
    // in rounds.
    private static void drainResumeFuzzQueues() {
        for (int i = 0; i < 3; i++) {
            drainWalAndMatViewQueues();
        }
    }

    private static void insertResumeFuzzRow(Rnd rnd, long ts) throws SqlException {
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base VALUES ('s").put(rnd.nextInt(3)).put("', ").put(rnd.nextInt(1000)).put(".5, ")
                .put(rnd.nextInt(100)).put(", '");
        MicrosFormatUtils.appendDateTimeUSec(sink, ts);
        sink.put("')");
        execute(sink);
    }

    private static void insertResumeFuzzRows(Rnd rnd, long dayLo, int dayCount, int rowCount) throws SqlException {
        for (int i = 0; i < rowCount; i++) {
            insertResumeFuzzRow(rnd, dayLo + rnd.nextLong(dayCount * Micros.DAY_MICROS));
        }
    }

    private static void appendDay(StringSink sink, int day) {
        sink.put('\'');
        MicrosFormatUtils.appendDateTimeUSec(sink, OPS_DAY_LO + day * Micros.DAY_MICROS);
        sink.put('\'');
    }

    // A view check of testViewAndBaseOperations: drains the queues, recovers an invalid view with RESUME or FULL,
    // backfills with a RANGE when a RESUME SKIP BACKLOG left a gap or a view column became mapped, rebuilds the twin
    // and asserts the view against it.
    private static void checkOperationsFuzzViews(Rnd rnd, MatViewFuzzOpGenerator generator, MatViewFuzzOracle oracle, int step) throws Exception {
        final StringSink context = new StringSink();
        context.put("[step=").put(step).put(", sql=").put(MatViewFuzzOpGenerator.viewSql(generator.getSqlColumns())).put(']');
        drainOperationsFuzzQueues(oracle);
        assertNotSuspended("base");
        assertNotSuspended("mv");
        final MatViewState state = opsViewState("mv");
        if (state.isInvalid()) {
            final String reason = state.getInvalidationReasonForReport();
            // the fuzzer fails no refresh: a view goes invalid only when it is told to or its base is replaced
            final boolean isReplacementReason = "table drop operation".equals(reason)
                    || MatViewRefreshJob.BASE_TABLE_CHANGED_REASON.equals(reason)
                    || "base table is dropped or renamed".equals(reason)
                    // a restart finds the view's refresh position past the replacement base's transactions
                    || "materialized view is ahead of base table and cannot be synchronized".equals(reason);
            if (!FUZZ_INVALIDATION_REASON.equals(reason) && !(isReplacementReason && oracle.isBaseReplacedSinceValid())) {
                Assert.fail("unexpected invalidation reason: " + reason + ' ' + context);
            }
            if (rnd.nextInt(3) == 0) {
                runOperationsFuzzFull(oracle);
            } else {
                if (rnd.nextBoolean()) {
                    // a WAL purge must leave the base WAL that a tracked backlog needs
                    drainPurgeJob();
                }
                runOperationsFuzzResume(oracle, reason, false);
                if (state.isInvalid()) {
                    runOperationsFuzzResume(oracle, reason, true);
                }
                drainOperationsFuzzQueues(oracle);
            }
        }
        Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());
        if (rnd.nextInt(8) == 0) {
            // a FULL refresh rebuilds the view, which the oracle checks bucket for bucket from then on
            runOperationsFuzzFull(oracle);
        }

        // the view's columns and SQL are the fuzzer's
        final ObjList<String> viewColumns = generator.getViewColumns();
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
            Assert.assertEquals(context.toString(), viewColumns.size(), metadata.getColumnCount());
            for (int i = 0, n = viewColumns.size(); i < n; i++) {
                Assert.assertEquals(context.toString(), viewColumns.getQuick(i), metadata.getColumnName(i));
            }
        }
        Assert.assertEquals(context.toString(), MatViewFuzzOpGenerator.viewSql(generator.getSqlColumns()), state.getViewDefinition().getMatViewSql());

        // The twin lives for the check only, so that it holds back no base WAL purge between the checks.
        final ObjList<String> allColumns = new ObjList<>(MatViewFuzzOpGenerator.TS, MatViewFuzzOpGenerator.SYM);
        for (String name : MatViewFuzzOpGenerator.CATALOGUE_NAMES) {
            allColumns.add(name);
        }
        execute("CREATE MATERIALIZED VIEW twin REFRESH MANUAL DEFERRED AS (" + MatViewFuzzOpGenerator.viewSql(allColumns) + ") PARTITION BY DAY");
        execute("REFRESH MATERIALIZED VIEW twin FULL");
        drainOperationsFuzzQueues(oracle);
        Assert.assertFalse(context.toString(), opsViewState("twin").isInvalid());
        final TreeMap<String, MatViewFuzzOracle.Row> twinRows = readOperationsFuzzRows("twin");
        execute("DROP MATERIALIZED VIEW twin");
        final boolean[] baseDays = new boolean[OPS_DAY_COUNT];
        for (MatViewFuzzOracle.Row row : twinRows.values()) {
            baseDays[row.day] = true;
        }

        if (oracle.isGapAccepted() || oracle.isNeedsBackfill() || rnd.nextInt(8) == 0) {
            final StringSink sql = new StringSink();
            sql.put("REFRESH MATERIALIZED VIEW mv RANGE FROM ");
            appendDay(sql, 0);
            sql.put(" TO ");
            appendDay(sql, OPS_DAY_COUNT);
            drainWalQueue();
            execute(sql);
            drainOperationsFuzzQueues(oracle);
            Assert.assertNull(context.toString(), state.getLastRefreshError());
            Assert.assertFalse("view is invalid: " + state.getInvalidationReasonForReport() + ' ' + context, state.isInvalid());
            oracle.onFullRange(baseDays);
        }

        final TreeMap<String, MatViewFuzzOracle.Row> viewRows = readOperationsFuzzRows("mv");
        oracle.assertView(viewRows, twinRows, baseDays, generator.getSqlColumns(), context);
        oracle.onChecked(viewRows, baseDays);
    }

    // Asserts a view against its SQL run on the current base, which a fully refreshed twin view holds: every bucket of
    // the SQL is in the view with the same values, and the view holds no other bucket outside the ranges the base
    // lost. A replace commit that empties a partition, and TRUNCATE, remove base rows while the view keeps its own
    // there ("Known limitations"), so the view may keep a bucket that intersects one of those ranges.
    private static void assertViewMatchesBase(SqlCompiler compiler, String viewSql, String viewName, LongList removedRanges) throws SqlException {
        final TreeMap<Long, String> expected = readRowsByTimestamp(compiler, viewSql);
        final TreeMap<Long, String> actual = readRowsByTimestamp(compiler, viewName);
        for (Map.Entry<Long, String> e : expected.entrySet()) {
            final String actualRow = actual.get(e.getKey());
            if (!e.getValue().equals(actualRow)) {
                Assert.fail("view " + viewName + " differs from its SQL at a bucket the base holds [ts=" + e.getKey()
                        + ", expected=" + e.getValue() + ", actual=" + actualRow + ']');
            }
        }
        final TableToken viewToken = engine.verifyTableName(viewName);
        final int timestampType;
        try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
            timestampType = metadata.getColumnType(metadata.getTimestampIndex());
        }
        final long bucketWidth = ColumnType.getTimestampDriver(timestampType).fromMicros(maxBucketWidthMicros(viewToken));
        for (Map.Entry<Long, String> e : actual.entrySet()) {
            final long ts = e.getKey();
            if (!expected.containsKey(ts) && !intersects(removedRanges, ts, ts + bucketWidth)) {
                Assert.fail("view " + viewName + " holds a bucket that neither the base nor a removed range explains [ts="
                        + ts + ", row=" + e.getValue() + ']');
            }
        }
    }

    // The ranges the transactions may remove from the base: a replace commit removes the rows in its range, and a
    // TRUNCATE all rows.
    private static LongList collectRemovedRanges(ObjList<FuzzTransaction> transactions, LongList sink) {
        for (int i = 0, n = transactions.size(); i < n; i++) {
            final FuzzTransaction transaction = transactions.getQuick(i);
            if (transaction.hasReplaceRange()) {
                sink.add(transaction.getReplaceLoTs(), transaction.getReplaceHiTs());
            }
            for (int j = 0, m = transaction.operationList.size(); j < m; j++) {
                final FuzzTransactionOperation operation = transaction.operationList.getQuick(j);
                if (operation instanceof FuzzTruncateTableOperation
                        || operation instanceof FuzzDropPartitionOperation
                        || operation instanceof FuzzSetTtlOperation
                        || operation instanceof FuzzDropCreateTableOperation) {
                    sink.add(Long.MIN_VALUE, Long.MAX_VALUE);
                }
            }
        }
        return sink;
    }

    private static boolean intersects(LongList ranges, long lo, long hi) {
        for (int i = 0, n = ranges.size(); i < n; i += 2) {
            if (ranges.getQuick(i) < hi && ranges.getQuick(i + 1) >= lo) {
                return true;
            }
        }
        return false;
    }

    // An upper bound of the width of a view's buckets, a DST shift included, in microseconds. A bucket of a view on
    // another view spans the parent's buckets it aggregates.
    private static long maxBucketWidthMicros(TableToken viewToken) {
        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
        Assert.assertNotNull(state);
        final long interval = state.getViewDefinition().getSamplingInterval();
        final long unitMicros = switch (state.getViewDefinition().getSamplingIntervalUnit()) {
            case 'y' -> 366 * Micros.DAY_MICROS;
            case 'M' -> 31 * Micros.DAY_MICROS;
            case 'w' -> Micros.WEEK_MICROS;
            case 'd' -> Micros.DAY_MICROS;
            case 'h' -> Micros.HOUR_MICROS;
            case 'm' -> Micros.MINUTE_MICROS;
            case 's' -> Micros.SECOND_MICROS;
            default -> throw new AssertionError("unexpected sampling interval unit: " + state.getViewDefinition().getSamplingIntervalUnit());
        };
        long width = interval * unitMicros + Micros.HOUR_MICROS;
        final TableToken baseToken = engine.getTableTokenIfExists(state.getViewDefinition().getBaseTableName());
        if (baseToken != null && baseToken.isMatView()) {
            width += maxBucketWidthMicros(baseToken);
        }
        return width;
    }

    // Reads the rows of a query that has one row per designated timestamp value, each row printed whole.
    private static TreeMap<Long, String> readRowsByTimestamp(SqlCompiler compiler, String sql) throws SqlException {
        final TreeMap<Long, String> rows = new TreeMap<>();
        final StringSink sink = new StringSink();
        try (
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final int timestampIndex = metadata.getTimestampIndex();
            Assert.assertTrue(timestampIndex > -1);
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sink.clear();
                CursorPrinter.println(record, metadata, sink);
                Assert.assertNull("two rows at one timestamp in " + sql, rows.put(record.getTimestamp(timestampIndex), sink.toString()));
            }
        }
        return rows;
    }

    private static void assertNotSuspended(String tableName) {
        Assert.assertFalse(tableName + " WAL is suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private static void createOperationsFuzzBase(String timestampType) throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, qty LONG, ts " + timestampType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    // Drains the WAL apply and the refresh job until neither has work: FULL on an invalid view waits for the view's
    // own WAL, which the WAL apply job redelivers.
    private static void drainOperationsFuzzQueues(MatViewFuzzOracle oracle) {
        for (int i = 0; i < 4; i++) {
            drainWalAndMatViewQueues();
            oracle.onDrained(isOperationsFuzzFullPending());
        }
    }

    private static void insertOperationsFuzzRows(MatViewFuzzOpGenerator.Op op, MatViewFuzzOracle oracle) throws SqlException {
        if (op.rowTs.size() == 0) {
            return;
        }
        final StringSink sink = new StringSink();
        sink.put("INSERT INTO base VALUES ");
        for (int i = 0, n = op.rowTs.size(); i < n; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put("('s").put(op.rowSyms.getQuick(i)).put("', ").put(op.rowPrices.getQuick(i)).put(".5, ").put(op.rowQtys.getQuick(i)).put(", '");
            MicrosFormatUtils.appendDateTimeUSec(sink, OPS_DAY_LO + op.rowTs.getQuick(i));
            sink.put("')");
            oracle.onInsert((int) (op.rowTs.getQuick(i) / Micros.DAY_MICROS));
        }
        execute(sink);
    }

    // Whether a view DROP PARTITION would remove the view's newest partition. The view's WAL applies first, as an
    // APPLY_WAL operation would, so the drop finds the partitions read here when it applies.
    private static boolean isDroppingNewestViewPartition(MatViewFuzzOpGenerator.Op op) {
        if (op.kind != MatViewFuzzOpGenerator.Kind.VIEW_DROP_PARTITION_LIST && op.kind != MatViewFuzzOpGenerator.Kind.VIEW_DROP_PARTITION_WHERE) {
            return false;
        }
        drainWalQueue();
        try (TableReader reader = engine.getReader("mv")) {
            final int partitionCount = reader.getPartitionCount();
            if (partitionCount == 0) {
                return false;
            }
            final TimestampDriver driver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            final long newestPartitionMicros = driver.toMicros(reader.getPartitionTimestampByIndex(partitionCount - 1));
            final int newestDay = (int) ((newestPartitionMicros - OPS_DAY_LO) / Micros.DAY_MICROS);
            // LIST drops one day; WHERE ts < day drops every day before it
            return op.kind == MatViewFuzzOpGenerator.Kind.VIEW_DROP_PARTITION_LIST ? newestDay == op.day : newestDay < op.day;
        }
    }

    // Whether the view still holds a FULL refresh: a FULL on an invalid view parks until the view's own WAL applies.
    private static boolean isOperationsFuzzFullPending() {
        return opsViewState("mv").hasPendingFullRefreshOwnerForTesting();
    }

    // Turns a detached partition of the base into one that ATTACH PARTITION takes, and returns whether it detached.
    private static boolean makeOperationsFuzzAttachable(int day) {
        final TableToken baseToken = engine.verifyTableName("base");
        final StringSink partition = new StringSink();
        MicrosFormatUtils.appendDateTimeUSec(partition, OPS_DAY_LO + day * Micros.DAY_MICROS);
        partition.clear(10);
        try (Path path = new Path(); Path other = new Path()) {
            path.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(TableUtils.DETACHED_DIR_MARKER).$();
            if (!Files.exists(path.$())) {
                return false;
            }
            other.of(configuration.getDbRoot()).concat(baseToken).concat(partition).put(configuration.getAttachPartitionSuffix()).$();
            Assert.assertTrue(Files.rename(path.$(), other.$()) > -1);
            return true;
        }
    }

    private static MatViewState opsViewState(String viewName) {
        return engine.getMatViewStateStore().getViewState(engine.verifyTableName(viewName));
    }

    // Reads a view's rows keyed by their bucket and symbol, with every column printed, NULL as "null".
    private static TreeMap<String, MatViewFuzzOracle.Row> readOperationsFuzzRows(String viewName) throws SqlException {
        final TreeMap<String, MatViewFuzzOracle.Row> rows = new TreeMap<>();
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(viewName, sqlExecutionContext).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final RecordMetadata metadata = factory.getMetadata();
            final int tsIndex = metadata.getColumnIndex(MatViewFuzzOpGenerator.TS);
            final int symIndex = metadata.getColumnIndex(MatViewFuzzOpGenerator.SYM);
            final TimestampDriver driver = ColumnType.getTimestampDriver(metadata.getColumnType(tsIndex));
            final long dayLo = driver.fromMicros(OPS_DAY_LO);
            final long dayLength = driver.fromMicros(Micros.DAY_MICROS);
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long ts = record.getTimestamp(tsIndex);
                final CharSequence sym = record.getSymA(symIndex);
                final MatViewFuzzOracle.Row row = new MatViewFuzzOracle.Row((int) Math.floorDiv(ts - dayLo, dayLength));
                Assert.assertTrue("bucket outside the fuzz days: " + ts, row.day > -1 && row.day < OPS_DAY_COUNT);
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    row.values.put(metadata.getColumnName(i), printOperationsFuzzValue(record, metadata.getColumnType(i), i));
                }
                final String key = String.format("%020d|%s", ts, sym);
                Assert.assertNull("two rows for one bucket and symbol: " + key, rows.put(key, row));
            }
        }
        return rows;
    }

    private static String printOperationsFuzzValue(Record record, int columnType, int columnIndex) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.DOUBLE -> {
                final double v = record.getDouble(columnIndex);
                return Numbers.isNull(v) ? "null" : Double.toString(v);
            }
            case ColumnType.LONG -> {
                final long v = record.getLong(columnIndex);
                return v == Numbers.LONG_NULL ? "null" : Long.toString(v);
            }
            case ColumnType.INT -> {
                final int v = record.getInt(columnIndex);
                return v == Numbers.INT_NULL ? "null" : Integer.toString(v);
            }
            case ColumnType.VARCHAR -> {
                final Utf8Sequence v = record.getVarcharA(columnIndex);
                return v == null ? "null" : v.toString();
            }
            case ColumnType.SYMBOL -> {
                final CharSequence v = record.getSymA(columnIndex);
                return v == null ? "null" : v.toString();
            }
            case ColumnType.TIMESTAMP -> {
                return Long.toString(record.getTimestamp(columnIndex));
            }
            default -> throw new AssertionError("unexpected column type: " + ColumnType.nameOf(columnType));
        }
    }

    private static void replaceOperationsFuzzBase(MatViewFuzzOpGenerator.Op op, MatViewFuzzOracle oracle, String timestampType) throws SqlException {
        LOG.info().$("fuzz: replacing base table").$();
        execute("DROP TABLE base");
        // the drop invalidates the views on the base
        oracle.onBaseReplaced();
        oracle.onInvalidationPublished();
        createOperationsFuzzBase(timestampType);
        insertOperationsFuzzRows(op, oracle);
    }

    // A restart of the view states: the store reloads them from disk, and loses the tasks it has not run.
    private static void restartOperationsFuzzViewStates(MatViewFuzzOracle oracle) {
        LOG.info().$("fuzz: restarting view states").$();
        engine.getMatViewStateStore().clear();
        engine.hydrateMatViewStateStore();
        oracle.onRestart();
    }

    private static void runOperationsFuzzFull(MatViewFuzzOracle oracle) throws SqlException {
        LOG.info().$("fuzz: rebuilding view with FULL").$();
        drainWalQueue();
        execute("REFRESH MATERIALIZED VIEW mv FULL");
        oracle.onFullIssued();
        drainOperationsFuzzQueues(oracle);
        final MatViewState state = opsViewState("mv");
        Assert.assertFalse("view is invalid after FULL: " + state.getInvalidationReasonForReport(), state.isInvalid());
        oracle.onFullCompleted();
        oracle.onResumed();
    }

    private static void runOperationsFuzzOp(
            MatViewFuzzOpGenerator generator,
            MatViewFuzzOpGenerator.Op op,
            ObjList<String> sqlColumnsBefore,
            MatViewFuzzOracle oracle,
            String timestampType
    ) throws Exception {
        final StringSink sql = new StringSink();
        switch (op.kind) {
            case INSERT -> insertOperationsFuzzRows(op, oracle);
            case BASE_DROP_PARTITION_LIST -> {
                sql.put("ALTER TABLE base DROP PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onBaseRemoved(op.day);
            }
            case BASE_DROP_PARTITION_WHERE -> {
                sql.put("ALTER TABLE base DROP PARTITION WHERE ts < ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onBaseRemovedBefore(op.day);
            }
            case BASE_DETACH_PARTITION -> {
                sql.put("ALTER TABLE base DETACH PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                // the partition detaches when the WAL applies
                drainWalQueue();
                final boolean isDetached = makeOperationsFuzzAttachable(op.day);
                generator.onDetached(op.day, isDetached);
                if (isDetached) {
                    oracle.onBaseRemoved(op.day);
                }
            }
            case BASE_ATTACH_PARTITION -> {
                sql.put("ALTER TABLE base ATTACH PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onAttach(op.day);
            }
            case BASE_TRUNCATE -> {
                execute("TRUNCATE TABLE base");
                oracle.onBaseRemovedBefore(OPS_DAY_COUNT);
            }
            case BASE_UPDATE -> {
                sql.put("UPDATE base SET price = price + 1.5, qty = qty + 1 WHERE ts >= ");
                appendDay(sql, op.day);
                sql.put(" AND ts < ");
                appendDay(sql, op.day + 1);
                execute(sql);
                oracle.onUpdate(op.day);
            }
            case BASE_SET_TTL -> {
                execute("ALTER TABLE base SET TTL " + op.day + " DAYS");
                oracle.onTtl(op.day);
            }
            case BASE_REPLACE -> replaceOperationsFuzzBase(op, oracle, timestampType);
            case BASE_REPLACE_FULL_RESTART -> {
                // the view goes invalid, FULL rebuilds it from the new base, and the restart comes before the view's
                // WAL applies the rebuild
                replaceOperationsFuzzBase(op, oracle, timestampType);
                drainOperationsFuzzQueues(oracle);
                LOG.info().$("fuzz: FULL after base replacement, then restart").$();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                oracle.onFullIssued();
                drainMatViewQueue(engine);
                oracle.onRefreshJobRun();
                restartOperationsFuzzViewStates(oracle);
            }
            case BASE_REPLACE_RESUME_RESTART -> {
                // the view goes invalid, RESUME SKIP BACKLOG positions it on the new base, and the restart comes before
                // the view's WAL applies its state. A FULL issued before the replacement can rebuild the view from the
                // new base before the drop's invalidation reaches it, and then RESUME catches up on the new base.
                final boolean isFullPending = oracle.isFullPending();
                replaceOperationsFuzzBase(op, oracle, timestampType);
                drainOperationsFuzzQueues(oracle);
                final MatViewState state = opsViewState("mv");
                if (state.isInvalid()) {
                    LOG.info().$("fuzz: RESUME after base replacement, then restart").$();
                    try {
                        execute("REFRESH MATERIALIZED VIEW mv RESUME");
                        Assert.assertTrue("RESUME must refuse the backlog of a replaced base", isFullPending);
                        oracle.onResumed();
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "materialized view backlog is lost");
                        execute("REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG");
                        oracle.onResumed();
                        oracle.onGapAccepted();
                    }
                    Assert.assertFalse(state.isInvalid());
                }
                restartOperationsFuzzViewStates(oracle);
            }
            case VIEW_ADD_COLUMN -> {
                execute("ALTER MATERIALIZED VIEW mv ADD COLUMN " + op.column + ' ' + op.columnType);
                oracle.onViewColumnAdded(op.column, generator.getSqlColumns());
            }
            case VIEW_DROP_COLUMN -> {
                execute("ALTER MATERIALIZED VIEW mv DROP COLUMN " + op.column);
                oracle.onViewColumnDropped(op.column);
            }
            case VIEW_DROP_PARTITION_LIST -> {
                sql.put("ALTER MATERIALIZED VIEW mv DROP PARTITION LIST ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onViewDropped(op.day);
            }
            case VIEW_DROP_PARTITION_WHERE -> {
                sql.put("ALTER MATERIALIZED VIEW mv DROP PARTITION WHERE ts < ");
                appendDay(sql, op.day);
                execute(sql);
                oracle.onViewDroppedBefore(op.day);
            }
            case VIEW_ALTER_AS -> {
                final String viewSql = MatViewFuzzOpGenerator.viewSql(op.sqlColumns);
                LOG.info().$("fuzz: ALTER AS ").$(viewSql).$();
                try {
                    execute("ALTER MATERIALIZED VIEW mv AS (" + viewSql + ')');
                    Assert.assertFalse("ALTER ... AS must refuse a renamed column at its position: " + viewSql, op.isRefused);
                    oracle.onSqlChanged(sqlColumnsBefore, op.sqlColumns);
                } catch (SqlException e) {
                    if (!op.isRefused) {
                        throw e;
                    }
                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized view column does not match the query column at the same position");
                }
            }
            case VIEW_RANGE -> {
                sql.put("REFRESH MATERIALIZED VIEW mv RANGE FROM ");
                appendDay(sql, op.day);
                sql.put(" TO ");
                appendDay(sql, op.dayHi);
                // a REFRESH waits for an ALTER ... AS until the view's WAL applies it
                drainWalQueue();
                try {
                    execute(sql);
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is invalid, run REFRESH MATERIALIZED VIEW mv RESUME first");
                    Assert.assertTrue(opsViewState("mv").isInvalid());
                }
            }
            case VIEW_FULL -> {
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mv FULL");
                oracle.onFullIssued();
            }
            case VIEW_RESUME -> {
                final MatViewState state = opsViewState("mv");
                runOperationsFuzzResume(oracle, state.isInvalid() ? state.getInvalidationReasonForReport() : null, op.isSkipBacklog);
            }
            case VIEW_INVALIDATE -> {
                engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("mv"), FUZZ_INVALIDATION_REASON);
                oracle.onInvalidationPublished();
            }
            case BACKLOG_UPDATE -> engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(engine.verifyTableName("mv"));
            case RESTART -> restartOperationsFuzzViewStates(oracle);
            case APPLY_WAL -> drainWalQueue();
            case RUN_REFRESH_JOB -> {
                drainMatViewQueue(engine);
                oracle.onRefreshJobRun();
            }
            case PURGE_WAL -> drainPurgeJob();
            default -> throw new AssertionError("unexpected operation: " + op.kind);
        }
    }

    // RESUME, or RESUME SKIP BACKLOG, of the view. Plain RESUME catches up on a tracked backlog and refuses a lost one;
    // either refuses while a FULL refresh is pending.
    private static void runOperationsFuzzResume(MatViewFuzzOracle oracle, String invalidationReason, boolean isSkipBacklog) throws SqlException {
        LOG.info().$("fuzz: resuming view [skipBacklog=").$(isSkipBacklog).I$();
        // RESUME waits for the view's WAL
        drainWalQueue();
        final MatViewState state = opsViewState("mv");
        final boolean wasInvalid = state.isInvalid();
        try {
            execute(isSkipBacklog ? "REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG" : "REFRESH MATERIALIZED VIEW mv RESUME");
        } catch (SqlException e) {
            final CharSequence message = e.getFlyweightMessage();
            if (Chars.contains(message, "materialized view backlog is lost")) {
                Assert.assertFalse("RESUME SKIP BACKLOG refused a lost backlog", isSkipBacklog);
                if (oracle.isBacklogExpectedTracked(invalidationReason)) {
                    Assert.fail("RESUME found the backlog of a forced invalidation lost: " + message);
                }
                return;
            }
            TestUtils.assertContains(message, "materialized view full refresh is pending");
            return;
        }
        if (wasInvalid) {
            Assert.assertFalse(state.isInvalid() && !state.isPendingInvalidation());
            oracle.onResumed();
            if (isSkipBacklog) {
                oracle.onGapAccepted();
            }
        }
    }

    private static void runViewAndBaseOperations(Rnd rnd, int steps) throws Exception {
        final String timestampType = rnd.nextBoolean() ? "TIMESTAMP" : "TIMESTAMP_NS";
        final MatViewFuzzOpGenerator generator = new MatViewFuzzOpGenerator(OPS_DAY_COUNT);
        final MatViewFuzzOpGenerator.Op op = new MatViewFuzzOpGenerator.Op();
        createOperationsFuzzBase(timestampType);
        final ObjList<String> sqlColumns = generator.initView(rnd);
        final MatViewFuzzOracle oracle = new MatViewFuzzOracle(OPS_DAY_COUNT, generator.getViewColumns());
        // rows in every day
        for (int day = 0; day < OPS_DAY_COUNT; day++) {
            op.clear();
            for (int i = 0, n = 1 + rnd.nextInt(10); i < n; i++) {
                op.rowTs.add(day * Micros.DAY_MICROS + rnd.nextLong(Micros.DAY_MICROS));
                op.rowSyms.add(rnd.nextInt(3));
                op.rowPrices.add(rnd.nextInt(1000));
                op.rowQtys.add(rnd.nextInt(100));
            }
            insertOperationsFuzzRows(op, oracle);
        }
        LOG.info().$("fuzz: view SQL ").$(MatViewFuzzOpGenerator.viewSql(sqlColumns)).$();
        execute("CREATE MATERIALIZED VIEW mv AS (" + MatViewFuzzOpGenerator.viewSql(sqlColumns) + ") PARTITION BY DAY");
        drainOperationsFuzzQueues(oracle);

        final ObjList<String> sqlColumnsBefore = new ObjList<>();
        boolean isViewColumnAdded = false;
        for (int step = 0; step < steps; step++) {
            sqlColumnsBefore.clear();
            sqlColumnsBefore.addAll(generator.getSqlColumns());
            generator.next(rnd, op);
            LOG.info().$("fuzz: step ").$(step).$(" ").$(op.kind.name()).$(" [day=").$(op.day).$(", column=").$(op.column).I$();
            // Skips the sequence that questdb/questdb#8330 breaks until master fixes it: once a column was added to the
            // view, a DROP PARTITION that removes the view's newest partition can leave the column's "added at"
            // partition stale in the view's _cv file, and the column then reads NULL in the partitions a later refresh
            // creates before it. A drop that empties the view is one such drop. Remove the skip with the fix.
            if (isViewColumnAdded && isDroppingNewestViewPartition(op)) {
                LOG.info().$("fuzz: skipping view DROP PARTITION of the newest view partition, see questdb/questdb#8330").$();
            } else {
                runOperationsFuzzOp(generator, op, sqlColumnsBefore, oracle, timestampType);
            }
            isViewColumnAdded |= op.kind == MatViewFuzzOpGenerator.Kind.VIEW_ADD_COLUMN;
            if (rnd.nextBoolean()) {
                drainWalAndMatViewQueues();
                oracle.onDrained(isOperationsFuzzFullPending());
            }
            if (step % 6 == 5 || step == steps - 1) {
                checkOperationsFuzzViews(rnd, generator, oracle, step);
            }
        }
        execute("DROP MATERIALIZED VIEW mv");
        execute("DROP TABLE base");
        drainWalAndMatViewQueues();
    }

    private static void createMatView(String viewSql, String mvName, boolean deferred) throws SqlException {
        execute("create materialized view " + mvName + " refresh immediate " + (deferred ? "deferred" : "") + " as (" + viewSql + ") partition by DAY");
    }

    private static void createPeriodMatView(String viewSql, String mvName, int interval, char intervalUnit, boolean deferred) throws SqlException {
        execute("create materialized view " + mvName + " refresh immediate " + (deferred ? "deferred" : "") + " period (length " + interval + intervalUnit + ") as (" + viewSql + ") partition by DAY");
    }

    private static void createTimerMatView(String viewSql, String mvName, long start, int interval, char intervalUnit, boolean deferred) throws SqlException {
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, start);
        execute("create materialized view " + mvName + " refresh every " + interval + intervalUnit + " " + (deferred ? "deferred" : "") + " start '" + sink + "' as (" + viewSql + ") partition by DAY");
    }

    // Half of the views exclude two hours of base data with their own NOT BETWEEN on the designated
    // timestamp. Each refresh adds its refresh range to the same where-clause model.
    private static String generateViewSql(Rnd rnd, String tableName) {
        return "select min(c3) as min, max(c3) as max, ts from " + tableName +
                (rnd.nextBoolean() ? " where ts not between " + NOT_BETWEEN_LO + " and " + NOT_BETWEEN_HI : "") +
                " sample by 1h";
    }

    // Returns an empty string, i.e. the default calendar alignment, or ALIGN TO CALENDAR with an optional
    // time zone and an optional offset. With a month or year stride, a positive offset moves the bucket
    // start past midnight on the 1st, and a negative offset moves it to the last day of the previous month,
    // which is shorter than 31 days at the end of February or of a 30-day month.
    private static String randomAlignment(Rnd rnd) {
        if (rnd.nextInt(4) == 0) {
            return "";
        }
        final StringBuilder sb = new StringBuilder(" align to calendar");
        if (rnd.nextInt(3) == 0) {
            sb.append(" time zone '").append(timeZones.getQuick(rnd.nextInt(timeZones.size()))).append('\'');
        }
        if (rnd.nextInt(4) > 0) {
            final int hours = rnd.nextInt(24);
            final int minutes = rnd.nextInt(60);
            sb.append(" with offset '").append(rnd.nextBoolean() ? "-" : "")
                    .append(hours < 10 ? "0" : "").append(hours).append(':')
                    .append(minutes < 10 ? "0" : "").append(minutes).append('\'');
        }
        return sb.toString();
    }

    // NOT BETWEEN reads on the designated timestamp, each on a compiler drawn from the pool that the
    // refresh jobs share, so the refresh compiles land on compilers that have analyzed a negated
    // BETWEEN. An equivalent OR filter checks the count of each read.
    private static void runNotBetweenReads(String tableName) throws SqlException {
        for (int i = 0; i < 30; i++) {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        "select count() from " + tableName + " where ts < " + NOT_BETWEEN_LO + " or ts > " + NOT_BETWEEN_HI,
                        "select count() from " + tableName + " where ts not between " + NOT_BETWEEN_LO + " and " + NOT_BETWEEN_HI,
                        LOG
                );
            }
        }
    }

    private ObjList<FuzzTransaction> createTransactionsAndMv(
            Rnd rnd,
            String tableNameBase,
            String matViewName,
            String viewSql
    ) throws SqlException, NumericException {
        fuzzer.createInitialTableWal(tableNameBase, timestampTypes[rnd.nextInt(10) % 2]);
        runNotBetweenReads(tableNameBase);
        final boolean deferred = rnd.nextBoolean();
        createMatView(viewSql, matViewName, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private ObjList<FuzzTransaction> createTransactionsAndPeriodMv(
            Rnd rnd,
            String tableNameBase,
            String matViewName,
            String viewSql,
            long start,
            long end,
            int interval,
            char intervalUnit
    ) throws SqlException {
        // create empty initial table as we want the timestamps to be in the [start, end] interval
        fuzzer.createInitialTableWal(tableNameBase, 0);
        final boolean deferred = rnd.nextBoolean();
        createPeriodMatView(viewSql, matViewName, interval, intervalUnit, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd, start, end);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private ObjList<FuzzTransaction> createTransactionsAndTimerMv(
            Rnd rnd,
            String tableNameBase,
            String matViewName,
            String viewSql,
            long start,
            int interval,
            char intervalUnit
    ) throws SqlException {
        fuzzer.createInitialTableWal(tableNameBase, timestampTypes[rnd.nextInt(10) % 2]);
        final boolean deferred = rnd.nextBoolean();
        createTimerMatView(viewSql, matViewName, start, interval, intervalUnit, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd, start);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private void runMvFuzz(Rnd rnd, String testTableName, int tableCount, boolean expectValidMatViews) throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();
        final ObjList<LongList> removedRanges = new ObjList<>();

        try {
            for (int i = 0; i < refreshJobCount; i++) {
                refreshJobs.add(startRefreshJob(i, stop, rnd));
            }

            for (int i = 0; i < tableCount; i++) {
                String tableNameBase = testTableName + "_" + i;
                String tableNameMv = tableNameBase + "_mv";
                String viewSql = generateViewSql(rnd, tableNameBase);
                ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, tableNameMv, viewSql);
                fuzzTransactions.add(transactions);
                removedRanges.add(collectRemovedRanges(transactions, new LongList()));
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
        } finally {
            stopAndJoinJobs(stop, refreshJobs);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                // Check that the view exists.
                assertQuery("select count() " +
                        "from materialized_views " +
                        "where view_name = '" + mvName + "';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                if (expectValidMatViews) {
                    assertQuery("select count() " +
                            "from materialized_views " +
                            "where view_name = '" + mvName + "' and view_status <> 'invalid';")
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("""
                                    count
                                    1
                                    """);
                    assertViewMatchesBase(compiler, viewSql, mvName, removedRanges.getQuick(i));
                }
            }
        }
    }

    private void runPeriodMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();
        final ObjList<LongList> removedRanges = new ObjList<>();

        final int length = 1 + rnd.nextInt(10);
        final char[] units = new char[]{'s', 'm', 'h'};
        final char lengthUnit = units[rnd.nextInt(units.length)];
        final long periodLengthMicros = switch (lengthUnit) {
            case 's' -> length * Micros.SECOND_MICROS;
            case 'm' -> length * Micros.MINUTE_MICROS;
            case 'h' -> length * Micros.HOUR_MICROS;
            default -> throw new IllegalStateException("unexpected unit: " + lengthUnit);
        };

        final long start = MicrosTimestampDriver.floor("2022-01-02T03");
        final long end = start + (1 + rnd.nextInt(10)) * periodLengthMicros;
        final long clockJumpLimit = Math.min(end, start + SPIN_LOCK_TIMEOUT);
        currentMicros = start;

        final AtomicBoolean stop = new AtomicBoolean();
        // Timer refresh job must be created after currentMicros is set.
        // We need it here since period mat views with immediate refresh create implicit timers.
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        try {
            for (int i = 0; i < refreshJobCount; i++) {
                refreshJobs.add(startTimerJob(i, stop, rnd, timerJob, periodLengthMicros, clockJumpLimit));
            }

            final int sampleByInterval = 1 + rnd.nextInt(length);
            for (int i = 0; i < tableCount; i++) {
                String tableNameBase = testTableName + "_" + i;
                String tableNameMv = tableNameBase + "_mv";
                String viewSql = "select min(c3) as min, max(c3) as max, ts from " + tableNameBase + " sample by " + sampleByInterval + lengthUnit;
                ObjList<FuzzTransaction> transactions = createTransactionsAndPeriodMv(rnd, tableNameBase, tableNameMv, viewSql, start, end, length, lengthUnit);
                fuzzTransactions.add(transactions);
                removedRanges.add(collectRemovedRanges(transactions, new LongList()));
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
        } finally {
            stopAndJoinJobs(stop, refreshJobs);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        currentMicros = Math.max(end, currentMicros) + periodLengthMicros;
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                assertQuery("select count() " +
                        "from materialized_views " +
                        "where view_name = '" + mvName + "' and view_status <> 'invalid';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                assertViewMatchesBase(compiler, viewSql, mvName, removedRanges.getQuick(i));
            }
        }
    }

    private void runTimerMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();
        final ObjList<LongList> removedRanges = new ObjList<>();

        final int interval = 1 + rnd.nextInt(10);
        final char[] units = new char[]{'m', 'h'};
        final char intervalUnit = units[rnd.nextInt(units.length)];
        final long clockJump = switch (intervalUnit) {
            case 'm' -> interval * Micros.MINUTE_MICROS;
            case 'h' -> interval * Micros.HOUR_MICROS;
            default -> throw new IllegalStateException("unexpected unit: " + intervalUnit);
        };

        final long start = MicrosTimestampDriver.floor("2022-02-24T17");
        currentMicros = start;
        final long clockJumpLimit = start + SPIN_LOCK_TIMEOUT;

        final AtomicBoolean stop = new AtomicBoolean();
        // Timer refresh job must be created after currentMicros is set.
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        try {
            for (int i = 0; i < refreshJobCount; i++) {
                refreshJobs.add(startTimerJob(i, stop, rnd, timerJob, clockJump, clockJumpLimit));
            }

            for (int i = 0; i < tableCount; i++) {
                String tableNameBase = testTableName + "_" + i;
                String tableNameMv = tableNameBase + "_mv";
                String viewSql = "select min(c3) as min, max(c3) as max, ts from  " + tableNameBase + " sample by 1h";
                ObjList<FuzzTransaction> transactions = createTransactionsAndTimerMv(rnd, tableNameBase, tableNameMv, viewSql, start, interval, intervalUnit);
                fuzzTransactions.add(transactions);
                removedRanges.add(collectRemovedRanges(transactions, new LongList()));
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);
        } finally {
            stopAndJoinJobs(stop, refreshJobs);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        currentMicros += clockJump;
        drainMatViewTimerQueue(timerJob);
        drainWalAndMatViewQueues();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewSql = viewSqls.getQuick(i);
                final String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                assertQuery("select count() " +
                        "from materialized_views " +
                        "where view_name = '" + mvName + "' and view_status <> 'invalid';")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                1
                                """);
                assertViewMatchesBase(compiler, viewSql, mvName, removedRanges.getQuick(i));
            }
        }
    }

    private void setFuzzParams(Rnd rnd, double colAddProb, double truncateProb) {
        setFuzzParams(rnd, 10_000, 100_000, colAddProb, truncateProb);
    }

    private void setFuzzParams(Rnd rnd, int transactionCount, int initialRowCount, double colAddProb, double truncateProb) {
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(transactionCount),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(initialRowCount),
                5 + rnd.nextInt(10)
        );

        // Easy, no column manipulations. Replace commits remove every partition they empty, and TRUNCATE
        // removes them all, while the views keep their rows there, so the oracle, assertViewMatchesBase(),
        // accepts view buckets in the ranges they removed. The min..max recompute after missing WAL files
        // runs at random, and so crosses the gaps they leave.
        fuzzer.setFuzzProbabilities(
                0.0,
                0.0,
                0.0,
                0.0,
                colAddProb,
                0.0,
                0.0,
                0.0,
                1,
                0.0,
                0.0,
                truncateProb,
                0.0,
                0.0,
                REPLACE_INSERT_PROB,
                0.0
        );
        setProperty(PropertyKey.DEBUG_MAT_VIEW_REFRESH_MISSING_WAL_FILES_FATAL, String.valueOf(rnd.nextBoolean()));
    }

    private Thread startRefreshJob(int workerId, AtomicBoolean stop, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                refreshJob.run();
                                Os.sleep(rnd.nextInt(50));
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (refreshJob.run());
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("Refresh job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        LOG.info().$("Refresh job stopped").$();
                    }
                }, "refresh-job" + workerId
        );
        th.start();
        return th;
    }

    private Thread startTimerJob(
            int workerId,
            AtomicBoolean stop,
            Rnd outsideRnd,
            MatViewTimerJob timerJob,
            long clockJump,
            long clockJumpLimit
    ) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                drainMatViewTimerQueue(timerJob);
                                refreshJob.run();
                                Os.sleep(rnd.nextInt(10));
                                if (rnd.nextBoolean()) {
                                    // Try to move the clock one jump forward.
                                    long timeBefore = currentMicros;
                                    synchronized (timerJob) {
                                        long timeNow = currentMicros;
                                        if (timeBefore == timeNow && timeNow < clockJumpLimit) {
                                            currentMicros += clockJump;
                                        }
                                    }
                                }
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (refreshJob.run());
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("Refresh job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
                        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                        LOG.info().$("Refresh job stopped").$();
                    }
                }, "refresh-interval-job" + workerId
        );
        th.start();
        return th;
    }

    private void testMvFuzz(Rnd rnd, String baseTableName, String... mvNamesAndSqls) throws Exception {
        long start = MicrosTimestampDriver.floor("2022-02-24T17");
        testMvFuzz(rnd, baseTableName, start, mvNamesAndSqls);
    }

    private void testMvFuzz(Rnd rnd, String baseTableName, long start, String... mvNamesAndSqls) throws Exception {
        testMvFuzz(rnd, baseTableName, start, start, mvNamesAndSqls);
    }

    // The first batch of transactions writes rows over 5 to 14 days from start. Later batches write rows
    // over up to 2 days starting within a day of hotTs, so their incremental refreshes start at various
    // points around hotTs and revisit buckets that an earlier refresh has already written.
    private void testMvFuzz(Rnd rnd, String baseTableName, long start, long hotTs, String... mvNamesAndSqls) throws Exception {
        assertMemoryLeak(() -> {
            fuzzer.createInitialTableWal(baseTableName, timestampTypes[rnd.nextInt(10) % 2]);

            for (int i = 0, n = mvNamesAndSqls.length; i < n; i += 2) {
                final String mvName = mvNamesAndSqls[i];
                final String mvSql = mvNamesAndSqls[i + 1];
                final boolean deferred = rnd.nextBoolean();
                createMatView(mvSql, mvName, deferred);
            }

            AtomicBoolean stop = new AtomicBoolean();
            ObjList<Thread> refreshJobs = new ObjList<>();
            final LongList removedRanges = new LongList();

            try {
                refreshJobs.add(startRefreshJob(0, stop, rnd));

                setFuzzParams(rnd, 0, 0);

                final int batchCount = 2 + rnd.nextInt(4);
                for (int b = 0; b < batchCount; b++) {
                    final ObjList<FuzzTransaction> transactions;
                    if (b == 0) {
                        transactions = fuzzer.generateTransactions(baseTableName, rnd, start);
                    } else {
                        // Let the refresh job catch up, so that the next batch gets refreshes of its own.
                        drainWalAndMatViewQueues();
                        final long lo = hotTs - Micros.DAY_MICROS + rnd.nextLong(2 * Micros.DAY_MICROS);
                        final long hi = lo + Micros.HOUR_MICROS + rnd.nextLong(2 * Micros.DAY_MICROS);
                        transactions = fuzzer.generateTransactions(baseTableName, rnd, lo, hi);
                    }
                    collectRemovedRanges(transactions, removedRanges);
                    ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
                    fuzzTransactions.add(transactions);
                    fuzzer.applyManyWalParallel(
                            fuzzTransactions,
                            rnd,
                            baseTableName,
                            false,
                            true
                    );
                }
            } finally {
                stopAndJoinJobs(stop, refreshJobs);
            }

            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0, n = mvNamesAndSqls.length; i < n; i += 2) {
                    final String mvName = mvNamesAndSqls[i];
                    final String mvSql = mvNamesAndSqls[i + 1];
                    LOG.info().$("asserting view ").$(mvName).$(" against ").$(mvSql).$();
                    // A failed incremental refresh invalidates the view, e.g. on a row outside the replace range.
                    assertQuery("select view_name, invalidation_reason from materialized_views where view_name = '" + mvName + "' and view_status = 'invalid';")
                            .noLeakCheck()
                            .noRandomAccess()
                            .returns("view_name\tinvalidation_reason\n");
                    assertViewMatchesBase(compiler, mvSql, mvName, removedRanges);
                }
            }
        });
    }
}
