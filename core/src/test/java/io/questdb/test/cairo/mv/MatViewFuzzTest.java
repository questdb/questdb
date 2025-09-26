/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MatViewFuzzTest extends AbstractFuzzTest {
    private static final int SPIN_LOCK_TIMEOUT = 100_000_000;
    private static final String[] timestampTypes = new String[]{"timestamp", "timestamp_ns"};

    @Test
    public void test2LevelDependencyView() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final String mv2Name = testName.getMethodName() + "_mv2";
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by 1h";
        final String view2Sql = "select min(min), max(max), ts from " + mvName + " sample by 2h";
        final Rnd rnd = fuzzer.generateRandom(LOG);
        testMvFuzz(rnd, tableName, mvName, viewSql, mv2Name, view2Sql);
    }

    @Test
    public void testBaseTableCanHaveColumnsAdded() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0.2, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testFullRefreshAfterUnsupportedOperations() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = fuzzer.generateRandom(LOG);

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
            final Thread refreshJobThread = startRefreshJob(0, stop, rnd);

            final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            final ObjList<String> viewSqls = new ObjList<>();

            for (int i = 0; i < tableCount; i++) {
                final String tableNameBase = testTableName + "_" + i;
                final String tableNameMv = tableNameBase + "_mv";
                final String viewSql = "select min(c3), max(c3), ts from " + tableNameBase + " sample by 1h";
                final ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, tableNameMv, viewSql);
                fuzzTransactions.add(transactions);
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);

            stop.set(true);
            refreshJobThread.join();

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
                assertSql(
                        "count\n" +
                                "1\n",
                        "select count() from materialized_views where view_name = '" + mvName + "' and view_status <> 'invalid';"
                );
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
    public void testInvalidate() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            // truncate will lead to mat view invalidation
            setFuzzParams(rnd, 0, 0.5);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1, false, false);
        });
    }

    @Test
    public void testManyTablesPeriodView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            // Timer refresh tests mess with the clock, so set the spin timeout
            // to a large value to avoid false positive errors.
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
            spinLockTimeout = 10_000_000;
            runPeriodMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testManyTablesRefreshJobRace() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 2_000, 1_000, 0, 0.0);
            setFuzzProperties(rnd);
            // use sleep(1) to make sure that the view is not refreshed too quickly
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), false, true);
        });
    }

    @Test
    public void testManyTablesTimerView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            // Timer refresh tests mess with the clock, so set the spin timeout
            // to a large value to avoid false positive errors.
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
            spinLockTimeout = 10_000_000;
            runTimerMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testManyTablesView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testMultipleQueryExecutionsPerRefresh() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTShiftBack() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2020-10-23T20:30:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTShiftForward() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshDSTWithOffset() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + mins + "m " +
                "align to calendar time zone 'Europe/Berlin' with offset '00:15'";
        long start = MicrosFormatUtils.parseUTCTimestamp("2021-03-28T00:59:00.000000Z");
        testMvFuzz(rnd, tableName, start, mvName, viewSql);
    }

    @Test
    public void testMultipleQueryExecutionsPerRefreshSmallSamplingInterval() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int rowsPerQuery = Math.max(100, rnd.nextInt(10_000));
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE, rowsPerQuery);
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final int secs = 1 + rnd.nextInt(30);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + secs + "s";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testOneView() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int mins = 1 + rnd.nextInt(300);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testOneView_randomSampleByUnit() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final char[] units = {'y', 'M', 'w', 'd', 'h', 'm'};
        final char unit = units[rnd.nextInt(units.length)];
        final int interval = 1 + rnd.nextInt(3);
        final String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by " + interval + unit;
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testPeriodRefreshConcurrent() throws Exception {
        // Timer refresh tests mess with the clock, so set the spin timeout
        // to a large value to avoid false positive errors.
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
        spinLockTimeout = 100_000_000;

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
    public void testSelfJoinQuery() throws Exception {
        final String tableName = testName.getMethodName();
        final String mvName = testName.getMethodName() + "_mv";
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final int mins = 1 + rnd.nextInt(60);
        final String viewSql = "select first(t2.c2), last(t2.c2), t1.ts from  " + tableName + " t1 asof join " + tableName + " t2 sample by " + mins + "m";
        testMvFuzz(rnd, tableName, mvName, viewSql);
    }

    @Test
    public void testSingleTablePeriodView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            // Timer refresh tests mess with the clock, so set the spin timeout
            // to a large value to avoid false positive errors.
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
            spinLockTimeout = SPIN_LOCK_TIMEOUT;
            runPeriodMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testSingleTableTimerView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            // Timer refresh tests mess with the clock, so set the spin timeout
            // to a large value to avoid false positive errors.
            node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
            spinLockTimeout = SPIN_LOCK_TIMEOUT;
            runTimerMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testStressSqlRecompilation() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES, 1);
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testStressWalPurgeJob() throws Exception {
        // Here we generate many WAL segments and run WalPurgeJob frequently.
        // The goal is to make sure WalPurgeJob doesn't delete WAL-E files used by MatViewRefreshJob.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10);
        setProperty(PropertyKey.CAIRO_WAL_PURGE_INTERVAL, 10);
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 4);
        });
    }

    @Test
    public void testUpdateRefreshIntervalsConcurrent() throws Exception {
        // Timer refresh tests mess with the clock, so set the spin timeout
        // to a large value to avoid false positive errors.
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, SPIN_LOCK_TIMEOUT);
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD, "10ms");
        spinLockTimeout = 100_000_000;

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

    private static void createMatView(String viewSql, String mvName, boolean deferred) throws SqlException {
        execute("create materialized view " + mvName + " refresh immediate " + (deferred ? "deferred" : "") + " as (" + viewSql + ") partition by DAY");
    }

    private static void createTimerMatView(String viewSql, String mvName, long start, int interval, char intervalUnit, boolean deferred) throws SqlException {
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, start);
        execute("create materialized view " + mvName + " refresh every " + interval + intervalUnit + " " + (deferred ? "deferred" : "") + " start '" + sink + "' as (" + viewSql + ") partition by DAY");
    }

    private ObjList<FuzzTransaction> createTransactionsAndMv(Rnd rnd, String tableNameBase, String matViewName, String viewSql) throws SqlException, NumericException {
        fuzzer.createInitialTableWal(tableNameBase, timestampTypes[rnd.nextInt(10) % 2]);
        final boolean deferred = rnd.nextBoolean();
        createMatView(viewSql, matViewName, deferred);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd);

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

    private void runMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        runMvFuzz(rnd, testTableName, tableCount, true, false);
    }

    private void runMvFuzz(Rnd rnd, String testTableName, int tableCount, boolean expectValidMatViews, boolean sleep) throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.add(startRefreshJob(i, stop, rnd));
        }

        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();

        for (int i = 0; i < tableCount; i++) {
            String tableNameBase = testTableName + "_" + i;
            String tableNameMv = tableNameBase + "_mv";
            String viewSql = "select min(c3), max(c3), ts from  " + tableNameBase + (sleep ? " where sleep(1)" : "") + " sample by 1h";
            ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, tableNameMv, viewSql);
            fuzzTransactions.add(transactions);
            viewSqls.add(viewSql);
        }

        // Can help to reduce memory consumption.
        engine.releaseInactive();
        fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);

        stop.set(true);
        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.getQuick(i).join();
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
                assertSql(
                        "count\n" +
                                "1\n",
                        "select count() " +
                                "from materialized_views " +
                                "where view_name = '" + mvName + "';"
                );
                if (expectValidMatViews) {
                    assertSql(
                            "count\n" +
                                    "1\n",
                            "select count() " +
                                    "from materialized_views " +
                                    "where view_name = '" + mvName + "' and view_status <> 'invalid';"
                    );
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            viewSql,
                            mvName,
                            LOG
                    );
                }
            }
        }
    }

    private void runPeriodMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();

        final int length = 1 + rnd.nextInt(24);
        final char[] units = new char[]{'m', 'h'};
        final char lengthUnit = units[rnd.nextInt(units.length)];
        final long clockJump;
        switch (lengthUnit) {
            case 'm':
                clockJump = length * Micros.MINUTE_MICROS;
                break;
            case 'h':
                clockJump = length * Micros.HOUR_MICROS;
                break;
            default:
                throw new IllegalStateException("unexpected unit: " + lengthUnit);
        }

        final long start = MicrosTimestampDriver.floor("2022-01-02T03");
        currentMicros = start;
        final long clockJumpLimit = start + (SPIN_LOCK_TIMEOUT / clockJump);

        final AtomicBoolean stop = new AtomicBoolean();
        // Timer refresh job must be created after currentMicros is set.
        // We need it here since period mat views with immediate refresh create implicit timers.
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.add(startTimerJob(i, stop, rnd, timerJob, clockJump, clockJumpLimit));
        }

        for (int i = 0; i < tableCount; i++) {
            String tableNameBase = testTableName + "_" + i;
            String tableNameMv = tableNameBase + "_mv";
            String viewSql = "select min(c3), max(c3), ts from  " + tableNameBase + " sample by 1h";
            ObjList<FuzzTransaction> transactions = createTransactionsAndTimerMv(rnd, tableNameBase, tableNameMv, viewSql, start, length, lengthUnit);
            fuzzTransactions.add(transactions);
            viewSqls.add(viewSql);
        }

        // Can help to reduce memory consumption.
        engine.releaseInactive();
        fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);

        stop.set(true);
        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.getQuick(i).join();
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
                assertSql(
                        "count\n" +
                                "1\n",
                        "select count() " +
                                "from materialized_views " +
                                "where view_name = '" + mvName + "' and view_status <> 'invalid';"
                );
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        mvName,
                        LOG
                );
            }
        }
    }

    private void runTimerMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();

        final int interval = 1 + rnd.nextInt(10);
        final char[] units = new char[]{'m', 'h'};
        final char intervalUnit = units[rnd.nextInt(units.length)];
        final long clockJump;
        switch (intervalUnit) {
            case 'm':
                clockJump = interval * Micros.MINUTE_MICROS;
                break;
            case 'h':
                clockJump = interval * Micros.HOUR_MICROS;
                break;
            default:
                throw new IllegalStateException("unexpected unit: " + intervalUnit);
        }

        final long start = MicrosTimestampDriver.floor("2022-02-24T17");
        currentMicros = start;
        final long clockJumpLimit = start + (SPIN_LOCK_TIMEOUT / clockJump);

        final AtomicBoolean stop = new AtomicBoolean();
        // Timer refresh job must be created after currentMicros is set.
        final MatViewTimerJob timerJob = new MatViewTimerJob(engine);
        final ObjList<Thread> refreshJobs = new ObjList<>();
        final int refreshJobCount = 1 + rnd.nextInt(4);

        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.add(startTimerJob(i, stop, rnd, timerJob, clockJump, clockJumpLimit));
        }

        for (int i = 0; i < tableCount; i++) {
            String tableNameBase = testTableName + "_" + i;
            String tableNameMv = tableNameBase + "_mv";
            String viewSql = "select min(c3), max(c3), ts from  " + tableNameBase + " sample by 1h";
            ObjList<FuzzTransaction> transactions = createTransactionsAndTimerMv(rnd, tableNameBase, tableNameMv, viewSql, start, interval, intervalUnit);
            fuzzTransactions.add(transactions);
            viewSqls.add(viewSql);
        }

        // Can help to reduce memory consumption.
        engine.releaseInactive();
        fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);

        stop.set(true);
        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.getQuick(i).join();
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
                assertSql(
                        "count\n" +
                                "1\n",
                        "select count() " +
                                "from materialized_views " +
                                "where view_name = '" + mvName + "' and view_status <> 'invalid';"
                );
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        mvName,
                        LOG
                );
            }
        }
    }

    private void setFuzzParams(Rnd rnd, double colAddProb, double truncateProb) {
        setFuzzParams(rnd, 2_000_000, 1_000_000, colAddProb, truncateProb);
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

        // Easy, no column manipulations
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
                0.1,
                0.0
        );
    }

    private Thread startRefreshJob(int workerId, AtomicBoolean stop, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                refreshJob.run(workerId);
                                Os.sleep(rnd.nextInt(50));
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (refreshJob.run(workerId));
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("Refresh job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
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
                                refreshJob.run(workerId);
                                Os.sleep(rnd.nextInt(50));
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
                                } while (refreshJob.run(workerId));
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("Refresh job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
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
        assertMemoryLeak(() -> {
            fuzzer.createInitialTableWal(baseTableName, timestampTypes[rnd.nextInt(10) % 2]);

            for (int i = 0, n = mvNamesAndSqls.length / 2; i < n; i += 2) {
                final String mvName = mvNamesAndSqls[i];
                final String mvSql = mvNamesAndSqls[i + 1];
                final boolean deferred = rnd.nextBoolean();
                createMatView(mvSql, mvName, deferred);
            }

            AtomicBoolean stop = new AtomicBoolean();
            Thread refreshJob = startRefreshJob(0, stop, rnd);

            setFuzzParams(rnd, 0, 0);

            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(baseTableName, rnd, start);
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            fuzzTransactions.add(transactions);
            fuzzer.applyManyWalParallel(
                    fuzzTransactions,
                    rnd,
                    baseTableName,
                    false,
                    true
            );

            stop.set(true);
            refreshJob.join();
            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0, n = mvNamesAndSqls.length / 2; i < n; i += 2) {
                    final String mvName = mvNamesAndSqls[i];
                    final String mvSql = mvNamesAndSqls[i + 1];
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            mvSql,
                            mvName,
                            LOG
                    );
                }
            }
        });
    }
}
