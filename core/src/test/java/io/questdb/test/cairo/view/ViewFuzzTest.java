/*+*****************************************************************************
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

package io.questdb.test.cairo.view;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.view.ViewCompilerTask;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.sql.RandomSelectGenerator;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

public class ViewFuzzTest extends AbstractFuzzTest {
    // a PIVOT over tp (k SYMBOL, x INT), whose compile fails while tp is empty
    private static final String TP_PIVOT_SQL = "SELECT * FROM tp PIVOT (sum(x) FOR k IN (SELECT DISTINCT k FROM tp ORDER BY k))";
    private static final String VP_STATUS_SQL = "SELECT view_status, invalidation_reason, view_status_update_time FROM views() WHERE view_name = 'vp'";
    private static final String[] timestampTypes = new String[]{"timestamp", "timestamp_ns"};

    @Test
    public void testSingleView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd);
            setFuzzProperties(rnd);
            runViewFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testManyIndependentViews() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd);
            setFuzzProperties(rnd);
            runViewFuzz(rnd, getTestName(), 2 + rnd.nextInt(4));
        });
    }

    @Test
    public void testMultipleLevelDependencyViewsWithRandomSelect() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final RandomSelectGenerator selectGenerator = new RandomSelectGenerator(engine, rnd);

        final String tableName = testName.getMethodName();
        final String view1Name = testName.getMethodName() + "_v1";
        final String view2Name = testName.getMethodName() + "_v2";
        final String view3Name = testName.getMethodName() + "_v3";
        final String view4Name = testName.getMethodName() + "_v4";
        testViewFuzz(selectGenerator, rnd, tableName, view1Name, view2Name, view3Name, view4Name);
    }

    @Test
    public void testNonWalDdlWithConcurrentViewCompiles() throws Exception {
        // An ALTER of a non-WAL table queues the recompile of the views over it, and a TRUNCATE,
        // RENAME or DROP of the table follows at once while view compile workers run. Every DDL
        // succeeds: it waits for a compile run that holds the table instead of failing as busy.
        // At times a query holds the table on another thread during the DDL: the DDL then fails
        // with its busy error at once, as it does without views, without waiting for the runs,
        // and succeeds when it runs again once the query ended. A compile that meets the lock of a
        // DDL leaves the views alone and retries once the busy retry timeout passed, so every view
        // ends valid, with the columns of its SQL. TRUNCATE queues no compile of its own, so the
        // PIVOT view over tp, which nothing ALTERs, keeps the status of its CREATE through the
        // TRUNCATEs of tp. One session runs all DDLs; ViewOverNonWalTableTest pins what a DDL does
        // when another session changes its table while it waits.
        assertMemoryLeak(() -> {
            final Rnd rnd = fuzzer.generateRandom(LOG);
            // Far above any wait for a compile run here, so that every DDL succeeds on a slow
            // machine too, but finite: a run that never ends, such as the run of a job that died
            // inside it, fails the DDL that waits for it, and the job's failure then fails the
            // test, instead of the DDL waiting until the JUnit timeout.
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 60_000);
            // a busy retry at the next compile run, or at the server default
            final long busyRetryTimeoutMillis = rnd.nextBoolean() ? 0 : 1_000;
            LOG.info().$("view compile busy retry timeout [millis=").$(busyRetryTimeoutMillis).I$();
            node1.setProperty(PropertyKey.CAIRO_VIEW_COMPILE_BUSY_RETRY_TIMEOUT, busyRetryTimeoutMillis);
            final int tableCount = 1 + rnd.nextInt(3);
            final boolean[] isPartitioned = new boolean[tableCount];
            final ObjList<String> viewNames = new ObjList<>();
            final ObjList<String> viewSqls = new ObjList<>();
            for (int i = 0; i < tableCount; i++) {
                isPartitioned[i] = rnd.nextBoolean();
                createNonWalTable("t" + i, isPartitioned[i]);
            }
            for (int i = 0; i < tableCount; i++) {
                final String tableName = "t" + i;
                for (int j = 0, n = 1 + rnd.nextInt(4); j < n; j++) {
                    final String viewSql = switch (j) {
                        case 0 -> "SELECT * FROM " + tableName;
                        case 1 -> "SELECT a FROM " + tableName + " WHERE a > 1";
                        // a view over a view
                        case 2 -> "SELECT * FROM " + tableName + "_v0";
                        default -> isPartitioned[i]
                                ? "SELECT name, numRows FROM table_partitions('" + tableName + "')"
                                : "SELECT a, count() c FROM " + tableName + " ORDER BY a";
                    };
                    createViewAndCompile(tableName + "_v" + j, viewSql, viewNames, viewSqls);
                }
            }
            if (tableCount > 1) {
                createViewAndCompile("join_v", "SELECT x.a, y.a a2 FROM t0 x JOIN t1 y ON x.a = y.a", viewNames, viewSqls);
            }
            execute("CREATE TABLE tp (k SYMBOL, x INT)");
            insertPivotRows();
            createViewAndCompile("vp", TP_PIVOT_SQL, viewNames, viewSqls);
            drainViewQueue();
            final StringSink vpCreateStatus = new StringSink();
            printSql(VP_STATUS_SQL, vpCreateStatus);

            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> viewCompilerJobError = new AtomicReference<>();
            final ObjList<Thread> viewCompilerJobs = new ObjList<>();
            // TRUNCATE, RENAME, DROP, TRUNCATE KEEP SYMBOL MAPS and, with two tables or more,
            // TRUNCATE of two tables
            final int ddlShapeCount = tableCount > 1 ? 5 : 4;
            final int[] ddlCounts = new int[ddlShapeCount];
            // the DDLs that ran while a query held their table
            final int[] queryHeldDdlCounts = new int[ddlShapeCount];
            final int ddlOffset = rnd.nextInt(ddlShapeCount);
            // TRUNCATE of tp alone, and as the second table of a TRUNCATE of two tables
            final int[] tpTruncateCounts = new int[2];
            int addedColumnCount = 0;
            final LogCapture capture = new LogCapture();
            try {
                for (int i = 0, n = 1 + rnd.nextInt(4); i < n; i++) {
                    // short naps, so that a worker often compiles while the DDL locks the table
                    viewCompilerJobs.add(startViewCompilerJob(i, stop, viewCompilerJobError, rnd, 3));
                }
                for (int i = 0, n = 20 + rnd.nextInt(20); i < n; i++) {
                    final int tableIndex = rnd.nextInt(tableCount);
                    final String tableName = "t" + tableIndex;
                    execute("ALTER TABLE " + tableName + " ADD COLUMN c" + addedColumnCount++ + " INT");
                    // the first iterations run each DDL once, then once more while a query holds the table
                    final int ddl = i < 2 * ddlShapeCount ? (i + ddlOffset) % ddlShapeCount : rnd.nextInt(ddlShapeCount);
                    final boolean isQueryHolding = i < 2 * ddlShapeCount ? i >= ddlShapeCount : rnd.nextInt(4) == 0;
                    if (isQueryHolding) {
                        queryHeldDdlCounts[ddl]++;
                        assertDdlFailsAtOnceWhileQueryHoldsTable(capture, ddl, tableName);
                    }
                    ddlCounts[ddl]++;
                    switch (ddl) {
                        case 0 -> execute("TRUNCATE TABLE " + tableName);
                        case 1 -> {
                            execute("RENAME TABLE " + tableName + " TO " + tableName + "_tmp");
                            execute("RENAME TABLE " + tableName + "_tmp TO " + tableName);
                        }
                        case 2 -> {
                            execute("DROP TABLE " + tableName);
                            createNonWalTable(tableName, isPartitioned[tableIndex]);
                        }
                        case 3 -> execute("TRUNCATE TABLE " + tableName + " KEEP SYMBOL MAPS");
                        default -> {
                            final int otherTableIndex = (tableIndex + 1 + rnd.nextInt(tableCount - 1)) % tableCount;
                            execute("TRUNCATE TABLE " + tableName + ", t" + otherTableIndex);
                        }
                    }
                    insertRows(tableName, isPartitioned[tableIndex]);
                    // the first iterations truncate tp in each way, then tp is truncated at random
                    final int tpTruncate = i < 2 ? i : rnd.nextInt(3);
                    if (tpTruncate < 2) {
                        if (tpTruncate == 0) {
                            execute("TRUNCATE TABLE tp");
                        } else {
                            execute("TRUNCATE TABLE " + tableName + ", tp");
                            insertRows(tableName, isPartitioned[tableIndex]);
                        }
                        tpTruncateCounts[tpTruncate]++;
                        insertPivotRows();
                    }
                }
            } finally {
                capture.stop();
                stopAndJoinJobs(stop, viewCompilerJobs);
                // in the finally, so that a job's failure replaces the error it caused here, such
                // as the busy error of a DDL that waited for the run of the dead job
                TestUtils.rethrowFirst(viewCompilerJobError);
            }
            for (int ddl = 0; ddl < ddlShapeCount; ddl++) {
                Assert.assertTrue("DDL " + ddl + " did not run", ddlCounts[ddl] > 0);
                Assert.assertTrue("DDL " + ddl + " did not run while a query held its table", queryHeldDdlCounts[ddl] > 0);
            }
            for (int tpTruncate = 0; tpTruncate < 2; tpTruncate++) {
                Assert.assertTrue("TRUNCATE " + tpTruncate + " of tp did not run", tpTruncateCounts[tpTruncate] > 0);
            }

            try {
                // the busy retries of the compiles that met a lock are due, and the compiles that
                // they and the DDLs queued leave every view valid, with the columns of its SQL
                setCurrentMicros(MicrosecondClockImpl.INSTANCE.getTicks() + busyRetryTimeoutMillis * 1_000 + 1);
                drainViewQueue();
                AbstractViewTest.assertPendingCompileBusyRetries("");
                // nothing compiled vp since its CREATE
                assertQuery(VP_STATUS_SQL)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize(false)
                        .returns(vpCreateStatus);
                assertViewsValidAndMatchSql(viewNames, viewSqls);

                // one more compile of each view, with no DDL running, keeps them so
                for (int i = 0, n = viewNames.size(); i < n; i++) {
                    engine.enqueueCompileView(engine.verifyTableName(viewNames.getQuick(i)));
                }
                drainViewQueue();
                assertViewsValidAndMatchSql(viewNames, viewSqls);
            } finally {
                // the other tests of the class run on the real clock
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testSingleViewWithRandomSelect() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final RandomSelectGenerator selectGenerator = new RandomSelectGenerator(engine, rnd);

        final String tableName = testName.getMethodName();
        final String viewName = testName.getMethodName() + "_v";
        testViewFuzz(selectGenerator, rnd, tableName, viewName);
    }

    /**
     * Runs the first statement of the DDL shape of testNonWalDdlWithConcurrentViewCompiles() while a
     * query holds the table's reader on another thread. The statement fails with its busy error at
     * once, as it does without views: it logs no wait for the view compile runs that the workers
     * run meanwhile. DROP and RENAME lock the table's pooled metadata before its readers, so they
     * fail on the metadata instead when a worker's compile holds it at that moment.
     */
    private static void assertDdlFailsAtOnceWhileQueryHoldsTable(LogCapture capture, int ddl, String tableName) throws Exception {
        final String sql;
        final String expectedErrorRegex;
        switch (ddl) {
            case 0 -> {
                sql = "TRUNCATE TABLE " + tableName;
                expectedErrorRegex = Pattern.quote("there is an active query against '" + tableName + "'. Try again.");
            }
            case 1 -> {
                sql = "RENAME TABLE " + tableName + " TO " + tableName + "_tmp";
                expectedErrorRegex = Pattern.quote("table busy [reason=") + "(busyReader|busyTableReaderMetaPool)" + Pattern.quote("]");
            }
            case 2 -> {
                sql = "DROP TABLE " + tableName;
                expectedErrorRegex = Pattern.quote("could not lock '" + engine.verifyTableName(tableName).getDirName() + "' [reason='")
                        + "(busyReader|busyTableReaderMetaPool)" + Pattern.quote("']");
            }
            case 3 -> {
                sql = "TRUNCATE TABLE " + tableName + " KEEP SYMBOL MAPS";
                expectedErrorRegex = Pattern.quote("there is an active query against '" + tableName + "'. Try again.");
            }
            default -> {
                // the query holds the first table of the statement
                sql = "TRUNCATE TABLE " + tableName + ", tp";
                expectedErrorRegex = Pattern.quote("there is an active query against '" + tableName + "'. Try again.");
            }
        }
        LOG.info().$("DDL while a query holds the table [sql=").$(sql).I$();
        final ReaderHoldingThread query = ReaderHoldingThread.start(engine, tableName);
        try {
            capture.start();
            try {
                execute(sql);
                Assert.fail("expected to fail: " + sql);
            } catch (SqlException | CairoException e) {
                final String message = e.getFlyweightMessage().toString();
                Assert.assertTrue("unexpected error of " + sql + ": " + message, Pattern.matches(expectedErrorRegex, message));
            }
            capture.drain();
            capture.assertNotLogged(ViewCompileParker.DDL_WAIT_LINE_PREFIX);
        } finally {
            query.close();
        }
    }

    /**
     * Reads a random view from inside a random nest of sub-queries, in a statement that fails to
     * compile, and checks that the failure queues one recompile for that view and for each view it
     * reads, directly or through other views. The expected views come from each view's recorded
     * dependencies, which CREATE VIEW collects on its own.
     */
    private static void assertFailedReadQueuesViewClosure(Rnd rnd, String tableName, String[] viewNames) throws Exception {
        final ViewCompilerTask task = new ViewCompilerTask();
        // the fuzz run may leave recompile signals behind; they are not part of the check
        while (engine.getViewStateStore().tryDequeueCompilerTask(task)) {
            // discard
        }
        for (int round = 0; round < 20; round++) {
            final String viewName = viewNames[rnd.nextInt(viewNames.length)];
            final int depth = 1 + rnd.nextInt(12);
            String q = "SELECT * FROM " + viewName;
            for (int i = 0; i < depth; i++) {
                q = switch (rnd.nextInt(4)) {
                    case 0 -> "SELECT * FROM " + tableName + " WHERE EXISTS (" + q + ")";
                    case 1 -> "SELECT * FROM " + tableName + " WHERE (SELECT count() FROM (" + q + ")) >= 0";
                    case 2 -> "SELECT * FROM (" + q + ")";
                    default -> "SELECT a.* FROM " + tableName + " a JOIN LATERAL (" + q + ") l ON true";
                };
            }
            final String sql = "SELECT no_such_col FROM (" + q + ")";
            LOG.info().$("failing read [sql=").$(sql).I$();
            // The binder reads FROM before the select list, so the deepest EXISTS, which has no function, fails first.
            final int existsPosition = sql.lastIndexOf("EXISTS (");
            if (existsPosition > -1) {
                assertExceptionNoLeakCheck(sql, existsPosition, "unknown function name: EXISTS(CURSOR)");
            } else {
                assertExceptionNoLeakCheck(sql, 7, "Invalid column: no_such_col");
            }

            final TreeMap<String, Integer> expected = new TreeMap<>();
            collectViewClosure(viewName, expected);
            final TreeMap<String, Integer> actual = new TreeMap<>();
            while (engine.getViewStateStore().tryDequeueCompilerTask(task)) {
                actual.merge(task.tableToken.getTableName(), 1, Integer::sum);
            }
            Assert.assertEquals(sql, expected, actual);
        }
    }

    /**
     * Adds {@code name} to {@code out} with a count of 1 when it names a view, then does the same
     * for every table and view that view's definition reads.
     */
    private static void collectViewClosure(CharSequence name, TreeMap<String, Integer> out) {
        final TableToken token = engine.getTableTokenIfExists(name);
        if (token == null || !token.isView() || out.putIfAbsent(token.getTableName(), 1) != null) {
            return;
        }
        final ViewDefinition definition = engine.getViewGraph().getViewDefinition(token);
        Assert.assertNotNull(name.toString(), definition);
        final ObjList<CharSequence> dependencies = definition.getDependencies().keys();
        for (int i = 0, n = dependencies.size(); i < n; i++) {
            collectViewClosure(dependencies.getQuick(i), out);
        }
    }

    private static void createNonWalTable(String tableName, boolean isPartitioned) throws SqlException {
        if (isPartitioned) {
            execute("CREATE TABLE " + tableName + " (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        } else {
            execute("CREATE TABLE " + tableName + " (a INT)");
        }
        insertRows(tableName, isPartitioned);
        Assert.assertFalse(engine.verifyTableName(tableName).isWal());
    }

    private static void createView(String viewName, String viewSql) throws SqlException {
        execute("create view " + viewName + " as (" + viewSql + ")");
    }

    private static void createViewAndCompile(String viewName, String viewSql, ObjList<String> viewNames, ObjList<String> viewSqls) throws SqlException {
        createView(viewName, viewSql);
        // compile the view before a view over it is created
        drainViewQueue();
        viewNames.add(viewName);
        viewSqls.add(viewSql);
    }

    private static void insertPivotRows() throws SqlException {
        execute("INSERT INTO tp VALUES ('a', 1), ('b', 2)");
    }

    private static void insertRows(String tableName, boolean isPartitioned) throws SqlException {
        if (isPartitioned) {
            execute("INSERT INTO " + tableName + " (a, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
        } else {
            execute("INSERT INTO " + tableName + " (a) VALUES (1), (2)");
        }
    }

    private void assertViewsValidAndMatchSql(ObjList<String> viewNames, ObjList<String> viewSqls) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0, n = viewNames.size(); i < n; i++) {
                final String viewName = viewNames.getQuick(i);
                assertQuery("SELECT view_status FROM views() WHERE view_name = '" + viewName + "'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_status
                                valid
                                """);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, viewSqls.getQuick(i), viewName, LOG);
                // A query of the view expands its SQL, so the rows match even when the last
                // compile of the view went missing, such as a lost busy retry; the catalogue
                // keeps the columns of the compile before it.
                AbstractViewTest.assertViewColumnsOfQuery(viewName, viewSqls.getQuick(i));
            }
        }
    }

    private ObjList<FuzzTransaction> createTransactionsAndView(
            Rnd rnd,
            String tableName,
            String viewName,
            String viewSql
    ) throws SqlException, NumericException {
        fuzzer.createInitialTableWal(tableName, timestampTypes[rnd.nextInt(10) % 2]);
        createView(viewName, viewSql);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private void runViewFuzz(Rnd rnd, String tableNameBase, int tableCount) throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> viewCompilerJobError = new AtomicReference<>();
        final ObjList<Thread> viewCompilerJobs = new ObjList<>();
        final int viewCompilerJobCount = 1 + rnd.nextInt(4);

        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();

        try {
            for (int i = 0; i < viewCompilerJobCount; i++) {
                viewCompilerJobs.add(startViewCompilerJob(i, stop, viewCompilerJobError, rnd, 50));
            }

            for (int i = 0; i < tableCount; i++) {
                String tableName = tableNameBase + "_" + i;
                String viewName = tableName + "_v";
                String viewSql = "select min(c3), max(c3), ts from " + tableName + " sample by 1h";
                ObjList<FuzzTransaction> transactions = createTransactionsAndView(rnd, tableName, viewName, viewSql);
                fuzzTransactions.add(transactions);
                viewSqls.add(viewSql);
            }

            // Can help to reduce memory consumption.
            engine.releaseInactive();
            fuzzer.applyManyWalParallel(fuzzTransactions, rnd, tableNameBase, true, true);
        } finally {
            stopAndJoinJobs(stop, viewCompilerJobs);
            TestUtils.rethrowFirst(viewCompilerJobError);
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        drainViewQueue();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewName = tableNameBase + "_" + i + "_v";
                final String viewSql = viewSqls.getQuick(i);
                LOG.info().$("asserting view ").$(viewName).$(" against ").$(viewSql).$();
                // check that the view exists and it is valid
                assertQuery("select view_status " +
                        "from views() " +
                        "where view_name = '" + viewName + "'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_status
                                valid
                                """);
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        viewName,
                        LOG
                );
            }
        }
    }

    private void setFuzzParams(Rnd rnd) {
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(300),
                rnd.nextInt(150),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(3000),
                5 + rnd.nextInt(10)
        );

        // Easy, no column manipulations
        fuzzer.setFuzzProbabilities(
                0.0,
                0.0,
                0.0,
                0.0,
                0.3,
                0.0,
                0.0,
                0.0,
                0.8,
                0.0,
                0.0,
                0.3,
                0.0,
                0.0,
                0.1,
                0.0
        );
    }

    private Thread startViewCompilerJob(int workerId, AtomicBoolean stop, AtomicReference<Throwable> jobError, Rnd outsideRnd, int maxNapMillis) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (ViewCompilerJob job = new ViewCompilerJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                job.run();
                                Os.sleep(rnd.nextInt(maxNapMillis));
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (job.run());
                            }
                        }
                    } catch (Throwable throwable) {
                        // the caller fails the test with the first failure of its jobs once it joined them
                        jobError.compareAndSet(null, throwable);
                        LOG.error().$("View compiler job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
                        LOG.info().$("View compiler job stopped").$();
                    }
                }, "view-compiler-job" + workerId
        );
        th.start();
        return th;
    }

    private void testViewFuzz(RandomSelectGenerator selectGenerator, Rnd rnd, String tableName, String... viewNames) throws Exception {
        // The final correctness check below orders each view by ALL non-binary columns to make the
        // cursor comparison deterministic. RandomSelectGenerator can chain self-joins whose
        // many-to-many key matches inflate the row count enough that ordering the result overruns
        // the sort-key budget the test config imposes (Overrides sets
        // cairo.sql.sort.key.max.bytes), throwing a spurious LimitOverflowException on an
        // otherwise legal query. Production leaves the sort-key budget unbounded (Long.MAX_VALUE);
        // mirror that here, since this test exercises view correctness, not sort memory limits.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, Long.MAX_VALUE);
        long start = MicrosTimestampDriver.floor("2022-02-24T17");
        assertMemoryLeak(() -> {
            fuzzer.createInitialTableWal(tableName, timestampTypes[rnd.nextInt(10) % 2]);
            selectGenerator.registerTable(tableName);

            final ObjList<String> viewSqls = new ObjList<>();
            try (ViewCompilerJob viewCompiler = new ViewCompilerJob(0, engine, 0)) {
                for (final String viewName : viewNames) {
                    final String viewSql = selectGenerator.generate();
                    viewSqls.add(viewSql);
                    createView(viewName, viewSql);
                    LOG.info().$("created view ").$(viewName).$(" as ").$(viewSql).$();
                    // compile the view before registering it, so dependent views can see this view's metadata
                    viewCompiler.run();
                    selectGenerator.registerTable(viewName);
                }
            }

            AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> viewCompilerJobError = new AtomicReference<>();
            ObjList<Thread> viewCompilerJobs = new ObjList<>();

            try {
                viewCompilerJobs.add(startViewCompilerJob(0, stop, viewCompilerJobError, rnd, 50));

                setFuzzParams(rnd);

                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd, start);
                ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
                fuzzTransactions.add(transactions);
                fuzzer.applyManyWalParallel(
                        fuzzTransactions,
                        rnd,
                        tableName,
                        false,
                        true
                );
            } finally {
                stopAndJoinJobs(stop, viewCompilerJobs);
                TestUtils.rethrowFirst(viewCompilerJobError);
            }

            drainWalQueue();
            // no compiler job runs from here on, so the queue holds only what a failing read adds
            assertFailedReadQueuesViewClosure(rnd, tableName, viewNames);

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0, n = viewNames.length; i < n; i++) {
                    final String viewName = viewNames[i];
                    final String viewSql = viewSqls.getQuick(i);

                    // Order by ALL non-binary columns to ensure deterministic ordering.
                    // Ordering by a single low-cardinality column can cause flaky failures
                    // when multiple rows have the same value.
                    StringSink orderByClause = new StringSink();
                    final TableToken viewToken = engine.getTableTokenIfExists(viewName);
                    try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
                        for (int j = 0; j < metadata.getColumnCount(); j++) {
                            if (metadata.getColumnType(j) != ColumnType.BINARY) {
                                if (!orderByClause.isEmpty()) {
                                    orderByClause.put(", ");
                                }
                                orderByClause.put(j + 1);
                            }
                        }
                    }
                    String orderBy = !orderByClause.isEmpty() ? " order by " + orderByClause : "";

                    LOG.info().$("asserting view ").$(viewName).$(" against ").$(viewSql).$(", order-by: ").$(orderBy).I$();
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            "(" + viewSql + ")" + orderBy,
                            "(" + viewName + ")" + orderBy,
                            LOG
                    );
                }
            }
        });
    }
}
