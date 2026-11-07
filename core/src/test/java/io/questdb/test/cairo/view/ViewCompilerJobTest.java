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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.NoOpViewStateStore;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.view.ViewState;
import io.questdb.cairo.view.ViewStateStore;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ViewCompilerJobTest extends AbstractViewTest {
    // DROP, RENAME or TRUNCATE of t logs this line when it waits for a view compile run
    private static final String DDL_WAIT_LINE = "waiting for view compilation to release table [table=t]";
    private static final String PIVOT_RACE_SQL = """
            SELECT * FROM data
            PIVOT (
                SUM(val)
                FOR cat IN (SELECT c FROM cats)
                GROUP BY grp
            ) ORDER BY grp""";
    // the view compile race hooks, inert until a test arms them, see setUpStatic()
    private static final AtomicReference<String> raceAddColumnThenRecreateSql = new AtomicReference<>();
    private static final AtomicInteger raceCatsSchemaChangesLeft = new AtomicInteger();
    private static final StringSink raceInvalidViews = new StringSink();
    private static final AtomicReference<String> raceRecreateTableSql = new AtomicReference<>();
    private static final ObjList<String> raceWatchedViews = new ObjList<>();
    private final String[] breakingSqls = new String[]{
            "RENAME TABLE " + TABLE1 + " TO " + TABLE3,
            "ALTER TABLE " + TABLE1 + " DROP COLUMN v",
            "ALTER TABLE " + TABLE1 + " RENAME COLUMN v to v_renamed",
            "RENAME TABLE " + TABLE2 + " TO " + TABLE4,
            "ALTER TABLE " + TABLE2 + " DROP COLUMN v",
            "ALTER TABLE " + TABLE2 + " RENAME COLUMN v to v_renamed"
    };
    private final String[] fixingSqls = new String[]{
            "RENAME TABLE " + TABLE3 + " TO " + TABLE1,
            "ALTER TABLE " + TABLE1 + " ADD COLUMN v LONG",
            "ALTER TABLE " + TABLE1 + " RENAME COLUMN v_renamed to v",
            "RENAME TABLE " + TABLE4 + " TO " + TABLE2,
            "ALTER TABLE " + TABLE2 + " ADD COLUMN v LONG",
            "ALTER TABLE " + TABLE2 + " RENAME COLUMN v_renamed to v"
    };
    private final Rnd rnd = new Rnd();
    private final String[] viewQueries = new String[]{
            "select ts, k, max(v) as value from " + TABLE1 + " where v > 4",
            "select ts, min(v) as value from " + TABLE2 + " where v > 6",
            "select ts, max(5 * v) as v_max from " + TABLE1 + " where v > 4",
            "select ts, k, sqrt(v) as v from " + TABLE2 + " where v > 3",
            "select value from " + VIEW1,
            "select t1.ts, t2.v from " + TABLE1 + " t1 join " + TABLE2 + " t2 on k"
    };

    /**
     * Stages the races a view compile can lose to a concurrent schema change. The hooks touch only
     * the tables {@code cats} and {@code race_t}, which only the race tests create.
     * <ul>
     *     <li>While {@code raceCatsSchemaChangesLeft} is positive, each open of a reader on
     *     {@code cats} at a known metadata version adds a column to {@code cats} just before the
     *     open and decrements it, as a concurrent ALTER would between the optimiser recording the
     *     version of the PIVOT IN sub-query's table and the sub-query opening its reader.</li>
     *     <li>While {@code raceRecreateTableSql} is set, the next open of a reader on {@code race_t}
     *     drops {@code race_t} and recreates it with that statement, as a concurrent DROP TABLE and
     *     CREATE TABLE would between the optimiser resolving the table token and opening the
     *     reader.</li>
     *     <li>While {@code raceAddColumnThenRecreateSql} is set, the next open of a reader on
     *     {@code race_t} at a known metadata version adds a column to {@code race_t} just before the
     *     open, as a concurrent ALTER would between the optimiser and code generation. It then moves
     *     the statement to {@code raceRecreateTableSql}, so the next open without a version, which
     *     the optimiser makes when code generation re-parses the statement, loses a DROP TABLE and
     *     CREATE TABLE race.</li>
     *     <li>Every reader open on either table records the watched views that are invalid at that
     *     moment into {@code raceInvalidViews}, so a test sees an invalid state that a later compile
     *     repairs.</li>
     * </ul>
     */
    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public TableReader getReader(TableToken tableToken, @Nullable ResourcePoolSupervisor<TableReader> readerPoolSupervisor) {
                if (Chars.equals(tableToken.getTableName(), "race_t")) {
                    final String recreateSql = raceRecreateTableSql.getAndSet(null);
                    if (recreateSql != null) {
                        try {
                            execute("DROP TABLE race_t", sqlExecutionContext);
                            execute(recreateSql, sqlExecutionContext);
                        } catch (SqlException e) {
                            throw new AssertionError("could not recreate race_t", e);
                        }
                    }
                    recordInvalidWatchedViews(this);
                }
                return super.getReader(tableToken, readerPoolSupervisor);
            }

            @Override
            public TableReader getReader(TableToken tableToken, long metadataVersion, @Nullable ResourcePoolSupervisor<TableReader> readerPoolSupervisor) {
                if (metadataVersion > -1 && Chars.equals(tableToken.getTableName(), "race_t")) {
                    final String recreateSql = raceAddColumnThenRecreateSql.getAndSet(null);
                    if (recreateSql != null) {
                        try (TableWriter writer = TestUtils.getWriter(this, "race_t")) {
                            writer.addColumn("extra", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                        }
                        raceRecreateTableSql.set(recreateSql);
                    }
                    recordInvalidWatchedViews(this);
                }
                if (metadataVersion > -1 && Chars.equals(tableToken.getTableName(), "cats")) {
                    if (raceCatsSchemaChangesLeft.get() > 0) {
                        try (TableWriter writer = TestUtils.getWriter(this, "cats")) {
                            writer.addColumn("extra" + raceCatsSchemaChangesLeft.decrementAndGet(), ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                        }
                    }
                    recordInvalidWatchedViews(this);
                }
                return super.getReader(tableToken, metadataVersion, readerPoolSupervisor);
            }
        };
        AbstractViewTest.setUpStatic();
    }

    @After
    public void tearDown() throws Exception {
        raceAddColumnThenRecreateSql.set(null);
        raceCatsSchemaChangesLeft.set(0);
        raceInvalidViews.clear();
        raceRecreateTableSql.set(null);
        raceWatchedViews.clear();
        super.tearDown();
    }

    @Test
    public void testCompileInvalidatesViewWhenRetriesRunOut() throws Exception {
        // Every compile attempt loses the race. The job must give up after the configured number
        // of recompile attempts, leave nothing open, and report the out-of-date table as the
        // reason the view is invalid.
        assertMemoryLeak(() -> {
            createPivotRaceTables();
            createView("pv", PIVOT_RACE_SQL);

            final int schemaChangeBudget = 1_000;
            raceCatsSchemaChangesLeft.set(schemaChangeBudget);
            engine.enqueueCompileView(engine.verifyTableName("pv"));
            drainViewQueue();

            // the first attempt and each retry lost the race once
            Assert.assertEquals(
                    configuration.getMaxSqlRecompileAttempts() + 1,
                    schemaChangeBudget - raceCatsSchemaChangesLeft.get()
            );
            assertViewInvalid("pv", "cached query plan cannot be used because table schema has changed [table=cats");
            assertNothingBusy();

            // the next compile repairs the view once the table settles
            raceCatsSchemaChangesLeft.set(0);
            engine.enqueueCompileView(engine.verifyTableName("pv"));
            drainViewQueue();
            assertViewState("pv");
            assertViewColumns("pv", "grp, X, Y");
            assertNothingBusy();
        });
    }

    @Test
    public void testCompileRetriesWhenPivotSubqueryTableChanges() throws Exception {
        // The view's PIVOT IN sub-query runs while the view's SQL is optimised, so the out-of-date
        // table surfaces from the model generation, not from the code generation the compiler
        // retries by itself. The job must recompile the view's SQL instead of invalidating the view
        // and its dependent view.
        assertMemoryLeak(() -> {
            createPivotRaceTables();
            createView("pv", PIVOT_RACE_SQL);
            createView("pv_dep", "SELECT grp, X FROM pv");
            assertViewColumns("pv", "grp, X, Y");

            // a category that the view's stored metadata does not have yet
            execute("INSERT INTO cats VALUES ('Z')");
            execute("INSERT INTO data VALUES ('A', 'Z', 50)");

            raceWatchedViews.add("pv");
            raceWatchedViews.add("pv_dep");
            raceCatsSchemaChangesLeft.set(1);
            // a change to the base table recompiles both views
            engine.enqueueCompileView(engine.verifyTableName("data"));
            drainViewQueue();
            recordInvalidWatchedViews(engine);

            Assert.assertEquals(0, raceCatsSchemaChangesLeft.get());
            Assert.assertEquals("", raceInvalidViews.toString());
            assertViewState("pv");
            assertViewState("pv_dep");
            assertViewColumns("pv", "grp, X, Y, Z");
            assertViewColumns("pv_dep", "grp, X");
            assertNothingBusy();
        });
    }

    @Test
    public void testCompileRetriesWhenTableIsRecreated() throws Exception {
        // The table the view reads is dropped and recreated with another column between the
        // optimiser resolving its token and opening its reader. The job must recompile the view's
        // SQL against the new table instead of invalidating the view until the CREATE TABLE's own
        // compile event repairs it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE race_t (a INT)");
            execute("INSERT INTO race_t VALUES (1)");
            createView("race_v", "SELECT * FROM race_t");
            assertViewColumns("race_v", "a");

            raceWatchedViews.add("race_v");
            raceRecreateTableSql.set("CREATE TABLE race_t (a INT, b INT)");
            engine.enqueueCompileView(engine.verifyTableName("race_v"));
            drainViewQueue();
            recordInvalidWatchedViews(engine);

            Assert.assertNull(raceRecreateTableSql.get());
            Assert.assertEquals("", raceInvalidViews.toString());
            assertViewState("race_v");
            assertViewColumns("race_v", "a, b");
            assertNothingBusy();
        });
    }

    @Test
    public void testCompileRetriesWhenTableIsRecreatedDuringPlanRetry() throws Exception {
        // Code generation loses a race to an ADD COLUMN and re-parses the view's SQL by itself.
        // That re-parse loses a second race, to a DROP TABLE and CREATE TABLE, so the out-of-date
        // table escapes code generation's own retry and surfaces from the metadata update rather
        // than from the model generation. The job must recompile the view's SQL instead of
        // invalidating the view until the CREATE TABLE's own compile event repairs it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE race_t (a INT)");
            execute("INSERT INTO race_t VALUES (1)");
            createView("race_v", "SELECT * FROM race_t");
            assertViewColumns("race_v", "a");

            raceWatchedViews.add("race_v");
            raceAddColumnThenRecreateSql.set("CREATE TABLE race_t (a INT, b INT, c INT)");
            engine.enqueueCompileView(engine.verifyTableName("race_v"));
            drainViewQueue();
            recordInvalidWatchedViews(engine);

            // both races ran
            Assert.assertNull(raceAddColumnThenRecreateSql.get());
            Assert.assertNull(raceRecreateTableSql.get());
            Assert.assertEquals("", raceInvalidViews.toString());
            assertViewState("race_v");
            assertViewColumns("race_v", "a, b, c");
            assertNothingBusy();
        });
    }

    @Test
    public void testConcurrentEventProcessing() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            for (int i = 0; i < viewQueries.length; i++) {
                createView("view" + i, viewQueries[i]);
                compileView("view" + i);
            }

            final ObjList<Thread> threads = new ObjList<>();
            final ObjList<ViewCompilerJob> compilerJobs = new ObjList<>();
            final ConcurrentHashMap<Throwable> errors = new ConcurrentHashMap<>();
            final AtomicBoolean stop = new AtomicBoolean(false);

            final int numOfCompileJobs = 4;
            for (int i = 0; i < numOfCompileJobs; i++) {
                final ViewCompilerJob compilerJob = new ViewCompilerJob(i, engine);
                compilerJobs.add(compilerJob);
                final int finalI = i;
                final Thread th = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            compilerJob.run();
                            Os.sleep(1);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        errors.put("compileThread " + finalI, e);
                    }
                });
                th.setName("compileThread " + finalI);
                threads.add(th);
                th.start();
            }

            final int numOfTableChangeThreads = 2;
            final int numOfSqls = breakingSqls.length / 2;
            for (int i = 0; i < numOfTableChangeThreads; i++) {
                final int finalI = i;
                final Thread th = new Thread(() -> {
                    while (!stop.get()) {
                        try {
                            final int index = finalI * numOfSqls + rnd.nextInt(numOfSqls);
                            execute(breakingSqls[index]);
                            execute(fixingSqls[index]);
                        } catch (Throwable e) {
                            e.printStackTrace(System.out);
                            errors.put("tableChangeThread " + finalI, e);
                        }
                        Os.sleep(1);
                    }
                });
                th.setName("tableChangeThread " + finalI);
                threads.add(th);
                th.start();
            }

            Os.sleep(1000);
            stop.set(true);

            for (int i = 0; i < threads.size(); i++) {
                threads.getQuick(i).join();
            }

            assertEquals(0, errors.size());

            drainWalQueue(engine);
            for (int i = 0; i < compilerJobs.size(); i++) {
                compilerJobs.getQuick(i).run();
            }
            drainWalQueue(engine);

            for (int i = 0; i < viewQueries.length; i++) {
                compileView("view" + i);
            }
        });
    }

    @Test
    public void testDdlRightAfterCreateViewWaitsForViewCompile() throws Exception {
        // CREATE VIEW queues the compile of the new view, and the run holds the reader of the
        // non-WAL table the view reads when a DROP, RENAME or TRUNCATE of the table locks it. The
        // DDL waits for the run instead of failing as busy.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            final LogCapture capture = new LogCapture();
            try {
                createTableAndViewThenExecuteWhileViewCompileHolds(capture, "TRUNCATE TABLE t");
                assertQuery("t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("a\n");
                drainViewQueue();
                assertViewState("v");
                assertViewColumns("v", "a");
                execute("DROP VIEW v");
                execute("DROP TABLE t");
                drainWalAndViewQueues();

                createTableAndViewThenExecuteWhileViewCompileHolds(capture, "DROP TABLE t");
                assertTableAbsent("t");
                execute("DROP VIEW v");
                drainWalAndViewQueues();

                createTableAndViewThenExecuteWhileViewCompileHolds(capture, "RENAME TABLE t TO u");
                assertTableAbsent("t");
                assertQuery("u")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                a
                                1
                                2
                                """);
            } finally {
                capture.stop();
            }
        });
    }

    @Test
    public void testDropWaitsForStartupViewCompile() throws Exception {
        // The server compiles every view in one pass at startup, while it already takes
        // statements. DROP of a non-WAL table that a view reads waits for the pass instead of
        // failing as busy.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, Long.MAX_VALUE);
            execute("CREATE TABLE t (a INT)");
            execute("INSERT INTO t VALUES (1), (2)");
            createView("v", "SELECT * FROM t");

            final LogCapture capture = new LogCapture();
            capture.start();
            final ViewCompileParker pass = ViewCompileParker.ofStartupPass(engine, "t", PoolListener.SRC_READER);
            engine.setPoolListener(pass);
            try {
                pass.start();
                pass.releaseAfterLogged(capture, DDL_WAIT_LINE, null);
                execute("DROP TABLE t");
            } finally {
                pass.join();
                engine.setPoolListener(null);
                capture.stop();
            }
            assertTableAbsent("t");
            drainViewQueue();
            assertViewState("v", "table does not exist [table=t]");
        });
    }

    @Test
    public void testViewCompilerJobPublishesCompileRuns() throws Exception {
        // A DDL tells a view compile run's borrow of a table from a query's by the compile runs
        // the job publishes: one run per drain that finds tasks, and one for the startup pass.
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            createView(VIEW1, "SELECT * FROM " + TABLE1);
            createView(VIEW2, "SELECT * FROM " + TABLE2);
            final ViewStateStore stateStore = engine.getViewStateStore();
            final long idleActivity = stateStore.getCompileRunActivity();
            Assert.assertFalse(ViewStateStore.isCompileRunRunning(idleActivity));
            Assert.assertFalse(ViewStateStore.hasCompileRunOverlapped(idleActivity, idleActivity));

            // a drain that finds no task is no run
            drainViewQueue();
            Assert.assertEquals(idleActivity, stateStore.getCompileRunActivity());

            // one drain of two tasks is one run, and it has finished
            engine.enqueueCompileView(engine.verifyTableName(TABLE1));
            engine.enqueueCompileView(engine.verifyTableName(TABLE2));
            try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
                job.run();
            }
            final long activityAfterDrain = stateStore.getCompileRunActivity();
            Assert.assertEquals(idleActivity + (1L << 32), activityAfterDrain);
            Assert.assertFalse(ViewStateStore.isCompileRunRunning(activityAfterDrain));
            Assert.assertTrue(ViewStateStore.hasCompileRunOverlapped(idleActivity, activityAfterDrain));
            assertViewState(VIEW1);
            assertViewState(VIEW2);

            // the run is running while it compiles
            engine.enqueueCompileView(engine.verifyTableName(TABLE1));
            final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, TABLE1, PoolListener.SRC_READER);
            engine.setPoolListener(run);
            final long activityWhileCompiling;
            try {
                run.start();
                activityWhileCompiling = stateStore.getCompileRunActivity();
            } finally {
                run.join();
                engine.setPoolListener(null);
            }
            Assert.assertEquals(activityAfterDrain + (1L << 32) + 1, activityWhileCompiling);
            Assert.assertTrue(ViewStateStore.isCompileRunRunning(activityWhileCompiling));
            Assert.assertTrue(ViewStateStore.hasCompileRunOverlapped(activityWhileCompiling, activityWhileCompiling));
            final long activityAfterRun = stateStore.getCompileRunActivity();
            Assert.assertEquals(activityAfterDrain + (1L << 32), activityAfterRun);

            // the startup pass over all views is one run
            try (SqlExecutionContext executionContext = engine.createViewCompilerContext(0)) {
                ViewCompilerJob.compileAllViews(engine, executionContext, new ObjList<>());
            }
            Assert.assertEquals(activityAfterRun + (1L << 32), stateStore.getCompileRunActivity());
            assertViewState(VIEW1);
            assertViewState(VIEW2);

            // no compiler consumes the queue of the no-op store, so it never has a run
            final ViewStateStore noOpStore = NoOpViewStateStore.INSTANCE;
            noOpStore.startCompileRun();
            Assert.assertEquals(0, noOpStore.getCompileRunActivity());
            noOpStore.finishCompileRun();
            Assert.assertEquals(0, noOpStore.getCompileRunActivity());
        });
    }

    private static void assertNothingBusy() {
        Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
        Assert.assertEquals(0, engine.getBusyReaderCount());
        Assert.assertEquals(0, engine.getBusyWriterCount());
    }

    private static void assertTableAbsent(String tableName) {
        Assert.assertNull(engine.getTableTokenIfExists(tableName));
    }

    private static void assertViewColumns(String viewName, String expectedColumns) {
        final StringSink sink = new StringSink();
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(viewName))) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (i > 0) {
                    sink.put(", ");
                }
                sink.put(metadata.getColumnName(i));
            }
        }
        Assert.assertEquals(expectedColumns, sink.toString());
    }

    private static void assertViewInvalid(String viewName, String expectedReasonFragment) {
        final ViewState viewState = engine.getViewStateStore().getViewState(engine.verifyTableName(viewName));
        Assert.assertNotNull(viewState);
        final StringSink reason = new StringSink();
        viewState.lockForRead();
        try {
            Assert.assertTrue(viewState.isInvalid());
            viewState.getInvalidationReason(reason);
        } finally {
            viewState.unlockAfterRead();
        }
        TestUtils.assertContains(reason, expectedReasonFragment);
    }

    private static void createPivotRaceTables() throws SqlException {
        execute("CREATE TABLE data (grp SYMBOL, cat SYMBOL, val INT)");
        execute("CREATE TABLE cats (c SYMBOL)");
        execute("INSERT INTO cats VALUES ('X'), ('Y')");
        execute("""
                INSERT INTO data VALUES
                    ('A', 'X', 10),
                    ('A', 'Y', 20),
                    ('B', 'X', 30),
                    ('B', 'Y', 40)
                """);
    }

    // runs on the thread that drains the view queue, which in these tests is the test thread
    private static void recordInvalidWatchedViews(CairoEngine engine) {
        final StringSink reason = new StringSink();
        for (int i = 0, n = raceWatchedViews.size(); i < n; i++) {
            final String viewName = raceWatchedViews.getQuick(i);
            final ViewState viewState = engine.getViewStateStore().getViewState(engine.verifyTableName(viewName));
            if (viewState == null) {
                continue;
            }
            viewState.lockForRead();
            try {
                if (viewState.isInvalid()) {
                    viewState.getInvalidationReason(reason);
                    raceInvalidViews.put(viewName).put(": ").put(reason).put('\n');
                }
            } finally {
                viewState.unlockAfterRead();
            }
        }
    }

    // Creates the non-WAL table t with rows 1 and 2 and the view v over it, then runs the DDL
    // while the compile run that CREATE VIEW queued holds t's reader. The run ends once the DDL
    // logs that it waits for it.
    private void createTableAndViewThenExecuteWhileViewCompileHolds(LogCapture capture, String ddl) throws Exception {
        execute("CREATE TABLE t (a INT)");
        execute("INSERT INTO t VALUES (1), (2)");
        Assert.assertFalse(engine.verifyTableName("t").isWal());
        execute("CREATE VIEW v AS (SELECT * FROM t)");
        // a wait line of an earlier DDL would release the run before this DDL waits
        capture.start();
        final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER);
        engine.setPoolListener(run);
        try {
            run.start();
            run.releaseAfterLogged(capture, DDL_WAIT_LINE, null);
            execute(ddl);
        } finally {
            run.join();
            engine.setPoolListener(null);
        }
    }
}
