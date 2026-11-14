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
import io.questdb.cairo.view.NoOpViewStateStore;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.view.ViewCompilerTask;
import io.questdb.cairo.view.ViewState;
import io.questdb.cairo.view.ViewStateStore;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ViewCompilerJobTest extends AbstractViewTest {
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
    // the borrows of t's reader without a metadata version, which the optimiser makes once per
    // compile of a view over t, on the thread that countReaderBorrowsOfT() arms
    private static final AtomicInteger readerBorrowsOfT = new AtomicInteger();
    private static volatile Thread readerBorrowCountingThread;
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
                if (Thread.currentThread() == readerBorrowCountingThread && Chars.equals(tableToken.getTableName(), "t")) {
                    readerBorrowsOfT.incrementAndGet();
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
        readerBorrowCountingThread = null;
        readerBorrowsOfT.set(0);
        super.tearDown();
    }

    @Test
    public void testBusyRetryCompilesViewAlone() throws Exception {
        // A busy retry repeats the compile of one view: each view over it that the original request
        // covered got its own compile, and its own retry when that compile met a lock. A request for
        // the view compiles the views over it too.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            createView("v", "SELECT * FROM t");
            createView("vp", "SELECT * FROM v PIVOT (sum(x) FOR k IN (SELECT DISTINCT k FROM v ORDER BY k))");
            // a compile of vp over the emptied table fails
            execute("TRUNCATE TABLE t");
            setCurrentMicros(ALTER_MICROS);
            final long dueMicros = ALTER_MICROS + busyRetryTimeoutMicros();
            Assert.assertEquals(dueMicros, engine.getViewStateStore().scheduleCompileBusyRetry(engine.verifyTableName("v"), ALTER_MICROS));
            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewStatus("vp", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertPendingCompileBusyRetries("");

            // 11:30
            setCurrentMicros(ALTER_MICROS + 30 * Micros.MINUTE_MICROS);
            engine.enqueueCompileView(engine.verifyTableName("v"));
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T11:30:00.000000Z");
            assertViewStatus("vp", "invalid", "PIVOT IN subquery returned empty result set", "2025-06-19T11:30:00.000000Z");
        });
    }

    @Test
    public void testBusyRetryOfOlderRequestDoesNotOverwriteNewerState() throws Exception {
        // The compile of a request meets the lock of t's readers, and a newer request compiles the
        // view before the busy retry is due. The retry then compiles the view over the emptied table
        // and fails, but its update timestamp is older than the view state's, so the view keeps the
        // state of the newer request.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createPivotTable();
            createView("vp", PIVOT_SQL);
            final long dueMicros = ALTER_MICROS + busyRetryTimeoutMicros();
            setCurrentMicros(ALTER_MICROS);
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                engine.enqueueCompileView(engine.verifyTableName("t"));
                drainViewQueue();
            }
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("vp", ALTER_MICROS, dueMicros));

            // 11:00:00.5, before the retry is due
            setCurrentMicros(ALTER_MICROS + busyRetryTimeoutMicros() / 2);
            execute("COMPILE VIEW vp");
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.500000Z");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("vp", ALTER_MICROS, dueMicros));

            execute("TRUNCATE TABLE t");
            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("vp", "valid", "", "2025-06-19T11:00:00.500000Z");
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testBusyRetryTimeoutSettingSetsRetryDelay() throws Exception {
        // The busy retry is due the configured timeout, in milliseconds or with a unit, after the
        // compile met the lock, and not a microsecond earlier.
        assertMemoryLeak(() -> {
            final String[] timeouts = {"1", "250ms"};
            final long[] delaysMicros = {1_000, 250_000};
            for (int i = 0; i < timeouts.length; i++) {
                node1.setProperty(PropertyKey.CAIRO_VIEW_COMPILE_BUSY_RETRY_TIMEOUT, timeouts[i]);
                createTableWithRowsAndView();
                setCurrentMicros(ALTER_MICROS);
                execute("ALTER TABLE t ADD COLUMN b INT");
                try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                    drainViewQueue();
                }
                final long dueMicros = ALTER_MICROS + delaysMicros[i];
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));

                setCurrentMicros(dueMicros - 1);
                drainViewQueue();
                assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewColumns("v", "a:INT");

                setCurrentMicros(dueMicros);
                drainViewQueue();
                assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
                assertViewColumns("v", "a:INT, b:INT");
                assertPendingCompileBusyRetries("");

                execute("DROP VIEW v");
                execute("DROP TABLE t");
                drainViewQueue();
            }
        });
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
            assertViewColumns("pv", "grp:SYMBOL, X:LONG, Y:LONG");
            assertNothingBusy();
        });
    }

    @Test
    public void testCompileMeetingLockAfterViewDropLeavesNoPendingBusyRetry() throws Exception {
        // DROP VIEW runs while a compile of the view is in flight, and the compile then meets the
        // lock of t's readers. The view has no state any more, so it gets no busy retry.
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                setCurrentMicros(CREATE_MICROS);
                createTableWithRows("t");
                createTableWithRows("t2");
                createView("vj", "SELECT x.*, y.a a2 FROM t x JOIN t2 y ON x.a = y.a");
                setCurrentMicros(ALTER_MICROS);
                execute("ALTER TABLE t ADD COLUMN b INT");
                // The optimiser borrows and returns t's reader, then the run parks at its first
                // borrow of t2. Code generation borrows t again once the run goes on.
                final ViewCompileParker run = ViewCompileParker.ofQueueDrain(engine, "t2", PoolListener.SRC_READER);
                engine.setPoolListener(run);
                try {
                    run.start();
                    try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                        execute("DROP VIEW vj");
                        run.join();
                    }
                } finally {
                    run.join();
                    engine.setPoolListener(null);
                }
                capture.drain();
                capture.assertLogged("view compile met a table lock [view=vj");
                assertPendingCompileBusyRetries("");

                drainViewQueue();
                assertPendingCompileBusyRetries("");
                capture.drain();
                capture.assertNotLogged("cannot compile view");
            } finally {
                capture.stop();
            }
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
            assertViewColumns("pv", "grp:SYMBOL, X:LONG, Y:LONG");

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
            assertViewColumns("pv", "grp:SYMBOL, X:LONG, Y:LONG, Z:LONG");
            assertViewColumns("pv_dep", "grp:SYMBOL, X:LONG");
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
            assertViewColumns("race_v", "a:INT");

            raceWatchedViews.add("race_v");
            raceRecreateTableSql.set("CREATE TABLE race_t (a INT, b INT)");
            engine.enqueueCompileView(engine.verifyTableName("race_v"));
            drainViewQueue();
            recordInvalidWatchedViews(engine);

            Assert.assertNull(raceRecreateTableSql.get());
            Assert.assertEquals("", raceInvalidViews.toString());
            assertViewState("race_v");
            assertViewColumns("race_v", "a:INT, b:INT");
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
            assertViewColumns("race_v", "a:INT");

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
            assertViewColumns("race_v", "a:INT, b:INT, c:INT");
            assertNothingBusy();
        });
    }

    @Test
    public void testConcurrentDrainsMeetingLockLeaveOnePendingBusyRetryPerView() throws Exception {
        // Two drains compile 100 requests for v at once, and every compile meets the lock of t's
        // readers. Whichever drain schedules first, each view ends with one busy retry with the
        // newest update timestamp of the requests, and keeps its state.
        assertMemoryLeak(() -> {
            createTableWithRowsAndViews();
            final int requestCount = 100;
            final TableToken viewToken = engine.verifyTableName("v");
            for (int i = 0; i < requestCount; i++) {
                setCurrentMicros(ALTER_MICROS + i);
                engine.enqueueCompileView(viewToken);
            }
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                final CyclicBarrier start = new CyclicBarrier(2);
                final AtomicReference<Throwable> drainError = new AtomicReference<>();
                final ObjList<Thread> drains = new ObjList<>();
                for (int i = 0; i < 2; i++) {
                    final Thread drain = new Thread(() -> {
                        try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
                            start.await();
                            job.run();
                        } catch (Throwable th) {
                            drainError.compareAndSet(null, th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    }, "view-compile-drain-" + i);
                    drains.add(drain);
                    drain.start();
                }
                for (int i = 0, n = drains.size(); i < n; i++) {
                    drains.getQuick(i).join();
                }
                if (drainError.get() != null) {
                    throw new AssertionError("view compile drain failed", drainError.get());
                }
            }
            assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T10:00:00.000000Z");
            final long newestRequestMicros = ALTER_MICROS + requestCount - 1;
            final long dueMicros = newestRequestMicros + busyRetryTimeoutMicros();
            assertPendingCompileBusyRetries(
                    pendingCompileBusyRetry("v", newestRequestMicros, dueMicros)
                            + pendingCompileBusyRetry("v_dep", newestRequestMicros, dueMicros)
            );
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
                assertViewColumns("v", "a:INT");
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
    public void testDropViewRemovesItsPendingBusyRetry() throws Exception {
        // DROP VIEW removes the pending busy retry of the view. A retry that a drain already queued
        // when the view is dropped finds no view, and compiles nothing.
        assertMemoryLeak(() -> {
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                final long dueMicros = ALTER_MICROS + busyRetryTimeoutMicros();
                // the retry is pending
                createTableWithRowsAndView();
                setCurrentMicros(ALTER_MICROS);
                execute("ALTER TABLE t ADD COLUMN b INT");
                try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                    drainViewQueue();
                    assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));
                    execute("DROP VIEW v");
                    assertPendingCompileBusyRetries("");
                }
                setCurrentMicros(dueMicros);
                drainViewQueue();
                assertPendingCompileBusyRetries("");
                assertViewAbsent("v");
                execute("DROP TABLE t");
                drainViewQueue();

                // the retry is queued
                createTableWithRowsAndView();
                setCurrentMicros(ALTER_MICROS);
                execute("ALTER TABLE t ADD COLUMN b INT");
                try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                    drainViewQueue();
                    assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));
                    setCurrentMicros(dueMicros);
                    engine.getViewStateStore().enqueueDueCompileBusyRetries();
                    assertPendingCompileBusyRetries("");
                    execute("DROP VIEW v");
                }
                drainViewQueue();
                assertPendingCompileBusyRetries("");
                assertViewAbsent("v");
                capture.drain();
                capture.assertNotLogged("cannot compile view");
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
            try {
                ViewCompileParker.ofStartupPass(engine, "t", PoolListener.SRC_READER).executeWhileHolding(capture, "t", () -> execute("DROP TABLE t"), null);
            } finally {
                capture.stop();
            }
            assertTableAbsent("t");
            drainViewQueue();
            assertViewState("v", "table does not exist [table=t]");
        });
    }

    @Test
    public void testLockedViewIsCompiledOncePerRequestAndOncePerDelay() throws Exception {
        // While the lock of t's readers lasts, each request compiles the views once, as without the
        // lock, and the busy retry compiles each view once per busy retry timeout, however many
        // requests met the lock meanwhile. Every drain runs on the thread that holds the lock, and
        // ends. The suite runs at the server default of the timeout.
        assertMemoryLeak(() -> {
            Assert.assertEquals(1_000, configuration.getViewCompileBusyRetryTimeout());
            final long delayMicros = busyRetryTimeoutMicros();
            createTableWithRowsAndViews();
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            countReaderBorrowsOfT();
            final long newestRequestMicros = ALTER_MICROS + delayMicros + 5;
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                // the request of the ALTER compiles v and v_dep once each
                Assert.assertEquals(2, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(
                        pendingCompileBusyRetry("v", ALTER_MICROS, ALTER_MICROS + delayMicros)
                                + pendingCompileBusyRetry("v_dep", ALTER_MICROS, ALTER_MICROS + delayMicros)
                );
                setCurrentMicros(ALTER_MICROS + delayMicros - 1);
                Assert.assertEquals(0, drainAndCountCompilesOverT());

                // the retries meet the lock again, and are due one timeout later
                setCurrentMicros(ALTER_MICROS + delayMicros);
                Assert.assertEquals(2, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(
                        pendingCompileBusyRetry("v", ALTER_MICROS, ALTER_MICROS + 2 * delayMicros)
                                + pendingCompileBusyRetry("v_dep", ALTER_MICROS, ALTER_MICROS + 2 * delayMicros)
                );

                // each failed read of v queues a compile of v, which compiles v_dep and v
                for (int i = 1; i <= 5; i++) {
                    setCurrentMicros(ALTER_MICROS + delayMicros + i);
                    assertExceptionNoLeakCheck("SELECT * FROM v", 14, "table is locked: t");
                }
                setCurrentMicros(ALTER_MICROS + delayMicros + 6);
                Assert.assertEquals(10, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(
                        pendingCompileBusyRetry("v", newestRequestMicros, ALTER_MICROS + 2 * delayMicros)
                                + pendingCompileBusyRetry("v_dep", newestRequestMicros, ALTER_MICROS + 2 * delayMicros)
                );

                setCurrentMicros(ALTER_MICROS + 2 * delayMicros);
                Assert.assertEquals(2, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(
                        pendingCompileBusyRetry("v", newestRequestMicros, ALTER_MICROS + 3 * delayMicros)
                                + pendingCompileBusyRetry("v_dep", newestRequestMicros, ALTER_MICROS + 3 * delayMicros)
                );
                assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewStatus("v_dep", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewColumns("v", "a:INT");
                assertViewColumns("v_dep", "a:INT");
            }

            setCurrentMicros(ALTER_MICROS + 3 * delayMicros);
            Assert.assertEquals(2, drainAndCountCompilesOverT());
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:01.000005Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T11:00:01.000005Z");
            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
            assertPendingCompileBusyRetries("");

            setCurrentMicros(ALTER_MICROS + 4 * delayMicros);
            Assert.assertEquals(0, drainAndCountCompilesOverT());
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:01.000005Z");
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testLongMaxBusyRetryTimeoutKeepsRetryPendingWithoutOverflow() throws Exception {
        // A busy retry timeout whose due time does not fit a long makes the retry due never, whether
        // the timeout overflows in microseconds or only once added to the clock: no drain runs the
        // retry, a later request still compiles the view, and DROP VIEW removes the retry.
        assertMemoryLeak(() -> {
            // the largest value, one below it, and the largest value whose microseconds fit a long
            for (long timeoutMillis : new long[]{Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE / 1_000}) {
                node1.setProperty(PropertyKey.CAIRO_VIEW_COMPILE_BUSY_RETRY_TIMEOUT, timeoutMillis);
                createTableWithRowsAndView();
                setCurrentMicros(ALTER_MICROS);
                execute("ALTER TABLE t ADD COLUMN b INT");
                try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                    drainViewQueue();
                }
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, Long.MAX_VALUE));

                // 9999-12-31T23:59:59.999999Z
                setCurrentMicros(253_402_300_799_999_999L);
                drainViewQueue();
                assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewColumns("v", "a:INT");
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, Long.MAX_VALUE));

                execute("COMPILE VIEW v");
                drainViewQueue();
                assertViewStatus("v", "valid", "", "9999-12-31T23:59:59.999999Z");
                assertViewColumns("v", "a:INT, b:INT");
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, Long.MAX_VALUE));

                execute("DROP VIEW v");
                assertPendingCompileBusyRetries("");
                execute("DROP TABLE t");
                drainViewQueue();
            }
        });
    }

    @Test
    public void testManyViewsMeetingLockLeaveOnePendingBusyRetryEach() throws Exception {
        // The compile that an ALTER queued meets the lock of t's readers for each of 40 views over t.
        // Each view keeps one busy retry, and one drain compiles each view once when they are due.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            final int viewCount = 40;
            for (int i = 0; i < viewCount; i++) {
                createView(manyViewName(i), "SELECT * FROM t");
            }
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            countReaderBorrowsOfT();
            final long dueMicros = ALTER_MICROS + busyRetryTimeoutMicros();
            final StringSink expectedPending = new StringSink();
            for (int i = 0; i < viewCount; i++) {
                expectedPending.put(pendingCompileBusyRetry(manyViewName(i), ALTER_MICROS, dueMicros));
            }
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                Assert.assertEquals(viewCount, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(expectedPending.toString());
                setCurrentMicros(dueMicros - 1);
                Assert.assertEquals(0, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(expectedPending.toString());
            }
            setCurrentMicros(dueMicros);
            Assert.assertEquals(viewCount, drainAndCountCompilesOverT());
            for (int i = 0; i < viewCount; i++) {
                assertViewStatus(manyViewName(i), "valid", "", "2025-06-19T11:00:00.000000Z");
                assertViewColumns(manyViewName(i), "a:INT, b:INT");
            }
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testMergedBusyRetryKeepsNewestRequestTime() throws Exception {
        // A request meets the lock while the view has a pending busy retry: the retry keeps its due
        // time and takes the newer update timestamp. The retry then serves the newer request, which
        // is newer than the state that a request in between set.
        assertMemoryLeak(() -> {
            createTableWithRowsAndView();
            final long delayMicros = busyRetryTimeoutMicros();
            final long dueMicros = ALTER_MICROS + delayMicros;
            setCurrentMicros(ALTER_MICROS);
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                engine.enqueueCompileView(engine.verifyTableName("t"));
                drainViewQueue();
            }
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, dueMicros));

            // 11:00:00.25
            setCurrentMicros(ALTER_MICROS + delayMicros / 4);
            execute("COMPILE VIEW v");
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.250000Z");
            assertViewColumns("v", "a:INT");

            // 11:00:00.5, the ALTER queues a request that meets the lock at 11:00:00.75
            setCurrentMicros(ALTER_MICROS + delayMicros / 2);
            execute("ALTER TABLE t ADD COLUMN b INT");
            setCurrentMicros(ALTER_MICROS + 3 * delayMicros / 4);
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                drainViewQueue();
            }
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS + delayMicros / 2, dueMicros));
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.250000Z");

            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.500000Z");
            assertViewColumns("v", "a:INT, b:INT");
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testNoOpViewStateStoreSchedulesNoBusyRetry() throws Exception {
        // The no-op store keeps no view state, so it gives no view a busy retry, and no compiler
        // consumes its queue.
        assertMemoryLeak(() -> {
            createTableWithRowsAndView();
            final ViewStateStore noOpStore = NoOpViewStateStore.INSTANCE;
            Assert.assertEquals(Numbers.LONG_NULL, noOpStore.scheduleCompileBusyRetry(engine.verifyTableName("v"), ALTER_MICROS));
            setCurrentMicros(Long.MAX_VALUE - 1);
            noOpStore.enqueueDueCompileBusyRetries();
            Assert.assertFalse(noOpStore.tryDequeueCompilerTask(new ViewCompilerTask()));
        });
    }

    @Test
    public void testPromotionMovesOnlyDueBusyRetries() throws Exception {
        // Each view keeps one busy retry, due the busy retry timeout after the view's first lock
        // failure. A failure with an older update timestamp leaves the retry be. A drain start
        // queues the retries that are due, and leaves the others pending until they are due.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createView("v1", "SELECT * FROM t");
            createView("v2", "SELECT * FROM t");
            final ViewStateStore stateStore = engine.getViewStateStore();
            final TableToken v1 = engine.verifyTableName("v1");
            final TableToken v2 = engine.verifyTableName("v2");
            final long delayMicros = busyRetryTimeoutMicros();
            final long v2RequestMicros = ALTER_MICROS + delayMicros / 2;
            final long v1DueMicros = ALTER_MICROS + delayMicros;
            final long v2DueMicros = ALTER_MICROS + 3 * delayMicros / 2;

            setCurrentMicros(ALTER_MICROS);
            Assert.assertEquals(v1DueMicros, stateStore.scheduleCompileBusyRetry(v1, ALTER_MICROS));
            setCurrentMicros(v2RequestMicros);
            Assert.assertEquals(v2DueMicros, stateStore.scheduleCompileBusyRetry(v2, v2RequestMicros));
            // 10:30, older than the update timestamp of the pending retry
            Assert.assertEquals(v1DueMicros, stateStore.scheduleCompileBusyRetry(v1, CREATE_MICROS + 30 * Micros.MINUTE_MICROS));
            assertPendingCompileBusyRetries(
                    pendingCompileBusyRetry("v1", ALTER_MICROS, v1DueMicros)
                            + pendingCompileBusyRetry("v2", v2RequestMicros, v2DueMicros)
            );

            setCurrentMicros(v1DueMicros);
            stateStore.enqueueDueCompileBusyRetries();
            assertCompilerTasks(compilerTask("v1", ALTER_MICROS, true));
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v2", v2RequestMicros, v2DueMicros));

            setCurrentMicros(v2DueMicros - 1);
            stateStore.enqueueDueCompileBusyRetries();
            assertCompilerTasks("");
            assertPendingCompileBusyRetries(pendingCompileBusyRetry("v2", v2RequestMicros, v2DueMicros));

            setCurrentMicros(v2DueMicros);
            stateStore.enqueueDueCompileBusyRetries();
            assertCompilerTasks(compilerTask("v2", v2RequestMicros, true));
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testRequestsMeetingTableLockLeaveOnePendingBusyRetryPerView() throws Exception {
        // The ALTER and two failed reads of v each queue a compile of the views, and each compile
        // meets the lock of t's readers. Each view keeps its state and one busy retry, with the
        // newest update timestamp of the requests, due the busy retry timeout after the first lock
        // failure. The retry compiles the view with that timestamp.
        assertMemoryLeak(() -> {
            createTableWithRowsAndViews();
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            // 11:30
            final long drainMicros = ALTER_MICROS + 30 * Micros.MINUTE_MICROS;
            final long dueMicros = drainMicros + busyRetryTimeoutMicros();
            // 11:20
            final long newestRequestMicros = ALTER_MICROS + 20 * Micros.MINUTE_MICROS;
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                setCurrentMicros(ALTER_MICROS + 10 * Micros.MINUTE_MICROS);
                assertExceptionNoLeakCheck("SELECT * FROM v", 14, "table is locked: t");
                setCurrentMicros(newestRequestMicros);
                assertExceptionNoLeakCheck("SELECT * FROM v", 14, "table is locked: t");
                // a drain on the thread that holds the lock ends
                setCurrentMicros(drainMicros);
                drainViewQueue();
                assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewStatus("v_dep", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewColumns("v", "a:INT");
                assertViewColumns("v_dep", "a:INT");
                assertPendingCompileBusyRetries(
                        pendingCompileBusyRetry("v", newestRequestMicros, dueMicros)
                                + pendingCompileBusyRetry("v_dep", newestRequestMicros, dueMicros)
                );
                setCurrentMicros(dueMicros - 1);
                engine.getViewStateStore().enqueueDueCompileBusyRetries();
                assertCompilerTasks("");
            }
            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T11:20:00.000000Z");
            assertViewStatus("v_dep", "valid", "", "2025-06-19T11:20:00.000000Z");
            assertViewColumns("v", "a:INT, b:INT");
            assertViewColumns("v_dep", "a:INT, b:INT");
            assertPendingCompileBusyRetries("");
        });
    }

    @Test
    public void testStartupViewCompileMeetingLockLeavesPendingBusyRetry() throws Exception {
        // The startup pass compiles every view with the clock as update timestamp. A view whose
        // compile meets the lock of a table it reads keeps its state and gets a busy retry, which a
        // drain runs once it is due.
        assertMemoryLeak(() -> {
            setCurrentMicros(CREATE_MICROS);
            createTableWithRows("t");
            createTableWithRows("t2");
            createView("v", "SELECT * FROM t");
            createView("v2", "SELECT * FROM t2");
            // 11:30
            final long passMicros = ALTER_MICROS + 30 * Micros.MINUTE_MICROS;
            final long dueMicros = passMicros + busyRetryTimeoutMicros();
            setCurrentMicros(passMicros);
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                try (SqlExecutionContext executionContext = engine.createViewCompilerContext(0)) {
                    ViewCompilerJob.compileAllViews(engine, executionContext, new ObjList<>());
                }
                assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewStatus("v2", "valid", "", "2025-06-19T11:30:00.000000Z");
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", passMicros, dueMicros));
            }
            setCurrentMicros(dueMicros);
            drainViewQueue();
            assertViewStatus("v", "valid", "", "2025-06-19T11:30:00.000000Z");
            assertViewColumns("v", "a:INT");
            assertPendingCompileBusyRetries("");
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

    @Test
    public void testZeroBusyRetryTimeoutRetriesAtNextCompileRun() throws Exception {
        // With a busy retry timeout of 0 the retry is due at once. The next drain runs it, but not
        // the drain that scheduled it, so a drain on the thread that holds the lock ends.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_VIEW_COMPILE_BUSY_RETRY_TIMEOUT, 0);
            createTableWithRowsAndView();
            setCurrentMicros(ALTER_MICROS);
            execute("ALTER TABLE t ADD COLUMN b INT");
            countReaderBorrowsOfT();
            try (ReadersLockOfT ignore = new ReadersLockOfT()) {
                Assert.assertEquals(1, drainAndCountCompilesOverT());
                assertViewStatus("v", "valid", "", "2025-06-19T10:00:00.000000Z");
                assertViewColumns("v", "a:INT");
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, ALTER_MICROS));
                // the clock stands still: the retry runs once, meets the lock, and is due again
                Assert.assertEquals(1, drainAndCountCompilesOverT());
                assertPendingCompileBusyRetries(pendingCompileBusyRetry("v", ALTER_MICROS, ALTER_MICROS));
            }
            Assert.assertEquals(1, drainAndCountCompilesOverT());
            assertViewStatus("v", "valid", "", "2025-06-19T11:00:00.000000Z");
            assertViewColumns("v", "a:INT, b:INT");
            assertPendingCompileBusyRetries("");
        });
    }

    // takes every task from the view compile queue, one line per task in queue order, each as
    // compilerTask() prints it
    private static void assertCompilerTasks(String expected) {
        final ViewCompilerTask task = new ViewCompilerTask();
        final StringSink sink = new StringSink();
        while (engine.getViewStateStore().tryDequeueCompilerTask(task)) {
            sink.put(compilerTask(task.tableToken.getTableName(), task.updateTimestamp, task.isBusyRetry));
        }
        Assert.assertEquals(expected, sink.toString());
    }

    private static void assertNothingBusy() {
        Assert.assertEquals(0, engine.getSqlCompilerPool().getBusyCount());
        Assert.assertEquals(0, engine.getBusyReaderCount());
        Assert.assertEquals(0, engine.getBusyWriterCount());
    }

    private static void assertTableAbsent(String tableName) {
        Assert.assertNull(engine.getTableTokenIfExists(tableName));
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

    // a line of assertCompilerTasks(): the name of the table or view, the update timestamp, and
    // whether the task is a busy retry
    private static String compilerTask(String tableName, long updateTimestamp, boolean isBusyRetry) {
        return tableName + '\t' + updateTimestamp + '\t' + isBusyRetry + '\n';
    }

    // counts, from now on, the borrows of t's reader that the optimiser makes on this thread
    private static void countReaderBorrowsOfT() {
        readerBorrowsOfT.set(0);
        readerBorrowCountingThread = Thread.currentThread();
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

    // drains the view compile queue on this thread, and returns the number of compiles of views
    // over t that the drain ran, see countReaderBorrowsOfT()
    private static int drainAndCountCompilesOverT() {
        final int borrows = readerBorrowsOfT.get();
        drainViewQueue();
        return readerBorrowsOfT.get() - borrows;
    }

    // a name of the views of testManyViewsMeetingLockLeaveOnePendingBusyRetryEach(), which sort as
    // their indexes do
    private static String manyViewName(int index) {
        return String.format("v%02d", index);
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

    private void assertViewAbsent(String viewName) throws Exception {
        assertQuery("SELECT view_name FROM views() WHERE view_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize(false)
                .returns("view_name\n");
    }

    // Creates the non-WAL table t with rows 1 and 2 and the view v over it, then runs the DDL
    // while the compile run that CREATE VIEW queued holds t's reader. The run ends once the DDL
    // logs that it waits for it.
    private void createTableAndViewThenExecuteWhileViewCompileHolds(LogCapture capture, String ddl) throws Exception {
        execute("CREATE TABLE t (a INT)");
        execute("INSERT INTO t VALUES (1), (2)");
        Assert.assertFalse(engine.verifyTableName("t").isWal());
        execute("CREATE VIEW v AS (SELECT * FROM t)");
        ViewCompileParker.ofQueueDrain(engine, "t", PoolListener.SRC_READER).executeWhileHolding(capture, "t", () -> execute(ddl), null);
    }

    // creates the non-WAL table t (a INT) with the rows 1 and 2, and the view v over t, at CREATE_MICROS
    private void createTableWithRowsAndView() throws SqlException {
        setCurrentMicros(CREATE_MICROS);
        createTableWithRows("t");
        createView("v", "SELECT * FROM t");
    }

    // as createTableWithRowsAndView(), and the view v_dep over v
    private void createTableWithRowsAndViews() throws SqlException {
        createTableWithRowsAndView();
        createView("v_dep", "SELECT * FROM v");
    }

    // holds the lock of t's readers, which the thread that creates it takes, until closed
    private static class ReadersLockOfT implements QuietCloseable {
        private final TableToken tableToken = engine.verifyTableName("t");

        private ReadersLockOfT() {
            Assert.assertTrue("could not lock the readers of t", engine.lockReadersByTableToken(tableToken));
        }

        @Override
        public void close() {
            engine.unlockReaders(tableToken);
        }
    }
}
