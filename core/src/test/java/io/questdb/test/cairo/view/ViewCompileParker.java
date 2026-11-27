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

package io.questdb.test.cairo.view;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a view compile run on a thread of its own and parks the thread on its first borrow of a
 * table from a pool, so that a test can run a DDL against the table while the run holds it. A
 * parker can park the thread at more tables, one after the other: at its first borrow of each
 * table after it left the park before, see {@link #moveOnToNextPark}. The engine passes its pool
 * events to {@link #onEvent}: a test sets the parker, or a listener that calls it, with
 * {@link CairoEngine#setPoolListener}.
 */
public class ViewCompileParker implements PoolListener {
    // the start of the line that DROP, RENAME or TRUNCATE of a non-WAL table logs when it waits for
    // a view compile run to let go of the table
    public static final String DDL_WAIT_LINE_PREFIX = "waiting for view compilation to release table [table=";
    private static final long HOLDING_TIMEOUT_SECONDS = 30;
    private final Thread compileThread;
    private final CairoEngine engine;
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    // per park, counted down when the thread holds the park's table
    private final ObjList<CountDownLatch> holdings = new ObjList<>();
    private final AtomicBoolean isJoining = new AtomicBoolean();
    private final byte poolSource;
    // the parks that the test let the thread leave, see moveOnToNextPark()
    private final AtomicInteger releasedParkCount = new AtomicInteger();
    // per park, counted down to let the thread leave the park
    private final ObjList<CountDownLatch> releases = new ObjList<>();
    // the tables of the parks, in the order the thread reaches them
    private final ObjList<String> tableNames = new ObjList<>();
    private Thread helperThread;
    // the park that the thread reaches next, read and written on the thread only
    private int nextPark;

    private ViewCompileParker(CairoEngine engine, byte poolSource, Runnable compileRun, String... tableNames) {
        this.engine = engine;
        this.poolSource = poolSource;
        for (String tableName : tableNames) {
            this.tableNames.add(tableName);
            holdings.add(new CountDownLatch(1));
            releases.add(new CountDownLatch(1));
        }
        this.compileThread = new Thread(() -> {
            try {
                compileRun.run();
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "parked-view-compiler");
    }

    // the line that DROP, RENAME or TRUNCATE of the table logs when it waits for a view compile run
    public static String ddlWaitLine(String tableName) {
        return DDL_WAIT_LINE_PREFIX + tableName + ']';
    }

    /**
     * Another session drops the table and, when asked, creates a new table (a INT) under its name that holds the
     * row 42.
     */
    public static void dropTableAsAnotherSession(CairoEngine engine, String tableName, boolean isRecreated) throws SqlException {
        try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
            engine.execute("DROP TABLE " + tableName, executionContext);
            if (isRecreated) {
                engine.execute("CREATE TABLE " + tableName + " (a INT)", executionContext);
                engine.execute("INSERT INTO " + tableName + " VALUES (42)", executionContext);
            }
        }
    }

    /**
     * Creates the table t (a INT) with the rows 1 and 2 and the view vt{scenarioIndex} over it, and runs the DDL of
     * t while a view compile run holds t's reader, so that the DDL waits for the run. The run compiles the new view,
     * then the views over t2, which the method queues by adding the column c{scenarioIndex} to t2: the caller has
     * created t2 and a view over it, and passes an index that names no column of t2 and no view. Once the DDL logs
     * its wait, the run returns t's reader and parks on t2's reader, so that it still runs while nothing holds t. The
     * action runs then, and the run ends.
     */
    public static void executeWhileViewCompileHoldsThenMovesOn(
            CairoEngine engine,
            SqlExecutionContext executionContext,
            LogCapture capture,
            int scenarioIndex,
            TestUtils.LeakProneCode ddl,
            @Nullable TestUtils.CheckedRunnable onWait
    ) throws Exception {
        // the compiles that earlier statements queued must not run first
        runQueueDrain(engine);
        engine.execute("CREATE TABLE t (a INT)", executionContext);
        engine.execute("INSERT INTO t VALUES (1), (2)", executionContext);
        // only a DDL of a non-WAL table waits for view compile runs
        Assert.assertFalse(engine.verifyTableName("t").isWal());
        // queues the compile of the new view, which reads t
        engine.execute("CREATE VIEW vt" + scenarioIndex + " AS (SELECT * FROM t)", executionContext);
        engine.execute("ALTER TABLE t2 ADD COLUMN c" + scenarioIndex + " INT", executionContext);
        final ViewCompileParker run = ofQueueDrainParkingTwice(engine, PoolListener.SRC_READER, "t", "t2");
        run.executeWhileHolding(capture, "t", ddl, () -> {
            run.moveOnToNextPark();
            if (onWait != null) {
                onWait.run();
            }
        });
    }

    /**
     * A run that drains the view compile queue, as a view-compiler worker does.
     */
    public static ViewCompileParker ofQueueDrain(CairoEngine engine, String tableName, byte poolSource) {
        return new ViewCompileParker(engine, poolSource, () -> drainQueue(engine), tableName);
    }

    /**
     * As {@link #ofQueueDrain}, but the run parks twice: at its first borrow of the first table, and then at its
     * first borrow of the second table, see {@link #moveOnToNextPark}.
     */
    public static ViewCompileParker ofQueueDrainParkingTwice(CairoEngine engine, byte poolSource, String firstTableName, String secondTableName) {
        return new ViewCompileParker(engine, poolSource, () -> drainQueue(engine), firstTableName, secondTableName);
    }

    /**
     * The run that compiles every view at server startup.
     */
    public static ViewCompileParker ofStartupPass(CairoEngine engine, String tableName, byte poolSource) {
        return new ViewCompileParker(engine, poolSource, () -> {
            try (SqlExecutionContext executionContext = engine.createViewCompilerContext(0)) {
                ViewCompilerJob.compileAllViews(engine, executionContext, new ObjList<>());
            }
        }, tableName);
    }

    /**
     * Two runs on one thread, one right after the other, as a view-compiler worker runs drains: a drain of the
     * view compile queue, which parks at its first borrow of the first table, and, once it ended, a drain of the
     * compile of the views over the second table, which the thread queues, and which parks at its first borrow of
     * the second table.
     */
    public static ViewCompileParker ofTwoQueueDrains(CairoEngine engine, byte poolSource, String firstTableName, String secondTableName) {
        return new ViewCompileParker(engine, poolSource, () -> {
            try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
                job.run();
                engine.enqueueCompileView(engine.verifyTableName(secondTableName));
                job.run();
            }
        }, firstTableName, secondTableName);
    }

    /**
     * Drains the view compile queue on a thread of its own, as a view-compiler worker does, and
     * waits until the drain ends. A pool listener calls it to have views compile while the thread
     * that fired the event holds, or tries to take, the lock of a table. Fails with the error the
     * drain met.
     */
    public static void runQueueDrain(CairoEngine engine) {
        final AtomicReference<Throwable> drainError = new AtomicReference<>();
        final Thread drainThread = new Thread(() -> {
            try {
                drainQueue(engine);
            } catch (Throwable th) {
                drainError.set(th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "view-compile-drain");
        drainThread.start();
        try {
            drainThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        final Throwable th = drainError.get();
        if (th != null) {
            throw new AssertionError("view compile drain failed", th);
        }
    }

    /**
     * Starts the run, runs the DDL while the run holds its table, and joins the run. Once the capture holds the line
     * that the DDL logs when it waits for the run, the action runs and the run ends. The method starts the capture
     * anew first, so that the wait line of an earlier DDL cannot end the run before this DDL waits. While it runs, the
     * engine passes its pool events to the run and then to the listener set before the call, which the method sets
     * back when it returns.
     */
    public void executeWhileHolding(
            LogCapture capture,
            String ddlTableName,
            TestUtils.LeakProneCode ddl,
            @Nullable TestUtils.CheckedRunnable onWait
    ) throws Exception {
        capture.start();
        final PoolListener listener = engine.getPoolListener();
        engine.setPoolListener(listener == null ? this : (factoryType, thread, tableToken, event, segment, position) -> {
            onEvent(factoryType, thread, tableToken, event, segment, position);
            listener.onEvent(factoryType, thread, tableToken, event, segment, position);
        });
        try {
            start();
            releaseAfterLogged(capture, ddlWaitLine(ddlTableName), onWait);
            ddl.run();
        } finally {
            join();
            engine.setPoolListener(listener);
        }
    }

    // the id of the thread that runs the run
    public long getCompileThreadId() {
        return compileThread.threadId();
    }

    /**
     * Releases the run at every park and waits for it and for the helper thread of
     * {@link #releaseAfterLogged}. Fails with the first error either thread met. A helper that is
     * still waiting for its line stops waiting and skips its action.
     */
    public void join() throws InterruptedException {
        isJoining.set(true);
        if (helperThread != null) {
            helperThread.join();
        }
        release();
        compileThread.join();
        final Throwable th = error.get();
        if (th != null) {
            throw new AssertionError("parked view compile failed", th);
        }
    }

    /**
     * Lets the run leave the park it holds, the first one the test has not let it leave yet, and
     * waits until it holds the table of the next park.
     */
    public void moveOnToNextPark() throws InterruptedException {
        final int park = releasedParkCount.getAndIncrement();
        releases.getQuick(park).countDown();
        awaitHolding(park + 1);
    }

    @Override
    public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
        if (thread == compileThread.threadId()
                && factoryType == poolSource
                && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)
                && nextPark < tableNames.size()
                && Chars.equals(tableToken.getTableName(), tableNames.getQuick(nextPark))) {
            final int park = nextPark++;
            holdings.getQuick(park).countDown();
            try {
                releases.getQuick(park).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Lets the run leave every park, so that it runs to its end.
     */
    public void release() {
        for (int i = 0, n = releases.size(); i < n; i++) {
            releases.getQuick(i).countDown();
        }
    }

    /**
     * Starts a helper thread that releases the run once the capture holds the line that the DDL of
     * the table logs when it waits for a view compile run, see {@link #join}.
     */
    public void releaseAfterDdlWaits(LogCapture capture, String ddlTableName) {
        releaseAfterLogged(capture, ddlWaitLine(ddlTableName), null);
    }

    /**
     * Starts the run and waits until it holds the table of its first park.
     */
    public void start() throws InterruptedException {
        compileThread.start();
        awaitHolding(0);
    }

    private static void drainQueue(CairoEngine engine) {
        try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
            job.run();
        }
    }

    private void awaitHolding(int park) throws InterruptedException {
        if (!holdings.getQuick(park).await(HOLDING_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("view compile did not borrow table " + tableNames.getQuick(park) + " [error=" + error.get() + ']');
        }
    }

    // returns false when the test joins before the line comes
    private boolean awaitLogged(LogCapture capture, String line) {
        while (true) {
            try {
                capture.waitFor(line, 10);
                return true;
            } catch (AssertionError e) {
                if (isJoining.get()) {
                    return false;
                }
            }
        }
    }

    /**
     * Starts a helper thread that waits until the capture holds the line, runs the action and
     * then releases the run.
     */
    private void releaseAfterLogged(LogCapture capture, String line, @Nullable TestUtils.CheckedRunnable action) {
        helperThread = new Thread(() -> {
            try {
                if (awaitLogged(capture, line) && action != null) {
                    action.run();
                }
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                release();
                Path.clearThreadLocals();
            }
        }, "view-compile-releaser");
        helperThread.start();
    }
}
