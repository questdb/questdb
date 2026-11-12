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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs a view compile run on a thread of its own and parks the thread on its first borrow of a
 * table from a pool, so that a test can run a DDL against the table while the run holds it. The
 * engine passes its pool events to {@link #onEvent}: a test sets the parker, or a listener that
 * calls it, with {@link CairoEngine#setPoolListener}.
 */
public class ViewCompileParker implements PoolListener {
    // the start of the line that DROP, RENAME or TRUNCATE of a non-WAL table logs when it waits for
    // a view compile run to let go of the table
    public static final String DDL_WAIT_LINE_PREFIX = "waiting for view compilation to release table [table=";
    private static final long HOLDING_TIMEOUT_SECONDS = 30;
    private final Thread compileThread;
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final CountDownLatch holding = new CountDownLatch(1);
    private final AtomicBoolean isJoining = new AtomicBoolean();
    private final byte poolSource;
    private final CountDownLatch release = new CountDownLatch(1);
    private final String tableName;
    private Thread helperThread;

    private ViewCompileParker(String tableName, byte poolSource, Runnable compileRun) {
        this.tableName = tableName;
        this.poolSource = poolSource;
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
     * A run that drains the view compile queue, as a view-compiler worker does.
     */
    public static ViewCompileParker ofQueueDrain(CairoEngine engine, String tableName, byte poolSource) {
        return new ViewCompileParker(tableName, poolSource, () -> {
            try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
                job.run();
            }
        });
    }

    /**
     * The run that compiles every view at server startup.
     */
    public static ViewCompileParker ofStartupPass(CairoEngine engine, String tableName, byte poolSource) {
        return new ViewCompileParker(tableName, poolSource, () -> {
            try (SqlExecutionContext executionContext = engine.createViewCompilerContext(0)) {
                ViewCompilerJob.compileAllViews(engine, executionContext, new ObjList<>());
            }
        });
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
            try (ViewCompilerJob job = new ViewCompilerJob(0, engine)) {
                job.run();
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
     * Releases the run and waits for it and for the helper thread of {@link #releaseAfterLogged}.
     * Fails with the first error either thread met. A helper that is still waiting for its line
     * stops waiting and skips its action.
     */
    public void join() throws InterruptedException {
        isJoining.set(true);
        if (helperThread != null) {
            helperThread.join();
        }
        release.countDown();
        compileThread.join();
        final Throwable th = error.get();
        if (th != null) {
            throw new AssertionError("parked view compile failed", th);
        }
    }

    @Override
    public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
        if (thread == compileThread.threadId()
                && factoryType == poolSource
                && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)
                && Chars.equals(tableToken.getTableName(), tableName)
                && holding.getCount() > 0) {
            holding.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    public void release() {
        release.countDown();
    }

    /**
     * Starts a helper thread that waits until the capture holds the line, runs the action and
     * then releases the run.
     */
    public void releaseAfterLogged(LogCapture capture, String line, @Nullable TestUtils.CheckedRunnable action) {
        helperThread = new Thread(() -> {
            try {
                if (awaitLogged(capture, line) && action != null) {
                    action.run();
                }
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                release.countDown();
                Path.clearThreadLocals();
            }
        }, "view-compile-releaser");
        helperThread.start();
    }

    /**
     * Starts the run and waits until it holds the table.
     */
    public void start() throws InterruptedException {
        compileThread.start();
        if (!holding.await(HOLDING_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            Assert.fail("view compile did not borrow table " + tableName + " [error=" + error.get() + ']');
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
}
