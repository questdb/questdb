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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewCommitFence;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * A named prepared statement that waits for a busy materialized view latch, re-executed over pg-wire, and a
 * role-switch writer (a primary-to-replica demote) that arrives meanwhile. pg-wire re-executes a prepared
 * {@code REFRESH MATERIALIZED VIEW} or {@code ALTER MATERIALIZED VIEW} through {@code executeFenced()}, the demote
 * fence of the statements it refuses on a read-only node. The holder of the view latch, a refresh that commits under
 * it, takes the role-switch read lock through {@link MatViewCommitFence} on its own thread, and a pending writer
 * makes that read lock wait. The writer must not wait for the statement's latch wait: the statement's timeout, which
 * is unbounded here as with {@code query.timeout=0}, is no bound on a demote, and every fenced commit on the node
 * waits behind the pending writer meanwhile.
 * <p>
 * A re-executed REFRESH that waits for the view's WAL to apply a pending ALTER ... AS holds the same read lock, and must
 * not hold the writer off either.
 * <p>
 * Each latch test plays the latch holder and the writer on two threads and orders them with latches. The holder stands for
 * a refresh job's backlog update or data commit: it holds the latch, takes the commit fence once the writer is
 * pending, and releases the latch after it. The writer waits up to {@link #DEMOTE_BOUND_SECONDS}, which only a stall
 * reaches; when it gives up, the cycle unwinds and the test reports the stall.
 */
public class PGMatViewRoleSwitchTest extends BasePGTest {
    private static final String ALTER_WITH_BASE2_SQL = "ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h)";
    private static final long DEMOTE_BOUND_SECONDS = 30;
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG";
    // ten times the hourly last price, a SQL whose rows tell it from the view's own
    private static final String TEN_TIMES_SQL = "SELECT ts, sym, last(price) * 10 AS price FROM base SAMPLE BY 1h";
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;

    @Test
    public void testDemoteDoesNotWaitForReExecutedAlterWithBasePollingLatch() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> {
            createBaseWithRefreshedView();
            execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base2 VALUES ('c', 5.0, '2024-01-02T00:10:00.000000Z')");
            drainWalAndMatViewQueues();
            try (PreparedStatement alter = connection.prepareStatement(ALTER_WITH_BASE2_SQL)) {
                // the first execution runs at parse time, outside the demote fence
                alter.execute();
                drainWalAndMatViewQueues();
                assertViewStatus("invalid", "base table changed");
                Assert.assertEquals("base2", viewDefinition().getBaseTableName());
                // back on the first base, so that the second execution changes the base again and takes the latch
                try (Statement statement = connection.createStatement()) {
                    statement.execute("ALTER MATERIALIZED VIEW price_1h WITH BASE base AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h)");
                }
                drainWalAndMatViewQueues();
                Assert.assertEquals("base", viewDefinition().getBaseTableName());

                final String error = executeWhileLatchHolderAndDemoteRace(alter);
                drainWalAndMatViewQueues();
                if (error == null) {
                    assertViewStatus("invalid", "base table changed");
                    Assert.assertEquals("base2", viewDefinition().getBaseTableName());
                } else {
                    TestUtils.assertContains(error, "materialized view is busy, retry later [view=price_1h]");
                    Assert.assertEquals("base", viewDefinition().getBaseTableName());
                }
            }
            assertConnectionUsable(connection);
        });
    }

    @Test
    public void testDemoteDoesNotWaitForReExecutedRefreshWaitingForDefinition() throws Exception {
        // The re-executed REFRESH waits for the view's WAL to apply a pending ALTER ... AS under the role-switch read
        // lock, and stops waiting with "retry later" once the role-switch writer is pending. Nothing applies the view's
        // WAL meanwhile, so only the refusal lets the writer through.
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> {
            createBaseWithRefreshedView();
            try (PreparedStatement refresh = connection.prepareStatement("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL")) {
                // the first execution runs at parse time, outside the demote fence
                refresh.execute();
                drainWalAndMatViewQueues();
                try (Statement statement = connection.createStatement()) {
                    statement.execute("ALTER MATERIALIZED VIEW price_1h AS (" + TEN_TIMES_SQL + ")");
                }
                execute("INSERT INTO base VALUES ('a', 3.0, '2024-01-01T00:20:00.000000Z')");

                final String error = executeWhileDefinitionWaitAndDemoteRace(refresh);
                TestUtils.assertContains(error, "materialized view is busy, retry later [view=price_1h]");
                drainWalAndMatViewQueues();
                Assert.assertEquals(TEN_TIMES_SQL, viewDefinition().getMatViewSql());
                // the IMMEDIATE view refreshed the bucket of the new base row with the new SQL
                final String rows = """
                        ts\tsym\tprice
                        2024-01-01T00:00:00.000000Z\ta\t30.0
                        2024-01-01T01:00:00.000000Z\tb\t2.0
                        """;
                assertViewRows(rows);
                assertViewStatus("valid", "");

                // the statement runs once the definition applied
                refresh.execute();
                drainWalAndMatViewQueues();
                assertViewRows(rows);
                assertViewStatus("valid", "");
            }
            assertConnectionUsable(connection);
        });
    }

    @Test
    public void testDemoteDoesNotWaitForReExecutedResumePollingLatch() throws Exception {
        assertWithPgServer(Mode.EXTENDED, true, -1, (connection, _, _, _) -> {
            createBaseWithRefreshedView();
            invalidate();
            try (PreparedStatement resume = connection.prepareStatement(RESUME_SQL)) {
                // the first execution runs at parse time, outside the demote fence
                resume.execute();
                drainWalAndMatViewQueues();
                assertViewStatus("valid", "");
                invalidate();

                final String error = executeWhileLatchHolderAndDemoteRace(resume);
                drainWalAndMatViewQueues();
                if (error == null) {
                    assertViewStatus("valid", "");
                } else {
                    TestUtils.assertContains(error, "materialized view is busy, retry later [view=price_1h]");
                    assertViewStatus("invalid", "test invalidation");
                    resume.execute();
                    drainWalAndMatViewQueues();
                    assertViewStatus("valid", "");
                }
                assertViewRows(VIEW_ROWS);
            }
            assertConnectionUsable(connection);
        });
    }

    private static void assertConnectionUsable(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("SELECT count() FROM price_1h")) {
            sink.clear();
            assertResultSet("count[BIGINT]\n2\n", sink, rs);
        }
    }

    // Re-executes the prepared statement while a latch holder and a role-switch writer race it, asserts that the
    // writer got through while the statement waited for the latch, and returns the statement's error, null when it
    // succeeded. The statement either succeeds once the holder releases the latch, when it waits without holding the
    // role-switch read lock, or refuses with "retry later", when it stops waiting for a pending writer; both let the
    // writer through.
    private static String executeWhileLatchHolderAndDemoteRace(PreparedStatement preparedStatement) throws Exception {
        final MatViewState state = viewState();
        final TableToken viewToken = viewToken();
        final CountDownLatch isPolling = new CountDownLatch(1);
        final AtomicInteger readHoldsWhilePolling = new AtomicInteger(-1);
        final AtomicBoolean isWriterLocked = new AtomicBoolean();
        final AtomicBoolean isWriterDone = new AtomicBoolean();
        final AtomicBoolean isFenceAcquired = new AtomicBoolean();
        final AtomicReference<Throwable> helperError = new AtomicReference<>();

        Assert.assertTrue(state.tryLock());
        state.setOnResumeLatchBusyForTesting(() -> {
            readHoldsWhilePolling.set(engine.getRoleSwitchReadLockCount());
            isPolling.countDown();
        });
        final Thread writer = new Thread(() -> {
            try {
                if (!isPolling.await(DEMOTE_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("the statement did not poll the busy latch");
                }
                final Lock writeLock = engine.getRoleSwitchWriteLock();
                if (writeLock.tryLock(DEMOTE_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    isWriterLocked.set(true);
                    writeLock.unlock();
                }
            } catch (Throwable th) {
                helperError.compareAndSet(null, th);
            } finally {
                isWriterDone.set(true);
            }
        }, "role-switch-writer");
        final Thread holder = new Thread(() -> {
            try {
                if (!isPolling.await(DEMOTE_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("the statement did not poll the busy latch");
                }
                // the read lock refuses new readers once the writer is pending
                final Lock readLock = engine.getRoleSwitchReadLock();
                while (!isWriterDone.get() && readLock.tryLock()) {
                    readLock.unlock();
                    Os.pause();
                }
                final Lock fence = MatViewCommitFence.acquire(engine);
                isFenceAcquired.set(true);
                fence.unlock();
            } catch (Throwable th) {
                helperError.compareAndSet(null, th);
            } finally {
                try {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                } catch (Throwable th) {
                    helperError.compareAndSet(null, th);
                }
                Path.clearThreadLocals();
            }
        }, "latch-holder");
        writer.start();
        holder.start();

        String error = null;
        try {
            preparedStatement.execute();
        } catch (PSQLException e) {
            error = e.getMessage();
        } finally {
            isPolling.countDown();
            writer.join();
            holder.join();
            state.setOnResumeLatchBusyForTesting(null);
        }
        if (helperError.get() != null) {
            throw new AssertionError(helperError.get());
        }
        Assert.assertTrue(
                "the role-switch writer waited " + DEMOTE_BOUND_SECONDS + "s for a prepared statement that polled the view latch"
                        + " [roleSwitchReadHoldsWhilePolling=" + readHoldsWhilePolling.get() + ", statementError=" + error + ']',
                isWriterLocked.get()
        );
        Assert.assertTrue(isFenceAcquired.get());
        Assert.assertFalse(state.isLocked());
        Assert.assertEquals(0, engine.getRoleSwitchReadLockCount());
        return error;
    }

    // Re-executes the prepared statement while it waits for the view's WAL to apply a pending definition, and a
    // role-switch writer arrives once it waits. Returns the statement's error, and asserts that the writer got through.
    // A statement that keeps waiting holds the writer off until the writer gives up and applies the view's WAL, which
    // ends the wait, so the test fails instead of hanging.
    private static String executeWhileDefinitionWaitAndDemoteRace(PreparedStatement preparedStatement) throws Exception {
        final MatViewState state = viewState();
        final CountDownLatch isWaiting = new CountDownLatch(1);
        final AtomicBoolean isWriterLocked = new AtomicBoolean();
        final AtomicReference<Throwable> helperError = new AtomicReference<>();

        state.setOnDefinitionWaitForTesting(isWaiting::countDown);
        final Thread writer = new Thread(() -> {
            try {
                if (!isWaiting.await(DEMOTE_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("the statement did not wait for the pending definition");
                }
                final Lock writeLock = engine.getRoleSwitchWriteLock();
                if (writeLock.tryLock(DEMOTE_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    isWriterLocked.set(true);
                    writeLock.unlock();
                } else {
                    drainWalQueue();
                }
            } catch (Throwable th) {
                helperError.compareAndSet(null, th);
            } finally {
                Path.clearThreadLocals();
            }
        }, "role-switch-writer");
        writer.start();

        String error = null;
        try {
            preparedStatement.execute();
        } catch (PSQLException e) {
            error = e.getMessage();
        } finally {
            isWaiting.countDown();
            writer.join();
            state.setOnDefinitionWaitForTesting(null);
        }
        if (helperError.get() != null) {
            throw new AssertionError(helperError.get());
        }
        Assert.assertTrue(
                "the role-switch writer waited " + DEMOTE_BOUND_SECONDS + "s for a prepared statement that waited for the view's WAL"
                        + " [statementError=" + error + ']',
                isWriterLocked.get()
        );
        Assert.assertEquals(0, engine.getRoleSwitchReadLockCount());
        return error;
    }

    private static MatViewDefinition viewDefinition() {
        return engine.getDependentViewGraph().getViewDefinition(viewToken());
    }

    private static MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private static TableToken viewToken() {
        return engine.verifyTableName("price_1h");
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewStatus(String status, String reason) throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\n" + status + "\t" + reason + "\n");
    }

    private void createBaseWithRefreshedView() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-01T01:10:00.000000Z')
                """);
        execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        assertViewRows(VIEW_ROWS);
        assertViewStatus("valid", "");
    }

    private void invalidate() throws Exception {
        engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
        drainWalAndMatViewQueues();
        assertViewStatus("invalid", "test invalidation");
    }
}
