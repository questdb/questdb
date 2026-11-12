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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewCommitFence;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

/**
 * A statement that waits for a busy view latch while its thread holds the role-switch read lock, as pg-wire's
 * {@code executeFenced()} holds it for a re-executed prepared statement. The latch holder, a refresh that commits
 * under the latch, takes the same read lock through {@link MatViewCommitFence} on its own thread, and a role-switch
 * writer (a demote) that arrives first makes that read lock wait for the writer, which waits for the statement's read
 * hold. The three threads wait on each other until the statement's timer expires. These tests pin how the cycle
 * unwinds then: the statement refuses with "retry later" and writes nothing, its read hold goes away, the writer and
 * the latch holder proceed, and the view takes the statement once the latch is free. Each test plays the three roles
 * on three threads and orders them with latches; the statement's timer expires once the latch holder heads for the
 * fence behind the pending writer.
 * <p>
 * The stall itself, on the real pg-wire path, is in {@code PGMatViewRoleSwitchTest}.
 */
public class MatViewRoleSwitchWaitTest extends AbstractCairoTest {
    private static final String ALTER_WITH_BASE_SQL = "ALTER MATERIALIZED VIEW price_1h WITH BASE base2 AS (SELECT ts, sym, last(price) AS price FROM base2 SAMPLE BY 1h)";
    // A bound on the waits of the helper threads, which only a hang reaches.
    private static final long HANG_BOUND_SECONDS = 60;
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG";
    private static final int VIEW_NAME_POSITION_IN_ALTER = "ALTER MATERIALIZED VIEW ".length();
    private static final int VIEW_NAME_POSITION_IN_REFRESH = "REFRESH MATERIALIZED VIEW ".length();
    private static final String VIEW_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            """;

    @Test
    public void testBaseChangeHoldingReadLockRefusesAtTimeoutBehindPendingWriter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView();
            createBase2();
            final long viewSeqTxn = viewSeqTxn();

            final SqlException e = runStatementHoldingReadLockBehindPendingWriter(ALTER_WITH_BASE_SQL);
            Assert.assertEquals(VIEW_NAME_POSITION_IN_ALTER, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is busy, retry later [view=price_1h]");

            // the refused base change wrote nothing and left the view valid on its base
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertEquals(-1, viewState().getPendingDefinitionSeqTxn());
            Assert.assertFalse(viewState().isInvalid());
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            Assert.assertEquals("base", viewDefinition().getBaseTableName());
            assertViewRows(VIEW_ROWS);

            // the view takes the statement once the latch is free
            execute(ALTER_WITH_BASE_SQL);
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "base table changed");
            Assert.assertEquals("base2", viewDefinition().getBaseTableName());
        });
    }

    @Test
    public void testResumeHoldingReadLockRefusesAtTimeoutBehindPendingWriter() throws Exception {
        assertMemoryLeak(() -> {
            createBaseWithRefreshedView();
            engine.getMatViewStateStore().enqueueInvalidate(viewToken(), "test invalidation");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            final long viewSeqTxn = viewSeqTxn();

            final SqlException e = runStatementHoldingReadLockBehindPendingWriter(RESUME_SQL);
            Assert.assertEquals(VIEW_NAME_POSITION_IN_REFRESH, e.getPosition());
            TestUtils.assertContains(e.getFlyweightMessage(), "materialized view is busy, retry later [view=price_1h]");

            // the refused RESUME wrote nothing and left the view invalid with its reason
            Assert.assertEquals(viewSeqTxn, viewSeqTxn());
            Assert.assertTrue(viewState().isInvalid());
            Assert.assertFalse(viewState().isResuming());
            drainWalAndMatViewQueues();
            assertViewStatus("invalid", "test invalidation");
            assertViewRows(VIEW_ROWS);

            // the view takes the statement once the latch is free, and refreshes again
            execute(RESUME_SQL);
            Assert.assertFalse(viewState().isInvalid());
            execute("INSERT INTO base VALUES ('c', 3.0, '2024-01-01T02:10:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewStatus("valid", "");
            assertViewRows(VIEW_ROWS + "2024-01-01T02:00:00.000000Z\tc\t3.0\n");
        });
    }

    private static NetworkSqlExecutionCircuitBreaker newCircuitBreaker(AtomicBoolean isTimeoutRequested) {
        return new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public int getCircuitBreakerThrottle() {
                return 0;
            }
        }) {
            @Override
            public void statefulThrowExceptionIfTrippedNoThrottle() {
                // the statement's own thread expires its timer, once the test asks for it
                if (isTimeoutRequested.get()) {
                    setTimeout(SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK);
                }
                super.statefulThrowExceptionIfTrippedNoThrottle();
            }

            @Override
            protected boolean testConnection(long fd) {
                return false;
            }
        };
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

    private void createBase2() throws SqlException {
        execute("CREATE TABLE base2 (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base2 VALUES ('c', 5.0, '2024-01-02T00:10:00.000000Z')");
        drainWalAndMatViewQueues();
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

    // Plays the three roles: this thread runs the statement holding the role-switch read lock, a writer thread takes
    // the role-switch write lock once the statement polls the busy latch, and a holder thread, which took the latch
    // first, heads for the commit fence once the writer is pending and releases the latch after it. Returns the
    // statement's refusal, and asserts that the writer and the holder both got through and that no read hold remains.
    private SqlException runStatementHoldingReadLockBehindPendingWriter(String sql) throws Exception {
        final MatViewState state = viewState();
        final TableToken viewToken = viewToken();
        final CountDownLatch isPolling = new CountDownLatch(1);
        final AtomicBoolean isTimeoutRequested = new AtomicBoolean();
        final AtomicBoolean isWriterLocked = new AtomicBoolean();
        final AtomicBoolean isWriterDone = new AtomicBoolean();
        final AtomicBoolean isFenceAcquired = new AtomicBoolean();
        final AtomicReference<Throwable> error = new AtomicReference<>();

        Assert.assertTrue(state.tryLock());
        state.setOnResumeLatchBusyForTesting(isPolling::countDown);
        final Thread writer = new Thread(() -> {
            try {
                if (!isPolling.await(HANG_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("the statement did not poll the busy latch");
                }
                final Lock writeLock = engine.getRoleSwitchWriteLock();
                if (writeLock.tryLock(HANG_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    isWriterLocked.set(true);
                    writeLock.unlock();
                }
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                isWriterDone.set(true);
            }
        }, "role-switch-writer");
        final Thread holder = new Thread(() -> {
            try {
                if (!isPolling.await(HANG_BOUND_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("the statement did not poll the busy latch");
                }
                // the read lock refuses new readers once the writer is pending
                final Lock readLock = engine.getRoleSwitchReadLock();
                while (!isWriterDone.get() && readLock.tryLock()) {
                    readLock.unlock();
                    Os.pause();
                }
                // the statement's timer expires while the holder waits at the fence behind the pending writer
                isTimeoutRequested.set(true);
                final Lock fence = MatViewCommitFence.acquire(engine);
                isFenceAcquired.set(true);
                fence.unlock();
            } catch (Throwable th) {
                error.compareAndSet(null, th);
            } finally {
                try {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                } catch (Throwable th) {
                    error.compareAndSet(null, th);
                }
                Path.clearThreadLocals();
            }
        }, "latch-holder");
        writer.start();
        holder.start();

        SqlException refusal = null;
        try (SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 1);
             NetworkSqlExecutionCircuitBreaker circuitBreaker = newCircuitBreaker(isTimeoutRequested)) {
            context.with(circuitBreaker);
            circuitBreaker.resetTimer();
            final Lock readLock = engine.getRoleSwitchReadLock();
            readLock.lock();
            try {
                execute(sql, context);
            } catch (SqlException e) {
                refusal = e;
            } finally {
                readLock.unlock();
            }
        } finally {
            isPolling.countDown();
            writer.join();
            holder.join();
            state.setOnResumeLatchBusyForTesting(null);
        }
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        Assert.assertNotNull("the statement must refuse once its timer expires", refusal);
        Assert.assertTrue("the role-switch writer must get through once the statement refused", isWriterLocked.get());
        Assert.assertTrue("the latch holder must get through the commit fence once the writer left", isFenceAcquired.get());
        Assert.assertFalse(state.isLocked());
        Assert.assertEquals(0, engine.getRoleSwitchReadLockCount());
        return refusal;
    }

    private MatViewDefinition viewDefinition() {
        return engine.getDependentViewGraph().getViewDefinition(viewToken());
    }

    private long viewSeqTxn() {
        return engine.getTableSequencerAPI().getTxnTracker(viewToken()).getSeqTxn();
    }

    private MatViewState viewState() {
        return engine.getMatViewStateStore().getViewState(viewToken());
    }

    private TableToken viewToken() {
        return engine.verifyTableName("price_1h");
    }
}
