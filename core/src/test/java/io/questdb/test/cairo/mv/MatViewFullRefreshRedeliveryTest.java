/*******************************************************************************
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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A REFRESH FULL that the user issues to repair a materialized view completes even when the refresh job
 * defers the task that carries it, or re-enqueues its owner under the new name after a rename, while a
 * second refresh worker is draining the shared queue. A full refresh that loses the view lock leaves a handoff on the view, and the lock holder's
 * release enqueues the pending owner.
 * <p>
 * The second worker runs inside the first worker's {@link MatViewState#setOnBeforeUnlockForTesting} hook,
 * so it always dequeues while the first worker still holds the view lock, which is the interleaving that
 * the default 2-4 refresh workers produce when they share one queue. The hook records whether the second
 * worker left a handoff on the view, so a test in which the second worker loses the lock proves that the
 * handoff path ran before the release claims the handoff.
 * <p>
 * A REFRESH FULL that a restart interrupts is not resumed: the view stays invalid until the user runs
 * REFRESH FULL again (docs/row-expiry-limitations.md).
 */
public class MatViewFullRefreshRedeliveryTest extends AbstractCairoTest {
    private static final AtomicBoolean IS_CLOSING = new AtomicBoolean();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // The restart test stops the rebuild the way a shutdown does: the engine reports closing, and the
        // refresh pump cancels on its circuit breaker.
        AbstractCairoTest.engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public boolean isClosing() {
                return IS_CLOSING.get() || super.isClosing();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testFullRefreshDeferredByPolicyChangeCompletesWithSecondWorker() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewState state = createInvalidJoinedView();
            execute("REFRESH MATERIALIZED VIEW mv FULL");

            final AtomicInteger policyChangeCount = new AtomicInteger();
            final PoolListener previousListener = installPolicyChangeOnFirstJoinedTableAccess(policyChangeCount);
            final AtomicInteger secondWorkerRuns = new AtomicInteger();
            final AtomicBoolean isLockedForSecondWorker = new AtomicBoolean();
            final AtomicBoolean isHandoffLeftBySecondWorker = new AtomicBoolean();
            state.setOnBeforeUnlockForTesting(() -> runSecondWorker(state, secondWorkerRuns, isLockedForSecondWorker, isHandoffLeftBySecondWorker));
            try (MatViewRefreshJob firstWorker = new MatViewRefreshJob(0, engine, 0)) {
                firstWorker.run();
            } finally {
                engine.setPoolListener(previousListener);
                state.setOnBeforeUnlockForTesting(null);
            }
            Assert.assertEquals("the policy epoch advances once, during the full refresh compile", 1, policyChangeCount.get());
            Assert.assertEquals("the second worker runs once, at the first worker's release", 1, secondWorkerRuns.get());
            Assert.assertTrue("the second worker runs while the first worker holds the view lock", isLockedForSecondWorker.get());
            Assert.assertTrue("the second worker loses the lock and leaves a handoff", isHandoffLeftBySecondWorker.get());
            Assert.assertFalse("the lock holder claims the handoff", state.hasPendingFullRefreshHandoffForTesting());

            drainWalAndMatViewQueues();
            Assert.assertFalse("the redelivered full refresh consumes its owner", state.hasPendingFullRefreshOwnerForTesting());
            assertViewStatus("valid");
            assertRebuiltJoinedViewRows();
        });
    }

    @Test
    public void testFullRefreshInterruptedByShutdownStaysInvalidUntilRefreshFull() throws Exception {
        setProperty(PropertyKey.CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE, 10);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW v AS (SELECT sym, last(price) AS price, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO base SELECT 'a', x::DOUBLE, timestamp_sequence('2024-01-01', 600_000_000L) FROM long_sequence(1_440)");
            drainWalAndMatViewQueues();

            // TRUNCATE invalidates the view. The new base rows are 'b' prices 1..1440, ten minutes apart from
            // 2024-02-01, so each of the 240 hours holds six of them and its last price is 6 * (hour + 1).
            execute("TRUNCATE TABLE base");
            drainWalAndMatViewQueues();
            execute("INSERT INTO base SELECT 'b', x::DOUBLE, timestamp_sequence('2024-02-01', 600_000_000L) FROM long_sequence(1_440)");
            drainWalAndMatViewQueues();
            assertViewStatus("invalid");

            // REFRESH FULL truncates the view and rebuilds it in commits of about ten rows. The engine starts
            // closing at the second commit after the rebuild has reset the view state, so the rebuild stops
            // with part of the view committed.
            execute("REFRESH MATERIALIZED VIEW v FULL");
            final AtomicBoolean isRebuildStarted = new AtomicBoolean();
            final AtomicInteger commitsAfterReset = new AtomicInteger();
            CairoEngine.setRoleSwitchMintObserver(() -> {
                if (isRebuildStarted.get() && commitsAfterReset.incrementAndGet() == 2) {
                    IS_CLOSING.set(true);
                }
            });
            // The finally block clears the closing flag on every exit path, so that a failure here does not
            // leave a closing engine for the other tests in this class.
            boolean isClosingObserved;
            try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine, 1)) {
                refreshJob.setOnHoldingLockForTesting(() -> isRebuildStarted.set(true));
                refreshJob.run();
            } finally {
                CairoEngine.setRoleSwitchMintObserver(null);
                isClosingObserved = IS_CLOSING.getAndSet(false);
            }
            Assert.assertTrue("the engine starts closing during the rebuild", isClosingObserved);
            // The first engine stops here: it releases the tables so that the second one owns them.
            engine.releaseInactive();
            engine.releaseAllWriters();

            try (
                    CairoEngine restarted = new CairoEngine(configuration);
                    SqlExecutionContext restartedContext = TestUtils.createSqlExecutionCtx(restarted)
            ) {
                restarted.load();
                restarted.buildViewGraphs();
                drainWalAndMatViewQueues(restarted);
                drainWalAndMatViewQueues(restarted);

                // Startup does not resume the interrupted rebuild. The view reports itself invalid, with the
                // reason that made it invalid, and holds only the rows the rebuild committed before it stopped.
                assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'v'")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                view_status\tinvalidation_reason
                                invalid\ttruncate operation
                                """);
                assertQuery("SELECT count() > 0 AND count() < 240 is_partial FROM v")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("is_partial\ntrue\n");

                // REFRESH FULL rebuilds the whole view.
                restarted.execute("REFRESH MATERIALIZED VIEW v FULL", restartedContext);
                drainWalAndMatViewQueues(restarted);
                assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'v'")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("view_status\nvalid\n");
                // 240 hourly rows; the last prices 6, 12, ..., 1440 sum to 6 * (240 * 241 / 2).
                assertQuery("SELECT count() AS cnt, sum(price) AS total, min(ts) AS lo, max(ts) AS hi FROM v")
                        .withEngine(restarted)
                        .withContext(restartedContext)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                cnt\ttotal\tlo\thi
                                240\t173520.0\t2024-02-01T00:00:00.000000Z\t2024-02-10T23:00:00.000000Z
                                """);
                restarted.releaseInactive();
            }
        });
    }

    @Test
    public void testFullRefreshOutOfRetriesCompletesWithSecondWorker() throws Exception {
        // With no retries, one plan made stale by DDL on the joined table exhausts the full refresh's
        // retry budget after it has truncated the view.
        setProperty(PropertyKey.CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dim (k SYMBOL, name VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 9.0, '2024-01-02T00:00:00.000000Z'),
                    ('A', 3.0, '2024-01-03T00:00:00.000000Z')""");
            execute("""
                    INSERT INTO dim VALUES
                    ('A', 'alpha', '2024-01-01T00:00:00.000000Z'),
                    ('B', 'beta', '2024-01-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv WITH BASE base AS (
                        SELECT b.ts, d.name, max(b.v) AS v FROM base b JOIN dim d ON (k) SAMPLE BY 1d
                    ) PARTITION BY DAY""");
            drainWalAndMatViewQueues();
            assertViewStatus("valid");

            final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
            Assert.assertNotNull(state);

            execute("REFRESH MATERIALIZED VIEW mv FULL");

            final AtomicInteger alterCount = new AtomicInteger();
            final AtomicInteger secondWorkerRuns = new AtomicInteger();
            final AtomicBoolean isLockedForSecondWorker = new AtomicBoolean();
            final AtomicBoolean isHandoffLeftBySecondWorker = new AtomicBoolean();
            state.setOnBeforeUnlockForTesting(() -> runSecondWorker(state, secondWorkerRuns, isLockedForSecondWorker, isHandoffLeftBySecondWorker));
            try (MatViewRefreshJob firstWorker = new MatViewRefreshJob(0, engine, 0)) {
                // The full refresh has compiled its plan and fixed its base reader; a column added to the
                // joined table now makes that plan stale when its cursor opens.
                firstWorker.setOnBaseReaderSnapshotForTesting(() -> {
                    if (alterCount.getAndIncrement() == 0) {
                        try {
                            execute("ALTER TABLE dim ADD COLUMN extra INT");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                firstWorker.run();
            } finally {
                state.setOnBeforeUnlockForTesting(null);
            }
            Assert.assertEquals("the joined table changes once, during the full refresh", 1, alterCount.get());
            Assert.assertEquals("the second worker runs once, at the first worker's release", 1, secondWorkerRuns.get());
            Assert.assertTrue("the second worker runs while the first worker holds the view lock", isLockedForSecondWorker.get());
            Assert.assertTrue("the second worker loses the lock and leaves a handoff", isHandoffLeftBySecondWorker.get());
            Assert.assertFalse("the lock holder claims the handoff", state.hasPendingFullRefreshHandoffForTesting());

            drainWalAndMatViewQueues();
            Assert.assertFalse("the redelivered full refresh consumes its owner", state.hasPendingFullRefreshOwnerForTesting());
            assertViewStatus("valid");
            assertQuery("SELECT ts, name, v FROM mv ORDER BY ts")
                    .timestamp("ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tname\tv
                            2024-01-01T00:00:00.000000Z\talpha\t1.0
                            2024-01-02T00:00:00.000000Z\tbeta\t9.0
                            2024-01-03T00:00:00.000000Z\talpha\t3.0
                            """);
        });
    }

    @Test
    public void testFullRefreshWinningSecondLockAttemptClaimsItsHandoff() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewState state = createInvalidJoinedView();
            final TableToken viewToken = engine.verifyTableName("mv");
            execute("REFRESH MATERIALIZED VIEW mv FULL");

            // The test holds the view lock, so the full refresh loses its first attempt. The lock goes free
            // before the refresh publishes its handoff, so the second attempt wins. The refresh then defers on
            // a policy change and re-enqueues its owner, which must be the only task that carries it.
            final AtomicInteger policyChangeCount = new AtomicInteger();
            final PoolListener previousListener = installPolicyChangeOnFirstJoinedTableAccess(policyChangeCount);
            final AtomicBoolean isReleased = new AtomicBoolean();
            Assert.assertTrue(state.tryLock());
            try {
                state.setOnPendingFullRefreshHandoffRequestForTesting(() -> {
                    isReleased.set(true);
                    state.unlock();
                });
                try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                    job.run();
                }
            } finally {
                engine.setPoolListener(previousListener);
                state.setOnPendingFullRefreshHandoffRequestForTesting(null);
                if (!isReleased.get()) {
                    state.unlock();
                }
            }
            Assert.assertTrue("the full refresh loses its first lock attempt", isReleased.get());
            Assert.assertEquals("the policy epoch advances once, during the full refresh compile", 1, policyChangeCount.get());
            Assert.assertFalse("the full refresh claims its own handoff", state.hasPendingFullRefreshHandoffForTesting());
            Assert.assertFalse("the full refresh releases the lock", state.isLocked());
            final Object owner = state.getPendingFullRefreshOwnerForTesting();
            Assert.assertNotNull("the deferred full refresh keeps its owner pending", owner);

            // A release that did not see its own handoff claimed would enqueue the owner a second time.
            final MatViewStateStore store = engine.getMatViewStateStore();
            final ObjList<MatViewRefreshTask> queued = new ObjList<>();
            MatViewRefreshTask task = new MatViewRefreshTask();
            while (store.tryDequeueRefreshTask(task)) {
                queued.add(task);
                task = new MatViewRefreshTask();
            }
            int ownerTaskCount = 0;
            for (int i = 0, n = queued.size(); i < n; i++) {
                final MatViewRefreshTask queuedTask = queued.getQuick(i);
                if (queuedTask.operation == MatViewRefreshTask.FULL_REFRESH) {
                    Assert.assertEquals(viewToken, queuedTask.matViewToken);
                    Assert.assertSame(owner, queuedTask.fullRefreshOwner);
                    ownerTaskCount++;
                }
                store.reenqueueRefreshTask(queuedTask);
            }
            Assert.assertEquals("exactly one queued task carries the deferred owner", 1, ownerTaskCount);

            drainWalAndMatViewQueues();
            Assert.assertFalse("the redelivered full refresh consumes its owner", state.hasPendingFullRefreshOwnerForTesting());
            assertViewStatus("valid");
            assertRebuiltJoinedViewRows();
        });
    }

    @Test
    public void testRenameDuringFullRefreshCompletesWithSecondWorker() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewState state = createInvalidJoinedView();
            final TableToken viewToken = engine.verifyTableName("mv");
            execute("REFRESH MATERIALIZED VIEW mv FULL");

            // The test renames the view before a worker dequeues the full refresh. The task carries the old
            // name, so the refresh cannot open the view's writer and re-enqueues its owner under the new name
            // while it holds the lock. RENAME TABLE rejects materialized views, so the test renames at the
            // storage level, as MatViewTest#testMatViewTableRename does.
            engine.applyTableRename(viewToken, viewToken.renamed("mv_renamed"));

            final AtomicInteger secondWorkerRuns = new AtomicInteger();
            final AtomicBoolean isLockedForSecondWorker = new AtomicBoolean();
            final AtomicBoolean isHandoffLeftBySecondWorker = new AtomicBoolean();
            state.setOnBeforeUnlockForTesting(() -> runSecondWorker(state, secondWorkerRuns, isLockedForSecondWorker, isHandoffLeftBySecondWorker));
            try (MatViewRefreshJob firstWorker = new MatViewRefreshJob(0, engine, 0)) {
                firstWorker.run();
            } finally {
                state.setOnBeforeUnlockForTesting(null);
            }
            Assert.assertEquals("the second worker runs once, at the first worker's release", 1, secondWorkerRuns.get());
            Assert.assertTrue("the second worker runs while the first worker holds the view lock", isLockedForSecondWorker.get());
            Assert.assertTrue("the second worker loses the lock and leaves a handoff", isHandoffLeftBySecondWorker.get());

            drainWalAndMatViewQueues();
            Assert.assertFalse("the redelivered full refresh consumes its owner", state.hasPendingFullRefreshOwnerForTesting());
            Assert.assertFalse("the lock holder claims the handoff", state.hasPendingFullRefreshHandoffForTesting());
            assertQuery("SELECT view_name, view_status FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status
                            mv_renamed\tvalid
                            """);
            assertQuery("SELECT ts, name, v FROM mv_renamed ORDER BY ts")
                    .timestamp("ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tname\tv
                            2024-01-03T00:00:00.000000Z\talpha\t5.0
                            2024-01-04T00:00:00.000000Z\tbeta\t7.0
                            """);
        });
    }

    // Runs inside the first worker's release, before the unlock. The release claims the handoff only after
    // the unlock, so a handoff the second worker leaves is still on the view when its run returns.
    private static void runSecondWorker(MatViewState state, AtomicInteger runs, AtomicBoolean isLocked, AtomicBoolean isHandoffLeft) {
        isLocked.set(state.isLocked());
        runs.incrementAndGet();
        try (MatViewRefreshJob secondWorker = new MatViewRefreshJob(0, engine, 0)) {
            secondWorker.run();
        }
        isHandoffLeft.set(state.hasPendingFullRefreshHandoffForTesting());
    }

    private void assertRebuiltJoinedViewRows() throws Exception {
        assertQuery("SELECT ts, name, v FROM mv ORDER BY ts")
                .timestamp("ts")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        ts\tname\tv
                        2024-01-03T00:00:00.000000Z\talpha\t5.0
                        2024-01-04T00:00:00.000000Z\tbeta\t7.0
                        """);
    }

    private void assertViewStatus(String expectedStatus) throws Exception {
        assertQuery("SELECT view_status FROM materialized_views()")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\n" + expectedStatus + "\n");
    }

    // Creates view mv over base joined with dim, then truncates base and inserts new rows, which leaves mv
    // invalid with its pre-truncate rows until a REFRESH FULL.
    private MatViewState createInvalidJoinedView() throws Exception {
        execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE dim (k SYMBOL, name VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                ('B', 9.0, '2024-01-02T00:00:00.000000Z')""");
        execute("""
                INSERT INTO dim VALUES
                ('A', 'alpha', '2024-01-01T00:00:00.000000Z'),
                ('B', 'beta', '2024-01-01T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
        execute("""
                CREATE MATERIALIZED VIEW mv WITH BASE base AS (
                    SELECT b.ts, d.name, max(b.v) AS v FROM base b JOIN dim d ON (k) SAMPLE BY 1d
                ) PARTITION BY DAY""");
        drainWalAndMatViewQueues();

        execute("TRUNCATE TABLE base");
        execute("""
                INSERT INTO base VALUES
                ('A', 5.0, '2024-01-03T00:00:00.000000Z'),
                ('B', 7.0, '2024-01-04T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
        assertViewStatus("invalid");
        assertQuery("SELECT ts, name, v FROM mv ORDER BY ts")
                .timestamp("ts")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        ts\tname\tv
                        2024-01-01T00:00:00.000000Z\talpha\t1.0
                        2024-01-02T00:00:00.000000Z\tbeta\t9.0
                        """);

        final MatViewState state = engine.getMatViewStateStore().getViewState(engine.verifyTableName("mv"));
        Assert.assertNotNull(state);
        return state;
    }

    // Advances the expiry-policy epoch the first time a pool event touches the joined table dim, which happens
    // while the full refresh compiles the view SQL. TableWriter publishes the same advance when it applies
    // SET EXPIRE or DROP EXPIRE on any table, so the full refresh defers. Returns the listener to restore.
    private PoolListener installPolicyChangeOnFirstJoinedTableAccess(AtomicInteger policyChangeCount) {
        final TableToken dimToken = engine.verifyTableName("dim");
        final AtomicBoolean isPolicyChangeArmed = new AtomicBoolean(true);
        final PoolListener previousListener = engine.getPoolListener();
        engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            if (dimToken.equals(tableToken) && isPolicyChangeArmed.compareAndSet(true, false)) {
                policyChangeCount.incrementAndGet();
                engine.getMetadataCache().publishExpiryPolicyUpdate();
            }
        });
        return previousListener;
    }
}
