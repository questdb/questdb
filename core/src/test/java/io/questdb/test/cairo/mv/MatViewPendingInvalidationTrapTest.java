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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pins the pending-invalidation trap on a plain primary (no role switch): an apply-time {@code INVALIDATE}
 * that defers because a concurrent refresh holds the view lock sets {@code pendingInvalidation} and
 * re-enqueues, the re-dequeued task is then swallowed by {@code invalidateView}'s top guard, and pre-fix
 * nothing finalized it -- the view stayed {@code valid} on disk with stale rows. The fix finalizes the
 * deferral when the lock-holder completes, so the view ends {@code invalid} (visible, recoverable) instead.
 * <p>
 * Each test drives the race deterministically with a {@code @TestOnly} seam that fires while a lock-holder
 * (a refresh, or {@code invalidateView} itself) holds the view lock and marks the view pending, exactly as
 * a losing concurrent {@code invalidateView} would; the holder's completion must finalize it.
 * <p>
 * One branch is left uncovered here: {@code finalizeDeferredInvalidation}'s read-only early-return. It needs
 * {@code engine.isReadOnlyMode()} true at finalize time, which in OSS requires a custom read-only engine that
 * cannot run this write-heavy setup; the demote/promote path that exercises it lives in the enterprise suite.
 */
public class MatViewPendingInvalidationTrapTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        // Materialized views require dev mode; without it the engine installs a no-op state store.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testFullRefreshHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // A base-cascade INVALIDATE deferring during the full-refresh pump: the seam fires once, after
            // resetInvalidState cleared the marker, while fullRefresh holds the view lock. fullRefresh has no
            // success-path markAsValid after the pump, so without a finalize in its finally the marker would
            // survive and freeze the view (silently stale, reporting valid). The finally must finalize it.
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("truncate operation");
                    }
                });

                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a full refresh", fired.get());

            // The deferred invalidation must be finalized: the view ends invalid (not valid-with-stale),
            // carrying the deferral's reason.
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\ttruncate operation
                            """);
        });
    }

    @Test
    public void testInvalidateViewHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            // MANUAL DEFERRED never refreshes incrementally, so a force=false base-cascade INVALIDATE on it
            // hits invalidateView's gate-false decline (lastRefreshBaseTxn == -1): invalidateView holds the
            // lock without minting -- the sixth lock-holder that, pre-fix, never finalized a deferral landing
            // in that window.
            execute("create materialized view price_1h refresh manual deferred as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalQueue();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // Populate via RANGE (no seam armed) so the view is valid with rows, lastRefreshBaseTxn still -1.
            engine.getMatViewStateStore().enqueueRangeRefresh(viewToken, 1L, Long.MAX_VALUE - 1);
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertEquals("precondition: the view has never been incrementally refreshed", -1, state.getLastRefreshBaseTxn());
            Assert.assertFalse("precondition: the view is valid before the cascade", state.isInvalid());

            // The seam fires inside invalidateView's gate-false lock-hold and marks the view pending, modelling
            // a second INVALIDATE deferring against it. invalidateView's finally must finalize that deferral so
            // the view ends invalid, not frozen-pending-and-valid.
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("truncate operation");
                    }
                });

                // A rows-affected base UPDATE enqueues a force=false base-cascade INVALIDATE for the view.
                execute("update base_price set amount = 7;");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired inside invalidateView", fired.get());

            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\ttruncate operation
                            """);
        });
    }

    @Test
    public void testLockContendedInvalidationDefersWithReason() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(state);

            // Hold the view lock from the test thread to simulate a concurrent refresh worker. The latch
            // is a non-reentrant AtomicBoolean, so the refresh job's invalidateView tryLock() fails exactly
            // as it would against a real second worker.
            Assert.assertTrue(state.tryLock());
            try {
                execute("update base_price set amount = 42;"); // rows-affected UPDATE -> apply-time INVALIDATE
                drainWalQueue();           // apply the UPDATE -> enqueue the INVALIDATE
                drainMatViewQueue(engine); // process it: invalidateView defers (we hold the lock) + guard swallow

                // The real defer site must record the cause so a later finalize can mint with it.
                Assert.assertTrue("invalidation should have deferred", state.isPendingInvalidation());
                Assert.assertEquals("update operation", state.getPendingInvalidationReason());
                // The deferral alone must not mint: the view is still valid on disk while pending in memory.
                Assert.assertFalse("deferral alone must not mark the view invalid", state.isInvalid());
            } finally {
                state.unlock();
            }
        });
    }

    @Test
    public void testMultipleDependentViewsEachFinalizeDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("create materialized view price_30m as (" +
                    "select sym, last(price) as price, ts from base_price sample by 30m" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            final MatViewState state1h = engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_1h"));
            final MatViewState state30m = engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_30m"));
            Assert.assertNotNull(state1h);
            Assert.assertNotNull(state30m);

            // Two dependents on one base drive refreshDependentViewsIncremental's loop with N > 1. The seam
            // runs inside refreshIncremental0, once per locked view, and marks whichever view currently holds
            // its lock pending (one-shot per view), modelling an INVALIDATE deferring against each in turn.
            // Each loop iteration's finally must finalize its own deferral independently.
            final AtomicBoolean fired1h = new AtomicBoolean();
            final AtomicBoolean fired30m = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (state1h.isLocked() && fired1h.compareAndSet(false, true)) {
                        state1h.markAsPendingInvalidation("update operation");
                    } else if (state30m.isLocked() && fired30m.compareAndSet(false, true)) {
                        state30m.markAsPendingInvalidation("update operation");
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired for price_1h", fired1h.get());
            Assert.assertTrue("the seam must have fired for price_30m", fired30m.get());

            // Both dependents finalize independently: each ends invalid, not frozen.
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views order by view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tupdate operation
                            price_30m\tbase_price\tinvalid\tupdate operation
                            """);
        });
    }

    @Test
    public void testNullReasonMarkerIsNotFinalizedAsInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // A null-reason marker is the full-refresh reschedule (markAsPendingInvalidation() with no reason,
            // see fullRefresh), NOT a deferred invalidation. finalize must leave it untouched -- it belongs to
            // the queued FULL refresh -- and must not mint the view invalid.
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation(); // no reason -> full-refresh reschedule marker
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", fired.get());

            // finalize saw a null reason and returned early: the marker is left for the full refresh and the
            // view stays valid, not spuriously invalidated.
            Assert.assertTrue("finalize must leave the full-refresh marker in place", state.isPendingInvalidation());
            Assert.assertNull("the full-refresh marker carries no invalidation reason", state.getPendingInvalidationReason());
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);
        });
    }

    @Test
    public void testRangeOnlyPopulatedViewFinalizesDeferredInvalidationToInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            // A MANUAL view never auto-refreshes incrementally, so lastRefreshBaseTxn stays -1 even after a
            // user RANGE refresh populates rows (rangeRefreshSuccess does not advance lastRefreshBaseTxn).
            // This is the frozen-branch class: finalize used to early-return on lastRefreshBaseTxn == -1 and
            // leave the view pending forever (silently stale while reporting valid).
            execute("create materialized view price_1h refresh manual deferred as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalQueue(); // apply the base rows; a MANUAL view does not refresh on base writes

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);
            Assert.assertEquals("precondition: the view has never been incrementally refreshed", -1, state.getLastRefreshBaseTxn());

            // Simulate a base-cascade INVALIDATE deferring while a user RANGE refresh holds the lock on this
            // range-only view. The range refresh completes (lastRefreshBaseTxn stays -1) and its finally must
            // finalize the deferral. The re-enqueued INVALIDATE re-delivers force=true and mints, so the view
            // ends cleanly invalid, not frozen-pending-and-valid.
            final AtomicBoolean fired = new AtomicBoolean();
            final AtomicLong baseTxnAtSeam = new AtomicLong(Long.MIN_VALUE);
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        baseTxnAtSeam.set(state.getLastRefreshBaseTxn());
                        state.markAsPendingInvalidation("truncate operation");
                    }
                });

                engine.getMatViewStateStore().enqueueRangeRefresh(viewToken, 1L, Long.MAX_VALUE - 1);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a range refresh", fired.get());
            // The seam fired inside rangeRefresh while the view had never been incrementally refreshed, so
            // finalize ran on the lastRefreshBaseTxn == -1 branch (rangeRefreshSuccess never advances it).
            Assert.assertEquals("seam must fire while lastRefreshBaseTxn is still -1", -1, baseTxnAtSeam.get());

            // The deferred invalidation is finalized even on the lastRefreshBaseTxn == -1 branch: the view
            // ends invalid (not frozen-pending), carrying the deferral's reason.
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\ttruncate operation
                            """);
        });
    }

    @Test
    public void testRangeRefreshHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // Simulate a concurrent INVALIDATE deferring mid-range-refresh: the seam fires once while
            // rangeRefresh holds the view lock and marks the view pending, exactly as a losing invalidateView
            // would. The range-refresh completion must finalize the deferred invalidation.
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                    }
                });

                // Enqueue a range refresh covering all base table data. The seam fires inside
                // rangeRefresh (while holding the lock) and marks the view pending; the refresh
                // then completes and the finally block must finalize the deferred invalidation.
                engine.getMatViewStateStore().enqueueRangeRefresh(viewToken, 1L, Long.MAX_VALUE - 1);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a range refresh", fired.get());

            // The deferred invalidation must be finalized: the view ends invalid (not valid-with-stale),
            // carrying the deferral's reason.
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tupdate operation
                            """);
        });
    }

    @Test
    public void testRefreshHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // A concurrent apply-time INVALIDATE deferring mid-refresh: the seam fires once, while the
            // refresh holds the view lock, and marks the view pending exactly as a losing invalidateView
            // would (markAsPendingInvalidation + the re-enqueued task swallowed by the guard).
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                    }
                });

                // A base write triggers an incremental refresh of the view; the seam marks it pending
                // mid-refresh, and the refresh completion must finalize that deferred invalidation.
                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", fired.get());

            // The deferred invalidation must be finalized: the view ends invalid (not valid-with-stale),
            // carrying the deferral's reason.
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tupdate operation
                            """);
        });
    }

    @Test
    public void testSingleViewIncrementalRefreshHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // A base insert routes through the base-keyed refreshDependentViewsIncremental loop. A VIEW-keyed
            // enqueueIncrementalRefresh instead drives the single-view refreshIncremental holder, whose own
            // finally must finalize a deferral too. The seam fires in the shared refreshIncremental0.
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                    }
                });

                engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a single-view incremental refresh", fired.get());

            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tupdate operation
                            """);
        });
    }

    @Test
    public void testUpdateRefreshIntervalsHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalAndMatViewQueues();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // Simulate a concurrent INVALIDATE deferring while an interval-update task holds the view lock:
            // the seam fires once (updateRefreshIntervals holds the lock) and marks the view pending,
            // exactly as a losing invalidateView would. The interval-update completion must finalize the
            // deferred invalidation.
            final AtomicBoolean fired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (fired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                    }
                });

                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during an interval-update task", fired.get());

            // The deferred invalidation must be finalized: the view ends invalid (not valid-with-stale),
            // carrying the deferral's reason.
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tinvalid\tupdate operation
                            """);
        });
    }
}
