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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
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
 * Most tests drive the race deterministically with a {@code @TestOnly} seam that fires while a lock-holder
 * (a refresh, or {@code invalidateView} itself) holds the view lock and marks the view pending -- the marker
 * half of what a losing concurrent {@code invalidateView} issues; the holder's completion must finalize it.
 * {@link #testRefreshHoldingLockFinalizesDeferredInvalidationWithQueuedTask()} exercises the complete
 * defer-site pair (marker plus the re-enqueued task the guard swallows),
 * {@link #testLockContendedInvalidationDefersWithReason()} exercises the real defer site itself, and the
 * {@code testFullRefreshLosingLock*} pair drives the real reschedule-sentinel site (a full refresh losing
 * the latch) end-to-end.
 * <p>
 * {@code finalizeDeferredInvalidation}'s read-only early-return is pinned here by
 * {@link #testReadOnlyEngineLeavesDeferredInvalidationUntouched()}: a mutable-flag engine (injected via
 * {@link AbstractCairoTest#engineFactory}) lets the test turn {@code isReadOnlyMode()} true under a held view
 * latch and then route the unlock through {@code finalizeAndUnlock}, standing in for a demote that turns the
 * node read-only while a lock-holder completes. The OSS base engine only ever reads the static
 * {@code isReadOnlyInstance()} flag, so the flip is synthetic -- but the branch it drives is real, and its
 * production trigger (an in-place role switch) lives in the enterprise demote suite
 * ({@code MatViewInvalidateQuiesceWedgeTest}, {@code MatViewInvalidateRepromoteLosslessTest},
 * {@code MatViewSwitchInvariantsTest}), which drives the read-only deferral end-to-end through a live demote
 * cascade.
 * <p>
 * One read-only branch stays OSS-uncovered: {@code invalidateView}'s {@code isSelfDeferred} skip. It needs
 * the flag to flip DURING the in-flight WAL-writer mint so the writer acquire throws an authorization error,
 * a race the OSS read-only-agnostic {@code getWalWriter} cannot produce (only the enterprise override refuses
 * on {@code isReadOnlyMode()}). That path is enterprise-only-reachable.
 */
public class MatViewPendingInvalidationTrapTest extends AbstractCairoTest {

    // A test-controlled read-only flip. The OSS engine reads a static isReadOnlyInstance() flag; the
    // injected engine below ORs this in so a test can turn the node read-only mid-hold, standing in for the
    // enterprise demote that toggles isReadOnlyMode() dynamically. Reset to false before every test.
    private static final AtomicBoolean readOnly = new AtomicBoolean();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject an engine whose isReadOnlyMode() follows the readOnly flag, so a lock-holder can be turned
        // read-only mid-hold without a live role switch. When readOnly is false (setup, and every other
        // test) this is identical to the base engine.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public boolean isReadOnlyMode() {
                return readOnly.get() || super.isReadOnlyMode();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Override
    public void setUp() {
        super.setUp();
        // Materialized views require dev mode; without it the engine installs a no-op state store.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        readOnly.set(false);
    }

    @Test
    public void testClosedStateLeavesDeferredInvalidationUntouched() throws Exception {
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

            // Pins finalizeDeferredInvalidation's isClosed early-return -- the teardown race the
            // invalidateView guard comment names ("a closed state dies with its marker and finalize
            // skips it anyway"). Model a lock-holder completing while the owner store tears down with
            // a deferral parked on the view: hold the latch as a refresh would, mark the view pending
            // (the marker a losing concurrent invalidateView left), close() the state mid-hold (close
            // cannot take the held latch, so it only flags closed and leaves the parked factory for
            // the holder), then route the unlock through finalizeAndUnlock -- the shared tail every
            // holder uses. finalize must skip: the marker dies with the discarded state and nothing
            // may be enqueued, while the unlock tail (tryCloseIfClosed) still frees the parked factory.
            Assert.assertTrue(state.tryLock());
            state.markAsPendingInvalidation("update operation");
            state.close();
            Assert.assertTrue(state.isClosed());
            MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);

            // finalize left the marker untouched (were the isClosed clause absent it would have
            // cleared it here and queued a force=true INVALIDATE against the discarded state).
            Assert.assertTrue("closed-state finalize must leave the deferral marker set", state.isPendingInvalidation());
            Assert.assertEquals("update operation", state.getPendingInvalidationReason());

            // Proof that finalize queued nothing: a full drain mints no invalidation and the view
            // stays valid on disk.
            drainMatViewQueue(engine);
            drainWalQueue();
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);
        });
    }

    // Hammers an off-latch reason-bearing deferral against an off-latch clear -- the two writers that, with
    // the former composite written in opposite field orders, could tear to (pending=true, reason=null) and
    // strand the view valid+stale with no self-heal. With a single atomic marker no torn pair can exist AT
    // REST: after each round quiesces, a pending marker must still carry its (only ever non-null) reason.
    //
    // Scope, stated honestly: this resting-state check catches a two-volatile revert whose write orders can
    // leave the torn pair as the FINAL state (the orders the original composite had); a revert with aligned
    // write orders produces only transient torn states that no black-box reader can pin either -- with two
    // separate getters, a clear landing between a reader's isPendingInvalidation() and
    // getPendingInvalidationReason() calls yields the same (true, null) observation on perfectly correct
    // code. The structural guarantee is that both getters derive from the single
    // MatViewState#pendingInvalidationMarker field; the concurrently-verifiable marker property is pinned by
    // testConcurrentSentinelMarkNeverDemotesReasonDeferral.
    @Test
    public void testConcurrentDeferAndClearNeverTearsToReasonlessPending() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (sym varchar, price double, amount int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            final int rounds = 50;
            final int iterations = 2_000;
            for (int r = 0; r < rounds; r++) {
                final AtomicBoolean go = new AtomicBoolean();
                final Runnable gate = () -> {
                    while (!go.get()) {
                        Thread.onSpinWait();
                    }
                };
                final Thread setter = new Thread(() -> {
                    gate.run();
                    for (int i = 0; i < iterations; i++) {
                        state.markAsPendingInvalidation("update operation");
                    }
                }, "defer-setter");
                final Thread clearer = new Thread(() -> {
                    gate.run();
                    for (int i = 0; i < iterations; i++) {
                        state.clearPendingInvalidation();
                    }
                }, "defer-clearer");
                setter.start();
                clearer.start();
                go.set(true);
                setter.join();
                clearer.join();

                // Quiesced -> a single stable resting state. The only reason ever written is non-null, so a
                // pending marker MUST report it; a reasonless pending marker would mean the torn write is back.
                if (state.isPendingInvalidation()) {
                    Assert.assertEquals(
                            "reason-bearing pending marker tore to a null reason -- the torn composite is back",
                            "update operation",
                            state.getPendingInvalidationReason()
                    );
                } else {
                    Assert.assertNull(state.getPendingInvalidationReason());
                }
            }

            // Leave the view clean for teardown.
            state.markAsValid();
        });
    }

    // Pins the keep-strongest no-arg mark under a live race: a losing fullRefresh's sentinel write runs
    // against reason-bearing deferrals while a reader samples the reason with SINGLE reads (no two-read
    // skew). Off-latch writers never clear and the sentinel CASes only into an empty marker, so once a
    // reason lands the reason stays non-null for the rest of the round; a reason -> null observation means
    // the plain last-write-wins sentinel overwrite (the demotion race) is back. Green deterministically on
    // the CAS code; the pre-CAS overwrite fails it within a few rounds.
    @Test
    public void testConcurrentSentinelMarkNeverDemotesReasonDeferral() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (sym varchar, price double, amount int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            final int rounds = 50;
            final int iterations = 1_000;
            for (int r = 0; r < rounds; r++) {
                state.clearPendingInvalidation(); // single-threaded between rounds; off-latch code never clears
                final AtomicBoolean go = new AtomicBoolean();
                final AtomicBoolean stop = new AtomicBoolean();
                final AtomicBoolean hasDemoted = new AtomicBoolean();
                final Runnable gate = () -> {
                    while (!go.get()) {
                        Thread.onSpinWait();
                    }
                };
                final Thread reasonSetter = new Thread(() -> {
                    gate.run();
                    for (int i = 0; i < iterations; i++) {
                        state.markAsPendingInvalidation("update operation");
                    }
                }, "reason-setter");
                final Thread sentinelSetter = new Thread(() -> {
                    gate.run();
                    for (int i = 0; i < iterations; i++) {
                        state.markAsPendingInvalidation(); // the losing-fullRefresh reschedule write
                    }
                }, "sentinel-setter");
                final Thread reader = new Thread(() -> {
                    gate.run();
                    boolean hasSeenReason = false;
                    while (!stop.get()) {
                        if (state.getPendingInvalidationReason() != null) {
                            hasSeenReason = true;
                        } else if (hasSeenReason) {
                            hasDemoted.set(true);
                            return;
                        }
                        Thread.onSpinWait();
                    }
                }, "reason-reader");
                reasonSetter.start();
                sentinelSetter.start();
                reader.start();
                go.set(true);
                reasonSetter.join();
                sentinelSetter.join();
                stop.set(true);
                reader.join();

                Assert.assertFalse("the sentinel mark demoted a reason-bearing deferral to a null reason", hasDemoted.get());
                // A reason landed in every round; keep-strongest must leave it as the resting marker.
                Assert.assertEquals("update operation", state.getPendingInvalidationReason());
            }

            // Leave the view clean for teardown.
            state.markAsValid();
        });
    }

    @Test
    public void testDeclinedInvalidationDoesNotCascadeToChainedView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (" +
                    "sym varchar, price double, amount int, ts timestamp" +
                    ") timestamp(ts) partition by DAY WAL");
            // A MANUAL DEFERRED parent never refreshes incrementally, so lastRefreshBaseTxn stays -1 and an
            // apply-time base-table invalidation (delivered force=false) declines to invalidate it.
            execute("create materialized view price_1h refresh manual deferred as (" +
                    "select sym, last(price) as price, ts from base_price sample by 1h" +
                    ") partition by DAY");
            // A chained child on the parent view: its only invalidation route is the parent's cascade.
            execute("create materialized view price_1d refresh manual deferred as (" +
                    "select sym, last(price) as price, ts from price_1h sample by 1d" +
                    ") partition by DAY");
            execute("insert into base_price (sym, price, ts) values" +
                    "('gbpusd', 1.320, '2024-09-10T12:01')" +
                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                    ",('jpyusd', 103.21, '2024-09-10T12:02')");
            drainWalQueue(); // apply the base rows; MANUAL DEFERRED views do not refresh on base writes

            // A rows-affected UPDATE fires an apply-time INVALIDATE task for the base table's dependents.
            execute("update base_price set amount = 42;");
            drainWalQueue();
            drainMatViewQueue(engine);
            drainWalQueue();

            // The parent declines the non-forced invalidation (never incrementally refreshed) and stays
            // valid, so nothing may cascade: the chained child must stay valid too. Pre-fix, the decline
            // still fell through to enqueueInvalidateDependentViews, and the per-child task re-delivered
            // force=true, hard-minting the child invalid under a reason claiming the parent was invalidated.
            assertQuery("select view_name, base_table_name, view_status from materialized_views order by view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1d\tprice_1h\tvalid
                            price_1h\tbase_price\tvalid
                            """);

            // Positive control: a real (force=true, view-scoped) invalidation of the parent must still
            // cascade -- the guard must not suppress the legitimate post-mint cascade.
            engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("price_1h"), "test invalidation");
            drainMatViewQueue(engine);
            drainWalQueue();
            drainMatViewQueue(engine);
            drainWalQueue();

            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views order by view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1d\tprice_1h\tinvalid\tbase materialized view is invalidated
                            price_1h\tbase_price\tinvalid\ttest invalidation
                            """);
        });
    }

    @Test
    public void testDroppedViewLeavesDeferredInvalidationUntouched() throws Exception {
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

            // Pins finalizeDeferredInvalidation's isDropped early-return: a deferral lands mid-hold AND the
            // view is dropped during the same hold. The store's removeViewState marks the state dropped but
            // cannot free the parked factory (the refresh holds the latch), so the holder's finalize must
            // skip the dead deferral (no re-enqueued INVALIDATE for a dropped view) and its unlock tail must
            // free the factory via tryCloseIfDropped -- assertMemoryLeak fails if it leaks.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                        try {
                            execute("drop materialized view price_1h");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", hasFired.get());
            Assert.assertTrue("the drop must reach the state while the refresh holds the latch", state.isDropped());
            Assert.assertTrue("finalize must leave the marker of a dropped view untouched", state.isPendingInvalidation());
            Assert.assertEquals("update operation", state.getPendingInvalidationReason());

            // The view is gone; the stranded marker died with the state and nothing re-enqueued for it.
            assertQuery("select count() from materialized_views")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            0
                            """);
        });
    }

    @Test
    public void testFailedRefreshLeavesDeferredInvalidationUntouched() throws Exception {
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

            // Pins finalizeDeferredInvalidation's isInvalid early-return: a deferral lands mid-hold AND the
            // holding refresh itself fails. The seam marks the view pending, then drops a base column the view
            // SQL needs, so insertAsSelect's recompile fails and refreshFailState marks the view invalid with
            // the compile error. finalize then sees the view already invalid and must return early: no
            // re-enqueued INVALIDATE may overwrite the fail reason, and the marker must survive untouched.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                        try {
                            execute("alter table base_price drop column price");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", hasFired.get());
            Assert.assertTrue("the failed refresh must mark the view invalid", state.isInvalid());
            Assert.assertTrue("finalize must not clear the marker on an already-invalid view", state.isPendingInvalidation());
            Assert.assertEquals("update operation", state.getPendingInvalidationReason());

            // The view carries the refresh-failure reason; the deferred "update operation" never minted.
            assertQuery("select view_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tview_status
                            price_1h\tinvalid
                            """);
            assertQuery("select count() from materialized_views where invalidation_reason = 'update operation'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("""
                            count
                            0
                            """);
        });
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
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("truncate operation");
                    }
                });

                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a full refresh", hasFired.get());

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
    public void testFullRefreshLosingLockArmsSentinelAndRecovers() throws Exception {
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

            // Drives fullRefresh's real losing branch end-to-end: the test thread holds the latch, so the
            // drained FULL_REFRESH task fails tryLock, arms the no-reason reschedule sentinel and republishes
            // itself. The drainer thread keeps spinning on the republished task until the latch frees.
            Assert.assertTrue(state.tryLock());
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                Thread drainer = null;
                try {
                    engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                    drainer = new Thread(() -> drainMatViewQueue(job), "losing-full-refresh-drainer");
                    drainer.start();

                    final long deadlineNanos = System.nanoTime() + 30_000_000_000L;
                    while (!state.isPendingInvalidation()) {
                        if (System.nanoTime() - deadlineNanos > 0) {
                            Assert.fail("the losing full refresh never armed the reschedule sentinel");
                        }
                        Thread.onSpinWait();
                    }
                    // The reschedule is a sentinel, not an invalidation: no reason, view still valid.
                    Assert.assertNull(state.getPendingInvalidationReason());
                    Assert.assertFalse(state.isInvalid());
                } finally {
                    state.unlock();
                    if (drainer != null) {
                        drainer.join();
                    }
                }
            }
            drainWalAndMatViewQueues();

            // The republished full refresh won the freed latch, cleared the sentinel and rebuilt the view.
            Assert.assertFalse("the re-queued full refresh must clear its reschedule sentinel", state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());
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
    public void testFullRefreshLosingLockCannotDemoteReasonDeferral() throws Exception {
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

            // Integration form of the keep-strongest CAS pin: the sentinel writer is fullRefresh's real
            // losing branch, spinning on its republished task while the test thread holds the latch.
            Assert.assertTrue(state.tryLock());
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                Thread drainer = null;
                try {
                    engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                    drainer = new Thread(() -> drainMatViewQueue(job), "losing-full-refresh-drainer");
                    drainer.start();
                    // The sentinel arm proves the losing branch is live and re-arming on every spin.
                    final long deadlineNanos = System.nanoTime() + 30_000_000_000L;
                    while (!state.isPendingInvalidation()) {
                        if (System.nanoTime() - deadlineNanos > 0) {
                            Assert.fail("the losing full refresh never armed the reschedule sentinel");
                        }
                        Thread.onSpinWait();
                    }

                    // A reason-bearing deferral lands mid-hold and upgrades the sentinel. The live sentinel
                    // loop must never demote it back to null; the read window overlaps thousands of losing
                    // fullRefresh passes, so a plain last-write-wins sentinel write fails this immediately.
                    state.markAsPendingInvalidation("update operation");
                    for (int i = 0; i < 10_000; i++) {
                        Assert.assertEquals("the losing full refresh demoted a reason-bearing deferral",
                                "update operation", state.getPendingInvalidationReason());
                    }
                } finally {
                    state.unlock();
                    if (drainer != null) {
                        drainer.join();
                    }
                }
            }
            drainWalAndMatViewQueues();

            // The republished full refresh won the freed latch and rebuilt the view; resetInvalidState wiped
            // the parked marker. That is the disclosed start-of-hold residual, and here it is benign: the
            // deferral had no queued task half (with one, the re-delivered force=true INVALIDATE would mint
            // after the wipe) and the rebuild snapshot post-dates all base data, so valid is correct.
            Assert.assertFalse(state.isPendingInvalidation());
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
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("truncate operation");
                    }
                });

                // A rows-affected base UPDATE enqueues a force=false base-cascade INVALIDATE for the view.
                execute("update base_price set amount = 7;");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired inside invalidateView", hasFired.get());

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
                // The drain processes the INVALIDATE (invalidateView defers: we hold the lock), then dequeues
                // the deferral's own re-enqueued task in the same loop -- the pending guard swallows it, which
                // the still-pending/still-valid assertions below pin.
                drainMatViewQueue(engine);

                // The real defer site must record the cause so a later finalize can mint with it.
                Assert.assertTrue("invalidation should have deferred", state.isPendingInvalidation());
                Assert.assertEquals(UpdateOperation.MAT_VIEW_INVALIDATION_REASON, state.getPendingInvalidationReason());
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
            final AtomicBoolean hasFired1h = new AtomicBoolean();
            final AtomicBoolean hasFired30m = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (state1h.isLocked() && hasFired1h.compareAndSet(false, true)) {
                        state1h.markAsPendingInvalidation("update operation");
                    } else if (state30m.isLocked() && hasFired30m.compareAndSet(false, true)) {
                        state30m.markAsPendingInvalidation("update operation");
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired for price_1h", hasFired1h.get());
            Assert.assertTrue("the seam must have fired for price_30m", hasFired30m.get());

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
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation(); // no reason -> full-refresh reschedule marker
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", hasFired.get());

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

    // Locks in the pending-invalidation marker state machine after the (pendingInvalidation,
    // pendingInvalidationReason) two-volatile composite was collapsed into a single atomic reference
    // (MatViewState#pendingInvalidationMarker). Every transition must be observable as exactly one of:
    // not-pending, pending-with-reason, or the no-reason full-refresh marker -- never a torn mix.
    @Test
    public void testPendingInvalidationMarkerStateMachineIsAtomic() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base_price (sym varchar, price double, amount int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY");
            drainWalAndMatViewQueues();

            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);

            // Fresh state: not pending, no reason.
            Assert.assertFalse(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // A reason-bearing deferral is observed atomically as pending AND carrying exactly that reason --
            // never the old torn (pending=true, reason=null).
            state.markAsPendingInvalidation("truncate operation");
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("truncate operation", state.getPendingInvalidationReason());

            // The no-arg overload is the full-refresh reschedule sentinel, and it is keep-strongest: on a
            // reason-bearing marker it is a no-op, so a losing full refresh cannot demote a deferral that a
            // lock-holder's finalize would recover into one only the queued full refresh clears.
            state.markAsPendingInvalidation();
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("the sentinel must not demote a reason-bearing deferral",
                    "truncate operation", state.getPendingInvalidationReason());

            // From an empty marker the sentinel arms: pending, but with no reason, still distinct from the
            // cleared state.
            state.clearPendingInvalidation();
            state.markAsPendingInvalidation();
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // The String overload routes a null reason into the same sentinel CAS, not a stored null reason.
            state.clearPendingInvalidation();
            state.markAsPendingInvalidation((String) null);
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // A reason-bearing mark upgrades the sentinel: a reason always wins the marker.
            state.markAsPendingInvalidation("truncate operation");
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("truncate operation", state.getPendingInvalidationReason());

            // Clearing drops the whole marker in a single write.
            state.clearPendingInvalidation();
            Assert.assertFalse(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // markAsValid also clears the marker.
            state.markAsPendingInvalidation("update operation");
            Assert.assertTrue(state.isPendingInvalidation());
            state.markAsValid();
            Assert.assertFalse(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());
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
            final AtomicBoolean hasFired = new AtomicBoolean();
            final AtomicLong baseTxnAtSeam = new AtomicLong(Long.MIN_VALUE);
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        baseTxnAtSeam.set(state.getLastRefreshBaseTxn());
                        state.markAsPendingInvalidation("truncate operation");
                    }
                });

                engine.getMatViewStateStore().enqueueRangeRefresh(viewToken, 1L, Long.MAX_VALUE - 1);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a range refresh", hasFired.get());
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
            // rangeRefresh holds the view lock and marks the view pending (the marker half of a losing
            // invalidateView's defer). The range-refresh completion must finalize the deferred invalidation.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
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

            Assert.assertTrue("the seam must have fired during a range refresh", hasFired.get());

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
    public void testReadOnlyEngineLeavesDeferredInvalidationUntouched() throws Exception {
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

            // Pins finalizeDeferredInvalidation's isReadOnlyMode early-return. Model a lock-holder completing
            // while the node is read-only with a deferral parked on the view: hold the latch as a refresh
            // would, mark the view pending (the marker a losing concurrent invalidateView left), flip the
            // engine read-only (a demote landing mid-hold), then route the unlock through finalizeAndUnlock --
            // the shared tail every holder uses. finalize must skip: leave the marker for the promote-time
            // rebuild from disk and re-enqueue NOTHING (a re-enqueue would self-feed the demote quiesce drain).
            //
            // The read-only branch is read in isolation on purpose. Draining a real refresh under read-only
            // would let invalidateView's own read-only defer re-set the marker and swallow the re-enqueued
            // task, so the frozen-pending end state is identical whether or not finalize skips -- it cannot
            // witness the branch. Reading finalizeAndUnlock directly, before any re-enqueue is processed,
            // does: with the clause present the marker stays set and nothing is queued; without it finalize
            // clears the marker and queues a force=true INVALIDATE.
            Assert.assertTrue(state.tryLock());
            state.markAsPendingInvalidation("update operation");
            readOnly.set(true);
            try {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            } finally {
                readOnly.set(false);
            }

            // finalize left the marker untouched (were the clause absent it would have cleared it here).
            Assert.assertTrue("read-only finalize must leave the deferral marker set", state.isPendingInvalidation());
            Assert.assertEquals("update operation", state.getPendingInvalidationReason());

            // Proof that finalize queued nothing: back in read-write mode, a full drain mints no invalidation
            // and the view stays valid on disk. Were the branch absent, finalize's re-enqueued force=true
            // INVALIDATE would mint here and flip the view to invalid.
            drainMatViewQueue(engine);
            drainWalQueue();
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            // Leave the view clean for teardown.
            state.markAsValid();
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
            final long refreshSeqBefore = state.getRefreshSeq();

            // A concurrent apply-time INVALIDATE deferring mid-refresh: the seam fires once, while the
            // refresh holds the view lock, and marks the view pending (the marker half of a losing
            // invalidateView's defer; the paired re-enqueued task is exercised in
            // testRefreshHoldingLockFinalizesDeferredInvalidationWithQueuedTask).
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
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

            Assert.assertTrue("the seam must have fired during a refresh", hasFired.get());
            // finalizeAndUnlock passes shouldIncrementRefreshSeq=true on data-refresh paths; MatViewTimerJob
            // reads the seq to skip refreshes made redundant by the one that just ran.
            Assert.assertTrue("a data refresh must bump the refresh seq through finalizeAndUnlock",
                    state.getRefreshSeq() > refreshSeqBefore);

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
    public void testRefreshHoldingLockFinalizesDeferredInvalidationWithQueuedTask() throws Exception {
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

            // The complete defer-site pair, as a losing concurrent invalidateView issues it: the pending
            // marker AND the re-enqueued INVALIDATE task. Pre-fix, the refresh completed without finalizing,
            // so the queued task was re-delivered against a still-pending view and the guard swallowed it for
            // good. Post-fix, the refresh's finalize clears the marker (and queues its own INVALIDATE), so the
            // re-delivered task passes the guard and mints; the second task is then swallowed by isInvalid.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                        engine.getMatViewStateStore().enqueueInvalidate(viewToken, "update operation");
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", hasFired.get());

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
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                    }
                });

                engine.getMatViewStateStore().enqueueIncrementalRefresh(viewToken);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a single-view incremental refresh", hasFired.get());

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
    public void testStatsResetFinalizesDeferredInvalidation() throws Exception {
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

            // A deferral already landed and its re-delivered task was swallowed by the pending guard: the
            // marker is armed and the queue drained away with no effect -- the frozen-pending state.
            state.markAsPendingInvalidation("update operation");
            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "update operation");
            drainMatViewQueue(engine);
            Assert.assertTrue("the re-delivered task must be swallowed while pending", state.isPendingInvalidation());
            Assert.assertFalse("the swallowed task must not mint", state.isInvalid());

            // REFRESH ... STATS takes the same per-view latch, synchronously on the SQL thread. It is a
            // lock-holder like any refresh: its unlock must finalize the deferral, or the view stays frozen.
            final long refreshSeqBefore = state.getRefreshSeq();
            execute("refresh materialized view price_1h stats");
            // Not a data refresh: the STATS holder passes shouldIncrementRefreshSeq=false, so the seq that
            // MatViewTimerJob reads for refresh dedup must not move.
            Assert.assertEquals(refreshSeqBefore, state.getRefreshSeq());
            drainMatViewQueue(engine);
            drainWalQueue();

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
            // the seam fires once (updateRefreshIntervals holds the lock) and marks the view pending (the
            // marker half of a losing invalidateView's defer). The interval-update completion must finalize
            // the deferred invalidation.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
                    }
                });

                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during an interval-update task", hasFired.get());

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
