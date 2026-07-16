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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.ForwardingMatViewStateStore;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewRefreshTask;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.MatViewStateStoreImpl;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pins the pending-invalidation handoff on a plain primary (no role switch). An apply-time
 * {@code INVALIDATE} publishes its marker before attempting the view latch. If a concurrent refresh owns
 * the latch, its post-release handoff wakes one retry while retaining the marker until a durable invalid
 * mint succeeds. The view therefore cannot remain {@code valid} on disk with stale rows.
 * <p>
 * Most tests drive the race deterministically with a {@code @TestOnly} seam that fires while a lock-holder
 * (a refresh, or {@code invalidateView} itself) holds the view lock and marks the view pending -- the marker
 * half of what a losing concurrent {@code invalidateView} issues; the holder's completion must wake it.
 * {@link #testRefreshHoldingLockFinalizesDeferredInvalidationWithQueuedTask()} exercises a marker plus an
 * already-queued duplicate delivery,
 * {@link #testLockContendedInvalidationDefersWithReason()} exercises the real defer site itself, and the
 * {@code testFullRefreshLosingLock*} pair drives the real reschedule-sentinel site (a full refresh losing
 * the latch) end-to-end.
 * <p>
 * The post-release handoff's read-only early-return is pinned here by
 * {@link #testReadOnlyEngineLeavesDeferredInvalidationUntouched()}: a mutable-flag engine (injected via
 * {@link AbstractCairoTest#engineFactory}) lets the test turn {@code isReadOnlyMode()} true under a held view
 * latch and then route the unlock through {@code finalizeAndUnlock}, standing in for a demote that turns the
 * node read-only while a lock-holder completes. The OSS base engine only ever reads the static
 * {@code isReadOnlyInstance()} flag, so the flip is synthetic -- but the branch it drives is real, and its
 * production trigger (an in-place role switch) lives in the enterprise demote suite
 * ({@code MatViewInvalidateQuiesceWedgeTest}, {@code MatViewInvalidateRepromoteLosslessTest},
 * {@code MatViewSwitchInvariantsTest}), which drives the read-only deferral end-to-end through a live demote
 * cascade.
 */
public class MatViewPendingInvalidationTrapTest extends AbstractCairoTest {

    // A test-controlled read-only flip. The OSS engine reads a static isReadOnlyInstance() flag; the
    // injected engine below ORs this in so a test can turn the node read-only mid-hold, standing in for the
    // enterprise demote that toggles isReadOnlyMode() dynamically. Reset to false before every test.
    private static final AtomicBoolean isReadOnly = new AtomicBoolean();
    // A test-controlled sticky writer refusal: while set, getWalWriter refuses this view's token with a
    // read-only authorization error even though isReadOnlyMode() stays false -- the enterprise TOCTOU
    // where a demote flips the writer chokepoint after invalidateView's top-of-method guard passed.
    // Reset to null before every test.
    private static final AtomicReference<TableToken> walRefusalToken = new AtomicReference<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Inject an engine whose isReadOnlyMode() follows the isReadOnly flag, so a lock-holder can be turned
        // read-only mid-hold without a live role switch. When isReadOnly is false (setup, and every other
        // test) this is identical to the base engine. getWalWriter additionally refuses the armed view
        // token, modelling the enterprise writer chokepoint refusing after the top-level guard passed.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public @NotNull WalWriter getWalWriter(TableToken tableToken) {
                if (tableToken.equals(walRefusalToken.get())) {
                    throw CairoException.readOnlyAccess();
                }
                return super.getWalWriter(tableToken);
            }

            @Override
            public boolean isReadOnlyMode() {
                return isReadOnly.get() || super.isReadOnlyMode();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Override
    public void setUp() {
        super.setUp();
        // Materialized views require dev mode; without it the engine installs a no-op state store.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        isReadOnly.set(false);
        walRefusalToken.set(null);
    }

    @Test
    public void testApplyWalBatchStampsInvalidationWithBatchEndTxn() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // Hold the latch so the INVALIDATE publishes its marker and loses the lock: the marker
            // stays observable instead of being consumed by the invalid mint.
            Assert.assertTrue(state.tryLock());
            try {
                // Three commits form ONE apply batch. The sticky task operation collapses them into a
                // single INVALIDATE whose txn stamp must be the batch-end frontier (the trailing
                // INSERT), not the mid-batch UPDATE that triggered the invalidation: a FULL with a
                // snapshot at the UPDATE txn must not consume a marker that also stands for the
                // trailing INSERT.
                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.320, '2024-09-10T16:00')");
                execute("update base_price set price = 1.111 where sym = 'gbpusd'");
                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.321, '2024-09-10T17:00')");
                drainWalQueue();
                drainMatViewQueue(engine);

                final TableToken baseToken = engine.getTableTokenIfExists("base_price");
                Assert.assertNotNull(baseToken);
                final long batchEndTxn = engine.getTableSequencerAPI().lastTxn(baseToken);

                Assert.assertTrue("the losing INVALIDATE must leave its marker", state.isPendingInvalidation());
                Assert.assertEquals(baseToken, state.getPendingInvalidationBaseTableTokenForTesting());
                Assert.assertEquals(
                        "the marker must carry the batch-end txn, not the invalidating txn",
                        batchEndTxn,
                        state.getPendingInvalidationBaseTxnForTesting()
                );
                state.clearPendingInvalidationForTesting();
            } finally {
                state.unlock();
            }
        });
    }

    @Test
    public void testClosedStateLeavesDeferredInvalidationUntouched() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Pins the post-release handoff's isClosed early-return. Model a lock-holder completing while
            // the owner store tears down with
            // a deferral parked on the view: hold the latch as a refresh would, mark the view pending
            // (the marker a losing concurrent invalidateView left), close() the state mid-hold (close
            // cannot take the held latch, so it only flags closed and leaves the parked factory for
            // the holder), then route the unlock through finalizeAndUnlock -- the shared tail every
            // holder uses. finalize must skip: the marker dies with the discarded state and nothing
            // may be enqueued, while the unlock tail (tryCloseIfClosed) still frees the parked factory.
            Assert.assertTrue(state.tryLock());
            try {
                state.markAsPendingInvalidation("update operation");
                state.close();
                Assert.assertTrue(state.isClosed());
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }

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

    @Test
    public void testConcurrentFullPublicationRetriesAfterReasonWinsCas() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createUnseededAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            final CountDownLatch hasReadEmptyMarker = new CountDownLatch(1);
            final CountDownLatch resumeFullPublisher = new CountDownLatch(1);
            final AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
            state.setOnPendingFullRefreshMarkerReadForTesting(() -> {
                hasReadEmptyMarker.countDown();
                try {
                    if (!resumeFullPublisher.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to resume the full-refresh publisher");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });

            final Thread fullPublisher = new Thread(() -> {
                try {
                    state.markAsPendingFullRefreshForTesting();
                } catch (Throwable th) {
                    publisherFailure.set(th);
                }
            }, "full-refresh-publisher");
            fullPublisher.start();
            try {
                Assert.assertTrue(
                        "the full-refresh publisher must pause after reading the empty marker",
                        hasReadEmptyMarker.await(30, TimeUnit.SECONDS)
                );
                state.markAsPendingInvalidation("update operation");
            } finally {
                resumeFullPublisher.countDown();
                fullPublisher.join(30_000);
            }

            Assert.assertFalse("the full-refresh publisher did not terminate", fullPublisher.isAlive());
            Assert.assertNull("the full-refresh publisher failed", publisherFailure.get());
            assertPendingReasonAndFullFacets(viewToken, state, "update operation");
        });
    }

    @Test
    public void testConcurrentReasonPublicationRetriesAfterFullWinsCas() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createUnseededAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            final CountDownLatch hasReadEmptyMarker = new CountDownLatch(1);
            final CountDownLatch resumeReasonPublisher = new CountDownLatch(1);
            final AtomicReference<Throwable> publisherFailure = new AtomicReference<>();
            state.setOnPendingInvalidationMarkerReadForTesting(() -> {
                hasReadEmptyMarker.countDown();
                try {
                    if (!resumeReasonPublisher.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to resume the invalidation publisher");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            });

            final Thread reasonPublisher = new Thread(() -> {
                try {
                    state.markAsPendingInvalidation("update operation");
                } catch (Throwable th) {
                    publisherFailure.set(th);
                }
            }, "invalidation-publisher");
            reasonPublisher.start();
            try {
                Assert.assertTrue(
                        "the invalidation publisher must pause after reading the empty marker",
                        hasReadEmptyMarker.await(30, TimeUnit.SECONDS)
                );
                state.markAsPendingFullRefreshForTesting();
            } finally {
                resumeReasonPublisher.countDown();
                reasonPublisher.join(30_000);
            }

            Assert.assertFalse("the invalidation publisher did not terminate", reasonPublisher.isAlive());
            Assert.assertNull("the invalidation publisher failed", publisherFailure.get());
            assertPendingReasonAndFullFacets(viewToken, state, "update operation");
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
    public void testDroppedViewFinalizeSkipsWakeWhenCloseRaces() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final TableToken viewToken = new TableToken("dropped_view", "dropped_view~1", null, 1, true, false, false);
            // Isolates the isDropped clause that testDroppedViewLeavesDeferredInvalidationUntouched
            // cannot: that job-driven test always has tryCloseIfDropped win the just-freed latch and flip
            // isClosed() true before the gate runs, so isDropped and isClosed agree there and removing
            // either one alone still leaves the other standing guard. Model instead the race where
            // tryCloseIfDropped loses the latch to a concurrent holder -- dropped stays true, but
            // isClosed() never flips -- by overriding isClosed() to stay false on a synthetic state. With
            // the view never invalidated either, the isDropped clause is now the ONLY thing stopping
            // finalize from waking a marker against a dropped view.
            final MatViewState viewState = new MatViewState(
                    new MatViewDefinition(),
                    (event, tableToken, baseTableTxn, errorMessage, latencyUs) -> {
                    }
            ) {
                @Override
                public boolean isClosed() {
                    return false;
                }
            };
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    invalidationWakeCount.incrementAndGet();
                }
            };
            try {
                Assert.assertTrue(viewState.tryLock());
                viewState.markAsPendingInvalidation("update operation");
                viewState.markAsDropped();
                MatViewRefreshJob.finalizeAndUnlock(engine, countingStore, viewToken, viewState, false);

                Assert.assertEquals("finalize must not wake the marker for a dropped view", 0, invalidationWakeCount.get());
                Assert.assertTrue("the marker dies with the dropped state, never consumed", viewState.isPendingInvalidation());
            } finally {
                if (viewState.isLocked()) {
                    viewState.unlock();
                }
                viewState.close();
            }
        });
    }

    @Test
    public void testDroppedViewLeavesDeferredInvalidationUntouched() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // Pins the dropped-view finalize path as a GROUP: a deferral lands mid-hold AND the view is
            // dropped during the same hold. In this job-driven scenario dropped, closed, and invalid all
            // end up true by the time finalize reads the gate -- unlockAndTryClose's tryCloseIfDropped
            // wins the just-freed latch and flips isClosed() before the gate runs, and the refresh itself
            // independently fails into invalid once its view is gone. The store's removeViewState marks
            // the state dropped but cannot free the parked factory (the refresh holds the latch), so the
            // holder's finalize must skip the dead deferral -- the counting store must observe zero
            // invalidation wakes -- and its unlock tail must free the factory via tryCloseIfDropped --
            // assertMemoryLeak fails if it leaks. Because all three gate clauses agree here, this test
            // cannot isolate the isDropped clause on its own (removing it alone still passes, masked by
            // isClosed/isInvalid); {@link #testDroppedViewFinalizeSkipsWakeWhenCloseRaces()} pins that
            // clause in isolation with a synthetic state modeling the race where tryCloseIfDropped loses
            // the latch and isClosed() stays false.
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    invalidationWakeCount.incrementAndGet();
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
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
            Assert.assertEquals(
                    "finalize must not wake the marker for a dropped view (the isDropped gate)",
                    0, invalidationWakeCount.get()
            );

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
    public void testEnqueueOomAfterUnlockIsRedrivenByNextJobTick() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            final AtomicBoolean hasFailedEnqueue = new AtomicBoolean();
            final MatViewStateStore failOnceStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    if (hasFailedEnqueue.compareAndSet(false, true)) {
                        throw new OutOfMemoryError("test queue growth failure");
                    }
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            Assert.assertTrue(state.tryLock());
            try {
                state.markAsPendingInvalidation("update operation");
                try {
                    MatViewRefreshJob.finalizeAndUnlock(engine, failOnceStore, viewToken, state, false);
                    Assert.fail("expected the fail-once queue wrapper to throw");
                } catch (OutOfMemoryError expected) {
                    Assert.assertEquals("test queue growth failure", expected.getMessage());
                }
                Assert.assertFalse("the holder must release the view latch before publication", state.isLocked());
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }

            Assert.assertTrue("the failed publication must retain the reason marker", state.isPendingInvalidation());
            Assert.assertFalse("queue failure must not mint an invalid state", state.isInvalid());

            // No task reached the queue. A normal empty refresh-job tick must discover the failed
            // publication and retry it; waiting for an unrelated holder/resume would leave this valid
            // stale view pending indefinitely.
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                drainMatViewQueue(job);
            }
            drainWalQueue();

            Assert.assertTrue("the test must exercise the injected queue failure", hasFailedEnqueue.get());
            Assert.assertFalse("the retry must consume the retained marker", state.isPendingInvalidation());
            Assert.assertTrue("the retry must durably invalidate the view", state.isInvalid());
        });
    }

    @Test
    public void testEnqueueOomWhileRedrivingRestoresSignal() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewStateStoreImpl stateStore = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            final MatViewState state = fixture.state();

            final AtomicBoolean hasFailedEnqueue = new AtomicBoolean();
            final MatViewStateStore failOnceStore = new ForwardingMatViewStateStore(stateStore) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    if (hasFailedEnqueue.compareAndSet(false, true)) {
                        throw new OutOfMemoryError("test initial queue growth failure");
                    }
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            Assert.assertTrue(state.tryLock());
            try {
                state.markAsPendingInvalidation("update operation");
                try {
                    MatViewRefreshJob.finalizeAndUnlock(engine, failOnceStore, viewToken, state, false);
                    Assert.fail("expected the fail-once queue wrapper to throw");
                } catch (OutOfMemoryError expected) {
                    Assert.assertEquals("test initial queue growth failure", expected.getMessage());
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }

            final AtomicBoolean hasFailedScan = new AtomicBoolean();
            stateStore.setOnPendingTaskReenqueueScanForTesting(() -> {
                if (hasFailedScan.compareAndSet(false, true)) {
                    throw new OutOfMemoryError("test retry scan allocation failure");
                }
            });
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                try {
                    drainMatViewQueue(job);
                    Assert.fail("expected the fail-once retry scan seam to throw");
                } catch (OutOfMemoryError expected) {
                    Assert.assertEquals("test retry scan allocation failure", expected.getMessage());
                }
                Assert.assertTrue("the retry scan must fail before claiming the pending state", state.isPendingInvalidation());

                // The next ordinary tick must retry the scan. If the first scan cleared the sole
                // store-wide signal, this tick sees no queue work and leaves the marker stranded.
                drainMatViewQueue(job);
            }
            drainWalQueue();

            Assert.assertTrue("the test must exercise the initial queue failure", hasFailedEnqueue.get());
            Assert.assertTrue("the test must exercise the retry scan failure", hasFailedScan.get());
            Assert.assertFalse("the restored signal must redrive and consume the marker", state.isPendingInvalidation());
            Assert.assertTrue("the redriven task must durably invalidate the view", state.isInvalid());
        });
    }

    @Test
    public void testFailedRefreshLeavesDeferredInvalidationUntouched() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // Pins the post-release handoff's isInvalid early-return: a deferral lands mid-hold AND the
            // holding refresh itself fails. The seam marks the view pending, then drops a base column the view
            // SQL needs, so insertAsSelect's recompile fails and refreshFailState marks the view invalid with
            // the compile error. finalize then sees the view already invalid and must return early: the
            // counting store must observe zero invalidation wakes, proving no re-enqueued INVALIDATE
            // overwrote the fail reason, and the marker must survive untouched.
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    invalidationWakeCount.incrementAndGet();
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
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
            Assert.assertEquals(
                    "finalize must not wake the marker while the view is invalid (the isInvalid gate)",
                    0, invalidationWakeCount.get()
            );

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
    public void testFinalizePartialFullWakeFailureRedrives() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore splittingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    throw new OutOfMemoryError("test full wake enqueue failure");
                }

                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    invalidationWakeCount.incrementAndGet();
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            // A synthetic holder defers both facets, then finalizes through the splitting store:
            // the invalidation wake is delivered, the FULL wake throws. The finalize must have
            // released the latch before either wake, and the FULL failure must arm the canonical
            // store's allocation-free retry bit rather than losing the owner.
            Assert.assertTrue(state.tryLock());
            try {
                state.markAsPendingInvalidation("split finalize witness");
                state.markAsPendingFullRefreshForTesting();
                try {
                    MatViewRefreshJob.finalizeAndUnlock(engine, splittingStore, viewToken, state, false);
                    Assert.fail("the FULL wake enqueue failure must propagate");
                } catch (OutOfMemoryError expected) {
                }
                Assert.assertFalse("finalize must unlock before attempting the wakes", state.isLocked());
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }
            Assert.assertEquals("the invalidation wake must be delivered despite the FULL failure",
                    1, invalidationWakeCount.get());
            Assert.assertTrue(state.isPendingInvalidation());

            // Next ticks: the retry scan redelivers the FULL owner from the marker, the queued
            // invalidation mints invalid state, and the redelivered FULL performs invalid-view
            // recovery back to valid.
            drainMatViewQueue(engine);
            drainWalQueue();
            drainMatViewQueue(engine);
            drainWalQueue();

            Assert.assertFalse("recovery must consume both facets", state.isPendingInvalidation());
            Assert.assertFalse("recovery must end valid", state.isInvalid());
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
    public void testFullRefreshConsumesInvalidationCoveredByFixedSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createSumAmountViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            Assert.assertTrue(state.tryLock());
            try {
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    // Apply UPDATE T and consume its base-cascade task while this synthetic holder owns the
                    // view latch. invalidateView publishes the reason marker, loses the latch, and consumes
                    // the original task without enqueueing a duplicate.
                    execute("UPDATE base_price SET amount = 7 WHERE sym = 'gbpusd'");
                    drainWalQueue();
                    drainMatViewQueue(job);
                    Assert.assertTrue("the UPDATE invalidation must defer behind the holder", state.isPendingInvalidation());

                    // Put FULL ahead of the holder's handoff. Its fixed reader includes UPDATE T, so the
                    // successful rebuild covers the deferred invalidation. The already-queued handoff task
                    // that follows FULL must also recognize that coverage instead of invalidating the view.
                    engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                    MatViewRefreshJob.finalizeAndUnlock(
                            engine,
                            engine.getMatViewStateStore(),
                            viewToken,
                            state,
                            false
                    );
                    drainMatViewQueue(job);
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }
            drainWalQueue();

            Assert.assertFalse("FULL must consume the invalidation covered by its fixed snapshot", state.isPendingInvalidation());
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tvalid\t
                            """);
            assertQuery("SELECT sym, amount FROM price_1h ORDER BY sym")
                    .expectSize()
                    .returns("""
                            sym\tamount
                            gbpusd\t14
                            jpyusd\t3
                            """);
        });
    }

    @Test
    public void testFullRefreshConsumesInvalidationCoveredByFixedSnapshotBeforePublisherLocks() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createSumAmountViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Hold UPDATE T's notification outside the queue. FULL will fix a snapshot that includes T,
            // then its holding-latch seam delivers the notification through a second refresh job.
            execute("UPDATE base_price SET amount = 7 WHERE sym = 'gbpusd'");
            drainWalQueue();
            final MatViewRefreshTask delayedInvalidation = new MatViewRefreshTask();
            Assert.assertTrue(
                    "the UPDATE must enqueue an invalidation task",
                    engine.getMatViewStateStore().tryDequeueRefreshTask(delayedInvalidation)
            );
            Assert.assertEquals(MatViewRefreshTask.INVALIDATE, delayedInvalidation.operation);

            final CountDownLatch hasPublishedInvalidation = new CountDownLatch(1);
            final CountDownLatch resumeInvalidationPublisher = new CountDownLatch(1);
            final AtomicBoolean hasStartedInvalidationPublisher = new AtomicBoolean();
            final AtomicReference<Throwable> invalidationFailure = new AtomicReference<>();
            try (
                    MatViewRefreshJob fullJob = createMatViewRefreshJob(engine);
                    MatViewRefreshJob invalidationJob = createMatViewRefreshJob(engine)
            ) {
                invalidationJob.setOnInvalidationPublishedForTesting(() -> {
                    hasPublishedInvalidation.countDown();
                    try {
                        if (!resumeInvalidationPublisher.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to resume the invalidation publisher");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
                final Thread invalidationPublisher = new Thread(() -> {
                    try {
                        drainMatViewQueue(invalidationJob);
                    } catch (Throwable th) {
                        invalidationFailure.set(th);
                    }
                }, "covered-invalidation-publisher");
                final AtomicReference<Throwable> seamFailure = new AtomicReference<>();
                fullJob.setOnHoldingLockForTesting(() -> {
                    if (hasStartedInvalidationPublisher.compareAndSet(false, true)) {
                        engine.getMatViewStateStore().reenqueueRefreshTask(delayedInvalidation);
                        invalidationPublisher.start();
                        try {
                            if (!hasPublishedInvalidation.await(30, TimeUnit.SECONDS)) {
                                seamFailure.set(new AssertionError("the invalidation must publish before FULL completes"));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            seamFailure.set(e);
                        }
                    }
                });

                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                try {
                    drainMatViewQueue(fullJob);
                } finally {
                    resumeInvalidationPublisher.countDown();
                    if (hasStartedInvalidationPublisher.get()) {
                        invalidationPublisher.join(30_000);
                    }
                }

                if (seamFailure.get() != null) {
                    throw new AssertionError("in-seam failure", seamFailure.get());
                }

                Assert.assertTrue("the invalidation publisher must run during FULL", hasStartedInvalidationPublisher.get());
                Assert.assertFalse("the invalidation publisher did not terminate", invalidationPublisher.isAlive());
                try {
                    Assert.assertNull("the covered invalidation publisher failed", invalidationFailure.get());
                    Assert.assertFalse("the covered invalidation publisher leaked the view latch", state.isLocked());
                } finally {
                    if (state.isLocked()) {
                        state.unlock();
                    }
                }
            }
            drainWalQueue();

            Assert.assertFalse("FULL must consume the covered invalidation", state.isPendingInvalidation());
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tvalid\t
                            """);
            assertQuery("SELECT sym, amount FROM price_1h ORDER BY sym")
                    .expectSize()
                    .returns("""
                            sym\tamount
                            gbpusd\t14
                            jpyusd\t3
                            """);
        });
    }

    @Test
    public void testFullRefreshConsumesInvalidationCoveredByFixedSnapshotWhenRecoveringInvalidView() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createSumAmountViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertTrue("precondition: FULL must start from an invalid view", state.isInvalid());

            // Hold the real UPDATE notification outside the queue, then place it behind FULL. This pins
            // the delayed-delivery case: no reason marker exists during the pump, so invalidateView must
            // consult the last successful FULL coverage after recovery has already completed.
            execute("UPDATE base_price SET amount = 7 WHERE sym = 'gbpusd'");
            drainWalQueue();
            final MatViewRefreshTask delayedInvalidation = new MatViewRefreshTask();
            Assert.assertTrue(
                    "the UPDATE must enqueue an invalidation task",
                    engine.getMatViewStateStore().tryDequeueRefreshTask(delayedInvalidation)
            );
            Assert.assertEquals(MatViewRefreshTask.INVALIDATE, delayedInvalidation.operation);

            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                engine.getMatViewStateStore().reenqueueRefreshTask(delayedInvalidation);
                drainMatViewQueue(job);
            }
            drainWalQueue();

            Assert.assertFalse("covered delayed invalidation must not remain pending", state.isPendingInvalidation());
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tvalid\t
                            """);
            assertQuery("SELECT sym, amount FROM price_1h ORDER BY sym")
                    .expectSize()
                    .returns("""
                            sym\tamount
                            gbpusd\t14
                            jpyusd\t3
                            """);
        });
    }

    @Test
    public void testFullRefreshConsumesProvenanceFreeMarkerCoveredByItsSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // Build the zombie: a provenance-free reason marker retained on an invalid view. The seam
            // publishes the marker mid-hold (as a losing apply-time INVALIDATE would), then breaks the
            // view SQL so the holding refresh fails into invalid state. finalize's isInvalid gate skips
            // the wake without clearing the facet, and invalidateView's entry gate never consumes a
            // marker on an invalid view -- only a successful REFRESH FULL can now consume it.
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
            Assert.assertTrue("the zombie setup needs an invalid view", state.isInvalid());
            Assert.assertTrue("the zombie setup needs a retained reason marker", state.isPendingInvalidation());

            // Repair the base so a FULL rebuild can succeed, then run the documented recovery.
            execute("alter table base_price add column price double");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h FULL;");
            drainMatViewQueue(engine);
            drainWalQueue();
            // Without the fix, the FULL's finalize enqueued a stale INVALIDATE (the coverage check
            // bails on the marker's missing provenance); this drain would flip the freshly repaired
            // view back to invalid.
            drainMatViewQueue(engine);
            drainWalQueue();

            Assert.assertFalse(
                    "a successful FULL rebuild must consume the provenance-free marker its snapshot covers",
                    state.isPendingInvalidation()
            );
            Assert.assertFalse("the repaired view must stay valid", state.isInvalid());
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
    public void testFullRefreshHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // A base-cascade INVALIDATE deferring during the full-refresh pump: the seam fires once after
            // resetInvalidState while fullRefresh holds the view lock. The marker must survive the pump and
            // the post-release handoff must wake it.
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
    public void testFullRefreshKeepsInvalidationNewerThanFixedSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // FULL has already fixed reader snapshot S when this seam applies UPDATE S+1. A second worker
            // delivers that real apply-time INVALIDATE against FULL's held latch. resetInvalidState must not
            // consume the newer owner: after FULL commits only S, the handoff must mint the view invalid.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (
                    MatViewRefreshJob fullJob = createMatViewRefreshJob(engine);
                    MatViewRefreshJob invalidationJob = createMatViewRefreshJob(engine)
            ) {
                fullJob.setOnBaseReaderSnapshotForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        try {
                            execute("UPDATE base_price SET price = 9.9 WHERE sym = 'gbpusd'");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();
                        drainMatViewQueue(invalidationJob);
                    }
                });

                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(fullJob);
                drainWalQueue();
            }

            Assert.assertTrue("the base update must land after FULL fixes its reader", hasFired.get());
            Assert.assertFalse("the newer invalidation must reach a durable terminal state", state.isPendingInvalidation());
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
    public void testFullRefreshKeepsInvalidationNewerThanFixedSnapshotWhenRecoveringInvalidView() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // FULL starts as recovery from a durable invalid state. Once it fixes reader snapshot S, UPDATE
            // S+1 must still publish ownership even though the old invalid flag remains set until FULL's
            // reset. Otherwise FULL can turn the view valid at S while silently dropping S+1.
            engine.getMatViewStateStore().enqueueInvalidate(viewToken, "test invalidation");
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertTrue("precondition: FULL must start from an invalid view", state.isInvalid());

            final AtomicBoolean hasFired = new AtomicBoolean();
            try (
                    MatViewRefreshJob fullJob = createMatViewRefreshJob(engine);
                    MatViewRefreshJob invalidationJob = createMatViewRefreshJob(engine)
            ) {
                fullJob.setOnBaseReaderSnapshotForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        try {
                            execute("UPDATE base_price SET price = 9.9 WHERE sym = 'gbpusd'");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();
                        drainMatViewQueue(invalidationJob);
                    }
                });

                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(fullJob);
                drainWalQueue();
            }

            Assert.assertTrue("the base update must land during invalid-view recovery", hasFired.get());
            Assert.assertFalse("the newer invalidation must reach a durable terminal state", state.isPendingInvalidation());
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
    public void testFullRefreshLosingLockArmsSentinelAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();
            final AtomicBoolean latchRescued = new AtomicBoolean();

            // Drive fullRefresh's losing branch deterministically. The task publishes the sentinel before
            // tryLock, loses to this holder, and returns without publishing N retries.
            Assert.assertTrue(state.tryLock());
            try {
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    final AtomicInteger dequeueCount = new AtomicInteger();
                    job.setOnRefreshTaskDequeuedForTesting(() -> Assert.assertEquals(
                            "the losing full refresh must not self-republish while the latch remains held",
                            1,
                            dequeueCount.incrementAndGet()
                    ));
                    try {
                        engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                        drainMatViewQueue(job);

                        Assert.assertTrue("the losing full refresh must retain its reschedule sentinel", state.isPendingInvalidation());
                        Assert.assertNull(state.getPendingInvalidationReason());
                        Assert.assertFalse(state.isInvalid());

                        job.setOnRefreshTaskDequeuedForTesting(null);
                        MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                        drainMatViewQueue(job);
                    } finally {
                        if (state.isLocked()) {
                            latchRescued.set(true);
                            state.clearPendingInvalidationForTesting();
                            state.unlock();
                        }
                    }
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }
            drainWalAndMatViewQueues();

            // The republished full refresh won the freed latch, cleared the sentinel and rebuilt the view.
            Assert.assertFalse("the latch must not have needed a test-side rescue", latchRescued.get());
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
    public void testFullRefreshLosingLockBehindInvalidationHolderRecovers() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The outer INVALIDATE owns the latch. Its seam delivers a FULL task through a second worker;
            // the FULL publishes its independent intent, loses the latch and returns. The INVALIDATE then
            // mints a durable invalid state while preserving that FULL intent. Its post-release handoff must
            // still wake FULL even though the view is now invalid.
            final AtomicBoolean hasEnteredInvalidationHolder = new AtomicBoolean();
            final AtomicBoolean hasLostFullRefresh = new AtomicBoolean();
            try (
                    MatViewRefreshJob fullJob = createMatViewRefreshJob(engine);
                    MatViewRefreshJob invalidationJob = createMatViewRefreshJob(engine)
            ) {
                final AtomicInteger fullDequeueCount = new AtomicInteger();
                fullJob.setOnRefreshTaskDequeuedForTesting(() -> Assert.assertEquals(
                        "the losing full refresh must not self-republish while invalidateView holds the latch",
                        1,
                        fullDequeueCount.incrementAndGet()
                ));
                invalidationJob.setOnHoldingLockForTesting(() -> {
                    if (hasEnteredInvalidationHolder.compareAndSet(false, true)) {
                        engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                        drainMatViewQueue(fullJob);
                        hasLostFullRefresh.set(true);
                    }
                });

                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "update operation");
                drainMatViewQueue(invalidationJob);
                drainWalQueue();
            }

            Assert.assertTrue("the full refresh must lose the latch inside invalidateView", hasLostFullRefresh.get());
            Assert.assertFalse("the re-delivered full refresh must consume its marker", state.isPendingInvalidation());
            assertQuery("select view_name, base_table_name, view_status, invalidation_reason from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status\tinvalidation_reason
                            price_1h\tbase_price\tvalid\t
                            """);
        });
    }

    @Test
    public void testFullRefreshLosingLockCannotDemoteReasonDeferral() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();
            final AtomicBoolean latchRescued = new AtomicBoolean();

            // A losing full refresh and a reason-bearing invalidation must retain independent ownership in
            // one atomic marker. Neither publication may demote or erase the other.
            Assert.assertTrue(state.tryLock());
            try {
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    final AtomicInteger dequeueCount = new AtomicInteger();
                    job.setOnRefreshTaskDequeuedForTesting(() -> Assert.assertTrue(
                            "a losing full refresh must not self-republish while the latch remains held",
                            dequeueCount.incrementAndGet() <= 2
                    ));
                    try {
                        engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                        drainMatViewQueue(job);
                        Assert.assertTrue(state.isPendingInvalidation());
                        Assert.assertNull(state.getPendingInvalidationReason());

                        state.markAsPendingInvalidation("update operation");
                        engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                        drainMatViewQueue(job);
                        Assert.assertEquals("the losing full refresh demoted a reason-bearing deferral",
                                "update operation", state.getPendingInvalidationReason());

                        job.setOnRefreshTaskDequeuedForTesting(null);
                        MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                        drainMatViewQueue(job);
                    } finally {
                        if (state.isLocked()) {
                            latchRescued.set(true);
                            state.clearPendingInvalidationForTesting();
                            state.unlock();
                        }
                    }
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }
            drainWalQueue();

            // The handoff delivers both operations. INVALIDATE mints first, then the retained FULL refresh
            // rebuilds the view and consumes only its own ownership flag.
            Assert.assertFalse("the latch must not have needed a test-side rescue", latchRescued.get());
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
    public void testFullRefreshLosingLockClearsPendingFullWhenBlocked() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            Assert.assertTrue(state.tryLock());
            try {
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                    drainMatViewQueue(job);
                    Assert.assertTrue("the losing full refresh must publish ownership", state.isPendingInvalidation());

                    setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "price_1h");
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                    drainMatViewQueue(job);
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }

            Assert.assertFalse("the blocked retry must consume its full-refresh ownership", state.isPendingInvalidation());
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_BLOCK_LIST, "");
            execute("INSERT INTO base_price (sym, price, ts) VALUES ('eurusd', 1.1, '2024-09-10T13:01')");
            drainWalAndMatViewQueues();
            Assert.assertFalse("ordinary refresh must resume after unblocking", state.isPendingInvalidation());
        });
    }

    @Test
    public void testFullRefreshLosingLockRedeliversAfterResume() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            Assert.assertTrue(state.tryLock());
            try {
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                    drainMatViewQueue(job);
                    execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                    drainMatViewQueue(job);
                    Assert.assertTrue("suspension must retain the losing full-refresh owner", state.isPendingInvalidation());

                    execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
                    drainMatViewQueue(job);
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }
            drainWalQueue();

            Assert.assertFalse("resume must re-drive and consume the full-refresh owner", state.isPendingInvalidation());
            Assert.assertFalse("the resumed full refresh must leave the view valid", state.isInvalid());
        });
    }

    @Test
    public void testFullRefreshOwnerWakeEnqueueOomRetainsOwner() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();
            final AtomicBoolean latchRescued = new AtomicBoolean();

            // The finalize owner wake is the sole handoff that redelivers a FULL request that lost the
            // latch (the auth-refusal deferral no longer re-enqueues). Queue growth inside that wake can
            // throw; finalizeAndUnlock0's catch must retain the owner and arm the allocation-free retry
            // signal so the next ordinary job tick redelivers it.
            final AtomicBoolean hasFailedEnqueue = new AtomicBoolean();
            final MatViewStateStore failOnceStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    if (hasFailedEnqueue.compareAndSet(false, true)) {
                        throw new OutOfMemoryError("test full retry queue growth failure");
                    }
                    super.enqueueFullRefresh(matViewToken, fullRefreshOwner);
                }
            };

            Assert.assertTrue(state.tryLock());
            try {
                try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, failOnceStore)) {
                    try {
                        engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                        drainMatViewQueue(job);
                        Assert.assertTrue("the losing full refresh must retain its owner", state.isPendingInvalidation());

                        try {
                            MatViewRefreshJob.finalizeAndUnlock(engine, failOnceStore, viewToken, state, false);
                            Assert.fail("expected the fail-once owner-wake queue wrapper to throw");
                        } catch (OutOfMemoryError expected) {
                            Assert.assertEquals("test full retry queue growth failure", expected.getMessage());
                        }

                        Assert.assertTrue("the test must exercise the injected queue failure", hasFailedEnqueue.get());
                        Assert.assertTrue("the failed owner wake must retain the full-refresh owner", state.isPendingInvalidation());
                        Assert.assertFalse("the holder must release the view latch after queue failure", state.isLocked());

                        // The wake's catch armed requestPendingFullRefreshReenqueue on the canonical store.
                        // The next ordinary job tick must rediscover the owner and complete the full refresh.
                        drainMatViewQueue(job);
                    } finally {
                        if (state.isLocked()) {
                            latchRescued.set(true);
                            state.clearPendingInvalidationForTesting();
                            state.unlock();
                        }
                    }
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
            }
            drainWalQueue();

            Assert.assertFalse("the latch must not have needed a test-side rescue", latchRescued.get());
            Assert.assertFalse("the successful retry must consume the retained full-refresh owner", state.isPendingInvalidation());
            Assert.assertFalse("the recovered full refresh must leave the view valid", state.isInvalid());
        });
    }

    @Test
    public void testFullRefreshOwnerClearFailureStillUnlocks() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // A successful full refresh clears its owner facet in the finally. When a reason shares
            // the marker (published mid-hold, unknown provenance -- survives the coverage check), the
            // clear allocates a replacement marker and can throw under memory pressure. The unlock
            // must still run: a skipped finalize leaves the view latch held forever, wedging every
            // later refresh, close, and drop of this view.
            walRefusalToken.set(null);
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                final AtomicBoolean hasPublished = new AtomicBoolean();
                job.setOnHoldingLockForTesting(() -> {
                    if (hasPublished.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("mid-hold publication");
                    }
                });
                state.setOnClearPendingFullRefreshForTesting(() -> {
                    throw new OutOfMemoryError("test marker replacement allocation failure");
                });
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                try {
                    drainMatViewQueue(job);
                    Assert.fail("the owner clear failure must propagate");
                } catch (OutOfMemoryError expected) {
                }

                Assert.assertFalse("the finally must release the latch even when the owner clear throws",
                        state.isLocked());
                Assert.assertTrue("the marker must survive the failed clear", state.isPendingInvalidation());
                Assert.assertEquals("mid-hold publication", state.getPendingInvalidationReason());

                // The nested finally's handoff already woke the surviving facets; the next drain
                // mints the invalidation and the redelivered FULL performs invalid-view recovery.
                drainMatViewQueue(job);
                drainWalQueue();
                drainMatViewQueue(job);
            }
            drainWalQueue();

            Assert.assertFalse("recovery must consume the marker", state.isPendingInvalidation());
            Assert.assertFalse("recovery must end valid", state.isInvalid());
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
    public void testFullRefreshTerminalFailureClearsPendingFull() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            execute("DROP TABLE base_price");
            drainWalAndMatViewQueues();

            try {
                try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                    // Stop the post-unlock handoff without consuming the marker. The read-only gate makes
                    // finalize return, so only terminal cleanup itself can satisfy the assertion below.
                    job.setOnFullRefreshTerminalFailureForTesting(() -> isReadOnly.set(true));
                    engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                    drainMatViewQueue(job);
                }
            } finally {
                isReadOnly.set(false);
            }

            Assert.assertTrue("missing base must leave the view invalid", state.isInvalid());
            Assert.assertFalse("terminal failure must consume its full-refresh ownership", state.isPendingInvalidation());
        });
    }

    @Test
    public void testFullRefreshTerminalFailurePreservesNewerFullRequest() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            execute("DROP TABLE base_price");
            drainWalAndMatViewQueues();

            final AtomicBoolean hasFired = new AtomicBoolean();
            try (
                    MatViewRefreshJob losingJob = createMatViewRefreshJob(engine);
                    MatViewRefreshJob terminalJob = createMatViewRefreshJob(engine)
            ) {
                terminalJob.setOnFullRefreshTerminalFailureForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        try {
                            execute("create table base_price (" +
                                    "sym varchar, price double, amount int, ts timestamp" +
                                    ") timestamp(ts) partition by DAY WAL");
                            execute("INSERT INTO base_price (sym, price, ts) VALUES " +
                                    "('eurusd', 1.1, '2024-09-10T13:01')");
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        drainWalQueue();

                        // This newer request publishes a distinct owner and loses the latch to the
                        // terminal missing-base attempt. Cleanup of the older owner must not erase it.
                        engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                        drainMatViewQueue(losingJob);
                    }
                });
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(terminalJob);
            }
            drainWalQueue();

            Assert.assertTrue("the terminal-failure seam must publish a newer full request", hasFired.get());
            Assert.assertFalse("the newer full request must consume its own ownership", state.isPendingInvalidation());
            Assert.assertFalse("the newer full request must recover the view", state.isInvalid());
        });
    }

    @Test
    public void testInvalidateViewHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            // MANUAL DEFERRED never refreshes incrementally, so a force=false base-cascade INVALIDATE on it
            // hits invalidateView's gate-false decline (lastRefreshBaseTxn == -1): invalidateView holds the
            // lock without minting -- the sixth lock-holder that, pre-fix, never finalized a deferral landing
            // in that window.
            final MatViewFixture fixture = createManualPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Populate via RANGE (no seam armed) so the view is valid with rows, lastRefreshBaseTxn still -1.
            engine.getMatViewStateStore().enqueueRangeRefresh(viewToken, 1L, Long.MAX_VALUE - 1);
            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertEquals("precondition: the view has never been incrementally refreshed", -1, state.getLastRefreshBaseTxn());
            Assert.assertFalse("precondition: the view is valid before the cascade", state.isInvalid());

            // The seam replaces the outer force=false task's marker with a distinct marker carrying the same
            // String reason. The decline must CAS-clear only its own marker; clearing by reason equality would
            // erase the forceful owner and leave the view stale.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("update operation");
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
                            price_1h\tbase_price\tinvalid\tupdate operation
                            """);
        });
    }

    @Test
    public void testLateInvalidationBetweenFinalizeAndUnlockIsReenqueued() throws Exception {
        assertMemoryLeak(() -> {
            final String invalidationReason = "update operation";
            final AtomicReference<String> enqueuedReason = new AtomicReference<>();
            final TableToken viewToken = new TableToken("late_view", "late_view~1", null, 1, true, false, false);
            final MatViewState viewState = new MatViewState(new MatViewDefinition(), null) {
                private boolean hasInjectedInvalidation;

                @Override
                public void unlock() {
                    if (!hasInjectedInvalidation) {
                        hasInjectedInvalidation = true;
                        markAsPendingInvalidation(invalidationReason);
                    }
                    super.unlock();
                }
            };

            try (MatViewStateStoreImpl stateStore = new MatViewStateStoreImpl(engine) {
                @Override
                public void enqueueInvalidate(TableToken matViewToken, String reason) {
                    Assert.assertEquals(viewToken, matViewToken);
                    Assert.assertNull("finalize must enqueue the late invalidation exactly once", enqueuedReason.get());
                    enqueuedReason.set(reason);
                }
            }) {
                Assert.assertTrue(viewState.tryLock());

                // unlock() injects the marker after the holder's work but immediately before releasing the
                // latch. This is the old inspection-to-release window: the losing invalidator has already
                // failed tryLock and published its intent, so the post-release handoff must observe it and
                // publish an authoritative retry while retaining ownership in the state.
                MatViewRefreshJob.finalizeAndUnlock(engine, stateStore, viewToken, viewState, false);

                Assert.assertEquals("finalize must re-enqueue the late invalidation", invalidationReason, enqueuedReason.get());
                Assert.assertTrue("finalize must retain invalidation intent until delivery succeeds", viewState.isPendingInvalidation());
                Assert.assertEquals(invalidationReason, viewState.getPendingInvalidationReason());
                Assert.assertFalse(viewState.isLocked());
            } finally {
                viewState.close();
            }
        });
    }

    @Test
    public void testLockContendedInvalidationDefersWithReason() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Hold the view lock from the test thread to simulate a concurrent refresh worker. The latch
            // is a non-reentrant AtomicBoolean, so the refresh job's invalidateView tryLock() fails exactly
            // as it would against a real second worker.
            Assert.assertTrue(state.tryLock());
            try {
                execute("update base_price set amount = 42;"); // rows-affected UPDATE -> apply-time INVALIDATE
                drainWalQueue();           // apply the UPDATE -> enqueue the INVALIDATE
                // The drain processes the INVALIDATE: it publishes intent, loses this latch, and returns
                // without adding a contender retry to the shared queue.
                drainMatViewQueue(engine);

                // The real defer site must record the cause so a later finalize can mint with it.
                Assert.assertTrue("invalidation should have deferred", state.isPendingInvalidation());
                Assert.assertEquals(UpdateOperation.MAT_VIEW_INVALIDATION_REASON, state.getPendingInvalidationReason());
                // The deferral alone must not mint: the view is still valid on disk while pending in memory.
                Assert.assertFalse("deferral alone must not mark the view invalid", state.isInvalid());
            } finally {
                MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
            }

            drainMatViewQueue(engine);
            drainWalQueue();
            Assert.assertFalse(state.isPendingInvalidation());
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
    public void testManyLockContendersPublishOneWakePerFacet() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createManualPriceViewAndDrainFixture();

            final int contenderCount = 32;
            final TableToken viewToken = fixture.viewToken();
            final MatViewStateStore stateStore = engine.getMatViewStateStore();
            final MatViewState state = fixture.state();

            final AtomicInteger dequeueCount = new AtomicInteger();
            final AtomicInteger dequeueLimit = new AtomicInteger(contenderCount);
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnRefreshTaskDequeuedForTesting(() -> {
                    final int count = dequeueCount.incrementAndGet();
                    if (count > dequeueLimit.get()) {
                        throw new AssertionError("lock losers amplified the refresh queue [count=" + count + ']');
                    }
                });

                // FULL phase: all N initial tasks lose one held latch. A loser may replace the shared
                // owner marker, but must not publish a replacement task. The holder publishes one wake.
                Assert.assertTrue(state.tryLock());
                try {
                    for (int i = 0; i < contenderCount; i++) {
                        stateStore.enqueueFullRefresh(viewToken);
                    }
                    drainMatViewQueue(job);
                    Assert.assertEquals("FULL losers must consume exactly the original tasks", contenderCount, dequeueCount.get());
                    Assert.assertTrue("FULL losers must retain one shared owner", state.isPendingInvalidation());

                    dequeueLimit.incrementAndGet();
                    MatViewRefreshJob.finalizeAndUnlock(engine, stateStore, viewToken, state, false);
                    drainMatViewQueue(job);
                    Assert.assertEquals("the holder must publish one FULL wake", contenderCount + 1, dequeueCount.get());
                    Assert.assertFalse("the FULL wake must consume the shared owner", state.isPendingInvalidation());
                } finally {
                    if (state.isLocked()) {
                        state.clearPendingInvalidationForTesting();
                        state.unlock();
                    }
                }

                // INVALIDATE phase: repeat the same bound for reason-bearing contenders. They coalesce
                // into one marker, and the holder publishes one authoritative invalidation task.
                dequeueCount.set(0);
                dequeueLimit.set(contenderCount);
                Assert.assertTrue(state.tryLock());
                try {
                    for (int i = 0; i < contenderCount; i++) {
                        stateStore.enqueueInvalidate(viewToken, "update operation");
                    }
                    drainMatViewQueue(job);
                    Assert.assertEquals("INVALIDATE losers must consume exactly the original tasks", contenderCount, dequeueCount.get());
                    Assert.assertEquals("update operation", state.getPendingInvalidationReason());

                    dequeueLimit.incrementAndGet();
                    MatViewRefreshJob.finalizeAndUnlock(engine, stateStore, viewToken, state, false);
                    drainMatViewQueue(job);
                    Assert.assertEquals("the holder must publish one INVALIDATE wake", contenderCount + 1, dequeueCount.get());
                    Assert.assertFalse("the INVALIDATE wake must consume the shared marker", state.isPendingInvalidation());
                    Assert.assertTrue("the authoritative wake must durably invalidate the view", state.isInvalid());
                } finally {
                    if (state.isLocked()) {
                        state.clearPendingInvalidationForTesting();
                        state.unlock();
                    }
                }
            }
            drainWalQueue();
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
    public void testNullReasonMarkerIsRedeliveredAsFullRefresh() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // A null-reason marker is the full-refresh reschedule (markAsPendingFullRefreshForTesting() with no reason,
            // see fullRefresh), not a deferred invalidation. The post-release handoff must route it back to
            // FULL_REFRESH rather than INVALIDATE.
            final AtomicBoolean hasFired = new AtomicBoolean();
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                job.setOnHoldingLockForTesting(() -> {
                    if (hasFired.compareAndSet(false, true)) {
                        state.markAsPendingFullRefreshForTesting(); // no reason -> full-refresh reschedule marker
                    }
                });

                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.500, '2024-09-10T14:00')");
                drainWalQueue();
                drainMatViewQueue(job);
                drainWalQueue();
            }

            Assert.assertTrue("the seam must have fired during a refresh", hasFired.get());

            Assert.assertFalse("the redelivered full refresh must consume its marker", state.isPendingInvalidation());
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
    public void testPendingInvalidationMarkerMergeSemantics() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createUnseededAutoPriceViewFixture();
            final MatViewState state = fixture.state();
            final TableToken baseToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseToken);
            final TableToken foreignBaseToken = new TableToken("other_base", "other_base~1", null, 7, true, false, false);

            // Same-token merge: the newer reason wins, the txn frontier keeps the maximum, and the
            // force flag ORs -- a forced publication can never be demoted by a later unforced one.
            state.markAsPendingInvalidationForTesting("first reason", baseToken, 5, false);
            Assert.assertFalse(state.isPendingInvalidationForcedForTesting());
            state.markAsPendingInvalidationForTesting("second reason", baseToken, 3, true);
            Assert.assertEquals("second reason", state.getPendingInvalidationReason());
            Assert.assertEquals("same-token merge must keep the maximum txn frontier",
                    5, state.getPendingInvalidationBaseTxnForTesting());
            Assert.assertTrue("force must merge with OR semantics",
                    state.isPendingInvalidationForcedForTesting());
            Assert.assertEquals(baseToken, state.getPendingInvalidationBaseTableTokenForTesting());
            state.markAsPendingInvalidationForTesting("third reason", baseToken, 9, false);
            Assert.assertEquals(9, state.getPendingInvalidationBaseTxnForTesting());
            Assert.assertTrue("force must stay sticky through later unforced merges",
                    state.isPendingInvalidationForcedForTesting());

            // Cross-token merge collapses provenance conservatively: without a comparable frontier
            // the marker must survive any full-refresh coverage check.
            state.markAsPendingInvalidationForTesting("fourth reason", foreignBaseToken, 11, false);
            Assert.assertNull("differing tokens must collapse provenance",
                    state.getPendingInvalidationBaseTableTokenForTesting());
            Assert.assertEquals(Numbers.LONG_NULL, state.getPendingInvalidationBaseTxnForTesting());

            // Unknown provenance merging with known collapses too. The one-arg publication carries
            // no provenance and defaults to forced.
            clearPendingInvalidation(state);
            state.markAsPendingInvalidationForTesting("known provenance", baseToken, 5, false);
            state.markAsPendingInvalidation("unknown provenance");
            Assert.assertNull(state.getPendingInvalidationBaseTableTokenForTesting());
            Assert.assertEquals(Numbers.LONG_NULL, state.getPendingInvalidationBaseTxnForTesting());
            Assert.assertTrue(state.isPendingInvalidationForcedForTesting());

            // Facets combine on one marker: the no-arg overload ADDS the owner facet (it is not a
            // no-op) while preserving the reason, and a later reason publication preserves the owner.
            Assert.assertFalse(state.hasPendingFullRefreshOwnerForTesting());
            state.markAsPendingFullRefreshForTesting();
            Assert.assertTrue("the no-arg overload must add the owner facet",
                    state.hasPendingFullRefreshOwnerForTesting());
            Assert.assertEquals("unknown provenance", state.getPendingInvalidationReason());
            state.markAsPendingInvalidationForTesting("fifth reason", baseToken, 2, false);
            Assert.assertTrue("a reason publication must preserve the owner facet",
                    state.hasPendingFullRefreshOwnerForTesting());
            Assert.assertEquals("fifth reason", state.getPendingInvalidationReason());

            clearPendingInvalidation(state);
        });
    }

    // Locks in the pending-invalidation marker state machine after the (pendingInvalidation,
    // pendingInvalidationReason) two-volatile composite was collapsed into a single atomic reference
    // (MatViewState#pendingInvalidationMarker). Every transition must be observable as exactly one of:
    // not-pending, pending-with-reason, owner-only, or reason-plus-owner combined -- never a torn mix.
    // Merge and facet-combination semantics are pinned by testPendingInvalidationMarkerMergeSemantics.
    @Test
    public void testPendingInvalidationMarkerStateMachineIsAtomic() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createUnseededAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // Fresh state: not pending, no reason.
            Assert.assertFalse(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // A reason-bearing deferral is observed atomically as pending AND carrying exactly that reason --
            // never the old torn (pending=true, reason=null).
            state.markAsPendingInvalidation("truncate operation");
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("truncate operation", state.getPendingInvalidationReason());

            // The no-arg overload is the full-refresh reschedule sentinel, and it is keep-strongest: on a
            // reason-bearing marker it adds the owner facet while preserving the reason, so a losing full
            // refresh cannot demote a deferral that a lock-holder's finalize would recover into one only
            // the queued full refresh clears.
            state.markAsPendingFullRefreshForTesting();
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("the sentinel must not demote a reason-bearing deferral",
                    "truncate operation", state.getPendingInvalidationReason());

            // From an empty marker the sentinel arms: pending, but with no reason, still distinct from the
            // cleared state.
            clearPendingInvalidation(state);
            state.markAsPendingFullRefreshForTesting();
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // The String overload rejects null; callers must use the no-arg overload for the sentinel.
            clearPendingInvalidation(state);
            try {
                state.markAsPendingInvalidation((String) null);
                Assert.fail("the reason-bearing overload must reject null");
            } catch (IllegalArgumentException expected) {
                Assert.assertEquals("invalidation reason must not be null", expected.getMessage());
            }
            Assert.assertFalse(state.isPendingInvalidation());

            // A reason-bearing mark upgrades the sentinel: a reason always wins the marker.
            state.markAsPendingInvalidation("truncate operation");
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("truncate operation", state.getPendingInvalidationReason());

            // Clearing drops the whole marker in a single write.
            clearPendingInvalidation(state);
            Assert.assertFalse(state.isPendingInvalidation());
            Assert.assertNull(state.getPendingInvalidationReason());

            // markAsValid changes only durable validity state; a concurrent pending owner survives.
            state.markAsPendingInvalidation("update operation");
            Assert.assertTrue(state.isPendingInvalidation());
            state.markAsValid();
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertEquals("update operation", state.getPendingInvalidationReason());
            clearPendingInvalidation(state);
        });
    }

    @Test
    public void testPendingTaskReenqueueScanIsSingleRunner() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            final MatViewStateStoreImpl store = (MatViewStateStoreImpl) engine.getMatViewStateStore();
            // Arm a claimable retry exactly as a failed finalize wake leaves it: a reason marker plus
            // the invalidation retry bit.
            Assert.assertTrue(state.tryLock());
            try {
                state.markAsPendingInvalidation("update operation");
            } finally {
                state.unlock();
            }
            store.requestPendingInvalidationReenqueue(state);

            // A scan entering while another runs must be a no-op. The seam fires inside the first
            // scan (after the running CAS), so the reentrant call exercises the guard: it must
            // neither throw nor deliver a duplicate wake.
            final AtomicInteger seamCalls = new AtomicInteger();
            try {
                store.setOnPendingTaskReenqueueScanForTesting(() -> {
                    seamCalls.incrementAndGet();
                    // Re-arm the store-wide signal: the outer scan cleared the requested flag before
                    // this seam fired, and without the re-arm the reentrant call would short-circuit
                    // on that flag instead of reaching the running-CAS guard under test.
                    store.requestPendingInvalidationReenqueue(state);
                    store.reenqueueFailedPendingTasks();
                });
                store.reenqueueFailedPendingTasks();
            } finally {
                store.setOnPendingTaskReenqueueScanForTesting(null);
            }

            Assert.assertEquals("the seam must fire once, in the outer scan only", 1, seamCalls.get());
            final MatViewRefreshTask task = new MatViewRefreshTask();
            Assert.assertTrue("the armed retry must deliver exactly one wake", store.tryDequeueRefreshTask(task));
            Assert.assertEquals(MatViewRefreshTask.INVALIDATE, task.operation);
            Assert.assertFalse("the concurrent scan attempt must not deliver a duplicate", store.tryDequeueRefreshTask(task));
        });
    }

    @Test
    public void testRangeOnlyPopulatedViewFinalizesDeferredInvalidationToInvalid() throws Exception {
        assertMemoryLeak(() -> {
            // A MANUAL view never auto-refreshes incrementally, so lastRefreshBaseTxn stays -1 even after a
            // user RANGE refresh populates rows (rangeRefreshSuccess does not advance lastRefreshBaseTxn).
            // This is the frozen-branch class: finalize used to early-return on lastRefreshBaseTxn == -1 and
            // leave the view pending forever (silently stale while reporting valid).
            final MatViewFixture fixture = createManualPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();
            Assert.assertEquals("precondition: the view has never been incrementally refreshed", -1, state.getLastRefreshBaseTxn());

            // Simulate a base-cascade INVALIDATE deferring while a user RANGE refresh holds the lock on this
            // range-only view. The range refresh completes (lastRefreshBaseTxn stays -1) and its finally must
            // wake the deferral. The INVALIDATE re-delivers force=true and mints, so the view
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
            final MatViewFixture fixture = createAutoPriceViewFixture();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

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
    public void testReadOnlyDeferralAtInvalidateViewRecordsReason() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Deliver an INVALIDATE on a read-only engine: invalidateView's top read-only branch must defer
            // carrying the reason, not the no-reason reschedule sentinel. The difference decides whether a
            // post-promote finalize can ever mint this deferral: finalize treats a null reason as a
            // full-refresh reschedule and never mints from it, so a sentinel here would strand the view
            // valid-but-stale after the promote instead of retrying the invalidation.
            isReadOnly.set(true);
            try {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "update operation");
                drainMatViewQueue(engine);
            } finally {
                isReadOnly.set(false);
            }

            Assert.assertTrue("the read-only delivery must defer", state.isPendingInvalidation());
            Assert.assertEquals("the read-only deferral must record the invalidation reason",
                    "update operation", state.getPendingInvalidationReason());
            Assert.assertFalse("the deferral alone must not mint", state.isInvalid());

            // The deferral is in-memory only: the view is still valid on disk.
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
    public void testReadOnlyEngineLeavesDeferredInvalidationUntouched() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Pins the post-release handoff's isReadOnlyMode early-return. Model a lock-holder completing
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
            try {
                state.markAsPendingInvalidation("update operation");
                isReadOnly.set(true);
                try {
                    MatViewRefreshJob.finalizeAndUnlock(engine, engine.getMatViewStateStore(), viewToken, state, false);
                } finally {
                    isReadOnly.set(false);
                }
            } finally {
                if (state.isLocked()) {
                    state.unlock();
                }
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
    public void testReenqueuePendingOnResumeIgnoresBaseTableToken() throws Exception {
        assertMemoryLeak(() -> {
            createAutoPriceViewFixture();

            // Every base-table RESUME WAL routes through reenqueuePendingOnResume with the BASE
            // token, which has no view state. The call must be a silent no-op.
            final TableToken baseToken = engine.getTableTokenIfExists("base_price");
            Assert.assertNotNull(baseToken);
            engine.getMatViewStateStore().reenqueuePendingOnResume(baseToken);

            final MatViewRefreshTask task = new MatViewRefreshTask();
            Assert.assertFalse(
                    "a base-table resume must not enqueue view work",
                    engine.getMatViewStateStore().tryDequeueRefreshTask(task)
            );
        });
    }

    @Test
    public void testReenqueuePendingOnResumeSkipsStillSuspendedView() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Park a reason marker, hard-suspend the view, then call the redelivery entry point
            // directly. While writes stay denied the redelivery must keep the marker parked:
            // enqueueing would bounce the task off the write-denied gate and consume it.
            Assert.assertTrue(state.tryLock());
            try {
                state.markAsPendingInvalidation("update operation");
            } finally {
                state.unlock();
            }
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken);

            Assert.assertTrue("a still-suspended view must keep its marker parked", state.isPendingInvalidation());
            final MatViewRefreshTask task = new MatViewRefreshTask();
            Assert.assertFalse(engine.getMatViewStateStore().tryDequeueRefreshTask(task));

            // Leave the fixture consistent for teardown.
            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainMatViewQueue(engine);
            drainWalQueue();
        });
    }

    @Test
    public void testRefreshHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final MatViewState state = fixture.state();
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
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Model a pending owner plus an already-queued duplicate. The holder's handoff retains the marker,
            // and whichever delivery acquires the latch first persists the invalid state and clears ownership.
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
    public void testResumeWalRedeliversSuspendedInvalidation() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // The real apply-time INVALIDATE publishes ownership, then the suspended-view gate consumes its
            // only task. RESUME WAL must redeliver that retained owner without relying on another base write.
            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            execute("UPDATE base_price SET amount = 7");
            drainWalAndMatViewQueues();

            Assert.assertTrue("the suspended invalidation must retain ownership", state.isPendingInvalidation());
            Assert.assertFalse("the suspended view cannot mint invalid state yet", state.isInvalid());
            Assert.assertEquals("update operation", state.getPendingInvalidationReason());

            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainWalAndMatViewQueues();

            Assert.assertFalse("resume must drive the retained invalidation to completion", state.isPendingInvalidation());
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
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

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
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final MatViewState state = fixture.state();

            // A deferral already landed while another holder was active. STATS is the holder that performs
            // the post-release handoff in this test.
            state.markAsPendingInvalidation("update operation");
            Assert.assertTrue(state.isPendingInvalidation());
            Assert.assertFalse(state.isInvalid());

            // REFRESH ... STATS takes the same per-view latch, synchronously on the SQL thread. It is a
            // lock-holder like any refresh: its unlock must finalize the deferral, or the view stays frozen.
            final long refreshSeqBefore = state.getRefreshSeq();
            execute("refresh materialized view price_1h stats");
            // Not a data refresh: the STATS holder passes shouldIncrementRefreshSeq=false, so the seq that
            // MatViewTimerJob reads for refresh dedup must not move.
            Assert.assertEquals("the STATS holder must not bump the refresh seq", refreshSeqBefore, state.getRefreshSeq());
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
    public void testStickyWriterRefusalBothFacetsRotateBoundedAndRecover() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The residual worst case with BOTH facets pending under a sticky refusal: each holder's
            // deferral suppresses only its OWN facet's wake, so the refused INVALIDATE wakes the FULL
            // owner and the refused FULL wakes the INVALIDATE -- a one-task rotation that cannot drain
            // to empty while the refusal holds. This test pins the two properties the rotation does
            // keep: no growth (each dequeue enqueues at most one task; the per-facet counters stay at
            // their expected exact values) and full recovery (once the refusal clears, whichever facet
            // runs next succeeds and the drain converges with both facets delivered).
            final AtomicInteger fullWakeCount = new AtomicInteger();
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    if (fullWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding full refresh loop detected");
                    }
                    super.enqueueFullRefresh(matViewToken, fullRefreshOwner);
                }

                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    if (invalidationWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding invalidation loop detected");
                    }
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            walRefusalToken.set(viewToken);
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                final AtomicInteger dequeueCount = new AtomicInteger();
                job.setOnRefreshTaskDequeuedForTesting(() -> {
                    final int dequeue = dequeueCount.incrementAndGet();
                    // Rotation ledger under the refusal: d1 INV defers (no wake: no owner facet yet),
                    // d2 FULL defers (wakes INV), d3 INV defers (wakes FULL), d4 FULL defers (wakes
                    // INV). The 5th dequeue is the INVALIDATE again; clear the refusal before it
                    // executes so the rotation self-terminates by delivering both facets.
                    if (dequeue == 5) {
                        walRefusalToken.set(null);
                    }
                    Assert.assertTrue("the both-facet rotation must not grow the queue", dequeue <= 8);
                });

                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "both facets witness");
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);

                Assert.assertEquals("each refused FULL pass must wake the invalidation exactly once",
                        2, invalidationWakeCount.get());
                Assert.assertEquals("each refused INVALIDATE pass and the final mint must wake the owner exactly once",
                        2, fullWakeCount.get());
            } finally {
                walRefusalToken.set(null);
            }
            drainWalQueue();

            Assert.assertFalse("the recovery must consume both facets", state.isPendingInvalidation());
            Assert.assertFalse("the final full refresh must leave the view valid", state.isInvalid());
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
    public void testStickyWriterRefusalDefersFullRefreshWithoutRequeue() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The enterprise TOCTOU on the FULL facet: fullRefresh's writer acquire refuses with a
            // read-only authorization error while isReadOnlyMode() still reads false, and the refusal
            // is sticky until a re-promote. The deferral must not redeliver through EITHER channel --
            // the handleErrorRetryRefresh re-enqueue and the finalize owner wake each hand the same
            // drain loop the owner-carrying task it just refused, and together they mint two tasks per
            // refused pass (unbounded queue growth; the drain never converges). The counting store and
            // the dequeue seam bound a regression deterministically instead of hanging the test.
            final AtomicInteger fullWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    if (fullWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding full refresh loop detected");
                    }
                    super.enqueueFullRefresh(matViewToken, fullRefreshOwner);
                }
            };

            walRefusalToken.set(viewToken);
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                final AtomicInteger dequeueCount = new AtomicInteger();
                job.setOnRefreshTaskDequeuedForTesting(() -> Assert.assertEquals(
                        "the refused full refresh must not redeliver its own owner",
                        1,
                        dequeueCount.incrementAndGet()
                ));
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);

                Assert.assertEquals("the refused holder must not wake its own owner", 0, fullWakeCount.get());
                Assert.assertTrue("the refused full refresh must be deferred (owner pending)", state.isPendingInvalidation());
                Assert.assertNull("the deferral must be owner-only (no invalidation reason)", state.getPendingInvalidationReason());
                Assert.assertFalse("a writer refusal must not mark the view invalid", state.isInvalid());
                Assert.assertFalse("the refused holder must release the view latch", state.isLocked());

                // The re-promote: the refusal clears and the retained owner must still be deliverable
                // through the out-of-band redelivery entry point -- the deferral must not have
                // stranded the requested rebuild.
                walRefusalToken.set(null);
                job.setOnRefreshTaskDequeuedForTesting(null);
                engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken);
                drainMatViewQueue(job);
            } finally {
                walRefusalToken.set(null);
            }
            drainWalQueue();

            Assert.assertFalse("the redelivered full refresh must consume the pending owner", state.isPendingInvalidation());
            Assert.assertFalse("the recovered full refresh must leave the view valid", state.isInvalid());
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
    public void testStickyWriterRefusalDefersMidPumpFullRefreshWithoutRequeue() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The commit-fence face of the same refusal: the writer acquire succeeds, the refusal lands
            // inside the pump (rethrowReadOnlyRefusal re-throws it to the outer catch), and the deferral
            // must take the same no-requeue path as the acquire face.
            final AtomicInteger fullWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    if (fullWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding full refresh loop detected");
                    }
                    super.enqueueFullRefresh(matViewToken, fullRefreshOwner);
                }
            };

            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                final AtomicInteger dequeueCount = new AtomicInteger();
                job.setOnRefreshTaskDequeuedForTesting(() -> Assert.assertEquals(
                        "the refused full refresh must not redeliver its own owner",
                        1,
                        dequeueCount.incrementAndGet()
                ));
                // Sticky: every pass refuses until the seam clears, exactly like the writer chokepoint.
                job.setOnBaseReaderSnapshotForTesting(() -> {
                    throw CairoException.readOnlyAccess();
                });
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);

                Assert.assertEquals("the refused holder must not wake its own owner", 0, fullWakeCount.get());
                Assert.assertTrue("the refused full refresh must be deferred (owner pending)", state.isPendingInvalidation());
                Assert.assertNull("the deferral must be owner-only (no invalidation reason)", state.getPendingInvalidationReason());
                Assert.assertFalse("a mid-pump refusal must not mark the view invalid", state.isInvalid());
                Assert.assertFalse("the refused holder must release the view latch", state.isLocked());

                // The re-promote: redelivery must complete the deferred rebuild.
                job.setOnBaseReaderSnapshotForTesting(null);
                job.setOnRefreshTaskDequeuedForTesting(null);
                engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken);
                drainMatViewQueue(job);
            }
            drainWalQueue();

            Assert.assertFalse("the redelivered full refresh must consume the pending owner", state.isPendingInvalidation());
            Assert.assertFalse("the recovered full refresh must leave the view valid", state.isInvalid());
        });
    }

    @Test
    public void testStickyWriterRefusalDefersWithoutSelfFeeding() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The enterprise TOCTOU (MatViewSwitchInvariantsTest#invalidateInnerLoopDefersOnReadOnlyRefusalAndDoesNotSpin):
            // invalidateView's top guard sees a writable node, but the getWalWriter acquire refuses with a
            // read-only authorization error, and the refusal is sticky until a re-promote. The auth-rollback
            // catch must defer once -- retain the marker and return -- not hand finalizeAndUnlock a wake-up
            // that the very next drain pass feeds back into the same refused acquire forever. The counting
            // store bounds that spin: with the self-feed present, every pass re-enqueues the INVALIDATE it
            // just dequeued and the counter trips within the first few iterations instead of hanging the test.
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    if (invalidationWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding invalidation loop detected");
                    }
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            walRefusalToken.set(viewToken);
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "sticky refusal witness");
                drainMatViewQueue(job);

                Assert.assertEquals("the refused holder must not wake its own marker", 0, invalidationWakeCount.get());
                Assert.assertTrue("the refused invalidation must be deferred (pending)", state.isPendingInvalidation());
                Assert.assertFalse("a writer refusal must not mark the view invalid", state.isInvalid());

                // The re-promote: the refusal clears and a redelivery must still mint from the retained
                // marker -- the deferral above must not have stranded the invalidation.
                walRefusalToken.set(null);
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "post-promote redelivery");
                drainMatViewQueue(job);
            } finally {
                walRefusalToken.set(null);
            }
            drainWalQueue();

            Assert.assertTrue("the redelivered invalidation must mint invalid state", state.isInvalid());
            Assert.assertFalse("the mint must consume the pending marker", state.isPendingInvalidation());
        });
    }

    @Test
    public void testStickyWriterRefusalRecoversViaNextRefreshHoldersFinalize() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The enterprise aborted-demote window: the writer chokepoint refuses while the engine
            // still reports writable, the refused FULL defers (owner retained, wake suppressed,
            // nothing re-enqueued), and the store stays alive -- no promote-time rebuild will run.
            walRefusalToken.set(viewToken);
            try (MatViewRefreshJob job = createMatViewRefreshJob(engine)) {
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);

                Assert.assertTrue("the refused full refresh must be deferred (owner pending)", state.isPendingInvalidation());
                Assert.assertNull("the deferral must be owner-only (no invalidation reason)", state.getPendingInvalidationReason());
                Assert.assertFalse(state.isInvalid());
                Assert.assertFalse(state.isLocked());

                // The refusal clears (the heal path of the aborted role switch). NO explicit
                // redelivery call: recovery must come from ordinary refresh traffic alone. The
                // owner-only marker must not gate the incremental holder off the latch; that
                // holder's post-release finalize is the redelivery channel for the retained owner.
                walRefusalToken.set(null);
                execute("insert into base_price (sym, price, ts) values('gbpusd', 1.423, '2024-09-10T15:00')");
                drainWalQueue();
                drainMatViewQueue(job);
            }
            drainWalQueue();
            drainMatViewQueue(engine);
            drainWalQueue();

            Assert.assertFalse(
                    "ordinary refresh traffic must redeliver and consume the deferred owner",
                    state.isPendingInvalidation()
            );
            Assert.assertFalse(state.isInvalid());
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
    public void testStickyWriterRefusalStillWakesConcurrentPublication() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // Guards the other half of the identity-keyed suppression: an invalidation that publishes while
            // the refused holder still owns the latch replaces the marker object, so the holder's finalize
            // must wake it (identity mismatch) -- suppression is strictly for the holder's own refused
            // marker. The wake re-delivers, the retry publishes its own marker, refuses, matches identity,
            // and stops: exactly one wake, no self-feed.
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    if (invalidationWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding invalidation loop detected");
                    }
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            walRefusalToken.set(viewToken);
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                final AtomicBoolean hasPublishedConcurrently = new AtomicBoolean();
                job.setOnHoldingLockForTesting(() -> {
                    // One-shot: only the first (refused) hold sees a concurrent publication; the woken
                    // retry must run without one so it can suppress its own refused marker and stop.
                    if (hasPublishedConcurrently.compareAndSet(false, true)) {
                        state.markAsPendingInvalidation("concurrent publication");
                    }
                });
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "sticky refusal witness");
                drainMatViewQueue(job);

                Assert.assertEquals("the concurrent publication must be woken exactly once",
                        1, invalidationWakeCount.get());
                Assert.assertTrue("the refused invalidation must be deferred (pending)", state.isPendingInvalidation());
                Assert.assertEquals("the retained marker must carry the concurrent publication's reason",
                        "concurrent publication", state.getPendingInvalidationReason());
                Assert.assertFalse("a writer refusal must not mark the view invalid", state.isInvalid());
            } finally {
                walRefusalToken.set(null);
            }
        });
    }

    @Test
    public void testStickyWriterRefusalStillWakesFullRefreshFacet() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // A full-refresh owner that publishes onto the marker while the refused holder owns the latch
            // must never be stranded by the suppression: finalizeAndUnlock0 skips only the invalidation
            // facet of the holder's own refused marker. The owner publication replaces the marker object,
            // so the first finalize sees an identity mismatch and wakes the invalidation once (bounded);
            // the woken retry then suppresses its own refused marker. The FULL facet wakes on both passes
            // (each woken task would defer against the refusal without re-enqueueing); the counting store
            // does not delegate them, so no full refresh runs against the armed refusal.
            final AtomicInteger fullWakeCount = new AtomicInteger();
            final AtomicInteger invalidationWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    fullWakeCount.incrementAndGet();
                }

                @Override
                public void enqueueInvalidate(
                        TableToken matViewToken,
                        String invalidationReason,
                        TableToken invalidationBaseTableToken,
                        long invalidationBaseTxn,
                        boolean isInvalidationForced
                ) {
                    if (invalidationWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding invalidation loop detected");
                    }
                    super.enqueueInvalidate(
                            matViewToken,
                            invalidationReason,
                            invalidationBaseTableToken,
                            invalidationBaseTxn,
                            isInvalidationForced
                    );
                }
            };

            walRefusalToken.set(viewToken);
            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                final AtomicBoolean hasPublishedFullOwner = new AtomicBoolean();
                job.setOnHoldingLockForTesting(() -> {
                    // One-shot: the FULL owner lands during the first (refused) hold only.
                    if (hasPublishedFullOwner.compareAndSet(false, true)) {
                        state.markAsPendingFullRefreshForTesting();
                    }
                });
                engine.getMatViewStateStore().enqueueInvalidate(viewToken, "sticky refusal witness");
                drainMatViewQueue(job);

                Assert.assertEquals("the owner publication must wake the invalidation exactly once",
                        1, invalidationWakeCount.get());
                Assert.assertEquals("both passes must wake the never-suppressed full-refresh facet",
                        2, fullWakeCount.get());
                Assert.assertTrue("the refused invalidation must be deferred (pending)", state.isPendingInvalidation());
                Assert.assertEquals("the retained marker must keep the refused reason",
                        "sticky refusal witness", state.getPendingInvalidationReason());
                Assert.assertFalse("a writer refusal must not mark the view invalid", state.isInvalid());
            } finally {
                walRefusalToken.set(null);
            }
        });
    }

    @Test
    public void testStickyWriterRefusalStillWakesNewerFullOwner() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            // The other half of the owner-identity suppression: a FULL request that publishes while the
            // refused holder owns the latch mints a fresh owner object, so the holder's finalize sees an
            // identity mismatch and must wake it -- suppression is strictly for the holder's own refused
            // owner. The woken retry publishes nothing new, refuses, matches identity, and stops:
            // exactly one wake, no self-feed.
            final AtomicInteger fullWakeCount = new AtomicInteger();
            final MatViewStateStore countingStore = new ForwardingMatViewStateStore(engine.getMatViewStateStore()) {
                @Override
                public void enqueueFullRefresh(TableToken matViewToken, Object fullRefreshOwner) {
                    if (fullWakeCount.incrementAndGet() > 10) {
                        throw new IllegalStateException("self-feeding full refresh loop detected");
                    }
                    super.enqueueFullRefresh(matViewToken, fullRefreshOwner);
                }
            };

            try (MatViewRefreshJob job = new MatViewRefreshJob(engine, 1, countingStore)) {
                final AtomicBoolean hasPublishedNewerOwner = new AtomicBoolean();
                // The refusal must land AFTER the concurrent publication, so the holder's own refused
                // owner is already stale when its finalize runs. The seam publishes once (mid-hold, the
                // losing-contender shape) and refuses on every pass -- sticky, like the writer chokepoint.
                job.setOnHoldingLockForTesting(() -> {
                    if (hasPublishedNewerOwner.compareAndSet(false, true)) {
                        state.markAsPendingFullRefreshForTesting();
                    }
                    throw CairoException.readOnlyAccess();
                });
                engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
                drainMatViewQueue(job);

                Assert.assertTrue("the test must publish a newer owner mid-hold", hasPublishedNewerOwner.get());
                Assert.assertEquals("the newer owner must be woken exactly once", 1, fullWakeCount.get());
                Assert.assertTrue("the refused full refresh must stay deferred (owner pending)", state.isPendingInvalidation());
                Assert.assertNull("the deferral must be owner-only (no invalidation reason)", state.getPendingInvalidationReason());
                Assert.assertFalse("a writer refusal must not mark the view invalid", state.isInvalid());

                // The re-promote: the retained owner must still complete through redelivery.
                job.setOnHoldingLockForTesting(null);
                engine.getMatViewStateStore().reenqueuePendingOnResume(viewToken);
                drainMatViewQueue(job);
            }
            drainWalQueue();

            Assert.assertFalse("the redelivered full refresh must consume the pending owner", state.isPendingInvalidation());
            Assert.assertFalse("the recovered full refresh must leave the view valid", state.isInvalid());
        });
    }

    @Test
    public void testSuspendedFullRefreshRetainsOwnerAndRedeliversOnResume() throws Exception {
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

            execute("ALTER MATERIALIZED VIEW price_1h SUSPEND WAL");
            engine.getMatViewStateStore().enqueueFullRefresh(viewToken);
            drainMatViewQueue(engine);

            // The suspended exit must park the request as a pending owner facet. The pre-fix code
            // consumed the ownerless task before the owner mint, so RESUME WAL had nothing to
            // redeliver and the user's REFRESH FULL vanished silently.
            Assert.assertTrue("a suspended REFRESH FULL must park its owner on the marker", state.isPendingInvalidation());
            Assert.assertNull("the parked facet must be owner-only", state.getPendingInvalidationReason());
            Assert.assertFalse(state.isInvalid());
            Assert.assertFalse(state.isLocked());

            execute("ALTER MATERIALIZED VIEW price_1h RESUME WAL");
            drainMatViewQueue(engine);
            drainWalQueue();

            Assert.assertFalse("RESUME WAL must redeliver and consume the parked owner", state.isPendingInvalidation());
            Assert.assertFalse(state.isInvalid());
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
    public void testUpdateRefreshIntervalsHoldingLockFinalizesDeferredInvalidation() throws Exception {
        assertMemoryLeak(() -> {
            final MatViewFixture fixture = createAutoPriceViewFixture();

            // Baseline: the view refreshed and is valid.
            assertQuery("select view_name, base_table_name, view_status from materialized_views")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("""
                            view_name\tbase_table_name\tview_status
                            price_1h\tbase_price\tvalid
                            """);

            final TableToken viewToken = fixture.viewToken();
            final MatViewState state = fixture.state();

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

    private void assertPendingReasonAndFullFacets(TableToken viewToken, MatViewState state, String expectedReason) {
        final MatViewStateStore stateStore = engine.getMatViewStateStore();
        final MatViewRefreshTask firstTask = new MatViewRefreshTask();
        final MatViewRefreshTask secondTask = new MatViewRefreshTask();
        final MatViewRefreshTask unexpectedTask = new MatViewRefreshTask();
        try {
            Assert.assertEquals("the concurrent FULL publication erased the invalidation reason",
                    expectedReason, state.getPendingInvalidationReason());
            Assert.assertTrue(state.tryLock());
            MatViewRefreshJob.finalizeAndUnlock(engine, stateStore, viewToken, state, false);

            Assert.assertTrue("the reason facet must publish an INVALIDATE task", stateStore.tryDequeueRefreshTask(firstTask));
            Assert.assertEquals(MatViewRefreshTask.INVALIDATE, firstTask.operation);
            Assert.assertTrue("the full-refresh facet must publish a FULL task", stateStore.tryDequeueRefreshTask(secondTask));
            Assert.assertEquals(MatViewRefreshTask.FULL_REFRESH, secondTask.operation);
            Assert.assertFalse("the marker must contain exactly two pending facets",
                    stateStore.tryDequeueRefreshTask(unexpectedTask));
        } finally {
            clearPendingInvalidation(state);
            while (stateStore.tryDequeueRefreshTask(unexpectedTask)) {
                unexpectedTask.clear();
            }
        }
    }

    private void clearPendingInvalidation(MatViewState state) {
        Assert.assertTrue("the test must own the view latch before clearing pending intent", state.tryLock());
        try {
            state.clearPendingInvalidationForTesting();
        } finally {
            state.unlock();
        }
    }

    private void createAutoPriceViewDefinition() throws SqlException {
        createBasePriceTable();
        execute("CREATE MATERIALIZED VIEW price_1h AS (" +
                "SELECT sym, last(price) AS price, ts FROM base_price SAMPLE BY 1h" +
                ") PARTITION BY DAY");
    }

    private MatViewFixture createAutoPriceViewFixture() throws SqlException {
        createAutoPriceViewDefinition();
        insertBasePriceRows();
        drainWalAndMatViewQueues();
        return resolvePriceViewFixture();
    }

    private void createBasePriceTable() throws SqlException {
        execute("CREATE TABLE base_price (" +
                "sym VARCHAR, price DOUBLE, amount INT, ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private MatViewFixture createManualPriceViewAndDrainFixture() throws SqlException {
        final MatViewFixture fixture = createManualPriceViewFixture();
        drainWalAndMatViewQueues();
        return fixture;
    }

    private void createManualPriceViewDefinition() throws SqlException {
        createBasePriceTable();
        execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (" +
                "SELECT sym, last(price) AS price, ts FROM base_price SAMPLE BY 1h" +
                ") PARTITION BY DAY");
    }

    private MatViewFixture createManualPriceViewFixture() throws SqlException {
        createManualPriceViewDefinition();
        insertBasePriceRows();
        drainWalQueue();
        return resolvePriceViewFixture();
    }

    private MatViewFixture createSumAmountViewFixture() throws SqlException {
        createBasePriceTable();
        execute("CREATE MATERIALIZED VIEW price_1h AS (" +
                "SELECT sym, sum(amount) AS amount, ts FROM base_price SAMPLE BY 1h" +
                ") PARTITION BY DAY");
        execute("""
                INSERT INTO base_price (sym, amount, ts) VALUES
                ('gbpusd', 1, '2024-09-10T12:01'),
                ('gbpusd', 2, '2024-09-10T12:02'),
                ('jpyusd', 3, '2024-09-10T12:02')
                """);
        drainWalAndMatViewQueues();

        return resolvePriceViewFixture();
    }

    private MatViewFixture createUnseededAutoPriceViewFixture() throws SqlException {
        createAutoPriceViewDefinition();
        drainWalAndMatViewQueues();
        return resolvePriceViewFixture();
    }

    private void insertBasePriceRows() throws SqlException {
        execute("""
                INSERT INTO base_price (sym, price, ts) VALUES
                ('gbpusd', 1.320, '2024-09-10T12:01'),
                ('gbpusd', 1.323, '2024-09-10T12:02'),
                ('jpyusd', 103.21, '2024-09-10T12:02')
                """);
    }

    private MatViewFixture resolvePriceViewFixture() {
        final TableToken viewToken = engine.verifyTableName("price_1h");
        final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
        Assert.assertNotNull("expected a real (non-no-op) state store to hold the view state", state);
        return new MatViewFixture(viewToken, state);
    }

    private record MatViewFixture(TableToken viewToken, MatViewState state) {
    }
}
