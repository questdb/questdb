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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.ForwardingLiveViewStateStore;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshTask;
import io.questdb.cairo.lv.LiveViewStateStore;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.lv.LiveViewPageFrameCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Concurrency tests for live views, covering the ingestion and lifecycle shapes the
 * single-writer {@link LiveViewFuzzTest} cannot reach. The production-shaped scenarios:
 * <ul>
 *   <li><b>Multi-WalWriter base interleaving.</b> Several threads each open their own
 *   {@link WalWriter} on the same base table and commit concurrently, so the sequencer
 *   weaves their transactions into an interleaved log. Because a later-committed
 *   transaction can carry earlier timestamps than an already-materialized one, the
 *   live view's incremental refresh exercises O3 head-miss replay over a transaction
 *   stream no single-writer test produces.</li>
 *   <li><b>Concurrent refresh during ingestion.</b> A refresh-driver thread applies
 *   the base WAL and runs the refresh job while the writer threads are still
 *   ingesting - the steady-state production timing where base writes and live-view
 *   maintenance overlap.</li>
 *   <li><b>Concurrent DROP during refresh.</b> A {@code DROP LIVE VIEW} races a
 *   refresh-driver thread that keeps pumping the refresh job. The refresh job swallows
 *   per-view failures (so a torn-down view never throws into the worker); the test
 *   asserts the drop tears the view down cleanly - registry empty, base table intact,
 *   no leak, no crash - whatever the interleaving.</li>
 *   <li><b>Concurrent CREATE during ingestion.</b> A {@code CREATE LIVE VIEW ...
 *   SEED} races concurrent base writes. The earliest rows are committed before
 *   CREATE so the seed floor sits at the global-min timestamp and no concurrently
 *   ingested row falls below it; the seed sweep and forward refresh between them
 *   cover every row exactly once, so the final state still equals a from-scratch
 *   recompute.</li>
 *   <li><b>Reader-churn soak.</b> Many reader threads repeatedly open and drain a
 *   cursor over an {@code IN MEMORY} live view while a refresh-driver appends to the
 *   in-memory tier via the fast-path CAS and writers ingest - the lock-free
 *   read/publish hand-off is the concurrency risk under test. Readers must never see a torn
 *   read or crash, and the quiesced final state still matches the recompute. The
 *   {@code InMem} variant uses a SYMBOL-free {@code row_number()} view so the reads
 *   route through Mode B (seam routing over the pinned slot), and each read asserts
 *   the per-snapshot invariant - rows ts-ascending, rn a gapless 1..N sequence - so
 *   a stale-restamped pre-O3 row or a seam duplicate/gap fails the read. The
 *   cross-writer O3 drives the in-mem rebuild against the live Mode B readers (the
 *   both-slots-pinned skip path). The {@code VarSize} variant adds STRING + VARCHAR
 *   passthrough columns so the reads also dereference the tier's var-length (data,
 *   aux) regions - which realloc and move their base address on append - under the
 *   same lock-free hand-off; the per-snapshot invariant extends to the var-length
 *   values decoding back to their ts-derived form. No ARM-specific canary is needed
 *   for it because, unlike the cross-slot symbol cache, var-length values live in
 *   the per-slot buffers and are frozen while a reader pins the slot.</li>
 *   <li><b>Parallel filter racing a tier swap.</b> Every soak above reads through the
 *   record-cursor path, whose slot reads all happen on the reading thread. A FILTERED
 *   read instead routes through {@code LiveViewPageFrameCursor}, which publishes the
 *   pinned slot's raw native addresses as a page frame and hands that frame to filter
 *   workers on other threads - so the slot gets read by threads that never pinned it.
 *   This variant runs real filter workers (a query {@code WorkerPool}) over the frame
 *   while the refresh worker swaps tier slots on EVERY publish (a growth budget of 0
 *   forces the slow path) and writers ingest. The safety argument is that a frame
 *   consumer cannot outlive the cursor that holds the pin, so the writer can only ever
 *   reclaim the slot the readers are not on; a break is a use-after-free, surfacing as
 *   a torn value against the same per-snapshot invariant, or as a crash.</li>
 * </ul>
 * <p>
 * <b>Why the oracle stays deterministic despite the concurrency.</b> The premise of
 * an incremental-maintenance engine is that the incrementally materialized state
 * equals a from-scratch recompute over the base table. The threads race only during
 * ingestion (and, for the second scenario, refresh); the test then joins every
 * worker, quiesces the refresh single-threaded, and only then compares the live view
 * against the same window query recomputed over the base table. Whatever order the
 * transactions interleaved in, the final state is a deterministic function of the row
 * set, so the comparison is sound. As in the fuzz test, every generated row has a
 * strictly-unique, strictly-increasing timestamp, so {@code OVER (ORDER BY ts ...)}
 * and the natural ts scan order used by {@code OVER ()} are total orders that both
 * the incremental and the batch path agree on. Out-of-order ingestion comes purely
 * from the cross-writer commit interleaving, never from colliding timestamps.
 * <p>
 * The test clock is pinned a full year below the data: a non-seed view's lower
 * bound is the wall-clock CREATE moment and O3 head-miss replay only re-emits base
 * rows at or above that floor, so the data must sit above the clock. The refresh
 * driver advances the clock to clear FLUSH EVERY gating, but never by enough to cross
 * the one-year gap, so every row stays above the floor.
 */
public class LiveViewConcurrencyTest extends AbstractLiveViewTest {

    // The clock sits at 2026-01-01 (the CREATE moment / view lower bound); the data
    // starts a year later so the refresh driver's clock advances can never lift the
    // floor above a data row, even if it spins many times during ingestion.
    private static final String CLOCK_START = "2026-01-01T00:00:00.000000Z";
    private static final String DATA_START = "2027-01-01T00:00:00.000000Z";
    // Filter workers for the parallel-filter tier-swap soak: the pool's size and, because
    // the two must agree for the reduce work to land on those threads, the shared-worker
    // count each reader's execution context declares.
    private static final int FILTER_WORKER_COUNT = 4;
    // The filtered read the parallel-filter soak routes through the tier's page frame. The
    // predicate keeps every row of the (ts, i, rn) view - see readFilteredRowNumberViewOnce.
    private static final String FILTERED_VIEW_SQL = "SELECT * FROM lv WHERE rn > 0";
    // How long a paced writer waits for the refresh driver's next tick before giving up on
    // it. Generous: it bounds a stalled driver, it does not pace anything itself.
    private static final long REFRESH_TICK_WAIT_NANOS = 30_000_000_000L;
    // Refresh passes the parallel-filter soak's driver runs per tick, matching drainJob's
    // own bound - the driver inlines that loop to sample the tier between passes.
    private static final int REFRESH_PASSES_PER_TICK = 64;
    private static final String[] SYMBOLS = {"AA", "BB", "CC", "DD"};
    // Fault injection for testCreateRollbackDefersFreeWhileRefreshLatchHeld, armed for the duration of
    // that one test. Null (the default) makes the state-store wrapper below a pure pass-through, so
    // every other test in the class sees a stock engine.
    private static volatile Runnable registerBaseTableFault;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // The soaks advance the mocked microsecond clock to drive FLUSH EVERY while readers hold
        // cursors open. Keep millisecond deadlines on the production wall clock so those synthetic
        // jumps cannot consume TableReader's spin timeout.
        AbstractCairoTest.configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public MillisecondClock getMillisecondClock() {
                        return MillisecondClockImpl.INSTANCE;
                    }
                };
        // The engine builds its LiveViewStateStore once, in load(), via the createLiveViewStateStore
        // hook. Wrapping it here - rather than reflecting the field out of a live engine and setting
        // it back afterwards - keeps the swap inside the API the engine already exposes for it.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            protected LiveViewStateStore createLiveViewStateStore() {
                return new ForwardingLiveViewStateStore(super.createLiveViewStateStore()) {
                    @Override
                    public void registerBaseTable(CharSequence baseTableName) {
                        final Runnable fault = registerBaseTableFault;
                        if (fault != null) {
                            fault.run();
                            return;
                        }
                        super.registerBaseTable(baseTableName);
                    }
                };
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testApplyLagDeferralRebuildsAdvancedWindowState() throws Exception {
        // Regression for questdb#7514: a reader saw the view's FIRST row carrying a running
        // count well above 1 (rn=193 on a 12-row lead, rn=201 on a 22-row generation).
        //
        // The drain walks base commits in sequencer order, feeding each through the compiled
        // window cursor. On an out-of-order commit it rolls the cycle back - the WAL draft and
        // latestSeenTs - and hands off to o3Replay, which clears the accumulators before it
        // recomputes. But o3Replay first gates on the base being applied, and when
        // ApplyWal2TableJob has not caught up ensureBaseApplied throws LiveViewApplyLagException
        // to defer the cycle. That deferral used to be treated as a clean no-op. It is not: the
        // commits BELOW the offending one had already advanced the accumulators, and the
        // rollback does not undo them. windowStateDirty is a per-turn field that refreshInstance
        // re-seeds from the instance at every entry, so the debt evaporated and the next turn
        // drained the same commits again over accumulators that already counted them.
        //
        // This test drives that interleaving with no threads at all: it withholds the base apply
        // so ensureBaseApplied is guaranteed to throw. The assertion is the debt itself -
        // isWindowStateDirty() after the deferral - because that is what the next turn reads.
        // Asserting only the final contents would not hold the fix: once the apply lands, the
        // re-drain hits the same O3 commit again, this time replays for real, and clearWindowState
        // converges the view. That is exactly why the soak only ever caught this mid-flight.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("CREATE TABLE base (ts TIMESTAMP, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS
                    SELECT ts, i, count(*) OVER (
                        PARTITION BY 0
                        ORDER BY ts
                        ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW
                    ) AS rn
                    FROM base
                    """);

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                Assert.assertFalse("the seeded view must start healthy", instance.isInvalid());
                Assert.assertFalse("a quiesced seed owes no window-state rebuild", instance.isWindowStateDirty());

                // Commit 1: three in-order rows. The drain feeds these through the window cursor,
                // which is what leaves the accumulators at 3.
                execute("""
                        INSERT INTO base VALUES
                            ('2027-01-01T00:00:01.000000Z', 1),
                            ('2027-01-01T00:00:02.000000Z', 2),
                            ('2027-01-01T00:00:03.000000Z', 3)
                        """);
                // Commit 2: a row below commit 1's maximum, so the drain classifies it as
                // cross-commit O3 and diverts to o3Replay.
                execute("INSERT INTO base VALUES ('2027-01-01T00:00:00.500000Z', 4)");

                // Both commits are in the sequencer, but only commit 1 queued a refresh task:
                // LiveViewStateStoreImpl gates the notification per base table and commit 2 found
                // the gate closed. A turn driven from that task would stop AT commit 1, so the two
                // commits would land in different turns and the O3 detect would fire having fed
                // nothing - not the shape this regression is about. Consume the task without
                // refreshing; notifyBaseRefreshed observes the newer commit and re-enqueues at
                // commit 2's seqTxn, so the next turn walks BOTH: it feeds commit 1 through the
                // window cursor and only THEN discovers commit 2 is out of order.
                final LiveViewStateStore stateStore = engine.getLiveViewStateStore();
                final LiveViewRefreshTask pendingTask = new LiveViewRefreshTask();
                Assert.assertTrue(
                        "the base commits must have queued a refresh task",
                        stateStore.tryDequeueRefreshTask(pendingTask)
                );
                stateStore.notifyBaseRefreshed(pendingTask, pendingTask.seqTxn);

                // Deliberately NO drainWalQueue() here. The raw-WAL drain reads both commits, but
                // the base TABLE is unapplied, so o3Replay's ensureBaseApplied gate cannot be
                // satisfied and the cycle defers. This is the whole point of the fixture: it makes
                // the apply lag a certainty rather than a race the test would have to win.
                drainJob(job);

                Assert.assertNotEquals(
                        "the cycle must have deferred on base apply lag, or this test is not exercising the gate",
                        Numbers.LONG_NULL,
                        instance.getApplyLagDeferTargetSeqTxn()
                );
                Assert.assertTrue(
                        "a deferred cycle that fed rows through the window cursor must leave the "
                                + "accumulator debt on the instance for the next turn to rebuild",
                        instance.isWindowStateDirty()
                );

                // Let the apply land and the view converge, then assert the view agrees with a
                // from-scratch evaluation of its own SELECT - rn gapless from 1.
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }

            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    """
                            (SELECT ts, i, count(*) OVER (
                                PARTITION BY 0
                                ORDER BY ts
                                ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW
                            ) AS rn FROM base) ORDER BY 1""",
                    "(lv) ORDER BY 1",
                    LOG,
                    true
            );
            assertNoRefreshFaults("lv");

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testCheckpointFreezeDuringLatchHeldRewriteDoesNotDeadlock() throws Exception {
        // Deterministic regression for the CHECKPOINT <-> refresh-worker deadlock.
        // advanceLiveViewConsumedSeqTxn (and the in-band applyLiveViewData reconcile
        // path) rewrite _lv.s while the refresh worker holds the refresh latch. If a
        // latch-held rewrite parked on waitForUnfrozen(), it would wait for an unfreeze
        // only endCheckpoint delivers - and endCheckpoint is never reached, because
        // startCheckpoint is still waiting for the latch the parked worker holds. A
        // permanent hang of CHECKPOINT plus the shared refresh worker. The
        // startCheckpoint latch handshake already serialises the rewrite against the
        // agent's file copy, so the latch-held path must not wait. This forces the
        // window: the worker takes the latch, the agent arms the freeze, then the
        // worker runs the rewrite with the freeze armed.
        //
        // Since startCheckpoint publishes the copy flag only under the latch, the
        // rewrite now runs with the intent armed but the copy flag clear, so a
        // re-introduced waitForUnfrozen() would no longer park here. The
        // structural guarantee has moved to the isFreezeArmed() gate in
        // refreshInstance, which precedes every latch-held rewrite.
        assertMemoryLeak(this::runCheckpointFreezeDuringLatchHeldRewrite);
    }

    @Test
    public void testCheckpointFreezeRequestDoesNotParkAnInvalidator() throws Exception {
        // The three-way stall. A refresh turn holds the refresh latch (in production it
        // is inside waitForApply, waiting for ApplyWal2TableJob). CHECKPOINT CREATE
        // requests a freeze and waits for that latch, holding the database-wide WAL
        // purge lock. Meanwhile the WAL apply worker - holding the base table's
        // TableWriter - reaches invalidateLiveViewsForBaseTable and parks in
        // waitForUnfrozen(). Apply then cannot advance, so the refresh turn cannot
        // finish, so the freeze cannot be taken: A waits on B waits on C waits on A.
        //
        // startCheckpoint therefore publishes only its INTENT before waiting for the
        // latch, and the copy flag waitForUnfrozen() parks on afterwards, under the
        // latch. An invalidator must sail straight through while the request is still
        // waiting. Before that split it parked here indefinitely.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("DROP LIVE VIEW IF EXISTS lv");
            execute("DROP TABLE IF EXISTS base");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            final TableToken baseToken = engine.verifyTableName("base");

            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            final CountDownLatch invalidatorReturned = new CountDownLatch(1);

            // Stand in for the refresh turn parked in waitForApply.
            Assert.assertTrue("test setup must take the refresh latch", instance.tryLockForRefresh());
            final Thread agent = new Thread(
                    () -> {
                        try {
                            instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                        } catch (Throwable th) {
                            errors.add(th);
                        } finally {
                            clearWorkerThreadLocals();
                        }
                    },
                    "lv-checkpoint-agent"
            );
            final Thread invalidator = new Thread(
                    () -> {
                        try {
                            engine.invalidateLiveViewsForBaseTable(baseToken, "test apply-worker invalidation");
                            invalidatorReturned.countDown();
                        } catch (Throwable th) {
                            errors.add(th);
                        } finally {
                            clearWorkerThreadLocals();
                        }
                    },
                    "lv-apply-worker"
            );
            try {
                agent.start();
                // Fence on the freeze INTENT, which both orderings publish before waiting
                // for the latch. That is strictly tighter than probing the agent's stack,
                // which can observe startCheckpoint before it has published anything. What
                // must not appear here is the copy flag - that is the whole property.
                TestUtils.assertEventually(() -> Assert.assertTrue(
                        "the checkpoint agent must have armed the freeze intent",
                        instance.isFreezeArmed()
                ), 30);
                Assert.assertFalse(
                        "the copy flag must not be published while the latch is held",
                        instance.isFreezeInProgress()
                );

                invalidator.start();
                Assert.assertTrue(
                        "the invalidator parked behind a freeze that is itself waiting for the refresh latch",
                        invalidatorReturned.await(30, TimeUnit.SECONDS)
                );
            } finally {
                instance.unlockAfterRefresh();
                agent.join(30_000);
                invalidator.join(30_000);
                if (instance.isFreezeArmed()) {
                    instance.endCheckpoint();
                }
            }

            Assert.assertFalse("checkpoint agent thread did not finish", agent.isAlive());
            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testCheckpointFreezeWaitIsCancellable() throws Exception {
        // CHECKPOINT CREATE holds the database-wide WAL purge lock while it waits for a
        // view's refresh latch, so an unbounded uninterruptible wait there blocks purge
        // for every table. The wait polls the statement's circuit breaker, which is what
        // lets CANCEL QUERY (and shutdown) abort it. On that abort nothing may be left
        // frozen: a stranded freeze intent would silently stop the view refreshing for
        // the process's life.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("DROP LIVE VIEW IF EXISTS lv");
            execute("DROP TABLE IF EXISTS base");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            final AtomicBooleanCircuitBreaker circuitBreaker = new AtomicBooleanCircuitBreaker(engine);
            circuitBreaker.cancel();

            // Hold the latch so the wait cannot complete, then let the breaker end it.
            Assert.assertTrue("test setup must take the refresh latch", instance.tryLockForRefresh());
            try {
                instance.startCheckpoint(circuitBreaker);
                Assert.fail("a tripped circuit breaker must abort the freeze wait");
            } catch (CairoException e) {
                Assert.assertTrue("expected a cancellation, got: " + e.getFlyweightMessage(), e.isCancellation());
            } finally {
                instance.unlockAfterRefresh();
            }

            Assert.assertFalse("an aborted freeze must not strand its intent", instance.isFreezeArmed());
            Assert.assertFalse("an aborted freeze must not publish the copy flag", instance.isFreezeInProgress());
            // The latch is free again, so a later checkpoint still works.
            Assert.assertTrue(instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            Assert.assertTrue(instance.isFreezeInProgress());
            instance.endCheckpoint();

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testConcurrentCheckpointDuringRefresh() throws Exception {
        // A checkpoint-agent thread cycles startCheckpoint/endCheckpoint on the
        // view (the DatabaseCheckpointAgent freeze handshake) while a refresh
        // driver maintains the view and writers ingest. The freeze gate must
        // serialise against the worker: each frozen turn is skipped and resumes
        // after endCheckpoint, so no _lv.s / on-disk tier advance is torn. After
        // every thread joins and the refresh quiesces single-threaded, the view
        // still equals the from-scratch recompute.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runCheckpointDuringRefresh(rnd, 4, 800));
    }

    @Test
    public void testConcurrentCreateDuringIngestion() throws Exception {
        // CREATE LIVE VIEW ... SEED races concurrent base ingestion.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runCreateDuringIngestion(rnd, 4, 700));
    }

    @Test
    public void testConcurrentDropDuringSeed() throws Exception {
        // DROP LIVE VIEW races a refresh driver that is still driving the SEED
        // sweep while writers ingest the suffix. This tears down the seed state
        // (sweep cursor, sealed seed boundaries, in-mem tier) mid-sweep, a path
        // the non-seed DROP-during-refresh test never reaches. Whatever the
        // interleaving, the drop must leave the registry empty and the base table
        // intact, with no leak or crash.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runDropDuringSeed(rnd, 4, 700));
    }

    @Test
    public void testConcurrentDropDuringRefresh() throws Exception {
        // DROP LIVE VIEW races a refresh-driver thread pumping the refresh job.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runDropDuringRefresh(rnd, 4, 700));
    }

    @Test
    public void testConcurrentMultiViewRefresh() throws Exception {
        // Two live views with different shapes over the same base, maintained by a
        // single refresh driver while four writers ingest concurrently. One base
        // commit fans out to both views (getViewsForBaseTable) and each carries its
        // own per-view refresh latch, so the driver advances both independently
        // under the cross-writer O3 stream. After quiescence both views must equal
        // their from-scratch recomputes.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runMultiViewConcurrent(rnd, 4, 800));
    }

    @Test
    public void testConcurrentRefreshCannotInvalidateFromStaleBaseHead() throws Exception {
        // The fallback worker used to read the base head first and the volatile view watermark
        // second, without the per-view refresh latch. Freeze it BETWEEN those two reads, publish a
        // new base commit, and let a notification worker consume that commit. The freeze point is
        // what gives this test its force: the commit lands after one operand is captured and before
        // the other, so the two read orders disagree. In the correct order the watermark is already
        // captured (old) and the base head is read after (new), which is coherent; in the racy order
        // the base head is captured (old) and the watermark is read after (new), and that mixed-time
        // pair durably invalidates a healthy view. Assert the view survives.
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS
                    SELECT ts, x, count(*) OVER (
                        PARTITION BY 0
                        ORDER BY ts
                        ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW
                    ) AS rn
                    FROM base
                    """);

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob seedJob = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(seedJob, "lv");
            }
            Assert.assertFalse("the seeded view must start healthy", instance.isInvalid());
            final long processedBefore = instance.getLastProcessedSeqTxn();
            final LiveViewStateStore stateStore = engine.getLiveViewStateStore();
            final LiveViewRefreshTask staleTask = new LiveViewRefreshTask();
            while (stateStore.tryDequeueRefreshTask(staleTask)) {
                stateStore.notifyBaseRefreshed(staleTask, staleTask.seqTxn);
            }

            // The idle fallback scan is sharded by table id (LiveViewRegistry.getShardedViews), so the
            // fallback worker reaches the ahead guard only for the views it owns. It owns this one today
            // - setUpCairo resets the table id generator per test, making base=1 and lv=2 - but pin the
            // assumption here. Without this, a fixture edit that shifts lv's id (one more CREATE TABLE
            // ahead of it) would surface as the 30s guardReadsSplit timeout below, which names the latch
            // rather than the shard it actually lost.
            final int fallbackWorkerId = 0;
            final int workerCount = 2;
            Assert.assertEquals(
                    "the fallback worker must own the lv shard, or its scan never reaches the ahead guard",
                    fallbackWorkerId,
                    Math.floorMod(instance.getLiveViewToken().getTableId(), workerCount)
            );

            final CountDownLatch guardReadsSplit = new CountDownLatch(1);
            final CountDownLatch releaseFallback = new CountDownLatch(1);
            final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            try (
                    LiveViewRefreshJob fallbackJob = new LiveViewRefreshJob(fallbackWorkerId, workerCount, engine, 1);
                    LiveViewRefreshJob notificationJob = new LiveViewRefreshJob(1, workerCount, engine, 1)
            ) {
                fallbackJob.setSimulateBaseCommitBetweenAheadGuardReadsForTest(() -> {
                    guardReadsSplit.countDown();
                    try {
                        if (!releaseFallback.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release fallback scan");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("fallback scan interrupted", e);
                    }
                });
                final Thread fallbackThread = new Thread(() -> {
                    try {
                        fallbackJob.processNotificationsForTest();
                    } catch (Throwable t) {
                        errors.add(t);
                        guardReadsSplit.countDown();
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-stale-base-head-scan");
                fallbackThread.start();
                try {
                    Assert.assertTrue(
                            "fallback scan did not stop between the two ahead-guard reads",
                            guardReadsSplit.await(30, TimeUnit.SECONDS)
                    );
                    if (!errors.isEmpty()) {
                        throw new RuntimeException("fallback scan thread failed", errors.peek());
                    }
                    execute("INSERT INTO base VALUES ('2026-06-01T00:00:00.000000Z', 1)");
                    Assert.assertTrue("the notification worker must consume the new commit",
                            notificationJob.processNotificationsForTest());
                    Assert.assertTrue(
                            "the notification worker must advance the view before the fallback resumes",
                            instance.getLastProcessedSeqTxn() > processedBefore
                    );
                } finally {
                    releaseFallback.countDown();
                    fallbackThread.join(30_000);
                }
                Assert.assertFalse("fallback scan thread did not finish", fallbackThread.isAlive());
            }
            if (!errors.isEmpty()) {
                throw new RuntimeException("fallback scan thread failed", errors.peek());
            }

            Assert.assertFalse(
                    "a fresh base commit processed concurrently with the fallback scan must not invalidate the live view",
                    instance.isInvalid()
            );
            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testConcurrentRefreshDuringIngestion() throws Exception {
        // A refresh-driver thread maintains the view while four writers ingest.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 0, 4, 800, false, true));
    }

    @Test
    public void testCreateRollbackDefersFreeWhileRefreshLatchHeld() throws Exception {
        // createLiveView publishes the instance into the registry the refresh worker
        // iterates (registerView -> getViews) BEFORE the CREATE commits. If a later
        // CREATE step throws, the rollback must not free the instance off-latch. A
        // worker that has latched the fresh instance and is installing its runtime
        // state would then either race close() into a UAF, or (the case reproduced
        // here) leak that state: the off-latch close() sets isClosed, so the worker's
        // finally-hook tryCloseIfDropped short-circuits and never frees. The fix routes
        // the rollback through the DROP path's latch-aware teardown, so the free defers
        // to that finally hook.
        //
        // This forces exactly that window: registerBaseTable (the step right after
        // registerView) throws while a worker holds the refresh latch on the fresh
        // instance. Pre-fix the tier the worker installs after the teardown leaks
        // (assertMemoryLeak fails); post-fix the worker's tryCloseIfDropped frees it.
        final CountDownLatch instanceRegistered = new CountDownLatch(1);
        final CountDownLatch workerLatched = new CountDownLatch(1);
        final CountDownLatch rollbackDone = new CountDownLatch(1);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            final Thread worker = new Thread(() -> {
                try {
                    instanceRegistered.await();
                    final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                    Assert.assertNotNull("worker must see the registered instance", instance);
                    // Mirror refreshInstance: take the latch, clear the fresh-instance
                    // guards (all false), then hold it across the teardown.
                    Assert.assertTrue(instance.tryLockForRefresh());
                    Assert.assertFalse(instance.isDropped());
                    workerLatched.countDown();
                    // The rollback teardown runs now, while we hold the latch. Install the
                    // runtime state only after it completes, reproducing the "worker sets
                    // inMemoryTier after the teardown" window.
                    rollbackDone.await();
                    final IntList types = new IntList(1);
                    types.add(ColumnType.LONG);
                    instance.setInMemoryTier(new LiveViewInMemoryTier(types, 0, 4096L));
                    // refreshInstance's finally hook: release the latch, then free if the
                    // view was dropped mid-cycle. The fix makes this free the tier; the
                    // off-latch close() would already have set isClosed, leaking it.
                    instance.unlockAfterRefresh();
                    instance.tryCloseIfDropped();
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-worker");

            // Arm the class's state-store wrapper (installed via the engine's own
            // createLiveViewStateStore hook in setUpStatic). registerBaseTable, the step right after
            // registerView, releases the worker to latch the instance, waits for it to hold the
            // latch, then throws - so the rollback teardown runs against a latched instance.
            registerBaseTableFault = () -> {
                instanceRegistered.countDown();
                try {
                    workerLatched.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                throw CairoException.critical(0).put("injected registerBaseTable failure");
            };

            worker.start();
            boolean threw = false;
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                        "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");
            } catch (Throwable expected) {
                threw = true; // the injected registerBaseTable failure rolls the CREATE back
            } finally {
                rollbackDone.countDown();
                registerBaseTableFault = null;
            }

            worker.join();
            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }
            Assert.assertTrue("CREATE LIVE VIEW must fail when registerBaseTable throws", threw);
            // The rollback removed the half-built view; the name is free to reuse.
            Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));

            execute("DROP TABLE base");
        });
    }

    @Test
    public void testRetryPendingApplyFreesRuntimeStateOfInvalidatedView() throws Exception {
        // Invalidation frees a view's runtime state (factory, maps, tier, tracker) through a
        // refresh-latch CAS. If that CAS loses because retryPendingLiveViewApply holds the latch,
        // the invalidator relies on the latch holder's finally to free instead - exactly as the
        // main refresh finally does. Without that mirror the invalid view's runtime state strands
        // until DROP or shutdown. Here the view is invalid while its installed tier is still
        // present (the leftover the losing CAS could not reclaim); retryPendingLiveViewApply's
        // finally must free it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("the created view must be registered", instance);

            // Install a native-memory-backed tier as the runtime state that must be reclaimed.
            Assert.assertNull("a never-refreshed view has no tier yet", instance.getInMemoryTier());
            final IntList types = new IntList(1);
            types.add(ColumnType.LONG);
            instance.setInMemoryTier(new LiveViewInMemoryTier(types, 0, 4096L));

            // Invalidate the view but leave its runtime state in place - the leftover a concurrent
            // invalidator's own tryFreeRuntimeStateIfInvalid could not free because the refresh
            // latch was held by this helper.
            instance.markInvalid("injected", 0);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                job.retryPendingLiveViewApplyForTest(instance);
            }
            Assert.assertNull(
                    "retryPendingLiveViewApply's finally must free the invalid view's runtime tier",
                    instance.getInMemoryTier()
            );

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testDemoteRefusedMintDoesNotInvalidateView() throws Exception {
        // Deterministic regression for the live-view commit demote fence (17a1f40e08).
        // Every LV commit family routes through fencedLiveViewCommit, which fires the
        // role-switch mint observer inside the role-switch read-lock hold at the exact
        // WAL externalization point. A witness that throws the read-only authorization
        // error there models a PRIMARY-to-REPLICA demote winning the race - the fence's
        // in-lock isReadOnlyMode() re-check (or getWalWriter's eager check) refusing the
        // mint. handleRefreshFailure must classify that authorization refusal as
        // retry-later and NEVER invalidate: a live view is derived state the new primary
        // recomputes forward, and invalidation is durable/sticky with no replica-side
        // recovery, so a demote refusal must not brick the view locally. Once the demote
        // clears, the same view must resume and converge to the from-scratch recompute.
        assertMemoryLeak(this::runDemoteRefusedMintDoesNotInvalidate);
    }

    @Test
    public void testCheckpointRefusesDroppedLiveView() throws Exception {
        // C2, lookup-before-drop half of the checkpoint/drop handshake: if a concurrent
        // DROP LIVE VIEW has already marked the instance dropped, a checkpoint freeze
        // started afterwards must be REFUSED, so DatabaseCheckpointAgent skips the view
        // rather than freeze (and copy) a directory whose file teardown is imminent. A
        // refused freeze must leave freezeInProgress clear.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("DROP LIVE VIEW IF EXISTS lv");
            execute("DROP TABLE IF EXISTS base");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // A DROP marked the instance dropped first; the agent looks it up and only now
            // calls startCheckpoint. It must observe the drop and refuse the freeze.
            instance.markAsDropped();
            instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            Assert.assertFalse(
                    "startCheckpoint on a dropped instance must not set freezeInProgress",
                    instance.isFreezeInProgress()
            );
            Assert.assertFalse(
                    "a refused freeze must not leave its intent behind either, or refresh stops",
                    instance.isFreezeArmed()
            );

            // Finish the drop cleanly (no freeze to wait out) and drop the base.
            engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
            Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testDropLiveViewWaitsForCheckpointFreeze() throws Exception {
        // C2, freeze-before-drop half of the checkpoint/drop handshake: with a
        // DatabaseCheckpointAgent freeze in progress (startCheckpoint published), a
        // concurrent DROP LIVE VIEW must PARK in markDroppedAndAwaitCheckpoint ->
        // waitForUnfrozen and only tear the view's files down after endCheckpoint clears
        // the freeze. A broken handshake lets the drop delete _lv / _lv.s / _meta while
        // the agent is mid-copy, aborting the whole checkpoint or corrupting the snapshot.
        final AtomicBoolean dropReturned = new AtomicBoolean(false);
        final CountDownLatch dropStarted = new CountDownLatch(1);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("DROP LIVE VIEW IF EXISTS lv");
            execute("DROP TABLE IF EXISTS base");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // Publish a checkpoint freeze on the main thread exactly as the agent does:
            // startCheckpoint fences the refresh latch and publishes freezeInProgress
            // under that hold.
            Assert.assertTrue(instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            Assert.assertTrue(instance.isFreezeInProgress());

            final Thread dropper = new Thread(() -> {
                try {
                    dropStarted.countDown();
                    engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
                    dropReturned.set(true);
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-dropper");

            try {
                dropper.start();
                dropStarted.await();
                // The dropper must park in the checkpoint handshake, not race the teardown.
                // markDroppedAndAwaitCheckpoint publishes dropped=true, then parks in waitForUnfrozen
                // under the freeze; awaiting that published drop state fences deterministically without
                // probing the dropper's stack for a private frame.
                awaitDropped(instance, 60_000);
                Assert.assertFalse(
                        "dropLiveView must block while a checkpoint freeze is in progress",
                        dropReturned.get()
                );
                Assert.assertTrue("dropper thread must still be parked in the handshake", dropper.isAlive());
                // Nothing torn down while frozen: the view is still fully present.
                Assert.assertTrue(engine.getLiveViewRegistry().hasView("lv"));
                Assert.assertNotNull(engine.getTableTokenIfExists("lv"));
            } finally {
                // Always clear the freeze so the parked dropper can unwind, even on failure.
                instance.endCheckpoint();
            }

            dropper.join(60_000);
            if (!errors.isEmpty()) {
                throw new RuntimeException("thread failed", errors.peek());
            }
            Assert.assertTrue("dropLiveView must complete once the freeze clears", dropReturned.get());
            Assert.assertFalse("dropper thread must have finished", dropper.isAlive());

            // Clean teardown after the freeze released: the view is gone, the base survived.
            Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
            Assert.assertNull("LV name must no longer resolve after the drop", engine.getTableTokenIfExists("lv"));
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testDropLiveViewFencesRefreshWorker() throws Exception {
        // Fence proof: dropLiveView's fenceRefresh() must block until an in-flight
        // refresh turn releases the latch. A worker holds the refresh latch; the
        // dropper's dropLiveView must park in the fence, then complete cleanly once
        // the latch releases (registry empty, base intact, no leak).
        final CountDownLatch latchHeld = new CountDownLatch(1);
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        final CountDownLatch dropStarted = new CountDownLatch(1);
        final AtomicBoolean dropReturned = new AtomicBoolean(false);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("DROP LIVE VIEW IF EXISTS lv");
            execute("DROP TABLE IF EXISTS base");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // Worker: takes the refresh latch and holds it across the whole drop
            // attempt (mirrors refreshInstance's tryLockForRefresh .. unlockAfterRefresh
            // turn), releasing only once the main thread has proven the drop is blocked.
            final Thread worker = new Thread(() -> {
                try {
                    Assert.assertTrue(instance.tryLockForRefresh());
                    latchHeld.countDown();
                    releaseLatch.await();
                    instance.unlockAfterRefresh();
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-worker");

            // Dropper: runs the real engine.dropLiveView, which must park in
            // fenceRefresh until the worker releases the latch.
            final Thread dropper = new Thread(() -> {
                try {
                    dropStarted.countDown();
                    engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
                    dropReturned.set(true);
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-dropper");

            worker.start();
            latchHeld.await();
            dropper.start();
            // Wait until the dropper is actually spinning in the fence, then prove it is blocked:
            // while the worker holds the latch, fenceRefresh cannot complete, so dropLiveView cannot
            // return. A broken fence would let the drop tear the table down and return.
            //
            // dropStarted counts down *before* dropLiveView is called, so it says nothing about the
            // fence, and the fixed sleep that used to stand in for one meant the two assertions below
            // would hold trivially on a machine where the dropper had not got going yet - a test that
            // passes without the fence ever being exercised. fenceRefresh busy-spins on Os.pause
            // instead of parking, so the dropper stays RUNNABLE and Thread.State is no fence either.
            // Instead await the drop's own published state: markDroppedAndAwaitCheckpoint sets
            // dropped=true before dropLiveView commits to the fenceRefresh spin, so once the view reads
            // dropped the dropper is at the fence and, with the latch held, cannot return.
            dropStarted.await();
            awaitDropped(instance, 60_000);
            Assert.assertFalse("dropLiveView must block in fenceRefresh while the refresh latch is held",
                    dropReturned.get());
            Assert.assertTrue("dropper thread must still be running (spinning in the fence)", dropper.isAlive());

            // Release the latch: the fence completes and the drop finishes.
            releaseLatch.countDown();
            worker.join();
            dropper.join(60_000);

            if (!errors.isEmpty()) {
                throw new RuntimeException("thread failed", errors.peek());
            }
            Assert.assertTrue("dropLiveView must complete once the latch is released", dropReturned.get());
            Assert.assertFalse("dropper thread must have finished", dropper.isAlive());

            // Clean teardown: the view is gone from the registry, the base survived.
            Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
            Assert.assertNull("LV name must no longer resolve after the drop", engine.getTableTokenIfExists("lv"));
            drainWalQueue();
            assertQuery("SELECT count(*) FROM base").noRandomAccess().expectSize().returns("count\n0\n");

            execute("DROP TABLE base");
        });
    }

    @Test
    public void testDropKeepsViewRegistryVisibleUntilFenced() throws Exception {
        // Regression for the DROP-vs-checkpoint freeze race. The checkpoint agent decides whether to
        // freeze a live view with a live getViewInstance(name) lookup and only calls startCheckpoint()
        // (which fences the refresh worker) when it finds one. dropLiveView used to remove the name
        // mapping BEFORE marking the view dropped and fencing it, so in the window between the two a
        // concurrent checkpoint could look the view up, get null, skip the freeze, and copy _lv.s +
        // the table data while a refresh turn was still mutating them.
        //
        // The fix marks+fences BEFORE unregistering. This test pins that ordering deterministically:
        // a worker holds the refresh latch (an in-flight refresh turn), so dropLiveView parks in
        // fenceRefresh; while it is parked, the checkpoint's freeze-lookup must still return the
        // instance (and see it already marked dropped). Pre-fix the lookup returned null here.
        final CountDownLatch latchHeld = new CountDownLatch(1);
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        final CountDownLatch dropStarted = new CountDownLatch(1);
        final AtomicBoolean dropReturned = new AtomicBoolean(false);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
            execute("DROP LIVE VIEW IF EXISTS lv");
            execute("DROP TABLE IF EXISTS base");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " +
                    "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // Worker: holds the refresh latch across the whole drop attempt, standing in for an
            // in-flight refresh turn that a checkpoint freeze would have to fence.
            final Thread worker = new Thread(() -> {
                try {
                    Assert.assertTrue(instance.tryLockForRefresh());
                    latchHeld.countDown();
                    releaseLatch.await();
                    instance.unlockAfterRefresh();
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-worker");

            // Dropper: runs the real engine.dropLiveView, which must park in fenceRefresh until the
            // worker releases the latch.
            final Thread dropper = new Thread(() -> {
                try {
                    dropStarted.countDown();
                    engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
                    dropReturned.set(true);
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-dropper");

            worker.start();
            latchHeld.await();
            dropper.start();
            dropStarted.await();
            // markDroppedAndAwaitCheckpoint publishes dropped=true before dropLiveView commits to the
            // fenceRefresh spin, so awaiting that published state parks the test at the fence
            // deterministically - no stack-frame probe, and with the latch held the drop cannot return.
            awaitDropped(instance, 60_000);

            // The observation: while the drop is parked in the fence (refresh still in flight), what
            // does the checkpoint agent's freeze-lookup see? Capture it all now, before releasing the
            // latch, so the assertions below are made against the exact window.
            final boolean isDropReturnedWhileFenced = dropReturned.get();
            final LiveViewInstance checkpointView = engine.getLiveViewRegistry().getViewInstance("lv");
            final boolean isDroppedDuringFence = checkpointView != null && checkpointView.isDropped();

            // Release the latch FIRST (no assertion between the await and here), so the fence
            // completes and both threads join cleanly regardless of what was observed. Only then
            // assert - a red run reports the assertion instead of stranding the parked threads.
            releaseLatch.countDown();
            worker.join();
            dropper.join(60_000);
            if (!errors.isEmpty()) {
                throw new RuntimeException("thread failed", errors.peek());
            }

            Assert.assertFalse("dropLiveView must block in fenceRefresh while the refresh latch is held",
                    isDropReturnedWhileFenced);
            Assert.assertNotNull(
                    "DROP must keep the view registry-visible until it is fenced, so a concurrent"
                            + " checkpoint freeze-lookup cannot miss it while a refresh is in flight",
                    checkpointView);
            Assert.assertTrue("the view must already be marked dropped before the fence completes",
                    isDroppedDuringFence);
            Assert.assertTrue("dropLiveView must complete once the latch is released", dropReturned.get());

            // Clean teardown: the view is gone from the registry, the base survived.
            Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
            Assert.assertNull("LV name must no longer resolve after the drop", engine.getTableTokenIfExists("lv"));
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testMultiRefreshWorkerConvergence() throws Exception {
        // Production runs one LiveViewRefreshJob per refresh-pool worker (2-4 by
        // default, ServerMain.setupLiveViewJobs) with no per-view sharding: every
        // worker scans the whole registry and contends the shared task queue and the
        // per-view refresh latch (tryLockForRefresh; the loser bails, no wait). This
        // soak runs several refresh workers on their own threads against sustained
        // multi-writer O3 ingestion, with reader threads asserting the prefix
        // invariant mid-flight, then asserts the quiesced view equals the
        // from-scratch recompute. Nothing else exercises refresh-vs-refresh: every
        // other test drives a single LiveViewRefreshJob(0, engine, 1).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runMultiRefreshWorkerSoak(rnd, 4, 3, 3, 800));
    }

    @Test
    public void testMultiWalWriterInterleaving() throws Exception {
        // Four concurrent writers, then a single-threaded refresh to quiescence.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 0, 4, 600, false, false));
    }

    @Test
    public void testMultiWalWriterInterleavingInMemory() throws Exception {
        // Same, with the in-memory tier enabled (fixed-width output, tier-eligible).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 0, 4, 600, true, false));
    }

    @Test
    public void testMultiWalWriterInterleavingRowNumber() throws Exception {
        // Ranking re-sequencing under interleaved multi-writer O3 (the Finding 1/2b
        // surface, now from genuinely concurrent commits rather than shuffled inserts).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runConcurrent(rnd, 1, 6, 600, false, false));
    }

    @Test
    public void testParallelFilterRacesTierSwap() throws Exception {
        // The page-frame read path under its own risk: a filtered read routes through the
        // tier's synthetic frame, which publishes the pinned slot's raw native addresses,
        // and the parallel filter hands that frame to workers on OTHER threads. The
        // argument that this is safe is that the frame cursor holds the pin for its whole
        // life and no worker outlives the cursor that produced it - so a swap can only
        // ever take the slot the readers are NOT on. This drives it: real filter workers
        // read tier frames while the refresh worker swaps slots on every publish and
        // writers ingest cross-writer O3. A worker reading a slot the writer reclaimed is
        // a use-after-free, so it surfaces as a torn value or a crash rather than a
        // wrong-but-plausible answer.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runParallelFilterTierSwapSoak(rnd, 4, 4, 2000));
    }

    @Test
    public void testReaderChurnSoak() throws Exception {
        // Reader threads churn cursors over an IN MEMORY view while a refresh driver
        // appends via the fast-path CAS and writers ingest - the read/publish risk.
        // The sum() view carries a SYMBOL passthrough, so the reads route disk-only;
        // this soak stresses the read/publish hand-off without Mode B in the loop.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runReaderChurnSoak(rnd, 4, 4, 800, false, false));
    }

    @Test
    public void testReaderChurnSoakInMem() throws Exception {
        // Same soak with a SYMBOL-free row_number() view so the reads genuinely
        // route through Mode B (seam routing over the pinned in-mem slot). The
        // readers assert a mid-flight invariant on every snapshot - ts strictly
        // ascending and rn a gapless 1..N sequence - so a torn slot, a seam
        // duplicate/gap, or a stale-restamped pre-O3 row surfaces as a value
        // mismatch, not merely a crash. The cross-writer O3 drives the in-mem
        // rebuild against the live Mode B readers (the both-slots-pinned race).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runReaderChurnSoak(rnd, 4, 4, 800, true, false));
    }

    @Test
    public void testReaderChurnSoakModeALead() throws Exception {
        // Mode A variant of the row_number() soak: the refresh driver advances the
        // clock only a fraction of FLUSH EVERY per tick, so most refreshes publish
        // an un-flushed lead into the in-mem tier (the tier leads disk) and flushes
        // land underneath the readers only every few ticks. The readers churn
        // cursors over the live lead and assert the same per-snapshot invariant - ts
        // strictly ascending, rn a gapless 1..N sequence - which must hold whether a
        // snapshot is served from the lead, the overlap, or disk-only after a fence
        // miss. So a torn lead publish, a seam duplicate/gap at the overlap/lead
        // boundary, or a stale-restamped slot surfaces as a value mismatch. After
        // the run quiesces the test rebuilds a known lead and asserts Mode A is
        // engaged (the cursor serves the lead and equals the recompute).
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runReaderChurnSoak(rnd, 4, 4, 800, true, true));
    }

    @Test
    public void testReaderChurnSoakVarSize() throws Exception {
        // Reader-churn soak over an IN MEMORY view whose output carries var-length
        // passthrough columns (STRING + VARCHAR) alongside row_number(), so the
        // reads route through the in-mem tier (Mode B) and dereference the
        // var-length (data, aux) regions per row while a refresh driver appends via
        // the fast-path CAS and writers ingest - the lock-free read/publish
        // hand-off under test, now with var-length buffers in play.
        // Each read asserts a per-snapshot invariant: rows ts-ascending, rn a
        // gapless 1..N sequence, and the STRING / VARCHAR passthroughs decoding
        // back to their ts-derived values (vs == decimal ts, vv == 'v' + ts). So a
        // torn var-length read - a stale base pointer after a region realloc, a
        // seam duplicate/gap, a use-after-free - surfaces as a value mismatch or a
        // crash, not silent corruption. No ARM-specific canary is needed here:
        // unlike the symbol cache, which is shared across BOTH tier slots and grows
        // concurrently with readers (hence its bounded-scan horizon and
        // volatile-backed list), var-length values live in the PER-SLOT buffers and
        // stay frozen while any reader pins the slot (the writer needs the slot's
        // exclusive sentinel to mutate it). Reader pins are shared/refcounted, not
        // exclusive, so several cursors can pin one slot at once; the read flyweights
        // are therefore NOT shared - each reader cursor owns its own per-column
        // var-size views (LiveViewRecordCursor's MergedRecord, so recordA vs recordB
        // do not alias either), pointing into the frozen buffer memory. That is what
        // makes concurrent var-length reads safe; see LiveViewInMemReadTest's
        // testInMemVarSizeRecordsAreIndependent for the deterministic proof.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> runVarSizeReaderChurnSoak(rnd, 4, 4, 800));
    }

    /**
     * False: {@link #setUpStatic} pins this class's {@code MillisecondClock} to
     * {@code MillisecondClockImpl.INSTANCE}, so the storage engine's spin deadlines measure real
     * time here no matter how far the soaks fast-forward the microsecond clock. That keeps
     * {@link AbstractLiveViewTest#setUp} from raising {@code spinLockTimeout} past a simulated year,
     * which would cost this class - the only live view suite running readers against a real clock -
     * its 5s reader-side "Transaction read timeout" and leave only the 20-minute class timeout.
     */
    @Override
    protected boolean isMillisecondClockSimulated() {
        return false;
    }

    private static void appendRow(WalWriter walWriter, long ts, int symIdx, long iv, double xv) {
        TableWriter.Row row = walWriter.newRow(ts);
        if (symIdx < 0) {
            row.putSym(1, (CharSequence) null);
        } else {
            row.putSym(1, SYMBOLS[symIdx]);
        }
        row.putLong(2, iv); // LONG_NULL stores as NULL
        row.putDouble(3, xv);
        row.append();
    }

    // Appends one row to the var-size base table (ts, vs STRING, vv VARCHAR), deriving
    // both var-length values from the row's (unique) timestamp - vs is the decimal ts,
    // vv is 'v' + the decimal ts - so a reader can decode them back and detect a torn
    // var-length read. The caller owns the two sinks so a tight writer loop reuses them.
    private static void appendVarSizeRow(WalWriter walWriter, long ts, StringSink strSink, Utf8StringSink vcSink) {
        final TableWriter.Row row = walWriter.newRow(ts);
        strSink.clear();
        strSink.put(ts);
        row.putStr(1, strSink); // vs STRING = decimal ts
        vcSink.clear();
        vcSink.put('v').put(ts);
        row.putVarchar(2, vcSink); // vv VARCHAR = 'v' + decimal ts
        row.append();
    }

    // Generates the logical dataset: strictly-unique, strictly-increasing timestamps
    // (so OVER (ORDER BY ts) and the natural ts scan order used by OVER () are total
    // orders both the incremental and the batch path agree on), random symbols and
    // values with occasional NULLs. The data starts a year above the test clock so
    // every row sits above a non-seed view's CREATE-moment lower bound.
    private static void generateDataset(Rnd rnd, int rowCount, long[] tsv, int[] symIdx, long[] iv, double[] xv) {
        long ts = MicrosTimestampDriver.floor(DATA_START);
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(5_000_000); // 1us .. 5s, keeps ts strictly increasing
            if (rnd.nextInt(20) == 0) {
                ts += 86_400_000_000L; // occasional full-day jump to span more partitions
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(SYMBOLS.length); // -1 => NULL symbol
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xv[k] = rnd.nextDouble() * 1000.0;
        }
    }

    // The two grammar-legal, deterministic window shapes the fuzz test also uses:
    // a partitioned bounded-frame aggregate and ranking OVER (). Both carry the
    // incremental-snapshot contract and are total deterministic functions of a
    // unique-ts row set, so the recompute oracle holds under any ingestion order.
    private static String projection(int variant, int n) {
        final String frame = "PARTITION BY sym ORDER BY ts ROWS BETWEEN " + n + " PRECEDING AND CURRENT ROW";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + frame + ") AS v";
            case 1 ->
                    "ts, sym, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // Like newPacedWriterThread, but for the var-size base table (ts, vs STRING,
    // vv VARCHAR): writer w ingests the round-robin slice fromIndex+w, fromIndex+w+numWriters,
    // ... one batch per refresh tick. Each row's var-length values are derived from its
    // (unique) timestamp - vs is the decimal ts, vv is 'v' + the decimal ts - so a reader
    // can decode them back and detect a torn var-length read. The cross-writer commit
    // interleaving is what produces O3, which drives the in-mem tier rebuild against the
    // live readers; the pacing is what keeps the writers from finishing inside the driver's
    // first tick and leaving the readers nothing to race.
    private Thread newPacedVarSizeWriterThread(
            int writerId,
            int numWriters,
            int fromIndex,
            int rowCount,
            int batch,
            long[] tsv,
            TableToken baseToken,
            CyclicBarrier barrier,
            AtomicLong refreshTicks,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        return new Thread(() -> {
            final StringSink strSink = new StringSink();
            final Utf8StringSink vcSink = new Utf8StringSink();
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                barrier.await();
                int sinceCommit = 0;
                for (int k = fromIndex + writerId; k < rowCount; k += numWriters) {
                    appendVarSizeRow(walWriter, tsv[k], strSink, vcSink);
                    if (++sinceCommit >= batch) {
                        walWriter.commit();
                        sinceCommit = 0;
                        awaitRefreshTick(refreshTicks);
                    }
                }
                walWriter.commit();
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                clearWorkerThreadLocals();
            }
        }, "lv-paced-varsize-writer-" + writerId);
    }

    // Like newWriterThread, but the writer waits for the refresh driver to complete a tick
    // after each commit, so its slice trickles in one batch per refresh cycle instead of
    // all inside the driver's first one. That is what gives the readers many publishes to
    // race rather than one; see runParallelFilterTierSwapSoak. The wait is bounded so a
    // driver that dies (or never starts) fails the run through the errors queue rather
    // than hanging it.
    private Thread newPacedWriterThread(
            int writerId,
            int numWriters,
            int fromIndex,
            int rowCount,
            int batch,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            TableToken baseToken,
            CyclicBarrier barrier,
            AtomicLong refreshTicks,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        return new Thread(() -> {
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                barrier.await();
                int sinceCommit = 0;
                for (int k = fromIndex + writerId; k < rowCount; k += numWriters) {
                    appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                    if (++sinceCommit >= batch) {
                        walWriter.commit();
                        sinceCommit = 0;
                        awaitRefreshTick(refreshTicks);
                    }
                }
                walWriter.commit();
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                clearWorkerThreadLocals();
            }
        }, "lv-paced-writer-" + writerId);
    }

    // Waits for the refresh driver to complete one more tick than it had when called.
    // Bounded: a driver that has stopped ticking must fail the run loudly rather than
    // silently drop the paced writer's intended refresh/writer overlap. The caller runs on
    // a writer thread whose try/catch routes any throw into the shared errors queue, so a
    // timeout surfaces as a test failure instead of a vacuous pass (or a hang).
    private static void awaitRefreshTick(AtomicLong refreshTicks) {
        final long seen = refreshTicks.get();
        final long deadlineNanos = System.nanoTime() + REFRESH_TICK_WAIT_NANOS;
        while (refreshTicks.get() == seen) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new AssertionError("refresh driver did not tick within "
                        + (REFRESH_TICK_WAIT_NANOS / 1_000_000_000L)
                        + "s; the paced writer lost its intended refresh/writer overlap");
            }
            Os.pause();
        }
    }

    // Builds a writer thread that owns its own WalWriter and ingests a round-robin
    // slice of [fromIndex, rowCount): writer w gets fromIndex+w, fromIndex+w+numWriters,
    // ... The slices are disjoint and globally ts-ordered, so timestamps stay unique;
    // the cross-writer commit interleaving is what produces O3. The thread awaits the
    // barrier before its first write and clears thread-locals on exit for the leak check.
    private Thread newWriterThread(
            int writerId,
            int numWriters,
            int fromIndex,
            int rowCount,
            int batch,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            TableToken baseToken,
            CyclicBarrier barrier,
            ConcurrentLinkedQueue<Throwable> errors
    ) {
        return new Thread(() -> {
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                barrier.await();
                int sinceCommit = 0;
                for (int k = fromIndex + writerId; k < rowCount; k += numWriters) {
                    appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                    if (++sinceCommit >= batch) {
                        walWriter.commit();
                        sinceCommit = 0;
                    }
                }
                walWriter.commit();
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                clearWorkerThreadLocals();
            }
        }, "lv-writer-" + writerId);
    }

    // Opens a fresh cursor over the live view and fully drains it, touching the
    // fixed-width columns so the read path actually runs over the row buffers. Called
    // in a tight loop by the reader threads while the view refreshes concurrently; it
    // asserts nothing about the row set (the view is mid-flight) - a torn read or a
    // corrupt tier slot surfaces as an exception or a JVM crash, not a value mismatch.
    // The final single-threaded oracle validates contents after quiescence.
    private void readViewOnce() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory();
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                record.getLong(0); // ts
                record.getLong(2); // i
                record.getLong(3); // v (sum aggregate)
            }
        }
    }

    // Opens a fresh cursor over the SYMBOL-free row_number() view (columns ts, i, rn) and
    // drains it, asserting the per-snapshot invariant (see
    // assertRowNumberSnapshotInvariant). Unfiltered, so it reads through the record-cursor
    // path's seam routing (Mode B).
    // <p>
    // Returns whether this read routed through the in-mem tier rather than falling back to
    // disk-only, which the soak counts as its evidence that the readers actually reached the
    // tier while it ran, not just once after quiescence. Reads the LV factory's own cursor
    // (unwrapping any QueryProgress wrapper) so isRoutingEligible answers for the read the
    // soak just drained.
    private boolean readRowNumberViewOnce() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory()
        ) {
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(ctx)) {
                assertRowNumberSnapshotInvariant(cursor);
                return cursor.isRoutingEligible();
            }
        }
    }

    // The same snapshot read as readRowNumberViewOnce, but FILTERED, so it routes through
    // the page-frame path: the parallel filter runs over the tier's synthetic frame the
    // way it runs over a native partition. The context declares the filter pool's worker
    // count, which is what enables the parallel filter at all (see
    // SqlExecutionContextImpl's parallelFilterEnabled) and puts the reduce work on those
    // threads rather than on this one.
    // <p>
    // The predicate keeps every row, deliberately: it makes the read take the frame path
    // without weakening the invariant to a subset the assertion could not check. What a
    // filter DISCARDS over a tier frame is pinned deterministically elsewhere
    // (LiveViewInMemReadTest#testPageFrameFilteredReadServesLeadFromRam); what this needs
    // is for every slot row to reach a worker.
    // <p>
    // Returns whether this read's shape found the tier's frame rather than the base scan's,
    // which the soak counts. The probe is a second cursor over the same factory rather than
    // the filter's own - that one is private to the async factory - so it answers for the
    // instant just before the read, over the identical predicate. That is enough for what
    // the count is for: proving the soak was routing at all while it ran, rather than
    // spending itself against a tier the fence had shut it out of.
    private boolean readFilteredRowNumberViewOnce() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine, FILTER_WORKER_COUNT);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(FILTERED_VIEW_SQL, ctx).getRecordCursorFactory()
        ) {
            final boolean routed;
            try (PageFrameCursor probe = unwrapLvFactory(factory).getPageFrameCursor(ctx, ORDER_ASC)) {
                routed = probe instanceof LiveViewPageFrameCursor;
            }
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                assertRowNumberSnapshotInvariant(cursor);
            }
            return routed;
        }
    }

    // The per-snapshot invariant of the SYMBOL-free row_number() view (columns ts, i, rn):
    // rows come back ts-ascending (the designated-timestamp total order) and rn is a
    // gapless 1..N sequence in that order. row_number() OVER () numbers rows in
    // ts-ascending scan order and the O3 replay re-sequences the whole table, so any
    // committed snapshot - served disk-only, through Mode B, or through the tier's page
    // frame - must satisfy it. A torn read, a seam duplicate/gap, or a stale pre-O3 row
    // re-stamped into the slot breaks it. The view is mid-flight, so the row count itself
    // is not asserted; the final single-threaded oracle validates the full contents after
    // quiescence.
    private static void assertRowNumberSnapshotInvariant(RecordCursor cursor) {
        final Record record = cursor.getRecord();
        long prevTs = Long.MIN_VALUE;
        long expectedRn = 1;
        while (cursor.hasNext()) {
            long ts = record.getLong(0);
            long rn = record.getLong(2);
            if (ts <= prevTs) {
                throw new AssertionError("ts not strictly ascending: prevTs=" + prevTs + ", ts=" + ts);
            }
            if (rn != expectedRn) {
                throw new AssertionError("rn not a gapless 1..N sequence: expected=" + expectedRn
                        + ", actual=" + rn + ", ts=" + ts);
            }
            prevTs = ts;
            expectedRn++;
        }
    }

    // Opens a fresh cursor over the var-size row_number() view (columns ts, vs
    // STRING, vv VARCHAR, rn) and drains it, asserting the per-snapshot invariant
    // that holds for every consistent LV-table version: rows come back
    // ts-ascending, rn is a gapless 1..N sequence, and the two var-length
    // passthroughs decode back to their ts-derived values (vs == the decimal ts,
    // vv == 'v' + the decimal ts). Reading vs/vv dereferences the tier's
    // var-length (data, aux) regions, so a torn read - a stale base pointer after a
    // realloc, a seam dup/gap, a use-after-free - surfaces as a value mismatch or a
    // crash. The view is mid-flight, so the row count itself is not asserted here;
    // the final single-threaded oracle validates the full contents after quiescence.
    // <p>
    // Returns whether this read routed through the in-mem tier, which the soak counts as
    // its evidence that the var-length reads actually dereferenced the tier's (data, aux)
    // regions while it ran. Reads the LV factory's own cursor (unwrapping any QueryProgress
    // wrapper) so isRoutingEligible answers for the read the soak just drained.
    private boolean readVarSizeViewOnce() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory()
        ) {
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(ctx)) {
                final Record record = cursor.getRecord();
                long prevTs = Long.MIN_VALUE;
                long expectedRn = 1;
                while (cursor.hasNext()) {
                    final long ts = record.getLong(0);
                    final CharSequence vs = record.getStrA(1);
                    final Utf8Sequence vv = record.getVarcharA(2);
                    final long rn = record.getLong(3);
                    if (ts <= prevTs) {
                        throw new AssertionError("ts not strictly ascending: prevTs=" + prevTs + ", ts=" + ts);
                    }
                    if (rn != expectedRn) {
                        throw new AssertionError("rn not a gapless 1..N sequence: expected=" + expectedRn
                                + ", actual=" + rn + ", ts=" + ts);
                    }
                    long decoded;
                    try {
                        decoded = vs == null ? Long.MIN_VALUE : Numbers.parseLong(vs);
                    } catch (NumericException e) {
                        throw new AssertionError("vs STRING passthrough not numeric: ts=" + ts + ", vs=" + vs);
                    }
                    if (decoded != ts) {
                        throw new AssertionError("vs STRING passthrough mismatch: ts=" + ts + ", vs=" + vs);
                    }
                    if (vv == null || vv.size() == 0 || vv.byteAt(0) != 'v') {
                        throw new AssertionError("vv VARCHAR passthrough mismatch: ts=" + ts + ", vv=" + vv);
                    }
                    prevTs = ts;
                    expectedRn++;
                }
                return cursor.isRoutingEligible();
            }
        }
    }

    // A checkpoint-agent thread cycles startCheckpoint/endCheckpoint on the view
    // (the DatabaseCheckpointAgent freeze handshake) while a refresh driver
    // maintains it and writers ingest. The freeze gate serialises against the
    // worker; the try/finally guarantees endCheckpoint so a freeze never leaks and
    // blocks the final quiescence drive. After all threads join and the refresh
    // quiesces single-threaded, the view equals the from-scratch recompute.
    private void runCheckpointDuringRefresh(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(0, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency checkpoint-during-refresh: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        try {
            // numWriters writers + the refresh driver + the checkpoint agent,
            // released together so the freeze lands while ingestion and refresh
            // are both in flight.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 2);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    while (running.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(job);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");
            final Thread agent = new Thread(() -> {
                try {
                    barrier.await();
                    while (running.get()) {
                        // Mirror DatabaseCheckpointAgent: freeze, (the per-LV file
                        // copy would run here), unfreeze. The finally guarantees
                        // endCheckpoint regardless of how the freeze interleaves.
                        instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                        try {
                            Thread.yield();
                        } finally {
                            instance.endCheckpoint();
                        }
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-checkpoint-agent");

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            agent.start();
            for (Thread t : writers) {
                t.join();
            }
            running.set(false);
            driver.join();
            agent.join();

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded (no agent), then assert the oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    private void runCheckpointFreezeDuringLatchHeldRewrite() throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS "
                + "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base");

        // One committed base row so the view carries a real durable _lv.s to rewrite.
        final TableToken baseToken = engine.verifyTableName("base");
        try (WalWriter w = engine.getWalWriter(baseToken)) {
            appendRow(w, MicrosTimestampDriver.floor(DATA_START), 0, 1, 1.0);
            w.commit();
        }
        drainWalQueue();

        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        final TableToken lvToken = instance.getLiveViewToken();
        // Advance both floors so each rewrite runs its full body instead of
        // short-circuiting at the <= guard (the guard sits after the removed wait).
        final long advanceConsumed = instance.getStateReader().getLvConsumedSeqTxn() + 1;
        final long advanceApplied = instance.getStateReader().getLastProcessedSeqTxn() + 1;

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final CountDownLatch latchHeld = new CountDownLatch(1);

        // Worker: mirrors refreshInstance - takes the refresh latch for the whole
        // turn, then (once the agent has armed the freeze mid-turn) runs the two
        // latch-held _lv.s rewrites. Before waitForUnfrozen() was dropped from them,
        // the first parked forever while still holding the latch.
        final Thread worker = new Thread(() -> {
            try (
                    BlockFileWriter bfw = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                Assert.assertTrue(instance.tryLockForRefresh());
                try {
                    latchHeld.countDown();
                    // Spin until the agent arms the freeze, so the rewrites below run
                    // strictly inside its window. It is the INTENT that is observable
                    // here: startCheckpoint publishes freezeInProgress only once it holds
                    // the refresh latch, which this thread is holding for the whole body.
                    final long deadline = System.currentTimeMillis() + 60_000;
                    while (!instance.isFreezeArmed()) {
                        if (System.currentTimeMillis() > deadline) {
                            throw new AssertionError("checkpoint freeze was never armed");
                        }
                        Thread.onSpinWait();
                    }
                    engine.advanceLiveViewConsumedSeqTxn(lvToken, advanceConsumed, bfw, path);
                    // The in-band reconcile caller runs under the refresh latch, so applyLiveViewData
                    // does not park on waitForUnfrozen().
                    engine.applyLiveViewData(lvToken, advanceApplied, bfw, path);
                } finally {
                    instance.unlockAfterRefresh();
                }
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                clearWorkerThreadLocals();
            }
        }, "lv-worker");

        // Agent: mirrors DatabaseCheckpointAgent - once the worker holds the latch,
        // startCheckpoint arms the freeze intent and then waits for that same latch,
        // blocking until the worker releases it after its rewrites.
        final Thread agent = new Thread(() -> {
            try {
                latchHeld.await();
                instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
                instance.endCheckpoint();
            } catch (Throwable th) {
                errors.add(th);
            } finally {
                clearWorkerThreadLocals();
            }
        }, "lv-checkpoint-agent");

        worker.start();
        agent.start();

        worker.join(60_000);
        if (worker.isAlive()) {
            // Release a leaked freeze so both threads can unwind before failing.
            // waitForUnfrozen deliberately ignores interrupts until this gate clears.
            instance.endCheckpoint();
            worker.join(60_000);
            agent.join(60_000);
            Assert.fail("advanceLiveViewConsumedSeqTxn deadlocked against a concurrent checkpoint freeze while holding the refresh latch");
        }
        agent.join(60_000);
        Assert.assertFalse("checkpoint agent thread did not finish", agent.isAlive());

        if (!errors.isEmpty()) {
            throw new RuntimeException("worker thread failed", errors.peek());
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    private void runConcurrent(
            Rnd rnd,
            int variant,
            int numWriters,
            int rowCount,
            boolean inMemory,
            boolean concurrentRefresh
    ) throws Exception {
        // Reset the clock per run to the fixed CREATE moment so the one-year gap to
        // the data is restored even after a prior run advanced it.
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(variant, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + "START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        LOG.info().$("LV concurrency: variant=").$(variant).$(", writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", inMem=").$(inMemory)
                .$(", concurrentRefresh=").$(concurrentRefresh).$(", sql=").$(viewSql).$();

        // Generate the logical dataset: strictly-unique, strictly-increasing
        // timestamps; random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        try {
            final int driverCount = concurrentRefresh ? 1 : 0;
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + driverCount);
            final AtomicBoolean ingesting = new AtomicBoolean(true);

            // Each writer owns its own WalWriter and writes a round-robin slice of
            // the rows (writer w gets w, w+numWriters, ...). The slices are disjoint
            // and globally ts-ordered, so timestamps stay unique; the cross-writer
            // commit interleaving is what produces O3.
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }

            // Optional refresh-driver thread: applies the base WAL and runs the
            // refresh job while ingestion is in flight (steady-state production
            // timing). Only this thread touches the clock during the concurrent
            // phase; the final quiescence drive runs after it has joined.
            final LiveViewRefreshJob driverJob = job;
            final Thread driver = concurrentRefresh ? new Thread(() -> {
                try {
                    barrier.await();
                    while (ingesting.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(driverJob);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver") : null;

            for (Thread t : writers) {
                t.start();
            }
            if (driver != null) {
                driver.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            ingesting.set(false);
            if (driver != null) {
                driver.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded, then assert the differential oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over the
        // base table. ORDER BY 1 (the unique ts) gives both sides a total order;
        // genericStringMatch tolerates SYMBOL-vs-STRING on passthrough.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // CREATE LIVE VIEW ... SEED races concurrent base ingestion: the writers and
    // the CREATE start together off the barrier, so the view comes into being while
    // the suffix is still being written.
    private void runCreateDuringIngestion(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(0, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        // Pre-commit the earliest rows [0, preCount) single-threaded, BEFORE CREATE, so
        // the SEED floor sits at the global-min timestamp (tsv[0]). Every row the
        // writers ingest concurrently with CREATE is then above the floor, so even an O3
        // commit is never rejected as sub-floor (Finding 3) - the seed sweep and
        // forward refresh between them cover the full row set exactly once, so the view
        // still equals the recompute. The suffix [preCount, rowCount) races CREATE.
        final int preCount = 1 + rnd.nextInt(8);
        final TableToken baseToken = engine.verifyTableName("base");

        LOG.info().$("LV concurrency CREATE-during-ingestion: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", preCount=").$(preCount)
                .$(", sql=").$(viewSql).$();

        try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
            for (int k = 0; k < preCount; k++) {
                appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
            }
            walWriter.commit();
        }
        drainWalQueue();

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        LiveViewRefreshJob job = null;
        try {
            // numWriters writers + this (main) thread, released together, so CREATE and
            // the suffix ingestion start at the same instant.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            for (Thread t : writers) {
                t.start();
            }
            barrier.await();
            execute(createSql); // races the concurrent suffix ingestion
            for (Thread t : writers) {
                t.join();
            }
            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded: finish the seed sweep, then drain forward.
            job = new LiveViewRefreshJob(0, engine, 1);
            drainWalQueue();
            driveSeedToCompletion(job, "lv");
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Drives a live-view refresh whose commit mint is refused by an armed demote
    // witness, then asserts the view is not invalidated and recovers once the demote
    // clears. See testDemoteRefusedMintDoesNotInvalidateView for the full rationale.
    private void runDemoteRefusedMintDoesNotInvalidate() throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A coupled (non-IN-MEMORY) view: incrementalRefresh commits the LV WAL block
        // directly through fencedLiveViewCommit on the same tick, so a single drive
        // deterministically reaches the mint - no deferred FLUSH cadence to wait on.
        final String viewSql = "SELECT ts, sym, sum(i) OVER (PARTITION BY sym ORDER BY ts "
                + "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS v FROM base";
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

        final TableToken baseToken = engine.verifyTableName("base");
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);

        // A healthy first cycle so the view is active before the demote window opens.
        try (WalWriter w = engine.getWalWriter(baseToken)) {
            appendRow(w, MicrosTimestampDriver.floor(DATA_START), 0, 1, 1.0);
            w.commit();
        }
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        try {
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertFalse("view must be valid before the demote", instance.getStateReader().isInvalid());

            // More base rows so the next refresh has a real LV WAL block to mint.
            try (WalWriter w = engine.getWalWriter(baseToken)) {
                appendRow(w, MicrosTimestampDriver.floor(DATA_START) + 1_000_000, 0, 2, 2.0);
                appendRow(w, MicrosTimestampDriver.floor(DATA_START) + 2_000_000, 1, 3, 3.0);
                w.commit();
            }
            drainWalQueue();

            // Arm the demote witness: throw the read-only authorization error at the mint
            // point, exactly where a role flip would make the fence's re-check refuse.
            final AtomicInteger refusals = new AtomicInteger(0);
            CairoEngine.setRoleSwitchMintObserver(() -> {
                refusals.incrementAndGet();
                throw CairoException.authorization().put(CairoException.READ_ONLY_ACCESS_MESSAGE);
            });
            try {
                // Every mint attempt while the witness is armed is refused. The refresh
                // must absorb each refusal as retry-later and leave the view valid.
                for (int i = 0; i < 8; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                    Assert.assertFalse(
                            "a demote-refused mint must not invalidate the view",
                            instance.getStateReader().isInvalid()
                    );
                }
                Assert.assertTrue("the fenced mint must have been refused at least once", refusals.get() > 0);
            } finally {
                CairoEngine.setRoleSwitchMintObserver(null);
            }

            // Unlike every other test here, this one *expects* faults - it throws the read-only
            // authorization error at the mint point on purpose. Pin them exactly: each refused mint
            // must be absorbed as one refresh fault and no more. The read-only gate is the branch of
            // handleRefreshFailure that deliberately never touches the flush-retry budget, so the
            // fault counter is the only place a refusal is observable at all.
            final long faultsUnderDemote = instance.getRefreshFaultCount();
            Assert.assertEquals(
                    "each refused mint must land as exactly one refresh fault",
                    refusals.get(),
                    faultsUnderDemote
            );

            // The demote cleared: the same view resumes forward and converges.
            driveRefreshToQuiescence(job);
            Assert.assertFalse("view must remain valid after recovery", instance.getStateReader().isInvalid());
            Assert.assertEquals(
                    "once the demote is lifted the view must refresh incrementally, without faulting",
                    faultsUnderDemote,
                    instance.getRefreshFaultCount()
            );
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1, 2",
                "(lv) ORDER BY 1, 2",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // DROP LIVE VIEW races a refresh driver that is still driving the SEED
    // sweep while writers ingest the suffix. The earliest rows are pre-committed so
    // the sweep has real history to chew through when the drop lands mid-sweep. The
    // contract is a clean teardown of the seed state (sweep cursor, rolling
    // sealed seed boundaries, in-mem tier): registry empty, base intact, no leak.
    private void runDropDuringSeed(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(0, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM BEGINNING AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        // Pre-commit the earliest half so SEED captures a non-trivial history
        // (the sweep is still running when the DROP lands). The remaining suffix is
        // ingested concurrently with the drop.
        final int preCount = Math.max(1, rowCount / 2);
        final TableToken baseToken = engine.verifyTableName("base");
        try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
            for (int k = 0; k < preCount; k++) {
                appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
            }
            walWriter.commit();
        }
        drainWalQueue();

        execute(createSql);

        LOG.info().$("LV concurrency DROP-during-seed: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", preCount=").$(preCount)
                .$(", sql=").$(viewSql).$();

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean refreshing = new AtomicBoolean(true);
        try {
            // numWriters writers + the refresh driver + this (main) thread, released
            // together, so by the time the main thread fires the DROP the writers
            // are ingesting and the driver is driving the seed sweep.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 2);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    while (refreshing.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(job); // drives both the seed sweep and forward refresh
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            barrier.await();
            execute("DROP LIVE VIEW lv"); // races the in-flight seed + ingestion

            for (Thread t : writers) {
                t.join();
            }
            refreshing.set(false);
            driver.join();

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }
        } finally {
            Misc.free(job);
        }

        // Clean teardown: the view is gone from the registry and the base table
        // survived intact. No leak - assertMemoryLeak wraps the whole run.
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
        drainWalQueue();
        assertQuery("SELECT count(*) FROM base").noRandomAccess().expectSize().returns("count\n" + rowCount + "\n");

        execute("DROP TABLE base");
    }

    // DROP LIVE VIEW races a refresh-driver thread that keeps pumping the refresh job
    // while writers ingest. The refresh job swallows per-view failures
    // (handleRefreshFailure), so a torn-down view never throws into the driver - the
    // contract under test is a clean teardown, not a thrown error.
    private void runDropDuringRefresh(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String viewSql = "SELECT " + projection(0, n) + " FROM base";
        // IN MEMORY so the drop tears down the in-mem tier (slot buffers, double
        // buffer) under the race, not just the on-disk path.
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency DROP-during-refresh: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean refreshing = new AtomicBoolean(true);
        try {
            // numWriters writers + the refresh driver + this (main) thread, released
            // together, so by the time the main thread fires the DROP the writers are
            // ingesting and the driver is turning the refresh job - the drop lands
            // mid-flight.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 2);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    while (refreshing.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(job);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            barrier.await();
            execute("DROP LIVE VIEW lv"); // races the in-flight refresh

            for (Thread t : writers) {
                t.join();
            }
            refreshing.set(false);
            driver.join();

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }
        } finally {
            Misc.free(job);
        }

        // Clean teardown: the view is gone from the registry and the base table
        // survived intact (the view drop leaves its row set untouched). No leak -
        // assertMemoryLeak wraps the whole run.
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv"));
        drainWalQueue();
        assertQuery("SELECT count(*) FROM base").noRandomAccess().expectSize().returns("count\n" + rowCount + "\n");

        execute("DROP TABLE base");
    }

    // Two live views with different shapes over the same base, maintained by a
    // single refresh driver while writers ingest concurrently. One base commit
    // fans out to both views; each carries its own per-view refresh latch, so the
    // driver advances them independently under the cross-writer O3 stream. After
    // quiescence both views equal their from-scratch recomputes.
    private void runMultiViewConcurrent(Rnd rnd, int numWriters, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        final int n = 1 + rnd.nextInt(8);
        final String view1Sql = "SELECT " + projection(0, n) + " FROM base"; // sum
        final String view2Sql = "SELECT " + projection(1, n) + " FROM base"; // row_number

        execute("DROP LIVE VIEW IF EXISTS lv1");
        execute("DROP LIVE VIEW IF EXISTS lv2");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute("CREATE LIVE VIEW lv1 FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + view1Sql);
        execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + view2Sql);

        LOG.info().$("LV concurrency multi-view: writers=").$(numWriters)
                .$(", rows=").$(rowCount).$(", n=").$(n).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean ingesting = new AtomicBoolean(true);
        try {
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    while (ingesting.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        drainJob(job);
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            for (Thread t : writers) {
                t.join();
            }
            ingesting.set(false);
            driver.join();

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(engine, sqlExecutionContext, "(" + view1Sql + ") ORDER BY 1", "(lv1) ORDER BY 1", LOG, true);
        assertNoRefreshFaults("lv1");
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, "(" + view2Sql + ") ORDER BY 1", "(lv2) ORDER BY 1", LOG, true);
        assertNoRefreshFaults("lv2");

        execute("DROP LIVE VIEW lv1");
        execute("DROP LIVE VIEW lv2");
        execute("DROP TABLE base");
    }

    // Reader threads churn cursors over an IN MEMORY view while the refresh driver
    // appends via the fast-path CAS and writers ingest - the reader-churn risk.
    // The readers detect torn reads / tier-slot corruption by crashing or
    // throwing; the quiesced final state still matches the recompute.
    // <p>
    // leadMode: the driver advances the clock only a fraction of FLUSH EVERY per
    // tick, so most refreshes publish an un-flushed lead (the tier leads disk) and
    // flushes land only every few ticks - the readers churn against a live lead
    // (Mode A) with flushes underneath, instead of a strict disk subset (Mode B).
    // Runs several LiveViewRefreshJob workers on their own threads (as production
    // does, one per pool worker) against sustained multi-writer O3 ingestion, plus
    // reader threads that assert a per-snapshot prefix invariant. The refresh
    // workers contend the shared registry, task queue and per-view refresh latch -
    // the refresh-vs-refresh path a single driver can never reach. After every
    // thread joins and one worker drives the refresh to quiescence single-threaded,
    // the view must equal the from-scratch recompute.
    private void runMultiRefreshWorkerSoak(Rnd rnd, int numWriters, int numRefreshWorkers, int numReaders, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));

        // A SYMBOL-free row_number() IN MEMORY view: the reads route through the
        // in-mem tier (Mode B) so the multi-worker refresh churns the tier under the
        // readers, and the readers can assert the gapless-rn prefix invariant.
        final String viewSql = "SELECT ts, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency multi-refresh-worker soak: writers=").$(numWriters)
                .$(", refreshWorkers=").$(numRefreshWorkers).$(", readers=").$(numReaders)
                .$(", rows=").$(rowCount).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        // One LiveViewRefreshJob per worker, exactly as ServerMain.setupLiveViewJobs
        // creates them (workerId = w). Nothing shards views by workerId, so every job
        // walks the whole registry.
        final LiveViewRefreshJob[] jobs = new LiveViewRefreshJob[numRefreshWorkers];
        for (int w = 0; w < numRefreshWorkers; w++) {
            jobs[w] = new LiveViewRefreshJob(w, engine, 1);
        }
        final AtomicBoolean running = new AtomicBoolean(true);
        try {
            // Writers + the base-apply/clock driver are released together; the refresh
            // workers and readers spin independently until running clears.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 5 + rnd.nextInt(20);
                writers[w] = newWriterThread(w, numWriters, 0, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, errors);
            }

            // A single driver applies the base (and LV) WAL and advances the clock so
            // FLUSH EVERY ticks come due. Keeping the clock and drainWalQueue on one
            // thread isolates the refresh-vs-refresh contention (the actual subject)
            // from driving the shared wal-apply job across many threads.
            final Thread applyDriver = new Thread(() -> {
                try {
                    barrier.await();
                    while (running.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                    }
                } catch (Throwable th) {
                    errors.add(th);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-apply-driver");

            // The refresh workers: each pumps its OWN job; all contend the same
            // registry, task queue and per-view latch. This is the production default.
            final Thread[] refreshWorkers = new Thread[numRefreshWorkers];
            for (int w = 0; w < numRefreshWorkers; w++) {
                final LiveViewRefreshJob job = jobs[w];
                refreshWorkers[w] = new Thread(() -> {
                    try {
                        while (running.get()) {
                            drainJob(job);
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-refresh-worker-" + w);
            }

            // Readers assert the prefix invariant on every snapshot (ts ascending, rn
            // gapless 1..N), so a torn slot or a seam dup/gap from two workers racing
            // the publish surfaces as a value mismatch, not merely a crash.
            final Thread[] readers = new Thread[numReaders];
            for (int r = 0; r < numReaders; r++) {
                readers[r] = new Thread(() -> {
                    try {
                        while (running.get()) {
                            readRowNumberViewOnce();
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-reader-" + r);
            }

            for (Thread t : writers) {
                t.start();
            }
            applyDriver.start();
            for (Thread t : refreshWorkers) {
                t.start();
            }
            for (Thread t : readers) {
                t.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            running.set(false);
            applyDriver.join();
            for (Thread t : refreshWorkers) {
                t.join();
            }
            for (Thread t : readers) {
                t.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded through one job, then assert the oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(jobs[0]);
        } finally {
            for (LiveViewRefreshJob job : jobs) {
                Misc.free(job);
            }
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Reader threads run a FILTERED read over an IN MEMORY view - the shape that routes
    // through LiveViewPageFrameCursor and hands the pinned slot's frame to the parallel
    // filter - while the refresh driver swaps tier slots and writers ingest.
    // <p>
    // Three things have to be arranged or the race does not happen at all, and none of them
    // announces itself: with any one missing the run still passes, having tested nothing.
    // <ul>
    //   <li><b>The writers pace themselves to the refresh driver</b>, one batch per tick.
    //   Left to run flat out they finish INSIDE the driver's first tick - at any row count,
    //   since the tick drains what they wrote - so the driver ticks exactly once, publishes
    //   nothing while the readers are alive, and the tier is still null when they all
    //   stop.</li>
    //   <li><b>The tier is pre-warmed</b>, so it is allocated, populated and stamped before
    //   the first reader opens. It only comes into being on the first publish, which is a
    //   tick the readers would otherwise spend reading a view that has no tier to route
    //   to.</li>
    //   <li><b>The growth budget is 0</b>, which makes isCompactionWorthwhile true on every
    //   publish, so the refresh worker always takes the SLOW path (fill the other slot,
    //   then publishSwap) rather than appending in place. It is a determinism knob rather
    //   than an enabler: swaps happen under this soak's churn either way, because a
    //   reader's pin defeats the fast-path CAS on the published slot and drops the writer
    //   onto the slow path regardless. The budget makes every publish a swap instead of
    //   only the ones a reader happened to collide with.</li>
    // </ul>
    // The readers assert the same per-snapshot invariant the other row_number() soaks do -
    // ts ascending, rn a gapless 1..N - which holds for the filtered output too because the
    // predicate keeps every row; what this adds is that the rows come back through filter
    // workers reading the slot's frame. The counters the run asserts on at the end are not
    // decoration: without them a soak that has quietly stopped routing, stopped swapping,
    // or stopped racing still passes, which is exactly the state the first draft was in.
    private void runParallelFilterTierSwapSoak(Rnd rnd, int numWriters, int numReaders, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);

        final String viewSql = "SELECT ts, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency parallel-filter tier-swap soak: writers=").$(numWriters)
                .$(", readers=").$(numReaders).$(", filterWorkers=").$(FILTER_WORKER_COUNT)
                .$(", rows=").$(rowCount).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        // Refresh ticks completed by the driver. The writers pace off it, so it doubles as
        // the clock the whole soak runs on.
        final AtomicLong refreshTicks = new AtomicLong();
        // Reads that found the tier's frame at the instant they looked, and swaps the
        // driver caught between two of its own ticks. Both are the run's own evidence that
        // it raced what it says it raced.
        final AtomicLong routedReads = new AtomicLong();
        final AtomicLong swapsObserved = new AtomicLong();
        // Query jobs only: the writer jobs would put a second ApplyWal2TableJob against the
        // refresh driver's own drainWalQueue, and the filter's reduce queue is all this needs.
        final WorkerPool filterPool = new WorkerPool(() -> FILTER_WORKER_COUNT);
        WorkerPoolUtils.setupQueryJobs(filterPool, engine);
        filterPool.start(LOG);
        try {
            // Pre-warm, single-threaded: commit the leading slice and refresh it, so the
            // tier is live and stamped before any reader opens.
            final int preCount = Math.max(1, rowCount / 4);
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                for (int k = 0; k < preCount; k++) {
                    appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                }
                walWriter.commit();
            }
            drainWalQueue();
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainJob(job);
            drainWalQueue();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            final LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("the pre-warm refresh must publish a tier for the readers to route to", tier);
            Assert.assertTrue("the pre-warmed tier must hold rows", tier.publishedRowCount() > 0);

            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                // Small batches on purpose, and the one knob here that is not shared with
                // the other soaks: a paced writer commits once per batch and then waits out
                // a refresh tick, so the batch size IS how many publishes the run gets to
                // race. The other soaks' 5..24 leaves a handful.
                final int batch = 4 + rnd.nextInt(4);
                writers[w] = newPacedWriterThread(
                        w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, refreshTicks, errors
                );
            }
            // The clock step is a fraction of FLUSH EVERY, so a flush comes due only every
            // few ticks and the refreshes in between leave an un-flushed lead resident -
            // which is what makes the frames the filter workers read carry slot rows disk
            // does not have.
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    int lastPublishedIdx = tier.getPublishedIdx();
                    while (running.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS / 6);
                        drainWalQueue();
                        // drainJob's own loop, opened up so the published index can be
                        // sampled between passes rather than once per tick. A tick runs up
                        // to 64 passes and the index only ever alternates between two
                        // slots, so a per-tick sample reports the parity a run of swaps
                        // happened to land on rather than its length - it read single
                        // digits against passes that had swapped an order of magnitude
                        // more. Per pass it is still a lower bound (a pass can publish and
                        // the sample can miss a pair), which is all the assertion needs.
                        for (int i = 0; i < REFRESH_PASSES_PER_TICK && job.run(); i++) {
                            final int publishedIdx = tier.getPublishedIdx();
                            if (publishedIdx != lastPublishedIdx) {
                                swapsObserved.incrementAndGet();
                                lastPublishedIdx = publishedIdx;
                            }
                        }
                        refreshTicks.incrementAndGet();
                    }
                } catch (Throwable th) {
                    errors.add(th);
                    // The writers pace off this counter; keep it moving or they wait out
                    // their deadline one batch at a time and the failure takes minutes to
                    // surface.
                    refreshTicks.addAndGet(Long.MAX_VALUE / 2);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");

            final Thread[] readers = new Thread[numReaders];
            for (int r = 0; r < numReaders; r++) {
                readers[r] = new Thread(() -> {
                    try {
                        while (running.get()) {
                            if (readFilteredRowNumberViewOnce()) {
                                routedReads.incrementAndGet();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-filter-reader-" + r);
            }

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            for (Thread t : readers) {
                t.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            running.set(false);
            driver.join();
            for (Thread t : readers) {
                t.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            drainWalQueue();
            driveRefreshToQuiescence(job);

            LOG.info().$("LV concurrency parallel-filter tier-swap soak done: refreshTicks=").$(refreshTicks.get())
                    .$(", routedReads=").$(routedReads.get()).$(", swapsObserved=").$(swapsObserved.get()).$();
            // The soak's own evidence. A green run that raced nothing looks identical
            // without these: reads that never routed read disk frames, and a tier that
            // never swapped hands the workers a slot the writer was never trying to
            // reclaim.
            Assert.assertTrue(
                    "the readers must have routed through the tier's page frame mid-soak, not just after it",
                    routedReads.get() > 0
            );
            Assert.assertTrue(
                    "the refresh worker must have swapped tier slots under the readers",
                    swapsObserved.get() > 0
            );
        } finally {
            // Before the leak check, and before the assertions below reach for the tier: a
            // live filter worker still holds frames over it.
            filterPool.haltAndAssertCleanForTest(WorkerPool.DEFAULT_HALT_TIMEOUT_NANOS);
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        // The plan is the other half of the guard: routedReads above proves the reads
        // reached the tier's frame, this proves the filter over it ran in PARALLEL. Neither
        // implies the other - a fork back to the interpreted cursor would still route, and
        // leave no worker to race.
        assertQuery(FILTERED_VIEW_SQL)
                .noLeakCheck()
                .assertsPlanContaining("Async", "Filter", "LiveView", "inMemory: true");

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Reader threads churn cursors over an IN MEMORY view while the refresh driver publishes
    // tier slots and paced writers ingest - the read/publish hand-off. Hardened the same way
    // runParallelFilterTierSwapSoak was, and for the same reason: left unpaced the writers
    // finish inside the driver's first tick at any row count, the driver publishes nothing
    // while the readers are alive, and the run races nothing while passing. So the writers
    // pace to the driver (one batch per tick), the tier is pre-warmed before any reader opens,
    // and the run asserts its own counters - swaps the driver caught under the readers, and
    // (for a tier-routing shape) reads that actually reached the tier mid-soak.
    private void runReaderChurnSoak(Rnd rnd, int numWriters, int numReaders, int rowCount, boolean modeB, boolean leadMode) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
        // Growth budget 0 makes isCompactionWorthwhile true on every publish, so the refresh
        // worker always takes the slow path (fill the other slot, then publishSwap) and the
        // driver's getPublishedIdx() sampling below counts a swap per publish deterministically
        // rather than only the ones a reader's pin happened to collide with. A determinism knob,
        // not an enabler: under this churn a reader pin defeats the fast-path CAS anyway.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);

        final int n = 1 + rnd.nextInt(8);
        // modeB: a SYMBOL-free row_number() view, so the read path routes through
        // the in-mem tier (Mode B seam routing) and the readers can assert the
        // gapless-rn invariant per snapshot. Otherwise: a sum() view with a SYMBOL
        // passthrough, which routes disk-only but still exercises the publish hand-off
        // (the readers open cursors over the view while the driver swaps under them).
        final String viewSql = modeB
                ? "SELECT ts, i, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base"
                : "SELECT " + projection(0, n) + " FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency reader-churn soak: writers=").$(numWriters)
                .$(", readers=").$(numReaders).$(", rows=").$(rowCount).$(", n=").$(n)
                .$(", modeB=").$(modeB).$(", leadMode=").$(leadMode).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        // Refresh ticks completed by the driver; the writers pace off it. And the run's own
        // evidence: reads that found the tier at the instant they looked, and swaps the driver
        // caught between two of its own passes.
        final AtomicLong refreshTicks = new AtomicLong();
        final AtomicLong routedReads = new AtomicLong();
        final AtomicLong swapsObserved = new AtomicLong();
        // In lead mode advance the clock by a fraction of FLUSH EVERY per tick so a flush comes
        // due only every few ticks; the refreshes in between publish the un-flushed lead (Mode
        // A). Otherwise advance past FLUSH EVERY every tick so every refresh also flushes and
        // the tier stays a disk subset the read seams over (Mode B).
        final long clockStepMicros = leadMode ? CLOCK_ADVANCE_MICROS / 6 : CLOCK_ADVANCE_MICROS;
        try {
            // Pre-warm, single-threaded: commit the leading slice and refresh it, so the tier is
            // live and stamped before any reader opens. It only comes into being on the first
            // publish, which is a tick the readers would otherwise spend reading a view with no
            // tier to route to.
            final int preCount = Math.max(1, rowCount / 4);
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                for (int k = 0; k < preCount; k++) {
                    appendRow(walWriter, tsv[k], symIdx[k], iv[k], xv[k]);
                }
                walWriter.commit();
            }
            drainWalQueue();
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainJob(job);
            drainWalQueue();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            final LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("the pre-warm refresh must publish a tier for the readers to route to", tier);
            Assert.assertTrue("the pre-warmed tier must hold rows", tier.publishedRowCount() > 0);

            // numWriters paced writers + the refresh driver, released together. Readers spin
            // independently (no synchronized start needed) until running clears.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                // Small batches, and the pacing means the batch size IS how many publishes the
                // run gets to race (one commit per tick); see runParallelFilterTierSwapSoak.
                final int batch = 4 + rnd.nextInt(4);
                writers[w] = newPacedWriterThread(
                        w, numWriters, preCount, rowCount, batch, tsv, symIdx, iv, xv, baseToken, barrier, refreshTicks, errors
                );
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    int lastPublishedIdx = tier.getPublishedIdx();
                    while (running.get()) {
                        setCurrentMicros(currentMicros + clockStepMicros);
                        drainWalQueue();
                        // drainJob's own loop, opened up so the published index can be sampled
                        // between passes: a tick runs up to 64 passes and the index only
                        // alternates between two slots, so a per-tick sample would report a run
                        // of swaps' parity rather than its length.
                        for (int i = 0; i < REFRESH_PASSES_PER_TICK && job.run(); i++) {
                            final int publishedIdx = tier.getPublishedIdx();
                            if (publishedIdx != lastPublishedIdx) {
                                swapsObserved.incrementAndGet();
                                lastPublishedIdx = publishedIdx;
                            }
                        }
                        refreshTicks.incrementAndGet();
                    }
                } catch (Throwable th) {
                    errors.add(th);
                    // The writers pace off this counter; keep it moving so a dead driver fails
                    // the run through the errors queue rather than hanging it.
                    refreshTicks.addAndGet(Long.MAX_VALUE / 2);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");

            final Thread[] readers = new Thread[numReaders];
            for (int r = 0; r < numReaders; r++) {
                readers[r] = new Thread(() -> {
                    try {
                        while (running.get()) {
                            if (modeB) {
                                if (readRowNumberViewOnce()) {
                                    routedReads.incrementAndGet();
                                }
                            } else {
                                readViewOnce();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-reader-" + r);
            }

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            for (Thread t : readers) {
                t.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            running.set(false);
            driver.join();
            for (Thread t : readers) {
                t.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded, then assert the differential oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(job);

            LOG.info().$("LV concurrency reader-churn soak done: modeB=").$(modeB).$(", leadMode=").$(leadMode)
                    .$(", refreshTicks=").$(refreshTicks.get()).$(", routedReads=").$(routedReads.get())
                    .$(", swapsObserved=").$(swapsObserved.get()).$();
            // The soak's own evidence. A green run that raced nothing looks identical without
            // these: a driver that never published under the readers, or reads that never
            // reached the tier.
            Assert.assertTrue(
                    "the refresh worker must have swapped tier slots under the churning readers",
                    swapsObserved.get() > 0
            );
            if (modeB) {
                Assert.assertTrue(
                        "the readers must have routed through the tier mid-soak, not just after it",
                        routedReads.get() > 0
                );
            }
        } finally {
            Misc.free(job);
        }

        // The driveRefreshToQuiescence above flushed every lead, so the view is a
        // strict disk subset now - the standard ORDER BY 1 oracle is safe (no lead
        // to drop on a fence miss).
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        if (modeB && !leadMode) {
            // Guard against the soak silently passing on disk-only reads: confirm
            // the quiesced production read path actually routes through Mode B.
            assertModeBEngaged();
        }

        if (leadMode) {
            // Confirm Mode A is reachable for this view shape: rebuild a known
            // un-flushed lead and assert the cursor serves it and equals the
            // recompute. (The soak above already exercised live leads under the
            // readers; this pins it deterministically post-quiescence.)
            assertModeALeadEngaged(viewSql, tsv[rowCount - 1]);
        }

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Reader threads churn cursors over an IN MEMORY view whose output carries
    // STRING + VARCHAR passthrough columns (plus row_number() so the reads route
    // through the in-mem tier, Mode B) while the refresh driver appends via the
    // fast-path CAS and writers ingest. This exercises the var-length tier read
    // path - the aux-region offset vector and the payload data region, both of
    // which realloc (and move their base address) on append - against the
    // lock-free read/publish hand-off. The readers assert a per-snapshot invariant
    // (ts ascending, rn gapless, vs/vv decoding back to their ts-derived values),
    // so a torn var-length read surfaces as a value mismatch; the quiesced final
    // state still matches the recompute.
    private void runVarSizeReaderChurnSoak(Rnd rnd, int numWriters, int numReaders, int rowCount) throws Exception {
        setCurrentMicros(MicrosTimestampDriver.floor(CLOCK_START));
        // See runReaderChurnSoak: forces every publish onto the slow path so the swap sampling
        // is deterministic. It also makes every publish realloc-and-move the (data, aux) regions
        // the var-length reads dereference, which is the base-pointer move this soak is about.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);

        final String viewSql = "SELECT ts, vs, vv, count(*) OVER (PARTITION BY 0 ORDER BY ts ROWS BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS rn FROM base";
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 60s START FROM NOW AS " + viewSql;

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, vs STRING, vv VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");

        // Only the timestamps are used; vs/vv are derived from each row's ts by the
        // writer. Reuse the shared generator for the strictly-unique ts stream.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        generateDataset(rnd, rowCount, tsv, symIdx, iv, xv);

        execute(createSql);

        LOG.info().$("LV concurrency var-size reader-churn soak: writers=").$(numWriters)
                .$(", readers=").$(numReaders).$(", rows=").$(rowCount).$(", sql=").$(viewSql).$();

        final TableToken baseToken = engine.verifyTableName("base");
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
        final AtomicBoolean running = new AtomicBoolean(true);
        // The writers pace off refreshTicks; routedReads and swapsObserved are the run's own
        // evidence that it raced the tier rather than spending itself against an empty one.
        final AtomicLong refreshTicks = new AtomicLong();
        final AtomicLong routedReads = new AtomicLong();
        final AtomicLong swapsObserved = new AtomicLong();
        try {
            // Pre-warm, single-threaded: commit the leading slice and refresh it, so the tier is
            // live and stamped before any reader opens.
            final int preCount = Math.max(1, rowCount / 4);
            try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                final StringSink strSink = new StringSink();
                final Utf8StringSink vcSink = new Utf8StringSink();
                for (int k = 0; k < preCount; k++) {
                    appendVarSizeRow(walWriter, tsv[k], strSink, vcSink);
                }
                walWriter.commit();
            }
            drainWalQueue();
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainJob(job);
            drainWalQueue();

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            final LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("the pre-warm refresh must publish a tier for the readers to route to", tier);
            Assert.assertTrue("the pre-warmed tier must hold rows", tier.publishedRowCount() > 0);

            // numWriters paced writers + the refresh driver, released together. Readers
            // spin independently (no synchronized start needed) until running clears.
            final CyclicBarrier barrier = new CyclicBarrier(numWriters + 1);
            final Thread[] writers = new Thread[numWriters];
            for (int w = 0; w < numWriters; w++) {
                final int batch = 4 + rnd.nextInt(4);
                writers[w] = newPacedVarSizeWriterThread(
                        w, numWriters, preCount, rowCount, batch, tsv, baseToken, barrier, refreshTicks, errors
                );
            }
            final Thread driver = new Thread(() -> {
                try {
                    barrier.await();
                    int lastPublishedIdx = tier.getPublishedIdx();
                    while (running.get()) {
                        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                        drainWalQueue();
                        // drainJob's own loop, opened up so a swap is sampled between passes
                        // rather than once per tick; see runReaderChurnSoak.
                        for (int i = 0; i < REFRESH_PASSES_PER_TICK && job.run(); i++) {
                            final int publishedIdx = tier.getPublishedIdx();
                            if (publishedIdx != lastPublishedIdx) {
                                swapsObserved.incrementAndGet();
                                lastPublishedIdx = publishedIdx;
                            }
                        }
                        refreshTicks.incrementAndGet();
                    }
                } catch (Throwable th) {
                    errors.add(th);
                    refreshTicks.addAndGet(Long.MAX_VALUE / 2);
                } finally {
                    clearWorkerThreadLocals();
                }
            }, "lv-refresh-driver");

            final Thread[] readers = new Thread[numReaders];
            for (int r = 0; r < numReaders; r++) {
                readers[r] = new Thread(() -> {
                    try {
                        while (running.get()) {
                            if (readVarSizeViewOnce()) {
                                routedReads.incrementAndGet();
                            }
                        }
                    } catch (Throwable th) {
                        errors.add(th);
                    } finally {
                        clearWorkerThreadLocals();
                    }
                }, "lv-varsize-reader-" + r);
            }

            for (Thread t : writers) {
                t.start();
            }
            driver.start();
            for (Thread t : readers) {
                t.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            running.set(false);
            driver.join();
            for (Thread t : readers) {
                t.join();
            }

            if (!errors.isEmpty()) {
                throw new RuntimeException("worker thread failed", errors.peek());
            }

            // Quiesce single-threaded, then assert the differential oracle below.
            drainWalQueue();
            driveRefreshToQuiescence(job);

            LOG.info().$("LV concurrency var-size reader-churn soak done: refreshTicks=").$(refreshTicks.get())
                    .$(", routedReads=").$(routedReads.get()).$(", swapsObserved=").$(swapsObserved.get()).$();
            Assert.assertTrue(
                    "the refresh worker must have swapped tier slots under the churning readers",
                    swapsObserved.get() > 0
            );
            Assert.assertTrue(
                    "the readers must have routed through the tier's var-length regions mid-soak, not just after it",
                    routedReads.get() > 0
            );
        } finally {
            Misc.free(job);
        }

        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");

        // Guard against the soak silently passing on disk-only reads: confirm the
        // quiesced production read path actually routes through Mode B.
        assertModeBEngaged();

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    // Rebuilds a deterministic un-flushed lead on top of the quiesced (fully
    // flushed) state and asserts Mode A serves it: pins the flush clock to now and
    // refreshes a forward batch above the global max ts so it publishes into the
    // tier without crossing FLUSH EVERY, then opens the inner cursor and asserts it
    // routes through the tier, serves exactly the lead, and equals the recompute.
    private void assertModeALeadEngaged(String viewSql, long maxTs) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        instance.setLastFlushTimeUs(currentMicros);
        execute("INSERT INTO base (ts, sym, i, x) VALUES ("
                + (maxTs + 1) + "::timestamp, 'AA', 1, 1.0), ("
                + (maxTs + 2) + "::timestamp, 'AA', 2, 2.0)");
        drainWalQueue();
        try (LiveViewRefreshJob leadJob = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(leadJob); // refresh only -> lead in RAM (clock not advanced past FLUSH EVERY)
        }

        final long leadRows = instance.getLeadRowCount();
        Assert.assertTrue("a non-empty lead must be resident", leadRows > 0);

        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", sqlExecutionContext).getRecordCursorFactory()
        ) {
            final LiveViewRecordCursorFactory f = unwrapLvFactory(factory);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) f.getCursor(sqlExecutionContext)) {
                StringSink sink = new StringSink();
                println(f.getMetadata(), cursor, sink);
                Assert.assertTrue("Mode A lead read must route through the tier", cursor.isRoutingEligible());
                Assert.assertEquals("the cursor must serve exactly the lead", leadRows, cursor.leadRowsServed());
            }
        }

        // Direct SELECT * FROM lv (native ts order) equals the recompute, including
        // the lead - not the ORDER BY 1 wrapper, whose routing is not guaranteed
        // Mode A.
        StringSink lvOut = new StringSink();
        printSql("SELECT * FROM lv", lvOut);
        StringSink recompute = new StringSink();
        printSql(viewSql, recompute);
        Assert.assertEquals("Mode A lead read must equal the recompute", recompute.toString(), lvOut.toString());
    }

    // Unwraps any wrapper factory (QueryProgress, the async filter) down to the live
    // view's own, so a test can ask it for a page frame cursor directly and see which
    // tier it routed to.
    private static LiveViewRecordCursorFactory unwrapLvFactory(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
        return (LiveViewRecordCursorFactory) f;
    }

    // Confirms the SYMBOL-free row_number() view engages Mode B on the production
    // read path. Run single-threaded after quiescence, so the published slot is
    // stable and stamped with the latest applied seqTxn the fresh reader reports -
    // the fence holds and the cursor routes through the in-mem slot rather than
    // disk-only. Without this the reader-churn soak could pass even if every read
    // had fallen back to disk-only, leaving Mode B untested.
    private void assertModeBEngaged() throws Exception {
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile("SELECT * FROM lv", ctx).getRecordCursorFactory()
        ) {
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(ctx)) {
                Assert.assertTrue("quiesced row_number() read must route through Mode B", cursor.isRoutingEligible());
            }
        }
    }
}
