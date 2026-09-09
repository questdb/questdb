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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A materialized view is an accepted live-view base (WAL-backed, designated timestamp -
 * see {@code LiveViewSmokeTest#testMaterializedViewAcceptedAsBase}), which makes the
 * freeze-and-continue contract for base data removal unsound for that one base kind.
 * <p>
 * Freeze-and-continue says a live view keeps its already-emitted rows and its window
 * accumulators across a base TRUNCATE / DROP PARTITION: the refresh worker walks past the
 * removal seqTxn without rewriting LV state. That is right for a plain base, where a
 * TRUNCATE retires settled data the view already consumed and a live view is a
 * forward-computed row stream rather than a re-derivable aggregate.
 * <p>
 * A materialized view's contents are <em>derived</em>, so a TRUNCATE of one is never data
 * retirement - it is the rebuild half of a full refresh, which re-materialises the same
 * logical rows. A live view that walked past it would emit derived rows for those rows a
 * second time, with its accumulators still carrying the pre-rebuild state, and would stay
 * ACTIVE while doing so.
 */
public class LiveViewMatViewBaseTest extends AbstractLiveViewTest {

    @Test
    public void testExpiryPolicyOnMatViewBaseInvalidatesLiveViewOnNextTurn() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (SELECT * FROM base)");
            drainWalAndMatViewQueues(engine);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, k, sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM mvbase");

            execute("ALTER MATERIALIZED VIEW mvbase SET EXPIRE ROWS WHEN v < 0");
            drainWalAndMatViewQueues(engine);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv_on_mv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\tinvalidation_reason\n"
                            + "invalid\tcannot materialize view 'lv_on_mv': source materialized view 'mvbase' has an active EXPIRE ROWS policy\n");
        });
    }

    @Test
    public void testExpiryPolicySequencedBeforeApplyInvalidatesBeforeNewRows() throws Exception {
        assertMemoryLeak(() -> {
            createExpirySourceAndDependents();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:00Z', 'a', 1.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                final LiveViewInstance dep = engine.getLiveViewRegistry().getViewInstance("dep");
                execute("ALTER MATERIALIZED VIEW source SET EXPIRE ROWS WHEN v < 2");
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertFalse(dep.isInvalid());
                final long processedBefore = dep.getLastProcessedSeqTxn();
                drainWalQueue();
                // A caught-up idle view need not revalidate merely because apply finished.
                driveRefreshToQuiescence(job);
                Assert.assertFalse(dep.isInvalid());
                assertQuery("SELECT count() FROM source").noLeakCheck().noRandomAccess().expectSize()
                        .returns("count\n0\n");
                execute("INSERT INTO base VALUES ('2026-01-01T00:01:00Z', 'a', 3.0)");
                drainWalAndMatViewQueues();
                Assert.assertTrue(job.processNotificationsForTest());
                Assert.assertTrue(dep.isInvalid());
                assertExpiryConflict();
                Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                assertQuery("SELECT s FROM dep").noLeakCheck().expectSize().returns("s\n1.0\n");
                assertNoRefreshFaults("dep");
            }
        });
    }

    @Test
    public void testMatViewFullRefreshInvalidatesDependentLiveView() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (" +
                    "SELECT ts, k, avg(v) AS av FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);

            // A bounded frame makes the stale-accumulator half of the corruption visible: the
            // ring still holds the deleted mat-view rows after the rebuild.
            final String viewSql = "SELECT ts, k, avg(av) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS a FROM mvbase";
            // Create over the still-empty mat view so the live view consumes its commits
            // incrementally rather than seeding.
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, k, v) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-01-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-01-01T02:00:00.000000Z', 'a', 3.0)");
                drainWalAndMatViewQueues(engine);
                driveRefreshToQuiescence(job);

                // The view tracked the mat view incrementally, and did so without faulting.
                assertQuery("lv_on_mv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ta\n" +
                        "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                        "2026-01-01T01:00:00.000000Z\ta\t1.5\n" +
                        "2026-01-01T02:00:00.000000Z\ta\t2.0\n");
                assertNoRefreshFaults("lv_on_mv");

                // The operator rebuilds the mat view over a now-truncated base. The mat view
                // TRUNCATEs itself and re-materialises a single bucket.
                execute("TRUNCATE TABLE base");
                execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T05:00:00.000000Z', 'a', 100.0)");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mvbase FULL");
                drainWalAndMatViewQueues(engine);

                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("mvbase ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\tav\n" +
                    "2026-01-01T05:00:00.000000Z\ta\t100.0\n");

            // Pre-fix the view stayed ACTIVE and served four rows: the three buckets the
            // rebuild deleted, plus a 05:00 row of 26.5 - avg(1, 2, 3, 100), the bounded
            // frame's ring still holding the deleted rows. It must invalidate instead.
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n" +
                            "lv_on_mv\tinvalid\tbase materialized view was rebuilt\n");
        });
    }

    /**
     * Same rebuild as {@link #testMatViewFullRefreshInvalidatesDependentLiveView}, but with the
     * refresh worker reaching the mat view's WAL before the apply job does.
     * <p>
     * The two run on different worker pools and nothing serialises them: the sequencer fans the
     * refresh notification out at commit time, and {@code drainBaseWal} walks the base's raw
     * sequencer log with no apply gate (unlike the applied-base and O3 paths, which call
     * {@code ensureBaseApplied}). So the worker can consume the rebuild commits while the TRUNCATE
     * that precedes them is still un-applied. An invalidation hung off the apply job is a lagging
     * backstop, not a fence.
     */
    @Test
    public void testMatViewFullRefreshInvalidatesWhenRefreshRunsBeforeApply() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mvbase AS (" +
                    "SELECT ts, k, avg(v) AS av FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues(engine);

            final String viewSql = "SELECT ts, k, avg(av) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS a FROM mvbase";
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv_on_mv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, k, v) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-01-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-01-01T02:00:00.000000Z', 'a', 3.0)");
                drainWalAndMatViewQueues(engine);
                driveRefreshToQuiescence(job);

                execute("TRUNCATE TABLE base");
                execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T05:00:00.000000Z', 'a', 100.0)");
                drainWalQueue();
                execute("REFRESH MATERIALIZED VIEW mvbase FULL");

                // Only the mat-view refresh job runs: the TRUNCATE and the re-materialised rows
                // are now committed to mvbase's WAL but NOT applied to its table.
                drainMatViewQueue(engine);

                // Give the refresh worker its shot at the sequencer ahead of the apply job, with
                // the FLUSH EVERY cadence due so the cycle also flushes its lead to the LV's own
                // disk tier (flushLead applies the LV WAL inline). drainJob, not
                // driveRefreshToQuiescence: the latter drains the WAL queue first, which would
                // apply the TRUNCATE and hide the interleaving.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                drainJob(job);

                // The worker must have stopped AT the TRUNCATE, not walked past it: the view still
                // serves exactly its pre-rebuild rows. Pre-fix it emitted a 4th row here -
                // 2026-01-01T05:00:00Z / 26.5, i.e. avg(1, 2, 3, 100), the bounded frame's ring
                // still holding the three rows the rebuild deleted - and served it while reporting
                // ACTIVE, since the apply job had not yet reached the TRUNCATE to invalidate.
                assertQuery("lv_on_mv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ta\n" +
                        "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                        "2026-01-01T01:00:00.000000Z\ta\t1.5\n" +
                        "2026-01-01T02:00:00.000000Z\ta\t2.0\n");

                // The apply job only now reaches the TRUNCATE, and invalidates.
                drainWalQueue();
            }
            drainWalQueue();

            // The view must not have emitted derived rows for the re-materialised base rows over
            // accumulators still holding pre-rebuild state.
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\tinvalidation_reason\n" +
                            "lv_on_mv\tinvalid\tbase materialized view was rebuilt\n");
            assertQuery("lv_on_mv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ta\n" +
                    "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                    "2026-01-01T01:00:00.000000Z\ta\t1.5\n" +
                    "2026-01-01T02:00:00.000000Z\ta\t2.0\n");
        });
    }

    @Test
    public void testPendingDropRetriesWhileUnrelatedTrafficContinues() throws Exception {
        assertPendingPolicyRetry(true, false);
    }

    @Test
    public void testPendingPolicyRetryPacing() throws Exception {
        assertPendingPolicyRetry(true, true);
    }

    @Test
    public void testPendingSetRetriesWhileUnrelatedTrafficContinues() throws Exception {
        assertPendingPolicyRetry(false, false);
    }

    @Test
    public void testPlainBaseTruncateStillFreezesAndContinues() throws Exception {
        // The mat-view carve-out above must not disturb freeze-and-continue for a plain
        // base: a user TRUNCATE retires settled data, and the view keeps its emitted rows,
        // its accumulators and its ACTIVE status. This is the contract
        // LiveViewFuzzTest#testFuzzRemovalFreezeAndContinue fuzzes.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final String viewSql = "SELECT ts, k, sum(v) OVER (" +
                    "PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s FROM base";
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, k, v) VALUES " +
                        "('2026-01-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-01-01T01:00:00.000000Z', 'a', 2.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                execute("TRUNCATE TABLE base");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Forward ingestion after the removal continues the accumulation as if the
                // removed rows were still there: 1 + 2 + 10 = 13.
                execute("INSERT INTO base (ts, k, v) VALUES ('2026-01-01T02:00:00.000000Z', 'a', 10.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            }
            drainWalQueue();

            assertQuery("SELECT view_name, view_status FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\tview_status\n" +
                            "lv\tactive\n");
            assertQuery("lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("ts\tk\ts\n" +
                    "2026-01-01T00:00:00.000000Z\ta\t1.0\n" +
                    "2026-01-01T01:00:00.000000Z\ta\t3.0\n" +
                    "2026-01-01T02:00:00.000000Z\ta\t13.0\n");
            assertNoRefreshFaults("lv");
        });
    }

    private void assertExpiryConflict() throws Exception {
        assertQuery("SELECT invalidation_reason FROM live_views() WHERE view_name = 'dep'")
                .noLeakCheck().noRandomAccess().returns("""
                        invalidation_reason
                        cannot materialize view 'dep': source materialized view 'source' has an active EXPIRE ROWS policy
                        """);
    }

    private void assertPendingPolicyRetry(boolean isDrop, boolean isCheckPacing) throws Exception {
        assertMemoryLeak(() -> {
            createExpirySourceAndDependents();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:00Z', 'a', 1.0)");
                drainWalAndMatViewQueues();
                driveRefreshToQuiescence(job);
                final LiveViewInstance dep = engine.getLiveViewRegistry().getViewInstance("dep");
                final TableToken source = engine.verifyTableName("source");
                final long processedBefore = dep.getLastProcessedSeqTxn();
                if (isDrop) {
                    execute("INSERT INTO base VALUES ('2026-01-01T00:01:00Z', 'a', 3.0)");
                    drainWalAndMatViewQueues();
                    execute("ALTER MATERIALIZED VIEW source SET EXPIRE ROWS WHEN v < 2");
                    drainWalQueue();
                }
                final CountDownLatch swapBarrierReached = new CountDownLatch(1);
                final CountDownLatch resumeSwap = new CountDownLatch(1);
                final AtomicReference<Throwable> applyError = new AtomicReference<>();
                execute(isDrop ? "ALTER MATERIALIZED VIEW source DROP EXPIRE"
                        : "ALTER MATERIALIZED VIEW source SET EXPIRE ROWS WHEN v < 2");
                TableWriter.setExpiryMetaSwapBarrier(() -> {
                    swapBarrierReached.countDown();
                    try {
                        Assert.assertTrue("timed out resuming policy publication", resumeSwap.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
                // Apply and refresh must not share WAL apply's carrier-local transaction cursor.
                final Thread applyThread = new Thread(() -> {
                    try {
                        drainWalQueue();
                    } catch (Throwable th) {
                        applyError.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "expire-apply");
                try {
                    applyThread.start();
                    Assert.assertTrue("writer did not reach policy publication", swapBarrierReached.await(30, TimeUnit.SECONDS));
                    Assert.assertTrue(engine.getMetadataCache().isExpiryPolicyUpdatePending(source));
                    Assert.assertTrue(job.processNotificationsForTest());
                    Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                    Assert.assertFalse(dep.isInvalid());
                    assertNoRefreshFaults("dep");
                    if (isCheckPacing) {
                        assertPendingRetryPacing(job, dep, processedBefore);
                    }
                } finally {
                    resumeSwap.countDown();
                    applyThread.join(TimeUnit.SECONDS.toMillis(30));
                    TableWriter.setExpiryMetaSwapBarrier(null);
                }
                Assert.assertFalse("policy apply did not finish", applyThread.isAlive());
                Assert.assertNull(applyError.get());
                Assert.assertFalse(engine.getTableSequencerAPI().getTxnTracker(source).isSuspended());
                Assert.assertFalse(engine.getMetadataCache().isExpiryPolicyUpdatePending(source));
                Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(source).getWriterTxn() > processedBefore);
                // Every turn has a real unrelated commit. Retry must not depend on a quiet turn.
                for (int i = 1; i <= 4; i++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    insertNoiseAndRun(job);
                }
                if (isDrop) {
                    Assert.assertFalse(dep.isInvalid());
                    Assert.assertEquals(engine.getTableSequencerAPI().getTxnTracker(source).getWriterTxn(),
                            dep.getLastProcessedSeqTxn());
                    assertQuery("SELECT count() FROM source").noLeakCheck().noRandomAccess().expectSize()
                            .returns("count\n2\n");
                    assertQuery("SELECT s FROM dep ORDER BY ts").noLeakCheck().expectSize()
                            .returns("s\n1.0\n4.0\n");
                } else {
                    Assert.assertTrue("pending SET must invalidate within the busy retry bound", dep.isInvalid());
                    assertExpiryConflict();
                    Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                    assertQuery("SELECT s FROM dep").noLeakCheck().expectSize().returns("s\n1.0\n");
                }
                // SET + DROP changes the metadata version. The existing metadata-drift
                // recovery counts one diagnostic fault when it recompiles the old plan,
                // but consumes no flush retry budget. Pending turns above must count none.
                Assert.assertEquals(isDrop ? 1 : 0, dep.getRefreshFaultCount());
                Assert.assertEquals(0, dep.getFlushRetryCount());
                assertNoRefreshFaults("noise_lv");
            }
        });
    }

    private void assertPendingRetryPacing(LiveViewRefreshJob job, LiveViewInstance dep, long processedBefore) throws Exception {
        final AtomicInteger scans = new AtomicInteger();
        try {
            for (int interval = 1; interval <= 3; interval++) {
                setCurrentMicros(currentMicros + 1_000_000L);
                job.setSimulateBaseCommitBetweenAheadGuardReadsForTest(scans::incrementAndGet);
                insertNoiseAndRun(job);
                Assert.assertEquals(interval, scans.get());
                // A due scan that cannot refresh must still consume its scheduling slot.
                job.setSimulateBaseCommitBetweenAheadGuardReadsForTest(scans::incrementAndGet);
                for (int turn = 0; turn < 100; turn++) {
                    insertNoiseAndRun(job);
                }
                setCurrentMicros(currentMicros + 999_999L);
                insertNoiseAndRun(job);
                Assert.assertEquals(interval, scans.get());
                Assert.assertEquals(processedBefore, dep.getLastProcessedSeqTxn());
                Assert.assertFalse(dep.isInvalid());
                assertNoRefreshFaults("dep");
            }
        } finally {
            job.setSimulateBaseCommitBetweenAheadGuardReadsForTest(null);
        }
        // With no notification work, a pending refresh reports no work rather than spinning.
        drainJob(job);
        Assert.assertFalse(job.processNotificationsForTest());
    }

    private void createExpirySourceAndDependents() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE MATERIALIZED VIEW source AS (SELECT * FROM base)");
        execute("CREATE TABLE noise (ts TIMESTAMP, k SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalAndMatViewQueues();
        setCurrentMicros(0L);
        execute("""
                CREATE LIVE VIEW dep FLUSH EVERY 100ms START FROM NOW AS
                SELECT ts, k, sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s
                FROM source
                """);
        execute("""
                CREATE LIVE VIEW noise_lv FLUSH EVERY 100ms START FROM NOW AS
                SELECT ts, k, sum(v) OVER (PARTITION BY k ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s
                FROM noise
                """);
    }

    private void insertNoiseAndRun(LiveViewRefreshJob job) throws Exception {
        execute("INSERT INTO noise VALUES (" + currentMicros + ", 'a', 1.0)");
        Assert.assertTrue(job.processNotificationsForTest());
    }
}
