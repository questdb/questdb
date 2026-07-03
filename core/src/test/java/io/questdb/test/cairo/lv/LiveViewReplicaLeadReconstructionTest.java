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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.ForwardingLiveViewStateStore;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewStateStore;
import io.questdb.cairo.lv.ReplicaLiveViewStateStore;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Read-only-replica live-view lead reconstruction: a replica reconstructs the primary's un-flushed
 * lead in RAM (its refresh workers run a compute-lead-only pass, gated by
 * {@link ReplicaLiveViewStateStore}). This suite pins the reconciliation of that reconstructed lead
 * against the replicated on-disk tier the global apply job advances asynchronously.
 * <p>
 * The engine wraps its live-view state store in a {@link ForwardingLiveViewStateStore} so a test can
 * flip the machinery between primary (create + ingest) and read-only replica (compute-lead-only)
 * without a real replication cluster. A replicated flush is faked by writing a LIVE_VIEW_DATA block
 * straight to the LV table (what {@code ApplyWal2TableJob} would materialise from replicated WAL) and
 * draining the apply queue, which advances the applied watermark exactly as replication would.
 */
public class LiveViewReplicaLeadReconstructionTest extends AbstractCairoTest {

    private static final String TS1 = "2026-05-12T00:00:01.000000Z";
    private static final String TS2 = "2026-05-12T00:00:02.000000Z";
    private static final String TS3 = "2026-05-12T00:00:03.000000Z";
    private static final String TS4 = "2026-05-12T00:00:04.000000Z";
    private static final String TS5 = "2026-05-12T00:00:05.000000Z";
    private static final String TS6 = "2026-05-12T00:00:06.000000Z";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Wrap the live-view state store so a test can swap the primary machinery for the read-only
        // replica compute-lead-only machinery mid-flight (see switchToReplicaMode).
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            protected LiveViewStateStore createLiveViewStateStore() {
                return new ForwardingLiveViewStateStore(super.createLiveViewStateStore());
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    // A non-BACKFILL view drops rows below its CREATE wall-clock floor; pin the clock below the
    // (2026) test data so every row stays in-frame.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    // Case B reached through an O3 reset. This is the F1 regression: the O3 reset in finishLeadRefresh
    // clears latestSeenTs back to cold start but sets refreshedUpToSeqTxn explicitly, so the next
    // reconcile tick can land in Case B (refreshedUpToSeqTxn < appliedWatermark) with latestSeenTs
    // still unset -- a state the exact-boundary O3 test never produces. Before the fix, Case B's
    // seam-arming guard required latestSeenTs != LONG_NULL, so it left the catch-up seam un-armed; the
    // ensuing cold-start drain (scan from the view lower bound with no seam) re-staged the entire
    // re-derived history as lead on top of the durable disk rows, so size() reported 9 (disk 4 + a
    // 5-row lead) instead of 5. reconcileLeadWithDisk now detects the unseeded loop ahead of the
    // Case B / partial-overlap branches and arms the seam at the on-disk max ts, so the drive-past
    // catch-up keeps the durable band out of the lead and only the genuine TS6 row stays lead.
    //
    // Timeline: batch 1 (TS2, TS4) is reconstructed as a 2-row lead; an O3 row (TS3 < TS4) trips the
    // reset (latestSeenTs -> unset, refreshedUpToSeqTxn -> 2); batch 3 (TS5) applies to the base but
    // the loop does not drive it; the primary's correction flush materialises the corrected
    // TS2..TS5 (rn 1..4) on the LV disk and advances the applied watermark to base seqTxn 3 -- past the
    // loop's frontier of 2, so refreshedUpToSeqTxn(2) < appliedWatermark(3); batch 4 (TS6) is the
    // genuine un-flushed lead above disk.
    @Test
    public void testCaseBAfterO3ResetDoesNotDoubleCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 lands strictly in order; the replica reconstructs it as a 2-row lead over
                // empty disk (accumulator -> 2, frontier at TS4).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS2 + "', 20), ('" + TS4 + "', 40)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 1 reconstructed as a 2-row lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("loop frontier at batch 1", 1, instance.getRefreshedUpToSeqTxn());

                // An out-of-order base row (TS3 < the seen TS4) applies. The replica's lead loop drops
                // the tentative lead WITHOUT rewriting disk, resets the accumulators to identity and
                // clears latestSeenTs, but pins refreshedUpToSeqTxn at the base seqTxn it reached (2).
                execute("INSERT INTO base (ts, x) VALUES ('" + TS3 + "', 30)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("O3 drops the tentative lead", 0, instance.getLeadRowCount());
                Assert.assertEquals("O3 resets the accumulator frontier to cold start",
                        Numbers.LONG_NULL, instance.getLatestSeenTs());
                Assert.assertEquals("O3 pins the loop frontier at the base seqTxn it reached", 2,
                        instance.getRefreshedUpToSeqTxn());

                // Batch 3 (TS5) applies to the base but the loop never drives it (no drainJob), so the
                // primary's correction flush can batch it together with the O3 correction.
                execute("INSERT INTO base (ts, x) VALUES ('" + TS5 + "', 50)");
                drainWalQueue();

                // The primary's correction flush materialises the corrected TS2..TS5 (rn 1..4) on the
                // LV disk and advances the applied watermark to base seqTxn 3 -- PAST the loop's
                // frontier of 2. This is Case B reached with latestSeenTs unset: refreshedUpToSeqTxn(2)
                // < appliedWatermark(3), the disk holds 4 rows, and the loop is cold.
                injectReplicatedFlush(
                        lvToken,
                        3,
                        new String[]{TS2, TS3, TS4, TS5},
                        new int[]{20, 30, 40, 50},
                        new long[]{1, 2, 3, 4}
                );
                Assert.assertEquals("correction flush advanced the applied watermark past the frontier",
                        3, instance.getAppliedWatermark());
                Assert.assertEquals("loop frontier still trails the flush", 2, instance.getRefreshedUpToSeqTxn());

                // Batch 4 (TS6) is the genuine un-flushed lead above disk.
                execute("INSERT INTO base (ts, x) VALUES ('" + TS6 + "', 60)");
                drainWalQueue();

                // Run the replica lead loop: reconcile arms the catch-up seam at the on-disk max ts
                // (TS5) because the loop is cold over non-empty disk, and the re-derive drives the
                // accumulator over the durable TS2..TS5 band WITHOUT staging it, then stages only TS6
                // as the genuine lead (rn 5). Before the fix the seam stayed unarmed and all 5 re-derived
                // rows were staged as lead.
                drainJob(job);
                Assert.assertEquals("only the genuine TS6 row stays lead, not the whole re-derived history",
                        1, instance.getLeadRowCount());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:02.000000Z\t20\t1
                    2026-05-12T00:00:03.000000Z\t30\t2
                    2026-05-12T00:00:04.000000Z\t40\t3
                    2026-05-12T00:00:05.000000Z\t50\t4
                    2026-05-12T00:00:06.000000Z\t60\t5
                    """;
            // Content is exact global rn 1..5 (no re-staged durable rows) ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() is exact: disk (4) + the genuine 1-row lead, NOT disk (4) + a lead that
            // re-counts the 4 durable rows (which reported 9 before the fix). The O3-reset catch-up
            // seam is what keeps size() at 5.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() must not double-count the re-derived durable band", 5, cursor.size());
            }
        });
    }

    // Case B (disk outran the loop). The replica reconstructs batch 1 as an un-flushed lead over
    // empty disk, then a replicated flush materialises batches 1+2 on the LV disk and advances the
    // applied watermark past the loop's frontier (refreshedUpToSeqTxn < appliedWatermark) while the
    // window accumulators still sit at batch 1. A plain drain-forward would re-scan the now-durable
    // batch-2 band (ts > latestSeenTs) and re-stage it as lead, so size() would double-count rows
    // disk already holds (8 instead of 6). reconcileLeadWithDisk arms the catch-up seam, and the
    // drain drives the row_number() accumulator over batch 2 without staging it, then stages only the
    // genuine batch-3 lead above disk -- so the reconstructed read is exactly disk + genuine lead,
    // 6 rows with global rn 1..6 and size() == 6.
    @Test
    public void testCaseBDiskOutranLoopDoesNotDoubleCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1 lands on the base; the replica reconstructs it as an un-flushed lead over
                // empty disk (row_number() accumulator -> 2, refreshedUpToSeqTxn -> 1).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS1 + "', 10), ('" + TS2 + "', 20)");
                drainWalQueue();
                drainJob(job);

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals("batch 1 reconstructed as a 2-row lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("loop frontier at batch 1", 1, instance.getRefreshedUpToSeqTxn());

                // Batch 2 lands on the base but the loop does NOT drain it (no drainJob), so the
                // accumulators stay at batch 1.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS3 + "', 30), ('" + TS4 + "', 40)");
                drainWalQueue();

                // A replicated flush materialises batches 1+2 on the LV disk (rn 1..4) and advances
                // the applied watermark to base seqTxn 2 -- past the loop's frontier (1). This is
                // Case B: refreshedUpToSeqTxn(1) < appliedWatermark(2), latestSeenTs(TS2) < disk max
                // (TS4), and the accumulators trail disk by batch 2.
                injectReplicatedFlush(
                        lvToken,
                        2,
                        new String[]{TS1, TS2, TS3, TS4},
                        new int[]{10, 20, 30, 40},
                        new long[]{1, 2, 3, 4}
                );
                Assert.assertEquals("replicated flush advanced the applied watermark", 2, instance.getAppliedWatermark());

                // Batch 3 is the genuine un-flushed lead above disk.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS5 + "', 50), ('" + TS6 + "', 60)");
                drainWalQueue();

                // Run the replica lead loop: it catches the accumulators up over batch 2 (on disk, not
                // staged) and reconstructs batch 3 as the genuine lead (rn 5, 6).
                drainJob(job);
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:01.000000Z\t10\t1
                    2026-05-12T00:00:02.000000Z\t20\t2
                    2026-05-12T00:00:03.000000Z\t30\t3
                    2026-05-12T00:00:04.000000Z\t40\t4
                    2026-05-12T00:00:05.000000Z\t50\t5
                    2026-05-12T00:00:06.000000Z\t60\t6
                    """;
            // Content is exact (no duplicate batch-2 rows, global rn 1..6) ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() is exact: disk (4) + the genuine 2-row lead, NOT disk (4) + a lead that
            // re-counts the 2 durable batch-2 rows. The seam catch-up is what keeps size() at 6.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() must not double-count the durable batch-2 band", 6, cursor.size());
            }
        });
    }

    // Cold start (the replica boots at a non-zero applied watermark). The replica comes up with batch
    // 1 already flushed on the LV disk (rn 1, 2) but has never driven a base commit through the window
    // pipeline this session, so latestSeenTs is unset, the row_number() accumulator sits at identity,
    // and refreshedUpToSeqTxn has never been computed. When batch 2 arrives as the genuine un-flushed
    // lead, a plain drain scans the whole history from the view lower bound (correctly re-seeding the
    // accumulator) but would ALSO stage the already-durable batch-1 band as lead, so size() would
    // double-count it (6 instead of 4). reconcileLeadWithDisk detects the unseeded loop over a
    // non-empty disk and arms the catch-up seam at the on-disk max ts, so the drain drives the
    // accumulator over batch 1 without staging it, then stages only batch 2 as the genuine lead (rn 3,
    // 4) -- the reconstructed read is exactly disk + genuine lead, 4 rows with global rn 1..4 and
    // size() == 4.
    @Test
    public void testColdStartSeedsAccumulatorsWithoutDoubleCounting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 lands on the base and applies (the base table now reflects it) but the
                // replica lead loop never runs over it -- no drainJob -- so the window accumulator
                // stays at identity and refreshedUpToSeqTxn is never computed. This is the cold-start
                // gap: the replica boots into a state it never drove through the pipeline.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS1 + "', 10), ('" + TS2 + "', 20)");
                drainWalQueue();

                // A replicated flush materialises batch 1 on the LV disk (rn 1, 2) and advances the
                // applied watermark to base seqTxn 1 -- exactly the state a freshly booted replica sees.
                injectReplicatedFlush(
                        lvToken,
                        1,
                        new String[]{TS1, TS2},
                        new int[]{10, 20},
                        new long[]{1, 2}
                );
                Assert.assertEquals("replicated flush advanced the applied watermark", 1, instance.getAppliedWatermark());
                Assert.assertEquals("no lead reconstructed yet", 0, instance.getLeadRowCount());
                Assert.assertEquals("loop never computed a frontier -> falls back to the applied point",
                        1, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals("accumulators unseeded (cold start)", Numbers.LONG_NULL, instance.getLatestSeenTs());

                // Batch 2 is the genuine un-flushed lead above disk.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS3 + "', 30), ('" + TS4 + "', 40)");
                drainWalQueue();

                // Run the replica lead loop for the first time: it seeds the row_number() accumulator
                // over batch 1 (on disk, not staged) and reconstructs batch 2 as the genuine lead.
                drainJob(job);

                Assert.assertEquals("batch 2 reconstructed as a 2-row lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("loop frontier advanced to batch 2", 2, instance.getRefreshedUpToSeqTxn());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:01.000000Z\t10\t1
                    2026-05-12T00:00:02.000000Z\t20\t2
                    2026-05-12T00:00:03.000000Z\t30\t3
                    2026-05-12T00:00:04.000000Z\t40\t4
                    """;
            // Content is exact (global rn 1..4, no duplicate batch-1 rows) ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() is exact: disk (2) + the genuine 2-row lead, NOT disk (2) + a lead that
            // re-counts the 2 durable batch-1 rows. The cold-start seam is what keeps size() at 4.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() must not double-count the durable batch-1 band", 4, cursor.size());
            }
        });
    }

    // Exact boundary (a replicated flush lands precisely the lead). The replica reconstructs batch 1 as
    // an un-flushed lead over empty disk (row_number() 1, 2). A replicated flush then materialises
    // EXACTLY that batch on the LV disk (rn 1, 2) and advances the applied watermark to the same base
    // seqTxn the loop already reached, so refreshedUpToSeqTxn == appliedWatermark with the 2-row lead
    // still published. The slot rows are now the just-flushed disk rows, so reconcileLeadWithDisk takes
    // the exact-boundary branch: it re-stamps the slot as a disk subset (restampSlotAfterFlush) and drops
    // the lead to 0, arming no catch-up seam because the accumulators already sit at disk. size() must
    // then equal disk alone (2), not disk plus a lead that re-counts the same 2 durable rows (4).
    @Test
    public void testExactBoundaryFlushRestampsSlotWithoutDoubleCounting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 lands and the replica reconstructs it as a 2-row lead over empty disk
                // (accumulator -> 2, frontier at base seqTxn 1).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS1 + "', 10), ('" + TS2 + "', 20)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 1 reconstructed as a 2-row lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("loop frontier at batch 1", 1, instance.getRefreshedUpToSeqTxn());

                // A replicated flush materialises EXACTLY batch 1 on the LV disk (rn 1, 2) and advances the
                // applied watermark to base seqTxn 1 -- the same point the loop reached. This is the exact
                // boundary: refreshedUpToSeqTxn(1) == appliedWatermark(1), with the 2-row lead still
                // published (the flush advanced disk on a different path than this loop).
                injectReplicatedFlush(
                        lvToken,
                        1,
                        new String[]{TS1, TS2},
                        new int[]{10, 20},
                        new long[]{1, 2}
                );
                Assert.assertEquals("flush advanced the applied watermark to the loop frontier",
                        1, instance.getAppliedWatermark());
                Assert.assertEquals("loop frontier unchanged", 1, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals("lead still published before reconcile", 2, instance.getLeadRowCount());

                // Run the replica lead loop: isLeadSlotStale fires (the on-disk seqTxn advanced past the
                // slot's stamp), so reconcile takes the exact-boundary branch and re-stamps the slot as a
                // disk subset, dropping the lead to 0.
                drainJob(job);
                Assert.assertEquals("exact-boundary flush drops the lead to zero", 0, instance.getLeadRowCount());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:01.000000Z\t10\t1
                    2026-05-12T00:00:02.000000Z\t20\t2
                    """;
            // Content is exactly the flushed disk rows (rn 1, 2) ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() equals disk alone: the re-stamped slot carries a zero lead, so the 2 durable
            // rows are not re-counted (before the re-stamp size() would report 4).
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertEquals("size() equals disk with the lead dropped to zero", 2, cursor.size());
            }
        });
    }

    // Partial-overlap (a replicated flush covers only a prefix of the lead). The replica drives BOTH
    // batch 1 and batch 2 through the window pipeline in one pass, reconstructing a 4-row lead over empty
    // disk (row_number() 1..4, frontier at base seqTxn 2). A replicated flush then materialises only
    // batch 1 on the LV disk (rn 1, 2) and advances the applied watermark to base seqTxn 1, leaving a
    // genuine un-flushed remainder above it: appliedWatermark(1) < refreshedUpToSeqTxn(2). reconcile takes
    // the partial-overlap branch: it binary-searches the ts-ascending slot for the first row above the
    // on-disk max ts (TS2), so the batch-1 prefix (TS1, TS2) counts as durable overlap and only the
    // batch-2 remainder (TS3, TS4) stays lead. The slot is re-stamped to the reader's seqTxn with the
    // trimmed 2-row lead, so size() is disk (2) + the genuine remainder (2) == 4, not the whole 4-row lead
    // on top of the 2 durable rows (6).
    @Test
    public void testPartialOverlapFlushTrimsDurablePrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 and batch 2 land as two separate base commits; a single lead pass drives BOTH
                // through the window pipeline, reconstructing a 4-row lead over empty disk (accumulator ->
                // 4, frontier at base seqTxn 2).
                execute("INSERT INTO base (ts, x) VALUES ('" + TS1 + "', 10), ('" + TS2 + "', 20)");
                drainWalQueue();
                execute("INSERT INTO base (ts, x) VALUES ('" + TS3 + "', 30), ('" + TS4 + "', 40)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("both batches reconstructed as a 4-row lead", 4, instance.getLeadRowCount());
                Assert.assertEquals("loop frontier advanced past both batches", 2, instance.getRefreshedUpToSeqTxn());

                // A replicated flush materialises only batch 1 on the LV disk (rn 1, 2) and advances the
                // applied watermark to base seqTxn 1 -- a prefix of the lead. This is partial-overlap:
                // appliedWatermark(1) < refreshedUpToSeqTxn(2), with batch 2 still a genuine un-flushed
                // remainder above disk.
                injectReplicatedFlush(
                        lvToken,
                        1,
                        new String[]{TS1, TS2},
                        new int[]{10, 20},
                        new long[]{1, 2}
                );
                Assert.assertEquals("flush advanced the applied watermark to the prefix",
                        1, instance.getAppliedWatermark());
                Assert.assertEquals("loop frontier still spans both batches", 2, instance.getRefreshedUpToSeqTxn());

                // Run the replica lead loop: reconcile binary-searches the slot for the first row above the
                // on-disk max ts (TS2), so the now-durable batch-1 prefix drops out and only batch 2 stays
                // lead.
                drainJob(job);
                Assert.assertEquals("partial-overlap trims the durable prefix, keeping batch 2 as lead",
                        2, instance.getLeadRowCount());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:01.000000Z\t10\t1
                    2026-05-12T00:00:02.000000Z\t20\t2
                    2026-05-12T00:00:03.000000Z\t30\t3
                    2026-05-12T00:00:04.000000Z\t40\t4
                    """;
            // Content is exact global rn 1..4: the batch-1 prefix served from disk, the batch-2 remainder
            // from the trimmed lead ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() is disk (2) + the genuine 2-row remainder, NOT disk (2) + the whole 4-row
            // lead (which would report 6). The binary-search seam split is what keeps size() at 4.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() must not double-count the durable prefix", 4, cursor.size());
            }
        });
    }

    // Out-of-order via a REPLACE_RANGE delete band on a replica (the D6 ghost-row shape reached through
    // the replica lead loop). The replica reconstructs batch 1 (TS1, TS2, TS3 in order) as an un-flushed
    // lead over empty disk, driving the row_number() accumulator to 3 with the frontier at TS3. A
    // non-dedup REPLACE_RANGE commit then deletes [TS2, TS5) from the base but its only inserted row
    // (TS4) sits ABOVE the frontier, so the raw event min ts reads TS4 > TS3 and hides the deletion.
    // Only the delete-band substitution catches it: drainAppliedBaseForLead clamps the range low (TS2)
    // to the view lower bound and treats it as the batch minimum, TS2 <= the frontier fires the o3
    // hatch, and finishLeadRefresh's replica hatch drops the tentative lead WITHOUT rewriting disk.
    // Before the fix the drain compared only getMinTimestamp() (TS4), missed the overlap, and
    // forward-appended TS4 on top of the now-deleted TS2, TS3 rows -- a stale lead the replica would
    // serve until the primary's replicated correction subsumed the band.
    //
    // The primary's o3Replay correction then lands as a replicated flush (post-replace base TS1, TS4 as
    // rn 1, 2); a later in-order batch (TS6) reconstructs the genuine lead above disk (rn 3), so the
    // replica converges to the exact post-replace result with global rn 1..3.
    @Test
    public void testReplicaConvergesAfterReplaceRangeDeletingBelowFrontier() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);
            final TableToken baseToken = engine.verifyTableName("base");

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 lands strictly in order; the replica reconstructs it as a 3-row lead over empty
                // disk (accumulator -> 3, frontier at TS3).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS1 + "', 10), ('" + TS2 + "', 20), ('" + TS3 + "', 30)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 1 reconstructed as a 3-row lead", 3, instance.getLeadRowCount());
                Assert.assertEquals("frontier advanced past the seen rows",
                        MicrosFormatUtils.parseUTCTimestamp(TS3), instance.getLatestSeenTs());

                // A REPLACE_RANGE commit deletes [TS2, TS5) from the base but its only inserted row (TS4)
                // sits ABOVE the frontier (TS3). getMinTimestamp() reads TS4 > TS3, so only the delete-band
                // substitution (clamped range low TS2 <= TS3) fires the overlap. The replica drops the
                // tentative lead WITHOUT rewriting disk and serves disk-only.
                try (WalWriter walWriter = engine.getWalWriter(baseToken)) {
                    TableWriter.Row row = walWriter.newRow(MicrosFormatUtils.parseUTCTimestamp(TS4));
                    row.putInt(1, 40);
                    row.append();
                    walWriter.commitWithParams(
                            MicrosFormatUtils.parseUTCTimestamp(TS2),
                            MicrosFormatUtils.parseUTCTimestamp(TS5),
                            WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
                    );
                }
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("the delete-band overlap drops the tentative lead",
                        0, instance.getLeadRowCount());
                Assert.assertEquals("O3 resets the accumulator frontier to cold start",
                        Numbers.LONG_NULL, instance.getLatestSeenTs());
                try (TableReader lvReader = engine.getReader(lvToken)) {
                    Assert.assertEquals("the replica never rewrites its own LV disk on O3", 0, lvReader.size());
                }

                // The primary's o3Replay correction lands as a replicated flush: the post-replace base
                // (TS1, TS4) materialises on the LV disk as rn 1, 2 and the applied watermark reaches base
                // seqTxn 2 (the replace commit).
                injectReplicatedFlush(
                        lvToken,
                        2,
                        new String[]{TS1, TS4},
                        new int[]{10, 40},
                        new long[]{1, 2}
                );
                Assert.assertEquals("correction advanced the applied watermark", 2, instance.getAppliedWatermark());
                drainJob(job);

                // A later in-order batch (TS6) is the genuine un-flushed lead above the corrected disk. The
                // replica re-seeds the accumulator over the corrected history (rn 1, 2, on disk, not staged)
                // and stages TS6 as the lead (rn 3).
                execute("INSERT INTO base (ts, x) VALUES ('" + TS6 + "', 60)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 3 reconstructed as a 1-row lead above disk",
                        1, instance.getLeadRowCount());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:01.000000Z\t10\t1
                    2026-05-12T00:00:04.000000Z\t40\t2
                    2026-05-12T00:00:06.000000Z\t60\t3
                    """;
            // Content is exactly the post-replace base numbered global rn 1..3 -- the delete-band detection
            // kept the deleted TS2, TS3 rows out of the reconstructed lead ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() is exact: disk (2) + the genuine 1-row lead, with no ghost rows.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() equals disk plus the genuine lead", 3, cursor.size());
            }
        });
    }

    // Partitioned + anchored lead reconstruction. A snapshot-capable window that both PARTITIONs BY a
    // key and carries an ANCHOR clause used to serve disk-only on a replica: its stalled-publish
    // rollback needs to round-trip the per-partition function maps and the anchor bucket map, which the
    // old scalar-only rollback could not do, so the reconstruction gate rejected these shapes. With the
    // rollback extended to snapshot+restore the anchor window and partition maps (the in-RAM analog of a
    // head-checkpoint write+restore), the replica now reconstructs the un-flushed lead for them too. This
    // pins that: the replica rebuilds all 4 rows as an un-flushed lead over empty disk, maintaining the
    // per-partition, per-day row_number() maps (sym A: 1, 2; sym B: 1, 2) rather than a global 1..4.
    @Test
    public void testReplicaReconstructsPartitionedAnchoredLead() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT sym, x, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Two partitions interleaved in one base commit. Per-partition, per-day row numbers:
                //   sym A: TS1 -> 1, TS3 -> 2 ; sym B: TS2 -> 1, TS4 -> 2.
                execute("INSERT INTO base (sym, x, ts) VALUES " +
                        "('A', 10, '" + TS1 + "'), ('B', 20, '" + TS2 + "'), " +
                        "('A', 30, '" + TS3 + "'), ('B', 40, '" + TS4 + "')");
                drainWalQueue();

                // The replica lead loop reconstructs all 4 rows as an un-flushed lead over empty disk,
                // driving the anchor bucket map and the per-partition row_number() maps through the same
                // window pipeline the primary uses. Before the gate was lifted this returned early
                // (disk-only), leaving leadRowCount == 0.
                drainJob(job);

                Assert.assertEquals("partitioned + anchored view reconstructed as a 4-row lead",
                        4, instance.getLeadRowCount());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    sym\tx\tts\trn
                    A\t10\t2026-05-12T00:00:01.000000Z\t1
                    B\t20\t2026-05-12T00:00:02.000000Z\t1
                    A\t30\t2026-05-12T00:00:03.000000Z\t2
                    B\t40\t2026-05-12T00:00:04.000000Z\t2
                    """;
            // Content is exact per-partition numbering, proving the partition maps (not a single global
            // counter) drove the reconstruction ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and the read routes through the reconstructed lead (empty disk + 4-row lead).
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() equals the reconstructed lead over empty disk", 4, cursor.size());
            }
        });
    }

    // Out-of-order base commit on a replica. The replica reconstructs batch 1 (strictly increasing ts)
    // as an un-flushed lead over empty disk, driving the row_number() accumulator to 2. Then a base row
    // lands below the frontier (TS3 < the seen TS4): the primary rewrites its LV disk via o3Replay and
    // replicates the REPLACE_RANGE, which the replica must NOT do (read-only). drainAppliedBaseForLead
    // detects the overlap off the WAL-E event min-ts and signals o3Detected, so finishLeadRefresh's
    // replica hatch drops the tentative lead, resets the window accumulators to identity and clears
    // latestSeenTs (the O3 reordered rows the accumulators already counted, so a forward-only re-derive
    // would keep drifting), and serves disk-only. The corrected rows then land as a replicated flush;
    // once a later in-order batch arrives, the replica re-seeds the accumulator over the whole corrected
    // history (rn 1..3 on disk, not staged) and stages only the genuine lead above disk (rn 4, 5) -- so
    // the reconstructed read is exactly disk + genuine lead, global rn 1..5, with no drift.
    //
    // Without the accumulator reset the row_number() counter would still sit at 2 after the O3, so the
    // re-derived batch-3 lead would number TS5, TS6 as rn 3, 4 (duplicating the disk's rn 3 and never
    // reaching rn 5) instead of rn 4, 5.
    @Test
    public void testReplicaReconstructsLeadAfterOutOfOrderBaseCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 lands strictly in order; the replica reconstructs it as a 2-row lead over
                // empty disk (accumulator -> 2, frontier at TS4).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS2 + "', 20), ('" + TS4 + "', 40)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 1 reconstructed as a 2-row lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("frontier advanced past the seen row",
                        MicrosFormatUtils.parseUTCTimestamp(TS4), instance.getLatestSeenTs());

                // An out-of-order base row (TS3 < the seen TS4) applies to the base table. The replica's
                // lead loop detects the overlap and drops the tentative lead WITHOUT rewriting disk.
                execute("INSERT INTO base (ts, x) VALUES ('" + TS3 + "', 30)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("O3 drops the tentative lead", 0, instance.getLeadRowCount());
                Assert.assertEquals("O3 resets the accumulator frontier to cold start",
                        Numbers.LONG_NULL, instance.getLatestSeenTs());
                try (TableReader lvReader = engine.getReader(lvToken)) {
                    Assert.assertEquals("the replica never rewrites its own LV disk on O3", 0, lvReader.size());
                }

                // The primary's o3Replay correction lands as a replicated flush: the LV disk now holds
                // the corrected TS2, TS3, TS4 (rn 1, 2, 3) and the applied watermark reaches base
                // seqTxn 2.
                injectReplicatedFlush(
                        lvToken,
                        2,
                        new String[]{TS2, TS3, TS4},
                        new int[]{20, 30, 40},
                        new long[]{1, 2, 3}
                );
                Assert.assertEquals("correction advanced the applied watermark", 2, instance.getAppliedWatermark());
                drainJob(job);

                // Batch 3 is the genuine un-flushed lead above the corrected disk. The replica re-seeds
                // the accumulator over the corrected history (rn 1..3, on disk, not staged) and stages
                // TS5, TS6 as the lead (rn 4, 5) -- not rn 3, 4 as a drifted counter would.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('" + TS5 + "', 50), ('" + TS6 + "', 60)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 3 reconstructed as a 2-row lead above disk", 2, instance.getLeadRowCount());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:02.000000Z\t20\t1
                    2026-05-12T00:00:03.000000Z\t30\t2
                    2026-05-12T00:00:04.000000Z\t40\t3
                    2026-05-12T00:00:05.000000Z\t50\t4
                    2026-05-12T00:00:06.000000Z\t60\t5
                    """;
            // Content is exact global rn 1..5 -- the accumulator reset kept the re-derived lead in step
            // with disk's numbering rather than duplicating rn 3 ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and size() is exact: disk (3) + the genuine 2-row lead.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() equals disk plus the genuine lead", 5, cursor.size());
            }
        });
    }

    // Replica stalled-publish rollback. On a read-only replica the lead publish can fail (both tier
    // slots reader-pinned, or a mid-swap error): the primary would flush the drained rows straight to
    // disk, but the replica cannot, so finishLeadRefresh rolls the window state back to its pre-drain
    // snapshot (restoreLeadRollback) and leaves refreshedUpToSeqTxn where it was, so the next tick
    // re-drains the exact same range and re-produces identical output. This pins that capture/restore
    // round-trip via the setFailNextPublishSwap hook (the same one the primary-path emergency-flush
    // smoke test uses) in replica mode; growth.bytes = 0 forces the slow-path swap the injection fires on.
    //
    // The rollback is load-bearing: without it the stalled drain's forward advance to the accumulator
    // (row_number() -> 4) and latestSeenTs (-> TS4) would persist while refreshedUpToSeqTxn stayed at 1,
    // so the re-drain would scan ts > TS4, find nothing, and lose batch 2 permanently (leaving only rn
    // 1, 2). With the rollback the re-drain reconstructs the identical rn 1..4.
    @Test
    public void testReplicaStalledPublishRollsBackAndRedrains() throws Exception {
        // The publishSwap injection fires only on the slow-path swap; growth.bytes = 0 forces it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final TableToken lvToken = engine.getTableTokenIfExists("lv");
            Assert.assertNotNull(lvToken);

            final LiveViewStateStore primaryStore = switchToReplicaMode();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);

                // Batch 1 lands strictly in order; the replica reconstructs it as a 2-row lead over empty
                // disk (accumulator -> 2, frontier at TS2, loop frontier at base seqTxn 1).
                execute("INSERT INTO base (ts, x) VALUES ('" + TS1 + "', 10), ('" + TS2 + "', 20)");
                drainWalQueue();
                drainJob(job);
                Assert.assertEquals("batch 1 reconstructed as a 2-row lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("loop frontier at batch 1", 1, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals("frontier advanced past the seen row",
                        MicrosFormatUtils.parseUTCTimestamp(TS2), instance.getLatestSeenTs());

                // Batch 2 applies to the base. Arm a one-shot publishSwap failure so the batch-2 lead
                // publish stalls on the slow-path swap.
                execute("INSERT INTO base (ts, x) VALUES ('" + TS3 + "', 30), ('" + TS4 + "', 40)");
                drainWalQueue();
                final LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull("tier allocated after the first reconstruction", tier);
                tier.setFailNextPublishSwap(new RuntimeException("test: simulated replica publish stall"));

                // Single-step the stalled tick (drainJob would loop straight past it into the recovery,
                // since the hook self-clears). The drain advances the accumulators over batch 2 and
                // captures the pre-drain snapshot, the publish throws, and finishLeadRefresh's replica
                // hatch rolls the window state back WITHOUT rewriting disk, leaving the loop frontier
                // unadvanced.
                Assert.assertTrue("stalled tick did work", job.run());
                Assert.assertEquals("stalled publish does not grow the lead", 2, instance.getLeadRowCount());
                Assert.assertEquals("stalled publish leaves the loop frontier unadvanced",
                        1, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals("rollback restores the pre-drain frontier",
                        MicrosFormatUtils.parseUTCTimestamp(TS2), instance.getLatestSeenTs());
                try (TableReader lvReader = engine.getReader(lvToken)) {
                    Assert.assertEquals("the replica never flushes the stalled lead to disk", 0, lvReader.size());
                }

                // The recovery tick re-drains the identical batch-2 range (the hook self-cleared) and
                // publishes the lead, re-producing the same row numbers and frontier a first, un-stalled
                // publish would have.
                drainJob(job);
                Assert.assertEquals("recovery re-drains batch 2 as the genuine lead", 4, instance.getLeadRowCount());
                Assert.assertEquals("recovery advances the loop frontier past batch 2",
                        2, instance.getRefreshedUpToSeqTxn());
                Assert.assertEquals("recovery frontier matches an un-stalled publish",
                        MicrosFormatUtils.parseUTCTimestamp(TS4), instance.getLatestSeenTs());
            } finally {
                switchToStore(primaryStore);
            }

            final String expected = """
                    ts\tx\trn
                    2026-05-12T00:00:01.000000Z\t10\t1
                    2026-05-12T00:00:02.000000Z\t20\t2
                    2026-05-12T00:00:03.000000Z\t30\t3
                    2026-05-12T00:00:04.000000Z\t40\t4
                    """;
            // Content is exact global rn 1..4 -- the rollback kept the re-drain in step, so no batch-2 row
            // was lost and none was double-numbered ...
            StringSink actual = new StringSink();
            printSql("SELECT * FROM lv ORDER BY ts", actual);
            Assert.assertEquals(expected, actual.toString());

            // ... and the read routes through the reconstructed lead over empty disk (size 4).
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("reconstructed lead must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() equals the reconstructed lead over empty disk", 4, cursor.size());
            }
        });
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Fakes a replicated flush: writes a LIVE_VIEW_DATA block straight to the LV table (the rows
    // ApplyWal2TableJob would materialise from replicated WAL) with the in-band base watermark, then
    // drains the apply queue so the replica-mode apply job applies the block and advances the LV's
    // applied watermark to maxBaseSeqTxn.
    private static void injectReplicatedFlush(TableToken lvToken, long maxBaseSeqTxn, String[] tsUtc, int[] xVals, long[] rnVals) {
        try (WalWriter w = engine.getWalWriter(lvToken)) {
            for (int i = 0; i < tsUtc.length; i++) {
                TableWriter.Row row = w.newRow(MicrosFormatUtils.parseUTCTimestamp(tsUtc[i]));
                row.putInt(1, xVals[i]);
                row.putLong(2, rnVals[i]);
                row.append();
            }
            w.commitLiveView(maxBaseSeqTxn);
        } catch (Exception e) {
            throw new AssertionError("could not inject replicated flush", e);
        }
        drainWalQueue();
    }

    private static LiveViewRecordCursor openLvCursor(RecordCursorFactory factory) throws SqlException {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
        return (LiveViewRecordCursor) f.getCursor(sqlExecutionContext);
    }

    private static void switchToStore(LiveViewStateStore store) {
        ((ForwardingLiveViewStateStore) engine.getLiveViewStateStore()).swapDelegate(store);
    }

    // Swaps the read-only-replica compute-lead-only store in and returns the primary store to restore.
    private static LiveViewStateStore switchToReplicaMode() {
        ForwardingLiveViewStateStore fwd = (ForwardingLiveViewStateStore) engine.getLiveViewStateStore();
        return fwd.swapDelegate(ReplicaLiveViewStateStore.INSTANCE);
    }
}
