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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.ForwardingLiveViewStateStore;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewStateStore;
import io.questdb.cairo.lv.ReplicaLiveViewStateStore;
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
