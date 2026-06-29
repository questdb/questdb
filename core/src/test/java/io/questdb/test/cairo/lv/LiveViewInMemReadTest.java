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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Phase-3a in-memory-tier read path.
 * <p>
 * Step 1 (fence): a query may serve the in-mem tier (rather than disk) for a
 * slot solely when the slot's stamped LV-table seqTxn matches the disk reader's
 * seqTxn. The {@code testFence*} tests pin the fence predicate
 * (LiveViewRecordCursor.isRoutingEligible) and the stamp coordinate.
 * <p>
 * Step 2 (Mode B routing): when the fence holds the cursor serves disk rows with
 * {@code ts < seamTs} and the entire pinned slot for {@code ts >= seamTs}. The
 * {@code testModeB*} tests assert the tier actually serves rows (via the
 * in-mem-rows-served counter) and run a differential oracle - the same SELECT
 * must return byte-identical results with the tier on (Mode B) and forced off
 * (disk-only) - across full scans, LIMIT, WHERE, ORDER BY, a seam split, the
 * rowId round-trip, and a toTop re-read.
 */
public class LiveViewInMemReadTest extends AbstractCairoTest {

    // A non-BACKFILL view drops rows below its CREATE wall-clock floor; pin the
    // clock below the (2026) test data so every row stays in-frame.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testFenceEligibleAndStampMatchesReaderSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("tier must be allocated after refresh", tier);
            LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
            Assert.assertTrue("published slot must hold rows", slot.rowCount() > 0);

            // The stamp source (sequencer writerTxn) must equal what a query
            // reader reports via getSeqTxn() - this is what the fence compares.
            try (TableReader reader = engine.getReader(instance.getLiveViewToken())) {
                Assert.assertEquals(reader.getSeqTxn(), slot.lvSeqTxn());
            }

            // Aligned identity read: same LV-table version on both sides.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("aligned identity read must be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    @Test
    public void testFenceNotEligibleForPrunedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            // Pruning the timestamp leaves no full-schema identity projection, so
            // the in-mem tier cannot be addressed -> disk-only.
            try (
                    RecordCursorFactory factory = select("SELECT rn FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("pruned projection must not be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    @Test
    public void testFenceNotEligibleOnSeqTxnMismatch() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
            Assert.assertTrue(slot.rowCount() > 0);

            // Force a mismatch: stamp the slot with a seqTxn the reader cannot
            // report. The fence must fall back to disk-only.
            slot.setLvSeqTxn(slot.lvSeqTxn() + 1000);
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("seqTxn mismatch must not be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    @Test
    public void testInMemRowIdRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createSeamSplitLv();
            try (RecordCursorFactory factory = select("SELECT * FROM lv")) {
                LiveViewRecordCursorFactory lvf = unwrapLvFactory(factory);
                try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvf.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("seam-split read must be Mode B", cursor.isRoutingEligible());
                    RecordMetadata md = lvf.getMetadata();
                    Record record = cursor.getRecord();

                    // Forward pass: capture each row's id and printed content.
                    // Disk rows (below the seam) carry non-negative ids; in-mem
                    // rows (at/above the seam) carry the sign-bit-tagged id.
                    LongList rowIds = new LongList();
                    ObjList<String> forwardRows = new ObjList<>();
                    int diskRowIds = 0;
                    int inMemRowIds = 0;
                    while (cursor.hasNext()) {
                        long rowId = record.getRowId();
                        if (rowId < 0) {
                            inMemRowIds++;
                        } else {
                            diskRowIds++;
                        }
                        rowIds.add(rowId);
                        StringSink rowSink = new StringSink();
                        TestUtils.println(record, md, rowSink);
                        forwardRows.add(rowSink.toString());
                    }
                    Assert.assertTrue("expected disk-below-seam rows", diskRowIds > 0);
                    Assert.assertTrue("expected in-mem rows", inMemRowIds > 0);

                    // Random-access each captured id via recordB; the round-trip
                    // must reproduce the forward row exactly for both tiers.
                    Record recordB = cursor.getRecordB();
                    for (int i = 0, n = rowIds.size(); i < n; i++) {
                        cursor.recordAt(recordB, rowIds.getQuick(i));
                        StringSink rowSink = new StringSink();
                        TestUtils.println(recordB, md, rowSink);
                        Assert.assertEquals("rowId round-trip mismatch at row " + i, forwardRows.get(i), rowSink.toString());
                    }
                }
            }
        });
    }

    @Test
    public void testModeBDisabledForBackwardScan() throws Exception {
        assertMemoryLeak(() -> {
            createSeamSplitLv();
            // ORDER BY ts DESC pushes a backward scan into the base. Mode B's
            // seam split assumes ascending ts, so the cursor must route disk-only
            // here (otherwise it would drop the disk rows below the seam).
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv ORDER BY ts DESC");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("backward scan must not be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    @Test
    public void testModeBDisabledForIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            createSeamSplitLv();
            // A WHERE on the designated timestamp pushes an interval into the disk
            // scan, so the disk side returns only a sub-range while the slot stays
            // unfiltered. Mode B must route disk-only, or it would over-return the
            // rows the interval excludes.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv WHERE ts >= '2023-11-14T22:13:25.000000Z'");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("interval-filtered scan must not be routing-eligible", cursor.isRoutingEligible());
            }
            // The disk-only path returns exactly the in-interval rows.
            assertQuery("SELECT ts, x FROM lv WHERE ts >= '2023-11-14T22:13:25.000000Z' ORDER BY ts")
                    .timestamp("ts")
                    .returns("ts\tx\n" +
                            "2023-11-14T22:13:25.000001Z\t4\n" +
                            "2023-11-14T22:13:25.000002Z\t5\n");
        });
    }

    @Test
    public void testModeBEnabledForSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, g, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, g) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 'aa'), " +
                    "('2026-05-12T00:00:00.000002Z', 'bb')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNotNull("tier allocated for SYMBOL schemas", instance.getInMemoryTier());

            // The refresh worker rewrote the tier's segment-local symbol ids with
            // LV-table-space ids after apply, so Mode B engages and the in-mem
            // branch resolves the SYMBOL against the disk reader's symbol table.
            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("SYMBOL output must be routing-eligible", modeB.routingEligible);
            Assert.assertEquals("every row served from the in-mem tier", 2, modeB.inMemRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:00.000001Z\taa\t1\n" +
                            "2026-05-12T00:00:00.000002Z\tbb\t2\n");
        });
    }

    @Test
    public void testModeBSymbolIdsAreLvSpaceNotSegmentLocal() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL, keep INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            // The WHERE drops the first 'aa' row, so the LV first sees 'bb' then
            // 'aa'. LV-table symbol ids (bb=0, aa=1) are therefore the reverse of
            // the base segment's first-appearance ids (aa=0, bb=1). A tier that
            // stored the base segment-local id would resolve both symbols to the
            // wrong string; storing LV-space ids is what makes Mode B == disk-only.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, g, row_number() OVER () AS rn FROM base WHERE keep > 0");
            execute("INSERT INTO base (ts, g, keep) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 'aa', 0), " +
                    "('2026-05-12T00:00:00.000002Z', 'bb', 1), " +
                    "('2026-05-12T00:00:00.000003Z', 'aa', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("SYMBOL output must be routing-eligible", modeB.routingEligible);
            Assert.assertEquals("both surviving rows served from the tier", 2, modeB.inMemRowsServed);

            // With segment-local ids the two symbols would print swapped; the
            // oracle and the explicit expectation both pin the correct strings.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:00.000002Z\tbb\t1\n" +
                            "2026-05-12T00:00:00.000003Z\taa\t2\n");
        });
    }

    @Test
    public void testModeBSymbolSurvivesO3Rebuild() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL, keep INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Pin the CREATE clock below the data so the non-backfill floor admits
            // the back-dated O3 row.
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, g, row_number() OVER () AS rn FROM base WHERE keep > 0");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: the dropped 'aa' row again reverses base vs LV symbol
                // order, so the normal-cycle translation is under test, not just
                // an identity mapping.
                execute("INSERT INTO base (ts, g, keep) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 'aa', 0), " +
                        "('2026-05-12T00:00:02.000000Z', 'bb', 1), " +
                        "('2026-05-12T00:00:03.000000Z', 'aa', 1)");
                drainWalQueue();
                setCurrentMicros(250_000L);
                drainJob(job);
                drainWalQueue();

                // O3: a back-dated row carrying a fresh symbol forces a head-miss
                // replay (REPLACE_RANGE) that rewrites the LV table and rebuilds
                // the in-mem tier from disk (LV-space ids by construction).
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, g, keep) VALUES ('2026-05-12T00:00:00.000000Z', 'cc', 1)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job);
                drainWalQueue();
            }

            // The rebuilt tier serves Mode B and resolves every symbol correctly.
            // The rebuild reads LV-space ids straight from the rewritten LV table,
            // so this is end-to-end O3 + SYMBOL + Mode B regression coverage (the
            // normal-cycle translation is pinned separately by
            // testModeBSymbolIdsAreLvSpaceNotSegmentLocal).
            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-O3 cursor must regain Mode B", modeB.routingEligible);
            Assert.assertTrue("rebuilt tier serves in-mem rows", modeB.inMemRowsServed > 0);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT ts, g, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:00.000000Z\tcc\t1\n" +
                            "2026-05-12T00:00:02.000000Z\tbb\t2\n" +
                            "2026-05-12T00:00:03.000000Z\taa\t3\n");
        });
    }

    @Test
    public void testModeBMatchesDiskOnlyAcrossShapes() throws Exception {
        assertMemoryLeak(() -> {
            createSeamSplitLv();
            // Seam in the middle: disk serves the older prefix, in-mem the recent
            // suffix. Every shape must match the disk-only path byte for byte.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertModeBMatchesDiskOnly("SELECT * FROM lv LIMIT 3");
            assertModeBMatchesDiskOnly("SELECT * FROM lv LIMIT -2");
            assertModeBMatchesDiskOnly("SELECT * FROM lv WHERE x > 2");
            assertModeBMatchesDiskOnly("SELECT * FROM lv ORDER BY ts DESC");
        });
    }

    @Test
    public void testModeBSeamSplitServesDiskBelowAndInMemAbove() throws Exception {
        assertMemoryLeak(() -> {
            createSeamSplitLv();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
            // The 1s IN MEMORY window evicted the first cycle; only the 2 recent
            // rows remain in the slot, while disk still holds all 5.
            Assert.assertEquals("published slot retains only the recent cycle", 2, slot.rowCount());

            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue(modeB.routingEligible);
            Assert.assertEquals("in-mem serves only the recent suffix", 2, modeB.inMemRowsServed);

            // The full result (5 rows) must equal the disk-only path.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
        });
    }

    @Test
    public void testModeBServesEntireSlotWhenWindowCoversAll() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            // Both rows sit inside the 30m IN MEMORY window, so the seam is the
            // minimum timestamp: disk-below-seam is empty and the entire result
            // comes from the in-mem slot.
            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("aligned identity read must be Mode B", modeB.routingEligible);
            Assert.assertEquals("every row served from the in-mem tier", 2, modeB.inMemRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
        });
    }

    @Test
    public void testO3RebuildSkipThenForwardCycleDropsStaleRows() throws Exception {
        // Reproduces the both-slots-pinned O3-rebuild-skip stale-restamp gap. When
        // the O3 rebuild cannot acquire either slot (both reader-pinned), the
        // published slot keeps its pre-O3 rows stamped with the pre-O3 seqTxn -
        // correctly fenced disk-only while stale. The bug: a later forward cycle
        // copies / appends onto those stale rows and re-stamps them with the new
        // (matching) seqTxn, so Mode B would then serve pre-O3 rows the O3 replay
        // re-sequenced on disk. The tierStale flag forces that next publish to drop
        // the retained rows, so the slot reflects only disk-consistent rows again.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: three in-order rows fast-path appended into slot 0,
                // which stays published.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2), " +
                        "('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                setCurrentMicros(250_000L);
                drainJob(job);
                drainWalQueue();

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                Assert.assertEquals("cycle 1 publishes slot 0", 0, tier.getPublishedIdx());

                // Pin the published slot 0, then drive a slow-path cycle so the
                // worker swaps the publish to slot 1. Now pin slot 1 too: both
                // slots are reader-pinned, exactly the state that makes the O3
                // rebuild skip.
                final int pinA = tier.acquireRead();
                Assert.assertEquals(0, pinA);
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:04.000000Z', 4)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job); // slot 0 pinned -> slow-path swap to slot 1
                drainWalQueue();
                Assert.assertEquals("cycle 2 slow-path swaps to slot 1", 1, tier.getPublishedIdx());
                final int pinB = tier.acquireRead();
                Assert.assertEquals(1, pinB);

                // O3 cycle: a back-dated row forces a head-miss replay that
                // rewrites the LV table (re-sequencing rn) and tries to rebuild
                // the in-mem tier. Both slots are pinned, so the rebuild is
                // skipped - the published slot 1 keeps its stale pre-O3 rows.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                setCurrentMicros(750_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals("rebuild skipped, published slot unchanged", 1, tier.getPublishedIdx());

                // While both slots are still pinned a fresh cursor must route
                // disk-only: the stale slot's pre-O3 seqTxn no longer matches the
                // rewritten disk, so the fence correctly refuses Mode B.
                try (
                        RecordCursorFactory factory = select("SELECT * FROM lv");
                        LiveViewRecordCursor cursor = openLvCursor(factory)
                ) {
                    Assert.assertFalse("stale slot must fence disk-only", cursor.isRoutingEligible());
                }

                // Release both pins, then run a forward cycle. This is the publish
                // that, before the fix, re-stamped the stale pre-O3 rows with the
                // current seqTxn and exposed them to Mode B.
                tier.releaseRead(pinA);
                tier.releaseRead(pinB);
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:05.000000Z', 5)");
                drainWalQueue();
                setCurrentMicros(1_000_000L);
                drainJob(job);
                drainWalQueue();
            }

            // Mode B is back (the forward cycle re-stamped a slot the disk reader
            // agrees with) and serves only disk-consistent rows: equal to the
            // disk-only path and to the O3 re-sequenced recompute. Before the fix
            // the slot still held the stale pre-O3 rows, so Mode B diverged here.
            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("forward cycle must restore Mode B", modeB.routingEligible);
            Assert.assertTrue("Mode B must serve in-mem rows", modeB.inMemRowsServed > 0);
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:00.000000Z\t99\t1\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t4\n" +
                            "2026-05-12T00:00:04.000000Z\t4\t5\n" +
                            "2026-05-12T00:00:05.000000Z\t5\t6\n");
        });
    }

    @Test
    public void testO3ReplayRebuildOracleSurvivesRestart() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Pin the CREATE clock below the data so the non-backfill floor admits
            // every row, including the back-dated O3 row.
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2), " +
                        "('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                setCurrentMicros(250_000L);
                drainJob(job);
                drainWalQueue();

                // A back-dated row forces an O3 head-miss replay (REPLACE_RANGE)
                // that rebuilds the in-mem tier from the rewritten LV table.
                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 4)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job);
                drainWalQueue();
            }

            // The rebuilt tier (pre-restart) serves Mode B and agrees with disk-only.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");

            // Simulated restart: drop the in-memory registry (and its tier) and
            // rebuild it from on-disk state. The O3-rewritten rows live in the LV
            // table, so the re-read must still match the recompute.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Settle the restored view (rehydrate window state from the head
                // .cp), then ingest one in-order row so the fresh tier repopulates
                // through the normal publish path post-restart.
                setCurrentMicros(750_000L);
                drainJob(job);
                drainWalQueue();
                LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(restored);
                restored.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:04.000000Z', 5)");
                drainWalQueue();
                setCurrentMicros(1_000_000L);
                drainJob(job);
                drainWalQueue();
            }

            // Post-restart reads agree with disk-only across the restart boundary,
            // and the LV's content reflects the O3 re-sequencing plus the new row.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:00.000000Z\t4\t1\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t4\n" +
                            "2026-05-12T00:00:04.000000Z\t5\t5\n");
        });
    }

    @Test
    public void testO3ReplayRebuildBoundsToInMemoryWindow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Pin the CREATE clock below the data so the non-backfill floor admits
            // every row, including the back-dated O3 row.
            setCurrentMicros(0L);
            // A tight 2s IN MEMORY window: after O3 the rewritten LV table spans
            // two day-partitions, but only the recent 2s suffix is resident.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 2s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: four in-order rows on day 2026-05-12.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:00.000000Z', 1), " +
                        "('2026-05-12T00:00:01.000000Z', 2), " +
                        "('2026-05-12T00:00:02.000000Z', 3), " +
                        "('2026-05-12T00:00:03.000000Z', 4)");
                drainWalQueue();
                setCurrentMicros(250_000L);
                drainJob(job);
                drainWalQueue();

                // O3: a row back-dated onto the previous day forces a head-miss
                // replay that rewrites the LV table across both day-partitions.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-11T23:59:59.000000Z', 5)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job);
                drainWalQueue();
            }

            // The rebuild's tail read skips the 2026-05-11 partition entirely
            // (its newest row is below maxTs - 2s) and binary-searches the
            // 2026-05-12 partition for the window's lower edge (00:00:01), so the
            // slot holds only the recent three rows, not all five.
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            Assert.assertEquals(
                    "rebuilt slot holds only the IN MEMORY window suffix",
                    3,
                    tier.getSlot(tier.getPublishedIdx()).rowCount()
            );

            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-O3 cursor must regain Mode B", modeB.routingEligible);
            Assert.assertEquals("in-mem serves only the recent suffix", 3, modeB.inMemRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            // Disk serves the two below-seam rows, the tier the three above it.
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-11T23:59:59.000000Z\t5\t1\n" +
                            "2026-05-12T00:00:00.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:01.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:02.000000Z\t3\t4\n" +
                            "2026-05-12T00:00:03.000000Z\t4\t5\n");
        });
    }

    @Test
    public void testO3ReplayRebuildRegainsModeB() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Pin the CREATE clock below the data so the non-backfill floor admits
            // every row, including the back-dated O3 row.
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: three in-order rows, forward-appended; the tier is
                // populated and the O3 watermark advances to the newest ts.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2), " +
                        "('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                setCurrentMicros(250_000L);
                drainJob(job);
                drainWalQueue();

                // Cycle 2: a back-dated row below the watermark forces an O3 replay
                // (head-miss - any head's maxTs >= the late row's ts), rewriting
                // the LV table via REPLACE_RANGE and rebuilding the in-mem tier.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 4)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job);
                drainWalQueue();
            }

            // The rebuild repopulated the tier from the rewritten LV table: a
            // cursor opened right after the O3 cycle regains Mode B and serves the
            // whole window (all four rows fit the 30m IN MEMORY window).
            InnerRead modeB = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-O3 cursor must regain Mode B", modeB.routingEligible);
            Assert.assertEquals("rebuilt tier serves the whole window", 4, modeB.inMemRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            // The O3 replay re-sequenced the rows; the rebuilt tier reflects it.
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:00.000000Z\t4\t1\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t4\n");
        });
    }

    @Test
    public void testToTopReReadIsConsistent() throws Exception {
        assertMemoryLeak(() -> {
            createSeamSplitLv();
            try (RecordCursorFactory factory = select("SELECT * FROM lv")) {
                LiveViewRecordCursorFactory lvf = unwrapLvFactory(factory);
                try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvf.getCursor(sqlExecutionContext)) {
                    RecordMetadata md = lvf.getMetadata();
                    StringSink first = new StringSink();
                    println(md, cursor, first);
                    long firstServed = cursor.inMemRowsServed();
                    Assert.assertTrue("first pass must serve in-mem rows", firstServed > 0);

                    cursor.toTop();
                    StringSink second = new StringSink();
                    println(md, cursor, second);

                    Assert.assertEquals("toTop re-read must reproduce the first pass", first.toString(), second.toString());
                    // The counter is cumulative, so the second pass doubles it.
                    Assert.assertEquals(2 * firstServed, cursor.inMemRowsServed());
                }
            }
        });
    }

    @Test
    public void testServesUnflushedLeadFromRam() throws Exception {
        assertMemoryLeak(() -> {
            buildFlushedPlusLead();

            // The tier leads disk: the cursor serves the 2 un-flushed lead rows
            // from RAM on top of the 3 applied rows. The whole 30m window is
            // resident, so all 5 rows come from the slot, but 2 are the lead.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("lead read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // Differential oracle: the lead read equals a from-scratch recompute
            // over the base table. printSql preserves the tier; assertQuery cannot
            // be used here because its battery calls engine.clear() up front, which
            // drops the LV registry entry and with it the un-flushed lead.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            // Forcing the tier off (stamp mismatch) drops to the applied prefix:
            // disk holds only the 3 flushed rows, not the lead.
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 3");
        });
    }

    @Test
    public void testLeadSizeAndLimitPushdown() throws Exception {
        assertMemoryLeak(() -> {
            buildFlushedPlusLead();

            // Full scan: size() = disk.size() + leadRowCount = 5; the read serves
            // all five rows, matching the recompute.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            // A tail LIMIT uses size() to find the offset, so it lands on the
            // un-flushed lead rows.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT -2",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT -2");
            // A head LIMIT crosses the overlap/lead boundary cleanly.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 4",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 4");
        });
    }

    @Test
    public void testFlushPromotesLeadToOverlap() throws Exception {
        assertMemoryLeak(() -> {
            buildFlushedPlusLead();

            // Before the flush the 2 recent rows are the un-flushed lead.
            InnerRead before = readInner("SELECT * FROM lv");
            Assert.assertEquals("two lead rows before flush", 2, before.leadRowsServed);

            // Advance the clock past FLUSH EVERY and tick once: the flush lands the
            // lead on disk and re-stamps the slot as a subset of disk.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();

            // The lead is now overlap: the cursor still routes through the tier,
            // serves all 5 rows, but none of them are an un-flushed lead anymore.
            InnerRead after = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-flush read must stay routing-eligible", after.routingEligible);
            Assert.assertEquals("all rows still served from the tier", 5, after.inMemRowsServed);
            Assert.assertEquals("no un-flushed lead after the flush", 0, after.leadRowsServed);

            // Disk now holds every row, so the tier read and the disk-only read
            // agree, and the tier read still matches the recompute.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
        });
    }

    @Test
    public void testRestartRecoversUnflushedLeadFromBaseWal() throws Exception {
        // Crash between refresh and flush: the un-flushed lead lives only in RAM,
        // so a restart loses it. The base WAL is retained up to the applied point
        // (lvConsumedSeqTxn == applied), so the first post-restart refresh rebuilds
        // the lead by draining the retained base WAL forward. No row is lost and
        // the read matches a from-scratch recompute.
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // 3 flushed rows on disk + 2 un-flushed lead rows in RAM

            // Sanity: the lead is resident before the "crash".
            InnerRead before = readInner("SELECT * FROM lv");
            Assert.assertEquals("two un-flushed lead rows before restart", 2, before.leadRowsServed);

            // Simulated crash + restart: drop the in-memory registry (and its tier,
            // so the RAM lead is gone) and rebuild from on-disk state. Disk holds
            // only the 3 flushed rows; the .cp sits at the applied point (no gap).
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(restored);
            // Keep the rebuilt lead un-flushed: lastFlushTimeUs is in-RAM and resets
            // to LONG_NULL on restart, which would otherwise flush on the first tick.
            // With the clock pinned at 0 and FLUSH EVERY 1s this suppresses the flush.
            restored.setLastFlushTimeUs(0L);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job); // restore from .cp, then drain the base WAL forward to rebuild the lead
            }

            // The lead is back in RAM (2 rows) and the read equals the recompute.
            // After a restart the rebuilt tier holds only the lead; the overlap (the
            // flushed rows within the IN MEMORY window) is served from disk until a
            // later flush rebuilds the resident window, so only the 2 lead rows are
            // served from the tier here. Correctness is unaffected - disk holds the
            // overlap - and the seam cut stitches the two together.
            InnerRead after = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-restart read must be routing-eligible", after.routingEligible);
            Assert.assertEquals("only the rebuilt lead is resident after restart", 2, after.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows recovered from the base WAL", 2, after.leadRowsServed);
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            // Disk still holds only the applied prefix (the lead is in RAM again).
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 3");
        });
    }

    @Test
    public void testRestartReplaysCheckpointCadenceGap() throws Exception {
        // The head .cp is written on a cadence (rows / duration), not every flush,
        // so its base seqTxn can lag the applied point: the on-disk LV table holds
        // rows the .cp's accumulators do not. On restart the restore must replay the
        // base WAL over (head, applied] WITHOUT re-emitting to advance the
        // accumulators to the disk state, then resume at the applied point so
        // drain-forward only rebuilds the un-flushed lead. Without replay-to-applied
        // the restore would resume at the head and re-emit the rows disk already
        // holds, duplicating them once the rebuilt lead is flushed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1 -> flush 1. The first flush always writes a head .cp
                // (firstCp), stamped at the applied point.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), ('2026-05-12T00:00:02.000000Z', 2)");
                drainWalQueue();
                drainJob(job); // clock 0: refresh batch 1 then flush (firstCp -> .cp written)

                // Batch 2 -> flush 2. Past FLUSH EVERY so it flushes, but neither the
                // row cadence (default 1M) nor the duration cadence (default 5m) is
                // met, so flush 2 does NOT write a fresh .cp. The .cp now lags applied.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:03.000000Z', 3), ('2026-05-12T00:00:04.000000Z', 4)");
                drainWalQueue();
                drainJob(job);

                // Batch 3 -> un-flushed lead (within FLUSH EVERY of flush 2).
                setCurrentMicros(250_000L);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:05.000000Z', 5), ('2026-05-12T00:00:06.000000Z', 6)");
                drainWalQueue();
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // The cadence gap exists: head .cp base seqTxn < applied watermark.
            Assert.assertTrue(
                    "test must create a checkpoint-cadence gap (head < applied)",
                    instance.getHeadCheckpointLvSeqTxn() < instance.getAppliedWatermark()
            );
            InnerRead before = readInner("SELECT * FROM lv");
            Assert.assertEquals("two un-flushed lead rows before restart", 2, before.leadRowsServed);

            // Simulated restart: the RAM lead (batch 3) is lost; disk holds batches
            // 1 + 2 at the applied point; the .cp sits at batch 1.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(restored);
            restored.setLastFlushTimeUs(250_000L); // suppress an immediate flush so the lead stays in RAM

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(250_000L);
                drainJob(job); // restore .cp@batch1 -> replay-to-applied (batch 2) -> drain forward (batch 3)
            }

            // The rebuilt view matches the recompute, and the disk-only (applied)
            // prefix holds batches 1 + 2 exactly once - replay-to-applied did not
            // re-emit batch 2.
            InnerRead after = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-restart read must be routing-eligible", after.routingEligible);
            Assert.assertEquals("two un-flushed lead rows recovered (batch 3)", 2, after.leadRowsServed);
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 4");

            // Flush the rebuilt lead and confirm disk holds all six rows exactly
            // once (no duplicate batch 2). assertQuery is safe now: the lead is on
            // disk, so engine.clear() loses nothing.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t1\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t2\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t3\n" +
                            "2026-05-12T00:00:04.000000Z\t4\t4\n" +
                            "2026-05-12T00:00:05.000000Z\t5\t5\n" +
                            "2026-05-12T00:00:06.000000Z\t6\t6\n");
        });
    }

    @Test
    public void testSymbolLvIsNotLeadEligible() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            // SYMBOL output: stays a subset of disk (no lead) until eager interning.
            execute("CREATE LIVE VIEW lv_sym FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, g, row_number() OVER () AS rn FROM base");
            // Non-SYMBOL output of the same base: lead-eligible.
            execute("CREATE LIVE VIEW lv_num FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, g, x) VALUES " +
                    "('2026-05-12T00:00:01.000000Z', 'aa', 1), " +
                    "('2026-05-12T00:00:02.000000Z', 'bb', 2)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance sym = engine.getLiveViewRegistry().getViewInstance("lv_sym");
            LiveViewInstance num = engine.getLiveViewRegistry().getViewInstance("lv_num");
            Assert.assertNotNull(sym);
            Assert.assertNotNull(num);
            Assert.assertTrue("lead eligibility must be computed", sym.isLeadEligibilityComputed());
            Assert.assertFalse("SYMBOL output is not lead-eligible", sym.isLeadEligible());
            Assert.assertTrue("fixed-width output is lead-eligible", num.isLeadEligible());

            // The SYMBOL view still reads correctly as a subset of disk.
            assertQuery("SELECT * FROM lv_sym")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:01.000000Z\taa\t1\n" +
                            "2026-05-12T00:00:02.000000Z\tbb\t2\n");
        });
    }

    // Builds an LV with 3 flushed rows (A) on disk and 2 un-flushed lead rows (B)
    // in the tier: cycle 1 flushes A (the first tick always flushes); cycle 2
    // refreshes B within the FLUSH EVERY window, so the refresh publishes B as the
    // lead without flushing. The clock stays at 0 (set in the class @Before) so the
    // second refresh is inside FLUSH EVERY relative to the first flush at t=0.
    private void buildFlushedPlusLead() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                "SELECT ts, x, row_number() OVER () AS rn FROM base");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:01.000000Z', 1), " +
                    "('2026-05-12T00:00:02.000000Z', 2), " +
                    "('2026-05-12T00:00:03.000000Z', 3)");
            drainWalQueue();
            drainJob(job); // clock 0: first tick flushes A to disk

            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:04.000000Z', 4), " +
                    "('2026-05-12T00:00:05.000000Z', 5)");
            drainWalQueue();
            drainJob(job); // clock still 0: refresh B as the un-flushed lead, no flush
        }
    }

    // Asserts the LV read (tier on) is byte-identical to an oracle SQL - a
    // from-scratch recompute over the base table. Uses printSql (not assertQuery,
    // whose battery calls engine.clear() and so wipes the un-flushed lead).
    private static void assertLvMatchesOracle(String lvSql, String oracleSql) throws SqlException {
        StringSink lv = new StringSink();
        printSql(lvSql, lv);
        StringSink oracle = new StringSink();
        printSql(oracleSql, oracle);
        Assert.assertEquals("LV read must match oracle [" + lvSql + "] vs [" + oracleSql + "]",
                oracle.toString(), lv.toString());
    }

    // Runs the LV SELECT with the fence forced off (disk-only, by mismatching both
    // slots' stamps) and asserts the output equals an oracle SQL - the applied
    // prefix. Restores the stamps afterwards.
    private static void assertDiskOnlyMatchesOracle(String lvSql, String oracleSql) throws SqlException {
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull(tier);
        long s0 = tier.getSlot(0).lvSeqTxn();
        long s1 = tier.getSlot(1).lvSeqTxn();
        tier.getSlot(0).setLvSeqTxn(mismatch(s0));
        tier.getSlot(1).setLvSeqTxn(mismatch(s1));
        StringSink diskOnly = new StringSink();
        try {
            printSql(lvSql, diskOnly);
        } finally {
            tier.getSlot(0).setLvSeqTxn(s0);
            tier.getSlot(1).setLvSeqTxn(s1);
        }
        StringSink oracle = new StringSink();
        printSql(oracleSql, oracle);
        Assert.assertEquals("disk-only read must match oracle for: " + lvSql, oracle.toString(), diskOnly.toString());
    }

    // Runs the SELECT with the tier on (Mode B) and then with the fence forced
    // off (disk-only, achieved by mismatching both slots' stamps), and asserts
    // the two outputs are byte-identical. Restores the stamps afterwards.
    private static void assertModeBMatchesDiskOnly(String sql) throws SqlException {
        StringSink modeB = new StringSink();
        printSql(sql, modeB);

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull(tier);
        long s0 = tier.getSlot(0).lvSeqTxn();
        long s1 = tier.getSlot(1).lvSeqTxn();
        tier.getSlot(0).setLvSeqTxn(mismatch(s0));
        tier.getSlot(1).setLvSeqTxn(mismatch(s1));
        StringSink diskOnly = new StringSink();
        try {
            printSql(sql, diskOnly);
        } finally {
            tier.getSlot(0).setLvSeqTxn(s0);
            tier.getSlot(1).setLvSeqTxn(s1);
        }
        Assert.assertEquals("Mode B vs disk-only mismatch for: " + sql, diskOnly.toString(), modeB.toString());
    }

    // Creates a fixed-width LV with the in-mem tier on, ingests two rows, and
    // drives one refresh cycle so the published slot is populated and stamped.
    private void createIngestRefresh() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
        execute("INSERT INTO base (ts, x) VALUES " +
                "('2026-05-12T00:00:00.000001Z', 4), " +
                "('2026-05-12T00:00:00.000002Z', 9)");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(job);
        }
        drainWalQueue();
    }

    // Builds an LV whose published in-mem slot holds only a recent suffix while
    // disk holds the full history, so the seam falls in the middle. growth.bytes
    // = 0 forces the slow-path (and its IN MEMORY eviction) every cycle; the two
    // ingest cycles are 5s apart, beyond the 1s IN MEMORY window, so cycle 2
    // evicts cycle 1 from the slot. Result: disk has 5 rows, the slot has the 2
    // most recent (seam = cycle-2 minimum).
    private void createSeamSplitLv() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Pin the CREATE wall clock below the data so every row stays in-frame.
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1s AS " +
                "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
        final long dataStart = 1_700_000_000_000_000L;
        final long cycle2Start = dataStart + 5_000_000L; // 5s later, beyond IN MEMORY 1s
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, x) VALUES " +
                    "(" + (dataStart + 1) + ", 1), (" + (dataStart + 2) + ", 2), (" + (dataStart + 3) + ", 3)");
            drainWalQueue();
            setCurrentMicros(250_000L); // > FLUSH EVERY 100ms
            drainJob(job);

            execute("INSERT INTO base (ts, x) VALUES " +
                    "(" + (cycle2Start + 1) + ", 4), (" + (cycle2Start + 2) + ", 5)");
            drainWalQueue();
            setCurrentMicros(500_000L);
            drainJob(job);
        }
        drainWalQueue();
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Maps a slot stamp to a value the disk reader can never report, forcing the
    // fence off. LONG_NULL slots map to 1 (any non-null mismatch will do).
    private static long mismatch(long seqTxn) {
        return seqTxn == Numbers.LONG_NULL ? 1 : seqTxn + 1_000_000;
    }

    // Unwraps any QueryProgress wrapper to the LiveViewRecordCursorFactory and
    // opens its cursor, so the test can read the fence predicate directly.
    private static LiveViewRecordCursor openLvCursor(RecordCursorFactory factory) throws SqlException {
        return (LiveViewRecordCursor) unwrapLvFactory(factory).getCursor(sqlExecutionContext);
    }

    // Opens a fresh inner LiveViewRecordCursor for the SELECT, drains it, and
    // reports the printed output alongside the Mode B observability counters.
    private static InnerRead readInner(String sql) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            LiveViewRecordCursorFactory lvf = unwrapLvFactory(factory);
            try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvf.getCursor(sqlExecutionContext)) {
                StringSink out = new StringSink();
                println(lvf.getMetadata(), cursor, out);
                return new InnerRead(out.toString(), cursor.inMemRowsServed(), cursor.leadRowsServed(), cursor.isRoutingEligible());
            }
        }
    }

    private static LiveViewRecordCursorFactory unwrapLvFactory(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
        return (LiveViewRecordCursorFactory) f;
    }

    // Captured output and seam-routing observability counters from one inner-cursor read.
    private static final class InnerRead {
        final long inMemRowsServed;
        final long leadRowsServed;
        final String output;
        final boolean routingEligible;

        InnerRead(String output, long inMemRowsServed, long leadRowsServed, boolean routingEligible) {
            this.output = output;
            this.inMemRowsServed = inMemRowsServed;
            this.leadRowsServed = leadRowsServed;
            this.routingEligible = routingEligible;
        }
    }
}
