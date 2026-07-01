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
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
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

            // The refresh worker eager-interned the symbols into the LV table's id
            // space, and the first tick flushed them to disk, so the slot is a
            // subset of disk and the in-mem branch resolves the SYMBOL through the
            // overlay (committed ids via the disk reader's symbol table).
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
    public void testArrayPassthroughServesLeadFromRam() throws Exception {
        // Passthrough DOUBLE[] output column: the tier carries the raw arrays from
        // RAM via ArrayTypeDriver, so an array LV gets the same lead-serving
        // behaviour as a purely-numeric one. Exercises the (data, aux) write path,
        // the flush flyweight, the merge-record getArray accessor, and
        // reset()/footprint end to end across the normal / null cases.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, arr, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: three flushed rows on disk.
                execute("INSERT INTO base (ts, arr) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', ARRAY[1.0, 2.0]), " +
                        "('2026-05-12T00:00:02.000000Z', ARRAY[3.0]), " +
                        "('2026-05-12T00:00:03.000000Z', ARRAY[4.0, 5.0, 6.0])");
                drainWalQueue();
                drainJob(job); // clock 0: first tick flushes the batch to disk

                // Cycle 2: a 2-row un-flushed lead within FLUSH EVERY. One lead row
                // carries NULL.
                execute("INSERT INTO base (ts, arr) VALUES " +
                        "('2026-05-12T00:00:04.000000Z', ARRAY[7.0, 8.0]), " +
                        "('2026-05-12T00:00:05.000000Z', NULL)");
                drainWalQueue();
                drainJob(job); // clock still 0: refresh the lead, no flush
            }

            // The tier leads disk: all 5 rows are resident, 2 of them the lead.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("var-size lead read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // size() folds the lead on top of the applied disk prefix.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                Assert.assertEquals("size() = applied rows on disk + un-flushed lead", 5, cursor.size());
            }

            // Differential oracle: the in-mem read (incl. the lead's arrays and the
            // NULL row) equals a from-scratch recompute over base.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, arr, row_number() OVER () AS rn FROM base");

            // Forcing the tier off drops to the applied prefix: the lead's arrays are
            // absent from disk.
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, arr, row_number() OVER () AS rn FROM base LIMIT 3");

            // A flush lands the lead on disk; the tier read then equals the
            // disk-only read byte for byte.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
        });
    }

    @Test
    public void testArrayElementFilterServedFromRamModeB() throws Exception {
        // A WHERE predicate on an array element - a1[i] / a2[i][j] - reaches
        // MergedRecord.getArrayDouble1d2d, the direct-index fast path
        // DoubleArrayAccessFunctionFactory takes when the array argument is a column
        // read straight off the routed cursor's record. (A projected a1[i] instead
        // resolves through the whole-array getArray slow path, already covered by
        // testArrayPassthroughServesLeadFromRam; the filter is what forces the
        // element-index override.) The read stays a full-schema projection
        // (SELECT ts, a1, a2, rn) so the seam fence routes it Mode B, and the filter
        // must decode each row's element out of the tier's (data, aux) region: the
        // predicate keeps an un-flushed LEAD row (absent from disk), so a wrong
        // decode - or a fall-back to the disk record - drops or mismatches it against
        // the base oracle. Covers the 1-D element, the 2-D [row][col] element, and
        // the NULL array -> NaN -> predicate-false branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, a1 DOUBLE[], a2 DOUBLE[][]) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, a1, a2, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: three flushed rows on disk, mixed 1-D and 2-D shapes.
                execute("INSERT INTO base (ts, a1, a2) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', ARRAY[1.0, 2.0, 3.0], ARRAY[[10.0, 11.0], [12.0, 13.0]]), " +
                        "('2026-05-12T00:00:02.000000Z', ARRAY[4.0], ARRAY[[14.0]]), " +
                        "('2026-05-12T00:00:03.000000Z', ARRAY[5.0, 6.0], ARRAY[[15.0, 16.0]])");
                drainWalQueue();
                drainJob(job); // clock 0: first tick flushes the batch to disk

                // Cycle 2: a 2-row un-flushed lead within FLUSH EVERY. One lead row
                // carries NULL arrays (the isNull -> NaN branch).
                execute("INSERT INTO base (ts, a1, a2) VALUES " +
                        "('2026-05-12T00:00:04.000000Z', ARRAY[7.0, 8.0], ARRAY[[17.0, 18.0], [19.0, 20.0]]), " +
                        "('2026-05-12T00:00:05.000000Z', NULL, NULL)");
                drainWalQueue();
                drainJob(job); // clock still 0: refresh the lead, no flush
            }

            // The unfiltered full-schema read routes Mode B and serves the 2-row
            // lead, so a filter over the same read sees the lead rows in RAM.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("array-filter base read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // 1-D element predicate: a1[1] > 4.5 keeps disk-overlap row 3 (a1[1]=5)
            // and lead row 4 (a1[1]=7), and drops the NULL-array lead row 5
            // (NaN > 4.5 false). The differential over base proves getArrayDouble1d2d
            // decodes the 1-D element - including the lead row's - out of the tier
            // correctly. Row 4 is un-flushed, so its presence proves the tier fed the
            // filter, not disk.
            // The oracle computes rn over all base rows in a subquery, then filters,
            // so it reproduces the LV's stored rn (a bare row_number() OVER () under a
            // WHERE would renumber the filtered rows and mismatch the materialized rn).
            assertLvMatchesOracle(
                    "SELECT ts, a1, a2, rn FROM lv WHERE a1[1] > 4.5",
                    "SELECT * FROM (SELECT ts, a1, a2, row_number() OVER () AS rn FROM base) WHERE a1[1] > 4.5");

            // Pin the 1-D element at a non-zero index: a1[2] = 8 matches only lead
            // row 4 (a1=[7,8]). A wrong 1-D offset would read 7 (or NaN) and drop the
            // row, mismatching the oracle.
            assertLvMatchesOracle(
                    "SELECT ts, a1, a2, rn FROM lv WHERE a1[2] = 8.0",
                    "SELECT * FROM (SELECT ts, a1, a2, row_number() OVER () AS rn FROM base) WHERE a1[2] = 8.0");

            // 2-D element predicate pins the exact value: a2[2][2] = 20 matches only
            // lead row 4 (a2=[[17,18],[19,20]]). The equality catches a wrong 2-D
            // flat-index (a bad stride would read 17 / 18 / 19, none of which equal
            // 20), so a broken row-major decode drops the row and mismatches the
            // oracle. The flushed rows' a2[2][2] are 13 (row 1) and NaN (rows 2 / 3
            // are 1-row so idx0 is out of bounds), and the NULL row is NaN.
            assertLvMatchesOracle(
                    "SELECT ts, a1, a2, rn FROM lv WHERE a2[2][2] = 20.0",
                    "SELECT * FROM (SELECT ts, a1, a2, row_number() OVER () AS rn FROM base) WHERE a2[2][2] = 20.0");

            // Forcing the tier off drops the lead: the disk-only scan holds only the
            // 3 flushed rows, none of which have a2[2][2] = 20, so the result is
            // empty. Contrast with the Mode B read above (which returns lead row 4) -
            // that difference is exactly the row the override decoded out of RAM.
            assertDiskOnlyMatchesOracle(
                    "SELECT ts, a1, a2, rn FROM lv WHERE a2[2][2] = 20.0",
                    "SELECT * FROM (SELECT ts, a1, a2, row_number() OVER () AS rn FROM base) WHERE a2[2][2] = 99999.0");

            // A flush lands the lead on disk; the Mode B filtered read then equals the
            // disk-only filtered read byte for byte.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();
            assertModeBMatchesDiskOnly("SELECT ts, a1, a2, rn FROM lv WHERE a1[1] > 4.5");
        });
    }

    @Test
    public void testStringBinaryPassthroughServesLeadFromRam() throws Exception {
        // Passthrough STRING + BINARY output columns: the tier carries the raw
        // values from RAM, so a var-size LV gets the same lead-serving behaviour as
        // a purely-numeric one. Exercises the (data, aux) write path, the flush
        // flyweight, the merge-record accessors, and reset()/footprint end to end.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, s STRING, b BINARY) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, s, b, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: three flushed rows on disk (rnd_bin is evaluated once at
                // insert, so the stored bytes are fixed for both reads below).
                execute("INSERT INTO base (ts, s, b) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 'aaa', rnd_bin(4, 16, 0)), " +
                        "('2026-05-12T00:00:02.000000Z', 'bb', rnd_bin(4, 16, 0)), " +
                        "('2026-05-12T00:00:03.000000Z', 'c', rnd_bin(4, 16, 0))");
                drainWalQueue();
                drainJob(job); // clock 0: first tick flushes the batch to disk

                // Cycle 2: a 2-row un-flushed lead within FLUSH EVERY. One lead row
                // carries NULL for both var-size columns.
                execute("INSERT INTO base (ts, s, b) VALUES " +
                        "('2026-05-12T00:00:04.000000Z', 'dddd', rnd_bin(4, 16, 0)), " +
                        "('2026-05-12T00:00:05.000000Z', NULL, NULL)");
                drainWalQueue();
                drainJob(job); // clock still 0: refresh the lead, no flush
            }

            // The tier leads disk: all 5 rows are resident, 2 of them the lead.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("var-size lead read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // size() folds the lead on top of the applied disk prefix.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                Assert.assertEquals("size() = applied rows on disk + un-flushed lead", 5, cursor.size());
            }

            // Differential oracle: the in-mem read (incl. the lead's STRING/BINARY
            // and the NULL row) equals a from-scratch recompute over base.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, s, b, row_number() OVER () AS rn FROM base");

            // Forcing the tier off drops to the applied prefix: the lead's var-size
            // values are absent from disk.
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, s, b, row_number() OVER () AS rn FROM base LIMIT 3");

            // A flush lands the lead on disk; the tier read then equals the
            // disk-only read byte for byte.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
        });
    }

    @Test
    public void testVarcharPassthroughServesLeadFromRam() throws Exception {
        // Passthrough VARCHAR output column: the tier carries the raw values from
        // RAM via VarcharTypeDriver, so a VARCHAR LV gets the same lead-serving
        // behaviour as a purely-numeric one. Exercises the (data, aux) write path,
        // the flush flyweight, the merge-record accessors, and reset()/footprint end
        // to end across the inlined / split / null cases.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, v, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: three flushed rows on disk. A short inlined value, a long
                // value forced through the split (data-region) path, and an empty.
                execute("INSERT INTO base (ts, v) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 'aaa'), " +
                        "('2026-05-12T00:00:02.000000Z', 'a long value beyond the inlined prefix'), " +
                        "('2026-05-12T00:00:03.000000Z', '')");
                drainWalQueue();
                drainJob(job); // clock 0: first tick flushes the batch to disk

                // Cycle 2: a 2-row un-flushed lead within FLUSH EVERY. One lead row
                // carries NULL.
                execute("INSERT INTO base (ts, v) VALUES " +
                        "('2026-05-12T00:00:04.000000Z', 'dddd'), " +
                        "('2026-05-12T00:00:05.000000Z', NULL)");
                drainWalQueue();
                drainJob(job); // clock still 0: refresh the lead, no flush
            }

            // The tier leads disk: all 5 rows are resident, 2 of them the lead.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("var-size lead read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // size() folds the lead on top of the applied disk prefix.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                Assert.assertEquals("size() = applied rows on disk + un-flushed lead", 5, cursor.size());
            }

            // Differential oracle: the in-mem read (incl. the lead's VARCHAR and the
            // NULL row) equals a from-scratch recompute over base.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, v, row_number() OVER () AS rn FROM base");

            // Forcing the tier off drops to the applied prefix: the lead's var-size
            // values are absent from disk.
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, v, row_number() OVER () AS rn FROM base LIMIT 3");

            // A flush lands the lead on disk; the tier read then equals the
            // disk-only read byte for byte.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
        });
    }

    @Test
    public void testLeadSizeAndLimitPushdown() throws Exception {
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // disk (applied): ts 01..03; lead (RAM): ts 04,05

            // Full scan: size() = disk.size() + leadRowCount = 5; the read serves
            // all five rows, matching the recompute.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            // A head LIMIT inside the overlap never reaches the lead.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 2",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 2");
            // A head LIMIT exactly at the overlap/lead boundary.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 3",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 3");
            // A head LIMIT crosses the overlap/lead boundary cleanly.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 4",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 4");
            // A head LIMIT past size() returns every row, no over-read.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 10",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 10");
            // A tail LIMIT uses size() to find the offset, so it lands on the
            // un-flushed lead rows.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT -2",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT -2");
            // A tail LIMIT that crosses the lead/overlap boundary back into disk.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT -4",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT -4");
            // A bounded range LIMIT straddling the overlap/lead boundary.
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 2,5",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 2,5");
        });
    }

    @Test
    public void testLeadSizeReportsDiskPlusLead() throws Exception {
        // size() must fold the un-flushed lead on top of the disk (applied) row
        // count so a LIMIT pushdown sees every served row. Asserts the raw value
        // in both modes, the disk-only fallback (fence forced off) reporting only
        // the applied prefix.
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // disk (applied): 3 rows; lead (RAM): +2 rows

            // Routing-eligible: size() = disk.size() (3) + leadRowCount (2) = 5.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("lead read must be routing-eligible", cursor.isRoutingEligible());
                Assert.assertEquals("size() = applied rows on disk + un-flushed lead", 5, cursor.size());
            }

            // Disk-only (both slot stamps mismatched): size() reports only the
            // applied prefix on disk - the lead is invisible.
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            long s0 = tier.getSlot(0).lvSeqTxn();
            long s1 = tier.getSlot(1).lvSeqTxn();
            tier.getSlot(0).setLvSeqTxn(mismatch(s0));
            tier.getSlot(1).setLvSeqTxn(mismatch(s1));
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("stamp mismatch must fence disk-only", cursor.isRoutingEligible());
                Assert.assertEquals("disk-only size() = applied prefix only", 3, cursor.size());
            } finally {
                tier.getSlot(0).setLvSeqTxn(s0);
                tier.getSlot(1).setLvSeqTxn(s1);
            }
        });
    }

    @Test
    public void testAsOfJoinRhsSeesAppliedPrefixNotLead() throws Exception {
        // ASOF JOIN with the LV on the RHS consumes the LV's time-frame cursor,
        // which is disk-only in V1: it serves the applied prefix and
        // trails the un-flushed lead by at most one flush cycle. A documented
        // freshness limitation, not a correctness issue - a flush lands the lead
        // on disk and the join catches up. Pin the disk-only ASOF path so it stays
        // explicit (the record-cursor read at the same instant DOES serve the
        // lead, proving the join deliberately ignores the live lead).
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // disk (applied): ts 01..03 x=1..3; lead (RAM): ts 04,05 x=4,5

            // The lead is live in the tier: a record-cursor read serves it from RAM.
            InnerRead direct = readInner("SELECT * FROM lv");
            Assert.assertEquals("two un-flushed lead rows live in the tier", 2, direct.leadRowsServed);

            // Probe rows land at and after the lead's timestamps.
            execute("CREATE TABLE probe (ts TIMESTAMP, id INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO probe (ts, id) VALUES " +
                    "('2026-05-12T00:00:04.500000Z', 1), " +
                    "('2026-05-12T00:00:06.000000Z', 2)");
            drainWalQueue();

            final String asofSql = "SELECT p.ts, p.id, lv.x FROM probe p ASOF JOIN lv";

            // The plan confirms the disk-only fast (time-frame) ASOF path over the
            // LV - never the record-cursor light path that would see the lead.
            assertQuery(asofSql).noLeakCheck().assertsPlanContaining("AsOf Join Fast", "LiveView");

            // Even though the lead (ts 04,05 / x=4,5) is live in RAM, the join
            // matches each probe row to the last *applied* lv row (ts 03, x=3).
            // printSql keeps the tier alive, so this proves the join ignores the
            // live lead, not that the tier happened to be empty.
            StringSink trailing = new StringSink();
            printSql(asofSql, trailing);
            Assert.assertEquals(
                    "ts\tid\tx\n" +
                            "2026-05-12T00:00:04.500000Z\t1\t3\n" +
                            "2026-05-12T00:00:06.000000Z\t2\t3\n",
                    trailing.toString());

            // A flush lands the lead on disk; the disk-only ASOF then catches up -
            // the freshness gap is bounded by one flush cycle, not a lost match.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();

            StringSink caughtUp = new StringSink();
            printSql(asofSql, caughtUp);
            Assert.assertEquals(
                    "ts\tid\tx\n" +
                            "2026-05-12T00:00:04.500000Z\t1\t4\n" +
                            "2026-05-12T00:00:06.000000Z\t2\t5\n",
                    caughtUp.toString());
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
    public void testSymbolLvIsLeadEligible() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            // SYMBOL output is now lead-eligible: eager interning gives the lead's
            // symbols LV-table-consistent ids the read path resolves from RAM.
            execute("CREATE LIVE VIEW lv_sym FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, g, row_number() OVER () AS rn FROM base");
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
            Assert.assertTrue("SYMBOL output is lead-eligible", sym.isLeadEligible());
            Assert.assertTrue("fixed-width output is lead-eligible", num.isLeadEligible());

            assertQuery("SELECT * FROM lv_sym")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:01.000000Z\taa\t1\n" +
                            "2026-05-12T00:00:02.000000Z\tbb\t2\n");
        });
    }

    @Test
    public void testSymbolLvServesUnflushedLeadFromRam() throws Exception {
        assertMemoryLeak(() -> {
            buildSymbolFlushedPlusLead();

            // The lead carries a SYMBOL value ('cc') that is new - not on disk - plus
            // a re-occurring committed value ('bb'). Both resolve from RAM: 'cc' via
            // the tier's symbol cache, 'bb' via the disk reader's committed table,
            // through the one-id-space overlay.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("lead read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // Differential oracle: the lead read equals a from-scratch recompute.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, g, row_number() OVER () AS rn FROM base");

            // Forcing the tier off drops to the applied prefix (the 3 flushed rows);
            // the lead-only 'cc' value is absent from that prefix.
            assertDiskOnlyMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, g, row_number() OVER () AS rn FROM base LIMIT 3");
        });
    }

    @Test
    public void testSymbolLvLeadFilterOnLeadOnlyValue() throws Exception {
        assertMemoryLeak(() -> {
            buildSymbolFlushedPlusLead();

            // A WHERE on the lead-only SYMBOL value resolves the constant through
            // the same overlay (keyOf finds 'cc' in the cache), and the per-row int
            // key matches, so the lead row is returned - not dropped or mismatched.
            // This pins the raw-int-key (path b) resolution, not just getSymA. The
            // oracle filters the LV's pre-computed row_number projection (a plain
            // recompute would re-rank after the filter and disagree on rn).
            assertLvMatchesOracle("SELECT * FROM lv WHERE g = 'cc'",
                    "SELECT * FROM (SELECT ts, g, row_number() OVER () AS rn FROM base) WHERE g = 'cc'");
            // A WHERE on a committed value spanning the overlap and the lead.
            assertLvMatchesOracle("SELECT * FROM lv WHERE g = 'bb'",
                    "SELECT * FROM (SELECT ts, g, row_number() OVER () AS rn FROM base) WHERE g = 'bb'");
            // ORDER BY the SYMBOL column: the static-symbol sort ranks by the raw
            // int key over the overlay's symbol count, which spans the lead's ids.
            assertLvMatchesOracle("SELECT * FROM lv ORDER BY g, ts",
                    "SELECT * FROM (SELECT ts, g, row_number() OVER () AS rn FROM base) ORDER BY g, ts");
        });
    }

    @Test
    public void testSymbolLeadFlushPromotesToOverlap() throws Exception {
        assertMemoryLeak(() -> {
            buildSymbolFlushedPlusLead();

            InnerRead before = readInner("SELECT * FROM lv");
            Assert.assertEquals("two lead rows before flush", 2, before.leadRowsServed);

            // Flush: the lead's new symbol 'cc' becomes committed at the id the
            // drain assigned, so the slot's ids still agree with disk.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(1_000_000L);
                drainJob(job);
            }
            drainWalQueue();

            InnerRead after = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-flush read must stay routing-eligible", after.routingEligible);
            Assert.assertEquals("all rows still served from the tier", 5, after.inMemRowsServed);
            Assert.assertEquals("no un-flushed lead after the flush", 0, after.leadRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, g, row_number() OVER () AS rn FROM base");
        });
    }

    @Test
    public void testSymbolLeadRecoversFromBaseWalOnRestart() throws Exception {
        assertMemoryLeak(() -> {
            buildSymbolFlushedPlusLead(); // 3 flushed rows + 2 un-flushed lead rows (incl. new 'cc')

            // Simulated crash + restart: the RAM lead (and its symbol cache) is gone.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(restored);
            restored.setLastFlushTimeUs(0L); // keep the rebuilt lead un-flushed (clock at 0)

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job); // drain the base WAL forward, re-interning the lead's symbols afresh
            }

            // The lead is back (2 rows) with correct symbol resolution: re-interning
            // re-derives 'cc' (new) and 'bb' (committed) against a fresh cache + the
            // restored disk symbol table. Only the rebuilt lead is resident.
            InnerRead after = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-restart read must be routing-eligible", after.routingEligible);
            Assert.assertEquals("only the rebuilt lead is resident after restart", 2, after.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows recovered", 2, after.leadRowsServed);
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, g, row_number() OVER () AS rn FROM base");
        });
    }

    @Test
    public void testSymbolLeadSurvivesO3() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL, keep INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, g, row_number() OVER () AS rn FROM base WHERE keep > 0");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: flush an in-order batch (the dropped 'aa' reverses base vs
                // LV symbol order, so the lead's interned ids are not identity).
                execute("INSERT INTO base (ts, g, keep) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 'aa', 0), " +
                        "('2026-05-12T00:00:02.000000Z', 'bb', 1), " +
                        "('2026-05-12T00:00:03.000000Z', 'aa', 1)");
                drainWalQueue();
                setCurrentMicros(250_000L);
                drainJob(job);
                drainWalQueue();

                // Refresh a lead with a fresh symbol 'cc' but do NOT flush it.
                execute("INSERT INTO base (ts, g, keep) VALUES ('2026-05-12T00:00:04.000000Z', 'cc', 1)");
                drainWalQueue();
                instance.setLastFlushTimeUs(250_000L); // within FLUSH EVERY 100ms: refresh only
                drainJob(job);

                // O3: a back-dated row carrying another new symbol 'dd' forces a
                // head-miss replay that rewrites the LV table and rebuilds the tier
                // from disk; the un-flushed 'cc' lead is recomputed from base.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, g, keep) VALUES ('2026-05-12T00:00:00.000000Z', 'dd', 1)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job);
                drainWalQueue();
            }

            InnerRead modeA = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-O3 cursor must regain routing", modeA.routingEligible);
            Assert.assertTrue("rebuilt tier serves in-mem rows", modeA.inMemRowsServed > 0);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT ts, g, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:00.000000Z\tdd\t1\n" +
                            "2026-05-12T00:00:02.000000Z\tbb\t2\n" +
                            "2026-05-12T00:00:03.000000Z\taa\t3\n" +
                            "2026-05-12T00:00:04.000000Z\tcc\t4\n");
        });
    }

    @Test
    public void testLeadO3HeadHitReplaysAboveHead() throws Exception {
        // O3 detected with a non-empty un-flushed lead, routed to the head-hit
        // branch. A first flush writes a head .cp at maxTs=03; a lead (ts 10,11)
        // is then refreshed above it without flushing, so headMaxTs stays at 03
        // while the lead leads disk in RAM. A back-dated row at ts=05 sits
        // strictly above headMaxTs (03) and below latestSeenTs (11): it is O3 and
        // head-hit eligible. finishLeadRefresh discards the RAM lead and o3Replay
        // recomputes the tail from base (the lead's base rows are retained, since
        // lvConsumedSeqTxn == applied), so the formerly-RAM-only lead rows land on
        // disk via REPLACE_RANGE and the rebuilt tier regains Mode A.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: flush three in-order rows. The first flush always writes
                // a head .cp; its maxTs is the batch maximum (03).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2), " +
                        "('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                drainJob(job); // clock 0: refresh + first flush -> disk holds 3 rows, head .cp maxTs=03
                drainWalQueue();
                Assert.assertNotEquals("first flush must write a head .cp",
                        Numbers.LONG_NULL, instance.getHeadCheckpointLvSeqTxn());
                Assert.assertEquals("head .cp sits at the flushed batch max",
                        MicrosFormatUtils.parseUTCTimestamp("2026-05-12T00:00:03.000000Z"),
                        instance.getHeadCheckpointMaxTs());

                // Cycle 2: refresh a lead (ts 10,11) above the head without flushing.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:10.000000Z', 10), " +
                        "('2026-05-12T00:00:11.000000Z', 11)");
                drainWalQueue();
                drainJob(job); // clock still 0: within FLUSH EVERY 1s -> refresh only, lead in RAM

                // Precondition: the lead is resident (2 rows) and the head .cp
                // still sits at 03, so the next O3 row at 05 routes head-hit.
                InnerRead beforeO3 = readInner("SELECT * FROM lv");
                Assert.assertEquals("two un-flushed lead rows before O3", 2, beforeO3.leadRowsServed);
                Assert.assertEquals("head still at the flushed batch max",
                        MicrosFormatUtils.parseUTCTimestamp("2026-05-12T00:00:03.000000Z"),
                        instance.getHeadCheckpointMaxTs());

                // Cycle 3: a back-dated row at ts=05 (03 < 05 < 11) is O3 and
                // head-hit eligible. The lead is discarded and recomputed from base.
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:05.000000Z', 5)");
                drainWalQueue();
                drainJob(job); // clock still 0: O3 in the lead drain -> o3Replay head-hit
                drainWalQueue();
            }

            // Post-O3: the lead was absorbed into disk, the tier rebuilt from disk,
            // and a fresh cursor regains Mode A serving the whole window from RAM.
            InnerRead afterO3 = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-O3 cursor must regain Mode A", afterO3.routingEligible);
            Assert.assertEquals("rebuilt tier serves the whole window", 6, afterO3.inMemRowsServed);
            Assert.assertEquals("no un-flushed lead after the O3 recompute", 0, afterO3.leadRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t1\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t2\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t3\n" +
                            "2026-05-12T00:00:05.000000Z\t5\t4\n" +
                            "2026-05-12T00:00:10.000000Z\t10\t5\n" +
                            "2026-05-12T00:00:11.000000Z\t11\t6\n");
        });
    }

    @Test
    public void testLeadO3HeadMissRecomputesFromBase() throws Exception {
        // O3 detected with a non-empty un-flushed lead, routed to the head-miss
        // branch. The back-dated row sits at/below the head's maxTs, so head-hit
        // is not eligible and the replay recomputes the whole view from the lower
        // bound. The RAM-only lead is discarded and recomputed from base (retained
        // because lvConsumedSeqTxn == applied); the rebuilt tier regains Mode A.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2), " +
                        "('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                drainJob(job); // cycle 1: refresh + first flush -> disk holds 3 rows
                drainWalQueue();

                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:10.000000Z', 10), " +
                        "('2026-05-12T00:00:11.000000Z', 11)");
                drainWalQueue();
                drainJob(job); // cycle 2: refresh the lead, no flush

                InnerRead beforeO3 = readInner("SELECT * FROM lv");
                Assert.assertEquals("two un-flushed lead rows before O3", 2, beforeO3.leadRowsServed);

                // Cycle 3: a row back-dated to ts=00 sits below headMaxTs=03, so
                // the replay is head-miss (full recompute from the lower bound).
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            InnerRead afterO3 = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-O3 cursor must regain Mode A", afterO3.routingEligible);
            Assert.assertEquals("rebuilt tier serves the whole window", 6, afterO3.inMemRowsServed);
            Assert.assertEquals("no un-flushed lead after the O3 recompute", 0, afterO3.leadRowsServed);

            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:00.000000Z\t99\t1\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t4\n" +
                            "2026-05-12T00:00:10.000000Z\t10\t5\n" +
                            "2026-05-12T00:00:11.000000Z\t11\t6\n");
        });
    }

    @Test
    public void testLeadO3OracleSurvivesRestart() throws Exception {
        // O3 with a non-empty lead, then a simulated restart. The O3 replay folded
        // the RAM-only lead onto disk (REPLACE_RANGE) and wrote a fresh post-O3
        // head .cp, so after a restart that drops the in-memory tier the on-disk
        // LV table still holds every row and the re-read matches a from-scratch
        // recompute across the restart boundary.
        assertMemoryLeak(() -> {
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
                drainJob(job); // cycle 1: refresh + first flush
                drainWalQueue();

                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:10.000000Z', 10), " +
                        "('2026-05-12T00:00:11.000000Z', 11)");
                drainWalQueue();
                drainJob(job); // cycle 2: refresh the lead, no flush

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertEquals("lead is non-empty before O3", 2, instance.getLeadRowCount());

                // Cycle 3: O3 head-miss folds the lead onto disk.
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertModeBMatchesDiskOnly("SELECT * FROM lv");

            // Simulated restart: drop the in-memory registry (and its tier) and
            // rebuild from on-disk state.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Settle the restored view (rehydrate from the post-O3 head .cp),
                // then ingest one in-order row so the fresh tier repopulates
                // through the normal publish path post-restart.
                drainJob(job);
                drainWalQueue();
                LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(restored);
                restored.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:12.000000Z', 12)");
                drainWalQueue();
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
                            "2026-05-12T00:00:00.000000Z\t99\t1\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t4\n" +
                            "2026-05-12T00:00:10.000000Z\t10\t5\n" +
                            "2026-05-12T00:00:11.000000Z\t11\t6\n" +
                            "2026-05-12T00:00:12.000000Z\t12\t7\n");
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

    // SYMBOL variant of buildFlushedPlusLead: 3 flushed rows (symbols aa=0, bb=1 in
    // LV id space) on disk, then a 2-row un-flushed lead where 'cc' is brand new
    // (assigned id 2, resolvable only from the tier's symbol cache) and 'bb'
    // re-occurs (committed id 1, resolvable via the disk reader). Exercises both
    // overlay bands.
    private void buildSymbolFlushedPlusLead() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                "SELECT ts, g, row_number() OVER () AS rn FROM base");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, g) VALUES " +
                    "('2026-05-12T00:00:01.000000Z', 'aa'), " +
                    "('2026-05-12T00:00:02.000000Z', 'bb'), " +
                    "('2026-05-12T00:00:03.000000Z', 'aa')");
            drainWalQueue();
            drainJob(job); // clock 0: first tick flushes the batch to disk (aa=0, bb=1)

            execute("INSERT INTO base (ts, g) VALUES " +
                    "('2026-05-12T00:00:04.000000Z', 'cc'), " +
                    "('2026-05-12T00:00:05.000000Z', 'bb')");
            drainWalQueue();
            drainJob(job); // clock still 0: refresh the lead (cc new, bb committed), no flush
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
