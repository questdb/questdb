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
    public void testModeBDisabledForSymbolColumn() throws Exception {
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
            // The tier is allocated (SYMBOL stores as INT), but the read path must
            // route disk-only: the tier holds WAL-segment-local symbol ids that the
            // disk reader's symbol table cannot resolve.
            Assert.assertNotNull("tier is still allocated for SYMBOL schemas", instance.getInMemoryTier());
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("SYMBOL output must not be routing-eligible", cursor.isRoutingEligible());
            }
            // The disk-only path resolves symbols correctly.
            assertQuery("SELECT * FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tg\trn\n" +
                            "2026-05-12T00:00:00.000001Z\taa\t1\n" +
                            "2026-05-12T00:00:00.000002Z\tbb\t2\n");
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
                return new InnerRead(out.toString(), cursor.inMemRowsServed(), cursor.isRoutingEligible());
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

    // Captured output and Mode B observability counters from one inner-cursor read.
    private static final class InnerRead {
        final long inMemRowsServed;
        final String output;
        final boolean routingEligible;

        InnerRead(String output, long inMemRowsServed, boolean routingEligible) {
            this.output = output;
            this.inMemRowsServed = inMemRowsServed;
            this.routingEligible = routingEligible;
        }
    }
}
