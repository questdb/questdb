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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
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
    public void testDiskOnlyReadReleasesTierPin() throws Exception {
        // M3: a statically disk-only read - a pruned/reordered projection, a
        // non-table cursor, or a non-ascending scan - can never engage the fence,
        // so LiveViewRecordCursor.of() releases the tier slot pin immediately
        // rather than holding it for the cursor's whole lifetime. Sustained
        // concurrent disk-only reads straddling a tier swap would otherwise pin
        // BOTH slots, so the refresh worker's publishToInMemoryTier fails and it
        // emergency-flushes the lead every cycle. A routing read still pins its slot.
        assertMemoryLeak(() -> {
            createIngestRefresh();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            Assert.assertTrue("published slot must hold rows", tier.getSlot(tier.getPublishedIdx()).rowCount() > 0);
            Assert.assertFalse("no reader open -> slot unpinned", isPublishedSlotReaderPinned(tier));

            // Control: a routing read holds the pin for its lifetime, released on close.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("aligned identity read must route", cursor.isRoutingEligible());
                Assert.assertTrue("a routing read must hold the slot pin", isPublishedSlotReaderPinned(tier));
            }
            Assert.assertFalse("closing the routing read releases the pin", isPublishedSlotReaderPinned(tier));

            // A pruned projection is disk-only and must not hold the pin while open.
            try (
                    RecordCursorFactory factory = select("SELECT rn FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("pruned projection must be disk-only", cursor.isRoutingEligible());
                Assert.assertFalse("a disk-only pruned read must not hold the slot pin", isPublishedSlotReaderPinned(tier));
            }

            // A backward scan is disk-only and must not hold the pin either.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv ORDER BY ts DESC");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("backward scan must be disk-only", cursor.isRoutingEligible());
                Assert.assertFalse("a disk-only backward scan must not hold the slot pin", isPublishedSlotReaderPinned(tier));
            }
        });
    }

    @Test
    public void testVersionFenceMissKeepsTierPin() throws Exception {
        // M3 guard: a version-fence miss (full-schema projection + ascending scan,
        // but the slot is stamped NEWER than the disk snapshot) serves disk-only
        // yet must KEEP the slot pinned, so getCursor's isSlotNewerThanDisk()
        // staleness retry still sees the slot. Only the STATIC disk-only cases
        // (projection shape / scan direction) release the pin early.
        assertMemoryLeak(() -> {
            createIngestRefresh();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
            Assert.assertTrue(slot.rowCount() > 0);

            // Force the slot newer than any reader's snapshot: the fence disengages
            // but the slot must stay pinned for the retry.
            slot.setLvSeqTxn(slot.lvSeqTxn() + 1000);
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("seqTxn mismatch must not route", cursor.isRoutingEligible());
                Assert.assertTrue("a version-fence miss must keep the slot pinned for the retry", isPublishedSlotReaderPinned(tier));
            }
        });
    }

    @Test
    public void testReorderedSameTypeProjectionRoutesDiskOnly() throws Exception {
        // C1 regression: the in-mem tier stores the LV's output row in declared
        // column order and MergedRecord indexes the buffer by output position. A
        // reordered projection over two same-typed columns (SELECT ts, b, a FROM
        // lv, with a and b both INT) shares the buffer's column count and its
        // per-position types, so the pre-fix count + type gate wrongly engaged the
        // tier and served a where b was expected: the optimiser fuses the reorder
        // into the page-frame scan as a reordered column mapping ([0, 2, 1, 3]),
        // leaving no SelectedRecord wrapper to correct it. The identity
        // column-mapping check now routes such a read disk-only (always correct),
        // while an in-declared-order read still routes through the tier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, a INT, b INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, a, b, row_number() OVER () AS rn FROM base");
            execute("INSERT INTO base (ts, a, b) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 10, 20), " +
                    "('2026-05-12T00:00:00.000002Z', 11, 21)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Positive control: the in-declared-order full-schema read routes
            // through the tier (identity mapping) and agrees with disk-only.
            try (
                    RecordCursorFactory factory = select("SELECT ts, a, b, rn FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("in-declared-order read must route through the tier", cursor.isRoutingEligible());
            }
            assertModeBMatchesDiskOnly("SELECT ts, a, b, rn FROM lv");

            // The reorder swaps two same-typed columns: pre-fix the tier engaged
            // and served swapped values. It must now route disk-only, and the
            // values must be correct (b then a).
            try (
                    RecordCursorFactory factory = select("SELECT ts, b, a, rn FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("reordered same-type projection must route disk-only", cursor.isRoutingEligible());
            }
            assertModeBMatchesDiskOnly("SELECT ts, b, a, rn FROM lv");
            assertQuery("SELECT ts, b, a, rn FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tb\ta\trn\n" +
                            "2026-05-12T00:00:00.000001Z\t20\t10\t1\n" +
                            "2026-05-12T00:00:00.000002Z\t21\t11\t2\n");
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
    public void testInMemVarSizeRecordsAreIndependent() throws Exception {
        // recordA and recordB must be independent consumers: a var-size value read
        // from recordA has to survive positioning recordB elsewhere. The disk read
        // path honours this because recordA and recordB delegate to two separate
        // disk records, each with its own column flyweights. The in-mem tier,
        // however, routes BOTH records' getStrA/getVarcharA to the same per-column
        // buffer flyweight (the data/aux column's csviewA / utf8SplitViewA), keyed
        // on the getter name rather than the record, so recordB's read re-points and
        // clobbers recordA's still-live value. This is a single-thread, deterministic
        // proof of the aliasing that also lets two concurrent reader cursors tear
        // each other's var-size reads (they share the one pinned slot's flyweights).
        assertMemoryLeak(() -> {
            createVarSizeSeamSplitLv();
            try (RecordCursorFactory factory = select("SELECT * FROM lv")) {
                LiveViewRecordCursorFactory lvf = unwrapLvFactory(factory);
                try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvf.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("view must serve the in-mem tier (Mode B)", cursor.isRoutingEligible());
                    Record recordA = cursor.getRecord();

                    // Forward pass: collect the in-mem row ids (sign-bit tagged) and a
                    // stable copy of each in-mem row's var-size values.
                    LongList inMemIds = new LongList();
                    ObjList<String> expectedStr = new ObjList<>();
                    ObjList<String> expectedVarchar = new ObjList<>();
                    while (cursor.hasNext()) {
                        long rowId = recordA.getRowId();
                        if (rowId < 0) { // in-mem row
                            inMemIds.add(rowId);
                            expectedStr.add(recordA.getStrA(1).toString());
                            Utf8Sequence vc = recordA.getVarcharA(2);
                            expectedVarchar.add(vc == null ? null : vc.toString());
                        }
                    }
                    Assert.assertTrue(
                            "need at least two in-mem rows for the aliasing check",
                            inMemIds.size() >= 2
                    );

                    Record recordB = cursor.getRecordB();

                    // Position recordA at the first in-mem row and read its values.
                    cursor.recordAt(recordA, inMemIds.getQuick(0));
                    CharSequence strA = recordA.getStrA(1);
                    Utf8Sequence vcharA = recordA.getVarcharA(2);
                    Assert.assertEquals(expectedStr.get(0), strA.toString());
                    Assert.assertEquals(expectedVarchar.get(0), vcharA.toString());

                    // Position recordB at a different in-mem row and read its values.
                    // If the flyweights alias, this re-points the very objects strA /
                    // vcharA still reference.
                    cursor.recordAt(recordB, inMemIds.getQuick(1));
                    CharSequence strB = recordB.getStrA(1);
                    Utf8Sequence vcharB = recordB.getVarcharA(2);
                    Assert.assertEquals(expectedStr.get(1), strB.toString());
                    Assert.assertEquals(expectedVarchar.get(1), vcharB.toString());

                    // recordA's values must be unchanged by recordB's reads.
                    Assert.assertEquals(
                            "recordA STRING clobbered by recordB read (in-mem flyweight aliasing)",
                            expectedStr.get(0), strA.toString()
                    );
                    Assert.assertEquals(
                            "recordA VARCHAR clobbered by recordB read (in-mem flyweight aliasing)",
                            expectedVarchar.get(0), vcharA.toString()
                    );
                }
            }
        });
    }

    @Test
    public void testInMemSymbolGetSymBIndependentOfGetSymA() throws Exception {
        // getSymB must resolve through the symbol table's B-flyweight, not the A one.
        // A consumer that holds getSymA of one in-mem row and getSymB of another (a
        // self ASOF/LT-join RHS, an A/B comparator) reads both from the ONE overlay
        // this cursor exposes via getSymbolTable(col). With a non-cached SYMBOL column
        // the overlay's disk base returns two distinct reused flyweights for
        // valueOf/valueBOf; routing getSymB through valueOf (the bug) would re-point
        // the very flyweight getSymA still references, so the second read clobbers the
        // first. This is the SYMBOL analogue of testInMemVarSizeRecordsAreIndependent.
        assertMemoryLeak(() -> {
            createSymbolSeamSplitLvNoCache();
            try (RecordCursorFactory factory = select("SELECT * FROM lv")) {
                LiveViewRecordCursorFactory lvf = unwrapLvFactory(factory);
                try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvf.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("view must serve the in-mem tier (Mode B)", cursor.isRoutingEligible());
                    Record recordA = cursor.getRecord();

                    // Forward pass: collect the in-mem row ids (sign-bit tagged) and a
                    // stable copy of each in-mem row's symbol value. The slot rows are
                    // flushed (overlap), so the symbol resolves via the disk base.
                    LongList inMemIds = new LongList();
                    ObjList<String> expectedSym = new ObjList<>();
                    while (cursor.hasNext()) {
                        long rowId = recordA.getRowId();
                        if (rowId < 0) { // in-mem row
                            inMemIds.add(rowId);
                            CharSequence sym = recordA.getSymA(1);
                            expectedSym.add(sym == null ? null : sym.toString());
                        }
                    }
                    Assert.assertTrue(
                            "need at least two in-mem rows with distinct symbols",
                            inMemIds.size() >= 2
                    );
                    Assert.assertNotEquals(
                            "the two probed in-mem symbols must differ",
                            expectedSym.get(0), expectedSym.get(1)
                    );

                    Record recordB = cursor.getRecordB();

                    // recordA reads the first in-mem row's symbol via getSymA.
                    cursor.recordAt(recordA, inMemIds.getQuick(0));
                    CharSequence symA = recordA.getSymA(1);
                    Assert.assertEquals(expectedSym.get(0), symA.toString());

                    // recordB reads a different in-mem row's symbol via getSymB. If
                    // getSymB aliased the A-flyweight, this re-points the object symA
                    // still references.
                    cursor.recordAt(recordB, inMemIds.getQuick(1));
                    CharSequence symB = recordB.getSymB(1);
                    Assert.assertEquals(expectedSym.get(1), symB.toString());

                    // symA must be unchanged by recordB's getSymB read.
                    Assert.assertEquals(
                            "recordA SYMBOL clobbered by recordB getSymB (A/B flyweight aliasing)",
                            expectedSym.get(0), symA.toString()
                    );
                }
            }
        });
    }

    @Test
    public void testInMemSymbolGetSymAIndependentAcrossRecords() throws Exception {
        // recordA and recordB are independent random-access consumers, so a
        // getSymA read off recordA must survive a getSymA read off recordB. The disk
        // path honours this (each record clones its own symbol table). Routing both
        // through the ONE cursor-level overlay shares its single valueOf flyweight -
        // a reused DirectString with a NOCACHE column - so recordB's getSymA re-points
        // the object recordA's getSymA still holds. Cross-record analogue of
        // testInMemSymbolGetSymBIndependentOfGetSymA (which only covers within-cursor A/B).
        assertMemoryLeak(() -> {
            createSymbolSeamSplitLvNoCache();
            try (RecordCursorFactory factory = select("SELECT * FROM lv")) {
                LiveViewRecordCursorFactory lvf = unwrapLvFactory(factory);
                try (LiveViewRecordCursor cursor = (LiveViewRecordCursor) lvf.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("view must serve the in-mem tier (Mode B)", cursor.isRoutingEligible());
                    Record recordA = cursor.getRecord();

                    LongList inMemIds = new LongList();
                    ObjList<String> expectedSym = new ObjList<>();
                    while (cursor.hasNext()) {
                        long rowId = recordA.getRowId();
                        if (rowId < 0) { // in-mem row
                            inMemIds.add(rowId);
                            CharSequence sym = recordA.getSymA(1);
                            expectedSym.add(sym == null ? null : sym.toString());
                        }
                    }
                    Assert.assertTrue(
                            "need at least two in-mem rows with distinct symbols",
                            inMemIds.size() >= 2
                    );
                    Assert.assertNotEquals(
                            "the two probed in-mem symbols must differ",
                            expectedSym.get(0), expectedSym.get(1)
                    );

                    Record recordB = cursor.getRecordB();

                    // recordA reads the first in-mem row's symbol via getSymA.
                    cursor.recordAt(recordA, inMemIds.getQuick(0));
                    CharSequence symA = recordA.getSymA(1);
                    Assert.assertEquals(expectedSym.get(0), symA.toString());

                    // recordB reads a DIFFERENT in-mem row's symbol via getSymA (the same
                    // accessor). If the two records shared one overlay, this re-points the
                    // object symA still references.
                    cursor.recordAt(recordB, inMemIds.getQuick(1));
                    CharSequence symB = recordB.getSymA(1);
                    Assert.assertEquals(expectedSym.get(1), symB.toString());

                    // symA must be unchanged by recordB's getSymA read.
                    Assert.assertEquals(
                            "recordA SYMBOL clobbered by recordB getSymA (shared overlay aliasing)",
                            expectedSym.get(0), symA.toString()
                    );
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
            // suffix. Each routing-eligible shape must both engage the tier (its
            // inner cursor serves in-mem rows) AND match the disk-only path byte for
            // byte. The engagement gate matters: the differential oracle alone would
            // still pass if a fence regression silently routed a shape disk-only,
            // because both sides would then read disk.
            assertModeBEngagesAndMatchesDiskOnly("SELECT * FROM lv");
            assertModeBEngagesAndMatchesDiskOnly("SELECT * FROM lv LIMIT 3");
            assertModeBEngagesAndMatchesDiskOnly("SELECT * FROM lv LIMIT -2");
            assertModeBEngagesAndMatchesDiskOnly("SELECT * FROM lv WHERE x > 2");
            // ORDER BY ts DESC is the non-routing control: a backward scan is
            // deliberately fenced disk-only (testModeBDisabledForBackwardScan), so
            // both sides read disk here. It still must match, but the tier serves
            // nothing - assert that so this line is not mistaken for Mode B coverage.
            InnerRead desc = readInner("SELECT * FROM lv ORDER BY ts DESC");
            Assert.assertFalse("backward scan must fence disk-only", desc.routingEligible);
            Assert.assertEquals("backward scan must not serve the tier", 0, desc.inMemRowsServed);
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
        // re-sequenced on disk. The tierStale flag routes that next lead publish
        // through a flush-to-disk plus a full tier rebuild (see finishLeadRefresh),
        // so the slot reflects only disk-consistent rows again.
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
    public void testO3RebuildSkipThenAdditiveFrontierTieKeepsAllRows() throws Exception {
        // M1a: the both-slots-pinned O3-rebuild-skip leaves tierStale set. The next
        // lead publish used to take the dropRetained path, which drops the overlap
        // and seams a pure-lead slot at the lead's minimum timestamp. When that
        // minimum equals a disk row's timestamp - an additive same-ts row at the
        // frontier, not diverted to O3 (its trigger is a strict below-frontier
        // compare) - the disk row at exactly the seam is served by neither disk
        // (which stops strictly below the seam) nor the slot (which holds only the
        // lead), so it is silently lost and size() overcounts. The fix flushes the
        // lead to disk and rebuilds the tier as a clean subset, keeping the overlap
        // so no row falls in the gap.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
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

                // Pin slot 0, slow-path swap to slot 1, then pin slot 1 too.
                final int pinA = tier.acquireRead();
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:04.000000Z', 4)");
                drainWalQueue();
                setCurrentMicros(500_000L);
                drainJob(job);
                drainWalQueue();
                final int pinB = tier.acquireRead();
                Assert.assertNotEquals("both slots must be pinned", pinA, pinB);

                // O3 row: a back-dated head-miss replay rewrites the LV table
                // (frontier stays at ts=04) and tries to rebuild the tier. Both slots
                // are pinned, so the rebuild is skipped and tierStale is set.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                setCurrentMicros(750_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertTrue("tier must be stale after the rebuild skip", instance.isTierStale());

                tier.releaseRead(pinA);
                tier.releaseRead(pinB);

                // Forward cycle: an ADDITIVE same-ts row at exactly the disk frontier
                // (ts=04). leadMin == diskMaxTs - the case a pure-lead seam would drop
                // the existing disk row at ts=04.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:04.000000Z', 6)");
                drainWalQueue();
                setCurrentMicros(1_000_000L);
                drainJob(job);
                drainWalQueue();
            }

            // Both rows at ts=04 must survive: Mode B must equal disk-only and size()
            // must match the iterated count.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            assertQuery("SELECT ts, x, rn FROM lv")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:00.000000Z\t99\t1\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t2\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t3\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t4\n" +
                            "2026-05-12T00:00:04.000000Z\t4\t5\n" +
                            "2026-05-12T00:00:04.000000Z\t6\t6\n");
        });
    }

    @Test
    public void testNonCapableO3ResyncsLeadRowCount() throws Exception {
        // M1b: finishLeadRefresh zeroes instance.leadRowCount before o3Replay because
        // the capable path rebuilds the tier as a pure disk subset. The non-capable
        // o3Replay branch rewrites nothing on disk and leaves the slot's un-flushed
        // lead in place, so instance.leadRowCount used to stay 0 while the slot still
        // stamped L. The next publish then reclassified those L never-flushed rows as
        // overlap: size() under-reported by L while iteration served them as phantoms.
        // The fix resyncs instance.leadRowCount to the untouched slot.
        //
        // CREATE rejects every non-snapshot-capable window shape (each
        // WindowFunction.supportsSnapshot() folds in the anchor key type check), so a
        // freshly-validated view never reaches the non-capable branch - it is a
        // defensive path for a runtime-non-capable view (e.g. a restored view whose
        // function lost snapshot support). This test forces that state directly via
        // setSnapshotCapability(false) to exercise the branch and pin the resync.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // Pin the clock, then pin the flush clock to it before every cycle so the
            // FLUSH EVERY 1s cadence never fires and the lead stays un-flushed in RAM.
            setCurrentMicros(1_000L);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: two in-order rows build an un-flushed lead (L = 2).
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();
                Assert.assertTrue("cycle 1 must build an un-flushed lead", instance.getLeadRowCount() >= 2);

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                final long slotLead = tier.getSlot(tier.getPublishedIdx()).leadRowCount();
                Assert.assertEquals("the slot must stamp the un-flushed lead", 2, slotLead);

                // Force the runtime-non-capable state CREATE normally gates, so the
                // next O3 takes o3Replay's non-capable branch (advance watermarks, no
                // rebuild) instead of the capable replay.
                instance.setSnapshotCapability(false);

                // Cycle 2: a back-dated O3 row -> non-capable branch. It leaves the
                // slot untouched (its stamped leadRowCount stays L=2). Pre-fix
                // instance.leadRowCount stayed at the pre-o3Replay 0, desyncing from
                // the slot; the fix resyncs it back to the slot's L.
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "the non-capable branch must resync instance.leadRowCount to the untouched slot",
                        tier.getSlot(tier.getPublishedIdx()).leadRowCount(),
                        instance.getLeadRowCount()
                );

                // Cycle 3: a forward row. Its publish computes newLeadRowCount from
                // instance.leadRowCount; only a resynced count keeps the slot's lead
                // classification correct (all three rows are still un-flushed lead, not
                // two of them silently reclassified as overlap).
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();

                LiveViewInMemoryBuffer pub = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals("all three rows are un-flushed lead", 3, pub.rowCount());
                Assert.assertEquals("no lead row may be reclassified as overlap", 3, pub.leadRowCount());
                Assert.assertEquals("instance and slot lead counts must agree", 3, instance.getLeadRowCount());
            }
        });
    }

    @Test
    public void testTierStaleEmergencyFlushThenNonCapableO3DoesNotDuplicate() throws Exception {
        // Regression for the M1a x M1b interaction: a non-snapshot-capable, lead-eligible
        // view whose tier went stale via an EMERGENCY FLUSH must not re-flush the stale
        // slot's already-durable lead rows on the next forward cycle.
        //
        // Cycle A: both tier slots reader-pinned -> a forward lead publish stalls, so the
        //   emergency flush writes the un-flushed lead straight to disk, sets tierStale, and
        //   zeroes instance.leadRowCount -- but leaves the published slot STILL stamped with
        //   its now-durable leadRowCount P.
        // Cycle B: a back-dated O3 on a (forced) non-capable view takes o3Replay's
        //   non-capable branch. Its M1b resync must NOT copy the stale slot's P back into
        //   instance.leadRowCount (those P rows are already on disk, not an un-flushed lead).
        // Cycle C: a forward row hits finishLeadRefresh's tierStale branch, whose flushLead
        //   trusts leadRowCount to be the un-flushed lead. Pre-fix, priorLead = P re-flushed
        //   the P rows already written in cycle A -> duplicate rows on disk + size() overcount.
        //
        // ensureLeadEligible gates on designated-timestamp + tier-storable types only, NOT
        // snapshot capability, so a non-capable view genuinely reaches this lead path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            // Pin the flush clock before each cycle so FLUSH EVERY 1s never fires and the
            // lead stays un-flushed in RAM until the emergency flush.
            setCurrentMicros(1_000L);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 0: build a two-row un-flushed lead in slot A.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                final int pinA = tier.acquireRead();

                // Cycle 1: A is pinned, so this forward publish slow-path swaps to slot B
                // (the lead is now three rows: 01, 02, 03).
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();
                final int pinB = tier.acquireRead();
                Assert.assertNotEquals("both slots must be pinned", pinA, pinB);
                Assert.assertTrue("the published slot must stamp its un-flushed lead",
                        tier.getSlot(tier.getPublishedIdx()).leadRowCount() > 0);

                // Cycle A: both slots pinned -> the forward lead publish stalls and the
                // emergency flush writes rows 01-04 to disk, sets tierStale, and zeroes
                // instance.leadRowCount. The published slot keeps its now-durable lead stamp.
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:04.000000Z', 4)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();
                Assert.assertTrue("emergency flush must mark the tier stale", instance.isTierStale());
                Assert.assertEquals("emergency flush zeroes instance.leadRowCount", 0, instance.getLeadRowCount());
                Assert.assertTrue("the stale slot still stamps its now-durable lead",
                        tier.getSlot(tier.getPublishedIdx()).leadRowCount() > 0);

                tier.releaseRead(pinA);
                tier.releaseRead(pinB);

                // Force the runtime-non-capable state so the next O3 takes o3Replay's
                // non-capable branch (the M1b resync path).
                instance.setSnapshotCapability(false);

                // Cycle B: a back-dated O3 -> non-capable branch. The M1b resync must skip
                // the tierStale slot; instance.leadRowCount must stay 0 (pre-fix it was
                // re-armed to the stale slot's 3).
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "a tierStale slot's stamped lead is already on disk; M1b must not re-arm leadRowCount from it",
                        0, instance.getLeadRowCount()
                );

                // Cycle C: a forward row hits the tierStale branch. Pre-fix priorLead = 3
                // re-flushed rows 01-03 as duplicates.
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:05.000000Z', 5)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros);
                drainJob(job);
                drainWalQueue();
            }

            // The emergency-flushed rows 01, 02, 03 must each appear exactly once, so the
            // count over them is 3. Pre-fix the cycle-C tierStale re-flush wrote them a
            // second time (count = 6).
            assertQuery("SELECT count() n FROM lv WHERE x >= 1 AND x <= 3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\n3\n");
        });
    }

    @Test
    public void testNormalFlushRestampFailThenNonCapableO3DoesNotDuplicate() throws Exception {
        // Sibling of testTierStaleEmergencyFlushThenNonCapableO3DoesNotDuplicate: the
        // SAME on-disk duplication reached through a NORMAL cadence flush whose restamp
        // CAS fails instead of an emergency flush. When one reader pins the published
        // slot across a normal flush, restampSlotAfterFlush's 0 -> -1 CAS loses, so the
        // slot keeps its now-durable leadRowCount stamp -- but a normal flush never sets
        // tierStale. An o3Replay non-capable resync gated only on !tierStale would then
        // re-arm instance.leadRowCount from that durable stamp, and the next flush would
        // re-flush the already-durable rows as on-disk duplicates. The resync instead
        // gates on the slot's stamped seqTxn still matching the applied disk seqTxn,
        // which excludes this stale-stamped slot as well as the tierStale one.
        //
        // Cycle 0: build a two-row un-flushed lead in the published slot.
        // Cycle A: pin the published slot, then a fully-filtered commit (0 output rows)
        //   fires the cadence flush without a publish/swap -> the same slot stays
        //   published, its restamp CAS fails, and tierStale stays FALSE.
        // Cycle B: a back-dated O3 on a (forced) non-capable view. The resync must NOT
        //   re-arm instance.leadRowCount from the stale-stamped slot.
        // Cycle C: a forward row + flush must not re-flush rows 01-02.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            setCurrentMicros(1_000L);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 0: build a two-row un-flushed lead in the published slot.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2)");
                drainWalQueue();
                instance.setLastFlushTimeUs(currentMicros); // suppress the cadence flush
                drainJob(job);
                drainWalQueue();

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                Assert.assertEquals("two-row un-flushed lead", 2, instance.getLeadRowCount());
                final int publishedIdx = tier.getPublishedIdx();

                // Pin the published slot so the upcoming NORMAL flush's restamp CAS fails.
                final int pin = tier.acquireRead();

                // Cycle A: a fully-filtered commit (x <= 0 -> 0 output rows) advances head
                // so refreshInstance runs and the cadence flush fires, but there is no
                // publish/swap, so the SAME slot stays published. flushLead writes the 2
                // lead rows to disk and zeroes instance.leadRowCount; restampSlotAfterFlush's
                // CAS fails (slot pinned), so the slot keeps its now-durable leadRowCount=2
                // stamp, and a normal flush never sets tierStale.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL); // force the flush due
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:03.000000Z', -1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals("published slot unchanged", publishedIdx, tier.getPublishedIdx());
                Assert.assertFalse("a normal flush never sets tierStale", instance.isTierStale());
                Assert.assertEquals("normal flush zeroes instance.leadRowCount", 0, instance.getLeadRowCount());
                Assert.assertTrue("the slot keeps its now-durable lead stamp (restamp CAS failed)",
                        tier.getSlot(tier.getPublishedIdx()).leadRowCount() > 0);

                tier.releaseRead(pin);

                // Force the runtime-non-capable state so the next O3 takes o3Replay's
                // non-capable branch (the resync path).
                instance.setSnapshotCapability(false);

                // Cycle B: a back-dated O3 -> non-capable branch. The slot's stamped
                // seqTxn no longer matches the applied disk seqTxn (the normal flush
                // advanced disk while the restamp CAS failed), so the resync must leave
                // instance.leadRowCount at 0 -- those rows are already durable.
                instance.setLastFlushTimeUs(currentMicros); // suppress the flush this cycle
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 99)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "a stale-stamped slot's lead is already on disk; the resync must not re-arm leadRowCount",
                        0, instance.getLeadRowCount()
                );

                // Cycle C: a forward row + flush. Pre-fix, priorLead = 2 re-flushed rows
                // 01-02 as duplicates.
                instance.setLastFlushTimeUs(Numbers.LONG_NULL);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:05.000000Z', 5)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            // Rows 01 and 02 must each appear exactly once (count = 2). Pre-fix the
            // cycle-C re-flush wrote them a second time (count = 4).
            assertQuery("SELECT count() n FROM lv WHERE x >= 1 AND x <= 2")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("n\n2\n");
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
    public void testDedupOverlapRebuildUsesOwnSchemaNotSiblingViews() throws Exception {
        // C2 regression. The staging buffer (stagingBuffer / stagingColumnTypes in
        // LiveViewRefreshJob) is a per-WORKER field shared across every view a
        // refresh worker serves, reshaped only in ensureStagingAndTier. The dedup
        // overlap branch of drainAppliedBase used to run o3Replay ->
        // rebuildInMemoryTier BEFORE calling ensureStagingAndTier, so it staged
        // THIS view's LV disk columns through whatever schema the worker last served
        // (a sibling view), then stamped the rebuilt slot with the current disk
        // seqTxn - so the read fence held and the tier served corrupt rows.
        //
        // Two views on one job with the SAME output column count but a different
        // type at column index 1 (DOUBLE for the victim, INT for the sibling). Same
        // count means the stale-schema stager does not fault on an index mismatch;
        // the wider-victim (8 bytes) read through the narrower sibling stride (4
        // bytes) corrupts the val column silently. Drive the sibling last so the
        // shared staging buffer holds its INT shape, then force the dedup view onto
        // the overlap rebuild with a below-frontier UPSERT. Pre-fix the rebuilt
        // tier's val column diverges from disk; post-fix the up-front reshape makes
        // the rebuild read the victim's own DOUBLE schema.
        assertMemoryLeak(() -> {
            // Victim: DOUBLE val over a DEDUP base -> the coupled applied-reader path.
            execute("CREATE TABLE base_a (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base_a");
            // Sibling: INT at the same column index, same total column count.
            execute("CREATE TABLE base_b (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lvb FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base_b");

            LiveViewInstance instanceA = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instanceA);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: the victim's initial forward append. Flushes 3 rows to the
                // LV table, populates the tier, and sets the frontier to ts=03.
                execute("INSERT INTO base_a (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30.0, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Cycle 2: refresh the SIBLING. Its ensureStagingAndTier reshapes the
                // shared worker staging buffer to the INT schema - the stale shape the
                // victim's overlap rebuild must NOT reuse.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base_b (sym, val, ts) VALUES " +
                        "('a', 1, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 2, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Cycle 3: a below-frontier dedup UPSERT on base_a (ts=02, val 20 ->
                // 999) forces the victim onto drainAppliedBase's overlap branch ->
                // o3Replay -> rebuildInMemoryTier. The disk REPLACE_RANGE is correct
                // either way; only the in-mem rebuild reads the staging schema, which
                // is still the sibling's INT shape until the fix reshapes it.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base_a (sym, val, ts) VALUES ('a', 999.0, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            // A cursor opened right after the overlap rebuild routes through the tier:
            // the slot was stamped with the current disk seqTxn, so the fence holds.
            InnerRead afterOverlap = readInner("SELECT * FROM lv");
            Assert.assertTrue("overlap rebuild must keep the tier routable", afterOverlap.routingEligible);
            Assert.assertTrue("the rebuilt tier must serve in-mem rows", afterOverlap.inMemRowsServed > 0);

            // The served tier must equal disk (fence forced off). Pre-fix the DOUBLE
            // val column was staged through the sibling's INT stride, so they diverge.
            assertModeBMatchesDiskOnly("SELECT * FROM lv");
            // ... and equal a from-scratch recompute over the post-dedup base.
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base_a");

            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10.0\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t999.0\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t30.0\t2026-01-01T00:00:03.000000Z\t3\n");
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
    public void testFilteredReadFiltersUnflushedLead() throws Exception {
        // Regression guard for a filter-bypass concern flagged in review: does a
        // WHERE over an LV skip tier rows (the disk-backed overlap and the
        // un-flushed lead served from RAM) and over-return them?
        //
        // It does not, and cannot, with the current pushdown rules. A predicate that
        // an LV read cannot turn into an intrinsic index scan (LV tables never carry
        // an index) is re-attached to the model by generateTableQuery0
        // (model.setWhereClause(intrinsicModel.filter)) and applied by a Filter node
        // wrapping the LiveView; the base cursor the LiveView routes through is an
        // unfiltered full forward scan. So every row the tier yields - overlap AND
        // lead - passes through the outer filter. (A timestamp-interval predicate is
        // the one shape pushed under the tier, and the routing fence disables tier
        // routing for it; a backward LATEST BY scan is disabled by the ascending-scan
        // fence.) This test pins that a filtered read matches a from-scratch oracle
        // and never leaks a non-matching tier row, including a lead-only symbol.
        assertMemoryLeak(() -> {
            buildSymbolFlushedPlusLead();

            // The tier is actively leading disk: 5 rows resident, 2 of them the
            // un-flushed lead (cc @ rn=4, bb @ rn=5). This is the state under which a
            // filter bypass would surface as over-returned tier rows.
            InnerRead lead = readInner("SELECT * FROM lv");
            Assert.assertTrue("lead read must be routing-eligible", lead.routingEligible);
            Assert.assertEquals("all rows served from the tier", 5, lead.inMemRowsServed);
            Assert.assertEquals("two un-flushed lead rows served from RAM", 2, lead.leadRowsServed);

            // The read must be full-schema (SELECT *) so it routes through the tier
            // and serves the lead; a pruned projection (e.g. SELECT g) would fence to
            // disk-only and never see the lead. g='bb' matches one disk-overlap row
            // (rn=2) and one un-flushed lead row (rn=5). rn=5 lives only in RAM, so
            // its presence in the filtered output proves the lead is both served AND
            // filtered; the aa/cc rows must be dropped even though the tier serves
            // every one of them from RAM. Differential oracle (a from-scratch recompute
            // over base) plus an explicit expectation. printSql, not assertQuery: the
            // latter's battery clears the engine and drops the un-flushed lead.
            assertLvMatchesOracle("SELECT * FROM lv WHERE g = 'bb'",
                    "SELECT * FROM (SELECT ts, g, row_number() OVER () AS rn FROM base) WHERE g = 'bb'");
            StringSink bb = new StringSink();
            printSql("SELECT * FROM lv WHERE g = 'bb'", bb);
            Assert.assertEquals("ts\tg\trn\n" +
                    "2026-05-12T00:00:02.000000Z\tbb\t2\n" +
                    "2026-05-12T00:00:05.000000Z\tbb\t5\n", bb.toString());

            // A lead-only symbol ('cc', first seen in the un-flushed lead) is returned
            // through the filter - the freshest match, correctly filtered.
            StringSink cc = new StringSink();
            printSql("SELECT * FROM lv WHERE g = 'cc'", cc);
            Assert.assertEquals("ts\tg\trn\n2026-05-12T00:00:04.000000Z\tcc\t4\n", cc.toString());

            // A disk-only symbol ('aa', never in the lead) returns just its overlap rows.
            StringSink aa = new StringSink();
            printSql("SELECT * FROM lv WHERE g = 'aa'", aa);
            Assert.assertEquals("ts\tg\trn\n" +
                    "2026-05-12T00:00:01.000000Z\taa\t1\n" +
                    "2026-05-12T00:00:03.000000Z\taa\t3\n", aa.toString());
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
    public void testFullEvictionKeepsSameTsOverlapRow() throws Exception {
        // C4 regression: a full in-memory eviction (the whole overlap ages out and
        // only the un-flushed lead survives) must not drop a disk-backed overlap
        // row that shares the lead's minimum timestamp. Such an additive same-ts
        // frontier row is admitted because the O3 trigger is a strict
        // below-frontier compare (txnMinTs < latestSeen): it lands on disk as
        // overlap AND again in the lead at the same timestamp. Pre-fix the eviction
        // seamed the lead-only slot at lead_min and evicted the on-disk overlap
        // copy, so the reader served neither - the disk scan stops strictly below
        // the seam and the slot holds only the lead - dropping the row while size()
        // still counted it, which breaks LIMIT. The fix clamps the eviction
        // threshold to lead_min so the same-ts overlap group stays resident at the
        // seam. The fuzz suite cannot catch this: it uses unique timestamps by
        // construction.
        assertMemoryLeak(() -> {
            // growth.bytes = 0 forces the slow-path (and its IN MEMORY eviction) on
            // every publish.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            setCurrentMicros(0L);
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Cycle 1: flush three in-order rows to disk. The frontier is ts=03.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:01.000000Z', 1), " +
                        "('2026-05-12T00:00:02.000000Z', 2), " +
                        "('2026-05-12T00:00:03.000000Z', 3)");
                drainWalQueue();
                drainJob(job); // clock 0: first tick flushes the batch to disk
                drainWalQueue();

                // Cycle 2: an additive same-ts row at the frontier (ts=03 ==
                // latestSeen) is NOT O3, so it lands as an un-flushed lead on top of
                // the on-disk overlap that also holds ts=03. FLUSH EVERY 1s has not
                // elapsed since the cycle-1 flush (clock still 0), so it stays
                // un-flushed.
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:03.000000Z', 4)");
                drainWalQueue();
                drainJob(job); // clock still 0: refresh only, lead in RAM
                drainWalQueue();

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                LiveViewInMemoryBuffer pre = tier.getSlot(tier.getPublishedIdx());
                // The slot carries the additive same-ts frontier: an overlap row at
                // ts=03 (on disk) directly below the lead row at ts=03 (RAM only).
                Assert.assertEquals("one un-flushed lead row before the far cycle", 1, pre.leadRowCount());
                final int tsCol = pre.getTimestampColumnIndex();
                final long leadStart = pre.rowCount() - pre.leadRowCount();
                final long leadMin = pre.getLong(leadStart, tsCol);
                final long overlapMax = pre.getLong(leadStart - 1, tsCol);
                Assert.assertEquals("overlap max must share the lead's minimum timestamp", leadMin, overlapMax);

                // Cycle 3: two rows far beyond lead_min + IN MEMORY push the eviction
                // threshold above the whole overlap, so it ages out entirely. Pre-fix
                // the seam lands at lead_min (ts=03) with only the lead retained; the
                // fix keeps the ts=03 overlap row resident at the seam.
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-05-12T00:00:10.000000Z', 5), " +
                        "('2026-05-12T00:00:11.000000Z', 6)");
                drainWalQueue();
                drainJob(job); // clock still 0: refresh only, full eviction of the overlap
                drainWalQueue();
            }

            // The tier still routes and the read matches a from-scratch recompute -
            // the additive same-ts disk row at ts=03 is served, not dropped.
            InnerRead read = readInner("SELECT * FROM lv");
            Assert.assertTrue("post-eviction read must stay routing-eligible", read.routingEligible);
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            // size() must equal the served row count so LIMIT pushdown is exact.
            // Pre-fix the vanished disk row left size() one over the rows actually
            // served, so a LIMIT past the boundary diverged from the oracle.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                Assert.assertEquals("size() must count every served row exactly once", 6, cursor.size());
            }
            assertLvMatchesOracle("SELECT * FROM lv LIMIT 6",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base LIMIT 6");

            // Explicit full result: both ts=03 rows present - the disk overlap
            // (rn=3) and the lead (rn=4) - in ts then rn order.
            StringSink out = new StringSink();
            printSql("SELECT * FROM lv", out);
            Assert.assertEquals("ts\tx\trn\n" +
                    "2026-05-12T00:00:01.000000Z\t1\t1\n" +
                    "2026-05-12T00:00:02.000000Z\t2\t2\n" +
                    "2026-05-12T00:00:03.000000Z\t3\t3\n" +
                    "2026-05-12T00:00:03.000000Z\t4\t4\n" +
                    "2026-05-12T00:00:10.000000Z\t5\t5\n" +
                    "2026-05-12T00:00:11.000000Z\t6\t6\n", out.toString());
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
    public void testRestartWithCorruptHeadCheckpointRebuildsFromBase() throws Exception {
        // C2 regression: a STRUCTURALLY corrupt head .cp on restart (bit rot /
        // truncation / a renamed window-function class - all errno 0) makes
        // restoreFromHead trip the CRC check, unlink the .cp, and clear the head
        // metadata WITHOUT stashing an invalidation reason. Before the fix
        // tryRestoreFromHead bare-returned, so the caller fell through to the
        // incremental drain from the applied watermark with COLD window
        // accumulators: row_number() (and every cumulative window function)
        // recomputed the post-watermark rows from zero and durably flushed the
        // wrong values (disk rn 1..3 + a cold-restart lead rn 1..2 instead of
        // 4..5). The restart must instead rebuild the whole view from the applied
        // base snapshot, exactly as a MISSING .cp already does, and must NOT
        // invalidate the view (the corruption is recoverable).
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // rn 1..3 flushed on disk, rn 4..5 un-flushed lead in RAM

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // The first flush always writes a head .cp; corrupt it in place.
            final long headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
            Assert.assertTrue("the first flush must have written a head .cp", headLvSeqTxn != Numbers.LONG_NULL);
            corruptHeadCheckpoint(instance.getLiveViewToken(), headLvSeqTxn);

            // Simulated restart: the RAM lead (rn 4..5) is lost; disk holds rn 1..3;
            // the head .cp is present but corrupt (the startup sweep stamps it by
            // filename without validating its content).
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(restored);
            Assert.assertEquals("the corrupt head .cp must be stamped on the restored instance",
                    headLvSeqTxn, restored.getHeadCheckpointLvSeqTxn());

            // First refresh cycle: restoreFromHead trips the CRC check, unlinks the
            // corrupt .cp, and (with the fix) rebuilds the whole view from the applied
            // base via o3HeadMissReplay instead of draining forward from cold state.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // The corruption is recoverable, so the view stays valid ...
            Assert.assertFalse("a recoverable corrupt .cp must not invalidate the view", restored.isInvalid());
            // ... and equals a from-scratch recompute: row_number continues 1..5, not a
            // cold-restart 1..3 (disk) + 1..2 (lead).
            assertLvMatchesOracle("SELECT * FROM lv",
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            // The rebuild retired the corrupt .cp and wrote a fresh post-rebuild head.
            Assert.assertTrue("a fresh head .cp must be written after the rebuild",
                    restored.getHeadCheckpointLvSeqTxn() != Numbers.LONG_NULL);

            // o3HeadMissReplay flushed the full view to disk, so assertQuery's
            // engine.clear() battery loses nothing. Confirm rn 1..5 exactly once.
            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-05-12T00:00:01.000000Z\t1\t1\n" +
                            "2026-05-12T00:00:02.000000Z\t2\t2\n" +
                            "2026-05-12T00:00:03.000000Z\t3\t3\n" +
                            "2026-05-12T00:00:04.000000Z\t4\t4\n" +
                            "2026-05-12T00:00:05.000000Z\t5\t5\n");
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

    @Test
    public void testSkipRowsDiskOnlyDelegatesToBase() throws Exception {
        // With the fence forced off (mismatched stamps) the read is a pure
        // pass-through of the disk cursor, so skipRows must delegate straight to
        // the base's frame skip and never touch the tier.
        assertMemoryLeak(() -> {
            createSeamSplitLv(); // disk: 5 rows (x 1..5)
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
                Assert.assertFalse("mismatched stamps must fence disk-only", cursor.isRoutingEligible());
                RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(3);
                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals("disk skip consumes all requested rows", 0, counter.get());
                Assert.assertEquals("disk-only skip serves nothing from the tier", 0, cursor.inMemRowsServed());

                Record record = cursor.getRecord();
                LongList xs = new LongList();
                while (cursor.hasNext()) {
                    xs.add(record.getInt(1));
                }
                Assert.assertEquals(2, xs.size());
                Assert.assertEquals(4, xs.get(0));
                Assert.assertEquals(5, xs.get(1));
                Assert.assertEquals("disk-only read never serves the tier", 0, cursor.inMemRowsServed());
            } finally {
                tier.getSlot(0).setLvSeqTxn(s0);
                tier.getSlot(1).setLvSeqTxn(s1);
            }
        });
    }

    @Test
    public void testSkipRowsFallsBackAfterIterationStarts() throws Exception {
        // The frame-skip fast path assumes a fresh cursor. Once iteration has
        // begun (the disk cursor may have advanced) skipRows must fall back to the
        // row-by-row default and still land on the correct rows.
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // 5 rows (x 1..5), all served through the slot
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                Record record = cursor.getRecord();
                // Advance one row so the cursor is no longer fresh.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getInt(1)); // x=1
                Assert.assertEquals(1, cursor.inMemRowsServed());

                // Mid-iteration skip: falls back to the default, walking x=2,3.
                RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(2);
                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(0, counter.get());

                LongList xs = new LongList();
                while (cursor.hasNext()) {
                    xs.add(record.getInt(1));
                }
                Assert.assertEquals(2, xs.size());
                Assert.assertEquals(4, xs.get(0));
                Assert.assertEquals(5, xs.get(1));
                // The fallback walked every slot row (1 read + 2 skipped + 2 read),
                // in contrast to the fast path which positions without serving.
                Assert.assertEquals("fallback walks the skipped rows", 5, cursor.inMemRowsServed());
            }
        });
    }

    @Test
    public void testSkipRowsFrameSkipsDiskRegion() throws Exception {
        // A skip that lands inside the disk region (below the seam) is handed to
        // the disk cursor's frame skip; the pinned slot is left untouched.
        assertMemoryLeak(() -> {
            createSeamSplitLv(); // disk: 5 rows (x 1..5); slot: 2 recent (x 4,5); seam after x=3
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                // Skip 2 of the 3 below-seam disk rows.
                RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(2);
                cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                Assert.assertEquals(0, counter.get());
                Assert.assertEquals("skipping the disk region serves nothing from the tier", 0, cursor.inMemRowsServed());

                // Remaining: disk row x=3 (below seam), then the 2 slot rows (x 4,5).
                Record record = cursor.getRecord();
                LongList xs = new LongList();
                while (cursor.hasNext()) {
                    xs.add(record.getInt(1));
                }
                Assert.assertEquals(3, xs.size());
                Assert.assertEquals(3, xs.get(0)); // disk, below the seam
                Assert.assertEquals(4, xs.get(1)); // slot overlap
                Assert.assertEquals(5, xs.get(2)); // slot overlap
                Assert.assertEquals("only the two slot rows served from the tier", 2, cursor.inMemRowsServed());
                Assert.assertEquals("both slot rows are flushed overlap, not lead", 0, cursor.leadRowsServed());
            }
        });
    }

    @Test
    public void testSkipRowsFrameSkipsIntoSlotTail() throws Exception {
        // The whole disk region sits inside the IN MEMORY window (seam at the
        // minimum ts), so every row routes through the slot. A tail skip must land
        // directly on the slot's tail WITHOUT walking the skipped rows through
        // hasNext() - the row-by-row default would have counted them.
        assertMemoryLeak(() -> {
            buildFlushedPlusLead(); // disk: x 1..3; lead (RAM): x 4,5; diskRoutedCount == 0
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue(cursor.isRoutingEligible());
                Assert.assertEquals("size folds the lead onto disk", 5, cursor.size());

                // Emulate LIMIT -2: skip 3, take 2.
                RecordCursor.Counter counter = new RecordCursor.Counter();
                counter.set(3);
                cursor.skipRows(counter, 2);
                Assert.assertEquals("skip consumes exactly the requested rows", 0, counter.get());
                Assert.assertEquals("fast path positions without serving", 0, cursor.inMemRowsServed());
                Assert.assertEquals(0, cursor.leadRowsServed());

                // The two surviving rows are the un-flushed lead (x 4,5).
                Record record = cursor.getRecord();
                LongList xs = new LongList();
                while (cursor.hasNext()) {
                    xs.add(record.getInt(1));
                }
                Assert.assertEquals(2, xs.size());
                Assert.assertEquals(4, xs.get(0));
                Assert.assertEquals(5, xs.get(1));
                Assert.assertEquals("only the two tail rows served from the tier", 2, cursor.inMemRowsServed());
                Assert.assertEquals("both tail rows are un-flushed lead", 2, cursor.leadRowsServed());
            }
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

    // Proves a routing-eligible shape actually engages the tier before running the
    // differential oracle: the inner cursor must be routing-eligible and serve at
    // least one in-mem row, otherwise a fence regression that silently routed the
    // shape disk-only would still pass assertModeBMatchesDiskOnly (both sides read
    // disk). The inner read walks the whole LiveView cursor, so a LIMIT / outer
    // WHERE wrapper does not suppress the counter.
    private static void assertModeBEngagesAndMatchesDiskOnly(String sql) throws SqlException {
        InnerRead read = readInner(sql);
        Assert.assertTrue("shape must stay routing-eligible: " + sql, read.routingEligible);
        Assert.assertTrue("shape must serve in-mem rows: " + sql, read.inMemRowsServed > 0);
        assertModeBMatchesDiskOnly(sql);
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

    // Flips a byte inside the head .cp's manifest payload (past the fixed file header)
    // so the checkpoint reader's CRC check fails on the next restart. This is the
    // errno-0 STRUCTURAL corruption class (bit rot / truncation / a renamed
    // window-function class) - distinct from a version-mismatch compatibility break,
    // which restoreFromHead reports separately and which invalidates the view. Mirrors
    // LiveViewCheckpointTest#overwriteByteInFile, which leaves a structurally intact
    // file with a stale CRC trailer (not a truncation).
    private static void corruptHeadCheckpoint(TableToken liveViewToken, long headLvSeqTxn) {
        final CairoConfiguration cfg = configuration;
        try (Path cpPath = new Path()) {
            cpPath.of(cfg.getDbRoot())
                    .concat(liveViewToken)
                    .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                    .slash();
            LiveViewCheckpointWriter.appendCpFileName(cpPath, headLvSeqTxn);
            Assert.assertTrue("head .cp must exist on disk: " + cpPath, cfg.getFilesFacade().exists(cpPath.$()));
            final long offset = LiveViewCheckpointWriter.FILE_HEADER_SIZE + 8;
            try (MemoryCMARW mem = Vm.getCMARWInstance()) {
                mem.of(
                        cfg.getFilesFacade(),
                        cpPath.$(),
                        cfg.getFilesFacade().getPageSize(),
                        offset + Byte.BYTES,
                        MemoryTag.MMAP_DEFAULT,
                        CairoConfiguration.O_NONE
                );
                mem.putByte(offset, (byte) 0xAB);
                mem.sync(false);
            }
        }
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

    // Like createSeamSplitLv, but the base carries var-size passthrough columns
    // (vs STRING, vv VARCHAR) so the in-mem slot holds var-length (data, aux)
    // regions. Disk ends up with the first 3 rows, the pinned slot with the 2 most
    // recent; each row's var-size values are distinct (and of different lengths) so
    // an aliased read across records surfaces as a value mismatch.
    private void createVarSizeSeamSplitLv() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        execute("CREATE TABLE base (ts TIMESTAMP, vs STRING, vv VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1s AS " +
                "SELECT ts, vs, vv, row_number() OVER () AS rn FROM base");
        final long dataStart = 1_700_000_000_000_000L;
        final long cycle2Start = dataStart + 5_000_000L; // 5s later, beyond IN MEMORY 1s
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, vs, vv) VALUES " +
                    "(" + (dataStart + 1) + ", 'a1', 'v1'), " +
                    "(" + (dataStart + 2) + ", 'a2', 'v2'), " +
                    "(" + (dataStart + 3) + ", 'a3', 'v3')");
            drainWalQueue();
            setCurrentMicros(250_000L); // > FLUSH EVERY 100ms
            drainJob(job);

            execute("INSERT INTO base (ts, vs, vv) VALUES " +
                    "(" + (cycle2Start + 1) + ", 'bbbb4', 'vvvv4'), " +
                    "(" + (cycle2Start + 2) + ", 'ccccc5', 'wwwww5')");
            drainWalQueue();
            setCurrentMicros(500_000L);
            drainJob(job);
        }
        drainWalQueue();
    }

    // Like createSeamSplitLv, but the base carries a non-cached SYMBOL passthrough
    // column so the pinned slot's flushed (overlap) rows resolve their symbol
    // through the LV table's disk symbol table. With the cache off that base hands
    // out two distinct reused flyweights for valueOf/valueBOf, which is what makes
    // getSymA vs getSymB aliasing observable. Disk ends up with the first 3 rows,
    // the slot with the 2 most recent - each carrying a distinct symbol.
    private void createSymbolSeamSplitLvNoCache() throws Exception {
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_CACHE_FLAG, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 0);
        execute("CREATE TABLE base (ts TIMESTAMP, s SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1s AS " +
                "SELECT ts, s, x, row_number() OVER () AS rn FROM base WHERE x > 0");
        final long dataStart = 1_700_000_000_000_000L;
        final long cycle2Start = dataStart + 5_000_000L; // 5s later, beyond IN MEMORY 1s
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, s, x) VALUES " +
                    "(" + (dataStart + 1) + ", 'aaa', 1), " +
                    "(" + (dataStart + 2) + ", 'bbb', 2), " +
                    "(" + (dataStart + 3) + ", 'ccc', 3)");
            drainWalQueue();
            setCurrentMicros(250_000L); // > FLUSH EVERY 100ms
            drainJob(job);

            execute("INSERT INTO base (ts, s, x) VALUES " +
                    "(" + (cycle2Start + 1) + ", 'ddd', 4), " +
                    "(" + (cycle2Start + 2) + ", 'eee', 5)");
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
    // Probes whether the tier's published slot currently carries a reader pin,
    // without disturbing it: tryAcquireWrite takes the writer sentinel via a
    // 0 -> -1 CAS that succeeds only when no reader holds the slot, in which case
    // the sentinel is released straight back to 0.
    private static boolean isPublishedSlotReaderPinned(LiveViewInMemoryTier tier) {
        final int idx = tier.getPublishedIdx();
        if (tier.tryAcquireWrite(idx) == null) {
            return true;
        }
        tier.releaseWriteWithoutPublish(idx);
        return false;
    }

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
