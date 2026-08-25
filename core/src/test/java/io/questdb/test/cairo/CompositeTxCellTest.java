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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Plan 3 (composite partitioning), Task 1 + Task 2: the {@code _txn} attached-partition record gets a
 * per-table stride -- 4 longs (32 bytes, today's byte-identical layout) for a plain table, 8 longs
 * for a COMPOSITE table (cellKey at slot 4, slots 5-7 reserved; forced to 8 rather than 5 because
 * {@code LongList.binarySearchBlock} needs a power-of-2 block size). Both {@link TableWriter} (via
 * {@link io.questdb.cairo.TxWriter}) and {@link TableReader} (via {@link io.questdb.cairo.TxReader})
 * derive the stride independently from the same {@code metadata.getPartitionSpec().getDimensionCount()
 * > 0} signal.
 * <p>
 * Task 1 established the stride + cellKey accessor machinery only (no real persistence). Task 2 (the
 * other two tests below) wires up actually writing + reloading a real cellKey, and fixes a
 * reopen-ordering defect Task 1's review surfaced: TableWriter's constructor blind-loads {@code _txn}
 * before it knows whether a table is composite, which -- unfixed -- silently corrupts the last
 * partition's reported size on reopen for an already-partitioned composite table (a stride mismatch,
 * reproducible even when every partition's cellKey is 0).
 */
public class CompositeTxCellTest extends AbstractCairoTest {

    @Test
    public void testStrideDerivedFromComposite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");           // composite (1 dimension)
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day"); // plain

            try (TableWriter cw = getWriter("c"); TableWriter pw = getWriter("p")) {
                Assert.assertEquals(8, cw.getTxWriter().getLongsPerAttachedPartition());
                Assert.assertEquals(4, pw.getTxWriter().getLongsPerAttachedPartition());
                // stride 4 has no cellKey slot: a plain table always reports 0, without ever
                // reading attachedPartitions (safe even though table p has 0 committed partitions).
                Assert.assertEquals(0, pw.getTxWriter().getPartitionCellKey(0));
            }

            // The reader side is threaded independently of the writer side (TableReader owns its own
            // TxReader); verify it derives the same per-table stride from the same PartitionSpec signal.
            try (TableReader cr = getReader("c"); TableReader pr = getReader("p")) {
                Assert.assertEquals(8, cr.getTxFile().getLongsPerAttachedPartition());
                Assert.assertEquals(4, pr.getTxFile().getLongsPerAttachedPartition());
                Assert.assertEquals(0, pr.getTxFile().getPartitionCellKey(0));
            }
        });
    }

    /**
     * Steps 1-4: round-trip (timestamp, cellKey, size, nameTxn) through a real composite-stride
     * {@code _txn} file, using a standalone {@link TxWriter}/{@link TxReader} pair driven by the
     * {@code *ForTest} seam -- isolated from the TableWriter constructor's blind-load path, so this
     * test proves ONLY cellKey persistence (the reopen-ordering defect is the next test). Writes three
     * partitions across two timestamps and two cells: day1/cell0, day1/cell1, day2/cell0.
     */
    @Test
    public void testCellKeyRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive(); // no pooled writer/reader may keep _txn open under our direct use

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day1, 20L, 101L, 1);
                    txWriter.appendPartitionForTest(day2, 30L, 102L, 0);
                    // The last partition's size normally lives only in transientRowCount, folded back
                    // into the attached-partitions list on load (see TxWriter#beginPartitionSizeUpdate's
                    // comment); sync it here so the round-trip below has a real persisted value.
                    txWriter.updateMaxTimestamp(day2 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.commit(new ObjList<>());
                }

                try (TxReader txReader = new TxReader(ff)) {
                    txReader.setCompositeForTest(true);
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals(3, txReader.getPartitionCount());

                    Assert.assertEquals(day1, txReader.getPartitionTimestampByIndex(0));
                    Assert.assertEquals(0, txReader.getPartitionCellKey(0));
                    Assert.assertEquals(10L, txReader.getPartitionSize(0));
                    Assert.assertEquals(100L, txReader.getPartitionNameTxn(0));

                    Assert.assertEquals(day1, txReader.getPartitionTimestampByIndex(1));
                    Assert.assertEquals(1, txReader.getPartitionCellKey(1));
                    Assert.assertEquals(20L, txReader.getPartitionSize(1));
                    Assert.assertEquals(101L, txReader.getPartitionNameTxn(1));

                    Assert.assertEquals(day2, txReader.getPartitionTimestampByIndex(2));
                    Assert.assertEquals(0, txReader.getPartitionCellKey(2));
                    Assert.assertEquals(30L, txReader.getPartitionSize(2));
                    Assert.assertEquals(102L, txReader.getPartitionNameTxn(2));

                    // Reserved slots 5-7 must always be zero: initPartitionAt() must not trust the
                    // LongList backing array's previous contents (it's reused across partitions as the
                    // list grows/shifts), so it explicitly zeroes them rather than relying on that.
                    LongList raw = new LongList();
                    txReader.dumpRawTxPartitionInfo(raw);
                    Assert.assertEquals(24, raw.size()); // 3 partitions * stride 8
                    for (int p = 0; p < 3; p++) {
                        int base = p * 8;
                        Assert.assertEquals("partition " + p + " slot 5", 0L, raw.getQuick(base + 5));
                        Assert.assertEquals("partition " + p + " slot 6", 0L, raw.getQuick(base + 6));
                        Assert.assertEquals("partition " + p + " slot 7", 0L, raw.getQuick(base + 7));
                    }
                }
            }
        });
    }

    /**
     * Step 6: the reopen acceptance test for the Task-1 blind-load defect (Step 5's fix). Unlike
     * {@link #testCellKeyRoundTrip}, this uses real SQL-inserted rows (landing at cellKey 0 -- real
     * (ts, cellKey) routing is Plan 4) and reopens via a genuine {@link TableWriter}, exercising the
     * constructor's blind-load path directly. This is the scenario that manifests the bug even though
     * every cellKey is 0: it is a STRIDE mismatch (plain-4 vs composite-8), not a cellKey-value mismatch.
     * <p>
     * Note what this deliberately does NOT assert: {@code getPartitionSize()} of the still-open LAST
     * partition. That always reads back 0 after a full TableWriter reopen, bug or no bug --
     * {@code configureAppendPosition()}/{@code initLastPartition()} unconditionally resets it later
     * regardless, by design (a writer always re-derives the open last partition's size from
     * transientRowCount going forward, never trusting the persisted slot for it). The fold this task
     * fixes is real, but by the time a fully-constructed TableWriter is available to a test, its effect
     * on slot 1 is already overwritten either way -- confirmed empirically while writing this test: an
     * assertion on getPartitionSize(last) fails identically with the Step-5 fix present AND absent, so it
     * cannot be the regression signal. The fold's lasting, persistent, and actually-discriminating
     * casualty is reserved slot 5 of the last partition's record: the misdirected fold leaves real
     * (non-zero) data there instead of the zero every partition's reserved slots must hold. That's what
     * this test checks, via a raw dump of the attached-partitions region.
     * <p>
     * Confirmed RED before the Step-5 fix and GREEN after (see task report for both captured runs).
     */
    @Test
    public void testReopenAfterCompositeBlindLoad() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c2 (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            execute("insert into c2 values " +
                    "('2020-01-01T00:00:00.000000Z', 'A', 1.0), " +
                    "('2020-01-01T01:00:00.000000Z', 'A', 2.0), " +
                    "('2020-01-02T00:00:00.000000Z', 'A', 3.0), " +
                    "('2020-01-02T01:00:00.000000Z', 'A', 4.0), " +
                    "('2020-01-02T02:00:00.000000Z', 'A', 5.0)");
            drainWalQueue();
            engine.releaseInactive(); // fully close so the next getWriter() below is a real cold reopen

            try (TableWriter tw = getWriter("c2")) {
                TxWriter txWriter = tw.getTxWriter();
                Assert.assertEquals(8, txWriter.getLongsPerAttachedPartition());
                Assert.assertEquals(2, txWriter.getPartitionCount());

                Assert.assertEquals(0, txWriter.getPartitionCellKey(0));
                Assert.assertEquals(0, txWriter.getPartitionCellKey(1));

                // day 1 (2020-01-01): 2 rows, sealed by the switch to day 2 before the commit that
                // persisted this _txn -- its masked-size slot was correctly written to disk at that
                // point (switchPartitions syncs it), so it round-trips correctly regardless of this bug.
                Assert.assertEquals(2L, txWriter.getPartitionSize(0));
                // day 2 (2020-01-02): 3 rows, still the open/last partition when this _txn was persisted.
                // The true row count lives only in transientRowCount (the top-level field, read directly
                // and unaffected by the buggy fold either way) -- NOT in this slot, which
                // configureAppendPosition() unconditionally zeroes on every reopen by design.
                Assert.assertEquals(3L, txWriter.getTransientRowCount());
                Assert.assertEquals(0L, txWriter.getPartitionSize(1));

                // The actual, persistent casualty of the stride-mismatched fold: it misdirects the
                // transientRowCount write into slot 5 of the last partition's record (reserved, must
                // always be 0) instead of the real masked-size slot 1. Pre-fix, this reads back 3
                // (leaked transientRowCount) instead of 0 for day 2; day 1 (not the last partition, so
                // never targeted by the fold) is unaffected either way.
                LongList raw = new LongList();
                txWriter.dumpRawTxPartitionInfo(raw);
                Assert.assertEquals(16, raw.size()); // 2 partitions * stride 8
                for (int p = 0; p < 2; p++) {
                    int base = p * 8;
                    Assert.assertEquals("partition " + p + " slot 5", 0L, raw.getQuick(base + 5));
                    Assert.assertEquals("partition " + p + " slot 6", 0L, raw.getQuick(base + 6));
                    Assert.assertEquals("partition " + p + " slot 7", 0L, raw.getQuick(base + 7));
                }
            }
        });
    }

    /**
     * Plan 3, Task 3: {@code findAttachedPartitionRawIndexBy(ts, cellKey)} must find the exact raw
     * offset of a (ts, cellKey) partition, and -- on a miss -- return the same negative
     * insertion-point encoding {@code findAttachedPartitionRawIndexByLoTimestamp} uses ({@code
     * -(rawInsertionIndex) - 1}), just disambiguated by cellKey within a same-ts run. Builds
     * day1/cell0, day1/cell1, day2/cell0 (stride 8, so raw offsets are index*8) via the
     * tail-append seam -- already in (ts, cellKey) order, so this test is purely about the finder,
     * not the insert.
     */
    @Test
    public void testPartitionFormatIsAddressablePerCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive();

            final long day1 = 0L;
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    // two SIBLING cells of the SAME day -- the shape a timestamp-keyed setter cannot tell
                    // apart, and the reason per-cell parquet needs a raw-index entry point
                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day1, 20L, 101L, 1);

                    final int rawCell1 = txWriter.findAttachedPartitionRawIndexBy(day1, 1);
                    txWriter.setPartitionParquetByRawIndex(rawCell1, 4096L);

                    Assert.assertTrue("the targeted cell must be parquet",
                            txWriter.isPartitionParquet(1));
                    Assert.assertFalse("its SIBLING at the same timestamp must stay native -- this is the"
                                    + " whole point of addressing by raw index rather than by timestamp",
                            txWriter.isPartitionParquet(0));
                    Assert.assertEquals(4096L, txWriter.getPartitionParquetFileSize(1));

                    // and back again, still without touching the sibling
                    txWriter.setPartitionNativeByRawIndex(rawCell1, 7L);
                    Assert.assertFalse(txWriter.isPartitionParquet(1));
                    Assert.assertFalse(txWriter.isPartitionParquet(0));
                }
            }
        });
    }

    @Test
    public void testFindRawIndexByTsAndCellKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive();

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;
            final long day3 = 2 * Micros.DAY_MICROS;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day1, 20L, 101L, 1);
                    txWriter.appendPartitionForTest(day2, 30L, 102L, 0);

                    // Exact hits: raw offset = partition index * stride (8).
                    Assert.assertEquals(0, txWriter.findAttachedPartitionRawIndexBy(day1, 0));
                    Assert.assertEquals(8, txWriter.findAttachedPartitionRawIndexBy(day1, 1));
                    Assert.assertEquals(16, txWriter.findAttachedPartitionRawIndexBy(day2, 0));

                    // Miss within an existing ts run: (day1, 5) sorts after (day1, 1) and before
                    // (day2, 0) -- insertion point must land exactly there, raw offset 16, encoded
                    // as -(16) - 1 = -17 (same convention as findAttachedPartitionRawIndexByLoTimestamp).
                    int missWithinRun = txWriter.findAttachedPartitionRawIndexBy(day1, 5);
                    Assert.assertEquals(-17, missWithinRun);
                    Assert.assertEquals("decoded insertion point must be right after (day1, cell1)'s raw offset",
                            16, -(missWithinRun) - 1);

                    // Miss entirely before every ts: insertion point is raw offset 0, encoded -1.
                    Assert.assertEquals(-1, txWriter.findAttachedPartitionRawIndexBy(day1 - Micros.DAY_MICROS, 0));

                    // Miss entirely after every ts: insertion point is raw offset 24 (append at tail),
                    // encoded -(24) - 1 = -25.
                    Assert.assertEquals(-25, txWriter.findAttachedPartitionRawIndexBy(day3, 0));
                }
            }
        });
    }

    /**
     * Plan 3, Task 3: the composite insert must place a new (ts, cellKey) partition at the position
     * that keeps the attached-partitions array totally ordered (ts ASC, cellKey ASC), not just
     * tail-appended. Starts from day1/cell0, day2/cell0 (built via the Task-2 tail-append seam) and
     * inserts day1/cell1 via the new {@code insertPartitionForTest} -- which, unlike {@code
     * appendPartitionForTest}, computes its slot from {@code findAttachedPartitionRawIndexBy} --
     * asserting it lands in the middle: cell0@day1, cell1@day1, cell0@day2.
     */
    @Test
    public void testInsertPartitionForTestKeepsOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive();

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day2, 30L, 102L, 0);
                    Assert.assertEquals(2, txWriter.getPartitionCount());

                    txWriter.insertPartitionForTest(day1, 20L, 101L, 1);

                    Assert.assertEquals(3, txWriter.getPartitionCount());

                    Assert.assertEquals(day1, txWriter.getPartitionTimestampByIndex(0));
                    Assert.assertEquals(0, txWriter.getPartitionCellKey(0));
                    Assert.assertEquals(10L, txWriter.getPartitionSize(0));
                    Assert.assertEquals(100L, txWriter.getPartitionNameTxn(0));

                    Assert.assertEquals(day1, txWriter.getPartitionTimestampByIndex(1));
                    Assert.assertEquals(1, txWriter.getPartitionCellKey(1));
                    Assert.assertEquals(20L, txWriter.getPartitionSize(1));
                    Assert.assertEquals(101L, txWriter.getPartitionNameTxn(1));

                    Assert.assertEquals(day2, txWriter.getPartitionTimestampByIndex(2));
                    Assert.assertEquals(0, txWriter.getPartitionCellKey(2));
                    Assert.assertEquals(30L, txWriter.getPartitionSize(2));
                    Assert.assertEquals(102L, txWriter.getPartitionNameTxn(2));
                }
            }
        });
    }

    /**
     * Plan 3, Task 3 plain/dormant guard: the 2-D {@code (ts, cellKey)} finder must resolve exactly
     * like today's single-key lookup family for (a) a plain table (stride 4, no cellKey slot at all)
     * and (b) a dormant composite table (stride 8, but every real cellKey is still 0 -- Plan 4 hasn't
     * landed write routing yet). Cross-checks the new raw finder against the pre-existing, untouched
     * public family ({@link TxReader#getPartitionIndex}, {@link TxReader#attachedPartitionsContains})
     * for both an exact hit and a genuine miss.
     */
    @Test
    public void testTwoDimensionalFinderMatchesPlainAndDormantBehavior() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day"); // plain
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal"); // dormant composite
            engine.releaseInactive();

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;
            final long day3 = 2 * Micros.DAY_MICROS;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();

            // (a) plain: stride 4, raw offsets are index * 4.
            TableToken plainToken = engine.verifyTableName("p");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(plainToken).concat(TXN_FILE_NAME).$();
                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(false);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day2, 20L, 101L, 0);

                    Assert.assertEquals(4, txWriter.getLongsPerAttachedPartition());
                    Assert.assertEquals(0, txWriter.findAttachedPartitionRawIndexBy(day1, 0));
                    Assert.assertEquals(4, txWriter.findAttachedPartitionRawIndexBy(day2, 0));
                    Assert.assertEquals(0, txWriter.getPartitionIndex(day1));
                    Assert.assertEquals(1, txWriter.getPartitionIndex(day2));
                    Assert.assertTrue(txWriter.attachedPartitionsContains(day1));

                    // genuine miss: day3 does not exist yet on either table.
                    Assert.assertEquals(-1, txWriter.getPartitionIndex(day3));
                    Assert.assertFalse(txWriter.attachedPartitionsContains(day3));
                    int missRaw = txWriter.findAttachedPartitionRawIndexBy(day3, 0);
                    Assert.assertEquals(8, -(missRaw) - 1); // append at raw offset 8 (tail, stride 4)
                }
            }

            // (b) dormant composite: stride 8, but every real cellKey is 0 -- must agree with (a).
            TableToken compositeToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(compositeToken).concat(TXN_FILE_NAME).$();
                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day2, 20L, 101L, 0);

                    Assert.assertEquals(8, txWriter.getLongsPerAttachedPartition());
                    Assert.assertEquals(0, txWriter.findAttachedPartitionRawIndexBy(day1, 0));
                    Assert.assertEquals(8, txWriter.findAttachedPartitionRawIndexBy(day2, 0));
                    Assert.assertEquals(0, txWriter.getPartitionIndex(day1));
                    Assert.assertEquals(1, txWriter.getPartitionIndex(day2));
                    Assert.assertTrue(txWriter.attachedPartitionsContains(day1));

                    Assert.assertEquals(-1, txWriter.getPartitionIndex(day3));
                    Assert.assertFalse(txWriter.attachedPartitionsContains(day3));
                    int missRaw = txWriter.findAttachedPartitionRawIndexBy(day3, 0);
                    Assert.assertEquals(16, -(missRaw) - 1); // append at raw offset 16 (tail, stride 8)
                }
            }
        });
    }

    /**
     * Plan 3, Task 4: the timestamp-resolving mutators/removers ({@code updatePartitionSizeByTimestamp},
     * {@code removeAttachedPartitions}, {@code incrementPartitionSquashCounter}) must resolve their raw
     * index via {@code (ts, cellKey)} (Task 3's {@link TxReader#findAttachedPartitionRawIndexBy}), not
     * just {@code ts} -- otherwise a mutation aimed at one cell silently lands on a different cell at the
     * same timestamp (whichever one a ts-only lookup happens to find, i.e. the lowest cellKey present,
     * since the array is sorted (ts ASC, cellKey ASC)). Builds day1/cell0 (size 10) and day1/cell1 (size
     * 20) via the tail-append seam, then drives each mutator at cell1 and asserts cell0 is untouched,
     * before finally removing cell0 and checking cell1 survives alone with its mutations intact.
     */
    @Test
    public void testMutatorsDoNotAliasAcrossCellsAtSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive();

            final long day1 = 0L;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day1, 20L, 101L, 1);
                    Assert.assertEquals(2, txWriter.getPartitionCount());
                    Assert.assertEquals(10L, txWriter.getPartitionSize(0));
                    Assert.assertEquals(20L, txWriter.getPartitionSize(1));

                    // Update (day1, cell1)'s size -- must land on cell1, not alias cell0.
                    txWriter.updatePartitionSizeByCell(day1, 1, 25L);
                    Assert.assertEquals("cell1 size after targeted update", 25L, txWriter.getPartitionSize(1));
                    Assert.assertEquals("cell0 size must be unaffected by a cell1 update", 10L, txWriter.getPartitionSize(0));

                    // Squash counter on (day1, cell1) -- must not touch cell0's counter. Both cells still
                    // present at this point; removal happens last.
                    Assert.assertTrue(txWriter.incrementPartitionSquashCounter(day1, 1));
                    Assert.assertEquals("cell1 squash counter incremented", 1, txWriter.getPartitionSquashCount(1));
                    Assert.assertEquals("cell0 squash counter must be unaffected", 0, txWriter.getPartitionSquashCount(0));

                    // Remove (day1, cell0); only (day1, cell1) must remain, with its mutations intact.
                    txWriter.removeAttachedPartitions(day1, 0);
                    Assert.assertEquals(1, txWriter.getPartitionCount());
                    Assert.assertEquals(day1, txWriter.getPartitionTimestampByIndex(0));
                    Assert.assertEquals(1, txWriter.getPartitionCellKey(0));
                    Assert.assertEquals(25L, txWriter.getPartitionSize(0));
                    Assert.assertEquals(1, txWriter.getPartitionSquashCount(0));
                }
            }
        });
    }

    /**
     * Plan 3, Task 4 (Task-3 carry-forward coverage lock): {@link TxReader#findAttachedPartitionRawIndexBy}'s
     * mid-scan early-exit branch (the {@code thisCellKey > cellKey} check, {@code TxReader.java:223}) fires
     * when a miss is discovered strictly INSIDE a same-ts run because a later cell's key already exceeds the
     * query key -- as opposed to running off the end of the run because the timestamp changed (the existing
     * {@link #testFindRawIndexByTsAndCellKey}'s "missWithinRun" case: querying cellKey 5 against cell0/cell1
     * only ever hits the ts-changed exit, since 5 is bigger than every cellKey actually present there, and
     * the run ends at day2). Building day1/cell0, day1/cell1, day1/cell9 -- all the SAME timestamp -- and
     * querying cellKey 2 forces the scan to reach cell9, still within the day1 run, and observe 9 &gt; 2,
     * hitting the early-exit branch specifically instead of running off the run's end.
     */
    @Test
    public void testFindRawIndexByTsAndCellKeyMidScanMiss() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange wal");
            engine.releaseInactive();

            final long day1 = 0L;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day1, 20L, 101L, 1);
                    txWriter.appendPartitionForTest(day1, 90L, 109L, 9);

                    // Sanity: raw offsets are index * stride (8); all three partitions share timestamp day1.
                    Assert.assertEquals(0, txWriter.findAttachedPartitionRawIndexBy(day1, 0));
                    Assert.assertEquals(8, txWriter.findAttachedPartitionRawIndexBy(day1, 1));
                    Assert.assertEquals(16, txWriter.findAttachedPartitionRawIndexBy(day1, 9));

                    // cellKey 2 is not present; the scan must stop as soon as it reaches cell9 (thisCellKey
                    // 9 > 2) rather than running to the end of the array -- the mid-scan early-exit branch.
                    int missRaw = txWriter.findAttachedPartitionRawIndexBy(day1, 2);
                    Assert.assertEquals(-17, missRaw);
                    int insertionPoint = -(missRaw) - 1;
                    Assert.assertEquals(
                            "insertion point must sit strictly between cell1 (raw 8) and cell9 (raw 16)",
                            16, insertionPoint
                    );
                }
            }
        });
    }

    /**
     * Plan 3 Task 4 fix-wave regression (overload-capture guard). Before this fix, {@code TxWriter} had
     * a public {@code updatePartitionSizeByTimestamp(long, int, long)} cellKey-aware overload with the
     * same arity as the pre-existing {@code updatePartitionSizeByTimestamp(long, long, long)}
     * (rowCount, partitionNameTxn), differing only by the primitive type of the middle parameter. Per
     * JLS 15.12.2.5 "most specific method," any 3-arg call whose middle argument is statically {@code
     * int} (e.g. an int literal) silently binds to the newer, more-specific overload instead of the
     * older one -- exactly what {@code TableWriter.processWalCommit}'s empty-table artificial-partition
     * creation does: {@code updatePartitionSizeByTimestamp(o3TimestampMin, 0, txWriter.getTxn() - 1)},
     * intending rowCount=0/partitionNameTxn={@code getTxn()-1} but (pre-fix) actually capturing
     * cellKey=0/rowCount={@code getTxn()-1} whenever {@code getTxn() != 1}. (The renamed cellKey overload
     * is {@code updatePartitionSizeByCell}; production is unaffected either way, since real writes are
     * cellKey 0 only while write-routing is dormant -- this test only guards the overload shape itself.)
     * <p>
     * Reproduces the exact {@code (long, int, long)} call shape directly against a standalone
     * {@code TxWriter} with {@code txn} forced above 1 (the hazard is masked for a brand-new table's
     * first-ever commit, where {@code getTxn() == 1}), and asserts the partition's size ends up {@code
     * 0} -- i.e. the call must resolve to the size overload's rowCount=0, not a cellKey overload's
     * rowCount=5L. Guards against any future overload of these cellKey-aware methods recapturing this
     * exact call shape.
     */
    @Test
    public void testUpdatePartitionSizeByTimestampThreeArgCallBindsToSizeOverloadNotCellOverload() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            engine.releaseInactive();

            final long day1 = 0L;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("p");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.updateMaxTimestamp(day1);
                    txWriter.finishPartitionSizeUpdate();
                    // Bump txn above 1 -- the hazard is masked for a brand-new table (getTxn() == 1).
                    txWriter.commit(new ObjList<>());
                    txWriter.commit(new ObjList<>());
                    Assert.assertTrue("txn must be forced above 1 for this canary to be meaningful", txWriter.getTxn() > 1);

                    // The exact (long, int, long) call shape used (pre-fix) by TableWriter.processWalCommit's
                    // artificial empty-table partition creation. Must bind to
                    // updatePartitionSizeByTimestamp(long timestamp, long rowCount, long partitionNameTxn)
                    // with rowCount=0, partitionNameTxn=5L -- NOT a cellKey overload's cellKey=0, rowCount=5L.
                    // (partitionNameTxn is only ever consulted on the insert-on-miss path, not this
                    // update-in-place path, so it is not independently observable here -- the size check
                    // alone discriminates the two overloads.)
                    txWriter.updatePartitionSizeByTimestamp(day1, 0, 5L);
                    Assert.assertEquals(
                            "call must bind to the (rowCount, partitionNameTxn) overload (rowCount=0), " +
                                    "not a cellKey-aware overload that would set size to 5",
                            0L, txWriter.getPartitionSize(0)
                    );
                }
            }
        });
    }
}
