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
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
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

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Plan 3 (composite partitioning), Task 9: this is a TEST-ONLY task that locks in two guarantees
 * Tasks 1-8 were each responsible for a slice of, but which no single earlier test proves end to end:
 * <ol>
 *     <li>a PLAIN (0-dimension) table's {@code _txn}/{@code _cv} on-disk bytes are exactly what they
 *     were before composite partitioning existed -- stride 4 (32 bytes/record) for {@code _txn}, and
 *     the {@code _cv} packed columnIndex with high 32 bits zero;</li>
 *     <li>the WIDENED composite {@code _txn} region (stride 8) is crash-safe and reloads purely from
 *     its self-describing byte-size prefix plus the per-table stride -- no format-version field, no
 *     migration -- riding the existing A/B dual-area commit mechanism unchanged.</li>
 * </ol>
 * <p>
 * {@link #testPlainTxnAttachedPartitionsRegionByteIdentity()} is Step 1, {@link
 * #testPlainColumnVersionByteIdentity()} is Step 2, {@link
 * #testCompositeTxnCrashSafetyReloadsFromSelfDescribingRegion()} is Step 3 of the task brief.
 */
public class CompositePartitionTableCompatTest extends AbstractCairoTest {

    /**
     * Step 1. Builds two PLAIN tables' {@code _txn} attached-partitions regions identically, via the
     * standalone {@code TxWriter} tail-append seam ({@link CompositeTxCellTest} establishes this idiom),
     * then reopens each with a brand-new {@link TxReader} (never touched by the writer that built it) to
     * prove the ON-DISK region -- not just an in-memory object's opinion of itself -- is exactly stride
     * 4: {@code partitionCount * 4 longs == partitionCount * 32 bytes}, with no fifth (cellKey) slot,
     * and byte-for-byte identical between the two tables.
     * <p>
     * <b>Discrimination evidence (see task report for the full before/after console capture):</b> with
     * {@code p1Composite} below temporarily flipped to {@code true} -- i.e. p1's own writer, not just a
     * reader, actually persists the region at stride 8 -- the region genuinely doubles in size (16 longs
     * instead of 8) and every assertion below that pins an exact longword count or offset fails. Restored
     * to {@code false} for the committed version of this test.
     */
    @Test
    public void testPlainTxnAttachedPartitionsRegionByteIdentity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p1 (ts timestamp, x double) timestamp(ts) partition by day");
            execute("create table p2 (ts timestamp, x double) timestamp(ts) partition by day");
            engine.releaseInactive(); // no pooled writer/reader may keep _txn open under our direct use

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;
            // Discrimination toggle (see Javadoc above): flip to `true` to prove this test fails when
            // p1's _txn is actually persisted at stride 8. Must be `false` in the committed test.
            final boolean p1Composite = false;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();

            buildTxnRegion("p1", ff, p1Composite, day1, day2);
            buildTxnRegion("p2", ff, false, day1, day2);

            LongList raw1 = new LongList();
            LongList raw2 = new LongList();
            try (TxReader r1 = openFreshTxReader("p1", ff, false)) {
                Assert.assertEquals("p1 must report the plain (4-long) stride", 4, r1.getLongsPerAttachedPartition());
                r1.dumpRawTxPartitionInfo(raw1);
                // Accessor-contract sanity check ONLY -- NOT a byte-identity proof. getPartitionCellKey()
                // short-circuits to a hard-coded 0 for ANY reader opened at the plain (4-long) stride,
                // without ever reading attachedPartitions (see TxReader#getPartitionCellKeyByRawIndex).
                // It would therefore pass identically even if the on-disk region were corrupt or carried
                // a stray fifth slot. The real byte-identity guarantee -- that no cellKey slot exists on
                // disk at all -- is carried by the exact longword-count assertion and the offset-4
                // field-level check below, not by these two lines.
                Assert.assertEquals(0, r1.getPartitionCellKey(0));
                Assert.assertEquals(0, r1.getPartitionCellKey(1));
            }
            try (TxReader r2 = openFreshTxReader("p2", ff, false)) {
                Assert.assertEquals(4, r2.getLongsPerAttachedPartition());
                r2.dumpRawTxPartitionInfo(raw2);
            }

            // The discriminating property: EXACT byte count for the region. 2 partitions * stride 4 =
            // 8 longs = 64 bytes. A stride-8 plain table would be 16 longs / 128 bytes instead. This is
            // what genuinely proves no cellKey slot exists in a plain record -- the getPartitionCellKey()
            // calls above cannot prove that, since they short-circuit to 0 without consulting the disk
            // bytes at all.
            Assert.assertEquals("p1 attached-partitions region must be exactly partitionCount(2) * stride(4) longs",
                    8, raw1.size());
            Assert.assertEquals("p2 attached-partitions region must match p1's shape", 8, raw2.size());

            // Byte-identical: two tables built identically must produce identical raw regions.
            Assert.assertEquals("two identically-built plain tables must have byte-identical _txn regions", raw1, raw2);

            // Field-level: exactly [ts, maskedSize, nameTxn, parquetFileSize] per partition, no fifth slot.
            // Partition 0 (day1).
            Assert.assertEquals(day1, raw1.getQuick(0));
            Assert.assertEquals("no flags set -> maskedSize == rowCount", 5L, raw1.getQuick(1));
            Assert.assertEquals(500L, raw1.getQuick(2));
            Assert.assertEquals("no parquet file generated", -1L, raw1.getQuick(3));
            // Partition 1 (day2) starts EXACTLY at raw index 4 -- if a phantom cellKey slot existed at
            // index 4, this ts value would instead be misread from index 5, and the assertion right
            // below (that index 4 holds day2's timestamp) would fail.
            Assert.assertEquals("no fifth slot: partition 1 must start at raw index 4, not 5", day2, raw1.getQuick(4));
            Assert.assertEquals(7L, raw1.getQuick(5));
            Assert.assertEquals(501L, raw1.getQuick(6));
            Assert.assertEquals(-1L, raw1.getQuick(7));
        });
    }

    /**
     * Step 2. Creates a plain table, inserts two rows into the still-open last partition (so {@code
     * ALTER TABLE ADD COLUMN}'s {@code txWriter.getTransientRowCount() > 0} guard is satisfied and a
     * REAL, non-zero per-partition column-top record is persisted to {@code _cv} -- not merely the
     * unconditional {@code COL_TOP_DEFAULT_PARTITION} sentinel record that {@code addColumn} also always
     * writes; see {@code TableWriter.addColumn}/{@code openNewColumnFiles}), then reopens the table's
     * real {@code _cv} file with a brand-new, standalone {@link ColumnVersionReader} and asserts the raw
     * stored {@code columnIndex} long at that record is exactly the bare column index -- high 32 bits
     * zero, i.e. {@code packColIndex(0, columnIndex) == columnIndex} -- proving the real ALTER TABLE
     * write path (not just the low-level {@code ColumnVersionWriter} API {@link
     * CompositeColumnVersionCellTest} already covers directly) produces byte-identical {@code _cv}
     * records for a plain table.
     */
    @Test
    public void testPlainColumnVersionByteIdentity() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            execute("insert into p values ('1970-01-01T00:00:00.000000Z', 1.0)");
            execute("insert into p values ('1970-01-01T01:00:00.000000Z', 2.0)");

            final long day1 = 0L; // both rows above fall in the day1 (1970-01-01) partition

            execute("alter table p add column y double");

            int yIndex;
            try (TableReader rdr = getReader("p")) {
                yIndex = rdr.getMetadata().getColumnIndex("y");
            }
            engine.releaseInactive(); // no pooled writer/reader may keep _cv open under our direct use

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("p");
            try (
                    Path path = new Path();
                    ColumnVersionReader cvReader = new ColumnVersionReader().ofRO(
                            ff, path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$()
                    )
            ) {
                cvReader.readUnsafe();

                // The REAL per-partition column-top record (as opposed to the always-written
                // COL_TOP_DEFAULT_PARTITION sentinel, which also encodes columnIndex=yIndex but at
                // timestamp Long.MIN_VALUE with an unrelated payload) -- located by the actual partition
                // timestamp, exactly as production code resolves it.
                int recordIndex = cvReader.getRecordIndex(day1, yIndex);
                Assert.assertTrue("a real column-top record for the new column at day1 must exist", recordIndex > -1);

                long rawPacked = cvReader.getCachedColumnVersionList().getQuick(recordIndex + ColumnVersionReader.COLUMN_INDEX_OFFSET);
                Assert.assertEquals(
                        "cellKey=0 must pack to the bare columnIndex -- high 32 bits zero (byte-identical to the " +
                                "pre-composite-partitioning layout)",
                        (long) yIndex, rawPacked
                );
                // Corroborates this is genuinely the row-count column-top record, not the sentinel:
                // its columnTop must equal the real row count (2) at ALTER time.
                Assert.assertEquals(2L, cvReader.getColumnTopByIndex(recordIndex));

                // Additional coverage: the OTHER real call site in the same ADD COLUMN path
                // (TableWriter.addColumn's unconditional upsertDefaultTxnName) must ALSO pack cellKey=0
                // byte-identically.
                int defaultRecordIndex = cvReader.getRecordIndex(ColumnVersionReader.COL_TOP_DEFAULT_PARTITION, yIndex);
                Assert.assertTrue("the sentinel COL_TOP_DEFAULT_PARTITION record for the new column must exist", defaultRecordIndex > -1);
                long defaultRawPacked = cvReader.getCachedColumnVersionList().getQuick(defaultRecordIndex + ColumnVersionReader.COLUMN_INDEX_OFFSET);
                Assert.assertEquals("sentinel record's cellKey=0 must also pack to the bare columnIndex", (long) yIndex, defaultRawPacked);
            }
        });
    }

    /**
     * Step 3. Writes a COMPOSITE (stride 8) {@code _txn} via two SEPARATE, sequential standalone {@code
     * TxWriter} commits against the same file -- the second commit mutates one cell's size and inserts a
     * brand-new (ts, cellKey) partition, a structurally different record from the first commit, so it
     * necessarily takes the slow/full-record path and is written to the OTHER A/B area than the first
     * commit (see {@code TxWriter.commit}/{@code commitFullRecord}/{@code finishABHeader} -- every
     * commit, fast or slow, targets whichever area the current base-version parity says is inactive,
     * then flips the version to make it current). A brand-new {@link TxReader} -- never touched by
     * either writer -- then reopens the file and must see the SECOND commit's state: 3 partitions, not
     * the first commit's 2, and cell1's mutated size, not its original one. This is only reachable if
     * {@code getPartitionCount()} and every {@code (ts, cellKey)} record are derived purely from the
     * self-describing region-size prefix plus the per-table composite stride at reload time, with no
     * separate format-version field consulted.
     * <p>
     * <b>Honest limitation:</b> this exercises the real, existing A/B ping-pong mechanism across two
     * genuinely fully-written commits and proves "last complete commit wins" -- it does NOT inject a
     * mid-write crash/torn write (this test harness has no fault-injection seam for a partial mmap
     * write), so it does not directly prove recovery from a TORN write. It proves the widened stride-8
     * region rides the existing dual-area commit/reload mechanism the same way the pre-existing stride-4
     * region already reliably does, which is the only crash-safety claim Task 9's brief asks this test to
     * carry.
     */
    @Test
    public void testCompositeTxnCrashSafetyReloadsFromSelfDescribingRegion() throws Exception {
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

                // ---- commit #1: day1/cell0 (size 10) and day1/cell1 (size 20). ----
                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    txWriter.appendPartitionForTest(day1, 10L, 100L, 0);
                    txWriter.appendPartitionForTest(day1, 20L, 101L, 1);
                    txWriter.updateMaxTimestamp(day1 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.commit(new ObjList<>());
                }

                // ---- commit #2 (fresh TxWriter, genuine cold reopen of the same file): mutate cell1's
                // size and insert a brand-new day2/cell0 partition -- structurally different from commit
                // #1, so this necessarily takes the slow/full-record path into the OTHER A/B area. ----
                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    // Sanity: the reopen must see exactly what commit #1 persisted before we mutate it.
                    Assert.assertEquals(2, txWriter.getPartitionCount());

                    txWriter.updatePartitionSizeByCell(day1, 1, 25L);
                    txWriter.insertPartitionForTest(day2, 30L, 102L, 0);
                    txWriter.updateMaxTimestamp(day2 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.commit(new ObjList<>());
                }

                // ---- reopen with a totally FRESH TxReader and confirm it reloads the LAST committed
                // state purely from the self-describing region size + the per-table composite stride. ----
                try (TxReader txReader = new TxReader(ff)) {
                    txReader.setCompositeForTest(true);
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals("must see commit #2's 3 partitions, not commit #1's 2", 3, txReader.getPartitionCount());

                    // partition 0: day1/cell0 -- untouched by commit #2, must survive from commit #1.
                    Assert.assertEquals(day1, txReader.getPartitionTimestampByIndex(0));
                    Assert.assertEquals(0, txReader.getPartitionCellKey(0));
                    Assert.assertEquals(10L, txReader.getPartitionSize(0));
                    Assert.assertEquals(100L, txReader.getPartitionNameTxn(0));

                    // partition 1: day1/cell1 -- commit #2's mutated size (25) must win over commit #1's
                    // original (20); this is the "last committed record wins" proof.
                    Assert.assertEquals(day1, txReader.getPartitionTimestampByIndex(1));
                    Assert.assertEquals(1, txReader.getPartitionCellKey(1));
                    Assert.assertEquals("commit #2's mutation must win over commit #1's original 20L", 25L, txReader.getPartitionSize(1));
                    Assert.assertEquals(101L, txReader.getPartitionNameTxn(1));

                    // partition 2: day2/cell0 -- brand new in commit #2; did not exist after commit #1.
                    Assert.assertEquals(day2, txReader.getPartitionTimestampByIndex(2));
                    Assert.assertEquals(0, txReader.getPartitionCellKey(2));
                    Assert.assertEquals(30L, txReader.getPartitionSize(2));
                    Assert.assertEquals(102L, txReader.getPartitionNameTxn(2));

                    // Bonus: reserved slots 5-7 must still be clean zero for all 3 partitions after two
                    // rounds of commit/reopen (including the A/B swap) at the widened stride.
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
     * Writes a plain (or, with {@code composite=true}, deliberately mis-stride-8) two-partition {@code
     * _txn} region to {@code tableName}'s real file via the standalone {@code TxWriter} tail-append
     * seam, and commits it. Shared by {@link #testPlainTxnAttachedPartitionsRegionByteIdentity()} to
     * build both its plain table and (via the discrimination toggle) the deliberately-wrong comparison.
     */
    private static void buildTxnRegion(CharSequence tableName, FilesFacade ff, boolean composite, long day1, long day2) throws Exception {
        TableToken tableToken = engine.verifyTableName(tableName);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
            try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                txWriter.setCompositeForTest(composite);
                txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                txWriter.appendPartitionForTest(day1, 5L, 500L, 0);
                txWriter.appendPartitionForTest(day2, 7L, 501L, 0);
                txWriter.updateMaxTimestamp(day2 + 1);
                txWriter.finishPartitionSizeUpdate();
                txWriter.commit(new ObjList<>());
            }
        }
    }

    /**
     * Opens a brand-new, standalone {@link TxReader} directly against {@code tableName}'s real {@code
     * _txn} file (never sharing state with whatever {@code TxWriter} built it), loads it, and returns it
     * for the caller to inspect and close. Used to prove reload genuinely round-trips through disk.
     */
    private static TxReader openFreshTxReader(CharSequence tableName, FilesFacade ff, boolean composite) throws Exception {
        TableToken tableToken = engine.verifyTableName(tableName);
        TxReader txReader = new TxReader(ff);
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
            txReader.setCompositeForTest(composite);
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            txReader.unsafeLoadAll();
        }
        return txReader;
    }
}
