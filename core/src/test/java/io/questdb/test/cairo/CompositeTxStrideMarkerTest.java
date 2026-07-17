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
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.RandomAccessFile;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_PARTITION_STRIDE_32;

/**
 * Plan 3b, Task 1: the {@code _txn} base header gets a self-describing partition-stride marker -- a
 * fixed 4-byte int at {@link TableUtils#TX_BASE_OFFSET_PARTITION_STRIDE_32}, in space that was pure
 * zero padding before this task -- so that ANY {@link TxReader}, even one that is never told via the
 * package-private {@code setComposite(boolean)} (or its test seam {@code setCompositeForTest}), can
 * tell whether the {@code _txn} file it is opening uses the plain (4-long) or COMPOSITE (8-long)
 * attached-partition record stride, purely from the file itself.
 * <p>
 * {@code 0} means plain (byte-identical to the zero padding this offset held before this task);
 * {@code 8} means composite. Written at every base-header write site ({@link TxReader#dumpTo},
 * {@code TableUtils#createTxn}, {@code TxWriter#finishABHeader}) and read once, early, in {@link
 * TxReader#unsafeLoadBaseOffset()} -- before {@code unsafeLoadPartitions} divides the raw
 * attached-partitions region by the stride.
 * <p>
 * This task does NOT remove the existing {@code setComposite} call TableWriter/TableReader make from
 * table metadata (that is Plan 3b Task 2) -- it only proves the marker alone is sufficient, in a reader
 * that never receives that call.
 */
public class CompositeTxStrideMarkerTest extends AbstractCairoTest {

    /**
     * Plain table: the marker must read back as exactly 0 -- byte-identical to the zero padding that
     * occupied this offset before this task -- and a totally fresh {@link TxReader}, given NO {@code
     * setComposite} call at all, must self-describe as stride 4 with the correct partition count.
     */
    @Test
    public void testPlainMarkerIsZeroAndReaderSelfDescribesStrideFour() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            engine.releaseInactive(); // no pooled writer/reader may keep _txn open under our direct use

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("p");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    // Deliberately NO setCompositeForTest call -- exactly what a real plain table's
                    // writer does today. Stride 4 is the TxReader/TxWriter class default.
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    txWriter.appendPartitionForTest(day1, 5L, 500L, 0);
                    txWriter.appendPartitionForTest(day2, 7L, 501L, 0);
                    txWriter.updateMaxTimestamp(day2 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.commit(new ObjList<>());
                }

                // Fresh reader, NO setComposite call at all.
                try (TxReader txReader = new TxReader(ff)) {
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals("plain table must self-describe as stride 4", 4, txReader.getLongsPerAttachedPartition());
                    Assert.assertEquals(2, txReader.getPartitionCount());
                }

                // Raw byte-identity proof: read the marker offset directly off disk with a plain
                // java.io.RandomAccessFile -- bypassing every production accessor this task also touches,
                // so this assertion cannot be fooled by a bug shared between the write and read sides.
                // Must be exactly 0: the same value this offset held as pure padding before this task.
                try (RandomAccessFile raf = new RandomAccessFile(path.toString(), "r")) {
                    raf.seek(TX_BASE_OFFSET_PARTITION_STRIDE_32);
                    int marker = raf.readInt();
                    Assert.assertEquals(
                            "marker must be exactly 0 for a plain table -- byte-identical to legacy zero padding",
                            0, marker);
                }
            }
        });
    }

    /**
     * Composite table: build a stride-8 {@code _txn} directly ({@code setCompositeForTest} +
     * {@code appendPartitionForTest} + commit), then open a completely FRESH {@link TxReader} that is
     * NEVER told {@code setComposite} -- proving the marker this task adds, not any caller-supplied
     * hint, is what makes the reader self-detect stride 8, the correct partition count, and correct
     * {@code (ts, cellKey)} records.
     */
    @Test
    public void testCompositeReaderSelfDetectsStrideFromMarkerWithNoSetComposite() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
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
                    txWriter.updateMaxTimestamp(day1 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.commit(new ObjList<>());
                }

                // THE point of this test: a brand-new TxReader, given NO setComposite call whatsoever.
                try (TxReader txReader = new TxReader(ff)) {
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals(
                            "must self-detect COMPOSITE stride from the marker alone -- setComposite was never called",
                            8, txReader.getLongsPerAttachedPartition());
                    Assert.assertEquals(
                            "self-detected stride must yield the true partition count, not a stride-4 mis-fold of the same bytes",
                            2, txReader.getPartitionCount());

                    Assert.assertEquals(day1, txReader.getPartitionTimestampByIndex(0));
                    Assert.assertEquals(0, txReader.getPartitionCellKey(0));
                    Assert.assertEquals(10L, txReader.getPartitionSize(0));
                    Assert.assertEquals(100L, txReader.getPartitionNameTxn(0));

                    Assert.assertEquals(day1, txReader.getPartitionTimestampByIndex(1));
                    Assert.assertEquals(1, txReader.getPartitionCellKey(1));
                    Assert.assertEquals(20L, txReader.getPartitionSize(1));
                    Assert.assertEquals(101L, txReader.getPartitionNameTxn(1));
                }
            }
        });
    }

    /**
     * Plan 3b, Task 2: the real {@link TableReader} -- not just a standalone {@link TxReader}, as above
     * -- must correctly self-detect a composite table's true stride and true partition content. A
     * completely ordinary {@code getReader()} open of a (dormant, single-cell -- real (ts, cellKey)
     * write-routing is Plan 4) composite table, built via real SQL inserts across several day partitions,
     * must report the TRUE partition count and TRUE row content -- identical to an equivalent plain table
     * built from the same rows.
     * <p>
     * Task 2 investigated retiring {@link TableReader}'s metadata-threaded {@code setComposite(metadata
     * .getPartitionSpec().getDimensionCount() > 0)} call in favour of relying on the marker alone (this
     * test would keep passing either way, since both committed tables here have real partitions and are
     * therefore already marker-upgradeable). That removal was REVERTED: {@code
     * CompositeTxCellTest#testStrideDerivedFromComposite} proved it unsafe for a composite table with
     * ZERO ever-committed partitions (a fresh {@code CREATE TABLE}, before any insert -- the on-disk
     * marker was still 0 in that window under Task 1/2's upgrade-only read, so a reader opened then had
     * no signal at all without the explicit call). See the task report for the full RED/GREEN evidence.
     * Task 3 later closed that specific window (createTxn now writes the real marker, 8 for composite,
     * from CREATE) and made the read symmetric, but did not re-investigate this removal -- out of that
     * task's scope. This test remains a regression lock for ordinary (non-empty) composite
     * {@link TableReader} correctness regardless.
     * <p>
     * Separately, see the task report for the RED/GREEN discrimination proving the MARKER itself (not
     * leftover threading) is what makes {@code table_storage()} -- a site that has never called {@code
     * setComposite} at all -- self-heal ({@link CompositeTxnConsumerSitesTest}).
     */
    @Test
    public void testTableReaderPartitionCountAndFullScanMatchPlainEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
            execute("create table p (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day");

            final String rows = " values " +
                    "('2020-01-01T00:00:00.000000Z','A',1.0), " +
                    "('2020-01-01T12:00:00.000000Z','A',2.0), " +
                    "('2020-01-02T00:00:00.000000Z','A',3.0), " +
                    "('2020-01-03T06:00:00.000000Z','A',4.0)";
            execute("insert into c" + rows);
            execute("insert into p" + rows);
            engine.releaseInactive(); // cold reopen -- no pooled reader may mask a fresh self-detect

            try (TableReader cr = getReader("c"); TableReader pr = getReader("p")) {
                Assert.assertEquals(
                        "composite TableReader must report the TRUE partition count, equal to an " +
                                "equivalent plain table's",
                        pr.getPartitionCount(), cr.getPartitionCount());
                Assert.assertEquals(3, cr.getPartitionCount());
            }

            assertSqlCursors("select ts, exchange, x from p order by ts", "select ts, exchange, x from c order by ts");
        });
    }

    /**
     * Plan 3b, Task 3: the marker must be correct from the moment the table is CREATEd, not just once
     * a first commit runs. Before this task, {@code TableUtils#createTxn} -- the physical file-creation
     * write, before any {@link TxWriter}/metadata object exists -- always wrote the literal plain
     * default {@code 0}, even for a composite table, relying on the marker "catching up" to {@code 8}
     * only once {@code TxWriter#finishABHeader} ran the table's first real commit. That create-time
     * window is exactly what forced {@link TxReader#unsafeLoadBaseOffset()}'s marker read to be
     * upgrade-only in the first place (see that method's Task 1 comment: a symmetric read taken at face
     * value during the window would have stomped an uncommitted composite table's stride back to
     * plain). This test proves the window is now closed: immediately after {@code CREATE TABLE}, with
     * NO row ever inserted and NO commit ever run, the on-disk marker already reads {@code 8} for a
     * composite table -- pre-fix this read back {@code 0}.
     */
    @Test
    public void testCreateTimeMarkerIsCompositeBeforeAnyCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
            engine.releaseInactive(); // no pooled writer/reader may keep _txn open under our direct use

            TableToken tableToken = engine.verifyTableName("c");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (RandomAccessFile raf = new RandomAccessFile(path.toString(), "r")) {
                    raf.seek(TX_BASE_OFFSET_PARTITION_STRIDE_32);
                    // RandomAccessFile#readInt() is big-endian (java.io.DataInput contract); QuestDB's
                    // off-heap memory (Unsafe.putInt, native byte order) writes little-endian on
                    // x86/ARM. reverseBytes reconciles the two -- without it a real 8 misreads as
                    // 0x08000000 (134217728), not 0, so this is NOT byte-identity-neutral like the
                    // existing plain-marker (0) checks, which happen to be order-invariant.
                    int marker = Integer.reverseBytes(raf.readInt());
                    Assert.assertEquals(
                            "composite table's _txn marker must already be 8 right after CREATE TABLE, " +
                                    "before any insert/commit -- createTxn must write the real marker " +
                                    "from creation, not always the plain default",
                            8, marker);
                }
            }
        });
    }

    /**
     * Companion byte-identity check for the test above, from the SAME create-time call site: a plain
     * table's marker must still read back {@code 0} immediately after {@code CREATE TABLE} -- unchanged
     * by threading composite-ness into {@code createTxn}'s caller.
     */
    @Test
    public void testCreateTimeMarkerIsZeroForPlainTableBeforeAnyCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, x double) timestamp(ts) partition by day");
            engine.releaseInactive();

            TableToken tableToken = engine.verifyTableName("p");
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (RandomAccessFile raf = new RandomAccessFile(path.toString(), "r")) {
                    raf.seek(TX_BASE_OFFSET_PARTITION_STRIDE_32);
                    // See the composite test above for why reverseBytes is required here (it is a no-op
                    // for 0 either way, but kept for consistency with that test's real requirement).
                    int marker = Integer.reverseBytes(raf.readInt());
                    Assert.assertEquals(
                            "plain table's _txn marker must remain 0 right after CREATE TABLE",
                            0, marker);
                }
            }
        });
    }
}
