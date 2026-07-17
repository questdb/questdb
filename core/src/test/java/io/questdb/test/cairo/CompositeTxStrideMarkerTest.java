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
}
