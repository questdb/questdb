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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.server.egress.QwpBackwardReadAhead;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Backward page-frame append over the frames the engine's backward cursor builds: every
 * column type, column tops, parquet and native partitions, partitions split into several
 * frames. Appending a frame backward must produce exactly the batch that appending its
 * rows one at a time, last to first, through the forward path produces - with the
 * columnar kernels and with the per-row fallbacks, which a SYMBOL column takes when the
 * batch has no symbol tables to resolve keys with. Reading each frame ahead covers the
 * same bytes whether it reads the frame at once or asks for it in MADV_WILLNEED windows.
 */
public class QwpResultBatchBufferBackwardFramesTest extends AbstractCairoTest {

    private static final Log LOG = LogFactory.getLog(QwpResultBatchBufferBackwardFramesTest.class);

    @Test
    public void testBackwardAppendOfRealFramesMatchesReversedRows() throws Exception {
        // parquet row groups are page frames too: split the converted partition into several
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 40);
        assertMemoryLeak(() -> {
            sqlExecutionContext.setRandom(TestUtils.generateRandom(LOG));
            // split each 150-row partition into frames of at most 64 rows
            sqlExecutionContext.changePageFrameSizes(8, 64);
            final int rowsPerPartition = 150;
            final long tsStep = 86_400_000_000L / rowsPerPartition;
            execute("CREATE TABLE t (" + QwpEgressBackwardScanTest.ALL_TYPES_COLUMNS + ") TIMESTAMP(ts) PARTITION BY DAY");
            // rows before the added columns: day 1 and the first half of day 2
            final long firstRows = rowsPerPartition + rowsPerPartition / 2;
            execute("INSERT INTO t " + QwpEgressBackwardScanTest.allTypesSelect(1, firstRows, 0, tsStep));
            for (String column : QwpEgressBackwardScanTest.COLUMN_TOP_COLUMNS.split(",\n")) {
                execute("ALTER TABLE t ADD COLUMN " + column.trim());
            }
            final long restRows = 3L * rowsPerPartition - firstRows;
            final String select = QwpEgressBackwardScanTest.allTypesSelect(firstRows + 1, restRows, firstRows * tsStep, tsStep);
            execute("INSERT INTO t " + select.substring(0, select.lastIndexOf(" FROM long_sequence("))
                    + QwpEgressBackwardScanTest.columnTopSelect(restRows));
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");

            // The oracle appends one row at a time, so it visits the columns row by row,
            // while a multi-row append visits them column by column. SYMBOL columns share
            // the connection dictionary and take ids in the order they first meet a value,
            // so with two SYMBOL columns in one projection the two orders assign different
            // ids to the same values; each projection therefore keeps one SYMBOL column.
            final StringBuilder columns = new StringBuilder();
            for (String definition : (QwpEgressBackwardScanTest.ALL_TYPES_COLUMNS + ",\n" + QwpEgressBackwardScanTest.COLUMN_TOP_COLUMNS).split(",\n")) {
                final String name = definition.trim().split(" ")[0];
                if (!name.equals("tsy")) {
                    columns.append(columns.isEmpty() ? "" : ", ").append(name);
                }
            }
            final int[] frameCounts = new int[2];
            assertBackwardFrames("SELECT " + columns + " FROM t ORDER BY ts DESC", frameCounts);
            // native and parquet frames, several per partition
            Assert.assertTrue("native frames: " + frameCounts[0], frameCounts[0] > 4);
            Assert.assertTrue("parquet frames: " + frameCounts[1], frameCounts[1] > 1);
            // the SYMBOL column added after the first rows, through its column top
            assertBackwardFrames("SELECT ts, tvc, vc, x, tsy, bn, x FROM t ORDER BY ts DESC", new int[2]);
        });
    }

    private static byte[] copy(long address, int len) {
        final byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getByte(address + i);
        }
        return bytes;
    }

    private static byte[] emit(QwpResultBatchBuffer buffer) {
        final int size = buffer.computeTableBlockSize(buffer.getRowCount(), false);
        final long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(size, buffer.emitTableBlock(buf, buf + size, false));
            return copy(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] emitDelta(QwpResultBatchBuffer buffer) {
        final int size = buffer.computeDeltaSize();
        final long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(size, buffer.emitDeltaSection(buf, buf + size));
            return copy(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertBackwardFrames(String sql, int[] frameCounts) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_DESC);
                PageFrameAddressCache addressCache = new PageFrameAddressCache();
                PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, 0L);
                PageFrameMemoryRecord record = new PageFrameMemoryRecord();
                QwpResultBatchBuffer backward = new QwpResultBatchBuffer();
                QwpResultBatchBuffer oracle = new QwpResultBatchBuffer();
                QwpEgressConnSymbolDict backwardDict = new QwpEgressConnSymbolDict();
                QwpEgressConnSymbolDict oracleDict = new QwpEgressConnSymbolDict();
                QwpEgressConnSymbolDict backwardTextDict = new QwpEgressConnSymbolDict();
                QwpEgressConnSymbolDict oracleTextDict = new QwpEgressConnSymbolDict()
        ) {
            Assert.assertNotNull(sql, cursor);
            final RecordMetadata metadata = factory.getMetadata();
            final ObjList<QwpEgressColumnDef> defs = new ObjList<>();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final QwpEgressColumnDef def = new QwpEgressColumnDef();
                def.of(metadata.getColumnName(i), metadata.getColumnType(i));
                defs.add(def);
            }
            final QwpBackwardReadAhead readFrame = new QwpBackwardReadAhead(new QwpBackwardReadAheadTest.RecordingFacade(false));
            readFrame.of(metadata);
            final QwpBackwardReadAheadTest.RecordingFacade willNeed = new QwpBackwardReadAheadTest.RecordingFacade(true);
            final QwpBackwardReadAhead adviseFrame = new QwpBackwardReadAhead(willNeed);
            adviseFrame.of(metadata);
            addressCache.of(metadata, cursor.getColumnMapping(), cursor.isExternal());
            pool.of(addressCache);
            record.of(cursor);
            try {
                int frameIndex = 0;
                long previousTs = Long.MAX_VALUE;
                PageFrame frame;
                while ((frame = cursor.next()) != null) {
                    addressCache.add(frameIndex, frame);
                    pool.navigateTo(frameIndex, record);
                    final long rows = frame.getPartitionHi() - frame.getPartitionLo();
                    final String context = sql + " frame=" + frameIndex + " rows=" + rows;
                    // frames arrive newest first
                    final int tsIndex = metadata.getColumnIndex("ts");
                    record.setRowIndex(rows - 1);
                    Assert.assertTrue(context, record.getTimestamp(tsIndex) < previousTs);
                    record.setRowIndex(0);
                    previousTs = record.getTimestamp(tsIndex);

                    // with the cursor's symbol tables SYMBOL keys go columnar, without them
                    // SYMBOL takes the per-row fallback
                    assertBackwardFrame(context, frame, record, rows, defs, cursor, backward, backwardDict, oracle, oracleDict);
                    assertBackwardFrame(context, frame, record, rows, defs, null, backward, backwardTextDict, oracle, oracleTextDict);

                    // the whole-frame read and the MADV_WILLNEED windows cover the same bytes,
                    // each once and inside the frame's published extents
                    readFrame.ofFrame(frame, rows);
                    final long frameBytes = readFrame.advise(Math.max(0, rows - 1_024));
                    willNeed.clear();
                    adviseFrame.ofFrame(frame, rows);
                    long advisedBytes = 0;
                    for (long emitted = 0; emitted < rows; ) {
                        final long slice = Math.min(1_024, rows - emitted);
                        advisedBytes += adviseFrame.advise(rows - emitted - slice);
                        emitted += slice;
                    }
                    Assert.assertEquals(context, frameBytes, advisedBytes);
                    Assert.assertEquals(context, frameBytes, willNeed.requestedBytes());
                    willNeed.assertWithinExtents(context, frame);
                    if (frame.getFormat() == PartitionFormat.NATIVE) {
                        frameCounts[0]++;
                        Assert.assertTrue(context, frameBytes > 0);
                    } else {
                        frameCounts[1]++;
                        Assert.assertEquals(context, 0, frameBytes);
                    }
                    frameIndex++;
                }
                Assert.assertTrue(sql, frameIndex > 1);
            } finally {
                record.of(null);
                pool.releaseQueryResources();
                addressCache.clear();
            }
        }
    }

    private void assertBackwardFrame(
            String context,
            PageFrame frame,
            PageFrameMemoryRecord record,
            long rows,
            ObjList<QwpEgressColumnDef> defs,
            SymbolTableSource symbolTables,
            QwpResultBatchBuffer backward,
            QwpEgressConnSymbolDict backwardDict,
            QwpResultBatchBuffer oracle,
            QwpEgressConnSymbolDict oracleDict
    ) {
        backward.beginBatch(defs, symbolTables, backwardDict);
        oracle.beginBatch(defs, symbolTables, oracleDict);
        backward.appendPageFrame(frame, record, 0, rows, true);
        for (long r = rows - 1; r >= 0; r--) {
            oracle.appendPageFrame(frame, record, r, r + 1, false);
        }
        final String ctx = context + " symbolTables=" + (symbolTables != null);
        Assert.assertEquals(ctx, rows, backward.getRowCount());
        Assert.assertEquals(ctx, rows, oracle.getRowCount());
        Assert.assertArrayEquals(ctx, emit(oracle), emit(backward));
        Assert.assertArrayEquals(ctx, emitDelta(oracle), emitDelta(backward));
        // the next frame's batch starts from the dictionary state this one leaves
        backward.advanceStartRow(backward.getRowCount());
        oracle.advanceStartRow(oracle.getRowCount());
    }
}
