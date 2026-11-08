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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the backward page-frame append of {@link QwpResultBatchBuffer}: appending rows
 * {@code [lo, hi)} of a frame backward must produce exactly the batch that appending the
 * same rows one at a time, {@code hi - 1} down to {@code lo}, through the forward path
 * produces. The forward single-row appends are the oracle, so every columnar kernel is
 * checked against the kernel it mirrors, for every fast-path type, every slice length up
 * to two 1,024-row slices, and every bit alignment a preceding prefix of rows leaves in
 * the null bitmap and in the bit-packed BOOLEAN values.
 */
public class QwpResultBatchBufferBackwardTest {

    private static final Log LOG = LogFactory.getLog(QwpResultBatchBufferBackwardTest.class);
    // Column types with a columnar fast path in appendPageFrame, i.e. the ones that never
    // touch the record. Narrow DECIMALs, GEOHASH and the var-size types take the per-row
    // fallback; QwpEgressBackwardScanTest covers those end to end.
    private static final int[] TYPES = {
            ColumnType.LONG,
            ColumnType.DATE,
            ColumnType.TIMESTAMP,
            ColumnType.TIMESTAMP_NANO,
            ColumnType.getDecimalType(18, 4),
            ColumnType.DOUBLE,
            ColumnType.INT,
            ColumnType.IPv4,
            ColumnType.FLOAT,
            ColumnType.SHORT,
            ColumnType.CHAR,
            ColumnType.BYTE,
            ColumnType.BOOLEAN,
            ColumnType.SYMBOL,
    };
    private static final int SYMBOL_KEY_COUNT = 37;

    @Test
    public void testBackwardAppendMatchesReversedForwardRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            // Every slice length up to past one full word of each element width, then random
            // lengths up to two full slices, each after a random prefix.
            for (int n = 0; n <= 70; n++) {
                assertBackwardMatchesForward(rnd, n, rnd.nextInt(20), false);
            }
            for (int i = 0; i < 200; i++) {
                assertBackwardMatchesForward(rnd, 1 + rnd.nextInt(2_048), rnd.nextInt(40), rnd.nextInt(4) == 0);
            }
        });
    }

    @Test
    public void testBackwardAppendOfSingleRowAndEmptyRange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int prefix = 0; prefix < 17; prefix++) {
                assertBackwardMatchesForward(rnd, 1, prefix, false);
                assertBackwardMatchesForward(rnd, 0, prefix, false);
            }
        });
    }

    @Test
    public void testBackwardAppendOverColumnTops() throws Exception {
        // A frame inside a column top publishes address 0 for that column: SHORT, CHAR,
        // BYTE and BOOLEAN ship zero values, every other type ships NULLs. Direction must
        // not matter.
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int i = 0; i < 40; i++) {
                assertBackwardMatchesForward(rnd, 1 + rnd.nextInt(1_500), rnd.nextInt(20), true);
            }
        });
    }

    @Test
    public void testBackwardAppendReversesSliceExactly() throws Exception {
        // A hand-checked case independent of the forward path: BYTE, SHORT and LONG values
        // 1..19 with LONG NULL on every 5th row, appended backward from rows [2, 19).
        TestUtils.assertMemoryLeak(() -> {
            final int rowCount = 19;
            final int[] types = {ColumnType.BYTE, ColumnType.SHORT, ColumnType.LONG};
            try (NativeFrame frame = new NativeFrame(types, rowCount)) {
                for (int r = 0; r < rowCount; r++) {
                    Unsafe.putByte(frame.address(0) + r, (byte) (r + 1));
                    Unsafe.putShort(frame.address(1) + 2L * r, (short) (-(r + 1)));
                    Unsafe.putLong(frame.address(2) + 8L * r, (r + 1) % 5 == 0 ? Numbers.LONG_NULL : (r + 1) * 1_000L);
                }
                final ObjList<QwpEgressColumnDef> defs = columnDefs(types);
                try (
                        QwpResultBatchBuffer buffer = new QwpResultBatchBuffer();
                        QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()
                ) {
                    buffer.beginBatch(defs, null, dict);
                    buffer.appendPageFrame(frame, null, 2, 19, true);
                    Assert.assertEquals(17, buffer.getRowCount());
                    final byte[] wire = emit(buffer);

                    // Expected table block, written out by hand: empty table name, row count,
                    // no schema (continuation batch), then per column the null flag, bitmap
                    // and dense values in emit order 19, 18, ..., 3.
                    final StringSink expected = new StringSink();
                    expected.put("0,17,");
                    // BYTE: no nulls, values 19..3
                    expected.put("0,");
                    for (int v = 19; v >= 3; v--) {
                        expected.put(v).put(',');
                    }
                    // SHORT: no nulls, values -19..-3 as little-endian pairs
                    expected.put("0,");
                    for (int v = 19; v >= 3; v--) {
                        final short s = (short) -v;
                        expected.put(s & 0xFF).put(',').put((s >> 8) & 0xFF).put(',');
                    }
                    // LONG: rows 15 and 10 and 5 (values 15, 10, 5) are NULL; they sit at emit
                    // positions 4, 9 and 14 -> bitmap bytes 0b0001_0000, 0b0100_0010, 0b0000_0000
                    expected.put("1,16,66,0,");
                    for (int v = 19; v >= 3; v--) {
                        if (v % 5 != 0) {
                            final long l = v * 1_000L;
                            for (int b = 0; b < 8; b++) {
                                expected.put((int) ((l >>> (8 * b)) & 0xFF)).put(',');
                            }
                        }
                    }
                    Assert.assertEquals(expected.toString(), unsignedCsv(wire));
                }
            }
        });
    }

    private static void assertBackwardMatchesForward(Rnd rnd, int sliceLen, int prefixLen, boolean withColumnTops) {
        final int rowCount = prefixLen + sliceLen + rnd.nextInt(5);
        final int lo = prefixLen + (rowCount - prefixLen - sliceLen > 0 ? rnd.nextInt(rowCount - prefixLen - sliceLen + 1) : 0);
        final int hi = lo + sliceLen;
        try (NativeFrame frame = new NativeFrame(TYPES, rowCount)) {
            fill(rnd, frame, rowCount);
            if (withColumnTops) {
                for (int ci = 0; ci < TYPES.length; ci++) {
                    if (rnd.nextBoolean()) {
                        frame.clearAddress(ci);
                    }
                }
            }
            final ObjList<QwpEgressColumnDef> defs = columnDefs(TYPES);
            final SymbolTableSource symbols = new KeySymbolTableSource();
            try (
                    QwpResultBatchBuffer backward = new QwpResultBatchBuffer();
                    QwpResultBatchBuffer oracle = new QwpResultBatchBuffer();
                    QwpEgressConnSymbolDict backwardDict = new QwpEgressConnSymbolDict();
                    QwpEgressConnSymbolDict oracleDict = new QwpEgressConnSymbolDict()
            ) {
                backward.beginBatch(defs, symbols, backwardDict);
                oracle.beginBatch(defs, symbols, oracleDict);
                // The prefix shifts the backward slice off byte alignment in the null
                // bitmaps and in the bit-packed BOOLEAN values.
                backward.appendPageFrame(frame, null, 0, prefixLen, false);
                oracle.appendPageFrame(frame, null, 0, prefixLen, false);

                backward.appendPageFrame(frame, null, lo, hi, true);
                for (int r = hi - 1; r >= lo; r--) {
                    oracle.appendPageFrame(frame, null, r, r + 1, false);
                }

                final String context = "rowCount=" + rowCount + ", prefix=" + prefixLen
                        + ", lo=" + lo + ", hi=" + hi + ", columnTops=" + withColumnTops;
                Assert.assertEquals(context, oracle.getRowCount(), backward.getRowCount());
                Assert.assertEquals(context, prefixLen + sliceLen, backward.getRowCount());
                Assert.assertArrayEquals(context, emit(oracle), emit(backward));
                Assert.assertArrayEquals(context, emitDelta(oracle), emitDelta(backward));
                Assert.assertEquals(context, oracleDict.size(), backwardDict.size());
            }
        }
    }

    private static ObjList<QwpEgressColumnDef> columnDefs(int[] types) {
        final ObjList<QwpEgressColumnDef> defs = new ObjList<>();
        for (int i = 0; i < types.length; i++) {
            final QwpEgressColumnDef def = new QwpEgressColumnDef();
            def.of("c" + i, types[i]);
            defs.add(def);
        }
        return defs;
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

    private static void fill(Rnd rnd, NativeFrame frame, int rowCount) {
        for (int ci = 0; ci < TYPES.length; ci++) {
            final long base = frame.address(ci);
            for (int r = 0; r < rowCount; r++) {
                final int pick = rnd.nextInt(16);
                switch (ColumnType.tagOf(TYPES[ci])) {
                    // TIMESTAMP_NANO carries the TIMESTAMP tag
                    case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DECIMAL64 -> Unsafe.putLong(
                            base + 8L * r,
                            switch (pick) {
                                case 0, 1 -> Numbers.LONG_NULL;
                                case 2 -> Long.MIN_VALUE + 1;
                                case 3 -> Long.MAX_VALUE;
                                case 4 -> 0;
                                case 5 -> -1;
                                default -> rnd.nextLong();
                            }
                    );
                    case ColumnType.DOUBLE -> Unsafe.putDouble(
                            base + 8L * r,
                            switch (pick) {
                                case 0, 1 -> Double.NaN;
                                case 2 -> -0.0;
                                case 3 -> Double.POSITIVE_INFINITY;
                                case 4 -> Double.NEGATIVE_INFINITY;
                                case 5 -> Double.MIN_VALUE;
                                case 6 -> Double.MAX_VALUE;
                                default -> rnd.nextDouble() * rnd.nextLong();
                            }
                    );
                    case ColumnType.INT -> Unsafe.putInt(
                            base + 4L * r,
                            switch (pick) {
                                case 0, 1 -> Numbers.INT_NULL;
                                case 2 -> Integer.MIN_VALUE + 1;
                                case 3 -> Integer.MAX_VALUE;
                                case 4 -> 0;
                                default -> rnd.nextInt();
                            }
                    );
                    case ColumnType.IPv4 -> Unsafe.putInt(
                            base + 4L * r,
                            switch (pick) {
                                case 0, 1 -> Numbers.IPv4_NULL;
                                case 2 -> -1;
                                case 3 -> 1;
                                default -> rnd.nextInt();
                            }
                    );
                    case ColumnType.FLOAT -> Unsafe.putFloat(
                            base + 4L * r,
                            switch (pick) {
                                case 0, 1 -> Float.NaN;
                                case 2 -> -0.0f;
                                case 3 -> Float.POSITIVE_INFINITY;
                                case 4 -> Float.MIN_VALUE;
                                default -> rnd.nextFloat() * rnd.nextInt();
                            }
                    );
                    case ColumnType.SHORT, ColumnType.CHAR -> Unsafe.putShort(
                            base + 2L * r,
                            switch (pick) {
                                case 0 -> Short.MIN_VALUE;
                                case 1 -> Short.MAX_VALUE;
                                case 2 -> (short) -1;
                                case 3 -> (short) 0;
                                default -> rnd.nextShort();
                            }
                    );
                    case ColumnType.BYTE -> Unsafe.putByte(
                            base + r,
                            switch (pick) {
                                case 0 -> Byte.MIN_VALUE;
                                case 1 -> Byte.MAX_VALUE;
                                case 2 -> (byte) -1;
                                case 3 -> (byte) 0;
                                default -> rnd.nextByte();
                            }
                    );
                    case ColumnType.BOOLEAN -> Unsafe.putByte(base + r, (byte) (rnd.nextBoolean() ? 1 : 0));
                    case ColumnType.SYMBOL -> Unsafe.putInt(
                            base + 4L * r,
                            pick < 3 ? SymbolTable.VALUE_IS_NULL : rnd.nextInt(SYMBOL_KEY_COUNT)
                    );
                    default -> throw new AssertionError("no generator for " + ColumnType.nameOf(TYPES[ci]));
                }
            }
        }
    }

    private static String unsignedCsv(byte[] bytes) {
        final StringSink sink = new StringSink();
        for (byte b : bytes) {
            sink.put(b & 0xFF).put(',');
        }
        return sink.toString();
    }

    /**
     * Symbol table that answers key-value access, so SYMBOL columns take the columnar
     * dictionary-key path of appendPageFrame.
     */
    private static final class KeySymbolTable implements SymbolTable {
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        @Override
        public boolean supportsKeyValueAccess() {
            return true;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return value(sinkB, key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return value(sinkA, key);
        }

        private static CharSequence value(StringSink sink, int key) {
            if (key == VALUE_IS_NULL) {
                return null;
            }
            sink.clear();
            sink.put("sym_").put(key);
            return sink;
        }
    }

    private static final class KeySymbolTableSource implements SymbolTableSource {
        private final KeySymbolTable table = new KeySymbolTable();

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return table;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return new KeySymbolTable();
        }
    }

    /**
     * NATIVE page frame over one malloc'd buffer per column, sized by column type.
     */
    private static final class NativeFrame implements PageFrame, AutoCloseable {
        private final long[] addresses;
        private final long[] allocations;
        private final long[] sizes;
        private final long rowCount;

        NativeFrame(int[] types, int rowCount) {
            this.rowCount = rowCount;
            final int n = types.length;
            addresses = new long[n];
            allocations = new long[n];
            sizes = new long[n];
            for (int i = 0; i < n; i++) {
                // at least one byte so an empty frame still has a valid, freeable address
                sizes[i] = Math.max(1L, (long) rowCount * ColumnType.sizeOf(types[i]));
                allocations[i] = Unsafe.malloc(sizes[i], MemoryTag.NATIVE_DEFAULT);
                addresses[i] = allocations[i];
            }
        }

        long address(int columnIndex) {
            return addresses[columnIndex];
        }

        void clearAddress(int columnIndex) {
            addresses[columnIndex] = 0;
        }

        @Override
        public void close() {
            for (int i = 0; i < allocations.length; i++) {
                Unsafe.free(allocations[i], sizes[i], MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return 0;
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return 0;
        }

        @Override
        public int getColumnCount() {
            return addresses.length;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return addresses[columnIndex];
        }

        @Override
        public long getPageSize(int columnIndex) {
            return addresses[columnIndex] == 0 ? 0 : sizes[columnIndex];
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionHi() {
            return rowCount;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }
    }
}
