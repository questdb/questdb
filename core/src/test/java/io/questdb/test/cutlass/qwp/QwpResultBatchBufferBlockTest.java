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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@link QwpResultBatchBuffer#appendBlock} against {@link QwpResultBatchBuffer#appendRow} over
 * synthetic blocks, independently of what the cursors happen to offer: rows at a stride that is
 * no column's size, NULLs at the edges of every block, BOOLEAN bytes other than 0 and 1, a column
 * read through the record (address 0), the dictionary budget stopping a block at its first and at
 * its last row, a partial emit's suffix carried into the next fill, and a second batch on the same
 * connection dictionary. Both buffers must emit the same bytes.
 */
public class QwpResultBatchBufferBlockTest {
    private static final int BUF_SIZE = 1 << 22;
    private static final Log LOG = LogFactory.getLog(QwpResultBatchBufferBlockTest.class);
    // the row layout, 67 bytes apart: no column's size divides the stride
    private static final int OFF_SYM = 0;
    private static final int OFF_LONG = 4;
    private static final int OFF_TS = 12;
    private static final int OFF_DOUBLE = 20;
    private static final int OFF_INT = 28;
    private static final int OFF_IPV4 = 32;
    private static final int OFF_FLOAT = 36;
    private static final int OFF_SHORT = 40;
    private static final int OFF_CHAR = 42;
    private static final int OFF_BYTE = 44;
    private static final int OFF_BOOL = 45;
    private static final int OFF_UUID = 46;
    private static final int OFF_SYM2 = 62;
    private static final int STRIDE = 67;
    private static final int ROWS = 3000;
    private static final int SYMBOL_KEYS = 40;
    private static final SymbolTable SYMBOL_TABLE = new SymbolTable() {
        @Override
        public boolean supportsKeyValueAccess() {
            return true;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return valueOf(key);
        }

        @Override
        public CharSequence valueOf(int key) {
            return key == VALUE_IS_NULL ? null : "symbol-value-" + key;
        }
    };

    private static final SymbolTableSource SYMBOL_TABLES = new SymbolTableSource() {
        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return SYMBOL_TABLE;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return SYMBOL_TABLE;
        }
    };

    @Test
    public void testOneSymbolColumn() throws Exception {
        assertBlockFillMatchesRowFill(false);
    }

    @Test
    public void testTwoSymbolColumns() throws Exception {
        assertBlockFillMatchesRowFill(true);
    }

    private static void assertBlockFillMatchesRowFill(boolean twoSymbols) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<QwpEgressColumnDef> columns = columns(twoSymbols);
            final long data = Unsafe.malloc((long) ROWS * STRIDE, MemoryTag.NATIVE_DEFAULT);
            final long wireA = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            final long wireB = Unsafe.malloc(BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                final int[] stops = new int[2];
                for (int iteration = 0; iteration < 200; iteration++) {
                    fill(data, rnd);
                    final SyntheticRecord record = new SyntheticRecord(data);
                    final SyntheticBlock block = new SyntheticBlock(data, record, twoSymbols);
                    try (
                            QwpResultBatchBuffer rowBuffer = new QwpResultBatchBuffer();
                            QwpResultBatchBuffer blockBuffer = new QwpResultBatchBuffer();
                            QwpEgressConnSymbolDict rowDict = new QwpEgressConnSymbolDict();
                            QwpEgressConnSymbolDict blockDict = new QwpEgressConnSymbolDict()
                    ) {
                        int row = 0;
                        for (int batch = 0; batch < 3 && row < ROWS; batch++) {
                            rowBuffer.beginBatch(columns, SYMBOL_TABLES, rowDict);
                            blockBuffer.beginBatch(columns, SYMBOL_TABLES, blockDict);
                            // a budget that new entries often pass, so fills stop all over, or one
                            // below the delta section's fixed bytes, so that every fill stops at once
                            final int budget = switch (rnd.nextInt(6)) {
                                case 0, 1 -> Integer.MAX_VALUE;
                                case 2 -> rnd.nextInt(3);
                                default -> rnd.nextInt(400);
                            };
                            int taken = fill(rowBuffer, blockBuffer, block, record, row, budget, rnd, stops);
                            row += taken;
                            if (rnd.nextBoolean() && rowBuffer.getRowCount() > 1 && row < ROWS) {
                                // a partial emit, then its suffix carried into the next fill
                                final int k = 1 + rnd.nextInt(rowBuffer.getRowCount() - 1);
                                assertEmitsMatch(rowBuffer, blockBuffer, k, batch == 0, wireA, wireB);
                                rowBuffer.advanceStartRow(k);
                                blockBuffer.advanceStartRow(k);
                                rowBuffer.advanceDeltaStart();
                                blockBuffer.advanceDeltaStart();
                                row += fill(rowBuffer, blockBuffer, block, record, row, Integer.MAX_VALUE, rnd, stops);
                            }
                            assertEmitsMatch(rowBuffer, blockBuffer, rowBuffer.getRowCount(), batch == 0, wireA, wireB);
                            rowBuffer.advanceStartRow(rowBuffer.getRowCount());
                            blockBuffer.advanceStartRow(blockBuffer.getRowCount());
                        }
                    }
                }
                Assert.assertTrue("the budget must have stopped a block at its first row", stops[0] > 0);
                Assert.assertTrue("the budget must have stopped a block at its last row", stops[1] > 0);
            } finally {
                Unsafe.free(data, (long) ROWS * STRIDE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(wireA, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(wireB, BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertEmitsMatch(
            QwpResultBatchBuffer rowBuffer,
            QwpResultBatchBuffer blockBuffer,
            int rows,
            boolean isFirstBatch,
            long wireA,
            long wireB
    ) {
        Assert.assertEquals(rowBuffer.getRowCount(), blockBuffer.getRowCount());
        final int deltaA = rowBuffer.emitDeltaSection(wireA, wireA + BUF_SIZE);
        final int deltaB = blockBuffer.emitDeltaSection(wireB, wireB + BUF_SIZE);
        assertBytes("delta section", wireA, deltaA, wireB, deltaB);
        final int tableA = rowBuffer.emitTableBlockPrefix(wireA, wireA + BUF_SIZE, rows, isFirstBatch);
        final int tableB = blockBuffer.emitTableBlockPrefix(wireB, wireB + BUF_SIZE, rows, isFirstBatch);
        assertBytes("table block", wireA, tableA, wireB, tableB);
    }

    private static void assertBytes(String what, long a, int lenA, long b, int lenB) {
        Assert.assertTrue(lenA > 0);
        Assert.assertEquals(what + " length", lenA, lenB);
        for (int i = 0; i < lenA; i++) {
            if (Unsafe.getByte(a + i) != Unsafe.getByte(b + i)) {
                Assert.fail(what + " differs at byte " + i + " of " + lenA);
            }
        }
    }

    private static ObjList<QwpEgressColumnDef> columns(boolean twoSymbols) {
        final ObjList<QwpEgressColumnDef> columns = new ObjList<>();
        addColumn(columns, "sym", ColumnType.SYMBOL);
        addColumn(columns, "l", ColumnType.LONG);
        addColumn(columns, "ts", ColumnType.TIMESTAMP);
        addColumn(columns, "d", ColumnType.DOUBLE);
        addColumn(columns, "i", ColumnType.INT);
        addColumn(columns, "ip", ColumnType.IPv4);
        addColumn(columns, "f", ColumnType.FLOAT);
        addColumn(columns, "sh", ColumnType.SHORT);
        addColumn(columns, "ch", ColumnType.CHAR);
        addColumn(columns, "by", ColumnType.BYTE);
        addColumn(columns, "b", ColumnType.BOOLEAN);
        addColumn(columns, "u", ColumnType.UUID);
        if (twoSymbols) {
            addColumn(columns, "sym2", ColumnType.SYMBOL);
        }
        return columns;
    }

    private static void addColumn(ObjList<QwpEgressColumnDef> columns, String name, int type) {
        final QwpEgressColumnDef def = new QwpEgressColumnDef();
        def.of(name, type);
        columns.add(def);
    }

    // random values, NULL in the first and last rows and here and there; symbol keys in runs
    private static void fill(long data, Rnd rnd) {
        int key = rnd.nextInt(SYMBOL_KEYS);
        for (int r = 0; r < ROWS; r++) {
            final long p = data + (long) r * STRIDE;
            final boolean isNull = r == 0 || r == ROWS - 1 || rnd.nextInt(10) == 0;
            if (rnd.nextInt(8) == 0) {
                key = rnd.nextInt(SYMBOL_KEYS);
            }
            Unsafe.putInt(p + OFF_SYM, isNull ? SymbolTable.VALUE_IS_NULL : key);
            Unsafe.putLong(p + OFF_LONG, isNull ? Numbers.LONG_NULL : rnd.nextLong());
            Unsafe.putLong(p + OFF_TS, isNull ? Numbers.LONG_NULL : 1_000_000L * r + rnd.nextInt(1000));
            Unsafe.putDouble(p + OFF_DOUBLE, isNull ? Double.NaN : rnd.nextDouble());
            Unsafe.putInt(p + OFF_INT, isNull ? Numbers.INT_NULL : rnd.nextInt());
            Unsafe.putInt(p + OFF_IPV4, isNull ? Numbers.IPv4_NULL : rnd.nextInt());
            Unsafe.putFloat(p + OFF_FLOAT, isNull ? Float.NaN : rnd.nextFloat());
            Unsafe.putShort(p + OFF_SHORT, rnd.nextShort());
            Unsafe.putChar(p + OFF_CHAR, rnd.nextChar());
            Unsafe.putByte(p + OFF_BYTE, rnd.nextByte());
            // the getter reads a byte of 1 as true, any other as false
            Unsafe.putByte(p + OFF_BOOL, (byte) rnd.nextInt(3));
            Unsafe.putLong(p + OFF_UUID, isNull ? Numbers.LONG_NULL : rnd.nextLong());
            Unsafe.putLong(p + OFF_UUID + 8, isNull ? Numbers.LONG_NULL : rnd.nextLong());
            Unsafe.putInt(p + OFF_SYM2, rnd.nextInt(4) == 0 ? SymbolTable.VALUE_IS_NULL : rnd.nextInt(SYMBOL_KEYS));
        }
    }

    /**
     * Fills both buffers from {@code from} on, up to the end of the data or the budget: the row
     * buffer as the egress row loop does, the block buffer as its block loop does, from blocks of
     * random sizes. Asserts both took the same rows.
     *
     * @return rows taken
     */
    private static int fill(
            QwpResultBatchBuffer rowBuffer,
            QwpResultBatchBuffer blockBuffer,
            SyntheticBlock block,
            SyntheticRecord record,
            int from,
            int budget,
            Rnd rnd,
            int[] stops
    ) {
        final int maxRows = Math.min(ROWS - from, 1 + rnd.nextInt(1500));
        int rowTaken = 0;
        while (rowTaken < maxRows) {
            rowBuffer.appendRow(record.of(from + rowTaken++));
            if (rowBuffer.currentBatchDeltaWireBytes() > budget) {
                break;
            }
        }
        // blocks of up to 3 rows, so that the budget often stops one at its first or last row,
        // or of up to 400
        final int blockRowsMax = rnd.nextBoolean() ? 3 : 400;
        int blockTaken = 0;
        while (blockTaken < maxRows) {
            final int blockRows = Math.min(maxRows - blockTaken, 1 + rnd.nextInt(blockRowsMax));
            block.of(from + blockTaken, blockRows);
            final int taken = blockBuffer.appendBlock(block, budget);
            Assert.assertTrue(taken >= 1 && taken <= blockRows);
            blockTaken += taken;
            if (blockBuffer.currentBatchDeltaWireBytes() > budget) {
                if (blockRows > 1 && taken == 1) {
                    stops[0]++;
                } else if (blockRows > 1 && taken == blockRows) {
                    stops[1]++;
                }
                break;
            }
            Assert.assertEquals("a block may stop early only on the budget", blockRows, taken);
        }
        Assert.assertEquals("rows taken", rowTaken, blockTaken);
        return rowTaken;
    }

    // rows [firstRow, firstRow + rowCount) of the data; UUID and the second SYMBOL column are read
    // through the record
    private static class SyntheticBlock implements RecordBlock {
        private final long data;
        private final SyntheticRecord record;
        private final boolean twoSymbols;
        private int firstRow;
        private int rowCount;

        SyntheticBlock(long data, SyntheticRecord record, boolean twoSymbols) {
            this.data = data;
            this.record = record;
            this.twoSymbols = twoSymbols;
        }

        @Override
        public long getColumnAddress(int columnIndex) {
            final long p = data + (long) firstRow * STRIDE;
            return switch (columnIndex) {
                case 0 -> p + OFF_SYM;
                case 1 -> p + OFF_LONG;
                case 2 -> p + OFF_TS;
                case 3 -> p + OFF_DOUBLE;
                case 4 -> p + OFF_INT;
                case 5 -> p + OFF_IPV4;
                case 6 -> p + OFF_FLOAT;
                case 7 -> p + OFF_SHORT;
                case 8 -> p + OFF_CHAR;
                case 9 -> p + OFF_BYTE;
                case 10 -> p + OFF_BOOL;
                default -> {
                    // UUID, and the second SYMBOL column: read through the record
                    Assert.assertTrue(columnIndex == 11 || (twoSymbols && columnIndex == 12));
                    yield 0;
                }
            };
        }

        @Override
        public long getColumnStride(int columnIndex) {
            return STRIDE;
        }

        @Override
        public Record getRecordAt(int row) {
            Assert.assertTrue(row >= 0 && row < rowCount);
            return record.of(firstRow + row);
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        void of(int firstRow, int rowCount) {
            this.firstRow = firstRow;
            this.rowCount = rowCount;
        }
    }

    private static class SyntheticRecord implements Record {
        private final long data;
        private long p;

        SyntheticRecord(long data) {
            this.data = data;
        }

        @Override
        public boolean getBool(int col) {
            Assert.assertEquals(10, col);
            return Unsafe.getByte(p + OFF_BOOL) == 1;
        }

        @Override
        public byte getByte(int col) {
            Assert.assertEquals(9, col);
            return Unsafe.getByte(p + OFF_BYTE);
        }

        @Override
        public char getChar(int col) {
            Assert.assertEquals(8, col);
            return Unsafe.getChar(p + OFF_CHAR);
        }

        @Override
        public double getDouble(int col) {
            Assert.assertEquals(3, col);
            return Unsafe.getDouble(p + OFF_DOUBLE);
        }

        @Override
        public float getFloat(int col) {
            Assert.assertEquals(6, col);
            return Unsafe.getFloat(p + OFF_FLOAT);
        }

        @Override
        public int getInt(int col) {
            return switch (col) {
                case 0 -> Unsafe.getInt(p + OFF_SYM);
                case 4 -> Unsafe.getInt(p + OFF_INT);
                case 5 -> Unsafe.getInt(p + OFF_IPV4);
                case 12 -> Unsafe.getInt(p + OFF_SYM2);
                default -> throw new AssertionError("getInt(" + col + ")");
            };
        }

        @Override
        public int getIPv4(int col) {
            Assert.assertEquals(5, col);
            return Unsafe.getInt(p + OFF_IPV4);
        }

        @Override
        public long getLong(int col) {
            Assert.assertEquals(1, col);
            return Unsafe.getLong(p + OFF_LONG);
        }

        @Override
        public long getLong128Hi(int col) {
            Assert.assertEquals(11, col);
            return Unsafe.getLong(p + OFF_UUID + 8);
        }

        @Override
        public long getLong128Lo(int col) {
            Assert.assertEquals(11, col);
            return Unsafe.getLong(p + OFF_UUID);
        }

        @Override
        public short getShort(int col) {
            Assert.assertEquals(7, col);
            return Unsafe.getShort(p + OFF_SHORT);
        }

        @Override
        public long getTimestamp(int col) {
            Assert.assertEquals(2, col);
            return Unsafe.getLong(p + OFF_TS);
        }

        SyntheticRecord of(int row) {
            p = data + (long) row * STRIDE;
            return this;
        }
    }
}
