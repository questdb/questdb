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

package io.questdb.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * The materialized encoded sort of rows that are their sort key and at most one other fixed-size column (see
 * {@link #isEligible}): each row is a 16-byte entry, its encoded key and that column's value, which a stable
 * radix sort orders by key ({@link Vect#sortEncodedEntriesStable}), so that equal keys keep the base's
 * order, as the row id tie-break of {@link EncodedSortRecordCursor} keeps it. The rows are read back from the
 * entries, the key decoded from its encoding: there is no record chain to fill nor to gather from.
 */
class EncodedSortPayloadRecordCursor implements DelegatingRecordCursor {
    private static final int BUILD_BLOCK_ROWS = 4096;
    // the most rows of a block over the sorted entries, see peekRecordBlock()
    private static final int SORTED_BLOCK_MAX_ROWS = 64 * 1024;
    private final SortKeyEncoder encoder;
    private final DirectLongList entryMem;
    private final boolean isDesc;
    private final int keyColumn;
    private final int keyType;
    private final long maxEntryMemBytes;
    // the other column, -1 for none, its type and size
    private final int payloadColumn;
    private final int payloadSize;
    private final int payloadType;
    private final EntryRecord recordA = new EntryRecord();
    private final EntryRecord recordB = new EntryRecord();
    private RecordCursor baseCursor;
    // a sorted block's key column, decoded, see peekRecordBlock()
    private DirectLongList blockKeys;
    private SortedBlock block;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private long count;
    private long currentAddr;
    private long endAddr;
    // the previous execution's row count, the entries' capacity for a base that does not know its size
    private long lastCount;
    private boolean isOpen;
    private boolean isSorted;
    private MemoryTracker memoryTracker;
    // the stable sort's scratch, as many entries again, freed once they are sorted
    private DirectLongList scratch;
    private long startAddr;

    EncodedSortPayloadRecordCursor(CairoConfiguration configuration, RecordMetadata metadata, IntList sortColumnFilter) {
        this.encoder = new SortKeyEncoder(metadata, sortColumnFilter);
        this.keyColumn = encoder.getDecodableKeyColumn();
        assert keyColumn >= 0;
        this.keyType = ColumnType.tagOf(metadata.getColumnType(keyColumn));
        this.isDesc = encoder.isFirstKeyDescending();
        int other = -1;
        for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
            if (c != keyColumn) {
                other = c;
            }
        }
        this.payloadColumn = other;
        this.payloadType = other >= 0 ? ColumnType.tagOf(metadata.getColumnType(other)) : -1;
        this.payloadSize = other >= 0 ? payloadSize(metadata.getColumnType(other)) : 0;
        this.maxEntryMemBytes = Math.min(configuration.getSqlSortKeyMaxBytes(), SortKeyEncoder.MAX_ENTRY_HEAP_BYTES);
        this.entryMem = new DirectLongList(16 * 1024, MemoryTag.NATIVE_DEFAULT, true);
        this.isOpen = true;
    }

    /**
     * Whether the sort's rows qualify: a single sort key of a type its encoding gives back (FLOAT, DOUBLE,
     * INT, LONG, DATE, TIMESTAMP), and besides it at most one column, of a fixed size of at most 8 bytes and
     * not a SYMBOL.
     */
    static boolean isEligible(RecordMetadata metadata, IntList sortColumnFilter) {
        final int n = metadata.getColumnCount();
        if (sortColumnFilter.size() != 1 || n > 2) {
            return false;
        }
        final int key = Math.abs(sortColumnFilter.getQuick(0)) - 1;
        switch (ColumnType.tagOf(metadata.getColumnType(key))) {
            case ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP:
                break;
            default:
                return false;
        }
        for (int c = 0; c < n; c++) {
            if (c != key && payloadSize(metadata.getColumnType(c)) == 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        if (isOpen) {
            isOpen = false;
            Misc.free(entryMem);
            Misc.free(encoder);
            scratch = Misc.free(scratch);
            blockKeys = Misc.free(blockKeys);
            baseCursor = Misc.free(baseCursor);
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return baseCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isSorted) {
            buildAndSort();
            isSorted = true;
        }
        if (currentAddr < endAddr) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            recordA.entryAddr = currentAddr;
            currentAddr += 16;
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return baseCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
        assert this.baseCursor == null;
        this.baseCursor = baseCursor;
        memoryTracker = executionContext.getMemoryTracker();
        entryMem.setMemoryTracker(memoryTracker);
        if (!isOpen) {
            isOpen = true;
            entryMem.reopen();
        }
        encoder.init(baseCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        isSorted = false;
        count = 0;
    }

    @Override
    public RecordBlock peekRecordBlock(int maxRows) {
        if (!isSorted) {
            buildAndSort();
            isSorted = true;
        }
        final long remaining = (endAddr - currentAddr) >> 4;
        if (remaining < 1 || maxRows < 1) {
            return null;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        final int n = (int) Math.min(Math.min(remaining, maxRows), SORTED_BLOCK_MAX_ROWS);
        if (blockKeys == null) {
            blockKeys = new DirectLongList(n, MemoryTag.NATIVE_DEFAULT, true);
            blockKeys.setMemoryTracker(memoryTracker);
            blockKeys.reopen();
        }
        if (block == null) {
            block = new SortedBlock();
        }
        blockKeys.clear();
        blockKeys.ensureCapacity(n);
        final long dst = blockKeys.getAddress();
        // the key column's values, decoded, in its type's layout
        switch (keyType) {
            case ColumnType.FLOAT:
                for (int r = 0; r < n; r++) {
                    Unsafe.putFloat(dst + ((long) r << 2), SortKeyEncoder.decodeFloatKey(Unsafe.getLong(currentAddr + ((long) r << 4)), isDesc));
                }
                break;
            case ColumnType.DOUBLE:
                for (int r = 0; r < n; r++) {
                    Unsafe.putDouble(dst + ((long) r << 3), SortKeyEncoder.decodeDoubleKey(Unsafe.getLong(currentAddr + ((long) r << 4)), isDesc));
                }
                break;
            case ColumnType.INT:
                for (int r = 0; r < n; r++) {
                    Unsafe.putInt(dst + ((long) r << 2), decodeIntKey(Unsafe.getLong(currentAddr + ((long) r << 4))));
                }
                break;
            default:
                for (int r = 0; r < n; r++) {
                    Unsafe.putLong(dst + ((long) r << 3), decodeLongKey(Unsafe.getLong(currentAddr + ((long) r << 4))));
                }
                break;
        }
        block.of(currentAddr, n);
        return block;
    }

    @Override
    public long preComputedStateSize() {
        return RecordCursor.fromBool(isSorted);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((EntryRecord) record).entryAddr = startAddr + (atRowId << 4);
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public void skipRecordBlock(int rowCount) {
        assert currentAddr + ((long) rowCount << 4) <= endAddr;
        currentAddr += (long) rowCount << 4;
    }

    @Override
    public boolean supportsRecordBlocks() {
        return true;
    }

    @Override
    public void toTop() {
        currentAddr = startAddr;
        endAddr = startAddr + (count << 4);
    }

    private static int payloadSize(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE -> 1;
            case ColumnType.SHORT, ColumnType.CHAR -> 2;
            case ColumnType.INT, ColumnType.FLOAT -> 4;
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.DOUBLE -> 8;
            default -> 0;
        };
    }

    // the payload column's values of a block's n rows into the entries' second words, as payloadAt() reads them
    private void copyPayloads(long dst, int n, long addr, long stride, long rows) {
        if (rows == 0 && payloadSize == 8) {
            for (int r = 0; r < n; r++) {
                Unsafe.putLong(dst + ((long) r << 4) + 8, Unsafe.getLong(addr + r * stride));
            }
            return;
        }
        for (int r = 0; r < n; r++) {
            final long i = rows != 0 ? Unsafe.getLong(rows + ((long) r << 3)) : r;
            Unsafe.putLong(dst + ((long) r << 4) + 8, payloadAt(addr + i * stride));
        }
    }

    // the key column's values of a block's n rows into the entries' first words, as encodeKey() encodes
    // them, one loop per key type
    private void encodeKeys(long dst, int n, long addr, long stride, long rows) {
        switch (keyType) {
            case ColumnType.FLOAT:
                for (int r = 0; r < n; r++) {
                    final long i = rows != 0 ? Unsafe.getLong(rows + ((long) r << 3)) : r;
                    Unsafe.putLong(dst + ((long) r << 4), SortKeyEncoder.floatKey(Unsafe.getFloat(addr + i * stride), isDesc));
                }
                break;
            case ColumnType.DOUBLE:
                for (int r = 0; r < n; r++) {
                    final long i = rows != 0 ? Unsafe.getLong(rows + ((long) r << 3)) : r;
                    Unsafe.putLong(dst + ((long) r << 4), SortKeyEncoder.doubleKey(Unsafe.getDouble(addr + i * stride), isDesc));
                }
                break;
            case ColumnType.INT: {
                final int xor = isDesc ? 0x7FFFFFFF : 0x80000000;
                for (int r = 0; r < n; r++) {
                    final long i = rows != 0 ? Unsafe.getLong(rows + ((long) r << 3)) : r;
                    Unsafe.putLong(dst + ((long) r << 4), Integer.toUnsignedLong(Unsafe.getInt(addr + i * stride) ^ xor) << 32);
                }
                break;
            }
            default: {
                final long xor = isDesc ? Long.MAX_VALUE : Long.MIN_VALUE;
                for (int r = 0; r < n; r++) {
                    final long i = rows != 0 ? Unsafe.getLong(rows + ((long) r << 3)) : r;
                    Unsafe.putLong(dst + ((long) r << 4), Unsafe.getLong(addr + i * stride) ^ xor);
                }
                break;
            }
        }
    }

    // a key column value of the base's record, as encodeFixed8() encodes it
    private long encodeKey(Record record) {
        return switch (keyType) {
            case ColumnType.FLOAT -> SortKeyEncoder.floatKey(record.getFloat(keyColumn), isDesc);
            case ColumnType.DOUBLE -> SortKeyEncoder.doubleKey(record.getDouble(keyColumn), isDesc);
            case ColumnType.INT -> Integer.toUnsignedLong(record.getInt(keyColumn) ^ (isDesc ? 0x7FFFFFFF : 0x80000000)) << 32;
            case ColumnType.DATE -> record.getDate(keyColumn) ^ (isDesc ? Long.MAX_VALUE : Long.MIN_VALUE);
            case ColumnType.TIMESTAMP -> record.getTimestamp(keyColumn) ^ (isDesc ? Long.MAX_VALUE : Long.MIN_VALUE);
            default -> record.getLong(keyColumn) ^ (isDesc ? Long.MAX_VALUE : Long.MIN_VALUE);
        };
    }

    // the payload column's value of the base's record, as its raw bits, zero-extended
    private long payload(Record record) {
        return switch (payloadType) {
            case ColumnType.BOOLEAN -> record.getBool(payloadColumn) ? 1 : 0;
            case ColumnType.BYTE -> record.getByte(payloadColumn) & 0xFFL;
            case ColumnType.SHORT -> record.getShort(payloadColumn) & 0xFFFFL;
            case ColumnType.CHAR -> record.getChar(payloadColumn);
            case ColumnType.INT -> Integer.toUnsignedLong(record.getInt(payloadColumn));
            case ColumnType.FLOAT -> Integer.toUnsignedLong(Float.floatToRawIntBits(record.getFloat(payloadColumn)));
            case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(payloadColumn));
            case ColumnType.DATE -> record.getDate(payloadColumn);
            case ColumnType.TIMESTAMP -> record.getTimestamp(payloadColumn);
            default -> record.getLong(payloadColumn);
        };
    }

    // the payload column's value at the address, in the column's layout, as payload() gives it
    private long payloadAt(long addr) {
        return switch (payloadSize) {
            case 1 -> Unsafe.getByte(addr) & 0xFFL;
            case 2 -> Unsafe.getShort(addr) & 0xFFFFL;
            case 4 -> Integer.toUnsignedLong(Unsafe.getInt(addr));
            default -> Unsafe.getLong(addr);
        };
    }

    private void buildAndSort() {
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        final long maxEntries = maxEntryMemBytes / 16;
        final long estimatedSize = baseCursor.size();
        if (estimatedSize > 0) {
            if (estimatedSize > maxEntries) {
                throwLimitOverflow();
            }
            entryMem.setCapacity(estimatedSize * 2);
        }
        if (estimatedSize <= 0 && lastCount > 0) {
            // a base that does not know its size: the previous execution's count, rather than growing to it
            entryMem.setCapacity(Math.min(lastCount, maxEntries) * 2);
        }
        entryMem.clear();
        count = 0;
        final Record record = baseCursor.getRecord();
        while (true) {
            final RecordBlock b = baseCursor.supportsRecordBlocks() ? baseCursor.peekRecordBlock(BUILD_BLOCK_ROWS) : null;
            if (b == null) {
                if (!baseCursor.hasNext()) {
                    break;
                }
                putEntry(encodeKey(record), payloadColumn >= 0 ? payload(record) : 0, maxEntries);
                continue;
            }
            final int n = b.getRowCount();
            final long keyAddr = b.getColumnAddress(keyColumn);
            final long payloadAddr = payloadColumn >= 0 ? b.getColumnAddress(payloadColumn) : -1;
            if (keyAddr == 0 || payloadAddr == 0) {
                // a column the block does not lay out: its rows one by one, as without blocks
                for (int r = 0; r < n && baseCursor.hasNext(); r++) {
                    putEntry(encodeKey(record), payloadColumn >= 0 ? payload(record) : 0, maxEntries);
                }
                continue;
            }
            if (count + n > maxEntries) {
                throwLimitOverflow();
            }
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
            entryMem.ensureCapacity((long) n * 2);
            final long dst = entryMem.getAppendAddress();
            encodeKeys(dst, n, keyAddr, b.getColumnStride(keyColumn), b.getColumnRowIndexesAddress(keyColumn));
            if (payloadColumn >= 0) {
                copyPayloads(dst, n, payloadAddr, b.getColumnStride(payloadColumn), b.getColumnRowIndexesAddress(payloadColumn));
            } else {
                for (int r = 0; r < n; r++) {
                    Unsafe.putLong(dst + ((long) r << 4) + 8, 0);
                }
            }
            entryMem.skip((long) n * 2);
            count += n;
            baseCursor.skipRecordBlock(n);
        }
        Misc.free(encoder);
        if (count > 1) {
            if (scratch == null) {
                scratch = new DirectLongList(count * 2, MemoryTag.NATIVE_DEFAULT, true);
            }
            scratch.setMemoryTracker(memoryTracker);
            scratch.reopen();
            try {
                scratch.ensureCapacity(count * 2);
                Vect.sortEncodedEntriesStable(entryMem.getAddress(), count, scratch.getAddress());
            } finally {
                scratch.close();
            }
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
        }
        lastCount = count;
        startAddr = entryMem.getAddress();
        toTop();
    }

    private int decodeIntKey(long key) {
        return (int) (key >>> 32) ^ (isDesc ? 0x7FFFFFFF : 0x80000000);
    }

    private long decodeLongKey(long key) {
        return key ^ (isDesc ? Long.MAX_VALUE : Long.MIN_VALUE);
    }

    private void putEntry(long key, long payload, long maxEntries) {
        if (count >= maxEntries) {
            throwLimitOverflow();
        }
        entryMem.ensureCapacity(2);
        entryMem.add(key);
        entryMem.add(payload);
        count++;
    }

    private void throwLimitOverflow() {
        throw LimitOverflowException.instance()
                .put("limit of ").put(maxEntryMemBytes)
                .put(" memory exceeded in EncodedSort (raise ")
                .put(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath())
                .put(')');
    }

    // a sorted entry as a row: the key column decoded, the other column's raw bits
    private class EntryRecord implements Record {
        long entryAddr;

        @Override
        public boolean getBool(int col) {
            return Unsafe.getByte(entryAddr + 8) != 0;
        }

        @Override
        public byte getByte(int col) {
            return Unsafe.getByte(entryAddr + 8);
        }

        @Override
        public char getChar(int col) {
            return Unsafe.getChar(entryAddr + 8);
        }

        @Override
        public long getDate(int col) {
            return getLong(col);
        }

        @Override
        public double getDouble(int col) {
            if (col == keyColumn) {
                return SortKeyEncoder.decodeDoubleKey(Unsafe.getLong(entryAddr), isDesc);
            }
            return Unsafe.getDouble(entryAddr + 8);
        }

        @Override
        public float getFloat(int col) {
            if (col == keyColumn) {
                return SortKeyEncoder.decodeFloatKey(Unsafe.getLong(entryAddr), isDesc);
            }
            return Unsafe.getFloat(entryAddr + 8);
        }

        @Override
        public int getInt(int col) {
            if (col == keyColumn) {
                return decodeIntKey(Unsafe.getLong(entryAddr));
            }
            return Unsafe.getInt(entryAddr + 8);
        }

        @Override
        public long getLong(int col) {
            if (col == keyColumn) {
                return decodeLongKey(Unsafe.getLong(entryAddr));
            }
            return Unsafe.getLong(entryAddr + 8);
        }

        @Override
        public long getRowId() {
            return (entryAddr - startAddr) >> 4;
        }

        @Override
        public short getShort(int col) {
            return Unsafe.getShort(entryAddr + 8);
        }

        @Override
        public long getTimestamp(int col) {
            return getLong(col);
        }
    }

    // the next sorted entries: the other column at the entries' second words, the key column decoded
    private class SortedBlock implements RecordBlock {
        private long firstAddr;
        private int rowCount;

        @Override
        public long getColumnAddress(int columnIndex) {
            return columnIndex == keyColumn ? blockKeys.getAddress() : firstAddr + 8;
        }

        @Override
        public long getColumnStride(int columnIndex) {
            if (columnIndex == keyColumn) {
                return keyType == ColumnType.FLOAT || keyType == ColumnType.INT ? 4 : 8;
            }
            return 16;
        }

        @Override
        public Record getRecordAt(int row) {
            recordA.entryAddr = firstAddr + ((long) row << 4);
            return recordA;
        }

        @Override
        public int getRowCount() {
            return rowCount;
        }

        void of(long firstAddr, int rowCount) {
            this.firstAddr = firstAddr;
            this.rowCount = rowCount;
        }
    }
}
