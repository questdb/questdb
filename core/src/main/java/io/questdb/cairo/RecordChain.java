/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.BinarySequence;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class RecordChain implements Closeable, RecordCursor, Mutable, RecordSinkSPI, WindowSPI, Reopenable {
    private final int columnCount;
    private final long[] columnOffsets;
    private final long fixOffset;
    private final MemoryCARW mem;
    private final RecordChainRecord recordA;
    private final RecordChainRecord recordB;
    private final RecordSink recordSink;
    private final long varOffset;
    private long nextRecordOffset = -1L;
    private RecordChainRecord recordC;
    private long recordOffset;
    private SymbolTableSource symbolTableResolver;
    private long varAppendOffset = 0L;

    public RecordChain(
            @Transient @NotNull ColumnTypes columnTypes,
            @NotNull RecordSink recordSink,
            long pageSize,
            int maxPages
    ) {
        try {
            this.mem = Vm.getCARWInstance(pageSize, maxPages, MemoryTag.NATIVE_RECORD_CHAIN);
            this.recordSink = recordSink;
            this.columnCount = columnTypes.getColumnCount();
            this.recordA = new RecordChainRecord(columnCount);
            this.recordB = new RecordChainRecord(columnCount);
            long varOffset = 0L;
            long fixOffset = 0L;

            this.columnOffsets = new long[columnCount];
            for (int i = 0; i < columnCount; i++) {
                int type = columnTypes.getColumnType(i);
                if (ColumnType.isVarSize(type)) {
                    columnOffsets[i] = varOffset;
                    varOffset += 8;
                } else {
                    columnOffsets[i] = fixOffset;
                    fixOffset += ColumnType.sizeOf(type);
                }
            }
            this.varOffset = varOffset;
            this.fixOffset = fixOffset;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public long addressOf(long offset) {
        return mem.addressOf(offset);
    }

    public long beginRecord(long prevOffset) {
        // no next record
        mem.putLong(varAppendOffset, -1);
        recordOffset = varAppendOffset;
        if (prevOffset != -1) {
            mem.putLong(prevOffset, recordOffset);
        }
        mem.jumpTo(rowToDataOffset(recordOffset + varOffset));
        varAppendOffset = rowToDataOffset(recordOffset + varOffset + fixOffset);
        return recordOffset;
    }

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        long result = 0;
        while (nextRecordOffset != -1) {
            result++;
            nextRecordOffset = mem.getLong(nextRecordOffset);
        }

        counter.add(result);
    }

    @Override
    public void clear() {
        mem.close();
        nextRecordOffset = -1L;
        varAppendOffset = 0L;
    }

    @Override
    public void close() {
        clear();
        symbolTableResolver = null;
    }

    @Override
    public long getAddress(long recordOffset, int columnIndex) {
        return addressOf(getOffsetOfColumn(recordOffset, columnIndex));
    }

    public long getOffsetOfColumn(long recordOffset, int columnIndex) {
        return rowToDataOffset(recordOffset) + varOffset + columnOffsets[columnIndex];
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordAt(long recordOffset) {
        if (recordC == null) {
            recordC = new RecordChainRecord(columnCount);
        }
        recordC.of(rowToDataOffset(recordOffset));
        return recordC;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public boolean hasNext() {
        if (nextRecordOffset != -1) {
            final long offset = nextRecordOffset;
            nextRecordOffset = mem.getLong(nextRecordOffset);
            recordA.of(rowToDataOffset(offset));
            return true;
        }
        return false;
    }

    public void of(long nextRecordOffset) {
        this.nextRecordOffset = nextRecordOffset;
    }

    public long put(Record record, long prevRecordOffset) {
        long offset = beginRecord(prevRecordOffset);
        recordSink.copy(record, this);
        return offset;
    }

    @Override
    public void putBin(BinarySequence value) {
        if (value == null) {
            putNull();
        } else {
            long offset = mem.getAppendOffset();
            mem.putLong(rowToDataOffset(recordOffset), varAppendOffset);
            recordOffset += 8;
            mem.jumpTo(varAppendOffset);
            mem.putBin(value);
            varAppendOffset = mem.getAppendOffset();
            mem.jumpTo(offset);
        }
    }

    @Override
    public void putBool(boolean value) {
        mem.putBool(value);
    }

    @Override
    public void putByte(byte value) {
        mem.putByte(value);
    }

    @Override
    public void putChar(char value) {
        mem.putChar(value);
    }

    @Override
    public void putDate(long date) {
        putLong(date);
    }

    @Override
    public void putDouble(double value) {
        mem.putDouble(value);
    }

    @Override
    public void putFloat(float value) {
        mem.putFloat(value);
    }

    @Override
    public void putIPv4(int value) {
        putInt(value);
    }

    @Override
    public void putInt(int value) {
        mem.putInt(value);
    }

    @Override
    public void putInterval(Interval interval) {
        mem.putLong128(interval.getLo(), interval.getHi());
    }

    @Override
    public void putLong(long value) {
        mem.putLong(value);
    }

    @Override
    public void putLong128(long lo, long hi) {
        mem.putLong128(lo, hi);
    }

    @Override
    public void putLong256(Long256 value) {
        mem.putLong256(value);
    }

    @Override
    public void putLong256(long l0, long l1, long l2, long l3) {
        mem.putLong256(l0, l1, l2, l3);
    }

    @Override
    public void putRecord(Record value) {
        // no-op
    }

    @Override
    public void putShort(short value) {
        mem.putShort(value);
    }

    @Override
    public void putStr(CharSequence value) {
        if (value != null) {
            mem.putLong(rowToDataOffset(recordOffset), varAppendOffset);
            recordOffset += 8;
            mem.putStr(varAppendOffset, value);
            varAppendOffset += Vm.getStorageLength(value.length());
        } else {
            putNull();
        }
    }

    @Override
    public void putStr(CharSequence value, int lo, int hi) {
        final int len = hi - lo;
        mem.putLong(rowToDataOffset(recordOffset), varAppendOffset);
        recordOffset += 8;
        mem.putStr(varAppendOffset, value, lo, len);
        varAppendOffset += Vm.getStorageLength(len);
    }

    @Override
    public void putTimestamp(long value) {
        putLong(value);
    }

    @Override
    public void putVarchar(Utf8Sequence value) {
        if (value != null) {
            mem.putLong(rowToDataOffset(recordOffset), varAppendOffset);
            recordOffset += 8;
            // appendAddressFor grows the memory if necessary
            int byteCount = VarcharTypeDriver.getSingleMemValueByteCount(value);
            final long appendAddress = mem.appendAddressFor(varAppendOffset, byteCount);
            VarcharTypeDriver.appendPlainValue(appendAddress, value, false);
            varAppendOffset += byteCount;
        } else {
            putNull();
        }
    }

    @Override
    public void recordAt(Record record, long row) {
        ((RecordChainRecord) record).of(rowToDataOffset(row));
    }

    @Override
    public void reopen() {
        // nothing to do here
    }

    public void setSymbolTableResolver(SymbolTableSource resolver) {
        this.symbolTableResolver = resolver;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void skip(int bytes) {
        mem.skip(bytes);
    }

    @Override
    public void toTop() {
        if (mem.getAppendOffset() == 0) {
            nextRecordOffset = -1L;
        } else {
            nextRecordOffset = 0L;
        }
    }

    private static long rowToDataOffset(long row) {
        return row + 8;
    }

    private void putNull() {
        mem.putLong(rowToDataOffset(recordOffset), TableUtils.NULL_LEN);
        recordOffset += 8;
    }

    private class RecordChainRecord implements Record {
        private final ObjList<MemoryCR.ByteSequenceView> bsViews;
        private final ObjList<DirectString> csViewsA;
        private final ObjList<DirectString> csViewsB;
        private final ObjList<Interval> intervals;
        private final ObjList<Long256Impl> longs256A;
        private final ObjList<Long256Impl> longs256B;
        private final ObjList<DirectUtf8String> utf8ViewsA;
        private final ObjList<DirectUtf8String> utf8ViewsB;
        private long baseOffset;
        private long fixedOffset;

        public RecordChainRecord(int columnCount) {
            this.bsViews = new ObjList<>(columnCount);
            this.csViewsA = new ObjList<>(columnCount);
            this.csViewsB = new ObjList<>(columnCount);
            this.intervals = new ObjList<>(columnCount);
            this.longs256A = new ObjList<>(columnCount);
            this.longs256B = new ObjList<>(columnCount);
            this.utf8ViewsA = new ObjList<>(columnCount);
            this.utf8ViewsB = new ObjList<>(columnCount);
        }

        @Override
        public BinarySequence getBin(int col) {
            long offset = varWidthColumnOffset(col);
            return offset == -1 ? null : mem.getBin(offset, bsView(col));
        }

        @Override
        public long getBinLen(int col) {
            long offset = varWidthColumnOffset(col);
            return offset == -1 ? TableUtils.NULL_LEN : mem.getLong(offset);
        }

        @Override
        public boolean getBool(int col) {
            return mem.getBool(fixedWithColumnOffset(col));
        }

        @Override
        public byte getByte(int col) {
            return mem.getByte(fixedWithColumnOffset(col));
        }

        @Override
        public char getChar(int col) {
            return mem.getChar(fixedWithColumnOffset(col));
        }

        @Override
        public double getDouble(int col) {
            return mem.getDouble(fixedWithColumnOffset(col));
        }

        @Override
        public float getFloat(int col) {
            return mem.getFloat(fixedWithColumnOffset(col));
        }

        @Override
        public byte getGeoByte(int col) {
            // No column tops, return byte from mem.
            return mem.getByte(fixedWithColumnOffset(col));
        }

        @Override
        public int getGeoInt(int col) {
            // No column tops, return int from mem.
            return mem.getInt(fixedWithColumnOffset(col));
        }

        @Override
        public long getGeoLong(int col) {
            // No column tops, return long from mem.
            return mem.getLong(fixedWithColumnOffset(col));
        }

        @Override
        public short getGeoShort(int col) {
            // No column tops, return short from mem.
            return mem.getShort(fixedWithColumnOffset(col));
        }

        @Override
        public int getIPv4(int col) {
            return mem.getIPv4(fixedWithColumnOffset(col));
        }

        @Override
        public int getInt(int col) {
            return mem.getInt(fixedWithColumnOffset(col));
        }

        @Override
        public Interval getInterval(int col) {
            final long offset = fixedWithColumnOffset(col);
            return interval(col).of(mem.getLong(offset), mem.getLong(offset + Long.BYTES));
        }

        @Override
        public long getLong(int col) {
            return mem.getLong(fixedWithColumnOffset(col));
        }

        @Override
        public long getLong128Hi(int col) {
            return mem.getLong(fixedWithColumnOffset(col) + Long.BYTES);
        }

        @Override
        public long getLong128Lo(int col) {
            return mem.getLong(fixedWithColumnOffset(col));
        }

        @Override
        public void getLong256(int col, CharSink<?> sink) {
            mem.getLong256(fixedWithColumnOffset(col), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            Long256Impl long256 = long256A(col);
            mem.getLong256(fixedWithColumnOffset(col), long256);
            return long256;
        }

        @Override
        public Long256 getLong256B(int col) {
            Long256Impl long256 = long256B(col);
            mem.getLong256(fixedWithColumnOffset(col), long256);
            return long256;
        }

        @Override
        public long getLongIPv4(int col) {
            return Numbers.ipv4ToLong(mem.getIPv4(fixedWithColumnOffset(col)));
        }

        @Override
        public long getRowId() {
            return baseOffset - 8;
        }

        @Override
        public short getShort(int col) {
            return mem.getShort(fixedWithColumnOffset(col));
        }

        @Override
        public CharSequence getStrA(int col) {
            long offset = varWidthColumnOffset(col);
            assert offset > -2;
            return offset == -1 ? null : mem.getStr(offset, csViewA(col));
        }

        @Override
        public CharSequence getStrB(int col) {
            long offset = varWidthColumnOffset(col);
            assert offset > -2;
            return offset == -1 ? null : mem.getStr(offset, csViewB(col));
        }

        @Override
        public int getStrLen(int col) {
            final long offset = varWidthColumnOffset(col);
            if (offset > -1) {
                return mem.getInt(offset);
            }
            return TableUtils.NULL_LEN;
        }

        @Override
        public CharSequence getSymA(int col) {
            return symbolTableResolver.getSymbolTable(col).valueOf(getInt(col));
        }

        @Override
        public CharSequence getSymB(int col) {
            return symbolTableResolver.getSymbolTable(col).valueBOf(getInt(col));
        }

        @Override
        public Utf8Sequence getVarcharA(int col) {
            long offset = varWidthColumnOffset(col);
            if (offset == -1) {
                return null;
            }
            long addr = mem.addressOf(offset);
            return VarcharTypeDriver.getPlainValue(addr, utf8ViewA(col));
        }

        @Override
        public Utf8Sequence getVarcharB(int col) {
            long offset = varWidthColumnOffset(col);
            if (offset == -1) {
                return null;
            }
            long addr = mem.addressOf(offset);
            return VarcharTypeDriver.getPlainValue(addr, utf8ViewB(col));
        }

        @Override
        public int getVarcharSize(int col) {
            final long offset = varWidthColumnOffset(col);
            if (offset > -1) {
                return VarcharTypeDriver.getPlainValueSize(mem, offset);
            }
            return TableUtils.NULL_LEN;
        }

        private MemoryCR.ByteSequenceView bsView(int columnIndex) {
            if (bsViews.getQuiet(columnIndex) == null) {
                bsViews.extendAndSet(columnIndex, new MemoryCR.ByteSequenceView());
            }
            return bsViews.getQuick(columnIndex);
        }

        private DirectString csViewA(int columnIndex) {
            if (csViewsA.getQuiet(columnIndex) == null) {
                csViewsA.extendAndSet(columnIndex, new DirectString());
            }
            return csViewsA.getQuick(columnIndex);
        }

        private DirectString csViewB(int columnIndex) {
            if (csViewsB.getQuiet(columnIndex) == null) {
                csViewsB.extendAndSet(columnIndex, new DirectString());
            }
            return csViewsB.getQuick(columnIndex);
        }

        private long fixedWithColumnOffset(int index) {
            return fixedOffset + columnOffsets[index];
        }

        private Interval interval(int columnIndex) {
            if (intervals.getQuiet(columnIndex) == null) {
                intervals.extendAndSet(columnIndex, new Interval());
            }
            return intervals.getQuick(columnIndex);
        }

        private Long256Impl long256A(int columnIndex) {
            if (longs256A.getQuiet(columnIndex) == null) {
                longs256A.extendAndSet(columnIndex, new Long256Impl());
            }
            return longs256A.getQuick(columnIndex);
        }

        private Long256Impl long256B(int columnIndex) {
            if (longs256B.getQuiet(columnIndex) == null) {
                longs256B.extendAndSet(columnIndex, new Long256Impl());
            }
            return longs256B.getQuick(columnIndex);
        }

        private void of(long offset) {
            this.baseOffset = offset;
            this.fixedOffset = offset + varOffset;
        }

        private DirectUtf8String utf8ViewA(int columnIndex) {
            if (utf8ViewsA.getQuiet(columnIndex) == null) {
                utf8ViewsA.extendAndSet(columnIndex, new DirectUtf8String());
            }
            return utf8ViewsA.getQuick(columnIndex);
        }

        private DirectUtf8String utf8ViewB(int columnIndex) {
            if (utf8ViewsB.getQuiet(columnIndex) == null) {
                utf8ViewsB.extendAndSet(columnIndex, new DirectUtf8String());
            }
            return utf8ViewsB.getQuick(columnIndex);
        }

        private long varWidthColumnOffset(int index) {
            return mem.getLong(baseOffset + columnOffsets[index]);
        }
    }
}
