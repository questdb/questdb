/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.AnalyticSPI;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

public class RecordChain implements Closeable, RecordCursor, Mutable, RecordSinkSPI, AnalyticSPI {

    private final long[] columnOffsets;
    private final MemoryARW mem;
    private final RecordChainRecord recordA = new RecordChainRecord();
    private final RecordChainRecord recordB = new RecordChainRecord();
    private final long varOffset;
    private final long fixOffset;
    private final RecordSink recordSink;
    private long recordOffset;
    private long varAppendOffset = 0L;
    private long nextRecordOffset = -1L;
    private RecordCursor symbolTableResolver;

    public RecordChain(@Transient ColumnTypes columnTypes, RecordSink recordSink, long pageSize, int maxPages) {
        this.mem = Vm.getARWInstance(pageSize, maxPages, MemoryTag.NATIVE_RECORD_CHAIN);
        this.recordSink = recordSink;
        int count = columnTypes.getColumnCount();
        long varOffset = 0L;
        long fixOffset = 0L;

        this.columnOffsets = new long[count];
        for (int i = 0; i < count; i++) {
            int type = columnTypes.getColumnType(i);
            if (ColumnType.isVariableLength(type)) {
                columnOffsets[i] = varOffset;
                varOffset += 8;
            } else {
                columnOffsets[i] = fixOffset;
                fixOffset += ColumnType.sizeOf(type);
            }
        }
        this.varOffset = varOffset;
        this.fixOffset = fixOffset;
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
    public void clear() {
        close();
    }

    @Override
    public void close() {
        mem.close();
        nextRecordOffset = -1L;
        varAppendOffset = 0L;
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
    public boolean hasNext() {
        if (nextRecordOffset != -1) {
            final long offset = nextRecordOffset;
            nextRecordOffset = mem.getLong(nextRecordOffset);
            recordA.of(rowToDataOffset(offset));
            return true;
        }
        return false;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long row) {
        ((RecordChainRecord) record).of(rowToDataOffset(row));
    }

    @Override
    public void toTop() {
        if (mem.getAppendOffset() == 0) {
            nextRecordOffset = -1L;
        } else {
            nextRecordOffset = 0L;
        }
    }

    @Override
    public long size() {
        return -1;
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
            mem.jumpTo(rowToDataOffset(recordOffset));
            mem.putLong(varAppendOffset);
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
    public void putInt(int value) {
        mem.putInt(value);
    }

    @Override
    public void putLong(long value) {
        mem.putLong(value);
    }

    @Override
    public void putLong256(Long256 value) {
        mem.putLong256(value);
    }

    @Override
    public void putShort(short value) {
        mem.putShort(value);
    }

    @Override
    public void putChar(char value) {
        mem.putChar(value);
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
    public void putRecord(Record value) {
        // noop
    }

    @Override
    public void putTimestamp(long value) {
        putLong(value);
    }

    @Override
    public void skip(int bytes) {
        mem.skip(bytes);
    }

    public void setSymbolTableResolver(RecordCursor resolver) {
        this.symbolTableResolver = resolver;
    }

    private static long rowToDataOffset(long row) {
        return row + 8;
    }

    private void putNull() {
        mem.putLong(rowToDataOffset(recordOffset), TableUtils.NULL_LEN);
        recordOffset += 8;
    }

    private class RecordChainRecord implements Record {
        long fixedOffset;
        long baseOffset;

        @Override
        public BinarySequence getBin(int col) {
            long offset = varWidthColumnOffset(col);
            return offset == -1 ? null : mem.getBin(offset);
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
        public int getInt(int col) {
            return mem.getInt(fixedWithColumnOffset(col));
        }

        @Override
        public long getLong(int col) {
            return mem.getLong(fixedWithColumnOffset(col));
        }

        @Override
        public void getLong256(int col, CharSink sink) {
            mem.getLong256(fixedWithColumnOffset(col), sink);
        }

        @Override
        public Long256 getLong256A(int col) {
            return mem.getLong256A(fixedWithColumnOffset(col));
        }

        @Override
        public Long256 getLong256B(int col) {
            return mem.getLong256B(fixedWithColumnOffset(col));
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
        public CharSequence getStr(int col) {
            long offset = varWidthColumnOffset(col);
            return offset == -1 ? null : mem.getStr(offset);
        }

        @Override
        public CharSequence getStrB(int col) {
            long offset = varWidthColumnOffset(col);
            return offset == -1 ? null : mem.getStr2(offset);
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
        public CharSequence getSym(int col) {
            return symbolTableResolver.getSymbolTable(col).valueOf(getInt(col));
        }

        @Override
        public CharSequence getSymB(int col) {
            return symbolTableResolver.getSymbolTable(col).valueBOf(getInt(col));
        }

        @Override
        public byte getGeoByte(int col) {
            // No column tops, return byte from mem.
            return mem.getByte(fixedWithColumnOffset(col));
        }

        @Override
        public short getGeoShort(int col) {
            // No column tops, return short from mem.
            return mem.getShort(fixedWithColumnOffset(col));
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

        private long fixedWithColumnOffset(int index) {
            return fixedOffset + columnOffsets[index];
        }

        private void of(long offset) {
            this.baseOffset = offset;
            this.fixedOffset = offset + varOffset;
        }

        private long varWidthColumnOffset(int index) {
            return mem.getLong(baseOffset + columnOffsets[index]);
        }
    }
}
