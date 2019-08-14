/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.std.BinarySequence;
import com.questdb.std.Mutable;
import com.questdb.std.Transient;
import com.questdb.std.Unsafe;

import java.io.Closeable;

public class RecordChain implements Closeable, RecordCursor, Mutable, RecordSinkSPI {

    private final long[] columnOffsets;
    private final VirtualMemory mem;
    private final RecordChainRecord record = new RecordChainRecord();
    private final long varOffset;
    private final long fixOffset;
    private final RecordSink recordSink;
    private long recordOffset;
    private long varAppendOffset = 0L;
    private long nextRecordOffset = -1L;
    private RecordCursor symbolTableResolver;

    public RecordChain(@Transient ColumnTypes columnTypes, RecordSink recordSink, long pageSize) {
        this.mem = new VirtualMemory(pageSize);
        this.recordSink = recordSink;
        int count = columnTypes.getColumnCount();
        long varOffset = 0L;
        long fixOffset = 0L;

        this.columnOffsets = new long[count];
        for (int i = 0; i < count; i++) {
            int type = columnTypes.getColumnType(i);

            switch (type) {
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    columnOffsets[i] = varOffset;
                    varOffset += 8;
                    break;
                default:
                    columnOffsets[i] = fixOffset;
                    fixOffset += ColumnType.sizeOf(type);
                    break;
            }
        }
        this.varOffset = varOffset;
        this.fixOffset = fixOffset;
    }

    private static long rowToDataOffset(long row) {
        return row + 8;
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
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        if (nextRecordOffset != -1) {
            long offset = nextRecordOffset;
            nextRecordOffset = mem.getLong(nextRecordOffset);
            record.of(rowToDataOffset(offset));
            return true;
        }
        return false;
    }

    @Override
    public Record newRecord() {
        return new RecordChainRecord();
    }

    @Override
    public void recordAt(Record record, long row) {
        ((RecordChainRecord) record).of(rowToDataOffset(row));
    }

    @Override
    public void recordAt(long row) {
        record.of(rowToDataOffset(row));
    }

    @Override
    public void toTop() {
        if (mem.getAppendOffset() == 0) {
            nextRecordOffset = -1L;
        } else {
            nextRecordOffset = 0L;
        }
    }

    public void of(long nextRecordOffset) {
        this.nextRecordOffset = nextRecordOffset;
    }

    public long put(Record record, long prevRecordOffset) {
        long offset = beginRecord(prevRecordOffset);
        recordSink.copy(record, this);
        return offset;
    }

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

    public void putInt(int value) {
        mem.putInt(value);
    }

    @Override
    public void putLong(long value) {
        mem.putLong(value);
    }

    @Override
    public void putShort(short value) {
        mem.putShort(value);
    }

    @Override
    public void putChar(char value) {
        mem.putChar(value);
    }

    public void putStr(CharSequence value) {
        if (value == null) {
            putNull();
        } else {
            mem.putLong(rowToDataOffset(recordOffset), varAppendOffset);
            recordOffset += 8;
            mem.putStr(varAppendOffset, value);
            varAppendOffset += value.length() * 2 + 4;
        }
    }

    public void putStr(CharSequence value, int lo, int hi) {
        final int len = hi - lo;
        mem.putLong(rowToDataOffset(recordOffset), varAppendOffset);
        recordOffset += 8;
        mem.putStr(varAppendOffset, value, lo, len);
        varAppendOffset += len * 2 + 4;
    }

    @Override
    public void putTimestamp(long value) {
        putLong(value);
    }

    public void setSymbolTableResolver(RecordCursor resolver) {
        this.symbolTableResolver = resolver;
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
        public long getRowId() {
            return baseOffset - 8;
        }

        @Override
        public short getShort(int col) {
            return mem.getShort(fixedWithColumnOffset(col));
        }

        @Override
        public char getChar(int col) {
            return mem.getChar(fixedWithColumnOffset(col));
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
            long offset = varWidthColumnOffset(col);
            if (offset == -1) {
                return TableUtils.NULL_LEN;
            }
            return mem.getInt(offset);
        }

        @Override
        public CharSequence getSym(int col) {
            return symbolTableResolver.getSymbolTable(col).value(getInt(col));
        }

        private long fixedWithColumnOffset(int index) {
            return fixedOffset + Unsafe.arrayGet(columnOffsets, index);
        }

        private void of(long offset) {
            this.baseOffset = offset;
            this.fixedOffset = offset + varOffset;
        }

        private long varWidthColumnOffset(int index) {
            return mem.getLong(baseOffset + Unsafe.arrayGet(columnOffsets, index));
        }
    }
}
