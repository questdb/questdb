/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.common.ColumnType;
import com.questdb.std.BinarySequence;
import com.questdb.std.Mutable;
import com.questdb.std.Unsafe;

import java.io.Closeable;

public class RecordChain implements Closeable, RecordCursor, Mutable {

    private final long columnOffsets[];
    private final VirtualMemory mem;
    private final int columnCount;
    private final RecordMetadata metadata;
    private final RecordChainRecord record = new RecordChainRecord();
    private final long varOffset;
    private final long fixOffset;
    private long recordOffset;
    private long varAppendOffset = 0L;
    private long nextRecordOffset = -1L;

    public RecordChain(RecordMetadata metadata, long pageSize) {
        this.mem = new VirtualMemory(pageSize);
        this.metadata = metadata;
        int count = metadata.getColumnCount();
        long varOffset = 0L;
        long fixOffset = 0L;

        this.columnOffsets = new long[count];
        for (int i = 0; i < count; i++) {
            int type = metadata.getColumnType(i);

            switch (type) {
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
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
        this.columnCount = count;
    }

    public long beginRecord(long prevOffset) {
        mem.jumpTo(varAppendOffset);
        // no next record
        mem.putLong(-1);
        recordOffset = varAppendOffset;
        if (prevOffset != -1) {
            mem.jumpTo(prevOffset);
            mem.putLong(recordOffset);
            mem.jumpTo(rowToDataOffset(recordOffset + varOffset));
        } else {
            mem.skip(varOffset);
        }
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

    //todo: not hit
    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new RecordChainRecord();
    }

    @Override
    public Record recordAt(long row) {
        return record.of(rowToDataOffset(row));
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
    public boolean hasNext() {
        return nextRecordOffset != -1;
    }

    @Override
    public Record next() {
        long offset = nextRecordOffset;
        nextRecordOffset = mem.getLong(nextRecordOffset);
        return record.of(rowToDataOffset(offset));
    }

    public void putBin(BinarySequence value) {
        long offset = mem.getAppendOffset();
        if (value == null) {
            putNull(offset);
        } else {
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

    public void putInt(int value) {
        mem.putInt(value);
    }

    public long putRecord(Record record, long prevRecordOffset) {
        long offset = beginRecord(prevRecordOffset);
        putRecord0(record);
        return offset;
    }

    public void putStr(CharSequence value) {
        long offset = mem.getAppendOffset();
        if (value == null) {
            putNull(offset);
        } else {
            mem.jumpTo(rowToDataOffset(recordOffset));
            mem.putLong(varAppendOffset);
            recordOffset += 8;
            mem.jumpTo(varAppendOffset);
            mem.putStr(value);
            varAppendOffset = mem.getAppendOffset();
            mem.jumpTo(offset);
        }
    }

    private static long rowToDataOffset(long row) {
        return row + 8;
    }

    private void putByte(byte value) {
        mem.putByte(value);
    }

    private void putDate(long date) {
        putLong(date);
    }

    private void putDouble(double value) {
        mem.putDouble(value);
    }

    private void putFloat(float value) {
        mem.putFloat(value);
    }

    private void putLong(long value) {
        mem.putLong(value);
    }

    private void putNull(long offset) {
        mem.jumpTo(rowToDataOffset(recordOffset));
        mem.putLong(TableUtils.NULL_LEN);
        recordOffset += 8;
        mem.jumpTo(offset);
    }

    private void putRecord0(Record record) {
        for (int i = 0; i < columnCount; i++) {
            switch (metadata.getColumnType(i)) {
                case ColumnType.BOOLEAN:
                    putBool(record.getBool(i));
                    break;
                case ColumnType.BYTE:
                    putByte(record.getByte(i));
                    break;
                case ColumnType.DOUBLE:
                    putDouble(record.getDouble(i));
                    break;
                case ColumnType.INT:
                    putInt(record.getInt(i));
                    break;
                case ColumnType.LONG:
                    putDate(record.getLong(i));
                    break;
                case ColumnType.DATE:
                    putLong(record.getDate(i));
                    break;
                case ColumnType.TIMESTAMP:
                    putLong(record.getTimestamp(i));
                    break;
                case ColumnType.SHORT:
                    putShort(record.getShort(i));
                    break;
                case ColumnType.SYMBOL:
                    putStr(record.getSym(i));
                    break;
                case ColumnType.FLOAT:
                    putFloat(record.getFloat(i));
                    break;
                case ColumnType.STRING:
                    putStr(record.getStr(i));
                    break;
                case ColumnType.BINARY:
                    putBin(record.getBin(i));
                    break;
                default:
                    throw CairoException.instance(0).put("Unsupported type: ").put(ColumnType.nameOf(metadata.getColumnType(i))).put(" [").put(metadata.getColumnType(i)).put(']');
            }
        }
    }

    private void putShort(short value) {
        mem.putShort(value);
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
        public int getStrLen(int col) {
            long offset = varWidthColumnOffset(col);
            if (offset == -1) {
                return TableUtils.NULL_LEN;
            }
            return mem.getInt(offset);
        }

        @Override
        public CharSequence getSym(int col) {
            return getStr(col);
        }

        private long fixedWithColumnOffset(int index) {
            return fixedOffset + Unsafe.arrayGet(columnOffsets, index);
        }

        private Record of(long offset) {
            this.baseOffset = offset;
            this.fixedOffset = offset + varOffset;
            return this;
        }

        private long varWidthColumnOffset(int index) {
            return mem.getLong(baseOffset + Unsafe.arrayGet(columnOffsets, index));
        }
    }
}
