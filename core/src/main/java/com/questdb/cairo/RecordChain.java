package com.questdb.cairo;

import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.RecordListRecord;
import com.questdb.std.DirectInputStream;
import com.questdb.std.Mutable;
import com.questdb.store.ColumnType;

import java.io.Closeable;
import java.io.OutputStream;

public class RecordChain implements Closeable, RecordCursor, Mutable {

    private final long columnOffsets[];
    private final VirtualMemory mem;
    private final int columnCount;
    private final RecordMetadata metadata;
    private final RecordChainRecord record = new RecordChainRecord();
    private long recordOffset;
    private long varOffset;
    private long fixOffset;
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
            int type = metadata.getColumnQuick(i).getType();

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
            mem.jumpTo(recordOffset + varOffset + 8);
        } else {
            mem.skip(varOffset);
        }
        varAppendOffset = recordOffset + varOffset + fixOffset + 8;
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
    public Record newRecord() {
        return new RecordChainRecord();
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public Record recordAt(long rowId) {
        return record.of(rowId);
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((RecordListRecord) record).of(atRowId);
    }

    @Override
    public void releaseCursor() {
        close();
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
        return record.of(offset + 8);
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
            mem.jumpTo(recordOffset + 8);
            mem.putLong(-1);
            recordOffset += 8;
            mem.jumpTo(offset);
        } else {
            mem.jumpTo(recordOffset + 8);
            mem.putLong(varAppendOffset);
            recordOffset += 8;
            mem.jumpTo(varAppendOffset);
            mem.putStr(value);
            varAppendOffset = mem.getAppendOffset();
            mem.jumpTo(offset);
        }
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

    private void putRecord0(Record record) {
        for (int i = 0; i < columnCount; i++) {
            switch (metadata.getColumnQuick(i).getType()) {
                case ColumnType.BOOLEAN:
                    putBool(record.getBool(i));
                    break;
                case ColumnType.BYTE:
                    putByte(record.get(i));
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
                    putStr(record.getFlyweightStr(i));
                    break;
                case ColumnType.BINARY:
                    // todo
                    break;
                default:
                    throw CairoException.instance(0).put("Record chain does not support: ").put(ColumnType.nameOf(metadata.getColumnQuick(i).getType()));
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
        public byte get(int col) {
            return mem.getByte(fixedWithColumnOffset(col));
        }

        @Override
        public void getBin(int col, OutputStream s) {
            // todo
        }

        @Override
        public DirectInputStream getBin(int col) {
            // todo
            return null;
        }

        @Override
        public long getBinLen(int col) {
            // todo
            return 0;
        }

        @Override
        public boolean getBool(int col) {
            return mem.getBool(fixedWithColumnOffset(col));
        }

        @Override
        public long getDate(int col) {
            return mem.getLong(fixedWithColumnOffset(col));
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
        public CharSequence getFlyweightStr(int col) {
            long offset = varWidthColumnOffset(col);
            return offset == -1 ? null : mem.getStr(offset);
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
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
            return baseOffset;
        }

        @Override
        public short getShort(int col) {
            return mem.getShort(fixedWithColumnOffset(col));
        }

        @Override
        public int getStrLen(int col) {
            long offset = varWidthColumnOffset(col);
            if (offset == -1) {
                return 0;
            }
            return mem.getInt(offset);
        }

        @Override
        public CharSequence getSym(int col) {
            return getFlyweightStr(col);
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
