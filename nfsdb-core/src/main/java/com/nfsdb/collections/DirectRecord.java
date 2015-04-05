package com.nfsdb.collections;

import com.nfsdb.column.DirectInputStream;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Unsafe;
import org.jetbrains.annotations.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class DirectRecord implements Record, Closeable {
    private final RecordMetadata metadata;
    private DirectLinkedBuffer buffer;
    private long localBufferAddress;
    private int[] offsets;
    private int[] colIndex;
    private long address;

    public DirectRecord(RecordMetadata metadata, DirectLinkedBuffer buffer) {
        this.metadata = metadata;
        this.buffer = buffer;
        offsets = new int[metadata.getColumnCount()];
        colIndex = new int[metadata.getColumnCount()];
        int lastOffset = 0;
        int lastIndex = 0;

        // Fixed columns first.
        for(int i = 0; i < offsets.length; i++) {
            ColumnType ct = metadata.getColumnType(i);
            if (ct.size() != 0) {
                colIndex[lastIndex] = i;
                offsets[lastIndex++] = lastOffset;
                lastOffset += ct.size();
            }
        }

        int firstVarLenOffset = lastIndex;
        // Var len columns at the end.
        for(int i = 0; i < offsets.length; i++) {
            ColumnType ct = metadata.getColumnType(i);
            if (ct.size() == 0) {
                colIndex[lastIndex] = i;
                offsets[lastIndex++] = -firstVarLenOffset;
            }
        }
    }

    DirectRecord init(long offset) {
        this.address = offset;
        return this;
    }

    public long write(Record record) {
        long writeAddress = address;
        for(int i = 0; i < metadata.getColumnCount(); i++) {
            ColumnType columnType = metadata.getColumnType(i);
            switch (columnType){
                case BOOLEAN:
                    writeAddress += buffer.writeByte((byte) (record.getBool(i) ? 1 : 0), writeAddress);
                case BYTE:
                    writeAddress += buffer.writeByte(record.get(i), writeAddress);
                    break;
                case DOUBLE:
                    writeAddress += buffer.writeDouble(record.getDouble(i), writeAddress);
                    break;
                case INT:
                    writeAddress += buffer.writeInt(record.getInt(i), writeAddress);
                    break;
                case LONG:
                    writeAddress += buffer.writeLong(record.getLong(i), writeAddress);
                    break;
                case SHORT:
                    writeAddress += buffer.writeShort(record.getShort(i), writeAddress);
                    break;
                case STRING:
                    writeAddress += buffer.writeString(record.getStr(i), writeAddress);
                    break;
                case SYMBOL:
                    writeAddress += buffer.writeString(record.getStr(i), writeAddress);
                    break;
                case BINARY:
                    writeAddress += buffer.writeBinary(record.getBin(i), writeAddress);
                    break;
                case DATE:
                    writeAddress += buffer.writeLong(record.getDate(i), writeAddress);
                    break;
            }
        }
        return writeAddress - address;
    }

    @Override
    public long getRowId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public byte get(String column) {
        return get(metadata.getColumnIndex(column));
    }

    @Override
    public byte get(int col) {
        int offset = findOffset(col);
        return buffer.getByte(address + offset);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        int offset = findOffset(col);
        long end = address + buffer.readInt(address + offset);
        offset += address + 4;

        try {
            while(offset < end) {
                s.write(Unsafe.getUnsafe().getByte(offset++));
            }
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void getBin(String column, OutputStream s) {
        getBin(metadata.getColumnIndex(column), s);
    }

    @Override
    public boolean getBool(String column) {
        return getBool(metadata.getColumnIndex(column));
    }

    @Override
    public boolean getBool(int col) {
        return buffer.getByte(address + findOffset(col)) != 0;
    }

    @Override
    public long getDate(int col) {
        return buffer.readLong(address + findOffset(col));
    }

    @Override
    public double getDouble(String column) {
        return getDouble(metadata.getColumnIndex(column));
    }

    @Override
    public double getDouble(int col) {
        return buffer.readDouble(address + findOffset(col));
    }

    @Override
    public float getFloat(String column) {
        return getFloat(metadata.getColumnIndex(column));
    }

    @Override
    public float getFloat(int col) {
        return 0;
    }

    @Override
    public int getInt(String column) {
        return getInt(metadata.getColumnIndex(column));
    }

    @Override
    public int getInt(int col) {
        return buffer.readInt(address + findOffset(col));
    }

    @Override
    public long getLong(String column) {
        return getLong(metadata.getColumnIndex(column));
    }

    @Override
    public long getLong(int col) {
        return buffer.readLong(address + findOffset(col));
    }

    @Override
    public short getShort(int col) {
        return buffer.readShort(address + findOffset(col));
    }

    @Override
    public CharSequence getStr(String column) {
        return getStr(metadata.getColumnIndex(column));
    }

    @Override
    public CharSequence getStr(int col) {
        final int offset = findOffset(col);
        final int len = buffer.readInt(address + offset);
        return new DirectCharSequence(address + offset + 4, len);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        int offset = findOffset(col);
        final int len = buffer.readInt(address + offset);
        offset += address + 4 - 2;
        for(int i = 0; i < len; i++) {
            sink.put(buffer.readChar(offset += 2));
        }
    }

    @Override
    public String getSym(String column) {
        return getStr(metadata.getColumnIndex(column)).toString();
    }

    @Override
    public String getSym(int col) {
        return getStr(col).toString();
    }

    @Override
    public DirectInputStream getBin(String column) {
        return getBin(metadata.getColumnIndex(column));
    }

    @Override
    public DirectInputStream getBin(int col) {
        int offset = findOffset(col);
        int len =  buffer.readInt(address + offset);
        return new DirectAddressStream(address + offset + 4, len);
    }

    private int findOffset(int colIndex) {
        int offset = offsets[colIndex];
        if (offset >= 0) {
            return offset;
        }

        int lastFixedIndex = -offset;
        assert(lastFixedIndex < colIndex);
        offset = offsets[lastFixedIndex];
        assert(offset >=0);

        // Each var len column has Int32 length the beginning.
        for(int i = lastFixedIndex; i < colIndex; i++) {
            offset += buffer.readInt(address + offset);
        }
        return offset;
    }

    @Override
    public void close() throws IOException {
        free();
    }

    private void free() {
        if (localBufferAddress != 0) {
            Unsafe.getUnsafe().freeMemory(localBufferAddress);
            localBufferAddress = 0;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        free();
        super.finalize();
    }

    private class DirectCharSequence implements CharSequence {
        private final long address;
        private final int len;

        public DirectCharSequence(long address, int len) {
            this.address = address;
            this.len = len;
        }

        @Override
        public int length() {
            return len;
        }

        @Override
        public char charAt(int index) {
            return buffer.readChar(address + index * 2);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new DirectCharSequence(address + start, end - start);
        }

        @NotNull
        @Override
        public String toString() {
            char[] chars = new char[len];
            for(int i = 0; i < chars.length; i++) {
                chars[i] = charAt(i);
            }
            return new String(chars);
        }
    }
}
