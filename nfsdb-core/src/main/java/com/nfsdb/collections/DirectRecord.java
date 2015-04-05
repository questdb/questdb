package com.nfsdb.collections;

import com.nfsdb.column.DirectInputStream;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Unsafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class DirectRecord implements Record {
    private final RecordMetadata metadata;
    private DirectPagedBuffer buffer;
    private int headerSize;
    private int fixedSize;
    private int[] offsets;
    //private int[] colIndex;
    private SymbolTable[] symbolTables;
    private long address;
    private int fixedBlockLen;

    public DirectRecord(RecordMetadata metadata, DirectPagedBuffer buffer) {
        this.metadata = metadata;
        this.buffer = buffer;
        offsets = new int[metadata.getColumnCount()];
        //colIndex = new int[metadata.getColumnCount()];
        symbolTables = new SymbolTable[metadata.getColumnCount()];
        int lastOffset = 0;

        int lastIndex = 0;
        int varColIndex = 0;
        for(int i = 0; i < offsets.length; i++) {
            ColumnType ct = metadata.getColumnType(i);
            if (ct.size() != 0) {
                // Fixed columns.
                fixedSize += ct.size();
                //colIndex[i] = lastIndex++;
                offsets[i] = lastOffset;
                lastOffset += ct.size();
            }
        }

        // Init order of var len fields
        for(int i = 0; i < offsets.length; i++) {
            if (metadata.getColumnType(i).size() == 0) {
                //colIndex[i] = lastIndex++;
                offsets[i] = -(varColIndex++);
            }
        }
        // Pad header size to 8 bytes.
        fixedSize = ((fixedSize + 7) >> 3) << 3;
        headerSize = varColIndex * 8;
        fixedBlockLen = (int) (fixedSize + headerSize);
    }

    public DirectRecord init(long offset) {
        this.address = buffer.toAddress(offset) + headerSize;
        return this;
    }

    public int getFixedBlockLength() {
        return fixedBlockLen;
    }

    public long write(Record record) {
        // Append to the end.
        return write(record, buffer.getWriteOffsetQuick(headerSize + fixedSize));
    }

    public long write(Record record, long recordStartOffset) {
        long headerAddress = buffer.toAddress(recordStartOffset);
        long writeAddress = headerAddress + headerSize;

        for(int i = 0; i < offsets.length; i++) {
            //int i = colIndex[col];
            ColumnType columnType = metadata.getColumnType(i);
            switch (columnType) {
                case BOOLEAN:
                    Unsafe.getUnsafe().putByte(writeAddress, (byte) (record.getBool(i) ? 1 : 0));
                    writeAddress +=1;
                    break;
                case BYTE:
                    Unsafe.getUnsafe().putByte(writeAddress, record.get(i));
                    writeAddress +=1;
                    break;
                case DOUBLE:
                    Unsafe.getUnsafe().putDouble(writeAddress, record.getDouble(i));
                    writeAddress +=8;
                    break;
                case INT:
                    Unsafe.getUnsafe().putInt(writeAddress, record.getInt(i));
                    writeAddress +=4;
                    break;
                case LONG:
                    Unsafe.getUnsafe().putLong(writeAddress, record.getLong(i));
                    writeAddress +=8;
                    break;
                case SHORT:
                    Unsafe.getUnsafe().putShort(writeAddress, record.getShort(i));
                    writeAddress +=2;
                    break;
                case SYMBOL:
                    Unsafe.getUnsafe().putInt(writeAddress, record.getInt(i));
                    writeAddress +=4;
                    break;
                case DATE:
                    Unsafe.getUnsafe().putLong(writeAddress, record.getDate(i));
                    writeAddress +=8;
                    break;
                case STRING:
                    writeString(headerAddress, record.getStr(i));
                    headerAddress += 8;
                    break;
                case BINARY:
                    writeBinary(headerAddress, record.getBin(i));
                    headerAddress += 8;
                    break;
            }
        }
        return recordStartOffset;
    }

    private void writeString(long headerAddress, CharSequence item) {
        if (item != null) {
            // Allocate.
            long offset = buffer.getWriteOffsetWithChecks(item.length() * 2 + 4);
            long address = buffer.toAddress(offset);

            // Save the address in the header.
            Unsafe.getUnsafe().putLong(headerAddress, offset);

            // Write length at the beginning of the field data.
            Unsafe.getUnsafe().putInt(address, item.length());
            address += 2;

            // Write body.
            for (int j = 0; j < item.length(); j++) {
                Unsafe.getUnsafe().putChar(address += 2, item.charAt(j));
            }
        }
        else {
            long offset = buffer.getWriteOffsetQuick(4);
            Unsafe.getUnsafe().putLong(headerAddress, offset);
            Unsafe.getUnsafe().putInt(buffer.toAddress(offset), -1);
        }
    }

    private void writeBinary(long headerAddress, DirectInputStream value) {
        long initialOffset = buffer.getWriteOffsetQuick(8);
        long len = value.getLength();

        // Write header offset.
        Unsafe.getUnsafe().putLong(headerAddress, initialOffset);

        // Write length.
        Unsafe.getUnsafe().putLong(buffer.toAddress(initialOffset), len);

        if (len <= 0) {
            return;
        }

        buffer.append(value);
    }

    @Override
    public long getRowId() {
        return address - headerSize;
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
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getByte(address + offsets[col]);
    }

    @Override
    public void getBin(int col, OutputStream s) {
        final long readOffset = findOffset(col);
        final long readAddress = buffer.toAddress(readOffset);
        try {
            long len = Unsafe.getUnsafe().getLong(readAddress);
            if (len > 0) {
                buffer.read(s, readAddress + 8, Unsafe.getUnsafe().getLong(readAddress));
            }
        }
        catch (IOException ex) {
            throw new JournalRuntimeException("Reading binary column failed", ex);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        final long readOffset = findOffset(col);
        final long readAddress = buffer.toAddress(readOffset);
        final long len =  Unsafe.getUnsafe().getLong(readAddress);
        if (len < 0) return null;
        return new DirectPagedBufferStream(buffer, readOffset + 8, len);
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
        return get(col) != 0;
    }

    @Override
    public long getDate(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getLong(address + offsets[col]);
    }

    @Override
    public double getDouble(String column) {
        return getDouble(metadata.getColumnIndex(column));
    }

    @Override
    public double getDouble(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getDouble(address + offsets[col]);
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
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getInt(address + offsets[col]);
    }

    @Override
    public long getLong(String column) {
        return getLong(metadata.getColumnIndex(column));
    }

    @Override
    public long getLong(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getLong(address + offsets[col]);
    }

    @Override
    public short getShort(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getShort(address + offsets[col]);
    }

    @Override
    public CharSequence getStr(String column) {
        return getStr(metadata.getColumnIndex(column));
    }

    @Override
    public CharSequence getStr(int col) {
        long readAddress = findAddress(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        if (len < 0) return null;
        return new DirectCharSequence(readAddress + 4, len);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        long readAddress = findAddress(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        readAddress += 2;
        for(int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(readAddress += 2));
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

    private long findAddress(int index) {
        return buffer.toAddress(findOffset(index));
    }

    private long findOffset(int index) {
        // Not fixed len.
        assert offsets[index] <= 0;
        return Unsafe.getUnsafe().getLong(address - headerSize + (-offsets[index]) * 8);
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
            return Unsafe.getUnsafe().getChar(address + index * 2);
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
