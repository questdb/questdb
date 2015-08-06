/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.collections;

import com.nfsdb.collections.DirectCharSequence;
import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.impl.AbstractRecord;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.OutputStream;

public class DirectRecord extends AbstractRecord {
    private final DirectCharSequence charSequence = new DirectCharSequence();
    private final DirectPagedBuffer buffer;
    private final int headerSize;
    private final int[] offsets;
    private final int fixedBlockLen;
    private int fixedSize;
    private long address;

    public DirectRecord(RecordMetadata metadata, DirectPagedBuffer buffer) {
        super(metadata);
        this.buffer = buffer;
        offsets = new int[metadata.getColumnCount()];
        int lastOffset = 0;

        int varColIndex = 0;
        for (int i = 0; i < offsets.length; i++) {
            ColumnType ct = metadata.getColumn(i).getType();
            if (ct.size() != 0) {
                // Fixed columns.
                fixedSize += ct.size();
                offsets[i] = lastOffset;
                lastOffset += ct.size();
            }
        }

        // Init order of var len fields
        for (int i = 0; i < offsets.length; i++) {
            if (metadata.getColumn(i).getType().size() == 0) {
                offsets[i] = -(varColIndex++);
            }
        }
        // Pad header size to 8 bytes.
        fixedSize = ((fixedSize + 7) >> 3) << 3;
        headerSize = varColIndex * 8;
        fixedBlockLen = fixedSize + headerSize;
    }

    @Override
    public byte get(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getByte(address + offsets[col]);
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public void getBin(int col, OutputStream s) {
        final long readOffset = findOffset(col);
        final long readAddress = buffer.toAddress(readOffset);
        try {
            long len = Unsafe.getUnsafe().getLong(readAddress);
            if (len > 0) {
                buffer.write(s, readAddress + 8, Unsafe.getUnsafe().getLong(readAddress));
            }
        } catch (IOException ex) {
            throw new JournalRuntimeException("Reading binary column failed", ex);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        final long readOffset = findOffset(col);
        final long readAddress = buffer.toAddress(readOffset);
        final long len = Unsafe.getUnsafe().getLong(readAddress);
        if (len < 0) return null;
        return new DirectPagedBufferStream(buffer, readOffset + 8, len);
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
    public double getDouble(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getDouble(address + offsets[col]);
    }

    @Override
    public float getFloat(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getFloat(address + offsets[col]);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        long readAddress = findAddress(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        if (len < 0) return null;
        return charSequence.init(readAddress + 4, readAddress + 4 + len * 2);
    }

    @Override
    public int getInt(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getInt(address + offsets[col]);
    }

    @Override
    public long getLong(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getLong(address + offsets[col]);
    }

    @Override
    public long getRowId() {
        return address - headerSize;
    }

    @Override
    public short getShort(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getShort(address + offsets[col]);
    }

    @Override
    public CharSequence getStr(int col) {
        long readAddress = findAddress(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        if (len < 0) return null;
        return new DirectCharSequence().init(readAddress + 4, readAddress + 4 + len * 2);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        long readAddress = findAddress(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        readAddress += 2;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(readAddress += 2));
        }
    }

    @Override
    public String getSym(int col) {
        return getStr(col).toString();
    }

    public int getFixedBlockLength() {
        return fixedBlockLen;
    }

    public void init(long offset) {
        this.address = buffer.toAddress(offset) + headerSize;
    }

    public long write(Record record) {
        // Append to the end.
        return write(record, buffer.getWriteOffsetQuick(headerSize + fixedSize));
    }

    public long write(Record record, long recordStartOffset) {
        long headerAddress = buffer.toAddress(recordStartOffset);
        long writeAddress = headerAddress + headerSize;

        for (int i = 0; i < offsets.length; i++) {
            ColumnType columnType = metadata.getColumn(i).getType();
            switch (columnType) {
                case BOOLEAN:
                    Unsafe.getUnsafe().putByte(writeAddress, (byte) (record.getBool(i) ? 1 : 0));
                    writeAddress += 1;
                    break;
                case BYTE:
                    Unsafe.getUnsafe().putByte(writeAddress, record.get(i));
                    writeAddress += 1;
                    break;
                case DOUBLE:
                    Unsafe.getUnsafe().putDouble(writeAddress, record.getDouble(i));
                    writeAddress += 8;
                    break;
                case INT:
                    Unsafe.getUnsafe().putInt(writeAddress, record.getInt(i));
                    writeAddress += 4;
                    break;
                case LONG:
                    Unsafe.getUnsafe().putLong(writeAddress, record.getLong(i));
                    writeAddress += 8;
                    break;
                case SHORT:
                    Unsafe.getUnsafe().putShort(writeAddress, record.getShort(i));
                    writeAddress += 2;
                    break;
                case SYMBOL:
                    Unsafe.getUnsafe().putInt(writeAddress, record.getInt(i));
                    writeAddress += 4;
                    break;
                case DATE:
                    Unsafe.getUnsafe().putLong(writeAddress, record.getDate(i));
                    writeAddress += 8;
                    break;
                case FLOAT:
                    Unsafe.getUnsafe().putFloat(writeAddress, record.getFloat(i));
                    writeAddress += 4;
                    break;
                case STRING:
                    writeString(headerAddress, record.getFlyweightStr(i));
                    headerAddress += 8;
                    break;
                case BINARY:
                    writeBinary(headerAddress, record.getBin(i));
                    headerAddress += 8;
                    break;
                default:
                    throw new JournalRuntimeException("Unsupported type: " + columnType);
            }
        }
        return recordStartOffset;
    }

    private long findAddress(int index) {
        return buffer.toAddress(findOffset(index));
    }

    private long findOffset(int index) {
        // Not fixed len.
        assert offsets[index] <= 0;
        return Unsafe.getUnsafe().getLong(address - headerSize + (-offsets[index]) * 8);
    }

    private void writeBinary(long headerAddress, DirectInputStream value) {
        long initialOffset = buffer.getWriteOffsetQuick(8);
        long len = value.getLength();

        // Write header offset.
        Unsafe.getUnsafe().putLong(headerAddress, initialOffset);

        // Write length.
        Unsafe.getUnsafe().putLong(buffer.toAddress(initialOffset), len);

        if (len < 1) {
            return;
        }

        buffer.append(value);
    }

    private void writeString(long headerAddress, CharSequence item) {
        if (item != null) {
            // Allocate.
            int k = item.length();
            long offset = buffer.getWriteOffsetWithChecks(k * 2 + 4);
            long address = buffer.toAddress(offset);
            // Save the address in the header.
            Unsafe.getUnsafe().putLong(headerAddress, offset);
            // Write length at the beginning of the field data.
            Unsafe.getUnsafe().putInt(address, k);
            address += 2;

            // Write body.
            for (int i = 0; i < k; i++) {
                Unsafe.getUnsafe().putChar(address += 2, item.charAt(i));
            }
        } else {
            long offset = buffer.getWriteOffsetQuick(4);
            Unsafe.getUnsafe().putLong(headerAddress, offset);
            Unsafe.getUnsafe().putInt(buffer.toAddress(offset), -1);
        }
    }
}
