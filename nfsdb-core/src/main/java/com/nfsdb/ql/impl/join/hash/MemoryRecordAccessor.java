/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ql.impl.join.hash;

import com.nfsdb.collections.DirectCharSequence;
import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.ql.AbstractRecord;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.SequentialMemory;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class MemoryRecordAccessor extends AbstractRecord {
    private final DirectCharSequence charSequence = new DirectCharSequence();
    private final SequentialMemory mem;
    private final int headerSize;
    private final int[] offsets;
    private final int fixedBlockLen;
    private int fixedSize;
    private long address;
    private StorageFacade storageFacade;

    public MemoryRecordAccessor(RecordMetadata metadata, SequentialMemory mem) {
        super(metadata);
        this.mem = mem;
        offsets = new int[metadata.getColumnCount()];

        int varColIndex = 0;
        for (int i = 0; i < offsets.length; i++) {
            ColumnType ct = metadata.getColumnQuick(i).getType();
            if (ct.size() != 0) {
                // Fixed columns.
                offsets[i] = fixedSize;
                fixedSize += ct.size();
            } else {
                offsets[i] = -(varColIndex++);
            }
        }

        // Pad header size to 8 bytes.
        fixedSize = ((fixedSize + 7) >> 3) << 3;
        headerSize = varColIndex * 8;
        fixedBlockLen = fixedSize + headerSize;
    }

    public long append(@NotNull Record record) {
        long offset = mem.allocate(headerSize + fixedSize);
        append(record, offset);
        return offset;
    }

    public void append(Record record, long offset) {
        long headerAddress = mem.addressOf(offset);
        long writeAddress = headerAddress + headerSize;

        for (int i = 0, n = offsets.length; i < n; i++) {
            switch (metadata.getColumnQuick(i).getType()) {
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
                    writeBin(headerAddress, record.getBin(i));
                    headerAddress += 8;
                    break;
                default:
                    throw new JournalRuntimeException("Unsupported type: " + metadata.getColumnQuick(i).getType());
            }
        }
    }

    @Override
    public byte get(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getUnsafe().getByte(address + offsets[col]);
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public void getBin(int col, OutputStream s) {
        final long readOffset = offsetOf(col);
        final long readAddress = mem.addressOf(readOffset);
        try {
            long len = Unsafe.getUnsafe().getLong(readAddress);
            if (len > 0) {
                writeBin(s, readAddress + 8, len);
            }
        } catch (IOException ex) {
            throw new JournalRuntimeException("Reading binary column failed", ex);
        }
    }

    @Override
    public DirectInputStream getBin(int col) {
        final long offset = offsetOf(col);
        final long len = Unsafe.getUnsafe().getLong(mem.addressOf(offset));
        if (len < 0) return null;
        return new DirectPagedBufferStream(mem, offset + 8, len);
    }

    @Override
    public long getBinLen(int col) {
        return Unsafe.getUnsafe().getLong(mem.addressOf(offsetOf(col)));
    }

    @Override
    public boolean getBool(int col) {
        assert offsets[col] >= 0;
        return Unsafe.getBool(address + offsets[col]);
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
        long readAddress = addressOf(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        if (len < 0) return null;
        return charSequence.of(readAddress + 4, readAddress + 4 + len * 2);
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
        long readAddress = addressOf(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        if (len < 0) return null;
        return new DirectCharSequence().of(readAddress + 4, readAddress + 4 + len * 2);
    }

    @Override
    public void getStr(int col, CharSink sink) {
        long readAddress = addressOf(col);
        final int len = Unsafe.getUnsafe().getInt(readAddress);
        readAddress += 2;
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(readAddress += 2));
        }
    }

    @Override
    public int getStrLen(int col) {
        return Unsafe.getUnsafe().getInt(addressOf(col));
    }

    @Override
    public String getSym(int col) {
        return storageFacade.getSymbolTable(col).value(getInt(col));
    }

    public int getFixedBlockLength() {
        return fixedBlockLen;
    }

    public void init(long offset) {
        this.address = mem.addressOf(offset) + headerSize;
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }

    private long addressOf(int index) {
        return mem.addressOf(offsetOf(index));
    }

    private long offsetOf(int index) {
        // Not fixed len.
        assert offsets[index] <= 0;
        return Unsafe.getUnsafe().getLong(address - headerSize + (-offsets[index]) * 8);
    }

    private void writeBin(OutputStream stream, long offset, long len) throws IOException {
        long position = offset;
        long copied = 0;
        while (copied < len) {
            long blockEndOffset = mem.pageRemaining(offset);
            long copyEndOffset = Math.min(blockEndOffset, len - copied);
            len += copyEndOffset - position;
            while (position < copyEndOffset) {
                stream.write(Unsafe.getUnsafe().getByte(mem.addressOf(offset) + position++));
            }
        }
    }

    private void writeBin(long headerAddress, DirectInputStream value) {
        long offset = mem.allocate(8);
        long len = value.size();

        // Write header offset.
        Unsafe.getUnsafe().putLong(headerAddress, offset);

        // Write length.
        Unsafe.getUnsafe().putLong(mem.addressOf(offset), len);

        if (len < 1) {
            return;
        }

        offset = mem.allocate(1);
        long p = 0;
        do {
            int remaining = mem.pageRemaining(offset);
            int sz = remaining < len ? remaining : (int) len;
            value.copyTo(mem.addressOf(offset), p, sz);
            p += sz;
            mem.allocate(sz);
            offset += sz;
            len -= sz;
        } while (len > 0);
    }

    private void writeString(long headerAddress, CharSequence item) {
        if (item != null) {
            // Allocate.
            int k = item.length();
            if (k > mem.pageSize()) {
                throw new JournalRuntimeException("String larger than pageSize");
            }
            long offset = mem.allocate(k * 2 + 4);
            // Save the address in the header.
            Unsafe.getUnsafe().putLong(headerAddress, offset);
            Chars.put(mem.addressOf(offset), item);
        } else {
            long offset = mem.allocate(4);
            Unsafe.getUnsafe().putLong(headerAddress, offset);
            Unsafe.getUnsafe().putInt(mem.addressOf(offset), -1);
        }
    }
}
