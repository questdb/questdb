/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ql.impl.join.hash;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.CharSink;
import com.questdb.misc.Chars;
import com.questdb.misc.Unsafe;
import com.questdb.ql.AbstractRecord;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.std.DirectCharSequence;
import com.questdb.std.DirectInputStream;
import com.questdb.store.ColumnType;
import com.questdb.store.MemoryPages;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class MemoryRecordAccessor extends AbstractRecord {
    private final DirectCharSequence charSequence = new DirectCharSequence();
    private final MemoryPages mem;
    private final int headerSize;
    private final int[] offsets;
    private final int fixedBlockLen;
    private int fixedSize;
    private long address;
    private StorageFacade storageFacade;

    public MemoryRecordAccessor(RecordMetadata metadata, MemoryPages mem) {
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

    public void of(long offset) {
        this.address = mem.addressOf(offset) + headerSize;
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }

    private long addressOf(int index) {
        return mem.addressOf(offsetOf(index));
    }

    int getFixedBlockLength() {
        return fixedBlockLen;
    }

    private long offsetOf(int index) {
        // Not fixed len.
        assert offsets[index] <= 0;
        return Unsafe.getUnsafe().getLong(address - headerSize + (-offsets[index]) * 8);
    }

    private void writeBin(OutputStream stream, long offset, final long len) throws IOException {
        long position = offset;
        long copied = 0;
        long l = len;
        while (copied < l) {
            long blockEndOffset = mem.pageRemaining(offset);
            long copyEndOffset = Math.min(blockEndOffset, l - copied);
            l += copyEndOffset - position;
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

    private void writeNullString(long headerAddress) {
        long offset = mem.allocate(4);
        Unsafe.getUnsafe().putLong(headerAddress, offset);
        Unsafe.getUnsafe().putInt(mem.addressOf(offset), -1);
    }

    private void writeString(long headerAddress, CharSequence item) {

        if (item == null) {
            writeNullString(headerAddress);
            return;
        }

        // Allocate.
        final int k = item.length();
        if (k > mem.pageSize()) {
            throw new JournalRuntimeException("String larger than pageSize");
        }
        long offset = mem.allocate(k * 2 + 4);
        // Save the address in the header.
        Unsafe.getUnsafe().putLong(headerAddress, offset);
        Chars.put(mem.addressOf(offset), item);
    }


}
