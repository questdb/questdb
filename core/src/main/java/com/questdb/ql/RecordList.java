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

package com.questdb.ql;

import com.questdb.std.*;
import com.questdb.store.*;

import java.io.Closeable;

public class RecordList implements Closeable, RecordCursor, Mutable {
    private final MemoryPages mem;
    private final RecordListRecord record;
    private final RecordMetadata metadata;
    private final int columnCount;
    private final int headerSize;
    private final int recordPrefix;
    private long readAddress = -1;
    private StorageFacade storageFacade;
    private long hPtr;
    private long wPtr;

    public RecordList(RecordMetadata recordMetadata, int pageSize) {
        this.metadata = recordMetadata;
        this.mem = new MemoryPages(pageSize);
        this.record = new RecordListRecord(recordMetadata, mem);
        this.headerSize = record.getHeaderSize();
        this.columnCount = recordMetadata.getColumnCount();
        this.recordPrefix = headerSize + record.getFixedSize();
    }

    public long append(Record record, long prevAddress) {
        long thisAddress = beginRecord0(prevAddress);
        append0(record, thisAddress);
        return thisAddress;
    }

    public void appendBool(boolean value) {
        appendByte((byte) (value ? 1 : 0));
    }

    public void appendByte(byte value) {
        Unsafe.getUnsafe().putByte(wPtr, value);
        wPtr += 1;
    }

    public void appendDouble(double value) {
        Unsafe.getUnsafe().putDouble(wPtr, value);
        wPtr += 8;
    }

    public void appendFloat(float value) {
        Unsafe.getUnsafe().putFloat(wPtr, value);
        wPtr += 4;
    }

    public void appendInt(int value) {
        Unsafe.getUnsafe().putInt(wPtr, value);
        wPtr += 4;
    }

    public void appendLong(long value) {
        Unsafe.getUnsafe().putLong(wPtr, value);
        wPtr += 8;
    }

    public void appendStr(CharSequence value) {
        writeString(hPtr, value);
        hPtr += 8;
    }

    public long beginRecord(long prevAddress) {
        this.hPtr = beginRecord0(prevAddress);
        this.wPtr = this.hPtr + headerSize;
        return this.hPtr;
    }

    public void clear() {
        mem.clear();
        readAddress = -1;
    }

    @Override
    public void close() {
        Misc.free(mem);
    }

    public Record getRecord() {
        return record;
    }

    public RecordListRecord newRecord() {
        RecordListRecord record = new RecordListRecord(metadata, mem);
        record.setStorageFacade(storageFacade);
        return record;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    public void setStorageFacade(StorageFacade storageFacade) {
        record.setStorageFacade(this.storageFacade = storageFacade);
    }

    @Override
    public Record recordAt(long rowId) {
        record.of(rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((RecordListRecord) record).of(atRowId);
    }

    @Override
    public void releaseCursor() {
        clear();
    }

    public void toTop() {
        if (mem.size() == 0L) {
            readAddress = -1L;
        } else {
            readAddress = mem.addressOf(0) + 8;
        }
    }

    @Override
    public boolean hasNext() {
        return readAddress > -1;
    }

    @Override
    public Record next() {
        record.of(readAddress);
        readAddress = Unsafe.getUnsafe().getLong(readAddress - 8);
        return record;
    }

    public void of(long offset) {
        this.readAddress = offset;
    }

    public void putShort(short value) {
        Unsafe.getUnsafe().putShort(wPtr, value);
        wPtr += 2;
    }

    private void append0(Record record, long address) {
        long headerAddress = address;
        long writeAddress = headerAddress + headerSize;

        for (int i = 0; i < columnCount; i++) {
            switch (metadata.getColumnQuick(i).getType()) {
                case ColumnType.BOOLEAN:
                    Unsafe.getUnsafe().putByte(writeAddress, (byte) (record.getBool(i) ? 1 : 0));
                    writeAddress += 1;
                    break;
                case ColumnType.BYTE:
                    Unsafe.getUnsafe().putByte(writeAddress, record.getByte(i));
                    writeAddress += 1;
                    break;
                case ColumnType.DOUBLE:
                    Unsafe.getUnsafe().putDouble(writeAddress, record.getDouble(i));
                    writeAddress += 8;
                    break;
                case ColumnType.INT:
                    Unsafe.getUnsafe().putInt(writeAddress, record.getInt(i));
                    writeAddress += 4;
                    break;
                case ColumnType.LONG:
                    Unsafe.getUnsafe().putLong(writeAddress, record.getLong(i));
                    writeAddress += 8;
                    break;
                case ColumnType.SHORT:
                    Unsafe.getUnsafe().putShort(writeAddress, record.getShort(i));
                    writeAddress += 2;
                    break;
                case ColumnType.SYMBOL:
                    Unsafe.getUnsafe().putInt(writeAddress, record.getInt(i));
                    writeAddress += 4;
                    break;
                case ColumnType.DATE:
                    Unsafe.getUnsafe().putLong(writeAddress, record.getDate(i));
                    writeAddress += 8;
                    break;
                case ColumnType.FLOAT:
                    Unsafe.getUnsafe().putFloat(writeAddress, record.getFloat(i));
                    writeAddress += 4;
                    break;
                case ColumnType.STRING:
                    writeString(headerAddress, record.getFlyweightStr(i));
                    headerAddress += 8;
                    break;
                case ColumnType.BINARY:
                    writeBin(headerAddress, record.getBin(i));
                    headerAddress += 8;
                    break;
                default:
                    throw new JournalRuntimeException("Unsupported type: " + metadata.getColumnQuick(i).getType());
            }
        }
    }

    private long beginRecord0(long prevAddress) {
        long thisAddress = mem.allocate(8 + recordPrefix) + 8;
        if (prevAddress != -1) {
            Unsafe.getUnsafe().putLong(prevAddress - 8, thisAddress);
        }
        Unsafe.getUnsafe().putLong(thisAddress - 8, -1L);
        return thisAddress;
    }

    private void writeBin(long headerAddress, DirectInputStream value) {

        long offset = mem.allocateOffset(8);
        long len = value == null ? -1L : value.size();

        // Write header offset.
        Unsafe.getUnsafe().putLong(headerAddress, offset);

        // Write length.
        Unsafe.getUnsafe().putLong(mem.addressOf(offset), len);

        if (len < 1) {
            return;
        }

        offset = mem.allocateOffset(1);
        long p = 0;
        do {
            int remaining = mem.pageRemaining(offset);
            int sz = remaining < len ? remaining : (int) len;
            value.copyTo(mem.addressOf(offset), p, sz);
            p += sz;
            mem.allocateOffset(sz);
            offset += sz;
            len -= sz;
        } while (len > 0);
    }

    private void writeNullString(long headerAddress) {
        long addr = mem.allocate(4);
        Unsafe.getUnsafe().putLong(headerAddress, addr);
        Unsafe.getUnsafe().putInt(addr, VariableColumn.NULL_LEN);
    }

    private void writeString(long headerAddress, CharSequence value) {
        if (value == null) {
            writeNullString(headerAddress);
            return;
        }

        // Allocate.
        final int l = value.length();
        if (l > mem.pageSize()) {
            throw new JournalRuntimeException("String larger than pageSize");
        }
        long addr = mem.allocate(l * 2 + 4);
        // Save the address in the header.
        Unsafe.getUnsafe().putLong(headerAddress, addr);
        Chars.strcpyw(value, addr);
    }
}
