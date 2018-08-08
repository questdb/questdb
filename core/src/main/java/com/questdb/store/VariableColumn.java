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

package com.questdb.store;

import com.questdb.std.Chars;
import com.questdb.std.DirectInputStream;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectBytes;
import com.questdb.std.str.DirectCharSequence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class VariableColumn extends AbstractColumn {
    public static final int NULL_LEN = -1;
    private final FixedColumn indexColumn;
    private final BinaryOutputStream binOut = new BinaryOutputStream();
    private final BinaryInputStream binIn = new BinaryInputStream();
    private final DirectCharSequence csA = new DirectCharSequence();
    private final DirectCharSequence csB = new DirectCharSequence();
    private char buffer[] = new char[32];
    private byte[] streamBuf;

    public VariableColumn(MemoryFile dataFile, MemoryFile indexFile) {
        super(dataFile);
        this.indexColumn = new FixedColumn(indexFile, 8);
    }

    @Override
    public void close() {
        indexColumn.close();
        super.close();
    }

    @Override
    public void commit() {
        if (binOut.offset != -1) {
            binOut.close();
        }
        super.commit();
        indexColumn.commit();
    }

    @Override
    public void compact() throws JournalException {
        super.compact();
        this.indexColumn.compact();
    }

    @Override
    public void force() {
        super.force();
        indexColumn.force();
    }

    @Override
    public long getOffset(long localRowID) {
        return indexColumn.getLong(localRowID);
    }

    @Override
    public void setSequentialAccess(boolean sequentialAccess) {
        super.setSequentialAccess(sequentialAccess);
        indexColumn.setSequentialAccess(sequentialAccess);
    }

    @Override
    public long size() {
        return indexColumn.size();
    }

    @Override
    public void truncate(long size) {

        if (size < 0) {
            size = 0;
        }

        if (size < size()) {
            preCommit(getOffset(size));
        }
        indexColumn.truncate(size);
    }

    public boolean cmpStr(long localRowID, CharSequence value) {

        if (value == null) {
            return isNull(localRowID);
        }

        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.addressOf(offset, 4));

        if (len != value.length()) {
            return false;
        }

        long address = mappedFile.addressOf(offset + 4, len * 2);
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getChar(address) != value.charAt(i)) {
                return false;
            }
            address += 2;
        }

        return true;
    }

    public void getBin(long localRowID, ByteBuffer target) {
        long offset = getOffset(localRowID) + 4; // skip size

        while (target.hasRemaining()) {
            long address = mappedFile.addressOf(offset, 1);
            int len = mappedFile.pageRemaining(offset);
            int min = len < target.remaining() ? len : target.remaining();

            for (int i = 0; i < min; i++) {
                target.put(Unsafe.getUnsafe().getByte(address++));
            }
            offset += min;
        }
    }

    public void getBin(long localRowID, OutputStream s) {
        getBin(localRowID, s, getBinLen(localRowID));
    }

    public DirectInputStream getBin(long localRowID) {
        binIn.reset(getOffset(localRowID));
        return binIn.isNull() ? null : binIn;
    }

    public int getBinLen(long localRowID) {
        return Unsafe.getUnsafe().getInt(mappedFile.addressOf(getOffset(localRowID), 4));
    }

    public CharSequence getFlyweightStr(long localRowID) {
        return getFlyweightStr0(localRowID, csA);
    }

    public CharSequence getFlyweightStrB(long localRowID) {
        return getFlyweightStr0(localRowID, csB);
    }

    public FixedColumn getIndexColumn() {
        return indexColumn;
    }

    public String getStr(long localRowID) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.addressOf(offset, 4));

        if (len == NULL_LEN) {
            return null;
        }
        return getStr0(mappedFile.addressOf(offset + 4, len * 2), len);
    }

    public void getStr(long localRowID, CharSink sink) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.addressOf(offset, 4));

        if (len == NULL_LEN) {
            return;
        }

        long address = mappedFile.addressOf(offset + 4, len * 2);
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += 2;
        }
    }

    public int getStrLen(long localRowID) {
        return Unsafe.getUnsafe().getInt(mappedFile.addressOf(indexColumn.getLong(localRowID), 4));
    }

    public void putBin(ByteBuffer value) {
        final long rowOffset = getOffset();
        final long targetOffset = rowOffset + value.remaining() + 4;
        long appendOffset = rowOffset;

        long address = mappedFile.addressOf(rowOffset, 4);
        Unsafe.getUnsafe().putInt(address, value.remaining());
        appendOffset += 4;
        address += 4;
        int len = mappedFile.pageRemaining(appendOffset);

        while (appendOffset < targetOffset) {

            if (len == 0) {
                address = mappedFile.addressOf(appendOffset, 1);
                len = mappedFile.pageRemaining(appendOffset);
            }
            int min = len < value.remaining() ? len : value.remaining();

            for (int i = 0; i < min; i++) {
                Unsafe.getUnsafe().putByte(address++, value.get());
            }

            len -= min;
            appendOffset += min;
        }
        commitAppend(rowOffset, (int) (targetOffset - rowOffset));
    }

    public void putBin(InputStream s) {
        if (s == null) {
            putNull();
            return;
        }

        initStreamBuf();

        final long rowOffset = getOffset();
        long off = rowOffset + 4;
        int blockRemaining = 0;
        long blockAddress = 0;
        int bufRemaining;
        int sz = 0;
        try {
            while ((bufRemaining = s.read(streamBuf)) != -1) {
                sz += bufRemaining;
                long streamOffset = Unsafe.BYTE_OFFSET;
                while (bufRemaining > 0) {

                    if (blockRemaining == 0) {
                        blockAddress = mappedFile.addressOf(off, 1);
                        blockRemaining = mappedFile.pageRemaining(off);
                    }

                    if (blockRemaining < 1) {
                        throw new JournalRuntimeException("Internal error. Unable to allocateOffset disk block");
                    }

                    int len = bufRemaining > blockRemaining ? blockRemaining : bufRemaining;
                    Unsafe.getUnsafe().copyMemory(streamBuf, streamOffset, null, blockAddress, len);
                    bufRemaining -= len;
                    off += len;
                    blockRemaining -= len;
                    blockAddress += len;
                    streamOffset += len;
                }
            }
            long a = mappedFile.addressOf(rowOffset, 4);
            Unsafe.getUnsafe().putInt(a, sz);
            commitAppend(rowOffset, sz + 4);
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }

    public OutputStream putBin() {
        binOut.reset(getOffset());
        return binOut;
    }

    public long putNull() {
        long offset = getOffset();
        Unsafe.getUnsafe().putInt(mappedFile.addressOf(offset, 4), NULL_LEN);
        return commitAppend(offset, 4);
    }

    public long putStr(CharSequence value) {
        if (value == null) {
            return putNull();
        } else {
            int len = value.length() * 2 + 4;
            long offset = getOffset();
            Chars.strcpyw(value, mappedFile.addressOf(offset, len));
            return commitAppend(offset, len);
        }
    }

    public long putStr(DirectBytes value) {
        if (value == null) {
            return putNull();
        } else {
            int len = value.byteLength();
            long offset = getOffset();
            long address = mappedFile.addressOf(offset, len + 4);
            Unsafe.getUnsafe().putInt(address, len / 2);
            Unsafe.getUnsafe().copyMemory(value.address(), address + 4, len);
            return commitAppend(offset, len + 4);
        }
    }

    private long commitAppend(long offset, int size) {
        preCommit(offset + size);
        return indexColumn.putLong(offset);
    }

    private void getBin(long localRowID, OutputStream s, int len) {
        initStreamBuf();

        long offset = getOffset(localRowID) + 4; // skip size

        int blockRemaining = 0;
        long blockAddress = 0;

        try {
            while (len > 0) {
                if (blockRemaining == 0) {
                    blockAddress = mappedFile.addressOf(offset, 1);
                    blockRemaining = mappedFile.pageRemaining(offset);
                }

                int l = len > blockRemaining ? blockRemaining : len;
                Unsafe.getUnsafe().copyMemory(null, blockAddress, streamBuf, Unsafe.BYTE_OFFSET, l);
                offset += l;
                blockRemaining -= l;
                len -= l;
                blockAddress += l;
                s.write(streamBuf, 0, l);
            }
        } catch (IOException e) {
            throw new JournalRuntimeException(e);
        }
    }

    private CharSequence getFlyweightStr0(long localRowID, DirectCharSequence cs) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.addressOf(offset, 4));
        if (len == NULL_LEN) {
            return null;
        }
        len = len << 1;
        long lo = mappedFile.addressOf(offset + 4, len);
        return cs.of(lo, lo + len);
    }

    private String getStr0(long address, int len) {
        if (buffer.length < len) {
            buffer = new char[len];
        }
        Unsafe.getUnsafe().copyMemory(null, address, buffer, Unsafe.CHAR_OFFSET, ((long) len) * 2);
        return new String(buffer, 0, len);
    }

    private void initStreamBuf() {
        if (streamBuf == null) {
            streamBuf = new byte[1024 * 1024];
        }
    }

    private boolean isNull(long localRowID) {
        return Unsafe.getUnsafe().getInt(mappedFile.addressOf(indexColumn.getLong(localRowID), 4)) == NULL_LEN;
    }

    private class BinaryOutputStream extends OutputStream {

        private long offset = -1;
        private long workOffset;
        private long blockAddress;
        private int blockRemaining;

        @Override
        public void write(int b) {
            if (blockRemaining == 0) {
                renew();
            }
            Unsafe.getUnsafe().putByte(blockAddress++, (byte) b);
            workOffset++;
            blockRemaining--;
        }

        @Override
        public void close() {
            long a = mappedFile.addressOf(offset, 4);
            Unsafe.getUnsafe().putInt(a, (int) (workOffset - offset - 4));
            commitAppend(offset, (int) (workOffset - offset));
            offset = -1;
        }

        private void renew() {
            blockAddress = mappedFile.addressOf(workOffset, 1);
            blockRemaining = mappedFile.pageRemaining(workOffset);
        }

        private void reset(long offset) {
            if (this.offset != -1) {
                close();
            }
            this.offset = offset;
            this.workOffset = offset + 4;
            this.blockRemaining = 0;
        }
    }

    private class BinaryInputStream extends DirectInputStream {
        private long workOffset;
        private long blockAddress;
        private int remaining;
        private int pageRemaining;

        @Override
        public long copyTo(long address, long start, long length) {
            long res;
            long rem = remaining - start;
            long size = res = length > rem ? rem : length;
            long offset = workOffset + start;

            do {
                long from = mappedFile.addressOf(offset, 1);
                int remaining = mappedFile.pageRemaining(offset);
                int sz = size > remaining ? remaining : (int) size;
                Unsafe.getUnsafe().copyMemory(from, address, sz);
                address += sz;
                offset += sz;
                size -= sz;
            } while (size > 0);

            return res;
        }

        @Override
        public long size() {
            return remaining;
        }

        public boolean isNull() {
            return remaining == -1;
        }

        @Override
        public int read() {
            if (remaining == 0) {
                return -1;
            }

            if (pageRemaining == 0) {
                renew();
            }

            pageRemaining--;
            workOffset++;
            remaining--;
            return (int) Unsafe.getUnsafe().getByte(blockAddress++) & 0xFF;
        }

        private void renew() {
            blockAddress = mappedFile.addressOf(workOffset, 1);
            pageRemaining = mappedFile.pageRemaining(workOffset);
        }

        private void reset(long offset) {
            this.workOffset = offset + 4;
            this.pageRemaining = 0;
            this.remaining = Unsafe.getUnsafe().getInt(mappedFile.addressOf(offset, 4));
        }
    }
}
