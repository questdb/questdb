/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.storage;

import com.nfsdb.collections.DirectCharSequence;
import com.nfsdb.column.DirectInputStream;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
public class VariableColumn extends AbstractColumn {
    private final FixedColumn indexColumn;
    private final BinaryOutputStream binOut = new BinaryOutputStream();
    private final BinaryInputStream binIn = new BinaryInputStream();
    private final DirectCharSequence charSequence = new DirectCharSequence();
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

    public boolean cmpStr(long localRowID, CharSequence value) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));

        if (len != value.length()) {
            return false;
        }

        long address = mappedFile.getAddress(offset + 4, len * 2);
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getChar(address) != value.charAt(i)) {
                return false;
            }
            address += 2;
        }

        return true;
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

    public void getBin(long localRowID, ByteBuffer target) {
        long offset = getOffset(localRowID) + 4; // skip size

        while (target.hasRemaining()) {
            long address = mappedFile.getAddress(offset, 1);
            int len = mappedFile.getAddressSize(offset);
            int min = len < target.remaining() ? len : target.remaining();

            for (int i = 0; i < min; i++) {
                target.put(Unsafe.getUnsafe().getByte(address++));
            }
            offset += min;
        }
    }

    public void getBin(long localRowID, OutputStream s) {
        getBin(localRowID, s, getBinSize(localRowID));
    }

    public void getBin(long localRowID, OutputStream s, int len) {
        initStreamBuf();

        long offset = getOffset(localRowID) + 4; // skip size

        int blockRemaining = 0;
        long blockAddress = 0;

        try {
            while (len > 0) {
                if (blockRemaining == 0) {
                    blockAddress = mappedFile.getAddress(offset, 1);
                    blockRemaining = mappedFile.getAddressSize(offset);
                }

                int l = len > blockRemaining ? blockRemaining : len;
                Unsafe.getUnsafe().copyMemory(null, blockAddress, streamBuf, sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET, l);
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

    public DirectInputStream getBin(long localRowID) {
        binIn.reset(getOffset(localRowID));
        return binIn;
    }

    public int getBinSize(long localRowID) {
        return Unsafe.getUnsafe().getInt(mappedFile.getAddress(getOffset(localRowID), 4));
    }

    public CharSequence getFlyweightStr(long localRowID) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));
        if (len == -1) {
            return null;
        }
        long lo = mappedFile.getAddress(offset + 4, len * 2);
        return charSequence.init(lo, lo + len * 2);
    }

    public FixedColumn getIndexColumn() {
        return indexColumn;
    }

    @Override
    public long getOffset(long localRowID) {
        return indexColumn.getLong(localRowID);
    }

    public String getStr(long localRowID) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));

        if (len == -1) {
            return null;
        }
        return getStr0(mappedFile.getAddress(offset + 4, len * 2), len);
    }

    public void getStr(long localRowID, CharSink sink) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));

        if (len == -1) {
            return;
        }

        long address = mappedFile.getAddress(offset + 4, len * 2);
        for (int i = 0; i < len; i++) {
            sink.put(Unsafe.getUnsafe().getChar(address));
            address += 2;
        }
    }

    public void putBin(ByteBuffer value) {
        final long rowOffset = getOffset();
        final long targetOffset = rowOffset + value.remaining() + 4;
        long appendOffset = rowOffset;

        long address = mappedFile.getAddress(rowOffset, 4);
        Unsafe.getUnsafe().putInt(address, value.remaining());
        appendOffset += 4;
        address += 4;
        int len = mappedFile.getAddressSize(appendOffset);

        while (appendOffset < targetOffset) {

            if (len == 0) {
                address = mappedFile.getAddress(appendOffset, 1);
                len = mappedFile.getAddressSize(appendOffset);
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
                while (bufRemaining > 0) {

                    if (blockRemaining == 0) {
                        blockAddress = mappedFile.getAddress(off, 1);
                        blockRemaining = mappedFile.getAddressSize(off);
                    }

                    if (blockRemaining <= 0) {
                        throw new JournalRuntimeException("Internal error. Unable to allocate disk block");
                    }

                    int len = bufRemaining > blockRemaining ? blockRemaining : bufRemaining;
                    Unsafe.getUnsafe().copyMemory(streamBuf, sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET, null, blockAddress, len);
                    bufRemaining -= len;
                    off += len;
                    blockRemaining -= len;
                    blockAddress += len;
                }
            }
            long a = mappedFile.getAddress(rowOffset, 4);
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
        Unsafe.getUnsafe().putInt(mappedFile.getAddress(offset, 4), -1);
        return commitAppend(offset, 4);
    }

    public long putStr(CharSequence value) {
        if (value == null) {
            return putNull();
        } else {
            int l;
            int len = (l = value.length()) * 2 + 4;
            long offset = getOffset();
            long address = mappedFile.getAddress(offset, len);
            Unsafe.getUnsafe().putInt(address, l);
            address += 4;
            for (int i = 0; i < l; i++) {
                Unsafe.getUnsafe().putChar(address, value.charAt(i));
                address += 2;
            }
            return commitAppend(offset, len);
        }
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

    long commitAppend(long offset, int size) {
        preCommit(offset + size);
        return indexColumn.putLong(offset);
    }

    private String getStr0(long address, int len) {
        if (buffer.length < len) {
            buffer = new char[len];
        }
        Unsafe.getUnsafe().copyMemory(null, address, buffer, sun.misc.Unsafe.ARRAY_CHAR_BASE_OFFSET, ((long) len) * 2);
        return new String(buffer, 0, len);
    }

    private void initStreamBuf() {
        if (streamBuf == null) {
            streamBuf = new byte[1024 * 1024];
        }
    }

    private class BinaryOutputStream extends OutputStream {

        private long offset = -1;
        private long workOffset;
        private long blockAddress;
        private int blockRemaining;

        @Override
        public void close() {
            long a = mappedFile.getAddress(offset, 4);
            Unsafe.getUnsafe().putInt(a, (int) (workOffset - offset - 4));
            commitAppend(offset, (int) (workOffset - offset));
            offset = -1;
        }

        @Override
        public void write(int b) throws IOException {
            if (blockRemaining == 0) {
                renew();
            }
            Unsafe.getUnsafe().putByte(blockAddress++, (byte) b);
            workOffset++;
            blockRemaining--;
        }

        private void renew() {
            blockAddress = mappedFile.getAddress(workOffset, 1);
            blockRemaining = mappedFile.getAddressSize(workOffset);
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
        private int blockRemaining;

        @Override
        public long copyTo(long address, long start, long length) {
            skipOffset(start);
            long totalLen = Math.min(remaining, length);
            long targetAddress = address + totalLen;

            while (address < targetAddress) {
                // copy to the block end.
                long readBlockLen = Math.min(targetAddress - address, blockRemaining);
                Unsafe.getUnsafe().copyMemory(blockAddress, address, readBlockLen);

                address += readBlockLen;
                workOffset += readBlockLen;
                blockAddress += readBlockLen;
                if (targetAddress - address > 0) {
                    renew();
                }
            }
            remaining -= totalLen;
            return totalLen;
        }

        @Override
        public long getLength() {
            return remaining;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }

            if (blockRemaining == 0) {
                renew();
            }

            blockRemaining--;
            workOffset++;
            remaining--;
            return Unsafe.getUnsafe().getByte(blockAddress++);
        }

        private void reset(long offset) {
            this.workOffset = offset + 4;
            this.blockRemaining = 0;
            this.remaining = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));
        }

        private void renew() {
            blockAddress = mappedFile.getAddress(workOffset, 1);
            blockRemaining = mappedFile.getAddressSize(workOffset);
        }

        private void skipOffset(long offset) {
            if (offset > remaining ) {
                throw new IndexOutOfBoundsException(String.format("Offset %d is greater than remaining length %d", offset, remaining));
            }
            remaining -= offset;
            long targetWorkOffset = workOffset + offset;

            while (workOffset < targetWorkOffset) {
                if (blockRemaining == 0) {
                    renew();
                }

                long blockSkip = Math.min(targetWorkOffset - workOffset, blockRemaining);
                workOffset += blockSkip;
                blockRemaining -= blockSkip;
                blockAddress += blockSkip;
            }
        }
    }
}
