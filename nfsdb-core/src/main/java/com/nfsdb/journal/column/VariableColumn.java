/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.column;

import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class VariableColumn extends AbstractColumn {
    private final FixedColumn indexColumn;
    private char buffer[] = new char[32];
    private byte[] streamBuf;

    public VariableColumn(MappedFile dataFile, MappedFile indexFile) {
        super(dataFile);
        this.indexColumn = new FixedColumn(indexFile, 8);
    }

    @Override
    public void commit() {
        super.commit();
        indexColumn.commit();
    }

    @Override
    public void force() {
        super.force();
        indexColumn.force();
    }

    @Override
    public void close() {
        indexColumn.close();
        super.close();
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

    @Override
    public long size() {
        return indexColumn.size();
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
        return getStr0(mappedFile.getAddress(offset, len * 2 + 4) + 4, len);
    }

    public boolean cmpStr(long localRowID, String value) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));

        if (len != value.length()) {
            return false;
        }

        long address = mappedFile.getAddress(offset, len * 2 + 4) + 4;
        for (int i = 0; i < len; i++) {
            if (Unsafe.getUnsafe().getChar(address) != value.charAt(i)) {
                return false;
            }
            address += 2;
        }

        return true;
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
    public void compact() throws JournalException {
        super.compact();
        this.indexColumn.compact();
    }

    public FixedColumn getIndexColumn() {
        return indexColumn;
    }

    public void putBin(ByteBuffer value) {
        final long rowOffset = getOffset();
        final long targetOffset = rowOffset + value.remaining() + 4;
        long appendOffset = rowOffset;

        ByteBuffer target = getBuffer(rowOffset, 4);
        target.putInt(value.remaining());
        appendOffset += 4;
        while (true) {
            appendOffset += ByteBuffers.copy(value, target, targetOffset - appendOffset);
            if (appendOffset < targetOffset) {
                target = getBuffer(appendOffset, 1);
            } else {
                break;
            }
        }
        commitAppend(rowOffset, (int) (targetOffset - rowOffset));
    }

    private void initStreamBuf() {
        if (streamBuf == null) {
            streamBuf = new byte[1024 * 1024];
        }
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
                        blockRemaining = mappedFile.getLocalRemaining(off);
                    }

                    if (blockRemaining <= 0) {
                        throw new JournalRuntimeException("Internal error. Unable to allocate disk block");
                    }

                    int len = bufRemaining > blockRemaining ? blockRemaining : bufRemaining;
                    Unsafe.getUnsafe().copyMemory(streamBuf, Unsafe.getByteArrayOffset(), null, blockAddress, len);
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

    public int getBinSize(long localRowID) {
        return Unsafe.getUnsafe().getInt(mappedFile.getAddress(getOffset(localRowID), 4));
    }

    public void getBin(long localRowID, ByteBuffer target) {
        long offset = getOffset(localRowID) + 4; // skip size

        while (target.remaining() > 0) {
            ByteBuffer bb = getBuffer(offset, 1);
            int sz = ByteBuffers.copy(bb, target, target.remaining());
            offset += sz;
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
                    blockRemaining = mappedFile.getLocalRemaining(offset);
                }

                if (blockRemaining <= 0) {
                    throw new JournalRuntimeException("Internal error. Unable to allocate disk block");
                }

                int l = len > blockRemaining ? blockRemaining : len;
                Unsafe.getUnsafe().copyMemory(null, blockAddress, streamBuf, Unsafe.getByteArrayOffset(), l);
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

    public long putNull() {
        long offset = getOffset();
        Unsafe.getUnsafe().putInt(mappedFile.getAddress(offset, 4), -1);
        return commitAppend(offset, 4);
    }

    private String getStr0(long address, int len) {
        if (buffer.length < len) {
            buffer = new char[len];
        }
        Unsafe.getUnsafe().copyMemory(null, address, buffer, Unsafe.getCharArrayOffset(), len * 2);
        return new String(buffer, 0, len);
    }

    long commitAppend(long offset, int size) {
        preCommit(offset + size);
        return indexColumn.putLong(offset);
    }
}
