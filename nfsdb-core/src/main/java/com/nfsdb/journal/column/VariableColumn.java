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

import java.nio.ByteBuffer;


public class VariableColumn extends AbstractColumn {
    private final FixedColumn indexColumn;
    private char buffer[] = new char[32];

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

    public String getString(long localRowID) {
        long offset = indexColumn.getLong(localRowID);
        int len = Unsafe.getUnsafe().getInt(mappedFile.getAddress(offset, 4));

        if (len == -1) {
            return null;
        }
        return asString(mappedFile.getAddress(offset, len * 2 + 4) + 4, len);
    }

    public boolean equalsString(long localRowID, String value) {
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

    public long putString(String value) {
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

    public void putBuffer(ByteBuffer value) {
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

    public int getBufferSize(long localRowID) {
        ByteBuffer bb = getBufferInternal(localRowID, 4);
        return bb.getInt(bb.position());
    }

    public void getBuffer(long localRowID, ByteBuffer target, int count) {
        long offset = getOffset(localRowID) + 4; // skip size
        if (count < target.remaining()) {
            throw new JournalRuntimeException("ByteBuffer too small");
        }

        while (count > 0) {
            ByteBuffer bb = getBuffer(offset, 1);
            int sz = ByteBuffers.copy(bb, target, count);
            count -= sz;
            offset += sz;
        }
    }

    public long putNull() {
        long offset = getOffset();
        Unsafe.getUnsafe().putInt(mappedFile.getAddress(offset, 4), -1);
        return commitAppend(offset, 4);
    }

    private String asString(long address, int len) {
        if (buffer.length < len) {
            buffer = new char[len];
        }
        for (int i = 0; i < len; i++) {
            buffer[i] = Unsafe.getUnsafe().getChar(address);
            address += 2;
        }
        return new String(buffer, 0, len);
    }

    private ByteBuffer getBufferInternal(long localRowID, int recordLength) {
        long max = indexColumn.size();
        if (localRowID > max) {
            throw new JournalRuntimeException("localRowID is out of bounds. %d > %d", localRowID, max);
        }
        return getBuffer(getOffset(localRowID), recordLength);
    }

    long commitAppend(long offset, int size) {
        preCommit(offset + size);
        return indexColumn.putLong(offset);
    }
}
