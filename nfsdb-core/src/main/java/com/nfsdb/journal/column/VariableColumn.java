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
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.utils.ByteBuffers;

import java.nio.ByteBuffer;


public class VariableColumn extends AbstractColumn {
    private final FixedColumn indexColumn;
    private char buffer[] = new char[32];

    public VariableColumn(MappedFile dataFile, MappedFile indexFile) {
        super(dataFile);
        this.indexColumn = new FixedColumn(indexFile, JournalConfiguration.VARCHAR_INDEX_COLUMN_WIDTH);
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
        // read delegate buffer which lets us read "null" flag and string length.
        ByteBuffer bb = getBufferInternal(localRowID, JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH);
        int len = bb.getInt();

        if (len == -1) {
            return null;
        }

        // check if buffer can have actual string (char=2*byte)
        if (bb.remaining() < len * 2) {
            bb = getBufferInternal(localRowID, len * 2 + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH);
            bb.position(bb.position() + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH);
        }

        return asString(bb, len);
    }

    public boolean equalsString(long localRowID, String value) {
        // read delegate buffer which lets us read "null" flag and string length.
        ByteBuffer buf = getBufferInternal(localRowID, JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH);
        int len = buf.getInt();

        if (len != value.length()) {
            return false;
        }

        int p;
        // check if buffer can have actual string (char=2*byte)
        if (buf.remaining() < len * 2) {
            buf = getBufferInternal(localRowID, len * 2 + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH);
            p = buf.position() + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH;
        } else {
            p = buf.position();
        }

        for (int i = 0; i < len; i++) {
            if (buf.getChar(p) != value.charAt(i)) {
                return false;
            }
            p += 2;
        }

        return true;
    }

    public long putString(String value) {
        if (value == null) {
            return putNull();
        } else {
            int len = value.length() * 2 + JournalConfiguration.VARCHAR_LARGE_HEADER_LENGTH;
            long offset = getOffset();
            ByteBuffer bb = getBuffer(offset, len);
            ByteBuffers.putStringDW(bb, value);
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
        ByteBuffer bb = getBuffer(offset, 4);
        bb.putInt(bb.position(), -1);
        return commitAppend(offset, 4);
    }

    private String asString(ByteBuffer bb, int len) {
        if (buffer.length < len) {
            buffer = new char[len];
        }
        int p = bb.position();
        for (int i = 0; i < len; i++) {
            buffer[i] = bb.getChar(p);
            p += 2;
        }
        return new String(buffer, 0, len);
    }

    private ByteBuffer getBufferInternal(long localRowID, int recordLength) {
        long max = indexColumn.size();

        if (localRowID > max) {
            throw new JournalRuntimeException("localRowID is out of bounds. %d > %d", localRowID, max);
        }

        if (localRowID == max) {
            return getBuffer(getOffset(), recordLength);
        } else {
            return getBuffer(getOffset(localRowID), recordLength);
        }
    }

    long commitAppend(long offset, int size) {
        preCommit(offset + size);
        return indexColumn.putLong(offset);
    }

}
