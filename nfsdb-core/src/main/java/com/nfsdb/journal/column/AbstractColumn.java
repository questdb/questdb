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

import java.io.Closeable;
import java.nio.ByteBuffer;

public abstract class AbstractColumn implements Closeable {
    protected final MappedFile mappedFile;
    protected long txAppendOffset = -1;

    public abstract void truncate(long size);

    public void preCommit(long appendOffset) {
        txAppendOffset = appendOffset;
    }

    public void close() {
        mappedFile.close();
    }

    public long getOffset() {
        return mappedFile.getAppendOffset();
    }

    public abstract long getOffset(long localRowID);

    public void commit() {
        if (txAppendOffset != -1) {
            mappedFile.setAppendOffset(txAppendOffset);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[file=" + mappedFile.toString() + ", size=" + size() + "]";
    }

    public abstract long size();

    public ByteBuffer getBuffer(long offset, int size) {
        return mappedFile.getBuffer(offset, size);
    }

    public void compact() throws JournalException {
        mappedFile.compact();
    }

    AbstractColumn(MappedFile storage) {
        this.mappedFile = storage;
    }
}
