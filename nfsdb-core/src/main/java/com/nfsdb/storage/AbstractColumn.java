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

package com.nfsdb.storage;

import com.nfsdb.exceptions.JournalException;

import java.io.Closeable;
import java.nio.ByteBuffer;

public abstract class AbstractColumn implements Closeable {
    final MemoryFile mappedFile;
    long txAppendOffset = -1;

    AbstractColumn(MemoryFile storage) {
        this.mappedFile = storage;
    }

    public void close() {
        mappedFile.close();
    }

    public void commit() {
        if (txAppendOffset != -1) {
            mappedFile.setAppendOffset(txAppendOffset);
        }
    }

    public void compact() throws JournalException {
        mappedFile.compact();
    }

    public void force() {
        mappedFile.force();
    }

    public ByteBuffer getBuffer(long offset, int size) {
        return mappedFile.getBuffer(offset, size);
    }

    public long getOffset() {
        return mappedFile.getAppendOffset();
    }

    public abstract long getOffset(long localRowID);

    public void preCommit(long appendOffset) {
        txAppendOffset = appendOffset;
    }

    public abstract long size();

    @Override
    public String toString() {
        return this.getClass().getName() + "[file=" + mappedFile + ", size=" + size() + ']';
    }

    public abstract void truncate(long size);
}
