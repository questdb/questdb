/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

import com.questdb.ex.JournalException;

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
