/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 *
 ******************************************************************************/

package com.nfsdb.store;

import com.nfsdb.ex.JournalException;

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
