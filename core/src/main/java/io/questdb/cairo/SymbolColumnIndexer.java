/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class SymbolColumnIndexer implements ColumnIndexer, Mutable {

    private static final long SEQUENCE_OFFSET;
    private final int bufferSize;
    private final BitmapIndexWriter writer;
    private long buffer;
    private long columnTop;
    private volatile boolean distressed = false;
    private long fd = -1;
    private FilesFacade ff;
    @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
    private volatile long sequence = 0L;

    public SymbolColumnIndexer(CairoConfiguration configuration) {
        writer = new BitmapIndexWriter(configuration);
        bufferSize = 4096 * 1024;
        buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_INDEX_READER);
    }

    @Override
    public void clear() {
        writer.clear();
    }

    @Override
    public void close() {
        releaseIndexWriter();
        if (buffer != 0) {
            fd = -1;
            Unsafe.free(buffer, bufferSize, MemoryTag.NATIVE_INDEX_READER);
            buffer = 0;
        }
    }

    @Override
    public void configureFollowerAndWriter(
            Path path,
            CharSequence name,
            long columnNameTxn,
            MemoryMA columnMem,
            long columnTop
    ) {
        this.columnTop = columnTop;
        try {
            this.writer.of(path, name, columnNameTxn);
            this.ff = columnMem.getFilesFacade();
            // we don't own the fd, it comes from column mem
            this.fd = columnMem.getFd();
        } catch (Throwable e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void configureWriter(Path path, CharSequence name, long columnNameTxn, long columnTop) {
        this.columnTop = columnTop;
        try {
            writer.of(path, name, columnNameTxn);
        } catch (Throwable e) {
            this.close();
            throw e;
        }
    }

    @Override
    public void distress() {
        distressed = true;
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public BitmapIndexWriter getWriter() {
        return writer;
    }

    @Override
    public void index(FilesFacade ff, long dataColumnFd, long loRow, long hiRow) {
        // while we may have to read column starting with zero offset
        // index values have to be adjusted to partition-level row id
        writer.rollbackConditionally(loRow);

        long lo = Math.max(loRow, columnTop);
        int bufferCount = (int) (((hiRow - lo) * 4 - 1) / bufferSize + 1);
        for (int i = 0; i < bufferCount; i++) {
            long fileOffset = (lo - columnTop) * 4;
            long bytesToRead = Math.min(bufferSize, (hiRow - lo) * 4);
            long read = ff.read(dataColumnFd, buffer, bytesToRead, fileOffset);
            if (read == -1) {
                throw CairoException.critical(ff.errno()).put("could not read symbol column during indexing [fd=").put(dataColumnFd)
                        .put(", fileOffset=").put(fileOffset)
                        .put(", bytesToRead=").put(bytesToRead)
                        .put(']');
            }
            long pHi = buffer + read;
            for (long p = buffer; p < pHi; p += 4, lo++) {
                writer.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p)), lo);
            }
        }
        writer.setMaxValue(hiRow - 1);
    }

    @Override
    public boolean isDistressed() {
        return distressed;
    }

    @Override
    public void refreshSourceAndIndex(long loRow, long hiRow) {
        index(ff, fd, loRow, hiRow);
    }

    public void releaseIndexWriter() {
        Misc.free(writer);
    }

    @Override
    public void resetColumnTop() {
        columnTop = 0;
    }

    @Override
    public void rollback(long maxRow) {
        this.writer.rollbackValues(maxRow);
    }

    @Override
    public void sync(boolean async) {
        writer.sync(async);
    }

    @Override
    public boolean tryLock(long expectedSequence) {
        return Unsafe.cas(this, SEQUENCE_OFFSET, expectedSequence, expectedSequence + 1);
    }

    static {
        SEQUENCE_OFFSET = Unsafe.getFieldOffset(SymbolColumnIndexer.class, "sequence");
    }
}
