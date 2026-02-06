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

import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class SymbolColumnIndexer implements ColumnIndexer, Mutable {

    private static final long SEQUENCE_OFFSET;
    // 32 MB budget for bulk build tracking structures (~56 bytes per key)
    private static final long MAX_BULK_BUILD_MEMORY = 32 * 1024 * 1024;
    private static final int BYTES_PER_KEY = 56;  // histogram + blockOffset + cellIndex

    private final int bufferSize;
    private final BitmapIndexWriter writer;
    private IntIntHashMap histogram;
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

    /**
     * Rebuilds the entire index from scratch using an optimized two-pass histogram approach.
     * This method should only be used for full rebuilds when the index is empty/new.
     * For incremental appends during normal table writes, use {@link #index(FilesFacade, long, long, long)}.
     *
     * @param ff           files facade
     * @param dataColumnFd file descriptor of the symbol column data file
     * @param loRow        starting row (typically columnTop for full rebuilds)
     * @param hiRow        ending row (exclusive)
     */
    public void rebuildNewIndex(FilesFacade ff, long dataColumnFd, long loRow, long hiRow) {
        // For full rebuilds, we start fresh - no rollback needed
        writer.truncate();

        long lo = Math.max(loRow, columnTop);
        long totalRows = hiRow - lo;
        if (totalRows <= 0) {
            return;
        }

        // Initialize or reuse histogram
        if (histogram == null) {
            histogram = new IntIntHashMap();
        } else {
            histogram.clear();
        }

        // Pass 1: Build histogram
        long cutoffRow = hiRow;  // where we stopped counting (for escape hatch)
        long currentRow = lo;
        int maxKey = -1;

        pass1:
        for (long filePos = (lo - columnTop) * 4L, remaining = totalRows * 4L; remaining > 0; ) {
            long bytesToRead = Math.min(bufferSize, remaining);
            long read = ff.read(dataColumnFd, buffer, bytesToRead, filePos);
            if (read == -1) {
                throw CairoException.critical(ff.errno()).put("could not read symbol column during indexing [fd=").put(dataColumnFd)
                        .put(", fileOffset=").put(filePos)
                        .put(", bytesToRead=").put(bytesToRead)
                        .put(']');
            }

            for (long p = buffer, pHi = buffer + read; p < pHi; p += 4, currentRow++) {
                int key = TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p));
                int idx = histogram.keyIndex(key);
                if (idx >= 0) {
                    // New key
                    if ((long) histogram.size() * BYTES_PER_KEY >= MAX_BULK_BUILD_MEMORY) {
                        // Escape hatch: too many keys, stop histogram building
                        cutoffRow = currentRow;
                        break pass1;
                    }
                    histogram.putAt(idx, key, 1);
                } else {
                    // Existing key, increment
                    histogram.putAt(idx, key, histogram.valueAt(idx) + 1);
                }
                if (key > maxKey) {
                    maxKey = key;
                }
            }

            filePos += read;
            remaining -= read;
        }

        // Initialize bulk build with histogram
        writer.initBulkBuild(histogram, maxKey);
        histogram = null;  // Ownership transferred to writer

        // Pass 2: Write values using bulk build
        currentRow = lo;
        for (long filePos = (lo - columnTop) * 4L, remaining = (cutoffRow - lo) * 4L; remaining > 0; ) {
            long bytesToRead = Math.min(bufferSize, remaining);
            long read = ff.read(dataColumnFd, buffer, bytesToRead, filePos);
            if (read == -1) {
                throw CairoException.critical(ff.errno()).put("could not read symbol column during indexing [fd=").put(dataColumnFd)
                        .put(", fileOffset=").put(filePos)
                        .put(", bytesToRead=").put(bytesToRead)
                        .put(']');
            }

            for (long p = buffer, pHi = buffer + read; p < pHi; p += 4, currentRow++) {
                int key = TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p));
                writer.addBulk(key, currentRow);
            }

            filePos += read;
            remaining -= read;
        }

        // Finalize bulk build
        writer.finalizeBulkBuild(cutoffRow - 1);

        // Pass 3 (if needed): Fall back to regular add() for remaining rows
        if (cutoffRow < hiRow) {
            for (long filePos = (cutoffRow - columnTop) * 4L, remaining = (hiRow - cutoffRow) * 4L; remaining > 0; ) {
                long bytesToRead = Math.min(bufferSize, remaining);
                long read = ff.read(dataColumnFd, buffer, bytesToRead, filePos);
                if (read == -1) {
                    throw CairoException.critical(ff.errno()).put("could not read symbol column during indexing [fd=").put(dataColumnFd)
                            .put(", fileOffset=").put(filePos)
                            .put(", bytesToRead=").put(bytesToRead)
                            .put(']');
                }

                for (long p = buffer, pHi = buffer + read; p < pHi; p += 4, currentRow++) {
                    int key = TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p));
                    writer.add(key, currentRow);
                }

                filePos += read;
                remaining -= read;
            }
            writer.setMaxValue(hiRow - 1);
        }
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
