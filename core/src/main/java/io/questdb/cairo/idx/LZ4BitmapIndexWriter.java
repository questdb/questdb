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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.idx.LZ4BitmapIndexUtils.*;

/**
 * Page-grouped LZ4-compressed bitmap index writer.
 * <p>
 * Buffers raw 8-byte longs per key in native memory. On commit/close,
 * groups consecutive keys into large pages (~64 KB), LZ4-compresses each
 * page, and writes them sequentially to the value file. Within each page,
 * values are interleaved by round (all val0s together, then val1s, etc.)
 * for optimal LZ4 compression.
 */
public class LZ4BitmapIndexWriter implements IndexWriter {
    private static final int INITIAL_KEY_CAPACITY = 64;
    private static final Log LOG = LogFactory.getLog(LZ4BitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final int[] hashTable = new int[4096];
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();

    private int blockValues;
    private long compressBufferAddr;
    private int compressBufferSize;
    private FilesFacade ff;
    private boolean hasPendingData;
    private int keyCapacity;
    private int keyCount;
    private int keysPerPage;
    private long pendingCountsAddr;  // int per key: number of pending values
    private long pendingValuesAddr;  // flat buffer: keyCapacity * blockValues longs
    private long valueMemSize;

    public LZ4BitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @TestOnly
    public LZ4BitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, false);
    }

    public static void initKeyMemory(MemoryMA keyMem) {
        initKeyMemory(keyMem, DEFAULT_BLOCK_VALUES);
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockValues) {
        initKeyMemory(keyMem, blockValues, LZ4BitmapIndexUtils.computeKeysPerPage(blockValues));
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockValues, int keysPerPage) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(SIGNATURE);
        keyMem.skip(7);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE_MEM_SIZE
        keyMem.putInt(blockValues); // BLOCK_VALUES
        keyMem.putInt(0); // KEY_COUNT
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE_CHECK
        keyMem.putLong(-1); // MAX_VALUE
        keyMem.putInt(keysPerPage); // KEYS_PER_PAGE
        keyMem.putInt(0); // PAGE_COUNT
        keyMem.putInt(0); // PAGES_PER_GEN (set on first flush)
        keyMem.skip(KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    @Override
    public void add(int key, long value) {
        if (key < 0) {
            throw CairoException.critical(0).put("index key cannot be negative [key=").put(key).put(']');
        }

        if (key >= keyCapacity) {
            growKeyBuffers(key + 1);
        }

        int count = Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES);

        if (count >= blockValues) {
            throw CairoException.critical(0)
                    .put("too many values for key [key=").put(key)
                    .put(", count=").put(count)
                    .put(", blockValues=").put(blockValues).put(']');
        }

        // Validate ordering
        if (count > 0) {
            long lastVal = Unsafe.getUnsafe().getLong(
                    pendingValuesAddr + ((long) key * blockValues + count - 1) * Long.BYTES);
            if (value < lastVal) {
                throw CairoException.critical(0)
                        .put("index values must be added in ascending order [lastValue=")
                        .put(lastVal).put(", newValue=").put(value).put(']');
            }
        }

        Unsafe.getUnsafe().putLong(
                pendingValuesAddr + ((long) key * blockValues + count) * Long.BYTES, value);
        Unsafe.getUnsafe().putInt(pendingCountsAddr + (long) key * Integer.BYTES, count + 1);

        if (key >= keyCount) {
            keyCount = key + 1;
        }
        hasPendingData = true;
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
        flushAllPending();

        if (keyMem.isOpen()) {
            int pageCount = keyMem.getInt(KEY_RESERVED_OFFSET_PAGE_COUNT);
            long keyFileSize = pageCount > 0
                    ? LZ4BitmapIndexUtils.getPageDirOffset(pageCount)
                    : KEY_FILE_RESERVED;
            keyMem.setSize(keyFileSize);
            Misc.free(keyMem);
        }
        if (valueMem.isOpen()) {
            if (valueMemSize > 0) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }

        freeNativeBuffers();
        keyCount = 0;
        valueMemSize = 0;
        hasPendingData = false;
    }

    @Override
    public void closeNoTruncate() {
        close();
    }

    @Override
    public void commit() {
        flushAllPending();
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    @TestOnly
    public RowCursor getCursor(int key) {
        flushAllPending();

        if (key >= keyCount || key < 0) {
            return EmptyRowCursor.INSTANCE;
        }

        int totalPages = keyMem.getInt(KEY_RESERVED_OFFSET_PAGE_COUNT);
        int pagesPerGen = keyMem.getInt(KEY_RESERVED_OFFSET_PAGES_PER_GEN);
        if (pagesPerGen == 0 || totalPages == 0) {
            return EmptyRowCursor.INSTANCE;
        }

        int genCount = totalPages / pagesPerGen;
        int pageSlot = key / keysPerPage;
        int slotInPage = key % keysPerPage;

        int keysInPage = LZ4BitmapIndexUtils.keysInPage(pageSlot, keysPerPage, keyCount);
        int countsSize = LZ4BitmapIndexUtils.pageCountsSize(keysInPage);
        int rawSize = LZ4BitmapIndexUtils.pageRawSize(keysInPage, blockValues);

        LongList values = new LongList();
        long decompBuf = Unsafe.malloc(rawSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int gen = 0; gen < genCount; gen++) {
                int pageIndex = gen * pagesPerGen + pageSlot;
                long dirOffset = LZ4BitmapIndexUtils.getPageDirOffset(pageIndex);
                long pageFileOffset = keyMem.getLong(dirOffset + PAGE_DIR_OFFSET_FILE_OFFSET);
                int compSize = keyMem.getInt(dirOffset + PAGE_DIR_OFFSET_COMPRESSED_SIZE);

                long srcAddr = valueMem.addressOf(pageFileOffset);
                if (compSize == rawSize) {
                    Unsafe.getUnsafe().copyMemory(srcAddr, decompBuf, rawSize);
                } else {
                    LZ4BitmapIndexUtils.decompress(srcAddr, compSize, decompBuf, rawSize);
                }

                int count = Unsafe.getUnsafe().getInt(decompBuf + (long) slotInPage * Integer.BYTES);
                for (int round = 0; round < count; round++) {
                    long offset = countsSize + (long) round * keysInPage * Long.BYTES + (long) slotInPage * Long.BYTES;
                    values.add(Unsafe.getUnsafe().getLong(decompBuf + offset));
                }
            }
        } finally {
            Unsafe.free(decompBuf, rawSize, MemoryTag.NATIVE_DEFAULT);
        }

        return new TestBwdCursor(values);
    }

    @Override
    public byte getIndexType() {
        return IndexType.LZ4;
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @Override
    public long getMaxValue() {
        return keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    @Override
    public boolean isOpen() {
        return keyMem.isOpen();
    }

    @Override
    public void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    public void of(Path path, CharSequence name, long columnNameTxn, boolean init) {
        close();
        final int plen = path.size();
        boolean kFdUnassigned = true;

        try {
            LPSZ keyFile = keyFileName(path, name, columnNameTxn);

            if (init) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockValues = DEFAULT_BLOCK_VALUES;
                this.keysPerPage = LZ4BitmapIndexUtils.computeKeysPerPage(blockValues);
                initKeyMemory(keyMem, blockValues);
                kFdUnassigned = false;
            } else {
                if (!ff.exists(keyFile)) {
                    throw CairoException.critical(0).put("index does not exist [path=").put(path).put(']');
                }

                long keyFileSize = ff.length(keyFile);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), -1L, MemoryTag.MMAP_INDEX_WRITER);
                kFdUnassigned = false;

                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid LZ4 index signature [expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }

                long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE);
                long seqCheck = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
                if (seq != seqCheck) {
                    throw CairoException.critical(0)
                            .put("Sequence mismatch (partial write detected) [seq=").put(seq)
                            .put(", seqCheck=").put(seqCheck).put(']');
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockValues = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_VALUES);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            this.keysPerPage = keyMem.getInt(KEY_RESERVED_OFFSET_KEYS_PER_PAGE);

            valueMem.of(
                    ff,
                    valueFileName(path.trimTo(plen), name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    init ? 0 : valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (!init && valueMemSize > 0) {
                valueMem.jumpTo(valueMemSize);
            }

            allocateNativeBuffers();
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                LOG.error().$("could not open LZ4 index [path=").$(path).$(']').$();
            }
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean init, int blockCapacity) {
        throw new UnsupportedOperationException("LZ4 index does not support fd-based open");
    }

    @Override
    public void rollbackConditionally(long row) {
        throw new UnsupportedOperationException("LZ4 index rollback not yet implemented");
    }

    @Override
    public void rollbackValues(long maxValue) {
        throw new UnsupportedOperationException("LZ4 index rollback not yet implemented");
    }

    @Override
    public void setMaxValue(long maxValue) {
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
    }

    @Override
    public void sync(boolean async) {
        if (keyMem.isOpen()) {
            keyMem.sync(async);
        }
        if (valueMem.isOpen()) {
            valueMem.sync(async);
        }
    }

    @Override
    public void truncate() {
        freeNativeBuffers();
        initKeyMemory(keyMem, blockValues);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        hasPendingData = false;
        allocateNativeBuffers();
    }

    private void allocateNativeBuffers() {
        keyCapacity = Math.max(INITIAL_KEY_CAPACITY, keyCount);
        long valBufSize = (long) keyCapacity * blockValues * Long.BYTES;
        long countBufSize = (long) keyCapacity * Integer.BYTES;

        pendingValuesAddr = Unsafe.malloc(valBufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr, valBufSize, (byte) 0);

        pendingCountsAddr = Unsafe.malloc(countBufSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr, countBufSize, (byte) 0);

        // Compress buffer sized for one page
        int maxKeysInPage = Math.min(keysPerPage, Math.max(keyCapacity, 1));
        int maxPageRawSize = LZ4BitmapIndexUtils.pageRawSize(maxKeysInPage, blockValues);
        compressBufferSize = LZ4BitmapIndexUtils.maxCompressedLength(maxPageRawSize);
        compressBufferAddr = Unsafe.malloc(compressBufferSize, MemoryTag.NATIVE_DEFAULT);
    }

    private void flushAllPending() {
        if (!hasPendingData || pendingCountsAddr == 0 || keyCount == 0) {
            return;
        }

        int existingPageCount = keyMem.getInt(KEY_RESERVED_OFFSET_PAGE_COUNT);
        int pagesPerGen = keyMem.getInt(KEY_RESERVED_OFFSET_PAGES_PER_GEN);

        if (pagesPerGen == 0) {
            pagesPerGen = (keyCount + keysPerPage - 1) / keysPerPage;
            keyMem.putInt(KEY_RESERVED_OFFSET_PAGES_PER_GEN, pagesPerGen);
        }

        int maxKeysInPage = Math.min(keysPerPage, keyCount);
        int maxRawSize = LZ4BitmapIndexUtils.pageRawSize(maxKeysInPage, blockValues);
        long pageBuf = Unsafe.malloc(maxRawSize, MemoryTag.NATIVE_DEFAULT);

        // Ensure compress buffer is large enough
        int maxCompSize = LZ4BitmapIndexUtils.maxCompressedLength(maxRawSize);
        if (compressBufferSize < maxCompSize) {
            if (compressBufferAddr != 0) {
                Unsafe.free(compressBufferAddr, compressBufferSize, MemoryTag.NATIVE_DEFAULT);
            }
            compressBufferSize = maxCompSize;
            compressBufferAddr = Unsafe.malloc(compressBufferSize, MemoryTag.NATIVE_DEFAULT);
        }

        long maxValue = keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);

        try {
            for (int page = 0; page < pagesPerGen; page++) {
                int firstKey = page * keysPerPage;
                int keysInPage = LZ4BitmapIndexUtils.keysInPage(page, keysPerPage, keyCount);
                int countsSize = LZ4BitmapIndexUtils.pageCountsSize(keysInPage);
                int rawSize = countsSize + keysInPage * blockValues * (int) Long.BYTES;

                // Zero the buffer
                Unsafe.getUnsafe().setMemory(pageBuf, rawSize, (byte) 0);

                // Build interleaved page: per-key counts + value rounds
                for (int i = 0; i < keysInPage; i++) {
                    int key = firstKey + i;
                    int count = (key < keyCapacity)
                            ? Unsafe.getUnsafe().getInt(pendingCountsAddr + (long) key * Integer.BYTES)
                            : 0;

                    // Write count
                    Unsafe.getUnsafe().putInt(pageBuf + (long) i * Integer.BYTES, count);

                    // Write interleaved values
                    for (int round = 0; round < count; round++) {
                        long value = Unsafe.getUnsafe().getLong(
                                pendingValuesAddr + ((long) key * blockValues + round) * Long.BYTES);
                        long destOffset = countsSize + (long) round * keysInPage * Long.BYTES + (long) i * Long.BYTES;
                        Unsafe.getUnsafe().putLong(pageBuf + destOffset, value);
                        if (value > maxValue) {
                            maxValue = value;
                        }
                    }
                }

                // LZ4 compress
                int compSize = LZ4BitmapIndexUtils.compress(
                        pageBuf, rawSize, compressBufferAddr, compressBufferSize, hashTable);
                boolean useCompressed = compSize < rawSize;
                int writeSize = useCompressed ? compSize : rawSize;
                long writeSrc = useCompressed ? compressBufferAddr : pageBuf;

                // Write compressed page to value file
                long pageOffset = valueMemSize;
                valueMem.jumpTo(pageOffset);

                int written = 0;
                while (written + Long.BYTES <= writeSize) {
                    valueMem.putLong(Unsafe.getUnsafe().getLong(writeSrc + written));
                    written += (int) Long.BYTES;
                }
                while (written < writeSize) {
                    valueMem.putByte(Unsafe.getUnsafe().getByte(writeSrc + written));
                    written++;
                }

                valueMemSize = pageOffset + writeSize;

                // Write page directory entry (append after existing pages)
                long dirOffset = LZ4BitmapIndexUtils.getPageDirOffset(existingPageCount + page);
                keyMem.putLong(dirOffset + PAGE_DIR_OFFSET_FILE_OFFSET, pageOffset);
                keyMem.putInt(dirOffset + PAGE_DIR_OFFSET_COMPRESSED_SIZE, writeSize);
            }
        } finally {
            Unsafe.free(pageBuf, maxRawSize, MemoryTag.NATIVE_DEFAULT);
        }

        // Update header
        keyMem.putInt(KEY_RESERVED_OFFSET_PAGE_COUNT, existingPageCount + pagesPerGen);
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
        updateHeaderAtomically();

        // Reset pending counts
        Unsafe.getUnsafe().setMemory(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, (byte) 0);
        hasPendingData = false;
    }

    private void freeNativeBuffers() {
        if (pendingValuesAddr != 0) {
            Unsafe.free(pendingValuesAddr, (long) keyCapacity * blockValues * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            pendingValuesAddr = 0;
        }
        if (pendingCountsAddr != 0) {
            Unsafe.free(pendingCountsAddr, (long) keyCapacity * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            pendingCountsAddr = 0;
        }
        if (compressBufferAddr != 0) {
            Unsafe.free(compressBufferAddr, compressBufferSize, MemoryTag.NATIVE_DEFAULT);
            compressBufferAddr = 0;
        }
        keyCapacity = 0;
    }

    private void growKeyBuffers(int minCapacity) {
        int newCapacity = Math.max(keyCapacity * 2, minCapacity);

        long oldValSize = (long) keyCapacity * blockValues * Long.BYTES;
        long newValSize = (long) newCapacity * blockValues * Long.BYTES;
        pendingValuesAddr = Unsafe.realloc(pendingValuesAddr, oldValSize, newValSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingValuesAddr + oldValSize, newValSize - oldValSize, (byte) 0);

        long oldCountSize = (long) keyCapacity * Integer.BYTES;
        long newCountSize = (long) newCapacity * Integer.BYTES;
        pendingCountsAddr = Unsafe.realloc(pendingCountsAddr, oldCountSize, newCountSize, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(pendingCountsAddr + oldCountSize, newCountSize - oldCountSize, (byte) 0);

        keyCapacity = newCapacity;
    }

    private void updateHeaderAtomically() {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private static class TestBwdCursor implements RowCursor {
        private final LongList values;
        private int position;

        TestBwdCursor(LongList values) {
            this.values = values;
            this.position = values.size() - 1;
        }

        @Override
        public boolean hasNext() {
            return position >= 0;
        }

        @Override
        public long next() {
            return values.getQuick(position--);
        }
    }
}
