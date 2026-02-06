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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

/**
 * Writer for Frame of Reference (FOR) bitmap index.
 * <p>
 * FOR compression stores row IDs in fixed-size blocks, achieving excellent compression
 * for sequential data while enabling fast SIMD-friendly decoding.
 */
public class FORBitmapIndexWriter implements IndexWriter {
    private static final Log LOG = LogFactory.getLog(FORBitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final IntList keyBlockCounts = new IntList();
    private final LongList keyFirstBlockOffsets = new LongList();
    private final LongList keyLastValues = new LongList();
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    // Cached key metadata
    private final LongList keyValueCounts = new LongList();
    // Per-key pending block buffer
    private final LongList pendingValues = new LongList(FORBitmapIndexUtils.BLOCK_CAPACITY);
    private final MemoryMARW valueMem = Vm.getCMARWInstance();
    // Current key being written to
    private int currentKey = -1;
    private long currentKeyOffset = -1;

    private int keyCount = -1;
    private long valueMemSize = -1;

    @TestOnly
    public FORBitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
    }

    public FORBitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Initializes key memory for a new FOR index.
     */
    public static void initKeyMemory(MemoryMA keyMem) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        // Offset 0: Signature (1 byte) + 7 bytes padding
        keyMem.putByte(FORBitmapIndexUtils.SIGNATURE);
        keyMem.skip(7);
        // Offset 8: Sequence
        keyMem.putLong(1);
        Unsafe.getUnsafe().storeFence();
        // Offset 16: Value mem size
        keyMem.putLong(0);
        // Offset 24: Key count (4 bytes) + 4 bytes padding
        keyMem.putInt(0);
        keyMem.skip(4);
        Unsafe.getUnsafe().storeFence();
        // Offset 32: Sequence check
        keyMem.putLong(1);
        // Offset 40: Max value
        keyMem.putLong(-1);
        // Offset 48-63: Reserved
        keyMem.skip(FORBitmapIndexUtils.KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    /**
     * Adds a key-value pair to the index. Values for the same key must be added in ascending order.
     */
    public void add(int key, long value) {
        assert key >= 0 : "key must be non-negative: " + key;

        // If switching to a different key, flush pending values for current key
        if (key != currentKey && currentKey >= 0 && pendingValues.size() > 0) {
            flushPendingBlock();
        }

        // Ensure key exists
        if (key >= keyCount) {
            // New key
            extendCachesForKey(key);
            keyCount = key + 1;
            updateKeyCount();
        }

        currentKey = key;
        currentKeyOffset = FORBitmapIndexUtils.getKeyEntryOffset(key);

        // Add to pending buffer
        pendingValues.add(value);

        // Update last value
        keyLastValues.setQuick(key, value);

        // Flush if block is full
        if (pendingValues.size() >= FORBitmapIndexUtils.BLOCK_CAPACITY) {
            flushPendingBlock();
        }
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
        // Flush any pending values
        if (currentKey >= 0 && pendingValues.size() > 0) {
            flushPendingBlock();
        }

        if (keyMem.isOpen()) {
            if (keyCount > -1) {
                keyMem.setSize(keyMemSize());
            }
            Misc.free(keyMem);
        }

        if (valueMem.isOpen()) {
            if (valueMemSize > -1) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }

        clearCaches();
    }

    public void commit() {
        // Flush pending values before commit
        if (currentKey >= 0 && pendingValues.size() > 0) {
            flushPendingBlock();
        }

        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            sync(commitMode == CommitMode.ASYNC);
        }
    }

    @Override
    public byte getIndexType() {
        return IndexType.FOR;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public long getMaxValue() {
        return keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_MAX_VALUE);
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    public boolean isOpen() {
        return keyMem.isOpen();
    }

    @Override
    public void closeNoTruncate() {
        // Flush pending before closing
        if (currentKey >= 0 && pendingValues.size() > 0) {
            flushPendingBlock();
        }
        keyMem.close(false);
        valueMem.close(false);
        clearCaches();
    }

    @Override
    public void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean init, int blockCapacity) {
        close();
        final FilesFacade ff = configuration.getFilesFacade();
        boolean kFdUnassigned = true;
        boolean vFdUnassigned = true;
        final long keyAppendPageSize = configuration.getDataIndexKeyAppendPageSize();
        final long valueAppendPageSize = configuration.getDataIndexValueAppendPageSize();
        try {
            if (init) {
                if (ff.truncate(keyFd, 0)) {
                    kFdUnassigned = false;
                    keyMem.of(ff, keyFd, false, null, keyAppendPageSize, keyAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    initKeyMemory(keyMem);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(keyFd).put(']');
                }
            } else {
                final long keyFileSize = ff.length(keyFd);
                kFdUnassigned = false;
                keyMem.of(ff, keyFd, null, keyFileSize, MemoryTag.MMAP_INDEX_WRITER);
            }
            long keyMemSize = keyMem.getAppendOffset();
            if (keyMemSize < FORBitmapIndexUtils.KEY_FILE_RESERVED) {
                keyMem.close(false);
                LOG.error().$("file too short [corrupt] [fd=").$(keyFd).I$();
                throw CairoException.critical(0).put("Index file too short (w): [fd=").put(keyFd).put(']');
            }

            if (keyMem.getByte(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != FORBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] [fd=").$(keyFd).I$();
                throw CairoException.critical(0).put("Unknown format: [fd=").put(keyFd).put(']');
            }

            this.keyCount = keyMem.getInt(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
            if (keyMemSize < keyMemSize()) {
                LOG.error().$("key count does not match file length [corrupt] [fd=").$(keyFd).$(", keyCount=").$(keyCount).I$();
                throw CairoException.critical(0).put("Key count does not match file length [fd=").put(keyFd).put(']');
            }

            if (keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) != keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                LOG.error().$("sequence mismatch [corrupt] at [fd=").$(keyFd).I$();
                throw CairoException.critical(0).put("Sequence mismatch [fd=").put(keyFd).put(']');
            }

            this.valueMemSize = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

            if (init) {
                if (ff.truncate(valueFd, 0)) {
                    vFdUnassigned = false;
                    valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    valueMem.jumpTo(0);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(valueFd).put(']');
                }
            } else {
                vFdUnassigned = false;
                valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueMemSize, MemoryTag.MMAP_INDEX_WRITER);
            }

            rebuildCaches();
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                ff.close(keyFd);
            }
            if (vFdUnassigned) {
                ff.close(valueFd);
            }
            throw e;
        }
    }

    public final void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    public final void of(Path path, CharSequence name, long columnNameTxn, boolean create) {
        close();
        final int plen = path.size();
        try {
            LPSZ keyFile = FORBitmapIndexUtils.keyFileName(path, name, columnNameTxn);

            if (create) {
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                initKeyMemory(keyMem);
            } else {
                boolean exists = ff.exists(keyFile);
                if (!exists) {
                    LOG.error().$(path).$(" not found").$();
                    throw CairoException.fileNotFound().put("index does not exist [path=").put(path).put(']');
                }
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), ff.length(keyFile), MemoryTag.MMAP_INDEX_WRITER);
            }

            long keyMemSize = keyMem.getAppendOffset();
            if (keyMemSize < FORBitmapIndexUtils.KEY_FILE_RESERVED) {
                LOG.error().$("file too short [corrupt] [path=").$(path).I$();
                throw CairoException.critical(0).put("Index file too short (w): ").put(path);
            }

            if (keyMem.getByte(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != FORBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            this.keyCount = keyMem.getInt(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
            if (keyMemSize < keyMemSize()) {
                LOG.error().$("key count does not match file length [corrupt] of ").$(path).$(" [keyCount=").$(keyCount).I$();
                throw CairoException.critical(0).put("Key count does not match file length of ").put(path);
            }

            if (keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) != keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                LOG.error().$("sequence mismatch [corrupt] at ").$(path).$();
                throw CairoException.critical(0).put("Sequence mismatch on ").put(path);
            }

            this.valueMemSize = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            valueMem.of(
                    ff,
                    FORBitmapIndexUtils.valueFileName(path.trimTo(plen), name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    this.valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (create) {
                assert valueMemSize == 0;
                valueMem.truncate();
            }

            // Rebuild caches from existing data
            rebuildCaches();

            currentKey = -1;
            pendingValues.clear();

        } catch (Throwable e) {
            this.close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public void setMaxValue(long maxValue) {
        keyMem.putLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
    }

    @Override
    public void rollbackConditionally(long row) {
        final long currentMaxRow;
        if (row >= 0 && ((currentMaxRow = getMaxValue()) < 1 || currentMaxRow >= row)) {
            if (row == 0) {
                truncate();
            } else {
                rollbackValues(row - 1);
            }
        }
    }

    @Override
    public void rollbackValues(long maxValue) {
        // Flush any pending values first
        if (currentKey >= 0 && pendingValues.size() > 0) {
            flushPendingBlock();
        }

        // FOR index stores fixed-size blocks without next-block pointers.
        // Each key has: valueCount, firstBlockOffset, lastValue, blockCount
        // Blocks are stored contiguously: minValue(8), bitWidth(1), valueCount(2), padding(1), packed data
        // We need to read all values, filter, and rewrite.

        long newValueMemSize = 0;

        for (int k = 0; k < keyCount; k++) {
            long offset = FORBitmapIndexUtils.getKeyEntryOffset(k);
            int blockCount = keyMem.getInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_BLOCK_COUNT);

            if (blockCount > 0) {
                // Read all values for this key and filter
                LongList values = new LongList();
                long dataOffset = keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK);

                // Iterate through blocks (contiguous, not linked)
                for (int b = 0; b < blockCount; b++) {
                    // Read block header
                    long minVal = valueMem.getLong(dataOffset + FORBitmapIndexUtils.BLOCK_OFFSET_MIN_VALUE);
                    int bitWidth = valueMem.getByte(dataOffset + FORBitmapIndexUtils.BLOCK_OFFSET_BIT_WIDTH) & 0xFF;
                    int valueCount = valueMem.getShort(dataOffset + FORBitmapIndexUtils.BLOCK_OFFSET_VALUE_COUNT) & 0xFFFF;

                    // Unpack values from this block
                    long[] blockValues = new long[valueCount];
                    FORBitmapIndexUtils.unpackAllValues(
                            valueMem.addressOf(dataOffset + FORBitmapIndexUtils.BLOCK_OFFSET_DATA),
                            valueCount, bitWidth, minVal, blockValues
                    );

                    for (int i = 0; i < valueCount; i++) {
                        if (blockValues[i] <= maxValue) {
                            values.add(blockValues[i]);
                        }
                    }

                    // Move to next block
                    int blockSize = FORBitmapIndexUtils.blockSize(valueCount, bitWidth);
                    dataOffset += blockSize;
                }

                // Rewrite this key's blocks with filtered values
                if (values.size() > 0) {
                    long firstBlockOffset = newValueMemSize;
                    int newBlockCount = 0;
                    long lastVal = 0;

                    for (int i = 0; i < values.size(); i += FORBitmapIndexUtils.BLOCK_CAPACITY) {
                        int count = Math.min(FORBitmapIndexUtils.BLOCK_CAPACITY, values.size() - i);

                        // Find min/max in this block
                        long minVal = values.getQuick(i);
                        long maxVal = values.getQuick(i);
                        for (int j = 1; j < count; j++) {
                            long val = values.getQuick(i + j);
                            minVal = Math.min(minVal, val);
                            maxVal = Math.max(maxVal, val);
                        }

                        // Calculate bit width
                        long maxOffset = maxVal - minVal;
                        int bitWidth = FORBitmapIndexUtils.bitsNeeded(maxOffset);
                        int packedSize = FORBitmapIndexUtils.packedDataSize(count, bitWidth);
                        int blockSize = FORBitmapIndexUtils.BLOCK_HEADER_SIZE + packedSize;

                        // Write block at newValueMemSize
                        valueMem.jumpTo(newValueMemSize);
                        valueMem.putLong(minVal);
                        valueMem.putByte((byte) bitWidth);
                        valueMem.putShort((short) count);
                        valueMem.putByte((byte) 0); // padding

                        // Pack and write values
                        long[] blockVals = new long[count];
                        for (int j = 0; j < count; j++) {
                            blockVals[j] = values.getQuick(i + j);
                            lastVal = blockVals[j];
                        }

                        valueMem.skip(packedSize);
                        FORBitmapIndexUtils.packValues(blockVals, count, minVal, bitWidth,
                                valueMem.addressOf(newValueMemSize + FORBitmapIndexUtils.BLOCK_OFFSET_DATA));

                        newValueMemSize += blockSize;
                        newBlockCount++;
                    }

                    // Update key entry
                    Unsafe.getUnsafe().storeFence();
                    keyMem.putLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, values.size());
                    keyMem.putLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK, firstBlockOffset);
                    keyMem.putLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, lastVal);
                    keyMem.putInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_BLOCK_COUNT, newBlockCount);
                    Unsafe.getUnsafe().storeFence();
                    keyMem.putInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, values.size());
                } else {
                    // Clear this key
                    keyMem.putLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 0);
                    keyMem.putLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK, 0);
                    keyMem.putLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, Long.MIN_VALUE);
                    keyMem.putInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_BLOCK_COUNT, 0);
                    keyMem.putInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 0);
                }
            }
        }

        valueMemSize = newValueMemSize;
        updateValueMemSize();
        setMaxValue(maxValue);
        rebuildCaches();
    }

    public void sync(boolean async) {
        keyMem.sync(async);
        valueMem.sync(async);
    }

    public void truncate() {
        // Flush pending first
        if (currentKey >= 0 && pendingValues.size() > 0) {
            flushPendingBlock();
        }

        initKeyMemory(keyMem);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        clearCaches();
        currentKey = -1;
        pendingValues.clear();
    }

    private void clearCaches() {
        keyValueCounts.clear();
        keyFirstBlockOffsets.clear();
        keyLastValues.clear();
        keyBlockCounts.clear();
        pendingValues.clear();
    }

    private void extendCachesForKey(int key) {
        while (keyValueCounts.size() <= key) {
            keyValueCounts.add(0);
            keyFirstBlockOffsets.add(-1);
            keyLastValues.add(Long.MIN_VALUE);
            keyBlockCounts.add(0);
        }
    }

    private void flushPendingBlock() {
        if (pendingValues.size() == 0) {
            return;
        }

        int count = pendingValues.size();
        long minValue = pendingValues.getQuick(0);
        long maxValue = pendingValues.getQuick(count - 1);

        // Calculate bit width needed
        long maxOffset = maxValue - minValue;
        int bitWidth = FORBitmapIndexUtils.bitsNeeded(maxOffset);

        // Calculate block size
        int packedSize = FORBitmapIndexUtils.packedDataSize(count, bitWidth);
        int blockSize = FORBitmapIndexUtils.BLOCK_HEADER_SIZE + packedSize;

        // Write block at end of value file
        long blockOffset = valueMemSize;
        valueMem.jumpTo(blockOffset);

        // Write header
        valueMem.putLong(minValue);
        valueMem.putByte((byte) bitWidth);
        valueMem.putShort((short) count);
        valueMem.putByte((byte) 0); // padding

        // Pack and write values
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = pendingValues.getQuick(i);
        }

        // Ensure memory is available BEFORE getting the address
        valueMem.skip(packedSize);
        long dataAddr = valueMem.addressOf(blockOffset + FORBitmapIndexUtils.BLOCK_OFFSET_DATA);
        FORBitmapIndexUtils.packValues(values, count, minValue, bitWidth, dataAddr);

        valueMemSize = valueMem.getAppendOffset();

        // Update key entry
        long keyOffset = currentKeyOffset;
        long prevValueCount = keyValueCounts.getQuick(currentKey);
        int prevBlockCount = keyBlockCounts.getQuick(currentKey);

        if (prevBlockCount == 0) {
            // First block for this key
            keyFirstBlockOffsets.setQuick(currentKey, blockOffset);
        }

        long newValueCount = prevValueCount + count;
        int newBlockCount = prevBlockCount + 1;

        // Update caches
        keyValueCounts.setQuick(currentKey, newValueCount);
        keyBlockCounts.setQuick(currentKey, newBlockCount);

        // Write to mmap
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(keyOffset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, newValueCount);
        keyMem.putLong(keyOffset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK, keyFirstBlockOffsets.getQuick(currentKey));
        keyMem.putLong(keyOffset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, keyLastValues.getQuick(currentKey));
        keyMem.putInt(keyOffset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_BLOCK_COUNT, newBlockCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(keyOffset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, (int) newValueCount);

        updateValueMemSize();

        pendingValues.clear();
    }

    private long keyMemSize() {
        return (long) this.keyCount * FORBitmapIndexUtils.KEY_ENTRY_SIZE + FORBitmapIndexUtils.KEY_FILE_RESERVED;
    }

    private void rebuildCaches() {
        clearCaches();
        for (int k = 0; k < keyCount; k++) {
            long offset = FORBitmapIndexUtils.getKeyEntryOffset(k);
            long valueCount = keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
            long firstBlockOffset = keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK);
            long lastValue = keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE);
            int blockCount = keyMem.getInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_BLOCK_COUNT);

            keyValueCounts.add(valueCount);
            keyFirstBlockOffsets.add(firstBlockOffset);
            keyLastValues.add(lastValue);
            keyBlockCounts.add(blockCount);
        }
    }

    private void updateKeyCount() {
        long seq = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private void updateValueMemSize() {
        long seq = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }
}
