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

import static io.questdb.cairo.idx.DeltaBitmapIndexUtils.*;

/**
 * Delta-encoded bitmap index writer with linked blocks.
 * <p>
 * Combines the linked-block structure of the legacy BitmapIndexWriter
 * (for O(1) append without relocation) with delta encoding for compression.
 * <p>
 * Each key has a linked list of blocks. Each block contains:
 * - Header: next/prev pointers, count, data length, first/last values
 * - Data: delta-encoded row IDs (first value stored absolute, rest as deltas)
 * <p>
 * Supports concurrent reads while appending.
 */
public class DeltaBitmapIndexWriter implements IndexWriter {
    private static final Log LOG = LogFactory.getLog(DeltaBitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();

    private int blockCapacity;
    private int blockDataCapacity;
    private int keyCount;
    private long valueMemSize;

    public DeltaBitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @TestOnly
    public DeltaBitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        // Open existing index (false = don't init/create new)
        of(path, name, columnNameTxn, false);
    }

    /**
     * Returns a backward cursor for the given key. For testing purposes only.
     * Values are returned in descending order (most recent first).
     */
    @TestOnly
    public RowCursor getCursor(int key) {
        if (key >= keyCount || key < 0) {
            return EmptyRowCursor.INSTANCE;
        }

        LongList values = new LongList();
        long keyOffset = getKeyEntryOffset(key);
        long valueCount = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT);

        if (valueCount == 0) {
            return EmptyRowCursor.INSTANCE;
        }

        // Check countCheck matches valueCount (corrupted entry detection)
        int countCheck = keyMem.getInt(keyOffset + KEY_ENTRY_OFFSET_COUNT_CHECK);
        if (countCheck != (int) valueCount) {
            return EmptyRowCursor.INSTANCE;  // Treat as invalid/empty
        }

        long blockOffset = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_FIRST_BLOCK);

        if (blockOffset < 0 || blockOffset >= valueMemSize) {
            return EmptyRowCursor.INSTANCE;
        }

        // Decode all values from blocks
        while (blockOffset >= 0 && blockOffset + BLOCK_HEADER_SIZE <= valueMemSize) {
            int blockCount = valueMem.getInt(blockOffset + BLOCK_OFFSET_COUNT);
            int dataLen = valueMem.getInt(blockOffset + BLOCK_OFFSET_DATA_LEN);
            long firstValue = valueMem.getLong(blockOffset + BLOCK_OFFSET_FIRST_VALUE);
            long nextBlock = valueMem.getLong(blockOffset + BLOCK_OFFSET_NEXT);

            // First value in block
            values.add(firstValue);
            long currentValue = firstValue;

            // Decode deltas
            long dataOffset = blockOffset + BLOCK_HEADER_SIZE;
            long dataEnd = dataOffset + dataLen;
            // Ensure dataEnd doesn't exceed valueMemSize
            if (dataEnd > valueMemSize) {
                dataEnd = valueMemSize;
            }
            int count = 1;
            long[] result = new long[2];

            while (dataOffset < dataEnd && count < blockCount) {
                decodeDelta(valueMem, dataOffset, result);
                currentValue += result[0];
                values.add(currentValue);
                dataOffset += result[1];
                count++;
            }

            blockOffset = nextBlock;
        }

        // Return backward cursor
        return new TestBwdCursor(values);
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    /**
     * Initializes key memory for a new index with default block capacity.
     */
    public static void initKeyMemory(MemoryMA keyMem) {
        initKeyMemory(keyMem, DEFAULT_BLOCK_CAPACITY);
    }

    /**
     * Initializes key memory for a new index.
     */
    public static void initKeyMemory(MemoryMA keyMem, int blockCapacity) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(SIGNATURE);
        keyMem.skip(7); // padding to offset 8
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE_MEM_SIZE
        keyMem.putInt(blockCapacity); // BLOCK_CAPACITY
        keyMem.putInt(0); // KEY_COUNT
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE_CHECK
        keyMem.putLong(-1); // MAX_VALUE (-1 means no rows)
        keyMem.skip(KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    /**
     * Adds a key-value pair to the index.
     * Values must be added in ascending order per key.
     */
    @Override
    public void add(int key, long value) {
        if (key < 0) {
            throw CairoException.critical(0).put("index key cannot be negative [key=").put(key).put(']');
        }

        final long keyOffset = getKeyEntryOffset(key);

        if (key < keyCount) {
            // Existing key
            long valueCount = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT);

            if (valueCount == 0) {
                // Key exists but has no values (sparse key creation)
                initFirstBlock(keyOffset, value);
            } else {
                // Key has values - append to last block
                long lastBlockOffset = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK);
                appendToBlock(keyOffset, lastBlockOffset, valueCount, value);
            }
        } else {
            // New key
            initFirstBlock(keyOffset, value);
            updateKeyCount(key);
        }
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
        if (keyMem.isOpen()) {
            if (keyCount > 0) {
                keyMem.setSize(keyMemSize());
            }
            Misc.free(keyMem);
        }
        if (valueMem.isOpen()) {
            if (valueMemSize > 0) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }
        keyCount = 0;
        valueMemSize = 0;
    }

    @Override
    public void closeNoTruncate() {
        close();
    }

    @Override
    public void commit() {
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            sync(configuration.getCommitMode() == CommitMode.ASYNC);
        }
    }

    @Override
    public byte getIndexType() {
        return IndexType.DELTA;
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @Override
    public long getMaxValue() {
        return keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);
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
                // Create new index
                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                this.blockCapacity = DeltaBitmapIndexUtils.DEFAULT_BLOCK_CAPACITY;
                initKeyMemory(keyMem, blockCapacity);
                kFdUnassigned = false;
            } else {
                // Open existing
                if (!ff.exists(keyFile)) {
                    throw CairoException.critical(0).put("index does not exist [path=").put(path).put(']');
                }

                // Check file size before mapping
                long keyFileSize = ff.length(keyFile);
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                keyMem.of(ff, keyFile, configuration.getDataIndexKeyAppendPageSize(), -1L, MemoryTag.MMAP_INDEX_WRITER);
                kFdUnassigned = false;

                // Validate signature
                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid delta index signature [expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }

                // Validate sequence consistency
                long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE);
                long seqCheck = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
                if (seq != seqCheck) {
                    throw CairoException.critical(0)
                            .put("Sequence mismatch (partial write detected) [seq=").put(seq)
                            .put(", seqCheck=").put(seqCheck).put(']');
                }
            }

            // Read header
            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.blockCapacity = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_CAPACITY);
            this.blockDataCapacity = blockDataCapacity(blockCapacity);
            this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);

            // Validate keyCount matches file length (only for existing files)
            if (!init && keyCount > 0) {
                long keyFileSize = keyMem.size();
                long expectedSize = KEY_FILE_RESERVED + (long) keyCount * KEY_ENTRY_SIZE;
                if (keyFileSize < expectedSize) {
                    throw CairoException.critical(0)
                            .put("Key count does not match file length [keyCount=").put(keyCount)
                            .put(", fileSize=").put(keyFileSize)
                            .put(", expectedSize=").put(expectedSize).put(']');
                }
            }

            // Open value file
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
        } catch (Throwable e) {
            close();
            if (kFdUnassigned) {
                LOG.error().$("could not open delta index [path=").$(path).$(']').$();
            }
            throw e;
        } finally {
            path.trimTo(plen);
        }
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
                // Initialize new index
                if (ff.truncate(keyFd, 0)) {
                    kFdUnassigned = false;
                    keyMem.of(ff, keyFd, false, null, keyAppendPageSize, keyAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    this.blockCapacity = blockCapacity > 0 ? blockCapacity : DEFAULT_BLOCK_CAPACITY;
                    this.blockDataCapacity = blockDataCapacity(this.blockCapacity);
                    initKeyMemory(keyMem, this.blockCapacity);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(keyFd).put(']');
                }
            } else {
                // Open existing index
                final long keyFileSize = ff.length(keyFd);

                // Check file size before mapping
                if (keyFileSize < KEY_FILE_RESERVED) {
                    throw CairoException.critical(0)
                            .put("Index file too short [fd=").put(keyFd)
                            .put(", expected>=").put(KEY_FILE_RESERVED)
                            .put(", actual=").put(keyFileSize).put(']');
                }

                kFdUnassigned = false;
                keyMem.of(ff, keyFd, null, keyFileSize, MemoryTag.MMAP_INDEX_WRITER);

                // Validate signature
                byte sig = keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE);
                if (sig != SIGNATURE) {
                    throw CairoException.critical(0)
                            .put("Unknown format: invalid delta index signature [fd=").put(keyFd)
                            .put(", expected=").put(SIGNATURE)
                            .put(", actual=").put(sig).put(']');
                }

                // Validate sequence consistency
                long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE);
                long seqCheck = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
                if (seq != seqCheck) {
                    throw CairoException.critical(0)
                            .put("Sequence mismatch [fd=").put(keyFd)
                            .put(", seq=").put(seq)
                            .put(", seqCheck=").put(seqCheck).put(']');
                }

                // Read header
                this.keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
                this.blockCapacity = keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_CAPACITY);
                this.blockDataCapacity = blockDataCapacity(this.blockCapacity);

                // Validate keyCount matches file length
                if (keyCount > 0) {
                    long expectedSize = KEY_FILE_RESERVED + (long) keyCount * KEY_ENTRY_SIZE;
                    if (keyFileSize < expectedSize) {
                        throw CairoException.critical(0)
                                .put("Key count does not match file length [fd=").put(keyFd)
                                .put(", keyCount=").put(keyCount)
                                .put(", fileSize=").put(keyFileSize)
                                .put(", expectedSize=").put(expectedSize).put(']');
                    }
                }
            }

            this.valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

            if (init) {
                // Initialize value file
                if (ff.truncate(valueFd, 0)) {
                    vFdUnassigned = false;
                    valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    valueMem.jumpTo(0);
                    valueMemSize = 0;
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(valueFd).put(']');
                }
            } else {
                // Open existing value file
                vFdUnassigned = false;
                valueMem.of(ff, valueFd, false, null, valueAppendPageSize, valueMemSize, MemoryTag.MMAP_INDEX_WRITER);
                if (valueMemSize > 0) {
                    valueMem.jumpTo(valueMemSize);
                }
            }
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

    @Override
    public void rollbackConditionally(long row) {
        long maxValue = getMaxValue();
        if (maxValue > row) {
            rollbackValues(row);
        }
    }

    @Override
    public void rollbackValues(long maxValue) {
        // For each key, walk blocks backwards to find values <= maxValue
        for (int key = 0; key < keyCount; key++) {
            final long keyOffset = getKeyEntryOffset(key);
            long valueCount = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT);

            if (valueCount == 0) {
                continue;
            }

            long lastBlockOffset = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK);
            long lastValue = valueMem.getLong(lastBlockOffset + BLOCK_OFFSET_LAST_VALUE);

            if (lastValue <= maxValue) {
                continue; // No rollback needed for this key
            }

            // Need to rollback - walk blocks backwards
            rollbackKey(keyOffset, maxValue);
        }

        // Recalculate valueMemSize based on remaining blocks
        recalculateValueMemSize();
        updateValueMemSize();

        // Recalculate keyCount - find highest key with values
        recalculateKeyCount();

        setMaxValue(maxValue);
    }

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

    public void truncate() {
        initKeyMemory(keyMem, blockCapacity);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
    }

    private void appendToBlock(long keyOffset, long blockOffset, long totalValueCount, long value) {
        // Get current block state
        int blockCount = valueMem.getInt(blockOffset + BLOCK_OFFSET_COUNT);
        int dataLen = valueMem.getInt(blockOffset + BLOCK_OFFSET_DATA_LEN);
        long lastValue = valueMem.getLong(blockOffset + BLOCK_OFFSET_LAST_VALUE);

        // Validate ordering
        if (value < lastValue) {
            throw CairoException.critical(0)
                    .put("index values must be added in ascending order [lastValue=")
                    .put(lastValue).put(", newValue=").put(value).put(']');
        }

        long delta = value - lastValue;
        int deltaSize = encodedSize(delta);

        // Check if delta fits in current block
        if (dataLen + deltaSize <= blockDataCapacity) {
            // Append delta to current block
            long dataOffset = blockOffset + BLOCK_HEADER_SIZE + dataLen;
            valueMem.jumpTo(dataOffset);
            encodeDelta(valueMem, delta);

            // Update block header
            valueMem.putInt(blockOffset + BLOCK_OFFSET_COUNT, blockCount + 1);
            valueMem.putInt(blockOffset + BLOCK_OFFSET_DATA_LEN, dataLen + deltaSize);
            valueMem.putLong(blockOffset + BLOCK_OFFSET_LAST_VALUE, value);

            // Update key entry atomically
            Unsafe.getUnsafe().storeFence();
            keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT, totalValueCount + 1);
            Unsafe.getUnsafe().storeFence();
            keyMem.putInt(keyOffset + KEY_ENTRY_OFFSET_COUNT_CHECK, (int) (totalValueCount + 1));
        } else {
            // Block is full - allocate new block
            allocateNewBlock(keyOffset, blockOffset, totalValueCount, value);
        }
    }

    private void allocateNewBlock(long keyOffset, long prevBlockOffset, long totalValueCount, long value) {
        // Allocate new block at end of value file
        long newBlockOffset = valueMemSize;
        valueMem.jumpTo(newBlockOffset);

        // Write block header
        valueMem.putLong(-1L);  // next = none
        valueMem.putLong(prevBlockOffset);  // prev
        valueMem.putInt(1);  // count = 1
        valueMem.putInt(0);  // dataLen = 0 (first value stored separately)
        valueMem.putLong(value);  // firstValue
        valueMem.putLong(value);  // lastValue

        // Reserve space for data area
        valueMem.skip(blockDataCapacity);

        // Update previous block's next pointer
        valueMem.putLong(prevBlockOffset + BLOCK_OFFSET_NEXT, newBlockOffset);

        valueMemSize += blockCapacity;
        updateValueMemSize();

        // Update key entry atomically
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT, totalValueCount + 1);
        keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK, newBlockOffset);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(keyOffset + KEY_ENTRY_OFFSET_COUNT_CHECK, (int) (totalValueCount + 1));
    }

    private void initFirstBlock(long keyOffset, long value) {
        // Allocate first block for this key
        long blockOffset = valueMemSize;
        valueMem.jumpTo(blockOffset);

        // Write block header
        valueMem.putLong(-1L);  // next = none
        valueMem.putLong(-1L);  // prev = none (first block)
        valueMem.putInt(1);  // count = 1
        valueMem.putInt(0);  // dataLen = 0 (first value stored separately)
        valueMem.putLong(value);  // firstValue
        valueMem.putLong(value);  // lastValue

        // Reserve space for data area
        valueMem.skip(blockDataCapacity);

        valueMemSize += blockCapacity;
        updateValueMemSize();

        // Update key entry atomically
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT, 1);
        keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_FIRST_BLOCK, blockOffset);
        keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK, blockOffset);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(keyOffset + KEY_ENTRY_OFFSET_COUNT_CHECK, 1);
    }

    private long keyMemSize() {
        return KEY_FILE_RESERVED + (long) keyCount * KEY_ENTRY_SIZE;
    }

    private void recalculateKeyCount() {
        // Only reset keyCount to 0 if ALL keys have no values
        // Otherwise keep the high water mark (sparse key support)
        boolean hasAnyValues = false;
        for (int key = 0; key < keyCount; key++) {
            long keyOffset = getKeyEntryOffset(key);
            long valueCount = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT);
            if (valueCount > 0) {
                hasAnyValues = true;
                break;
            }
        }
        if (!hasAnyValues) {
            keyCount = 0;
            keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        }
    }

    private void recalculateValueMemSize() {
        // Find the maximum block offset + blockCapacity among all keys
        long maxBlockEnd = 0;
        for (int key = 0; key < keyCount; key++) {
            long keyOffset = getKeyEntryOffset(key);
            long valueCount = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT);
            if (valueCount > 0) {
                long lastBlock = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK);
                long blockEnd = lastBlock + blockCapacity;
                if (blockEnd > maxBlockEnd) {
                    maxBlockEnd = blockEnd;
                }
            }
        }
        valueMemSize = maxBlockEnd;
    }

    private void rollbackKey(long keyOffset, long maxValue) {
        long blockOffset = keyMem.getLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK);
        long newValueCount = 0;
        long newLastBlock = -1;

        // Walk backwards through blocks
        while (blockOffset >= 0) {
            long firstValue = valueMem.getLong(blockOffset + BLOCK_OFFSET_FIRST_VALUE);

            if (firstValue <= maxValue) {
                // This block contains valid values - scan to find cutoff
                int blockCount = valueMem.getInt(blockOffset + BLOCK_OFFSET_COUNT);
                int dataLen = valueMem.getInt(blockOffset + BLOCK_OFFSET_DATA_LEN);

                // Decode values to find how many are <= maxValue
                long currentValue = firstValue;
                int validCount = 1;  // firstValue is valid if firstValue <= maxValue
                int validDataLen = 0;
                long validLastValue = firstValue;

                long dataOffset = blockOffset + BLOCK_HEADER_SIZE;
                long dataEnd = dataOffset + dataLen;
                long[] result = new long[2];

                while (dataOffset < dataEnd && validCount < blockCount) {
                    decodeDelta(valueMem, dataOffset, result);
                    long delta = result[0];
                    int bytesConsumed = (int) result[1];

                    currentValue += delta;
                    if (currentValue <= maxValue) {
                        validCount++;
                        validDataLen += bytesConsumed;
                        validLastValue = currentValue;
                        dataOffset += bytesConsumed;
                    } else {
                        break;
                    }
                }

                if (validCount > 0) {
                    // Update this block with truncated data
                    valueMem.putInt(blockOffset + BLOCK_OFFSET_COUNT, validCount);
                    valueMem.putInt(blockOffset + BLOCK_OFFSET_DATA_LEN, validDataLen);
                    valueMem.putLong(blockOffset + BLOCK_OFFSET_LAST_VALUE, validLastValue);
                    valueMem.putLong(blockOffset + BLOCK_OFFSET_NEXT, -1L);  // This becomes last block

                    newValueCount += validCount;
                    newLastBlock = blockOffset;

                    // Add values from previous blocks
                    long prevBlock = valueMem.getLong(blockOffset + BLOCK_OFFSET_PREV);
                    while (prevBlock >= 0) {
                        newValueCount += valueMem.getInt(prevBlock + BLOCK_OFFSET_COUNT);
                        prevBlock = valueMem.getLong(prevBlock + BLOCK_OFFSET_PREV);
                    }
                    break;
                }
            }

            // Move to previous block
            blockOffset = valueMem.getLong(blockOffset + BLOCK_OFFSET_PREV);
        }

        // Update key entry
        if (newLastBlock >= 0) {
            keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT, newValueCount);
            keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK, newLastBlock);
            keyMem.putInt(keyOffset + KEY_ENTRY_OFFSET_COUNT_CHECK, (int) newValueCount);
        } else {
            // All values rolled back
            keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_VALUE_COUNT, 0);
            keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_FIRST_BLOCK, -1L);
            keyMem.putLong(keyOffset + KEY_ENTRY_OFFSET_LAST_BLOCK, -1L);
            keyMem.putInt(keyOffset + KEY_ENTRY_OFFSET_COUNT_CHECK, 0);
        }
    }

    private void updateKeyCount(int key) {
        if (key >= keyCount) {
            keyCount = key + 1;
            // Update header atomically with sequence
            updateHeaderAtomically();
        }
    }

    private void updateValueMemSize() {
        // Update header atomically with sequence
        updateHeaderAtomically();
    }

    private void updateHeaderAtomically() {
        // Increment sequence, update data, set sequence check
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    /**
     * Simple backward cursor for testing. Returns values in descending order.
     */
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
