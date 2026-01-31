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
import io.questdb.cairo.sql.RowCursor;
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
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Writer for delta-encoded bitmap index.
 * <p>
 * Delta encoding achieves 2-4x compression for sequential row IDs common in time-series data.
 * Values for each key are stored as: first_value (8 bytes) followed by delta-encoded differences.
 */
public class DeltaBitmapIndexWriter implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(DeltaBitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final Cursor cursor = new Cursor();
    private final FilesFacade ff;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final IntList keyDataLens = new IntList();
    // Cached key entry metadata to avoid mmap reads on hot path
    private final LongList keyDataOffsets = new LongList();
    private final LongList keyValueCounts = new LongList();
    // Cached state for each key: last value written (for delta calculation)
    private final LongList lastValues = new LongList();
    // Reusable list for rollback operations to avoid allocations
    private final LongList rollbackValues = new LongList();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();
    private int keyCount = -1;
    private long valueMemSize = -1;

    @TestOnly
    public DeltaBitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn);
    }

    public DeltaBitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    /**
     * Initializes key memory for a new delta-encoded index.
     * Header is 8-byte aligned for better performance.
     */
    public static void initKeyMemory(MemoryMA keyMem) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        // Offset 0: Signature (1 byte) + 7 bytes padding
        keyMem.putByte(DeltaBitmapIndexUtils.SIGNATURE);
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
        keyMem.skip(DeltaBitmapIndexUtils.KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    /**
     * Adds a key-value pair to the index. Values for the same key must be added in ascending order.
     *
     * @param key   the index key (must be non-negative)
     * @param value the row ID value to add
     */
    public void add(int key, long value) {
        assert key >= 0 : "key must be non-negative: " + key;

        final long keyOffset = DeltaBitmapIndexUtils.getKeyEntryOffset(key);

        if (key < keyCount) {
            // Existing key - use cached metadata
            long valueCount = keyValueCounts.getQuick(key);
            if (valueCount > 0) {
                // Append delta-encoded value
                appendDeltaEncodedValue(keyOffset, key, value);
            } else {
                // Key exists but has no values yet (created as byproduct of sparse key)
                initValueDataAndStoreValue(keyOffset, key, value);
            }
        } else {
            // New key - initialize value data and update header atomically
            initValueDataForNewKey(keyOffset, key, value);
        }
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
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

    public void closeNoTruncate() {
        keyMem.close(false);
        valueMem.close(false);
        clearCaches();
    }

    public void commit() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            sync(commitMode == CommitMode.ASYNC);
        }
    }

    public RowCursor getCursor(int key) {
        if (key < keyCount) {
            // Use cached metadata
            long valueCount = keyValueCounts.getQuick(key);
            if (valueCount > 0) {
                cursor.of(key);
                return cursor;
            }
        }
        return EmptyRowCursor.INSTANCE;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public long getMaxValue() {
        return keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_MAX_VALUE);
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    public boolean isOpen() {
        return keyMem.isOpen();
    }

    public final void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    public final void of(Path path, CharSequence name, long columnNameTxn, boolean create) {
        close();
        final int plen = path.size();
        try {
            LPSZ keyFile = DeltaBitmapIndexUtils.keyFileName(path, name, columnNameTxn);

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
            if (keyMemSize < DeltaBitmapIndexUtils.KEY_FILE_RESERVED) {
                LOG.error().$("file too short [corrupt] [path=").$(path).I$();
                throw CairoException.critical(0).put("Index file too short (w): ").put(path);
            }

            if (keyMem.getByte(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != DeltaBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            this.keyCount = keyMem.getInt(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
            if (keyMemSize < keyMemSize()) {
                LOG.error().$("key count does not match file length [corrupt] of ").$(path).$(" [keyCount=").$(keyCount).I$();
                throw CairoException.critical(0).put("Key count does not match file length of ").put(path);
            }

            if (keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) != keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                LOG.error().$("sequence mismatch [corrupt] at ").$(path).$();
                throw CairoException.critical(0).put("Sequence mismatch on ").put(path);
            }

            this.valueMemSize = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            valueMem.of(
                    ff,
                    DeltaBitmapIndexUtils.valueFileName(path.trimTo(plen), name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    this.valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (create) {
                assert valueMemSize == 0;
                valueMem.truncate();
            }

            // Rebuild all caches from existing data
            rebuildCaches();

        } catch (Throwable e) {
            this.close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

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

    /**
     * Rolls back values to remove entries strictly greater than maxValue.
     * This requires re-reading and re-encoding the data for affected keys.
     * Note: This operation may leave garbage data in the value file (fragmentation).
     */
    public void rollbackValues(long maxValue) {
        long newValueMemSize = 0;

        for (int k = 0; k < keyCount; k++) {
            long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(k);
            // Read from cache
            long valueCount = keyValueCounts.getQuick(k);

            if (valueCount > 0) {
                long dataOffset = keyDataOffsets.getQuick(k);
                int dataLen = keyDataLens.getQuick(k);

                // Decode all values for this key (reusing pre-allocated list)
                decodeAllValues(dataOffset, dataLen, valueCount, rollbackValues);

                // Find how many values to keep
                long keepCount = 0;
                for (long i = 0; i < rollbackValues.size(); i++) {
                    if (rollbackValues.getQuick((int) i) <= maxValue) {
                        keepCount++;
                    } else {
                        break; // Values are ordered, so we can stop here
                    }
                }

                if (keepCount != valueCount) {
                    // Need to re-encode with fewer values
                    if (keepCount == 0) {
                        // Clear this key's data
                        keyMem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 0);
                        keyMem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, 0);
                        keyMem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, 0);
                        keyMem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 0);
                        // Update caches
                        keyDataOffsets.setQuick(k, 0);
                        keyDataLens.setQuick(k, 0);
                        keyValueCounts.setQuick(k, 0);
                        lastValues.setQuick(k, Long.MIN_VALUE);
                    } else {
                        // Re-encode with kept values at newValueMemSize
                        long newDataOffset = newValueMemSize;
                        valueMem.jumpTo(newValueMemSize);

                        // Write first value
                        long firstValue = rollbackValues.getQuick(0);
                        valueMem.putLong(firstValue);
                        long lastValue = firstValue;
                        int bytesWritten = 8;

                        // Write deltas
                        for (int i = 1; i < keepCount; i++) {
                            long val = rollbackValues.getQuick(i);
                            long delta = val - lastValue;
                            bytesWritten += DeltaBitmapIndexUtils.encodeDelta(valueMem, delta);
                            lastValue = val;
                        }

                        newValueMemSize = valueMem.getAppendOffset();

                        // Update key entry with reduced fences
                        Unsafe.getUnsafe().storeFence();
                        keyMem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, keepCount);
                        keyMem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, newDataOffset);
                        keyMem.putLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, lastValue);
                        keyMem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, bytesWritten);
                        Unsafe.getUnsafe().storeFence();
                        keyMem.putInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, (int) keepCount);

                        // Update caches
                        keyDataOffsets.setQuick(k, newDataOffset);
                        keyDataLens.setQuick(k, bytesWritten);
                        keyValueCounts.setQuick(k, keepCount);
                        lastValues.setQuick(k, lastValue);
                    }
                } else {
                    // Keep track of data end for this key
                    if (dataOffset + dataLen > newValueMemSize) {
                        newValueMemSize = dataOffset + dataLen;
                    }
                }
            }
        }

        valueMemSize = newValueMemSize;
        updateValueMemSize();
        setMaxValue(maxValue);
    }

    public void setMaxValue(long maxValue) {
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
    }

    public void sync(boolean async) {
        keyMem.sync(async);
        valueMem.sync(async);
    }

    public void truncate() {
        initKeyMemory(keyMem);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        clearCaches();
    }

    private void appendDeltaEncodedValue(long keyOffset, int key, long value) {
        // Get last value for this key to compute delta (from cache)
        long lastValue = lastValues.getQuick(key);
        assert value >= lastValue : "values must be added in ascending order";

        long delta = value - lastValue;
        int deltaSize = DeltaBitmapIndexUtils.encodedSize(delta);

        // Get current data info from cache
        long valueCount = keyValueCounts.getQuick(key);
        int dataLen = keyDataLens.getQuick(key);
        long dataOffset = keyDataOffsets.getQuick(key);

        // Check if this key's data ends at the current valueMemSize
        // If so, we can append in place. Otherwise, we need to relocate the data.
        // Note: The slow path leaves the old data as garbage (fragmentation trade-off).
        long dataEnd = dataOffset + dataLen;
        if (dataEnd == valueMemSize) {
            // Fast path: data is at the end, can append in place
            valueMem.jumpTo(dataEnd);
            DeltaBitmapIndexUtils.encodeDelta(valueMem, delta);

            int newDataLen = dataLen + deltaSize;
            long newValueCount = valueCount + 1;
            valueMemSize = valueMem.getAppendOffset();
            updateValueMemSize();

            // Update key entry atomically with reduced fences
            // Reader checks: countCheck == (int)valueCount, so we update countCheck last
            Unsafe.getUnsafe().storeFence();
            keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, newValueCount);
            keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, value);
            keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, newDataLen);
            Unsafe.getUnsafe().storeFence();
            keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, (int) newValueCount);

            // Update caches
            keyDataLens.setQuick(key, newDataLen);
            keyValueCounts.setQuick(key, newValueCount);
        } else {
            // Slow path: need to relocate data to the end
            long newDataOffset = valueMemSize;
            valueMem.jumpTo(newDataOffset);

            // Copy existing encoded data
            for (long i = 0; i < dataLen; i++) {
                valueMem.putByte(valueMem.getByte(dataOffset + i));
            }

            // Append new delta
            DeltaBitmapIndexUtils.encodeDelta(valueMem, delta);

            int newDataLen = dataLen + deltaSize;
            long newValueCount = valueCount + 1;
            valueMemSize = valueMem.getAppendOffset();
            updateValueMemSize();

            // Update key entry atomically with reduced fences
            Unsafe.getUnsafe().storeFence();
            keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, newValueCount);
            keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, newDataOffset);
            keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, value);
            keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, newDataLen);
            Unsafe.getUnsafe().storeFence();
            keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, (int) newValueCount);

            // Update caches
            keyDataOffsets.setQuick(key, newDataOffset);
            keyDataLens.setQuick(key, newDataLen);
            keyValueCounts.setQuick(key, newValueCount);
        }

        // Update last value cache
        lastValues.setQuick(key, value);
    }

    private void decodeAllValues(long dataOffset, int dataLen, long valueCount, LongList result) {
        result.clear();
        if (valueCount == 0) {
            return;
        }

        // Read first value (full 8 bytes)
        long value = valueMem.getLong(dataOffset);
        result.add(value);

        // Read delta-encoded values
        long[] decodeResult = new long[2];
        long offset = dataOffset + 8;
        long endOffset = dataOffset + dataLen;

        while (offset < endOffset && result.size() < valueCount) {
            DeltaBitmapIndexUtils.decodeDelta(valueMem, offset, decodeResult);
            value += decodeResult[0]; // Apply delta
            result.add(value);
            offset += decodeResult[1]; // Move by bytes consumed
        }
    }

    /**
     * Clears all cached key metadata.
     */
    private void clearCaches() {
        keyDataOffsets.clear();
        keyDataLens.clear();
        keyValueCounts.clear();
        lastValues.clear();
        rollbackValues.clear();
    }

    /**
     * Extends all caches to accommodate the given key.
     */
    private void extendCachesForKey(int key) {
        while (keyDataOffsets.size() <= key) {
            keyDataOffsets.add(0);
            keyDataLens.add(0);
            keyValueCounts.add(0);
            lastValues.add(Long.MIN_VALUE);
        }
    }

    private long keyMemSize() {
        return (long) this.keyCount * DeltaBitmapIndexUtils.KEY_ENTRY_SIZE + DeltaBitmapIndexUtils.KEY_FILE_RESERVED;
    }

    /**
     * Initializes value data for an existing key that had no values (sparse key byproduct).
     */
    private void initValueDataAndStoreValue(long keyOffset, int key, long value) {
        // Write first value at current end of value memory
        long dataOffset = valueMemSize;
        valueMem.jumpTo(dataOffset);
        valueMem.putLong(value);

        int dataLen = 8; // First value is always 8 bytes
        valueMemSize = valueMem.getAppendOffset();
        updateValueMemSize();

        // Update key entry atomically with reduced fences
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 1);
        keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, dataOffset);
        keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, value);
        keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, dataLen);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 1);

        // Update caches
        keyDataOffsets.setQuick(key, dataOffset);
        keyDataLens.setQuick(key, dataLen);
        keyValueCounts.setQuick(key, 1);
        lastValues.setQuick(key, value);
    }

    /**
     * Initializes value data for a new key (key >= keyCount).
     * Uses combined header update for efficiency.
     */
    private void initValueDataForNewKey(long keyOffset, int key, long value) {
        // Write first value at current end of value memory
        long dataOffset = valueMemSize;
        valueMem.jumpTo(dataOffset);
        valueMem.putLong(value);

        int dataLen = 8; // First value is always 8 bytes
        long newValueMemSize = valueMem.getAppendOffset();

        // Combined header update (single sequence bump for both keyCount and valueMemSize)
        updateHeaderAtomically(key + 1, newValueMemSize);

        // Update key entry atomically with reduced fences
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, 1);
        keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET, dataOffset);
        keyMem.putLong(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE, value);
        keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN, dataLen);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(keyOffset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 1);

        // Update caches - extend if needed
        extendCachesForKey(key);
        keyDataOffsets.setQuick(key, dataOffset);
        keyDataLens.setQuick(key, dataLen);
        keyValueCounts.setQuick(key, 1);
        lastValues.setQuick(key, value);
    }

    /**
     * Rebuilds all caches from key entries.
     * O(k) where k = keyCount, since all metadata is stored directly in each key entry.
     */
    private void rebuildCaches() {
        clearCaches();
        for (int k = 0; k < keyCount; k++) {
            long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(k);
            long valueCount = keyMem.getLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
            int countCheck = keyMem.getInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK);
            long dataOffset = keyMem.getLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_OFFSET);

            // Verify the entry is valid
            if (valueCount > 0 && countCheck == (int) valueCount && dataOffset >= 0 && dataOffset < valueMemSize) {
                int dataLen = keyMem.getInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_DATA_LEN);
                long lastValue = keyMem.getLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE);
                keyDataOffsets.add(dataOffset);
                keyDataLens.add(dataLen);
                keyValueCounts.add(valueCount);
                lastValues.add(lastValue);
            } else {
                keyDataOffsets.add(0);
                keyDataLens.add(0);
                keyValueCounts.add(0);
                lastValues.add(Long.MIN_VALUE);
            }
        }
    }

    /**
     * Updates header with new key count and value mem size in a single atomic operation.
     * This is more efficient than separate updateKeyCount + updateValueMemSize calls.
     */
    private void updateHeaderAtomically(int newKeyCount, long newValueMemSize) {
        keyCount = newKeyCount;
        valueMemSize = newValueMemSize;

        long seq = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, newKeyCount);
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, newValueMemSize);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private void updateValueMemSize() {
        long seq = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    /**
     * Internal cursor for reading values in backward order (most recent first).
     */
    private class Cursor implements RowCursor {
        private final LongList values = new LongList();
        private int position;

        @Override
        public boolean hasNext() {
            return position >= 0;
        }

        @Override
        public long next() {
            return values.getQuick(position--);
        }

        void of(int key) {
            // Use cached metadata
            long valueCount = keyValueCounts.getQuick(key);
            long dataOffset = keyDataOffsets.getQuick(key);
            int dataLen = keyDataLens.getQuick(key);

            decodeAllValues(dataOffset, dataLen, valueCount, values);
            position = values.size() - 1;
        }
    }
}
