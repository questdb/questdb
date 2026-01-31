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
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.cairo.RoaringBitmapIndexUtils.*;

/**
 * Roaring Bitmap Index Writer.
 * <p>
 * Supports concurrent reads while appending. Uses sequence-based atomicity
 * similar to the legacy BitmapIndexWriter.
 * <p>
 * Each key maps to a series of chunks (65536 row IDs per chunk).
 * Each chunk uses the optimal container type:
 * - Array: sorted shorts, for sparse data (< 4096 values)
 * - Bitmap: 8KB bitset, for dense data (>= 4096 values)
 * - Run: (start, length) pairs, for consecutive sequences
 * <p>
 * File layout for each key:
 * - Chunk directory (16 bytes per chunk) stored contiguously
 * - Containers stored after the directory
 */
public class RoaringBitmapIndexWriter implements IndexWriter {
    private static final Log LOG = LogFactory.getLog(RoaringBitmapIndexWriter.class);

    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final MemoryMARW keyMem = Vm.getCMARWInstance();
    private final MemoryMARW valueMem = Vm.getCMARWInstance();

    // Per-key state: tracks the current (mutable) chunk for each key
    private final ObjList<KeyState> keyStates = new ObjList<>();

    private int keyCount = 0;
    private long valueMemSize = 0;

    public RoaringBitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    @TestOnly
    public RoaringBitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn, true);
    }

    public static void initKeyMemory(MemoryMA keyMem) {
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(SIGNATURE);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE_MEM_SIZE
        keyMem.putInt(0);  // unused (was block value count)
        keyMem.putInt(0);  // KEY_COUNT
        keyMem.putInt(0);  // unused
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE_CHECK
        keyMem.putLong(-1); // MAX_VALUE (-1 means no rows)
        keyMem.skip(KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    /**
     * Adds key-value pair to index. Values must be added in ascending order per key.
     */
    public void add(int key, long value) {
        assert key >= 0 : "key must be non-negative: " + key;

        // Ensure we have state for this key
        while (key >= keyStates.size()) {
            keyStates.add(null);
        }

        KeyState state = keyStates.getQuick(key);
        if (state == null) {
            state = new KeyState();
            keyStates.setQuick(key, state);
        }

        int chunkId = getChunkId(value);
        short offset = getOffsetInChunk(value);

        // Check if we need a new chunk
        if (state.currentChunkId < 0 || chunkId > state.currentChunkId) {
            // Finalize previous chunk if exists
            if (state.currentChunkId >= 0) {
                finalizeCurrentChunk(state);
            }
            // Start new chunk
            startNewChunk(state, chunkId);
        }

        // Add value to current chunk
        addToCurrentChunk(state, offset);
        state.totalValueCount++;

        // Track max key for later keyCount update (deferred to commit)
        if (key >= keyCount) {
            keyCount = key + 1;
        }

        // Track max value (deferred update to commit)
        if (value > state.maxValue) {
            state.maxValue = value;
        }
    }

    private void startNewChunk(KeyState state, int chunkId) {
        state.currentChunkId = chunkId;
        state.currentContainerType = CONTAINER_TYPE_ARRAY;
        state.currentCardinality = 0;
        state.currentContainerOffset = -1;
        state.arrayBuffer.clear();
    }

    private void addToCurrentChunk(KeyState state, short offset) {
        if (state.currentContainerType == CONTAINER_TYPE_ARRAY) {
            // Add to array buffer
            state.arrayBuffer.add(offset & 0xFFFFL);
            state.currentCardinality++;

            // Check if we need to promote to bitmap
            if (state.currentCardinality >= ARRAY_TO_BITMAP_THRESHOLD) {
                promoteArrayToBitmap(state);
            }
        } else if (state.currentContainerType == CONTAINER_TYPE_BITMAP) {
            // Set bit in bitmap
            int wordIndex = (offset & 0xFFFF) >>> 6;
            long bit = 1L << (offset & 63);

            if (state.currentContainerOffset < 0) {
                // Allocate bitmap container
                state.currentContainerOffset = valueMemSize;
                // Zero-initialize bitmap using native memset
                valueMem.skip(BITMAP_CONTAINER_SIZE);  // Ensure memory is allocated
                Vect.memset(valueMem.addressOf(state.currentContainerOffset), BITMAP_CONTAINER_SIZE, 0);
                valueMemSize += BITMAP_CONTAINER_SIZE;
            }

            long wordOffset = state.currentContainerOffset + (long) wordIndex * 8;
            long oldWord = valueMem.getLong(wordOffset);
            if ((oldWord & bit) == 0) {
                valueMem.putLong(wordOffset, oldWord | bit);
                state.currentCardinality++;
            }
        } else if (state.currentContainerType == CONTAINER_TYPE_RUN) {
            addToRunContainer(state, offset);
        }
    }

    private void promoteArrayToBitmap(KeyState state) {
        // Allocate bitmap container
        long bitmapOffset = valueMemSize;

        // Zero-initialize bitmap using native memset
        valueMem.skip(BITMAP_CONTAINER_SIZE);  // Ensure memory is allocated
        Vect.memset(valueMem.addressOf(bitmapOffset), BITMAP_CONTAINER_SIZE, 0);

        // Copy array values to bitmap
        for (int i = 0, n = state.arrayBuffer.size(); i < n; i++) {
            int val = (int) state.arrayBuffer.getQuick(i);
            int wordIndex = val >>> 6;
            long bit = 1L << (val & 63);
            long wordOffset = bitmapOffset + (long) wordIndex * 8;
            long oldWord = valueMem.getLong(wordOffset);
            valueMem.putLong(wordOffset, oldWord | bit);
        }

        valueMemSize += BITMAP_CONTAINER_SIZE;

        state.currentContainerType = CONTAINER_TYPE_BITMAP;
        state.currentContainerOffset = bitmapOffset;
        state.arrayBuffer.clear();
    }

    private void addToRunContainer(KeyState state, short offset) {
        int uOffset = offset & 0xFFFF;

        if (state.runBuffer.size() == 0) {
            // First run
            state.runBuffer.add(uOffset);  // start
            state.runBuffer.add(1);        // length
            state.currentCardinality = 1;
        } else {
            int lastIdx = state.runBuffer.size() - 2;
            int lastStart = state.runBuffer.getQuick(lastIdx);
            int lastLen = state.runBuffer.getQuick(lastIdx + 1);

            if (uOffset == lastStart + lastLen) {
                // Extend current run
                state.runBuffer.setQuick(lastIdx + 1, lastLen + 1);
            } else {
                // Start new run
                state.runBuffer.add(uOffset);
                state.runBuffer.add(1);
            }
            state.currentCardinality++;
        }
    }

    /**
     * Finalizes the current chunk by writing container data to value file
     * and recording the chunk info in memory.
     */
    private void finalizeCurrentChunk(KeyState state) {
        if (state.currentChunkId < 0 || state.currentCardinality == 0) {
            return;
        }

        long containerOffset;

        if (state.currentContainerType == CONTAINER_TYPE_ARRAY) {
            // Write array container to value file
            containerOffset = valueMemSize;
            for (int i = 0, n = state.arrayBuffer.size(); i < n; i++) {
                valueMem.putShort(containerOffset + (long) i * Short.BYTES, (short) state.arrayBuffer.getQuick(i));
            }
            valueMemSize += state.currentCardinality * Short.BYTES;
            state.arrayBuffer.clear();

        } else if (state.currentContainerType == CONTAINER_TYPE_BITMAP) {
            // Bitmap already written
            containerOffset = state.currentContainerOffset;

        } else {
            // Write run container to value file
            containerOffset = valueMemSize;
            int runCount = state.runBuffer.size() / 2;
            valueMem.putShort(containerOffset, (short) runCount);
            for (int i = 0; i < state.runBuffer.size(); i += 2) {
                long offset = containerOffset + RUN_CONTAINER_HEADER_SIZE + (long) (i / 2) * 4;
                valueMem.putShort(offset, (short) state.runBuffer.getQuick(i));      // start
                valueMem.putShort(offset + 2, (short) state.runBuffer.getQuick(i + 1)); // length
            }
            valueMemSize += RUN_CONTAINER_HEADER_SIZE + runCount * 4;
            state.runBuffer.clear();
        }

        // Record chunk info in memory (will be written to directory on commit)
        state.chunkInfos.add(new ChunkInfo(
                state.currentChunkId,
                state.currentContainerType,
                state.currentCardinality,
                containerOffset
        ));

        updateValueMemSize();
    }

    /**
     * Writes chunk directory for a key to the value file.
     * Called during commit to write all finalized chunks.
     */
    private void writeChunkDirectory(int key, KeyState state) {
        if (state.chunkInfos.size() == 0) {
            return;
        }

        // Allocate space for chunk directory
        long dirOffset = valueMemSize;
        int chunkCount = state.chunkInfos.size();

        // Write directory entries
        for (int i = 0; i < chunkCount; i++) {
            ChunkInfo info = state.chunkInfos.getQuick(i);
            long entryOffset = dirOffset + (long) i * CHUNK_DIR_ENTRY_SIZE;

            valueMem.putShort(entryOffset + CHUNK_DIR_OFFSET_CHUNK_ID, (short) info.chunkId);
            valueMem.putByte(entryOffset + CHUNK_DIR_OFFSET_CONTAINER_TYPE, info.containerType);
            valueMem.putByte(entryOffset + CHUNK_DIR_OFFSET_FLAGS, (byte) 0);
            valueMem.putInt(entryOffset + CHUNK_DIR_OFFSET_CARDINALITY, info.cardinality);
            valueMem.putLong(entryOffset + CHUNK_DIR_OFFSET_CONTAINER_OFFSET, info.containerOffset);
        }

        valueMemSize += (long) chunkCount * CHUNK_DIR_ENTRY_SIZE;

        // Update key entry to point to this directory
        state.chunkDirectoryOffset = dirOffset;
        state.committedChunkCount = chunkCount;

        updateValueMemSize();
        updateKeyEntry(key, state);
    }

    private void updateKeyEntry(int key, KeyState state) {
        long offset = getKeyEntryOffset(key);

        // Ensure key memory is large enough
        long requiredSize = offset + KEY_ENTRY_SIZE;
        if (keyMem.getAppendOffset() < requiredSize) {
            keyMem.skip(requiredSize - keyMem.getAppendOffset());
        }

        // Atomic update pattern: write count, fence, write details, fence, write check
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();

        keyMem.putLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT, state.totalValueCount);
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_CHUNK_DIR_OFFSET, state.chunkDirectoryOffset);
        keyMem.putInt(offset + KEY_ENTRY_OFFSET_CHUNK_COUNT, state.committedChunkCount);

        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_COUNT_CHECK, state.totalValueCount);
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private void updateKeyCount(int newKeyCount) {
        keyCount = newKeyCount;
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private void updateValueMemSize() {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    @Override
    public final void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, false);
    }

    public final void of(Path path, CharSequence name, long columnNameTxn, boolean create) {
        close();
        final int plen = path.size();
        try {
            if (create) {
                keyMem.of(ff, keyFileName(path, name, columnNameTxn),
                        configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                initKeyMemory(keyMem);

                path.trimTo(plen);
                valueMem.of(ff, valueFileName(path, name, columnNameTxn),
                        configuration.getDataIndexValueAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                valueMem.truncate();

                keyCount = 0;
                valueMemSize = 0;
            } else {
                // Open existing index
                if (!ff.exists(keyFileName(path, name, columnNameTxn))) {
                    LOG.error().$(path).$(" not found").$();
                    throw CairoException.critical(0).put("index does not exist [path=").put(path).put(']');
                }
                keyMem.of(ff, keyFileName(path, name, columnNameTxn),
                        configuration.getDataIndexKeyAppendPageSize(),
                        ff.length(keyFileName(path.trimTo(plen), name, columnNameTxn)),
                        MemoryTag.MMAP_INDEX_WRITER);

                // Verify signature
                if (keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE) != SIGNATURE) {
                    throw CairoException.critical(0).put("Unknown format: ").put(path);
                }

                keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
                valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

                path.trimTo(plen);
                valueMem.of(ff, valueFileName(path, name, columnNameTxn),
                        configuration.getDataIndexValueAppendPageSize(),
                        valueMemSize,
                        MemoryTag.MMAP_INDEX_WRITER);

                // Rebuild key states from existing data
                rebuildKeyStates();
            }
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    private void rebuildKeyStates() {
        keyStates.clear();
        for (int key = 0; key < keyCount; key++) {
            long offset = getKeyEntryOffset(key);
            long valueCount = keyMem.getLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT);
            long chunkDirOffset = keyMem.getLong(offset + KEY_ENTRY_OFFSET_CHUNK_DIR_OFFSET);
            int chunkCount = keyMem.getInt(offset + KEY_ENTRY_OFFSET_CHUNK_COUNT);

            KeyState state = new KeyState();
            state.totalValueCount = valueCount;
            state.chunkDirectoryOffset = chunkDirOffset;
            state.committedChunkCount = chunkCount;

            // Read last chunk info to set up for appending
            if (chunkCount > 0) {
                long lastChunkDirOffset = chunkDirOffset + (long) (chunkCount - 1) * CHUNK_DIR_ENTRY_SIZE;
                state.currentChunkId = valueMem.getShort(lastChunkDirOffset + CHUNK_DIR_OFFSET_CHUNK_ID) & 0xFFFF;
                state.currentContainerType = valueMem.getByte(lastChunkDirOffset + CHUNK_DIR_OFFSET_CONTAINER_TYPE);
                state.currentCardinality = valueMem.getInt(lastChunkDirOffset + CHUNK_DIR_OFFSET_CARDINALITY);
                state.currentContainerOffset = valueMem.getLong(lastChunkDirOffset + CHUNK_DIR_OFFSET_CONTAINER_OFFSET);
            }

            keyStates.add(state);
        }
    }

    public int getKeyCount() {
        return keyCount;
    }

    public long getMaxValue() {
        return keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE);
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    public boolean isOpen() {
        return keyMem.isOpen();
    }

    public void setMaxValue(long maxValue) {
        keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, maxValue);
    }

    public void sync(boolean async) {
        keyMem.sync(async);
        valueMem.sync(async);
    }

    public void commit() {
        long globalMaxValue = -1;

        // Finalize all current chunks and write directories
        for (int i = 0, n = keyStates.size(); i < n; i++) {
            KeyState state = keyStates.getQuick(i);
            if (state != null) {
                // Finalize current chunk if it has data
                if (state.currentCardinality > 0) {
                    finalizeCurrentChunk(state);
                    state.currentChunkId = -1;
                    state.currentCardinality = 0;
                }
                // Write chunk directory if there are chunks to write
                // Note: we keep chunkInfos accumulated so directory always contains all chunks
                if (state.chunkInfos.size() > state.committedChunkCount) {
                    writeChunkDirectory(i, state);
                }
                // Track global max
                if (state.maxValue > globalMaxValue) {
                    globalMaxValue = state.maxValue;
                }
            }
        }

        // Update global metadata atomically (single fence operation)
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();

        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        if (globalMaxValue > keyMem.getLong(KEY_RESERVED_OFFSET_MAX_VALUE)) {
            keyMem.putLong(KEY_RESERVED_OFFSET_MAX_VALUE, globalMaxValue);
        }

        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);

        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            sync(commitMode == CommitMode.ASYNC);
        }
    }

    public void truncate() {
        initKeyMemory(keyMem);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
        keyStates.clear();
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
        if (keyMem.isOpen()) {
            // Commit any pending data before closing
            try {
                commit();
            } catch (Throwable ignore) {
                // Best effort commit on close
            }

            if (keyCount > 0) {
                keyMem.setSize(getKeyEntryOffset(keyCount));
            }
            Misc.free(keyMem);
        }

        if (valueMem.isOpen()) {
            if (valueMemSize > 0) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }

        keyStates.clear();
    }

    /**
     * Internal state for each key being written.
     */
    private static class KeyState {
        long totalValueCount = 0;
        long chunkDirectoryOffset = 0;
        int committedChunkCount = 0;
        long maxValue = -1;  // Track max value per key

        // Current (mutable) chunk state
        int currentChunkId = -1;
        byte currentContainerType = CONTAINER_TYPE_ARRAY;
        int currentCardinality = 0;
        long currentContainerOffset = -1;

        // Buffers for building containers
        final LongList arrayBuffer = new LongList();
        final IntList runBuffer = new IntList();

        // Pending chunk infos to be written to directory on commit
        final ObjList<ChunkInfo> chunkInfos = new ObjList<>();
    }

    /**
     * Chunk information recorded before writing directory.
     */
    private static class ChunkInfo {
        final int chunkId;
        final byte containerType;
        final int cardinality;
        final long containerOffset;

        ChunkInfo(int chunkId, byte containerType, int cardinality, long containerOffset) {
            this.chunkId = chunkId;
            this.containerType = containerType;
            this.cardinality = cardinality;
            this.containerOffset = containerOffset;
        }
    }
}
