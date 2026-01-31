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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.RoaringBitmapIndexUtils.*;

/**
 * Base class for Roaring Bitmap Index readers.
 * Provides common functionality for both forward and backward readers.
 */
public abstract class AbstractRoaringBitmapIndexReader implements BitmapIndexReader {

    public static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    protected static final Log LOG = LogFactory.getLog(AbstractRoaringBitmapIndexReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();

    protected MillisecondClock clock;
    protected long spinLockTimeoutMs;
    protected int keyCount;
    protected long valueMemSize;
    protected long columnTop;

    private long columnTxn;
    private long partitionTxn;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;

    @Override
    public void close() {
        Misc.free(keyMem);
        Misc.free(valueMem);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    @Override
    public int getValueBlockCapacity() {
        // For Roaring, this concept doesn't apply the same way
        // Return chunk size for compatibility
        return CHUNK_SIZE - 1;
    }

    @Override
    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public void of(
            CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        final int plen = path.size();
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();

        try {
            FilesFacade ff = configuration.getFilesFacade();
            LPSZ name = keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    getKeyEntryOffset(0),
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            // Verify signature
            if (keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE) != SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    valueFileName(path.trimTo(plen), columnName, columnNameTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public void reloadConditionally() {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            this.keyMem.extend(getKeyEntryOffset(keyCount));
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();

            if (keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
                Unsafe.getUnsafe().loadFence();

                if (seq == keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE)) {
                    break;
                }
            }

            if (clock.getTicks() > deadline) {
                this.keyCount = 0;
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }

        if (keyCount > this.keyCount) {
            this.keyCount = keyCount;
            this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
            keyMem.extend(getKeyEntryOffset(keyCount));
        }
    }

    private void readIndexMetadataAtomically() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE);
            int keyCount;
            long valueMemSize;

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
                valueMemSize = keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    keyMem.extend(getKeyEntryOffset(keyCount));
                    break;
                }
            }

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }
    }

    /**
     * Reads key entry atomically and populates the provided KeyEntryInfo.
     * Returns true if the key has data, false if valueCount is 0.
     */
    protected boolean readKeyEntryAtomically(int key, KeyEntryInfo keyInfo) {
        long offset = getKeyEntryOffset(key);
        keyMem.extend(offset + KEY_ENTRY_SIZE);

        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long valueCount = keyMem.getLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT);
            Unsafe.getUnsafe().loadFence();

            if (keyMem.getLong(offset + KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                long chunkDirOffset = keyMem.getLong(offset + KEY_ENTRY_OFFSET_CHUNK_DIR_OFFSET);
                int chunkCount = keyMem.getInt(offset + KEY_ENTRY_OFFSET_CHUNK_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                    keyInfo.valueCount = valueCount;
                    keyInfo.chunkDirOffset = chunkDirOffset;
                    keyInfo.chunkCount = chunkCount;
                    return valueCount > 0 && chunkCount > 0;
                }
            }

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, key=").$(key).$(']').$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }
    }

    /**
     * Reads chunk directory entry at given offset and populates the provided ChunkInfo.
     */
    protected void readChunkInfo(long chunkDirOffset, ChunkInfo chunkInfo) {
        chunkInfo.chunkId = valueMem.getShort(chunkDirOffset + CHUNK_DIR_OFFSET_CHUNK_ID) & 0xFFFF;
        chunkInfo.containerType = valueMem.getByte(chunkDirOffset + CHUNK_DIR_OFFSET_CONTAINER_TYPE);
        chunkInfo.cardinality = valueMem.getInt(chunkDirOffset + CHUNK_DIR_OFFSET_CARDINALITY);
        chunkInfo.containerOffset = valueMem.getLong(chunkDirOffset + CHUNK_DIR_OFFSET_CONTAINER_OFFSET);
    }

    /**
     * Key entry information (reusable, mutable).
     */
    protected static class KeyEntryInfo {
        long valueCount;
        long chunkDirOffset;
        int chunkCount;
    }

    /**
     * Chunk information (reusable, mutable).
     */
    protected static class ChunkInfo {
        int chunkId;
        byte containerType;
        int cardinality;
        long containerOffset;
    }
}
