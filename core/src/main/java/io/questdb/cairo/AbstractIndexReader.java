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

public abstract class AbstractIndexReader implements BitmapIndexReader {
    public static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    protected static final Log LOG = LogFactory.getLog(BitmapIndexBwdReader.class);
    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    protected int blockCapacity;
    protected int blockValueCountMod;
    protected MillisecondClock clock;
    protected long columnTop;
    protected int keyCount;
    protected long spinLockTimeoutMs;
    private long columnTxn;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private long valueMemSize = -1;

    @Override
    public void close() {
        // Do not add isOpen() condition here.
        // Failures in opening should have memories closed as well.
        Misc.free(keyMem);
        Misc.free(valueMem);
    }

    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    public int getValueBlockCapacity() {
        return blockValueCountMod;
    }

    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
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
            LPSZ name = BitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    BitmapIndexUtils.getKeyEntryOffset(0),
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            // verify header signature
            if (keyMem.getByte(BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != BitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
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

    /**
     * Reloads index by extending value memory. There are several assumptions here:
     * - index reader is open and memory objects are not null
     * - memory object can be read
     * - key file sequence value and "value memory size" are both updated
     * - we can resize key memory using only keyCount
     * - we can resize value memory using the "value memory size" we read from the key header
     */
    public void reloadConditionally() {
        long seq = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            // extend memory objects
            this.keyMem.extend(BitmapIndexUtils.getKeyEntryOffset(keyCount));
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
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
            keyMem.extend(BitmapIndexUtils.getKeyEntryOffset(keyCount));
        }
    }

    private void readIndexMetadataAtomically() {
        // Triple check atomic read. We read the first and last sequences. If they match - there is a chance at stable
        // read. Confirm the start sequence hasn't changed after values read. If it has changed - retry the whole thing.
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            int keyCount;
            long valueMemSize;
            int blockValueCountMod;

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                blockValueCountMod = keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
                keyCount = keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                valueMemSize = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.blockValueCountMod = blockValueCountMod;
                    this.blockCapacity = (blockValueCountMod + 1) * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    keyMem.extend(BitmapIndexUtils.getKeyEntryOffset(keyCount));
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
}
