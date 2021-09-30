/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;

import java.util.concurrent.locks.LockSupport;

public abstract class AbstractIndexReader implements BitmapIndexReader {
    public static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    protected final static Log LOG = LogFactory.getLog(BitmapIndexBwdReader.class);
    protected final MemoryMR keyMem = Vm.getMRInstance();
    protected final MemoryMR valueMem = Vm.getMRInstance();
    protected int blockValueCountMod;
    protected int blockCapacity;
    protected long spinLockTimeoutUs;
    protected MicrosecondClock clock;
    protected int keyCount;
    protected long unIndexedNullCount;
    private int keyCountIncludingNulls;

    @Override
    public void close() {
        if (isOpen()) {
            Misc.free(keyMem);
            Misc.free(valueMem);
        }
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    public long getKeyMemorySize() {
        return keyMem.size();
    }

    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    public long getValueMemorySize() {
        return valueMem.size();
    }

    public long getUnIndexedNullCount() {
        return unIndexedNullCount;
    }

    public int getValueBlockCapacity() {
        return blockValueCountMod;
    }

    public void of(CairoConfiguration configuration, Path path, CharSequence name, long unIndexedNullCount, long partitionTxn) {
        this.unIndexedNullCount = unIndexedNullCount;
        TableUtils.txnPartitionConditionally(path, partitionTxn);
        final int plen = path.length();
        this.spinLockTimeoutUs = configuration.getSpinLockTimeoutUs();

        try {
            this.keyMem.wholeFile(configuration.getFilesFacade(), BitmapIndexUtils.keyFileName(path, name), MemoryTag.MMAP_DEFAULT);
            this.clock = configuration.getMicrosecondClock();

            // key file should already be created at least with header
            long keyMemSize = this.keyMem.size();
            if (keyMemSize < BitmapIndexUtils.KEY_FILE_RESERVED) {
                LOG.error().$("file too short [corrupt] ").$(path).$();
                throw CairoException.instance(0).put("Index file too short: ").put(path);
            }

            // verify header signature
            if (this.keyMem.getByte(BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != BitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.instance(0).put("Unknown format: ").put(path);
            }

            // Triple check atomic read. We read first and last sequences. If they match - there is a chance at stable
            // read. Confirm start sequence hasn't changed after values read. If it has changed - retry the whole thing.
            int blockValueCountMod;
            int keyCount;
            final long deadline = clock.getTicks() + spinLockTimeoutUs;
            while (true) {
                long seq = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);

                Unsafe.getUnsafe().loadFence();
                if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {

                    blockValueCountMod = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
                    keyCount = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);

                    Unsafe.getUnsafe().loadFence();
                    if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                        break;
                    }
                }

                if (clock.getTicks() > deadline) {
                    LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutUs).utf8("μs]").$();
                    throw CairoException.instance(0).put(INDEX_CORRUPT);
                }

                LockSupport.parkNanos(1);
            }

            this.blockValueCountMod = blockValueCountMod;
            this.blockCapacity = (blockValueCountMod + 1) * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            this.keyCount = this.keyCountIncludingNulls = keyCount;
            if (unIndexedNullCount > 0) {
                this.keyCountIncludingNulls++;
            }
            this.valueMem.wholeFile(configuration.getFilesFacade(), BitmapIndexUtils.valueFileName(path.trimTo(plen), name), MemoryTag.MMAP_DEFAULT);
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    protected void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutUs;
        while (true) {
            long seq = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);

            Unsafe.getUnsafe().loadFence();
            if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {

                keyCount = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (seq == this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                    break;
                }
            }

            if (clock.getTicks() > deadline) {
                this.keyCount = 0;
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutUs).utf8("μs]").$();
                throw CairoException.instance(0).put(INDEX_CORRUPT);
            }
        }

        if (keyCount > this.keyCount) {
            this.keyCount = this.keyCountIncludingNulls = keyCount;
            if (unIndexedNullCount > 0) {
                this.keyCountIncludingNulls++;
            }
        }
    }
}
