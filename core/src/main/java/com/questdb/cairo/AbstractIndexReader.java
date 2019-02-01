/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Misc;
import com.questdb.std.Unsafe;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.str.Path;

import java.util.concurrent.locks.LockSupport;

public abstract class AbstractIndexReader implements BitmapIndexReader {
    protected final static Log LOG = LogFactory.getLog(BitmapIndexBwdReader.class);
    protected final ReadOnlyMemory keyMem = new ReadOnlyMemory();
    protected final ReadOnlyMemory valueMem = new ReadOnlyMemory();
    protected int blockValueCountMod;
    protected int blockCapacity;
    protected long spinLockTimeoutUs;
    protected MicrosecondClock clock;
    protected int keyCount;
    protected long unIndexedNullCount;

    @Override
    public void close() {
        if (isOpen()) {
            Misc.free(keyMem);
            Misc.free(valueMem);
        }
    }

    @Override
    public int getKeyCount() {
        return keyCount;
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    public void of(CairoConfiguration configuration, Path path, CharSequence name, long unIndexedNullCount) {
        this.unIndexedNullCount = unIndexedNullCount;
        final int plen = path.length();
        final long pageSize = configuration.getFilesFacade().getMapPageSize();
        this.spinLockTimeoutUs = configuration.getSpinLockTimeoutUs();

        try {
            this.keyMem.of(configuration.getFilesFacade(), BitmapIndexUtils.keyFileName(path, name), pageSize, 0);
            this.keyMem.grow(configuration.getFilesFacade().length(this.keyMem.getFd()));
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
                    LOG.error().$("failed to read index header consistently [corrupt?] [timeout=").$(spinLockTimeoutUs).utf8("μs]").$();
                    throw CairoException.instance(0).put("failed to read index header consistently [corrupt?]");
                }

                LockSupport.parkNanos(1);
            }

            this.blockValueCountMod = blockValueCountMod;
            this.blockCapacity = (blockValueCountMod + 1) * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            this.keyCount = keyCount;
            this.valueMem.of(configuration.getFilesFacade(), BitmapIndexUtils.valueFileName(path.trimTo(plen), name), pageSize, 0);
            this.valueMem.grow(configuration.getFilesFacade().length(this.valueMem.getFd()));
        } catch (CairoException e) {
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
                LOG.error().$("failed to consistently update key count [corrupt index?] [timeout=").$(spinLockTimeoutUs).utf8("μs]").$();
                throw CairoException.instance(0).put("failed to consistently update key count [corrupt index?]");
            }
        }

        if (keyCount > this.getKeyCount()) {
            this.keyCount = keyCount;
        }
    }
}
