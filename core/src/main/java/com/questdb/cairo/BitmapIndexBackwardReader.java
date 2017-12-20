/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import java.io.Closeable;

public class BitmapIndexBackwardReader implements Closeable {
    private final static Log LOG = LogFactory.getLog(BitmapIndexBackwardReader.class);
    private final ReadOnlyMemory keyMem;
    private final ReadOnlyMemory valueMem;
    private final Cursor cursor = new Cursor();
    private final int blockValueCountMod;
    private final int blockCapacity;
    private final long spinLockTimeoutUs;
    private final MicrosecondClock clock;
    private long keyCount;

    public BitmapIndexBackwardReader(CairoConfiguration configuration, Path path, CharSequence name) {
        final int plen = path.length();
        final long pageSize = configuration.getFilesFacade().getMapPageSize();
        this.spinLockTimeoutUs = configuration.getSpinLockTimeoutUs();

        try {
            BitmapIndexUtils.keyFileName(path, name);
            this.keyMem = new ReadOnlyMemory(configuration.getFilesFacade(), path, pageSize);
            this.clock = configuration.getClock();

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

            // Read key memory header atomically, in that start and end sequence numbers
            // must be read orderly and their values must match. If they don't match - we must retry.
            // This is always necessary in case reader is created at the same time as index itself.

            int blockValueCountMod;
            long keyCount;
            long timestamp = clock.getTicks();
            while (true) {
                long seq = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
                Unsafe.getUnsafe().loadFence();

                blockValueCountMod = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
                keyCount = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                Unsafe.getUnsafe().loadFence();

                if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                    break;
                }

                if (clock.getTicks() - timestamp > spinLockTimeoutUs) {
                    LOG.error().$("failed to read index header consistently [corrupt?] [timeout=").$(spinLockTimeoutUs).$("us]").$();
                    throw CairoException.instance(0).put("failed to read index header consistently [corrupt?]");
                }
            }

            this.blockValueCountMod = blockValueCountMod;
            this.blockCapacity = (blockValueCountMod + 1) * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
            this.keyCount = keyCount;

            BitmapIndexUtils.valueFileName(path.trimTo(plen), name);
            this.valueMem = new ReadOnlyMemory(configuration.getFilesFacade(), path, pageSize);
        } catch (CairoException e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void close() {
        Misc.free(keyMem);
        Misc.free(valueMem);
    }


    public BitmapIndexCursor getCursor(int key, long maxValue) {

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            cursor.of(key, maxValue);
            return cursor;
        }

        return BitmapIndexEmptyCursor.INSTANCE;
    }

    private void updateKeyCount() {
        long keyCount;
        long timestamp = clock.getTicks();
        while (true) {
            long seq = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();

            keyCount = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
            Unsafe.getUnsafe().loadFence();

            if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                break;
            }

            if (clock.getTicks() - timestamp > spinLockTimeoutUs) {
                this.keyCount = 0;
                LOG.error().$("failed to consistently update key count [corrupt index?] [timeout(us)=").$(spinLockTimeoutUs).$(']').$();
                throw CairoException.instance(0).put("failed to consistently update key count [corrupt index?]");
            }
        }

        if (keyCount > this.keyCount) {
            this.keyCount = keyCount;
        }
    }

    private class Cursor implements BitmapIndexCursor {
        private long valueBlockOffset;
        private long valueCount;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;

        @Override
        public boolean hasNext() {
            return valueCount > 0;
        }

        @Override
        public long next() {
            long cellIndex = getValueCellIndex(--valueCount);
            long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);
            if (cellIndex == 0 && valueCount > 0) {
                // we are at edge of block right now, next value will be in previous block
                jumpToPreviousValueBlock();
            }
            return result;
        }

        private long getPreviousBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            // we don't need to grow valueMem because we going from farthest block back to start of file
            // to closes, e.g. valueBlockOffset is decreasing.
            valueBlockOffset = getPreviousBlock(valueBlockOffset);
        }

        void of(int key, long maxValue) {
            assert key > -1 : "key must be positive integer: " + key;
            long offset = BitmapIndexUtils.getKeyEntryOffset(key);
            keyMem.grow(offset + BitmapIndexUtils.KEY_ENTRY_SIZE);
            // Read value count and last block offset atomically. In that we must orderly read value count first and
            // value count check last. If they match - everything we read between those holds true. We must retry
            // should these values do not match.
            long valueCount;
            long valueBlockOffset;
            long timestamp = clock.getTicks();
            while (true) {
                valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
                Unsafe.getUnsafe().loadFence();

                valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
                Unsafe.getUnsafe().loadFence();

                if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                    break;
                }

                if (clock.getTicks() - timestamp > spinLockTimeoutUs) {
                    LOG.error().$("cursor failed to read index header consistently [corrupt?] [timeout=").$(spinLockTimeoutUs).$("us, key=").$(key).$(", offset=").$(offset).$(']').$();
                    throw CairoException.instance(0).put("cursor failed to read index header consistently [corrupt?]");
                }
            }

            valueMem.grow(valueBlockOffset + blockCapacity);

            if (valueCount > 0) {
                BitmapIndexUtils.seekValueBlock(valueCount, valueBlockOffset, valueMem, maxValue, blockValueCountMod, SEEKER);
            } else {
                seekValue(valueCount, valueBlockOffset);
            }
        }

        private void seekValue(long count, long offset) {
            this.valueCount = count;
            this.valueBlockOffset = offset;
        }
    }
}