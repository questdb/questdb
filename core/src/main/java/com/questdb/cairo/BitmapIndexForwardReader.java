/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.common.RowCursor;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Misc;
import com.questdb.std.Unsafe;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.str.Path;

import java.util.concurrent.locks.LockSupport;

public class BitmapIndexForwardReader implements BitmapIndexReader {
    private final static Log LOG = LogFactory.getLog(BitmapIndexForwardReader.class);
    private final ReadOnlyMemory keyMem = new ReadOnlyMemory();
    private final ReadOnlyMemory valueMem = new ReadOnlyMemory();
    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();
    private int blockValueCountMod;
    private int blockCapacity;
    private long spinLockTimeoutUs;
    private MicrosecondClock clock;
    private int keyCount;
    private long unIndexedNullCount;

    public BitmapIndexForwardReader(CairoConfiguration configuration, Path path, CharSequence name, long unIndexedNullCount) {
        of(configuration, path, name, unIndexedNullCount);
    }

    @Override
    public void close() {
        if (isOpen()) {
            BitmapIndexReader.super.close();
            Misc.free(keyMem);
            Misc.free(valueMem);
        }
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && minValue < unIndexedNullCount) {
            // we need to return some nulls and the whole set of actual index values
            nullCursor.nullPos = minValue;
            nullCursor.nullCount = unIndexedNullCount;
            nullCursor.of(key, 0, maxValue);
            return nullCursor;
        }

        if (key < keyCount) {
            cursor.of(key, minValue, maxValue);
            return cursor;
        }

        return EmptyRowCursor.INSTANCE;
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

    private void updateKeyCount() {
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

        if (keyCount > this.keyCount) {
            this.keyCount = keyCount;
        }
    }

    private class Cursor implements RowCursor {
        protected long position;
        protected long valueCount;
        protected long next;
        private long valueBlockOffset;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;
        private long maxValue;

        @Override
        public boolean hasNext() {
            if (position < valueCount) {
                long cellIndex = getValueCellIndex(position++);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);

                if (result > maxValue) {
                    valueCount = 0;
                    return false;
                }

                if (cellIndex == blockValueCountMod && position < valueCount) {
                    // we are at edge of block right now, next value will be in previous block
                    jumpToPreviousValueBlock();
                }

                this.next = result;
                return true;
            }
            return false;
        }

        @Override
        public long next() {
            return next;
        }

        private long getNextBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + 8);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            // we don't need to grow valueMem because we going from farthest block back to start of file
            // to closes, e.g. valueBlockOffset is decreasing.
            valueBlockOffset = getNextBlock(valueBlockOffset);
        }

        void of(int key, long minValue, long maxValue) {
            assert key > -1 : "key must be positive integer: " + key;
            long offset = BitmapIndexUtils.getKeyEntryOffset(key);
            keyMem.grow(offset + BitmapIndexUtils.KEY_ENTRY_SIZE);
            // Read value count and last block offset atomically. In that we must orderly read value count first and
            // value count check last. If they match - everything we read between those holds true. We must retry
            // should these values do not match.
            long valueCount;
            long valueBlockOffset;
            long lastValueBlockOffset;
            final long deadline = clock.getTicks() + spinLockTimeoutUs;
            while (true) {
                valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                    valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);
                    lastValueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                        break;
                    }
                }

                if (clock.getTicks() > deadline) {
                    LOG.error().$("cursor failed to read index header consistently [corrupt?] [timeout=").$(spinLockTimeoutUs).utf8("μs, key=").$(key).$(", offset=").$(offset).$(']').$();
                    throw CairoException.instance(0).put("cursor failed to read index header consistently [corrupt?]");
                }
            }

            valueMem.grow(lastValueBlockOffset + blockCapacity);
            this.valueCount = valueCount;
            if (valueCount > 0) {
                BitmapIndexUtils.seekValueBlockLTR(valueCount, valueBlockOffset, valueMem, minValue, blockValueCountMod, SEEKER);
            } else {
                seekValue(valueCount, valueBlockOffset);
            }

            this.maxValue = maxValue;
        }

        private void seekValue(long count, long offset) {
            this.position = count;
            this.valueBlockOffset = offset;
        }
    }

    private class NullCursor extends Cursor {
        private long nullCount;
        private long nullPos;

        @Override
        public boolean hasNext() {
            if (nullPos < nullCount) {
                next = nullPos++;
                return true;
            }
            return super.hasNext();
        }
    }
}