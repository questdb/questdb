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

package com.questdb.store;

import com.questdb.std.ByteBuffers;
import com.questdb.std.LongList;
import com.questdb.std.Numbers;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;

import java.io.Closeable;
import java.io.File;

public class KVIndex implements Closeable {

    /*
        storage for row count and offset
        block structure in kData is [int, long] for header and [long, long] for [offset, count]
        struct kdata{
           int rowBlockSize
           long firstEntryOffset
           struct kdataEntry {
                 long offsetOfTail
                 long rowCount
           }
        }
    */

    private static final int ENTRY_SIZE = 32;
    private final RevIndexCursor cachedCursor = new RevIndexCursor();
    private final FwdIndexCursor fwdIndexCursor = new FwdIndexCursor();
    private final int rowBlockSize;
    private final int rowBlockLen;
    private final MemoryFile kData;
    // storage for rows
    // block structure is [ rowid1, rowid2 ..., rowidn, prevBlockOffset]
    private final MemoryFile rData;
    private final int mask;
    private final int bits;
    private long firstEntryOffset;
    private long keyBlockSize;
    private long keyBlockAddressOffset;
    private long keyBlockSizeOffset;
    private long maxValue;
    private boolean startTx = true;

    public KVIndex(File baseName, long keyCountHint, long recordCountHint, int txCountHint, int journalMode, long txAddress, boolean sequentialAccess) throws JournalException {
        int keyCount = (int) Math.min(Integer.MAX_VALUE, Math.max(keyCountHint, 1));
        this.kData = new MemoryFile(new File(baseName.getParentFile(), baseName.getName() + ".k"), ByteBuffers.getBitHint(8, keyCount * txCountHint), journalMode, sequentialAccess);
        this.keyBlockAddressOffset = 8;

        if (kData.getAppendOffset() > 0) {
            this.rowBlockLen = (int) getLong(kData, 0);
            this.keyBlockSizeOffset = txAddress == 0 ? getLong(kData, keyBlockAddressOffset) : txAddress;
            this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
            this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
        } else if (journalMode == JournalMode.APPEND) {
            int l = (int) (recordCountHint / keyCount);
            this.rowBlockLen = l < 1 ? 1 : Numbers.ceilPow2(l);
            this.keyBlockSizeOffset = 16;
            this.keyBlockSize = 0;
            this.maxValue = 0;
            putLong(kData, 0, this.rowBlockLen); // 8
            putLong(kData, keyBlockAddressOffset, keyBlockSizeOffset); // 8
            putLong(kData, keyBlockSizeOffset, keyBlockSize); // 8
            putLong(kData, keyBlockSizeOffset + 8, maxValue); // 8
            kData.setAppendOffset(8 + 8 + 8 + 8);
        } else {
            this.kData.close();
            throw new JournalException("Cannot open uninitialized index in read-only mode");
        }

        // x & mask = x % rowBlockLen
        this.mask = rowBlockLen - 1;
        // x >>> bits = x / rowBlockLen
        this.bits = Numbers.msb(rowBlockLen);
        this.firstEntryOffset = keyBlockSizeOffset + 16;
        this.rowBlockSize = rowBlockLen * 8 + 16;
        try {
            this.rData = new MemoryFile(new File(baseName.getParentFile(), baseName.getName() + ".r"), ByteBuffers.getBitHint(rowBlockSize, keyCount), journalMode, sequentialAccess);
        } catch (JournalException e) {
            this.kData.close();
            throw e;
        }
    }

    public static void delete(File base) {
        Files.delete(new File(base.getParentFile(), base.getName() + ".k"));
        Files.delete(new File(base.getParentFile(), base.getName() + ".r"));
    }

    /**
     * Adds value to index. Values will be stored in same order as they were added.
     *
     * @param key   value of key
     * @param value value
     */
    public void add(int key, long value) {

        if (startTx) {
            tx();
        }

        long keyOffset = getKeyOffset(key);

        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            keyBlockSize = keyOffset + ENTRY_SIZE - firstEntryOffset;
        }

        long address = kData.addressOf(keyOffset, ENTRY_SIZE);
        long rowBlockOffset = Unsafe.getUnsafe().getLong(address);
        long rowCount = Unsafe.getUnsafe().getLong(address + 8);

        int cellIndex = (int) (rowCount & mask);
        if (rowBlockOffset == 0 || cellIndex == 0) {
            rowBlockOffset = allocateRowBlock(address, rowBlockOffset);
        }
        Unsafe.getUnsafe().putLong(rData.addressOf(rowBlockOffset - rowBlockSize + 8 * cellIndex, 8), value);
        Unsafe.getUnsafe().putLong(address + 8, rowCount + 1);

        if (maxValue <= value) {
            maxValue = value + 1;
        }
    }

    /**
     * Closes underlying files.
     */
    public void close() {
        rData.close();
        kData.close();
    }

    public void commit() {
        if (!startTx) {
            putLong(kData, keyBlockSizeOffset, keyBlockSize); // 8
            putLong(kData, keyBlockSizeOffset + 8, maxValue); // 8
            kData.setAppendOffset(firstEntryOffset + keyBlockSize);
            putLong(kData, keyBlockAddressOffset, keyBlockSizeOffset); // 8
            startTx = true;
        }
    }

    /**
     * Removes empty space at end of index files. This is useful if your chosen file copy routine does not support
     * sparse files, e.g. where size of file content significantly smaller then file size in directory catalogue.
     *
     * @throws JournalException in case of IO problems.
     */
    public void compact() throws JournalException {
        kData.compact();
        rData.compact();
    }

    /**
     * Checks if key exists in index. Use case for this method is in combination with #lastValue method.
     *
     * @param key value of key
     * @return true if key has any values associated with it, false otherwise.
     */
    public boolean contains(int key) {
        return getValueCount(key) > 0;
    }

    public IndexCursor cursor(int key) {
        return this.cachedCursor.setKey(key);
    }

    public void force() {
        kData.force();
    }

    public FwdIndexCursor fwdCursor(int key) {
        return this.fwdIndexCursor.setKey(key);
    }

    public long getTxAddress() {
        return keyBlockSizeOffset;
    }

    public void setTxAddress(long txAddress) {
        if (txAddress == 0) {
            refresh();
        } else {
            this.keyBlockSizeOffset = txAddress;
            this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
            this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
            this.firstEntryOffset = keyBlockSizeOffset + 16;
        }
    }

    /**
     * Counts values for a key. Uses case for this method is best illustrated by this code examples:
     * <pre>
     * int c = index.getValueCount(key)
     * for (int i = c-1; i {@literal >}=0; i--) {
     *     long value = index.getValueQuick(key, i)
     * }
     * </pre>
     * Note that the loop above is deliberately in reverse order, as #getValueQuick performance diminishes the
     * father away from last the current index is.
     * <p>
     * If it is your intention to iterate through all index values consider #getValues, as it offers better for
     * this use case.
     *
     * @param key value of key
     * @return number of values associated with key. 0 if either key doesn't exist or it doesn't have values.
     */
    public int getValueCount(int key) {
        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            return 0;
        } else {
            return (int) getLong(kData, keyOffset + 8);
        }
    }

    /**
     * Searches for indexed value of a key. This method will lookup newest values much faster then oldest.
     * If either key doesn't exist in index or value index is out of bounds an exception will be thrown.
     *
     * @param key value of key
     * @param i   index of key value to get.
     * @return long value for given index.
     */
    public long getValueQuick(int key, int i) {

        long address = keyAddressOrError(key);
        long rowBlockOffset = Unsafe.getUnsafe().getLong(address);
        long rowCount = Unsafe.getUnsafe().getLong(address + 8);

        if (i >= rowCount) {
            throw new JournalRuntimeException("Index out of bounds: %d, max: %d", i, rowCount - 1);
        }

        int rowBlockCount = (int) ((rowCount >>> bits) + 1);
        if ((rowCount & mask) == 0) {
            rowBlockCount--;
        }

        int targetBlock = i >>> bits;
        int cellIndex = i & mask;

        while (targetBlock < --rowBlockCount) {
            rowBlockOffset = getLong(rData, rowBlockOffset - 8);
            if (rowBlockOffset == 0) {
                throw new JournalRuntimeException("Count doesn't match number of row blocks. Corrupt index? : %s", this);
            }
        }

        return getLong(rData, rowBlockOffset - rowBlockSize + 8 * cellIndex);
    }

    /**
     * List of values for key. Values are in order they were added.
     * This method is best for use cases where you are most likely  to process all records from index. In cases
     * where you need last few records from index #getValueCount and #getValueQuick are faster and more memory
     * efficient.
     *
     * @param key key value
     * @return List of values or exception if key doesn't exist.
     */
    public LongList getValues(int key) {
        LongList result = new LongList();
        getValues(key, result);
        return result;
    }

    /**
     * This method does the same thing as #getValues(int key). In addition it lets calling party to reuse their
     * array for memory efficiency.
     *
     * @param key    key value
     * @param values the array to copy values to. The contents of this array will be overwritten with new values
     *               beginning from 0 index.
     */
    public void getValues(int key, LongList values) {

        if (key < 0) {
            return;
        }

        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            return;
        }
        long address = kData.addressOf(keyOffset, ENTRY_SIZE);
        long rowBlockOffset = Unsafe.getUnsafe().getLong(address);
        long rowCount = Unsafe.getUnsafe().getLong(address + 8);

        values.clear();
        values.setPos((int) rowCount);

        int rowBlockCount = (int) (rowCount >>> bits) + 1;
        int len = (int) (rowCount & mask);
        if (len == 0) {
            rowBlockCount--;
            len = rowBlockLen;
        }

        for (int i = rowBlockCount - 1; i >= 0; i--) {
            address = rData.addressOf(rowBlockOffset - rowBlockSize, rowBlockSize);
            int z = i << bits;
            for (int k = 0; k < len; k++) {
                values.set(z + k, Unsafe.getUnsafe().getLong(address));
                address += 8;
            }
            if (i > 0) {
                rowBlockOffset = Unsafe.getUnsafe().getLong(address + (rowBlockLen - len) * 8 + 8);
            }
            len = rowBlockLen;
        }
    }

    /**
     * Gets last value for key. If key doesn't exist in index an exception will be thrown. This method has to be used
     * in combination with #contains. E.g. check if index contains key and if it does - get last value. Thrown
     * exception is intended to point out breaking of this convention.
     *
     * @param key value of key
     * @return value
     */
    @SuppressWarnings("unused")
    public long lastValue(int key) {
        long address = keyAddressOrError(key);
        long rowBlockOffset = Unsafe.getUnsafe().getLong(address);
        long rowCount = Unsafe.getUnsafe().getLong(address + 8);
        int cellIndex = (int) ((rowCount - 1) & mask);
        return getLong(rData, rowBlockOffset - rowBlockSize + 8 * cellIndex);
    }

    public FwdIndexCursor newFwdCursor(int key) {
        FwdIndexCursor cursor = new FwdIndexCursor();
        cursor.setKey(key);
        return cursor;
    }

    public void setSequentialAccess(boolean sequentialAccess) {
        kData.setSequentialAccess(sequentialAccess);
        rData.setSequentialAccess(sequentialAccess);
    }

    /**
     * Size of index is in fact maximum of all row IDs. This is useful to keep it in same units of measure as
     * size of columns.
     *
     * @return max of all row IDs in index.
     */
    public long size() {
        return maxValue;
    }

    public void truncate(long size) {
        long offset = firstEntryOffset;
        long sz = 0;
        while (offset < firstEntryOffset + keyBlockSize) {
            long keyBlockAddress = kData.addressOf(offset, ENTRY_SIZE);
            long rowBlockOffset = Unsafe.getUnsafe().getLong(keyBlockAddress);
            long rowCount = Unsafe.getUnsafe().getLong(keyBlockAddress + 8);
            int len = (int) (rowCount & mask);

            if (len == 0) {
                len = rowBlockLen;
            }
            while (rowBlockOffset > 0) {
                long rowAddress = rData.addressOf(rowBlockOffset - rowBlockSize, rowBlockSize);
                long addr = rowAddress;
                int pos = 0;
                long max = -1;
                while (pos < len) {
                    long v = Unsafe.getUnsafe().getLong(addr);
                    if (v >= size) {
                        break;
                    }
                    addr += 8;
                    pos++;
                    max = v;
                }

                if (max >= sz) {
                    sz = max + 1;
                }

                if (pos == 0) {
                    // discard whole block
                    rowBlockOffset = Unsafe.getUnsafe().getLong(rowAddress + rowBlockSize - 8);
                    rowCount -= len;
                    len = rowBlockLen;
                } else {
                    rowCount -= len - pos;
                    break;
                }
            }
            Unsafe.getUnsafe().putLong(keyBlockAddress, rowBlockOffset);
            Unsafe.getUnsafe().putLong(keyBlockAddress + 8, rowCount);
            offset += ENTRY_SIZE;
        }

        maxValue = sz;
        commit();
    }

    private long allocateRowBlock(long address, long rowBlockOffset) {
        long offset = rData.getAppendOffset() + rowBlockSize;
        rData.setAppendOffset(offset);
        Unsafe.getUnsafe().putLong(rData.addressOf(offset - 8, 8), rowBlockOffset);
        Unsafe.getUnsafe().putLong(address, offset);
        if (rowBlockOffset == 0) {
            Unsafe.getUnsafe().putLong(address + 16, offset);
        } else {
            Unsafe.getUnsafe().putLong(rData.addressOf(rowBlockOffset - 16, 8), offset);
        }
        return offset;
    }

    private long getKeyOffset(long key) {
        return firstEntryOffset + (key + 1) * ENTRY_SIZE;
    }

    private long getLong(MemoryFile storage, long offset) {
        return Unsafe.getUnsafe().getLong(storage.addressOf(offset, 8));
    }

    private long keyAddressOrError(int key) {
        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            throw new JournalRuntimeException("Key doesn't exist: %d", key);
        }
        return kData.addressOf(keyOffset, ENTRY_SIZE);
    }

    private void putLong(MemoryFile storage, long offset, long value) {
        Unsafe.getUnsafe().putLong(storage.addressOf(offset, 8), value);
    }

    private void refresh() {
        commit();
        this.keyBlockSizeOffset = getLong(kData, keyBlockAddressOffset);
        this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
        this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
        this.firstEntryOffset = keyBlockSizeOffset + 16;
    }

    private void tx() {
        if (startTx) {
            this.keyBlockSizeOffset = kData.getAppendOffset();
            this.firstEntryOffset = keyBlockSizeOffset + 16;

            long srcOffset = getLong(kData, keyBlockAddressOffset);
            long dstOffset = this.keyBlockSizeOffset;
            int size = (int) (this.keyBlockSize + 8 + 8);

            while (size > 0) {
                long src = kData.addressOf(srcOffset, 1);
                int srcLen = kData.pageRemaining(srcOffset);

                kData.lockBuffers();
                long dst = kData.addressOf(dstOffset, 1);
                int dstLen = kData.pageRemaining(dstOffset);
                kData.unlockBuffers();

                int len = size < (srcLen < dstLen ? srcLen : dstLen) ? size : (srcLen < dstLen ? srcLen : dstLen);

                Unsafe.getUnsafe().copyMemory(src, dst, len);
                size -= len;
                srcOffset += len;
                dstOffset += len;
            }
            keyBlockSize = dstOffset - firstEntryOffset;
        }
        startTx = false;
    }

    private class RevIndexCursor implements IndexCursor {
        private int remainingBlockCount;
        private int remainingRowCount;
        private long size;
        private long address;

        public boolean hasNext() {
            return this.remainingRowCount > 0 || this.remainingBlockCount > 0;
        }

        public RevIndexCursor setKey(int key) {
            this.remainingBlockCount = 0;
            this.remainingRowCount = 0;

            if (key < 0) {
                return this;
            }

            long keyOffset = getKeyOffset(key);
            if (keyOffset < firstEntryOffset + keyBlockSize) {
                long addr = kData.addressOf(keyOffset, ENTRY_SIZE);
                this.size = Unsafe.getUnsafe().getLong(addr + 8);

                if (size == 0) {
                    return this;
                }

                int k = (int) (size & mask);
                if (k == 0) {
                    remainingBlockCount = (int) (this.size >>> bits) - 1;
                    remainingRowCount = rowBlockLen;
                } else {
                    remainingBlockCount = (int) (this.size >>> bits);
                    remainingRowCount = k;
                }
                this.address = rData.addressOf(Unsafe.getUnsafe().getLong(addr) - rowBlockSize, rowBlockSize);
            }

            return this;
        }

        public long next() {
            if (remainingRowCount > 0) {
                return Unsafe.getUnsafe().getLong(address + ((--this.remainingRowCount) << 3));
            } else {
                remainingBlockCount--;
                this.address = rData.addressOf(Unsafe.getUnsafe().getLong(address + (rowBlockLen << 3) + 8) - rowBlockSize, rowBlockSize);
                this.remainingRowCount = mask;
                return Unsafe.getUnsafe().getLong(address + (this.remainingRowCount << 3));
            }
        }


        public long size() {
            return size;
        }
    }

    private class FwdIndexCursor implements IndexCursor {
        private long rowCount;
        private long size;
        private long address;

        public FwdIndexCursor setKey(int key) {
            this.rowCount = 0;
            this.size = 0;

            if (key < -1) {
                return this;
            }

            long keyOffset = getKeyOffset(key);
            if (keyOffset >= firstEntryOffset + keyBlockSize) {
                return this;
            }

            long addr = kData.addressOf(keyOffset, ENTRY_SIZE);
            this.size = Unsafe.getUnsafe().getLong(addr + 8);

            if (size == 0) {
                return this;
            }

            this.rowCount = 0;
            this.address = rData.addressOf(Unsafe.getUnsafe().getLong(addr + 16) - rowBlockSize, rowBlockSize);
            return this;
        }

        public boolean hasNext() {
            return this.rowCount < size;
        }

        public long next() {
            int r = (int) (rowCount++ & mask);
            long v = Unsafe.getUnsafe().getLong(address + (r << 3));
            if (r == mask && rowCount < size) {
                this.address = rData.addressOf(Unsafe.getUnsafe().getLong(address + (rowBlockLen << 3)) - rowBlockSize, rowBlockSize);
            }
            return v;
        }


        public long size() {
            return size;
        }
    }
}
