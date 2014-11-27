/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.index;

import com.nfsdb.journal.JournalMode;
import com.nfsdb.journal.collections.DirectLongList;
import com.nfsdb.journal.column.MappedFileImpl;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Files;
import com.nfsdb.journal.utils.Unsafe;

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

    private static final int ENTRY_SIZE = 16;
    private final IndexCursor cachedCursor = new IndexCursor();
    int rowBlockSize;
    int rowBlockLen;
    long firstEntryOffset;
    private MappedFileImpl kData;
    // storage for rows
    // block structure is [ rowid1, rowid2 ..., rowidn, prevBlockOffset]
    private MappedFileImpl rData;
    private long keyBlockSize;
    private long keyBlockAddressOffset;
    private long keyBlockSizeOffset;
    private long maxValue;
    private boolean inTransaction = false;

    public KVIndex(File baseName, long keyCountHint, long recordCountHint, int txCountHint, JournalMode mode, long txAddress) throws JournalException {
        int bitHint = (int) Math.min(Integer.MAX_VALUE, Math.max(keyCountHint, 1));
        this.rowBlockLen = (int) Math.min(134217728, Math.max(recordCountHint / bitHint, 1));
        this.kData = new MappedFileImpl(new File(baseName.getParentFile(), baseName.getName() + ".k"), ByteBuffers.getBitHint(8, bitHint * txCountHint), mode);
        this.keyBlockAddressOffset = 8;

        this.keyBlockSizeOffset = 16;
        this.keyBlockSize = 0;
        this.maxValue = 0;

        if (kData.getAppendOffset() > 0) {
            this.rowBlockLen = (int) getLong(kData, 0);
            this.keyBlockSizeOffset = txAddress == 0 ? getLong(kData, keyBlockAddressOffset) : txAddress;
            this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
            this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
        } else if (mode == JournalMode.APPEND || mode == JournalMode.BULK_APPEND) {
            putLong(kData, 0, this.rowBlockLen); // 8
            putLong(kData, keyBlockAddressOffset, keyBlockSizeOffset); // 8
            putLong(kData, keyBlockSizeOffset, keyBlockSize); // 8
            putLong(kData, keyBlockSizeOffset + 8, maxValue); // 8
            kData.setAppendOffset(8 + 8 + 8 + 8);
        }

        this.firstEntryOffset = keyBlockSizeOffset + 16;
        this.rowBlockSize = rowBlockLen * 8 + 8;
        this.rData = new MappedFileImpl(new File(baseName.getParentFile(), baseName.getName() + ".r"), ByteBuffers.getBitHint(rowBlockSize, bitHint), mode);
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

        if (!inTransaction) {
            tx();
        }

        long keyOffset = getKeyOffset(key);
        long rowBlockOffset;
        long rowCount;

        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            long oldSize = keyBlockSize;
            keyBlockSize = keyOffset + ENTRY_SIZE - firstEntryOffset;
            // if keys are added in random order there will be gaps in key block with possibly random values
            // to mitigate that as soon as we see an attempt to extend key block past ENTRY_SIZE we need to
            // fill created gap with zeroes.
            if (keyBlockSize - oldSize > ENTRY_SIZE) {
                Unsafe.getUnsafe().setMemory(
                        kData.getAddress(
                                firstEntryOffset + oldSize
                                , (int) (keyBlockSize - oldSize - ENTRY_SIZE)
                        )
                        , keyBlockSize - oldSize - ENTRY_SIZE
                        , (byte) 0
                );
            }
        }

        long address = kData.getAddress(keyOffset, ENTRY_SIZE);
        rowBlockOffset = Unsafe.getUnsafe().getLong(address);
        rowCount = Unsafe.getUnsafe().getLong(address + 8);

        int cellIndex = (int) (rowCount % rowBlockLen);
        if (rowBlockOffset == 0 || cellIndex == 0) {
            long prevBlockOffset = rowBlockOffset;
            rowBlockOffset = rData.getAppendOffset() + rowBlockSize;
            rData.setAppendOffset(rowBlockOffset);
            Unsafe.getUnsafe().putLong(rData.getAddress(rowBlockOffset - 8, 8), prevBlockOffset);
            Unsafe.getUnsafe().putLong(address, rowBlockOffset);
        }
        Unsafe.getUnsafe().putLong(rData.getAddress(rowBlockOffset - rowBlockSize + 8 * cellIndex, 8), value);
        Unsafe.getUnsafe().putLong(address + 8, rowCount + 1);

        if (maxValue <= value) {
            maxValue = value + 1;
        }
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

    public void refresh() {
        commit();
        this.keyBlockSizeOffset = getLong(kData, keyBlockAddressOffset);
        this.keyBlockSize = getLong(kData, keyBlockSizeOffset);
        this.maxValue = getLong(kData, keyBlockSizeOffset + 8);
        this.firstEntryOffset = keyBlockSizeOffset + 16;
    }

    public void commit() {
        if (inTransaction) {
            putLong(kData, keyBlockSizeOffset, keyBlockSize); // 8
            putLong(kData, keyBlockSizeOffset + 8, maxValue); // 8
            kData.setAppendOffset(firstEntryOffset + keyBlockSize);
            putLong(kData, keyBlockAddressOffset, keyBlockSizeOffset); // 8
            inTransaction = false;
        }
    }

    public void force() {
        kData.force();
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

        int rowBlockCount = (int) (rowCount / rowBlockLen + 1);
        if (rowCount % rowBlockLen == 0) {
            rowBlockCount--;
        }

        int targetBlock = i / rowBlockLen;
        int cellIndex = i % rowBlockLen;

        while (targetBlock < --rowBlockCount) {
            rowBlockOffset = getLong(rData, rowBlockOffset - 8);
            if (rowBlockOffset == 0) {
                throw new JournalRuntimeException("Count doesn't match number of row blocks. Corrupt index? : %s", this);
            }
        }

        return getLong(rData, rowBlockOffset - rowBlockSize + 8 * cellIndex);
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

    /**
     * Counts values for a key. Uses case for this method is best illustrated by this code examples:
     * <p/>
     * int c = index.getValueCount(key)
     * for (int i = c-1; i >=0; i--) {
     * long value = index.getValueQuick(key, i)
     * }
     * <p/>
     * Note that the loop above is deliberately in reverse order, as #getValueQuick performance diminishes the
     * father away from last the current index is.
     * <p/>
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
        int cellIndex = (int) ((rowCount - 1) % rowBlockLen);
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
    public DirectLongList getValues(int key) {
        DirectLongList result = new DirectLongList();
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
    public void getValues(int key, DirectLongList values) {

        if (key < 0) {
            return;
        }

        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            return;
        }
        long address = kData.getAddress(keyOffset, ENTRY_SIZE);
        long rowBlockOffset = Unsafe.getUnsafe().getLong(address);
        long rowCount = Unsafe.getUnsafe().getLong(address + 8);

        values.reset((int) rowCount);
        values.setPos((int) rowCount);

        int rowBlockCount = (int) (rowCount / rowBlockLen) + 1;
        int len = (int) (rowCount % rowBlockLen);
        if (len == 0) {
            rowBlockCount--;
            len = rowBlockLen;
        }

        for (int i = rowBlockCount - 1; i >= 0; i--) {
            address = rData.getAddress(rowBlockOffset - rowBlockSize, rowBlockSize);
            int z = i * rowBlockLen;
            for (int k = 0; k < len; k++) {
                values.set(z + k, Unsafe.getUnsafe().getLong(address));
                address += 8;
            }
            if (i > 0) {
                rowBlockOffset = Unsafe.getUnsafe().getLong(address + (rowBlockLen - len) * 8);
            }
            len = rowBlockLen;
        }
    }

    public IndexCursor cachedCursor(int key) {
        return this.cachedCursor.setKey(key);
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

    /**
     * Closes underlying files.
     */
    public void close() {
        rData.close();
        kData.close();
    }

    /**
     * Removes empty space at end of index files. This is useful if your chosen file copy routine does not support
     * sparse files, e.g. where size of file content significantly smaller then file size in directory catalogue.
     *
     * @throws JournalException
     */
    public void compact() throws JournalException {
        kData.compact();
        rData.compact();
    }

    public void truncate(long size) {
        long offset = firstEntryOffset;
        long sz = 0;
        while (offset < firstEntryOffset + keyBlockSize) {
            long keyBlockAddress = kData.getAddress(offset, ENTRY_SIZE);
            long rowBlockOffset = Unsafe.getUnsafe().getLong(keyBlockAddress);
            long rowCount = Unsafe.getUnsafe().getLong(keyBlockAddress + 8);
            int len = (int) (rowCount % rowBlockLen);

            if (len == 0) {
                len = rowBlockLen;
            }
            while (rowBlockOffset > 0) {
                long rowAddress = rData.getAddress(rowBlockOffset - rowBlockSize, rowBlockSize);
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

    private long getLong(MappedFileImpl storage, long offset) {
        return Unsafe.getUnsafe().getLong(storage.getAddress(offset, 8));
    }

    private void putLong(MappedFileImpl storage, long offset, long value) {
        Unsafe.getUnsafe().putLong(storage.getAddress(offset, 8), value);
    }

    private void tx() {
        if (!inTransaction) {
            this.keyBlockSizeOffset = kData.getAppendOffset();
            this.firstEntryOffset = keyBlockSizeOffset + 16;

            long srcOffset = getLong(kData, keyBlockAddressOffset);
            long dstOffset = this.keyBlockSizeOffset;
            int size = (int) (this.keyBlockSize + 8 + 8);

            while (size > 0) {
                long src = kData.getAddress(srcOffset, 1);
                int srcLen = kData.getLocalRemaining(srcOffset);

                long dst = kData.getAddress(dstOffset, 1);
                int dstLen = kData.getLocalRemaining(dstOffset);

                int len = size < (srcLen < dstLen ? srcLen : dstLen) ? size : (srcLen < dstLen ? srcLen : dstLen);

                Unsafe.getUnsafe().copyMemory(src, dst, len);
                size -= len;
                srcOffset += len;
                dstOffset += len;
            }
            keyBlockSize = dstOffset - firstEntryOffset;
        }
        inTransaction = true;
    }

    long getKeyOffset(long key) {
        return firstEntryOffset + (key + 1) * ENTRY_SIZE;
    }

    private long keyAddressOrError(int key) {
        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            throw new JournalRuntimeException("Key doesn't exist: %d", key);
        }
        return kData.getAddress(keyOffset, ENTRY_SIZE);
    }

    public class IndexCursor implements Cursor {
        private int remainingBlockCount;
        private int remainingRowCount;
        private long rowBlockOffset;
        private long size;
        private long address;

        public IndexCursor setKey(int key) {
            this.remainingBlockCount = 0;
            this.remainingRowCount = 0;

            if (key < 0) {
                return this;
            }

            long keyOffset = getKeyOffset(key);
            if (keyOffset >= firstEntryOffset + keyBlockSize) {
                return this;
            }

            long addr = kData.getAddress(keyOffset, ENTRY_SIZE);
            this.rowBlockOffset = Unsafe.getUnsafe().getLong(addr);
            this.size = Unsafe.getUnsafe().getLong(addr + 8);

            if (size == 0) {
                return this;
            }

            this.remainingBlockCount = (int) (this.size / rowBlockLen);
            this.remainingRowCount = (int) (this.size % rowBlockLen);

            if (remainingRowCount == 0) {
                remainingBlockCount--;
                remainingRowCount = rowBlockLen;
            }

            this.address = rData.getAddress(this.rowBlockOffset - rowBlockSize, rowBlockSize);
            return this;
        }

        public boolean hasNext() {
            return this.remainingRowCount > 0 || this.remainingBlockCount > 0;
        }

        public long next() {
            if (remainingRowCount > 0) {
                return Unsafe.getUnsafe().getLong(address + --this.remainingRowCount * 8);
            } else {
                remainingBlockCount--;
                this.rowBlockOffset = Unsafe.getUnsafe().getLong(address + rowBlockLen * 8);
                this.address = rData.getAddress(rowBlockOffset - rowBlockSize, rowBlockSize);
                this.remainingRowCount = rowBlockLen;
                return Unsafe.getUnsafe().getLong(address + --this.remainingRowCount * 8);
            }
        }

        public long size() {
            return size;
        }
    }
}
