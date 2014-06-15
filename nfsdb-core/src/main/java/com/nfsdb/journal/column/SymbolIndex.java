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

package com.nfsdb.journal.column;

import com.nfsdb.journal.JournalMode;
import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.utils.ByteBuffers;
import com.nfsdb.journal.utils.Files;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class SymbolIndex implements Closeable {

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

    static final byte[] ZERO_BYTE_ARRAY = new byte[1024 * 1024];

    static {
        Arrays.fill(ZERO_BYTE_ARRAY, (byte) 0);
    }

    private static final int ENTRY_SIZE = 16;
    private MappedFileImpl kData;
    private MappedFileImpl kDataR;
    // storage for rows
    // block structure is [ rowid1, rowid2 ..., rowidn, prevBlockOffset]
    private MappedFileImpl rData;
    private int rowBlockSize;
    private int rowBlockLen;
    private long firstEntryOffset;
    private long keyBlockAddressOffset;
    private long keyBlockSizeOffset;
    private long keyBlockSize;
    private long maxValue;
    private boolean inTransaction = false;

    public SymbolIndex(File baseName, long keyCountHint, long recordCountHint, int txCountHint, JournalMode mode, long txAddress) throws JournalException {
        int bitHint = (int) Math.min(Integer.MAX_VALUE, Math.max(keyCountHint, 1));
        this.rowBlockLen = (int) Math.min(134217728, Math.max(recordCountHint / bitHint, 1));
        this.kData = new MappedFileImpl(new File(baseName.getParentFile(), baseName.getName() + ".k"), ByteBuffers.getBitHint(8 + 8, bitHint * txCountHint), mode);
        this.kDataR = new MappedFileImpl(new File(baseName.getParentFile(), baseName.getName() + ".k"), ByteBuffers.getBitHint(8 + 8, bitHint * txCountHint), JournalMode.BULK_READ);
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
    public void put(int key, long value) {

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
                ByteBuffer buf = kData.getBuffer(firstEntryOffset + oldSize, (int) (keyBlockSize - oldSize - ENTRY_SIZE));
                int remaining;
                while ((remaining = buf.remaining()) > 0) {
                    buf.put(ZERO_BYTE_ARRAY, 0, Math.min(ZERO_BYTE_ARRAY.length, remaining));
                }
            }
        }

        ByteBuffer buf = kData.getBuffer(keyOffset, ENTRY_SIZE);
        int pos = buf.position();
        rowBlockOffset = buf.getLong(pos);
        rowCount = buf.getLong(pos + 8);

        int cellIndex = (int) (rowCount % rowBlockLen);
        if (rowBlockOffset == 0 || cellIndex == 0) {
            long prevBlockOffset = rowBlockOffset;
            rowBlockOffset = rData.getAppendOffset() + rowBlockSize;
            rData.setAppendOffset(rowBlockOffset);
            ByteBuffer bb = rData.getBuffer(rowBlockOffset - 8, 8);
            bb.putLong(bb.position(), prevBlockOffset);
            buf.putLong(pos, rowBlockOffset);
        }
        ByteBuffer bb = rData.getBuffer(rowBlockOffset - rowBlockSize + 8 * cellIndex, 8);
        bb.putLong(bb.position(), value);
        buf.putLong(pos + 8, rowCount + 1);

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

        ByteBuffer buf = keyBufferOrError(key);
        int pos = buf.position();
        long rowBlockOffset = buf.getLong(pos);
        long rowCount = buf.getLong(pos + 8);

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
            ByteBuffer buf = kData.getBuffer(keyOffset + 8, 8);
            return (int) buf.getLong(buf.position());
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
        ByteBuffer buf = keyBufferOrError(key);
        int pos = buf.position();
        long rowBlockOffset = buf.getLong(pos);
        long rowCount = buf.getLong(pos + 8);
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
    public LongArrayList getValues(int key) {
        LongArrayList result = new LongArrayList();
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
    public void getValues(int key, LongArrayList values) {

        values.resetQuick();

        if (key < 0) {
            return;
        }

        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            return;
        }
        ByteBuffer buf = kData.getBuffer(keyOffset, ENTRY_SIZE);
        int pos = buf.position();
        long rowBlockOffset = buf.getLong(pos);
        long rowCount = buf.getLong(pos + 8);

        values.setCapacity((int) rowCount);
        values.setPos((int) rowCount);

        int rowBlockCount = (int) (rowCount / rowBlockLen) + 1;
        int len = (int) (rowCount % rowBlockLen);
        if (len == 0) {
            rowBlockCount--;
            len = rowBlockLen;
        }

        for (int i = rowBlockCount - 1; i >= 0; i--) {
            ByteBuffer b = rData.getBuffer(rowBlockOffset - rowBlockSize, rowBlockSize);
            int z = i * rowBlockLen;
            int p = b.position();
            for (int k = 0; k < len; k++) {
                values.setQuick(z + k, b.getLong(p));
                p += 8;
            }
            rowBlockOffset = b.getLong(p + (rowBlockLen - len) * 8);
            len = rowBlockLen;
        }
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
        kDataR.close();
        kData.close();
    }

    /**
     * Removes empty space at end of index files. This is useful if your chosen file copy routine does not support
     * sparse files, e.g. where size of file content significantly smaller then file size in directory catalogue.
     *
     * @throws JournalException
     */
    public void compact() throws JournalException {
        kDataR.close();
        kData.compact();
        rData.compact();
        kDataR.open();
    }

    public void truncate(long size) {
        long offset = firstEntryOffset;
        long sz = 0;
        while (offset < firstEntryOffset + keyBlockSize) {
            ByteBuffer buffer = kData.getBuffer(offset, ENTRY_SIZE);
            buffer.mark();
            long rowBlockOffset = buffer.getLong();
            long rowCount = buffer.getLong();
            int len = (int) (rowCount % rowBlockLen);

            if (len == 0) {
                len = rowBlockLen;
            }
            while (rowBlockOffset > 0) {
                ByteBuffer buf = rData.getBuffer(rowBlockOffset - rowBlockSize, rowBlockSize);
                buf.mark();
                int pos = 0;
                long max = -1;
                while (pos < len) {
                    long v = buf.getLong();
                    if (v >= size) {
                        break;
                    }
                    pos++;
                    max = v;
                }

                if (max >= sz) {
                    sz = max + 1;
                }

                if (pos == 0) {
                    // discard whole block
                    buf.reset();
                    buf.position(buf.position() + rowBlockSize - 8);
                    rowBlockOffset = buf.getLong();
                    rowCount -= len;
                    len = rowBlockLen;
                } else {
                    rowCount -= len - pos;
                    break;
                }
            }
            buffer.reset();
            buffer.putLong(rowBlockOffset);
            buffer.putLong(rowCount);
            offset += ENTRY_SIZE;
        }

        maxValue = sz;
        commit();
    }

    private long getLong(MappedFileImpl storage, long offset) {
        ByteBuffer bb = storage.getBuffer(offset, 8);
        return bb.getLong(bb.position());
    }

    private void putLong(MappedFileImpl storage, long offset, long value) {
        ByteBuffer bb = storage.getBuffer(offset, 8);
        bb.putLong(bb.position(), value);
    }

    private void tx() {
        if (!inTransaction) {
            this.keyBlockSizeOffset = kData.getAppendOffset();
            this.firstEntryOffset = keyBlockSizeOffset + 16;

            long srcOffset = getLong(kDataR, keyBlockAddressOffset);
            long dstOffset = this.keyBlockSizeOffset;
            long size = this.keyBlockSize + 8 + 8;
            ByteBuffer src = null;
            ByteBuffer dst = null;

            while (size > 0) {
                if (src == null || !src.hasRemaining()) {
                    src = kDataR.getBuffer(srcOffset, 1);
                }

                if (dst == null || !dst.hasRemaining()) {
                    dst = kData.getBuffer(dstOffset, 1);
                }

                int limit = src.limit();
                int len;
                try {
                    if (src.remaining() > size) {
                        src.limit(src.position() + (int) size);
                    }
                    len = ByteBuffers.copy(src, dst);
                    size -= len;
                } finally {
                    src.limit(limit);
                }
                srcOffset += len;
                dstOffset += len;
            }
            keyBlockSize = dstOffset - firstEntryOffset;
        }
        inTransaction = true;
    }

    private long getKeyOffset(long key) {
        return firstEntryOffset + (key + 1) * ENTRY_SIZE;
    }

    private ByteBuffer keyBufferOrError(int key) {
        long keyOffset = getKeyOffset(key);
        if (keyOffset >= firstEntryOffset + keyBlockSize) {
            throw new JournalRuntimeException("Key doesn't exist: %d", key);
        }
        return kData.getBuffer(keyOffset, ENTRY_SIZE);
    }
}
