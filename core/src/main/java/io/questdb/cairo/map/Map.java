/*+*****************************************************************************
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

package io.questdb.cairo.map;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public interface Map extends Mutable, Closeable, Reopenable {

    /**
     * Mask for the offset field of a packed batch entry (39 bits, bits 0–38).
     */
    long BATCH_OFFSET_MASK = 0x7F_FFFF_FFFFL;
    /**
     * Mask for the row index field of a packed batch entry after shifting (24 bits).
     */
    int BATCH_ROW_INDEX_MASK = 0xFF_FFFF;
    /**
     * Bit shift for the row index field within a packed batch entry.
     * Layout (MSB to LSB): {@code [isNew:1][rowIndex:24][offset:39]}.
     */
    int BATCH_ROW_INDEX_SHIFT = 39;

    /**
     * Decodes the entry byte offset from an encoded batch entry. Combine with
     * {@link #getEntryBase()} to obtain the absolute entry address.
     */
    static long decodeBatchOffset(long encoded) {
        return encoded & BATCH_OFFSET_MASK;
    }

    /**
     * Decodes the frame-relative row index from an encoded batch entry.
     */
    static int decodeBatchRowIndex(long encoded) {
        return (int) ((encoded >>> BATCH_ROW_INDEX_SHIFT) & BATCH_ROW_INDEX_MASK);
    }

    /**
     * Encodes a batch entry from its components. The layout is
     * {@code [isNew:1][rowIndex:24][offset:39]}.
     */
    static long encodeBatchEntry(long rowIndex, long offset, boolean isNew) {
        long encoded = (rowIndex << BATCH_ROW_INDEX_SHIFT) | (offset & BATCH_OFFSET_MASK);
        return isNew ? encoded | Long.MIN_VALUE : encoded;
    }

    /**
     * Returns {@code true} if the encoded batch entry represents a newly
     * inserted map entry.
     */
    static boolean isNewBatchEntry(long encoded) {
        return encoded < 0;
    }

    @Override
    void close();

    MapRecordCursor getCursor();

    /**
     * Returns the current base address against which entry offsets from
     * {@link MapValue#getStartOffset()} are interpreted.
     */
    default long getEntryBase() {
        throw new UnsupportedOperationException();
    }

    @TestOnly
    default long getHeapSize() {
        return -1;
    }

    /**
     * Returns full (physical) key capacity of the map ignoring the load factor.
     */
    @TestOnly
    int getKeyCapacity();

    MapRecord getRecord();

    /**
     * Returns used heap size in bytes for maps that store keys and values separately like {@link OrderedMap}.
     * Returns -1 if the map doesn't use heap.
     */
    default long getUsedHeapSize() {
        return -1;
    }

    boolean isOpen();

    void merge(Map srcMap, MapValueMergeFunction mergeFunc);

    /**
     * Probes rows {@code batchStart..batchEnd-1} and writes packed longs into
     * {@code batchAddr}. Each packed long has the layout:
     * {@code [isNew:1][rowIndex:24][offset:39]}.
     * <p>
     * The default implementation delegates to {@link #withKey()},
     * {@link MapKey#createValue()}, etc. Map implementations may override
     * with an inlined probe loop that avoids MapKey/MapValue abstractions.
     *
     * @return the entryBase address valid for the written batch
     */
    default long probeBatch(
            PageFrameMemoryRecord record,
            RecordSink mapSink,
            long batchStart,
            long batchEnd,
            long batchAddr
    ) {
        for (long r = batchStart; r < batchEnd; r++) {
            record.setRowIndex(r);
            final MapKey key = withKey();
            mapSink.copy(record, key);
            final MapValue value = key.createValue();
            long encoded = encodeBatchEntry(r, value.getStartAddress() - getEntryBase(), value.isNew());
            Unsafe.getUnsafe().putLong(batchAddr, encoded);
            batchAddr += Long.BYTES;
        }
        return getEntryBase();
    }

    /**
     * Reopens previously closed map with given key capacity and initial heap size.
     * Heap size value is ignored if the map does not use heap to store keys and values, e.g. {@link Unordered8Map}.
     */
    void reopen(int keyCapacity, long heapSize);

    /**
     * Ensures the map can accept up to {@code additionalKeys} new entries
     * without triggering a rehash that would invalidate harvested offsets.
     */
    default void reserveCapacity(long additionalKeys) {
        // no-op by default
    }

    void restoreInitialCapacity();

    void setKeyCapacity(int keyCapacity);

    long size();

    MapValue valueAt(long address);

    MapKey withKey();
}
