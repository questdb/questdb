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
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.std.Mutable;
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
     * Decodes the value-region byte offset from an encoded batch entry. Combine
     * with {@link #probeBatch}'s result to obtain the absolute start address of the
     * value region for that entry. Per-function value columns can then be
     * addressed by adding the column's offset within the value region.
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

    /**
     * Reinitializes an existing cursor (previously created by {@link #newCursor()})
     * with the map's current state, avoiding allocation.
     */
    void initCursor(MapRecordCursor cursor);

    boolean isOpen();

    void merge(Map srcMap, MapValueMergeFunction mergeFunc);

    /**
     * Creates an independent cursor over the same materialized data.
     * Each cursor has its own iteration state (position, record) but
     * shares the underlying data store.
     */
    MapRecordCursor newCursor();

    /**
     * Probes rows {@code batchStart..batchEnd-1} and writes packed longs into
     * {@code batchAddr}. Each packed long has the layout:
     * {@code [isNew:1][rowIndex:24][offset:39]}, where {@code offset} is the
     * byte distance from Map's base address to the <b>start of the value
     * region</b> of that entry — not to the entry start. This keeps the
     * encoding uniform across fixed- and variable-size maps and lets
     * consumers address per-column values as
     * {@code entryBase + offset + valueOffsets[valueIndex]}.
     * <p>
     * Callers must invoke {@link #reserveCapacity(long)} with at least
     * {@code batchEnd - batchStart} beforehand. Inlined implementations rely on
     * the pre-reserved headroom to skip the per-insert rehash check in the hot
     * loop; a mid-batch rehash would invalidate offsets already written into
     * {@code batchAddr} for earlier rows in the same batch.
     *
     * @return the baseValueAddress address valid for the written batch
     */
    long probeBatch(
            PageFrameMemoryRecord record,
            RecordSink mapSink,
            long batchStart,
            long batchEnd,
            long batchAddr
    );

    /**
     * Filtered variant of {@link #probeBatch}. Iterates positions
     * {@code p = batchStart..batchEnd-1} in the row-id list at
     * {@code rowIdsAddr} (packed longs, 8 bytes each) and probes each
     * {@code r = Unsafe.getLong(rowIdsAddr + p * 8)}. The encoded row index
     * in each batch entry is the frame-relative {@code r}, not {@code p}, so
     * downstream {@code computeKeyedBatch} consumers can set
     * {@link PageFrameMemoryRecord#setRowIndex} directly from it.
     * <p>
     * All the reservation and rehash constraints from {@link #probeBatch}
     * apply here as well.
     *
     * @return the baseValueAddress valid for the written batch
     */
    long probeBatchFiltered(
            PageFrameMemoryRecord record,
            RecordSink mapSink,
            long rowIdsAddr,
            long batchStart,
            long batchEnd,
            long batchAddr
    );

    /**
     * Reopens previously closed map with given key capacity and initial heap size.
     * Heap size value is ignored if the map does not use heap to store keys and values, e.g. {@link Unordered8Map}.
     */
    void reopen(int keyCapacity, long heapSize);

    /**
     * Ensures the map can accept up to {@code additionalKeys} new entries
     * while keeping offsets harvested by {@link #probeBatch} stable for the
     * duration of the batch. Implementations whose offsets are invariant
     * across internal growth may treat this as a no-op.
     */
    default void reserveCapacity(long additionalKeys) {
        // no-op by default
    }

    void restoreInitialCapacity();

    /**
     * Initializes the empty value pattern used by {@link #probeBatch} to pre-populate
     * the value region of newly inserted entries. The map allocates an internal scratch
     * buffer the size of one entry's value region, invokes
     * {@link GroupByFunctionsUpdater#updateEmpty(MapValue)} against that buffer, then
     * memcpy's the value region into every new entry created by subsequent
     * {@link #probeBatch} calls.
     * <p>
     * Maps may detect that the resulting empty value is all zeros and skip the per-entry
     * memcpy entirely (since fresh slots are already zeroed by {@link #clear()}).
     * <p>
     * The default implementation is a no-op and is appropriate for maps that do not
     * implement an inlined {@link #probeBatch}; for those maps the per-function loop
     * still calls {@code setEmpty}/{@code computeFirst} on new entries directly.
     */
    default void setBatchEmptyValue(GroupByFunctionsUpdater updater) {
        // no-op
    }

    void setKeyCapacity(int keyCapacity);

    long size();

    MapValue valueAt(long address);

    MapKey withKey();
}
