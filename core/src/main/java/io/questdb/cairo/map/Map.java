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

    boolean isOpen();

    void merge(Map srcMap, MapValueMergeFunction mergeFunc);

    /**
     * Reopens previously closed map with given key capacity and initial heap size.
     * Heap size value is ignored if the map does not use heap to store keys and values, e.g. {@link Unordered8Map}.
     */
    void reopen(int keyCapacity, long heapSize);

    void restoreInitialCapacity();

    void setKeyCapacity(int keyCapacity);

    long size();

    MapValue valueAt(long address);

    MapKey withKey();

    /**
     * Returns the current base address against which entry offsets from
     * {@link MapValue#getStartOffset()} are interpreted.
     */
    default long getEntryBase() {
        throw new UnsupportedOperationException();
    }

    /**
     * Ensures the map can accept up to {@code additionalKeys} new entries
     * without triggering a rehash that would invalidate harvested offsets.
     */
    default void reserveCapacity(long additionalKeys) {
        // no-op by default
    }

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
            long packed = (r << 39) | ((value.getStartAddress() - getEntryBase()) & 0x7F_FFFF_FFFFL);
            if (value.isNew()) {
                packed |= Long.MIN_VALUE;
            }
            Unsafe.getUnsafe().putLong(batchAddr, packed);
            batchAddr += Long.BYTES;
        }
        return getEntryBase();
    }
}
