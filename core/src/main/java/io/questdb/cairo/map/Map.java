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

import io.questdb.cairo.Reopenable;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public interface Map extends Mutable, Closeable, Reopenable {

    @Override
    void close();

    /**
     * Fast path for {@code count(*)} aggregation over a single fixed-size
     * column key. Reads {@code rowCount} keys from the column data at
     * {@code dataAddr}, hashes and probes the map for each key, and
     * increments a LONG count slot at {@code countByteOffset} within each
     * entry. Bypasses the MapKey/MapValue/RecordSink/FunctionUpdater layers.
     * <p>
     * Throws {@link UnsupportedOperationException} if the map type does not
     * support this fast path.
     */
    default void countByFixedSizeColumn(long dataAddr, long rowCount, int countByteOffset) {
        throw new UnsupportedOperationException();
    }

    /**
     * Filtered variant of {@link #countByFixedSizeColumn}. Reads keys at row
     * indices listed in {@code rowsAddr} (an array of {@code rowCount} 8-byte
     * row indices into the column).
     */
    default void countByFixedSizeColumnFiltered(long dataAddr, long rowsAddr, long rowCount, int countByteOffset) {
        throw new UnsupportedOperationException();
    }

    /**
     * Fast path for {@code SELECT key FROM ... GROUP BY key} (no aggregates)
     * over a single fixed-size column key. Inserts each key from the column
     * into the map without any value updates. See {@link #countByFixedSizeColumn}
     * for parameter semantics.
     */
    default void distinctByFixedSizeColumn(long dataAddr, long rowCount) {
        throw new UnsupportedOperationException();
    }

    /**
     * Filtered variant of {@link #distinctByFixedSizeColumn}.
     */
    default void distinctByFixedSizeColumnFiltered(long dataAddr, long rowsAddr, long rowCount) {
        throw new UnsupportedOperationException();
    }

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

    /**
     * Sharded variant of {@link #countByFixedSizeColumn}. Hashes each key
     * upfront and dispatches it to the right shard map via
     * {@link GroupByMapFragment#getShardMap(long)}.
     */
    default void shardedCountByFixedSizeColumn(long dataAddr, long rowCount, int countByteOffset, GroupByMapFragment fragment) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sharded + filtered variant of {@link #countByFixedSizeColumn}.
     */
    default void shardedCountByFixedSizeColumnFiltered(long dataAddr, long rowsAddr, long rowCount, int countByteOffset, GroupByMapFragment fragment) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sharded variant of {@link #distinctByFixedSizeColumn}.
     */
    default void shardedDistinctByFixedSizeColumn(long dataAddr, long rowCount, GroupByMapFragment fragment) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sharded + filtered variant of {@link #distinctByFixedSizeColumn}.
     */
    default void shardedDistinctByFixedSizeColumnFiltered(long dataAddr, long rowsAddr, long rowCount, GroupByMapFragment fragment) {
        throw new UnsupportedOperationException();
    }

    long size();

    MapValue valueAt(long address);

    MapKey withKey();
}
