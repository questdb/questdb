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

package io.questdb.cairo.idx;


import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.DirectBitSet;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import java.io.Closeable;

public interface IndexReader extends Closeable {

    int DIR_BACKWARD = 2;
    int DIR_FORWARD = 1;

    int MAX_CACHED_FREE_CURSORS = 128;
    String NAME_BACKWARD = "backward";
    String NAME_FORWARD = "forward";

    static CharSequence nameOf(int direction) {
        return DIR_FORWARD == direction ? NAME_FORWARD : NAME_BACKWARD;
    }

    @Override
    default void close() {
    }

    /**
     * Bulk-marks all keys present in this partition into the bit set.
     * Walks dense generation strides sequentially (one pass, optimal page access).
     * Returns the number of newly found keys.
     * For bitmap indexes this is a no-op returning -1 (caller uses getCursor fallback).
     *
     * @param foundKeys bit set indexed by index key (0 = NULL, 1..N = symbol keys)
     * @return number of keys newly marked as found, or -1 if not supported
     */
    default int collectDistinctKeys(DirectBitSet foundKeys) {
        return -1;
    }

    default int collectDistinctKeysInRange(DirectBitSet foundKeys, long rowLo, long rowHi) {
        return -1;
    }

    long getColumnTop();

    long getColumnTxn();

    /**
     * Acquire a value cursor bounded by the given min/max (inclusive). The returned
     * cursor must be {@link RowCursor#close() closed} when iteration is done — prefer
     * try-with-resources. On close, pool-backed cursors return to a free list for
     * reuse; stateless cursors are no-op. Order of values is determined by specific
     * implementations of this method.
     *
     * @param key      index key
     * @param minValue inclusive minimum value
     * @param maxValue inclusive maximum value
     * @return index value cursor, relative to the minValue
     */
    RowCursor getCursor(int key, long minValue, long maxValue);

    default RowCursor getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        return getCursor(key, minValue, maxValue);
    }

    /**
     * Acquire a NON-pooled covering cursor that a single worker thread owns
     * outright, for use during parallel covered decode. Unlike {@link #getCursor},
     * the returned cursor is never drawn from / returned to the reader's shared
     * free-cursor pool, so N such cursors are safe to iterate concurrently over
     * ONE reader provided the reader was made read-only first (warmed + frozen).
     * Positioning matches {@link #getCursor(int, long, long, int[])}; only the
     * construct/close lifecycle differs. Implemented by the forward posting
     * reader (the reader carried on covered frames); other readers do not support
     * it.
     */
    default RowCursor getDetachedCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        throw new UnsupportedOperationException();
    }

    default IndexFrameCursor getFrameCursor(int key, long minValue, long maxValue) {
        throw new UnsupportedOperationException();
    }

    long getKeyBaseAddress();

    int getKeyCount();

    long getKeyMemorySize();

    long getPartitionTxn();

    long getValueBaseAddress();

    int getValueBlockCapacity();

    long getValueMemorySize();

    boolean isOpen();

    /**
     * Whether this reader is currently frozen (see {@link #setFrozen(boolean)}). Readers that do
     * not support freezing always report {@code false}. Exposed so the parallel-decode freeze
     * wiring ({@code PageFrameAddressCache.freezeCoveredReaders}) can be verified directly.
     */
    default boolean isFrozen() {
        return false;
    }

    void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    );

    void reloadConditionally();

    /**
     * While frozen, {@link #reloadConditionally()} must be a no-op so the
     * value / sidecar mmaps stay stable for in-flight worker cursors that hold
     * raw page addresses into them. The parallel covered-decode pipeline freezes
     * each per-partition posting reader after it has been positioned at the query
     * txn and warmed (so the mmaps are at their final extents) and unfreezes it
     * once all worker cursors have finished. Readers that hold no in-flight
     * worker state (bitmap, null) ignore the flag.
     */
    default void setFrozen(boolean frozen) {
    }

    /**
     * Pin the reader at the given table {@code _txn} for snapshot isolation.
     * The posting reader's picker selects the chain entry with the highest
     * {@code txnAtSeal <= pinnedTableTxn} and trims in-flight tail gens
     * within that entry. Bitmap and null readers ignore the pin.
     * <p>
     * The pin takes effect on the next {@link #reloadConditionally} or
     * {@link #of}; the setter itself does not re-pick.
     */
    default void setPinnedTableTxn(long pinnedTableTxn) {
    }
}
