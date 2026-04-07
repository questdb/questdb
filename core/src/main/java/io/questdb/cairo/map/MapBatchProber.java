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

import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.Reopenable;

/**
 * Map-specific helper for batched hash computation and prefetching.
 * <p>
 * Packs keys from records via {@link RecordSinkSPI}, computes hashes in a
 * native batch call with prefetch hints, and probes the owning map using
 * precomputed hashes.
 * <p>
 * Instances are created by {@link Map#createBatchProber(int)} and are tightly
 * coupled to the creating map's internal layout.
 */
public interface MapBatchProber extends RecordSinkSPI, Reopenable {

    /**
     * Marks the start of a new key in the batch. Must be called before
     * writing key columns via the {@link RecordSinkSPI} put methods.
     * No-op for fixed-size key probers.
     */
    default void beginKey() {
    }

    /**
     * Marks the end of the current key in the batch. Must be called after
     * writing all key columns. No-op for fixed-size key probers.
     */
    default void endKey() {
    }

    /**
     * Returns the precomputed hash for the key at the given batch index.
     * Must be called after {@link #hashAndPrefetch(int)}.
     */
    long getHash(int index);

    /**
     * Computes hashes for the packed keys and prefetches the corresponding
     * map slots. Must be called after packing {@code keyCount} keys via
     * the {@link RecordSinkSPI} put methods.
     */
    void hashAndPrefetch(int keyCount);

    /**
     * Probes the owning map with the precomputed hash for the key at the
     * given batch index. Returns the map value; {@link MapValue#isNew()}
     * indicates whether the key was inserted.
     */
    MapValue probeWithHash(int index);

    /**
     * Resets the write position to start packing a new batch of keys.
     */
    void resetBatch();
}
