/*******************************************************************************
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

package io.questdb.cutlass.line.tcp;

import io.questdb.std.IntIntHashMap;

/**
 * A cache that maps client symbol IDs to table-specific symbol IDs for a single symbol column.
 * <p>
 * This cache is used to avoid repeated string lookups when the same symbols are used
 * across multiple rows. The cache stores mappings from client-side symbol dictionary
 * indices to the table's internal symbol IDs.
 * <p>
 * Thread safety: This class is NOT thread-safe. It should only be accessed from a single thread.
 * <p>
 * Cache invalidation: When the symbol watermark changes (e.g., after WAL segment rollover),
 * the cache should be cleared using {@link #checkAndInvalidate(int)}.
 */
public class ClientSymbolCache {

    /**
     * Value returned when a key is not found in the cache.
     */
    public static final int NO_ENTRY = -1;

    private static final int DEFAULT_INITIAL_CAPACITY = 64;

    private final IntIntHashMap cache;

    // Tracks the last known watermark for invalidation detection
    private int lastKnownWatermark = -1;

    /**
     * Creates a new cache with default initial capacity.
     */
    public ClientSymbolCache() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * Creates a new cache with the specified initial capacity.
     *
     * @param initialCapacity the initial capacity
     */
    public ClientSymbolCache(int initialCapacity) {
        this.cache = new IntIntHashMap(initialCapacity);
    }

    /**
     * Returns the table symbol ID for the given client symbol ID, or {@link #NO_ENTRY} if not cached.
     *
     * @param clientSymbolId the client-side symbol dictionary index
     * @return the table symbol ID, or {@link #NO_ENTRY} if not found
     */
    public int get(int clientSymbolId) {
        return cache.get(clientSymbolId);
    }

    /**
     * Stores a mapping from client symbol ID to table symbol ID.
     *
     * @param clientSymbolId the client-side symbol dictionary index
     * @param tableSymbolId  the table's internal symbol ID
     */
    public void put(int clientSymbolId, int tableSymbolId) {
        cache.put(clientSymbolId, tableSymbolId);
    }

    /**
     * Removes all entries from the cache.
     */
    public void clear() {
        cache.clear();
        lastKnownWatermark = -1;
    }

    /**
     * Returns the number of cached entries.
     *
     * @return the cache size
     */
    public int size() {
        return cache.size();
    }

    /**
     * Checks if the watermark has changed and clears the cache if needed.
     * <p>
     * This method should be called before each lookup with the current watermark.
     * If the watermark has changed, the entire cache is cleared as a conservative
     * approach - this is simpler and the cache will warm up quickly.
     *
     * @param currentWatermark the current symbol count watermark
     * @return true if cache was cleared, false otherwise
     */
    public boolean checkAndInvalidate(int currentWatermark) {
        if (lastKnownWatermark == -1) {
            // First call, just record the watermark
            lastKnownWatermark = currentWatermark;
            return false;
        }

        if (currentWatermark != lastKnownWatermark) {
            // Watermark changed - clear the cache
            // This is conservative but safe - the cache will warm up again
            lastKnownWatermark = currentWatermark;
            cache.clear();
            return true;
        }

        return false;
    }

    /**
     * Returns the last known watermark value.
     * Used for testing and debugging.
     *
     * @return the last known watermark
     */
    public int getLastKnownWatermark() {
        return lastKnownWatermark;
    }
}
