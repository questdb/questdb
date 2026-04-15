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

package io.questdb.cutlass.line.tcp;

import io.questdb.std.IntIntHashMap;

import java.util.Arrays;

/**
 * A cache that maps client symbol IDs to table-specific symbol IDs for a single symbol column.
 * <p>
 * Uses a hybrid strategy: a flat int[] array for IDs in [0, ARRAY_THRESHOLD) — the common
 * case with monotonically increasing client dictionaries — and falls back to an IntIntHashMap
 * for any IDs at or above the threshold. The array path gives O(1) lookups with no hashing,
 * no probe chains, and sequential-access-friendly memory layout.
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

    /**
     * IDs below this use the flat array; IDs at or above fall back to the hash map.
     */
    private static final int ARRAY_THRESHOLD = 10_000;

    // Fast path: flat array for small, dense IDs.
    private int[] values;

    // Slow path: hash map for sparse / large IDs.
    private IntIntHashMap overflow;

    private int size;

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
     * @param initialCapacity the initial capacity (for the array portion)
     */
    public ClientSymbolCache(int initialCapacity) {
        this.values = new int[Math.max(initialCapacity, 16)];
        Arrays.fill(this.values, NO_ENTRY);
        this.size = 0;
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
            clearEntries();
            return true;
        }

        return false;
    }

    /**
     * Removes all entries from the cache.
     */
    public void clear() {
        clearEntries();
        lastKnownWatermark = -1;
    }

    /**
     * Returns the table symbol ID for the given client symbol ID, or {@link #NO_ENTRY} if not cached.
     *
     * @param clientSymbolId the client-side symbol dictionary index
     * @return the table symbol ID, or {@link #NO_ENTRY} if not found
     */
    public int get(int clientSymbolId) {
        if (clientSymbolId >= 0 && clientSymbolId < ARRAY_THRESHOLD) {
            if (clientSymbolId < values.length) {
                return values[clientSymbolId];
            }
            return NO_ENTRY;
        }
        // Overflow path
        if (overflow != null) {
            return overflow.get(clientSymbolId);
        }
        return NO_ENTRY;
    }

    /**
     * Returns the last known watermark value.
     *
     * @return the last known watermark
     */
    public int getLastKnownWatermark() {
        return lastKnownWatermark;
    }

    /**
     * Stores a mapping from client symbol ID to table symbol ID.
     *
     * @param clientSymbolId the client-side symbol dictionary index
     * @param tableSymbolId  the table's internal symbol ID
     */
    public void put(int clientSymbolId, int tableSymbolId) {
        if (clientSymbolId >= 0 && clientSymbolId < ARRAY_THRESHOLD) {
            if (clientSymbolId >= values.length) {
                grow(clientSymbolId + 1);
            }
            if (values[clientSymbolId] == NO_ENTRY) {
                size++;
            }
            values[clientSymbolId] = tableSymbolId;
        } else {
            if (overflow == null) {
                overflow = new IntIntHashMap(DEFAULT_INITIAL_CAPACITY);
            }
            int idx = overflow.keyIndex(clientSymbolId);
            if (idx >= 0) {
                size++;
            }
            overflow.putAt(idx, clientSymbolId, tableSymbolId);
        }
    }

    /**
     * Returns the number of cached entries.
     *
     * @return the cache size
     */
    public int size() {
        return size;
    }

    private void clearEntries() {
        Arrays.fill(values, 0, values.length, NO_ENTRY);
        if (overflow != null) {
            overflow.clear();
        }
        size = 0;
    }

    private void grow(int minCapacity) {
        int newLen = Math.max(values.length * 2, minCapacity);
        if (newLen > ARRAY_THRESHOLD) {
            newLen = ARRAY_THRESHOLD;
        }
        int[] newValues = new int[newLen];
        System.arraycopy(values, 0, newValues, 0, values.length);
        Arrays.fill(newValues, values.length, newLen, NO_ENTRY);
        values = newValues;
    }
}
