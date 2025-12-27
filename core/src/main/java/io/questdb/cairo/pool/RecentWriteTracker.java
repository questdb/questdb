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

package io.questdb.cairo.pool;

import io.questdb.cairo.TableToken;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks recently written tables with their last write timestamps and row counts.
 * <p>
 * This data structure is optimized for concurrent writes with minimal contention.
 * Multiple writers can record writes simultaneously without blocking each other.
 * <p>
 * <b>Design Goals:</b>
 * <ul>
 *   <li>Zero contention on write path (different tables can be updated concurrently)</li>
 *   <li>Minimal allocation on write path (reuses WriteStats objects for existing entries)</li>
 *   <li>Bounded memory usage via lazy eviction at 2x capacity</li>
 * </ul>
 * <p>
 * <b>Limitations:</b>
 * <ul>
 *   <li><b>No persistence:</b> Tracking data is lost on server restart</li>
 *   <li><b>Row count accuracy:</b> Row count reflects the state at last write time.
 *       It may become stale if rows are deleted without subsequent writes.</li>
 *   <li><b>Eventual consistency:</b> Readers may momentarily see inconsistent state
 *       where timestamp and rowCount are from different writes.</li>
 *   <li><b>Table drops:</b> Entries for dropped tables remain until evicted.
 *       Call {@link #removeTable(TableToken)} explicitly when dropping tables.</li>
 * </ul>
 *
 * @see #recordWrite(TableToken, long, long)
 * @see #getRecentlyWrittenTables(int)
 */
public class RecentWriteTracker {

    // Eviction state - protected by evictionInProgress flag
    // Using instance fields avoids allocation during eviction
    private final AtomicBoolean evictionInProgress = new AtomicBoolean(false);
    private final int maxCapacity;
    private final ObjList<TableToken> readBuffer;
    // Read path state - protected by readLock
    // Pre-allocated buffers for getRecentlyWrittenTables() to minimize allocation
    private final ReentrantLock readLock = new ReentrantLock();
    private final LongList readTimestamps;
    // ConcurrentHashMap provides lock-free reads and fine-grained locking for writes
    // Different keys can be updated concurrently without contention
    private final ConcurrentHashMap<TableToken, WriteStats> writeStats = new ConcurrentHashMap<>();
    private TableToken evictCandidateKey;
    private long evictCandidateTimestamp;

    /**
     * Creates a new tracker with the specified maximum capacity.
     *
     * @param maxCapacity maximum number of tables to track (must be positive)
     */
    public RecentWriteTracker(int maxCapacity) {
        this.maxCapacity = Math.max(1, maxCapacity);
        // Pre-allocate buffers for read path
        this.readBuffer = new ObjList<>(this.maxCapacity);
        this.readTimestamps = new LongList(this.maxCapacity);
    }

    /**
     * Clears all tracking data.
     */
    public void clear() {
        writeStats.clear();
    }

    /**
     * Returns the configured maximum capacity.
     */
    public int getMaxCapacity() {
        return maxCapacity;
    }

    /**
     * Returns the most recently written tables, sorted by write time (most recent first).
     * <p>
     * This method takes a snapshot of the current state and returns a sorted list.
     * It uses pre-allocated buffers to minimize allocation, but requires a lock
     * to protect the shared buffers.
     * <p>
     * <b>Thread Safety:</b> This method is thread-safe but may block if another
     * thread is also reading. Writes are not blocked.
     *
     * @param limit maximum number of tables to return
     * @return new list of TableTokens sorted by write time (most recent first)
     */
    @NotNull
    public ObjList<TableToken> getRecentlyWrittenTables(int limit) {
        int effectiveLimit = Math.min(limit, maxCapacity);
        ObjList<TableToken> result = new ObjList<>(effectiveLimit);

        if (writeStats.isEmpty()) {
            return result;
        }

        readLock.lock();
        try {
            // Clear and populate buffers
            readBuffer.clear();
            readTimestamps.clear();

            // Collect all entries into buffers
            // Note: forEach on ConcurrentHashMap has small internal allocations for traversal
            writeStats.forEach((key, stats) -> {
                readBuffer.add(key);
                readTimestamps.add(stats.timestamp);
            });

            int size = readBuffer.size();
            if (size == 0) {
                return result;
            }

            // Sort by timestamp descending using indices
            // We sort timestamps and reorder tokens accordingly
            if (size > 1) {
                quickSortDescending(readBuffer, readTimestamps, 0, size - 1);
            }

            // Copy top N to result
            int toCopy = Math.min(effectiveLimit, size);
            for (int i = 0; i < toCopy; i++) {
                result.add(readBuffer.getQuick(i));
            }
        } finally {
            readLock.unlock();
        }

        return result;
    }

    /**
     * Returns the row count for a specific table.
     *
     * @param tableToken the table to look up
     * @return row count, or {@link Numbers#LONG_NULL} if not tracked
     */
    public long getRowCount(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        return stats != null ? stats.rowCount : Numbers.LONG_NULL;
    }

    /**
     * Returns the write statistics for a specific table, or null if not tracked.
     *
     * @param tableToken the table to look up
     * @return WriteStats containing timestamp and row count, or null if not found
     */
    public WriteStats getWriteStats(@NotNull TableToken tableToken) {
        return writeStats.get(tableToken);
    }

    /**
     * Returns the write timestamp for a specific table.
     *
     * @param tableToken the table to look up
     * @return timestamp in microseconds, or {@link Numbers#LONG_NULL} if not tracked
     */
    public long getWriteTimestamp(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        return stats != null ? stats.timestamp : Numbers.LONG_NULL;
    }

    /**
     * Records a write to the specified table.
     * <p>
     * This method is thread-safe and optimized for minimal contention.
     * Multiple concurrent calls with different table tokens will not block each other.
     * For existing entries, this method performs zero allocations by reusing the
     * existing {@link WriteStats} object.
     *
     * @param tableToken the table that was written to
     * @param timestamp  the write timestamp in microseconds
     * @param rowCount   the total row count after the write
     */
    public void recordWrite(@NotNull TableToken tableToken, long timestamp, long rowCount) {
        WriteStats stats = writeStats.get(tableToken);
        if (stats != null) {
            // Fast path: entry exists, just update (zero allocation)
            stats.update(timestamp, rowCount);
        } else {
            // Slow path: need to create new entry
            WriteStats newStats = new WriteStats(timestamp, rowCount);
            WriteStats existing = writeStats.putIfAbsent(tableToken, newStats);
            if (existing != null) {
                // Another thread inserted first, update theirs
                existing.update(timestamp, rowCount);
            }
        }

        // Lazy eviction: only clean up when we exceed 2x capacity
        // This amortizes the cleanup cost and reduces contention
        if (writeStats.size() > maxCapacity * 2) {
            evictOldest();
        }
    }

    /**
     * Removes tracking for a specific table.
     * <p>
     * This should be called when a table is dropped to prevent stale entries
     * from accumulating in the tracker.
     *
     * @param tableToken the table to stop tracking
     */
    public void removeTable(@NotNull TableToken tableToken) {
        writeStats.remove(tableToken);
    }

    /**
     * Returns the current number of tracked tables.
     */
    @TestOnly
    public int size() {
        return writeStats.size();
    }

    /**
     * Evicts the oldest entries to bring size down to maxCapacity.
     * This method is called lazily when size exceeds 2x capacity.
     * <p>
     * Uses a simple O(n*k) algorithm where k is the number of entries to evict.
     * Since eviction is rare (only at 2x capacity) and k is typically small,
     * this approach avoids heap allocation while maintaining acceptable performance.
     * <p>
     * <b>Note:</b> ConcurrentHashMap.forEach() has small internal allocations for
     * traversal state. This is acceptable since eviction is infrequent.
     */
    private void evictOldest() {
        // Try to acquire eviction lock - if another thread is evicting, skip
        if (!evictionInProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            int currentSize = writeStats.size();
            if (currentSize <= maxCapacity) {
                return;
            }

            int toEvict = currentSize - maxCapacity;

            // Simple O(n*k) approach: find and remove the oldest entry k times
            for (int i = 0; i < toEvict; i++) {
                evictCandidateKey = null;
                evictCandidateTimestamp = Long.MAX_VALUE;

                writeStats.forEach((key, stats) -> {
                    if (stats.timestamp < evictCandidateTimestamp) {
                        evictCandidateTimestamp = stats.timestamp;
                        evictCandidateKey = key;
                    }
                });

                if (evictCandidateKey != null) {
                    writeStats.remove(evictCandidateKey);
                }
            }
        } finally {
            evictionInProgress.set(false);
        }
    }

    private int partitionDescending(ObjList<TableToken> tokens, LongList timestamps, int lo, int hi) {
        long pivot = timestamps.getQuick(hi);
        int i = lo - 1;

        for (int j = lo; j < hi; j++) {
            // Descending order: greater values come first
            if (timestamps.getQuick(j) > pivot) {
                i++;
                swap(tokens, timestamps, i, j);
            }
        }

        swap(tokens, timestamps, i + 1, hi);
        return i + 1;
    }

    /**
     * Quick sort descending by timestamp, reordering tokens accordingly.
     */
    private void quickSortDescending(ObjList<TableToken> tokens, LongList timestamps, int lo, int hi) {
        if (lo >= hi) {
            return;
        }

        int p = partitionDescending(tokens, timestamps, lo, hi);
        quickSortDescending(tokens, timestamps, lo, p - 1);
        quickSortDescending(tokens, timestamps, p + 1, hi);
    }

    private void swap(ObjList<TableToken> tokens, LongList timestamps, int i, int j) {
        if (i != j) {
            TableToken tmpToken = tokens.getQuick(i);
            tokens.setQuick(i, tokens.getQuick(j));
            tokens.setQuick(j, tmpToken);

            long tmpTs = timestamps.getQuick(i);
            timestamps.setQuick(i, timestamps.getQuick(j));
            timestamps.setQuick(j, tmpTs);
        }
    }

    /**
     * Holds statistics for a single table write.
     * <p>
     * Uses volatile fields for lock-free concurrent access. This allows zero-allocation
     * updates for existing entries, but means readers may see momentarily inconsistent
     * state between fields (e.g., timestamp from write A, rowCount from write B).
     * This trade-off is acceptable for observability use cases.
     */
    public static class WriteStats {
        private volatile long rowCount;
        private volatile long timestamp;

        WriteStats(long timestamp, long rowCount) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
        }

        /**
         * Returns the row count at the time of the last write.
         * <p>
         * <b>Note:</b> This value may be stale if rows were deleted after the last write.
         * Due to lock-free updates, this value may also be momentarily inconsistent
         * with {@link #getTimestamp()} during concurrent writes.
         *
         * @return row count, or {@link Numbers#LONG_NULL} if not available
         */
        public long getRowCount() {
            return rowCount;
        }

        /**
         * Returns the timestamp of the last write in microseconds.
         *
         * @return timestamp in microseconds, or {@link Numbers#LONG_NULL} if not available
         */
        public long getTimestamp() {
            return timestamp;
        }

        void update(long timestamp, long rowCount) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
        }
    }
}
