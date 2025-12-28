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
import java.util.concurrent.atomic.AtomicLong;
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
 * @see #recordWrite(TableToken, long, long, long)
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
            // Uses max(timestamp, walTimestamp) to consider both writer and WAL activity
            writeStats.forEach((key, stats) -> {
                readBuffer.add(key);
                readTimestamps.add(stats.getMaxTimestamp());
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
     * Returns the writer transaction number for a specific table.
     * <p>
     * This is the transaction number from the TableWriter, not the WAL sequence transaction.
     * WAL transactions can be ahead of this value.
     *
     * @param tableToken the table to look up
     * @return writer transaction number, or {@link Numbers#LONG_NULL} if not tracked
     */
    public long getWriterTxn(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        return stats != null ? stats.writerTxn : Numbers.LONG_NULL;
    }

    /**
     * Returns the sequencer transaction number for a specific table.
     * <p>
     * This is the highest sequencerTxn seen from any WAL writer for this table.
     * WAL transactions can be ahead of the writer transaction.
     *
     * @param tableToken the table to look up
     * @return sequencer transaction number, or {@link Numbers#LONG_NULL} if not tracked
     */
    public long getSequencerTxn(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        return stats != null ? stats.getSequencerTxn() : Numbers.LONG_NULL;
    }

    /**
     * Returns the WAL write timestamp for a specific table.
     * <p>
     * This is the highest walTimestamp seen from any WAL writer for this table.
     *
     * @param tableToken the table to look up
     * @return WAL timestamp in microseconds, or {@link Numbers#LONG_NULL} if not tracked
     */
    public long getLastWalTimestamp(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        return stats != null ? stats.getLastWalTimestamp() : Numbers.LONG_NULL;
    }

    /**
     * Records a WAL write, updating the sequencerTxn and walTimestamp fields.
     * <p>
     * This method is called when a WalWriter is returned to the pool. Multiple WAL
     * writers can update the same table concurrently; the highest values win.
     * <p>
     * If no entry exists, creates a "blank" entry with LONG_NULL for writer fields.
     * This blank entry will be overwritten by the real writer when it updates.
     *
     * @param tableToken   the table that was written to
     * @param sequencerTxn the sequencer transaction number
     * @param walTimestamp the max timestamp from the WAL transaction
     */
    public void recordWalWrite(@NotNull TableToken tableToken, long sequencerTxn, long walTimestamp) {
        WriteStats stats = writeStats.get(tableToken);
        if (stats != null) {
            // Entry exists - update WAL fields
            stats.updateWal(sequencerTxn, walTimestamp);
        } else {
            // No entry - create blank entry with only WAL fields set
            WriteStats newStats = new WriteStats(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, sequencerTxn, walTimestamp);
            WriteStats existing = writeStats.putIfAbsent(tableToken, newStats);
            if (existing != null) {
                // Another thread inserted first, update their WAL fields
                existing.updateWal(sequencerTxn, walTimestamp);
            }
        }

        // Lazy eviction
        if (writeStats.size() > maxCapacity * 2) {
            evictOldest();
        }
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
     * @param writerTxn  the writer transaction number (not WAL sequence txn)
     */
    public void recordWrite(@NotNull TableToken tableToken, long timestamp, long rowCount, long writerTxn) {
        WriteStats stats = writeStats.get(tableToken);
        if (stats != null) {
            // Fast path: entry exists, just update writer fields (preserves WAL fields)
            stats.updateWriter(timestamp, rowCount, writerTxn);
        } else {
            // Slow path: need to create new entry (WAL fields start as LONG_NULL)
            WriteStats newStats = new WriteStats(timestamp, rowCount, writerTxn, Numbers.LONG_NULL, Numbers.LONG_NULL);
            WriteStats existing = writeStats.putIfAbsent(tableToken, newStats);
            if (existing != null) {
                // Another thread inserted first, update theirs (preserves their WAL fields)
                existing.updateWriter(timestamp, rowCount, writerTxn);
            }
        }

        // Lazy eviction: only clean up when we exceed 2x capacity
        // This amortizes the cleanup cost and reduces contention
        if (writeStats.size() > maxCapacity * 2) {
            evictOldest();
        }
    }

    /**
     * Records a write only if no entry exists for this table (CAS with expected null).
     * <p>
     * This method is used during hydration to populate the tracker from existing tables
     * without overwriting fresher data from concurrent writers. Writer data always wins
     * over hydrated data.
     *
     * @param tableToken   the table that was written to
     * @param timestamp    the write timestamp in microseconds
     * @param rowCount     the total row count after the write
     * @param writerTxn    the writer transaction number (not WAL sequence txn)
     * @param sequencerTxn the sequencer transaction number from WAL
     * @param walTimestamp the WAL write timestamp in microseconds (use same as timestamp for hydration)
     * @return true if entry was inserted, false if entry already existed
     */
    public boolean recordWriteIfAbsent(@NotNull TableToken tableToken, long timestamp, long rowCount, long writerTxn, long sequencerTxn, long walTimestamp) {
        // Check first to avoid allocation when entry exists
        if (writeStats.containsKey(tableToken)) {
            return false;
        }

        // CAS insert - only succeeds if no entry exists
        WriteStats existing = writeStats.putIfAbsent(tableToken, new WriteStats(timestamp, rowCount, writerTxn, sequencerTxn, walTimestamp));
        if (existing != null) {
            // Another thread (likely a writer) inserted first - their data wins
            return false;
        }

        // Lazy eviction
        if (writeStats.size() > maxCapacity * 2) {
            evictOldest();
        }
        return true;
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
            // Uses max(timestamp, walTimestamp) to consider both writer and WAL activity
            for (int i = 0; i < toEvict; i++) {
                evictCandidateKey = null;
                evictCandidateTimestamp = Long.MAX_VALUE;

                writeStats.forEach((key, stats) -> {
                    long maxTs = stats.getMaxTimestamp();
                    if (maxTs < evictCandidateTimestamp) {
                        evictCandidateTimestamp = maxTs;
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
     * <p>
     * The sequencerTxn field uses AtomicLong for CAS operations since multiple WAL
     * writers can update it concurrently, and the highest value must win.
     */
    public static class WriteStats {
        // WAL fields - updated by WalWriters only, highest wins via CAS
        private final AtomicLong sequencerTxn;
        private volatile long timestamp;
        private volatile long writerTxn;
        private final AtomicLong walTimestamp;
        // Writer fields - updated by TableWriter only
        private volatile long rowCount;

        WriteStats(long timestamp, long rowCount, long writerTxn, long sequencerTxn, long walTimestamp) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
            this.writerTxn = writerTxn;
            this.sequencerTxn = new AtomicLong(sequencerTxn);
            this.walTimestamp = new AtomicLong(walTimestamp);
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
         * Returns the maximum of writer timestamp and WAL timestamp.
         * Used for eviction and sorting - considers both writer and WAL activity.
         * Treats {@link Numbers#LONG_NULL} as Long.MIN_VALUE for comparison.
         *
         * @return max timestamp in microseconds
         */
        public long getMaxTimestamp() {
            long ts = timestamp;
            long walTs = walTimestamp.get();
            // Treat LONG_NULL as very old (it's Long.MIN_VALUE)
            return Math.max(ts, walTs);
        }

        /**
         * Returns the sequencer transaction number from WAL writers.
         * <p>
         * This is the highest sequencer txn seen from any WAL writer for this table.
         * WAL transactions can be ahead of the writer transaction.
         *
         * @return sequencer transaction number, or {@link Numbers#LONG_NULL} if not set
         */
        public long getSequencerTxn() {
            return sequencerTxn.get();
        }

        /**
         * Returns the timestamp of the last TableWriter write in microseconds.
         *
         * @return timestamp in microseconds, or {@link Numbers#LONG_NULL} if not available
         */
        public long getTimestamp() {
            return timestamp;
        }

        /**
         * Returns the timestamp of the last WAL write in microseconds.
         *
         * @return timestamp in microseconds, or {@link Numbers#LONG_NULL} if not available
         */
        public long getLastWalTimestamp() {
            return walTimestamp.get();
        }

        /**
         * Returns the writer transaction number of the last write.
         * <p>
         * This is the transaction number from the TableWriter, not the WAL sequence transaction.
         * WAL transactions can be ahead of this value.
         *
         * @return writer transaction number, or {@link Numbers#LONG_NULL} if not available
         */
        public long getWriterTxn() {
            return writerTxn;
        }

        /**
         * Updates WAL fields using CAS - only succeeds if new values are higher.
         * Multiple WAL writers may call this concurrently; highest values win.
         *
         * @param newTxn          the new sequencer transaction number
         * @param newWalTimestamp the WAL write timestamp in microseconds
         */
        void updateWal(long newTxn, long newWalTimestamp) {
            // CAS update walTimestamp - highest wins
            long currentTs;
            do {
                currentTs = walTimestamp.get();
                if (newWalTimestamp <= currentTs) {
                    break;
                }
            } while (!walTimestamp.compareAndSet(currentTs, newWalTimestamp));

            // CAS update sequencerTxn - highest wins
            long currentTxn;
            do {
                currentTxn = sequencerTxn.get();
                if (newTxn <= currentTxn) {
                    return;
                }
            } while (!sequencerTxn.compareAndSet(currentTxn, newTxn));
        }

        /**
         * Updates writer fields only, preserving sequencerTxn.
         */
        void updateWriter(long timestamp, long rowCount, long writerTxn) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
            this.writerTxn = writerTxn;
        }
    }
}
