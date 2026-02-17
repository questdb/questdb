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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
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
    private static final Log LOG = LogFactory.getLog(RecentWriteTracker.class);

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
     * Returns the configured maximum capacity.
     */
    public int getMaxCapacity() {
        return maxCapacity;
    }

    public WriteStats getOrCreateStats(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        if (stats == null) {
            WriteStats newStats = new WriteStats(Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL, Numbers.LONG_NULL);
            WriteStats existing = writeStats.putIfAbsent(tableToken, newStats);
            stats = existing != null ? existing : newStats;
        }
        return stats;
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
     * Returns the total number of WAL transactions recorded for a specific table.
     *
     * @param tableToken the table to look up
     * @return total transaction count, or 0 if not tracked
     */
    public long getTxnCount(@NotNull TableToken tableToken) {
        WriteStats stats = writeStats.get(tableToken);
        return stats != null ? stats.getTxnCount() : 0;
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
     * Records merge statistics for a specific table including write amplification,
     * throughput, and table timestamps.
     * <p>
     * This is a convenience method that records all merge metrics in a single call,
     * avoiding redundant map lookups and using a single lock acquisition.
     *
     * @param tableToken        the table that was written to
     * @param amplification     the write amplification ratio (physicalRows / logicalRows)
     * @param throughput        the throughput in rows/second
     * @param tableMinTimestamp the minimum timestamp of data in the table
     * @param tableMaxTimestamp the maximum timestamp of data in the table
     */
    public void recordMergeStats(@NotNull TableToken tableToken, double amplification, long throughput, long tableMinTimestamp, long tableMaxTimestamp) {
        try {
            getOrCreateStats(tableToken).recordMergeStats(amplification, throughput, tableMinTimestamp, tableMaxTimestamp);
        } catch (Throwable th) {
            LOG.error().$("could not record merge stats [table=").$(tableToken).$(", error=").$(th).I$();
        }
    }

    /**
     * Records that WAL rows have been processed (applied to the table).
     * <p>
     * This decrements the pending WAL row count (if seqTxn > floor) and accumulates dedup row count.
     * Called after successful WAL transaction application.
     *
     * @param tableToken       the table whose WAL was processed
     * @param seqTxn           the sequencer transaction number being processed
     * @param walRowCount      the number of WAL rows that were processed (before dedup)
     * @param dedupRowsRemoved the number of duplicate rows removed during dedup
     */
    public void recordWalProcessed(@NotNull TableToken tableToken, long seqTxn, long walRowCount, long dedupRowsRemoved) {
        try {
            WriteStats stats = getOrCreateStats(tableToken);
            // Only subtract if seqTxn is above the floor (rows added after tracking started)
            if (seqTxn > stats.getFloorSeqTxn()) {
                stats.walRowCount.add(-walRowCount);
            }
            if (dedupRowsRemoved > 0) {
                stats.dedupRowCount.add(dedupRowsRemoved);
            }
        } catch (Throwable th) {
            LOG.error().$("could not record WAL processed [table=").$(tableToken).$(", error=").$(th).I$();
        }
    }

    /**
     * Records a replica download batch, updating sequencerTxn, walTimestamp, and walRowCount.
     * <p>
     * This method is called by replicas when WAL data is downloaded from the object store.
     * Unlike {@link #recordWalWrite}, this does NOT record to the transaction size histogram
     * because replicas download data in batches that may contain multiple transactions.
     * Instead, it records to a separate batch size histogram for replica-specific metrics.
     * <p>
     * The highest values win for sequencerTxn and walTimestamp, while batchRowCount is accumulated.
     *
     * @param tableToken    the table that was downloaded
     * @param sequencerTxn  the sequencer transaction number (last txn in the batch)
     * @param walTimestamp  the max timestamp from the downloaded WAL data
     * @param batchRowCount the total number of rows in this download batch
     * @param morePending   true if the download batch was limited and more data is available
     */
    @SuppressWarnings("unused")
    public void recordReplicaDownload(@NotNull TableToken tableToken, long sequencerTxn, long walTimestamp, long batchRowCount, boolean morePending) {
        try {
            getOrCreateStats(tableToken).updateReplicaDownload(sequencerTxn, walTimestamp, batchRowCount, morePending);

            // Lazy eviction
            if (writeStats.size() > maxCapacity * 2) {
                evictOldest();
            }
        } catch (Throwable th) {
            LOG.error().$("could not record replica download [table=").$(tableToken).$(", error=").$(th).I$();
        }
    }

    /**
     * Records a WAL write, updating the sequencerTxn, walTimestamp, and walRowCount fields.
     * <p>
     * This method is called when a WalWriter is returned to the pool. Multiple WAL
     * writers can update the same table concurrently; the highest values win for
     * sequencerTxn and walTimestamp, while txnRowCount is accumulated.
     * <p>
     * If no entry exists, creates a "blank" entry with LONG_NULL for writer fields.
     * This blank entry will be overwritten by the real writer when it updates.
     *
     * @param tableToken   the table that was written to
     * @param sequencerTxn the sequencer transaction number
     * @param walTimestamp the max timestamp from the WAL transaction
     * @param txnRowCount  the number of rows in this WAL segment
     */
    public void recordWalWrite(@NotNull TableToken tableToken, long sequencerTxn, long walTimestamp, long txnRowCount) {
        try {
            getOrCreateStats(tableToken).updateWal(sequencerTxn, walTimestamp, txnRowCount);

            // Lazy eviction
            if (writeStats.size() > maxCapacity * 2) {
                evictOldest();
            }
        } catch (Throwable th) {
            LOG.error().$("could not record WAL write [table=").$(tableToken).$(", error=").$(th).I$();
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
     * @param tableToken     the table that was written to
     * @param writeTimestamp the write timestamp in microseconds
     * @param rowCount       the total row count after the write
     * @param writerTxn      the writer transaction number (not WAL sequence txn)
     */
    public void recordWrite(@NotNull TableToken tableToken, long writeTimestamp, long rowCount, long writerTxn) {
        try {
            getOrCreateStats(tableToken).updateWriter(writeTimestamp, rowCount, writerTxn);

            // Lazy eviction: only clean up when we exceed 2x capacity
            // This amortizes the cleanup cost and reduces contention
            if (writeStats.size() > maxCapacity * 2) {
                evictOldest();
            }
        } catch (Throwable th) {
            LOG.error().$("could not record write [table=").$(tableToken).$(", error=").$(th).I$();
        }
    }

    /**
     * Records a write only if no entry exists for this table (CAS with expected null).
     * <p>
     * This method is used during hydration to populate the tracker from existing tables
     * without overwriting fresher data from concurrent writers. Writer data always wins
     * over hydrated data.
     *
     * @param tableToken        the table that was written to
     * @param writeTimestamp    the write timestamp in microseconds
     * @param rowCount          the total row count after the write
     * @param writerTxn         the writer transaction number (not WAL sequence txn)
     * @param sequencerTxn      the sequencer transaction number from WAL
     * @param walTimestamp      the WAL write timestamp in microseconds (use same as writeTimestamp for hydration)
     * @param tableMinTimestamp the minimum timestamp of data in the table
     * @param tableMaxTimestamp the maximum timestamp of data in the table
     * @return true if entry was inserted, false if entry already existed
     */
    public boolean recordWriteIfAbsent(
            @NotNull TableToken tableToken,
            long writeTimestamp,
            long rowCount,
            long writerTxn,
            long sequencerTxn,
            long walTimestamp,
            long tableMinTimestamp,
            long tableMaxTimestamp
    ) {
        try {
            // Check first to avoid allocation when entry exists
            if (writeStats.containsKey(tableToken)) {
                return false;
            }

            // CAS insert - only succeeds if no entry exists
            WriteStats existing = writeStats.putIfAbsent(
                    tableToken,
                    new WriteStats(writeTimestamp, rowCount, writerTxn, sequencerTxn, walTimestamp, tableMinTimestamp, tableMaxTimestamp)
            );
            if (existing != null) {
                // Another thread (likely a writer) inserted first - their data wins
                return false;
            }

            // Lazy eviction
            if (writeStats.size() > maxCapacity * 2) {
                evictOldest();
            }
            return true;
        } catch (Throwable th) {
            LOG.error().$("could not record write if absent [table=").$(tableToken).$(", error=").$(th).I$();
            return false;
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
     * Sets the floor seqTxn for a table. WAL rows with seqTxn &lt;= floor will not be subtracted
     * from pending count since they were never added (existed before tracking started).
     * Called on startup for tables with pending WALs. Creates WriteStats if it doesn't exist.
     *
     * @param tableToken  the table to set floor for
     * @param floorSeqTxn the floor seqTxn value
     */
    public void setFloorSeqTxn(@NotNull TableToken tableToken, long floorSeqTxn) {
        getOrCreateStats(tableToken).setFloorSeqTxn(floorSeqTxn);
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
                TableToken oldestKey = null;
                long oldestTimestamp = Long.MAX_VALUE;

                for (Map.Entry<TableToken, WriteStats> entry : writeStats.entrySet()) {
                    long maxTs = entry.getValue().getMaxTimestamp();
                    if (maxTs < oldestTimestamp) {
                        oldestTimestamp = maxTs;
                        oldestKey = entry.getKey();
                    }
                }

                if (oldestKey != null) {
                    writeStats.remove(oldestKey);
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
        // Replica batch size histogram - tracks distribution of download batch sizes (replica only)
        private final Histogram batchSizeHistogram = new Histogram(2);
        // Lock for batch size histogram access
        private final SimpleReadWriteLock batchSizeHistogramLock = new SimpleReadWriteLock();
        // Dedup row count - accumulated count of rows removed by deduplication
        private final LongAdder dedupRowCount = new LongAdder();
        // Floor seqTxn - set on startup, WAL rows with seqTxn <= floor are not subtracted
        private final AtomicLong floorSeqTxn = new AtomicLong(0);
        // Lock for merge stats (write amplification and merge throughput) access
        private final SimpleReadWriteLock mergeStatsLock = new SimpleReadWriteLock();
        // Merge throughput histogram - tracks rows/second during WAL apply
        private final Histogram mergeThroughputHistogram = new Histogram(2);
        // Replica more pending flag - true if download batch was limited and more data is available
        private final AtomicBoolean replicaMorePending = new AtomicBoolean(false);
        // WAL fields - updated by WalWriters only, highest wins via CAS
        private final AtomicLong sequencerTxn;
        // Transaction size histogram - tracks distribution of WAL transaction sizes
        private final Histogram txnSizeHistogram = new Histogram(2);
        // Lock for transaction size histogram access
        private final SimpleReadWriteLock txnSizeHistogramLock = new SimpleReadWriteLock();
        // WAL row count - incremented by WalWriters, contention-free via LongAdder
        private final LongAdder walRowCount = new LongAdder();
        private final AtomicLong walTimestamp;
        // Write amplification histogram - tracks ratio of physical rows written to logical rows
        private final DoubleHistogram writeAmplificationHistogram = new DoubleHistogram(2);
        // Table data timestamps - min/max timestamp values of actual data in the table (updated during WAL merge)
        // Protected by mergeStatsLock
        private long tableMaxTimestamp;
        private long tableMinTimestamp;
        // Writer fields - updated by TableWriter only
        private volatile long rowCount;
        private volatile long timestamp;
        private volatile long writerTxn;

        WriteStats(long timestamp, long rowCount, long writerTxn, long sequencerTxn, long walTimestamp, long tableMinTimestamp, long tableMaxTimestamp) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
            this.writerTxn = writerTxn;
            this.sequencerTxn = new AtomicLong(sequencerTxn);
            this.walTimestamp = new AtomicLong(walTimestamp);
            this.tableMinTimestamp = tableMinTimestamp;
            this.tableMaxTimestamp = tableMaxTimestamp;
            this.batchSizeHistogram.setAutoResize(true);
            this.mergeThroughputHistogram.setAutoResize(true);
            this.txnSizeHistogram.setAutoResize(true);
            this.writeAmplificationHistogram.setAutoResize(true);
        }

        /**
         * Returns the total number of replica download batches recorded.
         * <p>
         * This is only populated on replicas via {@link #recordReplicaDownload}.
         *
         * @return total batch count
         */
        public long getBatchCount() {
            batchSizeHistogramLock.readLock().lock();
            try {
                return batchSizeHistogram.getTotalCount();
            } finally {
                batchSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the maximum batch size recorded (replica only).
         *
         * @return maximum batch size in rows, or 0 if no batches recorded
         */
        public long getBatchSizeMax() {
            batchSizeHistogramLock.readLock().lock();
            try {
                return batchSizeHistogram.getMaxValue();
            } finally {
                batchSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the 50th percentile (median) batch size (replica only).
         *
         * @return median batch size in rows, or 0 if no batches recorded
         */
        public long getBatchSizeP50() {
            batchSizeHistogramLock.readLock().lock();
            try {
                return batchSizeHistogram.getValueAtPercentile(50);
            } finally {
                batchSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the 90th percentile batch size (replica only).
         *
         * @return 90th percentile batch size in rows, or 0 if no batches recorded
         */
        public long getBatchSizeP90() {
            batchSizeHistogramLock.readLock().lock();
            try {
                return batchSizeHistogram.getValueAtPercentile(90);
            } finally {
                batchSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the 99th percentile batch size (replica only).
         *
         * @return 99th percentile batch size in rows, or 0 if no batches recorded
         */
        public long getBatchSizeP99() {
            batchSizeHistogramLock.readLock().lock();
            try {
                return batchSizeHistogram.getValueAtPercentile(99);
            } finally {
                batchSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the cumulative count of rows removed by deduplication.
         * <p>
         * This is the difference between WAL rows before dedup and physical rows written.
         * Accumulated during WAL apply processing.
         *
         * @return cumulative dedup row count (always >= 0)
         */
        public long getDedupRowCount() {
            return dedupRowCount.sum();
        }

        /**
         * Returns the floor seqTxn. WAL rows with seqTxn &lt;= floor are not subtracted
         * from pending count (they were never added because they existed before tracking started).
         *
         * @return floor seqTxn, or 0 if not set
         */
        public long getFloorSeqTxn() {
            return floorSeqTxn.get();
        }

        /**
         * Returns the maximum timestamp of actual data in the table.
         * <p>
         * This is the highest timestamp value present in the table's data,
         * updated when WAL transactions are merged into the table.
         *
         * @return max data timestamp in microseconds, or {@link Numbers#LONG_NULL} if not available
         */
        public long getTableMaxTimestamp() {
            mergeStatsLock.readLock().lock();
            try {
                return tableMaxTimestamp;
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the minimum timestamp of actual data in the table.
         * <p>
         * This is the lowest timestamp value present in the table's data,
         * updated when WAL transactions are merged into the table.
         *
         * @return min data timestamp in microseconds, or {@link Numbers#LONG_NULL} if not available
         */
        public long getTableMinTimestamp() {
            mergeStatsLock.readLock().lock();
            try {
                return tableMinTimestamp;
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns whether the last replica download batch was limited and more data is available.
         * <p>
         * This is only populated on replicas via {@link RecentWriteTracker#recordReplicaDownload}.
         * On primaries, this will always return false.
         *
         * @return true if more data is available to download, false otherwise
         */
        public boolean isReplicaMorePending() {
            return replicaMorePending.get();
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
         * Returns the total number of merge throughput samples recorded.
         *
         * @return total sample count
         */
        public long getMergeThroughputCount() {
            mergeStatsLock.readLock().lock();
            try {
                return mergeThroughputHistogram.getTotalCount();
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the maximum merge throughput recorded.
         *
         * @return maximum throughput in rows/second, or 0 if no samples recorded
         */
        public long getMergeThroughputMax() {
            mergeStatsLock.readLock().lock();
            try {
                return mergeThroughputHistogram.getMaxValue();
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the 50th percentile (median) merge throughput.
         *
         * @return median throughput in rows/second, or 0 if no samples recorded
         */
        public long getMergeThroughputP50() {
            mergeStatsLock.readLock().lock();
            try {
                return mergeThroughputHistogram.getValueAtPercentile(50);
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the throughput that 90% of merge jobs exceeded.
         * <p>
         * For throughput (where higher is better), this shows the slow tail:
         * the throughput at which the slowest 10% of jobs performed.
         *
         * @return throughput in rows/second that 90% of jobs exceeded, or 0 if no samples recorded
         */
        public long getMergeThroughputP90() {
            mergeStatsLock.readLock().lock();
            try {
                // Inverted: P90 for throughput = value that 90% exceeded = 10th percentile
                return mergeThroughputHistogram.getValueAtPercentile(10);
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the throughput that 99% of merge jobs exceeded.
         * <p>
         * For throughput (where higher is better), this shows the slow tail:
         * the throughput at which the slowest 1% of jobs performed.
         *
         * @return throughput in rows/second that 99% of jobs exceeded, or 0 if no samples recorded
         */
        public long getMergeThroughputP99() {
            mergeStatsLock.readLock().lock();
            try {
                // Inverted: P99 for throughput = value that 99% exceeded = 1st percentile
                return mergeThroughputHistogram.getValueAtPercentile(1);
            } finally {
                mergeStatsLock.readLock().unlock();
            }
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
         * Returns the total number of WAL transactions recorded in the histogram.
         *
         * @return total transaction count
         */
        public long getTxnCount() {
            txnSizeHistogramLock.readLock().lock();
            try {
                return txnSizeHistogram.getTotalCount();
            } finally {
                txnSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the maximum transaction size recorded.
         *
         * @return maximum transaction size in rows, or 0 if no transactions recorded
         */
        public long getTxnSizeMax() {
            txnSizeHistogramLock.readLock().lock();
            try {
                return txnSizeHistogram.getMaxValue();
            } finally {
                txnSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the 50th percentile (median) transaction size.
         *
         * @return median transaction size in rows, or 0 if no transactions recorded
         */
        public long getTxnSizeP50() {
            txnSizeHistogramLock.readLock().lock();
            try {
                return txnSizeHistogram.getValueAtPercentile(50);
            } finally {
                txnSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the 90th percentile transaction size.
         *
         * @return 90th percentile transaction size in rows, or 0 if no transactions recorded
         */
        public long getTxnSizeP90() {
            txnSizeHistogramLock.readLock().lock();
            try {
                return txnSizeHistogram.getValueAtPercentile(90);
            } finally {
                txnSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the 99th percentile transaction size.
         *
         * @return 99th percentile transaction size in rows, or 0 if no transactions recorded
         */
        public long getTxnSizeP99() {
            txnSizeHistogramLock.readLock().lock();
            try {
                return txnSizeHistogram.getValueAtPercentile(99);
            } finally {
                txnSizeHistogramLock.readLock().unlock();
            }
        }

        /**
         * Returns the cumulative row count from all WAL segments written to this table.
         * <p>
         * This counter is incremented each time a WAL segment is written, and never reset.
         * It represents the total number of rows written via WAL since the tracker started
         * tracking this table.
         *
         * @return cumulative WAL row count (always >= 0)
         */
        public long getWalRowCount() {
            return walRowCount.sum();
        }

        /**
         * Returns the total number of write amplification samples recorded.
         *
         * @return total sample count
         */
        public long getWriteAmplificationCount() {
            mergeStatsLock.readLock().lock();
            try {
                return writeAmplificationHistogram.getTotalCount();
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the maximum write amplification recorded.
         *
         * @return maximum write amplification, or 0.0 if no samples recorded
         */
        public double getWriteAmplificationMax() {
            mergeStatsLock.readLock().lock();
            try {
                return writeAmplificationHistogram.getMaxValue();
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the 50th percentile (median) write amplification.
         *
         * @return median write amplification, or 0.0 if no samples recorded
         */
        public double getWriteAmplificationP50() {
            mergeStatsLock.readLock().lock();
            try {
                return writeAmplificationHistogram.getValueAtPercentile(50);
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the 90th percentile write amplification.
         *
         * @return 90th percentile write amplification, or 0.0 if no samples recorded
         */
        public double getWriteAmplificationP90() {
            mergeStatsLock.readLock().lock();
            try {
                return writeAmplificationHistogram.getValueAtPercentile(90);
            } finally {
                mergeStatsLock.readLock().unlock();
            }
        }

        /**
         * Returns the 99th percentile write amplification.
         *
         * @return 99th percentile write amplification, or 0.0 if no samples recorded
         */
        public double getWriteAmplificationP99() {
            mergeStatsLock.readLock().lock();
            try {
                return writeAmplificationHistogram.getValueAtPercentile(99);
            } finally {
                mergeStatsLock.readLock().unlock();
            }
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
         * Updates replica download fields. Unlike updateWal, this records to the batch
         * histogram instead of the transaction histogram, since replicas download data
         * in batches that may contain multiple transactions.
         *
         * @param newTxn          the new sequencer transaction number (last txn in batch)
         * @param newWalTimestamp the WAL write timestamp in microseconds
         * @param batchRowCount   the total number of rows in this download batch
         * @param morePending     true if the download batch was limited and more data is available
         */
        private void updateReplicaDownload(long newTxn, long newWalTimestamp, long batchRowCount, boolean morePending) {
            // Always increment WAL row count - contention-free via LongAdder
            walRowCount.add(batchRowCount);

            // Update the more pending flag
            replicaMorePending.set(morePending);

            // Record batch size in replica-specific histogram (NOT txnSizeHistogram)
            if (batchRowCount > 0) {
                batchSizeHistogramLock.writeLock().lock();
                try {
                    batchSizeHistogram.recordValue(batchRowCount);
                } finally {
                    batchSizeHistogramLock.writeLock().unlock();
                }
            }

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
         * Updates WAL fields using CAS - only succeeds if new values are higher.
         * Multiple WAL writers may call this concurrently; highest values win.
         * Row count is always added (accumulated).
         *
         * @param newTxn          the new sequencer transaction number
         * @param newWalTimestamp the WAL write timestamp in microseconds
         * @param txnRowCount     the number of rows in this WAL transaction
         */
        private void updateWal(long newTxn, long newWalTimestamp, long txnRowCount) {
            // Always increment WAL row count - contention-free via LongAdder
            walRowCount.add(txnRowCount);

            // Record transaction size in histogram
            if (txnRowCount > 0) {
                txnSizeHistogramLock.writeLock().lock();
                try {
                    txnSizeHistogram.recordValue(txnRowCount);
                } finally {
                    txnSizeHistogramLock.writeLock().unlock();
                }
            }

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
        private void updateWriter(long timestamp, long rowCount, long writerTxn) {
            this.timestamp = timestamp;
            this.rowCount = rowCount;
            this.writerTxn = writerTxn;
        }

        /**
         * Records merge statistics including write amplification, throughput, and table timestamps.
         *
         * @param amplification     the write amplification ratio (physicalRows / logicalRows)
         * @param throughput        the throughput in rows/second
         * @param tableMinTimestamp the minimum timestamp of data in the table
         * @param tableMaxTimestamp the maximum timestamp of data in the table
         */
        void recordMergeStats(double amplification, long throughput, long tableMinTimestamp, long tableMaxTimestamp) {
            mergeStatsLock.writeLock().lock();
            try {
                this.tableMinTimestamp = tableMinTimestamp;
                this.tableMaxTimestamp = tableMaxTimestamp;
                if (amplification > 0) {
                    writeAmplificationHistogram.recordValue(amplification);
                }
                if (throughput > 0) {
                    mergeThroughputHistogram.recordValue(throughput);
                }
            } finally {
                mergeStatsLock.writeLock().unlock();
            }
        }

        /**
         * Sets the floor seqTxn. Only succeeds if not already set (floor is 0).
         * Called on startup to mark WAL transactions that existed before tracking started.
         *
         * @param floor the floor seqTxn to set
         */
        void setFloorSeqTxn(long floor) {
            floorSeqTxn.compareAndSet(0, floor);
        }
    }
}
