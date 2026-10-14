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

package io.questdb.cairo;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Engine-wide cache of partition directory sizes behind the {@code table_storage()} function.
 * <p>
 * Measuring a table directory means stat-ing every file of every partition, which takes seconds
 * on databases with millions of files. Time-series tables change almost exclusively in their last
 * partition, so this cache keeps the size of every other partition and re-walks a partition
 * directory only when one of these changes:
 * <ul>
 *     <li>the directory name, which carries the partition name transaction: a partition rewrite
 *     (O3 merge, squash, conversion) produces a new directory, which is a different cache key;</li>
 *     <li>the partition row count or Parquet file size recorded in {@code _txn}, which catches
 *     in-place appends;</li>
 *     <li>the modification time of the partition directory, which the file system updates
 *     whenever a file is created, deleted or renamed in it: new column versions, purges of
 *     superseded files. It does not change while a file grows or shrinks in place;</li>
 *     <li>the metadata version recorded in {@code _txn}. A schema change such as ADD INDEX or a
 *     column type change creates files in every partition and then grows them in place before it
 *     commits, so a size measured in the middle of it would pass every check above. Its commit
 *     bumps the metadata version, which makes the cache re-walk every partition of the table;</li>
 *     <li>the removal of the last partition, which the truncate version recorded in {@code _txn}
 *     or the disappearance of the previously last partition reveals. The writer then reopens
 *     the partition before the removed one and grows its column files to an append page before
 *     the removal commits, so a call that read {@code _txn} before the commit measures a grown
 *     partition that it believes sealed. The cache re-walks the newest partition it knew
 *     before;</li>
 *     <li>the age of the cached size, which {@link CairoConfiguration#getTableStorageCacheTTL()}
 *     bounds, so that changes invisible to the checks above eventually show up: in-place edits
 *     by external tools, writes that grow a file and then roll back, column files that UPDATE
 *     rewrites until the purge of their superseded versions, and a replace commit that removes
 *     the last partition when a partition with the same timestamp replaces it before the next
 *     call.</li>
 * </ul>
 * The cache measures on every call: the partitions the writer may hold open for appends, the
 * files in the table root, and the directories {@code _txn} does not list (WAL segments, the
 * sequencer, detached partitions, partition versions awaiting purge). The writer holds open the
 * last partition and, when that partition is in Parquet format, possibly the newest native one,
 * whose column files stay grown to an append page until the writer closes them.
 * <p>
 * REINDEX rewrites index files in place without a {@code _txn} commit, so it evicts the table,
 * see {@link #evict(TableToken)}.
 * <p>
 * The cache reads the TTL on every call and applies it to every cached size, so that a
 * configuration reload takes effect immediately, including for the sizes cached before it. A TTL
 * of 0 bypasses the cache, which then releases the sizes it holds.
 * <p>
 * Directory modification times have a coarse granularity on some file systems, so the cache does
 * not keep a size measured within {@link #RACY_WINDOW_MILLIS} of the directory's last
 * modification: a further change within the same clock tick would leave the modification time,
 * and so the cache entry, looking unchanged. File systems that report no modification time for
 * directories leave every partition uncached.
 * <p>
 * Callers measuring the same table serialize on a per-table lock; different tables proceed in
 * parallel.
 * <p>
 * The table name registry evicts a table when it retires the table's directory, see
 * {@link #evict(TableToken)}, and re-validates the cache after it reloads, see
 * {@link #evictDroppedTables()}, so queries never scan the cache for dropped tables.
 */
public class TableDiskSizeCache {
    /**
     * Minimum age of a directory modification for its measured size to be cached.
     */
    public static final long RACY_WINDOW_MILLIS = 2_000;
    private static final long LOCK_POLL_MILLIS = 50;
    private final CairoEngine engine;
    private final ConcurrentHashMap<TableEntry> tables = new ConcurrentHashMap<>();

    public TableDiskSizeCache(CairoEngine engine) {
        this.engine = engine;
    }

    public void clear() {
        tables.clear();
    }

    /**
     * Forgets the sizes of a table. The table name registry calls it when it retires the table's
     * directory: a dropped table, or a non-WAL table that a rename moved to a new directory. The
     * registry calls it once the directory stops resolving, and a drop calls it before releasing
     * the table name, so no table created under the same directory name can own an entry yet.
     * REINDEX calls it after it rewrites index files in place, so that the next call re-measures
     * the partitions it rebuilt. A call measuring the table concurrently keeps the entry it
     * holds, and the next call starts a new one.
     *
     * @param tableToken table to forget
     */
    public void evict(@NotNull TableToken tableToken) {
        tables.remove(tableToken.getDirName());
    }

    /**
     * Evicts the entry of every directory that the table name registry no longer resolves to a
     * live table. The registry calls it after it reloads, which is how a read-only instance
     * learns about dropped tables.
     */
    public void evictDroppedTables() {
        for (CharSequence dirName : tables.keySet()) {
            if (engine.getTableTokenByDirName(dirName) == null) {
                tables.remove(dirName);
            }
        }
    }

    /**
     * Returns the size, in bytes, of the table directory: the total size of the regular files
     * below it, following symlinks to directories, the same value that
     * {@link FilesFacade#getDirSize(Path)} returns for the directory.
     *
     * @param tableToken     table to measure
     * @param txReader       consistent snapshot of the table's {@code _txn} file
     * @param timestampType  type of the designated timestamp, to derive partition directory names
     * @param partitionBy    partitioning of the table, to derive partition directory names
     * @param path           scratch path, left in an undefined state
     * @param circuitBreaker circuit breaker of the calling query
     */
    public long getDiskSize(
            @NotNull TableToken tableToken,
            @NotNull TxReader txReader,
            int timestampType,
            int partitionBy,
            @NotNull Path path,
            @NotNull SqlExecutionCircuitBreaker circuitBreaker
    ) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        final long ttl = configuration.getTableStorageCacheTTL();
        if (ttl <= 0) {
            // a configuration reload can disable the cache after queries filled it
            if (!tables.isEmpty()) {
                tables.clear();
            }
            return ff.getDirSize(path);
        }

        final TableEntry entry = getOrCreateEntry(tableToken.getDirName());
        lock(entry, tableToken, circuitBreaker);
        try {
            return entry.measure(
                    tableToken,
                    txReader,
                    timestampType,
                    partitionBy,
                    path,
                    ff,
                    configuration.getMillisecondClock(),
                    ttl,
                    circuitBreaker
            );
        } finally {
            entry.lock.unlock();
        }
    }

    @TestOnly
    public int getPartitionCount(@NotNull TableToken tableToken) {
        final TableEntry entry = tables.get(tableToken.getDirName());
        return entry != null ? entry.partitions.size() : 0;
    }

    @TestOnly
    public int getTableCount() {
        return tables.size();
    }

    private static void lock(TableEntry entry, TableToken tableToken, SqlExecutionCircuitBreaker circuitBreaker) {
        try {
            while (!entry.lock.tryLock(LOCK_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                // another query is measuring the same table, stay cancellable while waiting
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw CairoException.nonCritical()
                    .put("interrupted while waiting for the table size cache [table=")
                    .put(tableToken.getTableName())
                    .put(']');
        }
    }

    private TableEntry getOrCreateEntry(String dirName) {
        TableEntry entry = tables.get(dirName);
        if (entry == null) {
            final TableEntry newEntry = new TableEntry();
            entry = tables.putIfAbsent(dirName, newEntry);
            if (entry == null) {
                entry = newEntry;
                // A query that listed the table before a drop can get here after the registry
                // evicted it. The registry retires the directory before it evicts, so either this
                // check sees the directory retired, or the eviction runs after the insert above.
                if (engine.getTableTokenByDirName(dirName) == null) {
                    tables.remove(dirName, newEntry);
                }
            }
        }
        return entry;
    }

    private static class PartitionEntry {
        // modification time of the partition directory when the size was measured
        private long dirMtime;
        // measurement round in which _txn last listed the partition
        private long epoch;
        // random value that places the expiry of the size in [ttl/2, ttl] after its measurement
        private long expiryJitter;
        // time of the measurement of the cached size
        private long measuredAt;
        private long parquetFileSize;
        private long rowCount;
        // cached size, -1 when there is none
        private long size = -1;
        // index of the partition in the _txn snapshot of the current measurement round
        private int txnIndex;
    }

    private static class TableEntry {
        private final ReentrantLock lock = new ReentrantLock();
        private final StringSink nameSink = new StringSink();
        // partition directory name -> partition entry
        private final CharSequenceObjHashMap<PartitionEntry> partitions = new CharSequenceObjHashMap<>();
        private final Rnd rnd = new Rnd();
        private long epoch;
        // timestamp of the last partition in the _txn snapshot of the previous measurement round
        private long lastPartitionTimestamp = Numbers.LONG_NULL;
        // metadata version in the _txn snapshot of the previous measurement round
        private int metadataVersion = -1;
        private int tableId = -1;
        // truncate version in the _txn snapshot of the previous measurement round
        private long truncateVersion = -1;

        private void evictStalePartitions(long epoch) {
            final ObjList<CharSequence> names = partitions.keys();
            // walk backwards: removeAtQuick() moves the last name into the vacated slot
            for (int i = names.size() - 1; i > -1; i--) {
                final int keyIndex = partitions.keyIndex(names.getQuick(i));
                if (partitions.valueAtQuick(keyIndex).epoch != epoch) {
                    partitions.removeAtQuick(keyIndex, i);
                }
            }
        }

        private long measure(
                TableToken tableToken,
                TxReader txReader,
                int timestampType,
                int partitionBy,
                Path path,
                FilesFacade ff,
                MillisecondClock clock,
                long ttl,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            if (tableId != tableToken.getTableId()) {
                // the table was dropped and re-created under the same directory name
                partitions.clear();
                tableId = tableToken.getTableId();
            }

            // A schema change such as ADD INDEX grows files of every partition in place and only
            // then commits, bumping the metadata version: drop the sizes measured before the bump.
            final boolean isMetadataChanged = txReader.getMetadataVersion() != metadataVersion;
            metadataVersion = txReader.getMetadataVersion();
            // A drop bumps the truncate version. Dropping the last partition, like a replace commit
            // that removes it, makes the writer reopen the partition before it and grow its column
            // files before the removal commits.
            final boolean isTruncated = txReader.getTruncateVersion() != truncateVersion;
            truncateVersion = txReader.getTruncateVersion();

            // Mark the partitions of the _txn snapshot, keyed by their directory names.
            final long epoch = ++this.epoch;
            final int partitionCount = txReader.getPartitionCount();
            boolean hasPrevLastPartition = false;
            int lastNativeIndex = -1;
            PartitionEntry newestKnownPartition = null;
            for (int i = 0; i < partitionCount; i++) {
                final long partitionTimestamp = txReader.getPartitionTimestampByIndex(i);
                hasPrevLastPartition |= partitionTimestamp == lastPartitionTimestamp;
                if (!txReader.isPartitionParquet(i)) {
                    lastNativeIndex = i;
                }
                nameSink.clear();
                TableUtils.setSinkForNativePartition(
                        nameSink,
                        timestampType,
                        partitionBy,
                        partitionTimestamp,
                        txReader.getPartitionNameTxn(i)
                );
                final int keyIndex = partitions.keyIndex(nameSink);
                final PartitionEntry partition;
                if (keyIndex < 0) {
                    partition = partitions.valueAtQuick(keyIndex);
                    if (isMetadataChanged) {
                        partition.size = -1;
                    }
                    newestKnownPartition = partition;
                } else {
                    partition = new PartitionEntry();
                    partitions.putAt(keyIndex, nameSink.toString(), partition);
                }
                partition.epoch = epoch;
                partition.txnIndex = i;
            }
            // When the last partition of the previous round is gone or a drop happened, the newest
            // partition known before may have been the last one in between, its files grown by the
            // writer. Any older partition kept a newer one throughout, so the writer never reopened it.
            if (newestKnownPartition != null && (isTruncated || !hasPrevLastPartition)) {
                newestKnownPartition.size = -1;
            }
            lastPartitionTimestamp = partitionCount > 0
                    ? txReader.getPartitionTimestampByIndex(partitionCount - 1)
                    : Numbers.LONG_NULL;

            // Walk the table root, sizing the partitions of the snapshot through the cache.
            final int rootLen = path.size();
            long total = 0;
            final long pFind = ff.findFirst(path.$());
            if (pFind > 0) {
                try {
                    do {
                        final long pName = ff.findName(pFind);
                        if (!Files.notDots(pName)) {
                            continue;
                        }
                        path.trimTo(rootLen).concat(pName).$();
                        final int type = ff.findType(pFind);
                        if (type == Files.DT_FILE || (type == Files.DT_UNKNOWN && !ff.isDirOrSoftLinkDir(path.$()))) {
                            total += Math.max(ff.length(path.$()), 0);
                            continue;
                        }

                        PartitionEntry partition = null;
                        nameSink.clear();
                        if (Utf8s.utf8ToUtf16Z(pName, nameSink)) {
                            final int keyIndex = partitions.keyIndex(nameSink);
                            if (keyIndex < 0) {
                                partition = partitions.valueAtQuick(keyIndex);
                                if (partition.epoch != epoch) {
                                    partition = null;
                                }
                            }
                        }
                        total += partition != null
                                ? measurePartition(partition, partitionCount, lastNativeIndex, txReader, path, ff, clock, ttl)
                                : ff.getDirSize(path);
                        circuitBreaker.statefulThrowExceptionIfTripped();
                    } while (ff.findNext(pFind) > 0);
                } finally {
                    ff.findClose(pFind);
                }
            }

            if (partitions.size() > partitionCount) {
                evictStalePartitions(epoch);
            }
            return total;
        }

        private long measurePartition(
                PartitionEntry partition,
                int partitionCount,
                int lastNativeIndex,
                TxReader txReader,
                Path path,
                FilesFacade ff,
                MillisecondClock clock,
                long ttl
        ) {
            final int index = partition.txnIndex;
            if (index == partitionCount - 1 || index == lastNativeIndex) {
                // Appends land in the last partition and grow its files in place. When the last
                // partition is in Parquet format, the writer may still hold the newest native
                // partition open, its column files grown to an append page until it closes them.
                partition.size = -1;
                return ff.getDirSize(path);
            }

            final long rowCount = txReader.getPartitionSize(index);
            final long parquetFileSize = txReader.isPartitionParquet(index) ? txReader.getPartitionParquetFileSize(index) : -1;
            final long dirMtime = ff.getLastModified(path.$());
            final long now = clock.getTicks();
            if (
                    partition.size > -1
                            && partition.dirMtime == dirMtime
                            && partition.rowCount == rowCount
                            && partition.parquetFileSize == parquetFileSize
                            // the current TTL applies, a configuration reload may have changed it
                            && now - partition.measuredAt < ttl - partition.expiryJitter % (ttl / 2 + 1)
            ) {
                return partition.size;
            }

            final long size = ff.getDirSize(path);
            if (dirMtime > 0 && now - dirMtime >= RACY_WINDOW_MILLIS) {
                partition.dirMtime = dirMtime;
                partition.rowCount = rowCount;
                partition.parquetFileSize = parquetFileSize;
                partition.size = size;
                partition.measuredAt = now;
                // spread expiry over [ttl/2, ttl] to avoid re-walking all partitions in one call
                partition.expiryJitter = rnd.nextPositiveLong();
            } else {
                partition.size = -1;
            }
            return size;
        }
    }
}
