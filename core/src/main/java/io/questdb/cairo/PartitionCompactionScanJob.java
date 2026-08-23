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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Periodically scans every table for idle composite (multi-piece) or Parquet-format partitions and
 * dispatches the appropriate compaction entry point, the way Enterprise's storage-policy job swaps in a
 * recompacted partition.
 * <p>
 * Styled on {@link io.questdb.cairo.wal.WalPurgeJob}'s interval gate: {@link #runSerially()} does nothing
 * until {@link #checkInterval} has elapsed since the last sweep, then walks every table exactly like
 * {@link CairoEngine#hydrateRecentWriteTracker()} does - a standalone {@link TxReader} opened straight over
 * each table's {@code _txn} file, no writer or reader lock, closed (well, {@link TxReader#clear()}-ed) right
 * after.
 * <p>
 * Per partition, the sweep applies three gates before it does anything expensive:
 * <ol>
 *     <li>cheapest first - skip unless the {@code _txn} record already says the partition is composite or
 *     Parquet-format; a plain single-piece native partition has no dead space to reclaim and is never
 *     touched again;</li>
 *     <li>a recency check against the partition's own upper timestamp bound, using only fields already in
 *     {@code _txn} - for time-partitioned data this alone rules out anything that could plausibly still be
 *     taking O3 writes;</li>
 *     <li>one targeted read for the survivors only - {@code _geometry}'s {@code lastWriteMicros} for a
 *     composite candidate, {@code _pm}'s {@code unusedBytes}/parquet file size for a Parquet candidate.</li>
 * </ol>
 * A Parquet candidate is dispatched via {@link CairoEngine#getWriterOrPublishCommand}: an idle writer
 * applies the command in-process, right here on this job's thread; a busy writer gets it queued onto its
 * own {@link io.questdb.tasks.TableWriterTask} command queue and applies it later, on its own thread, via
 * {@link TableWriter#tick()}.
 * <p>
 * A composite candidate is dispatched straight through {@link TableWriter#compactPartitionNoCommit(int)},
 * the one PUBLIC compaction entry point reachable from outside the writer's own per-commit housekeeping -
 * see that method's javadoc. Skipped, not queued, on a busy writer: a writer mid-commit is already about
 * to run its own housekeeping pass, which reaches the same partition through the ordinary per-commit path
 * this job exists only to back up. Reached only for a partition idle for so long that no further commit -
 * and so no further housekeeping pass - is coming to ever re-check it.
 */
public class PartitionCompactionScanJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(PartitionCompactionScanJob.class);
    private final long checkInterval;
    private final Clock clock;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final PartitionGeometry geometry = new PartitionGeometry();
    private final long idleTimeoutMicros;
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private final Path path = new Path();
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final TxReader txReader;
    private long last = 0;

    public PartitionCompactionScanJob(CairoEngine engine, FilesFacade ff, Clock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.configuration = engine.getConfiguration();
        this.checkInterval = configuration.getPartitionCompactionCheckInterval() * 1000;
        // PartitionCompactionPolicy's own AGE rule reads this same key straight in microseconds - see
        // PartitionCompactionPolicy#selectPartition - so this job's idle gate uses the exact same
        // threshold the per-commit path would have applied, had a commit come along to run it.
        this.idleTimeoutMicros = configuration.getPartitionCompactionIdleTimeout();
        this.txReader = new TxReader(ff);
    }

    public PartitionCompactionScanJob(CairoEngine engine) {
        this(engine, engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getMicrosecondClock());
    }

    @Override
    public void close() {
        geometry.close();
        parquetMetaReader.clear();
        path.close();
        txReader.close();
    }

    @Override
    protected boolean runSerially() {
        final long t = clock.getTicks();
        if (last + checkInterval < t) {
            last = t;
            sweep(t);
        }
        return false;
    }

    /**
     * Builds the table's root directory path as a {@link String}, the shape {@link PartitionGeometry#of}
     * needs to resolve {@code _geometry} paths later - see {@link TableWriter#getGeometry()} for the exact
     * same idiom on the writer side.
     */
    private String buildTableRoot(TableToken tableToken) {
        try (Path root = new Path()) {
            root.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            return root.toString();
        }
    }

    /**
     * Runs {@link TableWriter#compactPartitionNoCommit(int)} for one idle composite partition and commits
     * it - the entry point stays inside the caller's own transaction rather than committing one of its
     * own, the same contract {@code UpdateOperatorImpl} relies on, so this job supplies the commit an
     * UPDATE would otherwise have supplied.
     * <p>
     * Silently skipped, not queued or retried through any command, when the writer is busy: a writer mid-
     * commit is already about to run its own {@code housekeep} pass, which reaches this same partition -
     * were it still a candidate - through the ordinary per-commit path. Also silently skipped when the
     * partition is no longer composite, or no longer exists, by the time the writer is actually reached
     * (re-resolved fresh off the live writer, not off the standalone snapshot {@link #scanTable} read) -
     * both are simply "nothing to do", never an error.
     */
    private void dispatchComposite(TableToken tableToken, long partitionTimestamp) {
        final TableWriter writer;
        try {
            writer = engine.getWriter(tableToken, "partition compaction scan");
        } catch (EntryUnavailableException e) {
            return;
        }
        try {
            final int partitionIndex = writer.getPartitionIndexByTimestamp(partitionTimestamp);
            if (partitionIndex >= 0 && writer.compactPartitionNoCommit(partitionIndex)) {
                writer.commit();
            }
        } finally {
            writer.close();
        }
    }

    private void dispatchParquet(TableToken tableToken, long partitionTimestamp) {
        final ParquetPartitionCompactionCommand command = new ParquetPartitionCompactionCommand(tableToken, tableToken.getTableId(), partitionTimestamp);
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
            if (writer != null) {
                command.apply(writer, true);
            }
        }
    }

    /**
     * Reads the {@code _pm} footer standalone (no live {@link TableWriter}, no O3 commit in flight) and
     * applies the same update-vs-rewrite dead-space thresholds {@link O3PartitionJob} already uses when it
     * decides whether to rewrite a Parquet partition mid-commit - reused as-is, not duplicated.
     */
    private boolean isParquetPartitionIdle(
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long nameTxn,
            long parquetFileSize
    ) {
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForParquetPartitionMetadata(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetaReader);
        try {
            if (addr == 0 || !parquetMetaReader.resolveFooter(parquetFileSize)) {
                return false;
            }
            final long unusedBytes = parquetMetaReader.getUnusedBytes();
            final long actualParquetFileSize = parquetMetaReader.getParquetFileSize();
            return actualParquetFileSize > 0 && (
                    (double) unusedBytes / actualParquetFileSize > configuration.getPartitionEncoderParquetO3RewriteUnusedRatio()
                            || unusedBytes > configuration.getPartitionEncoderParquetO3RewriteUnusedMaxBytes()
            );
        } finally {
            // Capture before clear() zeros the fields so the mapping can be released.
            final long mappedSize = parquetMetaReader.getFileSize();
            parquetMetaReader.clear();
            if (addr != 0) {
                ff.munmap(addr, mappedSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    /**
     * Opens {@code tableToken}'s {@code _txn} standalone and walks its attached partitions. Mirrors
     * {@link CairoEngine#hydrateRecentWriteTracker()}'s own per-table body: same lightweight
     * {@link TxReader#ofRO} snapshot, same {@link TableUtils#safeReadTxn} torn-read guard.
     */
    private void scanTable(TableToken tableToken, long nowMicros) {
        path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
        if (!ff.exists(path.$())) {
            return;
        }

        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            final int timestampType = metadata.getTimestampType();
            final int partitionBy = metadata.getPartitionBy();
            if (!PartitionBy.isPartitioned(partitionBy)) {
                return;
            }

            txReader.ofRO(path.$(), timestampType, partitionBy);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());

            final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            final long nowInTableUnits = timestampDriver.fromMicros(nowMicros);
            final long idleTimeoutInTableUnits = timestampDriver.fromMicros(idleTimeoutMicros);

            String tableRoot = null;
            final int partitionCount = txReader.getPartitionCount();
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                final boolean isComposite = txReader.isPartitionComposite(partitionIndex);
                final long parquetFileSize = txReader.getPartitionParquetFileSize(partitionIndex);
                if (!isComposite && parquetFileSize <= 0) {
                    // Plain, single-piece native partition: no dead space, nothing to do, ever.
                    continue;
                }

                final long upperBound = partitionIndex + 1 < partitionCount
                        ? txReader.getPartitionTimestampByIndex(partitionIndex + 1)
                        : txReader.getMaxTimestamp();
                if (upperBound > nowInTableUnits - idleTimeoutInTableUnits) {
                    // Still inside the idle window: only the newest partitions can plausibly take O3
                    // writes, so this alone rules out most candidates with no extra I/O.
                    continue;
                }

                final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
                if (isComposite) {
                    if (tableRoot == null) {
                        tableRoot = buildTableRoot(tableToken);
                        geometry.of(ff, txReader, tableRoot, timestampType, partitionBy);
                    }
                    if (geometry.getLastWriteMicros(partitionIndex) > nowMicros - idleTimeoutMicros) {
                        continue;
                    }
                    dispatchComposite(tableToken, partitionTimestamp);
                } else {
                    final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
                    if (!isParquetPartitionIdle(tableToken, timestampType, partitionBy, partitionTimestamp, nameTxn, parquetFileSize)) {
                        continue;
                    }
                    dispatchParquet(tableToken, partitionTimestamp);
                }
            }
        }
    }

    private void sweep(long nowMicros) {
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            final TableToken tableToken = tableTokenBucket.get(i);
            try {
                scanTable(tableToken, nowMicros);
            } catch (CairoException | TableReferenceOutOfDateException e) {
                LOG.info().$("skipping table during partition compaction scan [table=").$(tableToken)
                        .$(", error=").$(e.getMessage()).I$();
            } finally {
                txReader.clear();
            }
        }
    }
}
