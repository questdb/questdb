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

import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameAlgebra;
import io.questdb.cairo.frm.file.FrameFactory;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.Hash;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.function.LongConsumer;

/**
 * Periodically scans every WAL table for idle LOGICAL partitions - one period of the table's PARTITION BY unit, i.e.
 * the main directory plus every MOVE-TAIL split inside it - and dispatches the appropriate compaction entry point.
 * Non-WAL tables are out of scope - see {@code scanTable}.
 * <p>
 * A logical partition whose every folder has been idle for {@code cairo.partition.compaction.squash.idle.timeout} is
 * merged whole: one staging copy of all its live rows replaces its run of {@code _txn} entries with a single entry.
 * Otherwise each COMPOSITE folder idle for {@code cairo.partition.compaction.idle.timeout} is compacted on its own,
 * and plain folders are left alone.
 * <p>
 * A swap this job hands to a busy writer's command queue takes ownership of the staging directory the build filled.
 * An in-flight record suppresses any further work on the WHOLE logical partition while the writer instance that
 * received the command remains live and the logical partition's {@code _txn} state stays unchanged.
 */
public class PartitionCompactionScanJob extends SynchronizedJob implements Closeable {
    private static final int IN_FLIGHT_EXPIRY_OFFSET = 3;
    private static final int IN_FLIGHT_LOGICAL_TIMESTAMP_OFFSET = 1;
    private static final int IN_FLIGHT_STATE_OFFSET = 2;
    private static final int IN_FLIGHT_STRIDE = 5;
    private static final int IN_FLIGHT_TABLE_ID_OFFSET = 0;
    private static final int IN_FLIGHT_WRITER_ID_OFFSET = 4;
    private static final Log LOG = LogFactory.getLog(PartitionCompactionScanJob.class);
    // Longs per folder in the scan's own folders list - see collectFolders. One word narrower than
    // CompositePartitionSwapCommand.LONGS_PER_FOLDER: a swap re-checks the folder's column version too, and
    // the scan reads no _cv file to fill that word with.
    private static final int LONGS_PER_SCANNED_FOLDER = 4;
    // A queued swap the writer never reports back on - it refused the command without moving _txn, say, or
    // dropped it - would otherwise park its logical partition for the life of that writer instance. Once a
    // record is this old the sweep takes the writer out of the pool and ticks it, which consumes whatever is
    // still queued, and only then forgets the record. Nothing is rebuilt on the strength of the timeout
    // alone: that is what made the note worth keeping in the first place.
    private static final long MAX_IN_FLIGHT_MICROS = 30 * Micros.MINUTE_MICROS;
    // Bounds the clean-parquet memo. Kept across two generations so a full memo evicts its oldest half
    // rather than being wiped whole - see rememberCleanParquetPartition.
    private static final int MAX_MEMO_SIZE = 100_000;
    // Caps how many parquet partitions one sweep footer-probes (an mmap of the _pm file plus a parse). A
    // memo hit costs nothing and is never charged; only an actual probe is. The first sweep after startup,
    // and any sweep after the memo evicts, would otherwise footer-probe every idle parquet partition of
    // every table in one tick, on this job's single thread. Charging probes against a per-sweep budget
    // spreads that cost across sweeps; a partition memoized clean is never probed again until it changes.
    private static final int MAX_PROBE_PER_SWEEP = 10_000;
    private final long checkInterval;
    private final Clock clock;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    // The sweep's own frame factory, deliberately built with a NULL messageBus so a REWRITE copies its
    // partition serially, inline on this dedicated compaction thread. The engine's own factory carries the
    // shared column-task bus, which would fan the per-column copy out onto sharedPoolWrite - the pool
    // running WAL apply and O3 - defeating the whole reason this job owns a separate thread (see
    // ServerMain, where the compaction pool is created).
    private final FrameFactory frameFactory;
    // Scratch for one logical partition's folders, LONGS_PER_SCANNED_FOLDER longs each - see collectFolders.
    private final LongList folders = new LongList();
    private final PartitionGeometry geometry = new PartitionGeometry();
    private final long idleTimeoutMicros;
    // Sorted by (tableId, logicalPartitionTimestamp), five longs per record.
    private final LongList inFlightSwaps = new LongList();
    private final long ioBudget;
    private final IntHashSet liveTableIds = new IntHashSet();
    private final Path other = new Path();
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private final Path path = new Path();
    private final Rnd rnd;
    private final Utf8StringSink sidecarName = new Utf8StringSink();
    private final FindVisitor sidecarVisitor = this::copyParquetPartitionSidecar;
    private final TableUtils.SymbolTableProviderFromReader symbolTableProvider = new TableUtils.SymbolTableProviderFromReader();
    private final long squashIdleTimeoutMicros;
    // Scratch for the folder records a MERGE hands the writer, CompositePartitionSwapCommand.LONGS_PER_FOLDER
    // longs each - see describeSwapFolders.
    private final LongList swapFolders = new LongList();
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final long timeBudgetMicros;
    private final TxReader txReader;
    private final LongConsumer writerIdSink = this::capturePublishedWriterId;
    // Fingerprints of parquet partitions already found to hold no dead space AND no stale schema. Any write
    // to a partition changes its nameTxn or its file size, and any DDL changes the metadata version, so
    // neither a changed partition nor a changed schema can match its own stale entry. Held in two
    // generations: the active set takes new entries, and when it fills to half the memo bound it is retired
    // and the previously retired one dropped (see rememberCleanParquetPartition), so a full memo loses only
    // its oldest half instead of everything.
    private LongHashSet cleanParquetPartitions = new LongHashSet();
    private boolean isBudgetExhausted;
    private boolean isGeometryOpen;
    private boolean isIoDispatchStarted;
    private long last = 0;
    // Both default to their constants; tests shrink them to exercise the memo eviction and the probe
    // budget without materializing 100k parquet partitions.
    private int maxProbesPerSweep = MAX_PROBE_PER_SWEEP;
    private int memoCapacity = MAX_MEMO_SIZE;
    private int probeBudget;
    private long publishedWriterId;
    private long remainingIoBudget;
    private LongHashSet retiringCleanParquetPartitions = new LongHashSet();
    private int sidecarDstLen;
    private int sidecarSrcLen;
    private long sweepDeadline;
    private int sweepDispatchCount;

    public PartitionCompactionScanJob(CairoEngine engine, FilesFacade ff, Clock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.configuration = engine.getConfiguration();
        this.checkInterval = configuration.getPartitionCompactionCheckInterval() * 1000;
        this.ioBudget = configuration.getPartitionCompactionIoBudget();
        this.timeBudgetMicros = configuration.getPartitionCompactionTimeBudgetMs() * Micros.MILLI_MICROS;
        final Rnd configurationRnd = configuration.getRandom();
        this.rnd = new Rnd(configurationRnd.getSeed0(), configurationRnd.getSeed1());
        // The same key PartitionCompactionPolicy's AGE rule reads, so this job's idle gate matches the
        // threshold the per-commit path would have applied. It gates a folder compacted ON ITS OWN.
        this.idleTimeoutMicros = configuration.getPartitionCompactionIdleTimeout();
        // The lower of the two: merging a whole logical partition also reclaims the splits, so it is worth
        // doing sooner. Configuration clamps it to at most the single-folder threshold.
        this.squashIdleTimeoutMicros = configuration.getPartitionCompactionSquashIdleTimeout();
        this.txReader = new TxReader(ff);
        this.frameFactory = new FrameFactory(configuration, null);
    }

    public PartitionCompactionScanJob(CairoEngine engine) {
        this(engine, engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getMicrosecondClock());
    }

    @Override
    public void close() {
        cleanParquetPartitions.clear();
        retiringCleanParquetPartitions.clear();
        inFlightSwaps.clear();
        geometry.close();
        other.close();
        parquetMetaReader.clear();
        path.close();
        txReader.close();
        frameFactory.close();
    }

    @TestOnly
    public int getCleanParquetPartitionMemoSize() {
        return cleanParquetPartitions.size() + retiringCleanParquetPartitions.size();
    }

    @TestOnly
    public int getPendingSwapMemoSize() {
        return inFlightSwaps.size() / IN_FLIGHT_STRIDE;
    }

    @TestOnly
    public void setMaxProbesPerSweep(int maxProbesPerSweep) {
        this.maxProbesPerSweep = maxProbesPerSweep;
    }

    @TestOnly
    public void setMemoCapacity(int memoCapacity) {
        this.memoCapacity = memoCapacity;
    }

    /**
     * A column type in the form the two sides can be compared in. Exact rather than by tag, so an ALTER that
     * keeps the tag (DECIMAL(10,2) to DECIMAL(12,4), TIMESTAMP to TIMESTAMP_NS) is caught. The
     * designated-timestamp flag is the exception: the file sets it on its own timestamp column and the
     * table's type does not carry it.
     */
    private static int comparableColumnType(int columnType) {
        return ColumnType.tagOf(columnType) == ColumnType.TIMESTAMP
                ? ColumnType.setDesignatedTimestampBit(columnType, false)
                : columnType;
    }

    private static long estimateCompactionIoBytes(TableMetadata metadata, long liveRows) {
        final long avgRecordSize = Math.max(1, TableUtils.estimateAvgRecordSize(metadata));
        if (liveRows > Long.MAX_VALUE / avgRecordSize / 2) {
            return Long.MAX_VALUE;
        }
        return liveRows * avgRecordSize * 2;
    }

    /**
     * The index one past the last folder of the logical partition {@code lo} starts.
     */
    private static int findLogicalPartitionRunEnd(TxReader txFile, int lo, long logicalPartitionTimestamp) {
        int hi = lo + 1;
        while (hi < txFile.getPartitionCount()
                && txFile.getLogicalPartitionTimestamp(txFile.getPartitionTimestampByIndex(hi)) == logicalPartitionTimestamp) {
            hi++;
        }
        return hi;
    }

    /**
     * The index of a logical partition's FIRST folder, which is a split when the main folder is missing - a shape
     * some replace-commits leave behind. {@code -1} when the logical partition holds no folder at all.
     */
    private static int findLogicalPartitionRunStart(TxReader txFile, long logicalPartitionTimestamp) {
        int partitionIndex = txFile.findAttachedPartitionIndexByLoTimestamp(logicalPartitionTimestamp);
        if (partitionIndex < 0) {
            // No folder starts exactly on the logical start: the run, if any, begins at the insertion point.
            partitionIndex = -partitionIndex - 1;
        }
        if (partitionIndex >= txFile.getPartitionCount()
                || txFile.getLogicalPartitionTimestamp(txFile.getPartitionTimestampByIndex(partitionIndex)) != logicalPartitionTimestamp) {
            return -1;
        }
        return partitionIndex;
    }

    /**
     * Whether any column the table still has is stored under {@code columnId} - the parquet field id, i.e. an
     * original writer index. A dropped column's id is not evidence of anything while a live column is still
     * keyed by it.
     */
    private static boolean hasLiveColumnWithId(TableMetadata metadata, int columnId) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) > 0 && metadata.getColumnMetadata(i).getOriginalWriterIndex() == columnId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reports whether the file's own schema has fallen behind the table's - a column DROPped or its type ALTERed
     * since the conversion - which {@link O3PartitionJob#compactParquetPartition} repairs by re-encoding every row
     * group under the current schema instead of copying it verbatim.
     * <p>
     * Both tests are ones the re-encode actually clears, which is what keeps the sweep from re-picking the same
     * partition forever. {@code compactParquetPartition}'s own {@code hasTypeConvertedColumns} is deliberately NOT
     * used: {@code originalWriterIndex} is durable metadata and the re-encode stamps it back into the new file's
     * field ids, so that predicate stays true for the life of the table. An ADD since the conversion is not a reason
     * on its own - the read path already serves the missing column as nulls.
     */
    private static boolean isParquetSchemaStale(TableMetadata metadata, ParquetMetaFileReader parquetMeta) {
        final int parquetColumnCount = parquetMeta.getColumnCount();
        int mappedParquetColumns = 0;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            // Matched on the parquet field id, which is the column's ORIGINAL writer index, so a column
            // re-keyed by ALTER COLUMN TYPE is still found under the id it was converted with.
            final int columnId = metadata.getColumnMetadata(i).getOriginalWriterIndex();
            final int parquetIdx = parquetMeta.getColumnIndexById(columnId);
            final int tableType = metadata.getColumnType(i);
            if (tableType < 0) {
                // A dropped column, whose tombstone this metadata still carries. It counts only when no LIVE
                // column claims the same id: ALTER COLUMN TYPE leaves the tombstone and its replacement sharing
                // one originalWriterIndex, and the replacement's own type check below decides that column.
                if (parquetIdx >= 0 && !hasLiveColumnWithId(metadata, columnId)) {
                    return true;
                }
                continue;
            }
            if (parquetIdx < 0) {
                continue;
            }
            mappedParquetColumns++;
            if (comparableColumnType(parquetMeta.getColumnType(parquetIdx)) != comparableColumnType(tableType)) {
                return true;
            }
        }
        // The other half of the dropped-column question: a parquet column no live table column claims.
        return mappedParquetColumns < parquetColumnCount;
    }

    /**
     * One long per {@code folders} word, folded in order, so any change to the run - a folder added, dropped,
     * rebuilt under a new name txn, grown, or moved to a new geometry generation - gives a different value. This
     * is the whole identity the sweep tracks a logical partition by.
     */
    private static long logicalPartitionState(LongList folders) {
        long state = folders.size();
        for (int i = 0, n = folders.size(); i < n; i++) {
            state = Hash.hashLong128_64(state, folders.getQuick(i));
        }
        return state;
    }

    /**
     * Builds a composite partition's REWRITE off {@code reader}'s own snapshot, holding no writer.
     *
     * @return a command ready to publish, or {@code null} when the partition holds no live rows
     */
    private CompositePartitionSwapCommand buildCompactedComposite(
            TableToken tableToken,
            TableReader reader,
            int partitionIndex,
            long partitionTimestamp
    ) {
        final PartitionGeometry readerGeometry = reader.getGeometry();
        readerGeometry.resolve(partitionIndex);
        final int pieceCount = readerGeometry.getPieceCount(partitionIndex);
        if (pieceCount == 0) {
            return null;
        }
        long liveRows = 0;
        for (int p = 0; p < pieceCount; p++) {
            liveRows += readerGeometry.getPieceRowCount(partitionIndex, p);
        }
        if (liveRows == 0) {
            return null;
        }

        final TxReader txFile = reader.getTxFile();
        final long srcNameTxn = txFile.getPartitionNameTxn(partitionIndex);
        final long writerTxn = readerGeometry.getWriterTxn(partitionIndex);
        final long e = readerGeometry.getE(partitionIndex);
        final int timestampType = reader.getMetadata().getTimestampType();
        final int partitionBy = reader.getPartitionedBy();
        final ColumnVersionReader cvr = reader.getColumnVersionReader();

        setStagingPath(other, tableToken, timestampType, partitionBy, partitionTimestamp, srcNameTxn, writerTxn);

        final CompositePartitionSwapCommand command = new CompositePartitionSwapCommand();
        // Strictly before the build: of() resets the recorder, so arming it after the copy would lose the
        // tops the build recorded.
        command.of(tableToken, tableToken.getTableId(), partitionTimestamp, srcNameTxn, writerTxn, reader.getMetadataVersion(), liveRows);
        final ColumnTopRecorder columnTops = command.getColumnTops();
        Frame targetFrame = null;
        boolean built = false;
        try {
            if (ff.exists(other.$())) {
                // A build that never reached its swap.
                ff.rmdir(other, false);
            }
            TableUtils.createDirsOrFail(ff, other, configuration.getMkDirMode());
            targetFrame = frameFactory.openRW(other, partitionTimestamp, reader.getMetadata(), cvr, columnTops, 0);

            final int tableRootLen = path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).size();
            TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
            try (Frame sourceFrame = frameFactory.openRO(path, partitionTimestamp, reader.getMetadata(), cvr, e)) {
                for (int p = 0; p < pieceCount; p++) {
                    final long rowCount = readerGeometry.getPieceRowCount(partitionIndex, p);
                    if (rowCount == 0) {
                        continue;
                    }
                    final long rowOffset = readerGeometry.getPieceRowOffset(partitionIndex, p);
                    FrameAlgebra.append(targetFrame, sourceFrame, rowOffset, rowOffset + rowCount, -1L, configuration.getCommitMode());
                }
            } finally {
                path.trimTo(tableRootLen);
            }
            built = true;
        } finally {
            Misc.free(targetFrame);
            if (!built) {
                // Best-effort: the swap only cleans up a directory it was actually handed via a command,
                // so a partial build has to remove its own.
                if (ff.exists(other.$())) {
                    ff.rmdir(other, false);
                }
            }
        }

        return command;
    }

    /**
     * The parquet twin of {@link #buildCompactedComposite}: copies the partition's live row groups off {@code reader}'s
     * snapshot into a staging directory, index files included, and returns the swap command describing the result.
     */
    private ParquetPartitionSwapCommand buildCompactedParquet(
            TableToken tableToken,
            TableReader reader,
            int partitionIndex,
            long partitionTimestamp
    ) {
        final TxReader txFile = reader.getTxFile();
        final long srcNameTxn = txFile.getPartitionNameTxn(partitionIndex);
        final long parquetFileSize = txFile.getPartitionParquetFileSize(partitionIndex);
        final int timestampType = reader.getMetadata().getTimestampType();
        final int partitionBy = reader.getPartitionedBy();

        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
        setStagingPath(other, tableToken, timestampType, partitionBy, partitionTimestamp, srcNameTxn, parquetFileSize);

        final ParquetPartitionSwapCommand command = new ParquetPartitionSwapCommand();
        command.of(tableToken, tableToken.getTableId(), partitionTimestamp, srcNameTxn, parquetFileSize, reader.getMetadataVersion());
        symbolTableProvider.of(reader);
        try {
            if (ff.exists(other.$())) {
                // A build that never reached its swap.
                ff.rmdir(other, false);
            }
            O3PartitionJob.compactParquetPartition(
                    configuration,
                    ff,
                    tableToken,
                    reader.getMetadata(),
                    symbolTableProvider,
                    path,
                    other,
                    // Fallback only: the rebuilt _pm keeps the source footer's own seqTxn.
                    reader.getSeqTxn(),
                    command
            );
            copyParquetPartitionSidecars(path, other);
        } catch (Throwable e) {
            if (ff.exists(other.$())) {
                ff.rmdir(other, false);
            }
            throw e;
        } finally {
            symbolTableProvider.clear();
        }
        return command;
    }

    /**
     * Builds one directory holding the live rows of a WHOLE logical partition - every folder in {@code [lo, hi)},
     * in folder order and then piece order - off {@code reader}'s own snapshot, holding no writer. Folders do not
     * overlap in time and pieces ascend by {@code tsLo}, so that order is timestamp order.
     *
     * @param folders  the source folders, {@link #LONGS_PER_SCANNED_FOLDER} longs each, as
     *                 {@link #collectFolders} filled them
     * @param liveRows the live rows the copy has to end up holding
     * @return a command ready to publish
     */
    private CompositePartitionSwapCommand buildMergedLogicalPartition(
            TableToken tableToken,
            TableReader reader,
            int lo,
            int hi,
            long logicalPartitionTimestamp,
            LongList folders,
            long liveRows
    ) {
        final TxReader txFile = reader.getTxFile();
        final PartitionGeometry readerGeometry = reader.getGeometry();
        final int timestampType = reader.getMetadata().getTimestampType();
        final int partitionBy = reader.getPartitionedBy();
        final ColumnVersionReader cvr = reader.getColumnVersionReader();
        describeSwapFolders(folders, cvr);

        setMergeStagingPath(
                other,
                tableToken,
                timestampType,
                partitionBy,
                logicalPartitionTimestamp,
                folders.getQuick(1),
                folders.size() / LONGS_PER_SCANNED_FOLDER
        );

        final CompositePartitionSwapCommand command = new CompositePartitionSwapCommand();
        // Strictly before the build: ofMerge() resets the recorder, so arming it after the copy would lose
        // the tops the build recorded.
        command.ofMerge(tableToken, tableToken.getTableId(), logicalPartitionTimestamp, swapFolders, reader.getMetadataVersion(), liveRows);
        final ColumnTopRecorder columnTops = command.getColumnTops();
        Frame targetFrame = null;
        boolean built = false;
        try {
            if (ff.exists(other.$())) {
                // A build that never reached its swap.
                ff.rmdir(other, false);
            }
            TableUtils.createDirsOrFail(ff, other, configuration.getMkDirMode());
            targetFrame = frameFactory.openRW(other, logicalPartitionTimestamp, reader.getMetadata(), cvr, columnTops, 0);

            final int tableRootLen = path.of(configuration.getDbRoot()).concat(tableToken.getDirName()).size();
            long copiedRows = 0;
            try {
                for (int partitionIndex = lo; partitionIndex < hi; partitionIndex++) {
                    final long folderTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
                    path.trimTo(tableRootLen);
                    TableUtils.setPathForNativePartition(
                            path,
                            timestampType,
                            partitionBy,
                            folderTimestamp,
                            txFile.getPartitionNameTxn(partitionIndex)
                    );
                    if (txFile.isPartitionComposite(partitionIndex)) {
                        readerGeometry.resolve(partitionIndex);
                        final int pieceCount = readerGeometry.getPieceCount(partitionIndex);
                        // A composite folder's live rows stop short of its files, so it is opened at its
                        // extent and its pieces appended one by one.
                        try (Frame sourceFrame = frameFactory.openRO(
                                path,
                                folderTimestamp,
                                reader.getMetadata(),
                                cvr,
                                readerGeometry.getE(partitionIndex)
                        )) {
                            for (int p = 0; p < pieceCount; p++) {
                                final long rowCount = readerGeometry.getPieceRowCount(partitionIndex, p);
                                if (rowCount == 0) {
                                    continue;
                                }
                                final long rowOffset = readerGeometry.getPieceRowOffset(partitionIndex, p);
                                FrameAlgebra.append(targetFrame, sourceFrame, rowOffset, rowOffset + rowCount, -1L, configuration.getCommitMode());
                                copiedRows += rowCount;
                            }
                        }
                    } else {
                        final long rowCount = txFile.getPartitionSize(partitionIndex);
                        if (rowCount == 0) {
                            continue;
                        }
                        try (Frame sourceFrame = frameFactory.openRO(path, folderTimestamp, reader.getMetadata(), cvr, rowCount)) {
                            FrameAlgebra.append(targetFrame, sourceFrame, 0, rowCount, -1L, configuration.getCommitMode());
                        }
                        copiedRows += rowCount;
                    }
                }
            } finally {
                path.trimTo(tableRootLen);
            }
            if (copiedRows != liveRows) {
                // The row count the swap will publish has to be the one the copy holds. A composite folder
                // whose pieces do not add up to its _txn size is a bug, not a state to swap in.
                throw CairoException.critical(0)
                        .put("merged logical partition holds a different row count than _txn states [table=")
                        .put(tableToken.getTableName())
                        .put(", copiedRows=").put(copiedRows)
                        .put(", liveRows=").put(liveRows)
                        .put(']');
            }
            built = true;
        } finally {
            Misc.free(targetFrame);
            if (!built) {
                // Best-effort: the swap only cleans up a directory it was actually handed via a command,
                // so a partial build has to remove its own.
                if (ff.exists(other.$())) {
                    ff.rmdir(other, false);
                }
            }
        }

        return command;
    }

    /**
     * The table's root directory as a {@link String}, the shape {@link PartitionGeometry#of} needs.
     */
    private String buildTableRoot(TableToken tableToken) {
        try (Path root = new Path()) {
            root.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            return root.toString();
        }
    }

    private void capturePublishedWriterId(long writerId) {
        publishedWriterId = writerId;
    }

    private boolean chargeDispatch(long estimatedIoBytes) {
        if ((sweepDispatchCount > 0 && clock.getTicks() >= sweepDeadline)
                || (isIoDispatchStarted && estimatedIoBytes > remainingIoBudget)) {
            isBudgetExhausted = true;
            return false;
        }
        if (estimatedIoBytes > 0) {
            isIoDispatchStarted = true;
            remainingIoBudget = Math.max(0, remainingIoBudget - estimatedIoBytes);
        }
        sweepDispatchCount++;
        return true;
    }

    /**
     * Describes the folders of one logical partition into {@link #folders}: {@link #LONGS_PER_SCANNED_FOLDER}
     * longs each - the folder's own start timestamp, its name txn, its generation (a composite folder's writer
     * txn, a parquet folder's file size, zero for a plain one) and its live row count. This is what the sweep
     * tracks a logical partition by; {@link #describeSwapFolders} adds the word a swap re-checks on top.
     */
    private void collectFolders(TxReader txFile, PartitionGeometry partitionGeometry, int lo, int hi) {
        folders.clear();
        for (int partitionIndex = lo; partitionIndex < hi; partitionIndex++) {
            final long generation;
            if (txFile.isPartitionComposite(partitionIndex)) {
                partitionGeometry.resolve(partitionIndex);
                generation = partitionGeometry.getWriterTxn(partitionIndex);
            } else if (txFile.isPartitionParquet(partitionIndex)) {
                generation = txFile.getPartitionParquetFileSize(partitionIndex);
            } else {
                // A plain folder has no generation of its own; its name txn and row count are its identity.
                generation = 0;
            }
            folders.add(txFile.getPartitionTimestampByIndex(partitionIndex));
            folders.add(txFile.getPartitionNameTxn(partitionIndex));
            folders.add(generation);
            folders.add(txFile.getPartitionSize(partitionIndex));
        }
    }

    /**
     * One entry of the source partition directory: everything but {@code data.parquet} and {@code _pm} is an index file
     * and is carried into the staging directory, hard-linked where the file system allows.
     */
    private void copyParquetPartitionSidecar(long pUtf8NameZ, int type) {
        if (!Files.notDots(pUtf8NameZ)) {
            return;
        }
        sidecarName.clear();
        Utf8s.utf8ZCopy(pUtf8NameZ, sidecarName);
        if (Utf8s.equalsAscii(TableUtils.PARQUET_PARTITION_NAME, sidecarName)
                || Utf8s.equalsAscii(TableUtils.PARQUET_METADATA_FILE_NAME, sidecarName)) {
            return;
        }
        path.trimTo(sidecarSrcLen).concat(pUtf8NameZ).$();
        other.trimTo(sidecarDstLen).concat(pUtf8NameZ).$();
        final boolean ok;
        if (type == Files.DT_DIR) {
            ok = ff.hardLinkDirRecursive(path, other, configuration.getMkDirMode()) == 0
                    || ff.copyRecursive(path, other, configuration.getMkDirMode()) == 0;
        } else {
            ok = ff.hardLink(path.$(), other.$()) == Files.FILES_RENAME_OK
                    || ff.copy(path.$(), other.$()) >= 0;
        }
        if (!ok) {
            throw CairoException.critical(ff.errno())
                    .put("could not carry parquet partition sidecar into staging directory [from=").put(path)
                    .put(", to=").put(other)
                    .put(']');
        }
    }

    private void copyParquetPartitionSidecars(Path srcPartitionDir, Path dstPartitionDir) {
        assert srcPartitionDir == path && dstPartitionDir == other;
        sidecarSrcLen = srcPartitionDir.size();
        sidecarDstLen = dstPartitionDir.size();
        try {
            ff.iterateDir(srcPartitionDir.$(), sidecarVisitor);
        } finally {
            srcPartitionDir.trimTo(sidecarSrcLen);
            dstPartitionDir.trimTo(sidecarDstLen);
        }
    }

    /**
     * Copies {@code scannedFolders} into {@link #swapFolders}, widening each record with the column version
     * {@code cvr} reads the folder's files at - the max column name txn of the folder's own {@code _cv}
     * records, which is the same word {@link TableReader} re-reads a partition on.
     * <p>
     * That word is what tells the writer an UPDATE landed on a source folder while the copy ran: an UPDATE
     * rewrites the folder's column files under new name txns and leaves everything else the swap re-checks
     * standing - same folders, same name txns, same generations, same row counts, same metadata version. A
     * swap that took the copy anyway would publish the pre-UPDATE files under the post-UPDATE names, which
     * loses the UPDATE and leaves the partition unreadable.
     */
    private void describeSwapFolders(LongList scannedFolders, ColumnVersionReader cvr) {
        swapFolders.clear();
        for (int i = 0, n = scannedFolders.size(); i < n; i += LONGS_PER_SCANNED_FOLDER) {
            final long folderTimestamp = scannedFolders.getQuick(i);
            swapFolders.add(folderTimestamp);
            swapFolders.add(scannedFolders.getQuick(i + 1));
            swapFolders.add(scannedFolders.getQuick(i + 2));
            swapFolders.add(scannedFolders.getQuick(i + 3));
            swapFolders.add(cvr.getMaxPartitionVersion(folderTimestamp));
        }
    }

    /**
     * Builds the REWRITE off a {@link TableReader} snapshot ({@link #buildCompactedComposite}), then publishes a {@link
     * CompositePartitionSwapCommand} the way {@link #dispatchParquet} publishes its own.
     *
     * @param partitionTimestamp the folder's OWN start, which is the split's own timestamp for a split folder -
     *                           never the logical partition start, which resolves to the main folder instead
     * @param expectedState      the logical partition state the scan decided on; a reader that no longer matches it
     *                           has moved on, and the next sweep decides again
     */
    private void dispatchComposite(
            TableToken tableToken,
            long logicalPartitionTimestamp,
            long partitionTimestamp,
            long expectedState
    ) {
        final CompositePartitionSwapCommand command;
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
            if (readerLogicalPartitionState(reader, logicalPartitionTimestamp) != expectedState) {
                return;
            }
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !reader.getTxFile().isPartitionComposite(partitionIndex)) {
                return;
            }
            timestampDriver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            reader.getGeometry().resolve(partitionIndex);
            // Hoisted: every value the chain below prints is read before the ring slot is taken.
            final long srcNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
            final long writerTxn = reader.getGeometry().getWriterTxn(partitionIndex);
            final int pieceCount = reader.getGeometry().getPieceCount(partitionIndex);
            final long liveRows = reader.getTxFile().getPartitionSize(partitionIndex);
            final long physicalRows = reader.getGeometry().getE(partitionIndex);
            LOG.info().$("compaction sweep is rebuilding a composite partition, REWRITE [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", nameTxn=").$(srcNameTxn)
                    .$(", generation=").$(writerTxn)
                    .$(", pieces=").$(pieceCount)
                    .$(", liveRows=").$(liveRows)
                    .$(", deadRows=").$(physicalRows - liveRows)
                    .I$();
            command = buildCompactedComposite(tableToken, reader, partitionIndex, partitionTimestamp);
        }
        if (command == null) {
            LOG.info().$("composite partition REWRITE built nothing, partition holds no live rows [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .I$();
            return;
        }
        publishCommand(tableToken, logicalPartitionTimestamp, expectedState, command, "composite partition REWRITE", timestampDriver, partitionTimestamp);
    }

    /**
     * Builds the whole logical partition's merged copy off a {@link TableReader} snapshot
     * ({@link #buildMergedLogicalPartition}), then publishes the {@link CompositePartitionSwapCommand} that
     * replaces its run of {@code _txn} entries with one.
     */
    private void dispatchMerge(TableToken tableToken, long logicalPartitionTimestamp, long expectedState) {
        final CompositePartitionSwapCommand command;
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
            final TxReader txFile = reader.getTxFile();
            final int lo = findLogicalPartitionRunStart(txFile, logicalPartitionTimestamp);
            if (lo < 0) {
                return;
            }
            final int hi = findLogicalPartitionRunEnd(txFile, lo, logicalPartitionTimestamp);
            collectFolders(txFile, reader.getGeometry(), lo, hi);
            if (logicalPartitionState(folders) != expectedState) {
                // The logical partition moved between the scan's _txn snapshot and this reader's.
                return;
            }
            long liveRows = 0;
            for (int i = lo; i < hi; i++) {
                liveRows += txFile.getPartitionSize(i);
            }
            if (liveRows == 0) {
                return;
            }
            timestampDriver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            LOG.info().$("compaction sweep is merging a logical partition [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, logicalPartitionTimestamp)
                    .$(", folders=").$(hi - lo)
                    .$(", liveRows=").$(liveRows)
                    .I$();
            command = buildMergedLogicalPartition(tableToken, reader, lo, hi, logicalPartitionTimestamp, folders, liveRows);
        }
        publishCommand(tableToken, logicalPartitionTimestamp, expectedState, command, "logical partition MERGE", timestampDriver, logicalPartitionTimestamp);
    }

    /**
     * Asks the table's writer to run MAKE-PLAIN and TRIM-FILES on one partition, in place - the same {@link
     * CompositePartitionSwapCommand} {@link #dispatchComposite} sends, in its MAKE-PLAIN mode. Nothing is staged and
     * nothing is copied, so a command that lands on a partition that has moved on is simply dropped by the writer.
     * The in-flight record is still taken: it is what keeps the sweep from queueing a second command against the
     * same logical partition while this one waits its turn on a busy writer.
     */
    private void dispatchMakePlain(
            TableToken tableToken,
            long logicalPartitionTimestamp,
            long partitionTimestamp,
            long expectedState
    ) {
        final CompositePartitionSwapCommand command = new CompositePartitionSwapCommand();
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
            if (readerLogicalPartitionState(reader, logicalPartitionTimestamp) != expectedState) {
                return;
            }
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !reader.getTxFile().isPartitionComposite(partitionIndex)) {
                return;
            }
            timestampDriver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            reader.getGeometry().resolve(partitionIndex);
            if (!PartitionCompactionPolicy.isMakePlainShape(reader.getTxFile(), reader.getGeometry(), partitionIndex)) {
                // The sweep read a _txn snapshot without holding anything; this reader is the current one.
                LOG.debug().$("composite partition moved off the MAKE-PLAIN shape, skipping [table=").$(tableToken)
                        .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                        .I$();
                return;
            }
            // Hoisted: every value the chain below prints is read before the ring slot is taken.
            final long srcNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
            final long writerTxn = reader.getGeometry().getWriterTxn(partitionIndex);
            final long liveRows = reader.getTxFile().getPartitionSize(partitionIndex);
            final long physicalRows = reader.getGeometry().getE(partitionIndex);
            LOG.info().$("compaction sweep is trimming a composite partition, MAKE-PLAIN [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", nameTxn=").$(srcNameTxn)
                    .$(", generation=").$(writerTxn)
                    .$(", liveRows=").$(liveRows)
                    .$(", deadRows=").$(physicalRows - liveRows)
                    .I$();
            command.ofMakePlain(
                    tableToken,
                    tableToken.getTableId(),
                    partitionTimestamp,
                    srcNameTxn,
                    writerTxn,
                    reader.getMetadataVersion()
            );
        }
        // This reader has to be gone before the writer runs: MAKE-PLAIN waits for the readers that still
        // resolve the geometry record it is about to retire, and this one is holding exactly that record.
        publishCommand(tableToken, logicalPartitionTimestamp, expectedState, command, "composite partition MAKE-PLAIN", timestampDriver, partitionTimestamp);
    }

    /**
     * Compacts the partition off a {@link TableReader} snapshot ({@link #buildCompactedParquet}), then publishes a
     * {@link ParquetPartitionSwapCommand} exactly as {@link #dispatchComposite} does.
     */
    private void dispatchParquet(TableToken tableToken, long logicalPartitionTimestamp, long partitionTimestamp, long expectedState) {
        final ParquetPartitionSwapCommand command;
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
            if (readerLogicalPartitionState(reader, logicalPartitionTimestamp) != expectedState) {
                return;
            }
            final TxReader txFile = reader.getTxFile();
            final int partitionIndex = txFile.getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !txFile.isPartitionParquet(partitionIndex)) {
                return;
            }
            timestampDriver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            final long srcNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            final long parquetFileSize = txFile.getPartitionParquetFileSize(partitionIndex);
            LOG.info().$("compaction sweep is rebuilding a parquet partition [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", nameTxn=").$(srcNameTxn)
                    .$(", parquetFileSize=").$(parquetFileSize)
                    .I$();
            command = buildCompactedParquet(tableToken, reader, partitionIndex, partitionTimestamp);
        }
        publishCommand(tableToken, logicalPartitionTimestamp, expectedState, command, "parquet partition rebuild", timestampDriver, partitionTimestamp);
    }

    /**
     * Resolves this table's in-flight records that have outlived {@link #MAX_IN_FLIGHT_MICROS}. A record that old
     * describes a command the writer never reported on - one it refused without moving {@code _txn}, say - and it
     * would otherwise park its logical partition for the life of that writer instance. Taking the writer out of
     * the pool and ticking it consumes whatever is still queued, which is what makes forgetting the records safe:
     * nothing is rebuilt on the strength of the timeout alone. A writer too busy to hand over keeps its records,
     * and the next sweep tries again.
     */
    private void drainExpiredSwaps(TableToken tableToken, long nowMicros) {
        int recordIndex = findPendingSwap(tableToken.getTableId(), Long.MIN_VALUE);
        recordIndex = recordIndex < 0 ? -recordIndex - 1 : recordIndex;
        final int firstRecordIndex = recordIndex;
        boolean isExpired = false;
        while (recordIndex * IN_FLIGHT_STRIDE < inFlightSwaps.size()) {
            final int offset = recordIndex * IN_FLIGHT_STRIDE;
            if (inFlightSwaps.getQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET) != tableToken.getTableId()) {
                break;
            }
            isExpired |= nowMicros >= inFlightSwaps.getQuick(offset + IN_FLIGHT_EXPIRY_OFFSET);
            recordIndex++;
        }
        final int lastRecordIndex = recordIndex;
        if (!isExpired) {
            return;
        }
        boolean isDrained = false;
        try (TableWriter writer = engine.getWriter(tableToken, "compaction swap drain")) {
            writer.tick(false);
            isDrained = true;
        } catch (CairoException e) {
            // EntryUnavailableException included: the writer is busy, which is the ordinary case for a
            // command that got queued in the first place.
            LOG.debug().$("could not drain an expired compaction swap [table=").$(tableToken)
                    .$(", error=").$(e.getFlyweightMessage())
                    .I$();
        }
        if (!isDrained) {
            // Do not retry on every sweep: a pool round-trip per table per tick buys nothing.
            for (int i = firstRecordIndex; i < lastRecordIndex; i++) {
                inFlightSwaps.setQuick(i * IN_FLIGHT_STRIDE + IN_FLIGHT_EXPIRY_OFFSET, nowMicros + MAX_IN_FLIGHT_MICROS);
            }
            return;
        }
        // The tick drained the whole queue, so no record here can still be protecting a live command.
        for (int i = lastRecordIndex - 1; i >= firstRecordIndex; i--) {
            removePendingSwap(i);
        }
        notifyWalApplyIfLagging(tableToken);
        LOG.info().$("drained an expired compaction swap [table=").$(tableToken)
                .$(", records=").$(lastRecordIndex - firstRecordIndex)
                .I$();
    }

    /**
     * The record for {@code (tableId, logicalPartitionTimestamp)}, or {@code -insertionPoint - 1}.
     */
    private int findPendingSwap(int tableId, long logicalPartitionTimestamp) {
        int lo = 0;
        int hi = inFlightSwaps.size() / IN_FLIGHT_STRIDE - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int offset = mid * IN_FLIGHT_STRIDE;
            final long midTableId = inFlightSwaps.getQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET);
            final long midLogicalTimestamp = inFlightSwaps.getQuick(offset + IN_FLIGHT_LOGICAL_TIMESTAMP_OFFSET);
            if (midTableId < tableId || midTableId == tableId && midLogicalTimestamp < logicalPartitionTimestamp) {
                lo = mid + 1;
            } else if (midTableId > tableId || midLogicalTimestamp > logicalPartitionTimestamp) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -lo - 1;
    }

    /**
     * When one folder was last written: a composite folder's {@code _geometry} says so outright, and a plain
     * folder's age is the modification time of its designated timestamp column file - the same signal
     * {@link #isParquetPartitionIdle} reads off a parquet file.
     *
     * @return {@link Long#MAX_VALUE} when neither can be read, which counts the folder as just written
     */
    private long folderLastWriteMicros(
            TableToken tableToken,
            TableMetadata metadata,
            int timestampType,
            int partitionBy,
            int partitionIndex
    ) {
        if (txReader.isPartitionComposite(partitionIndex)) {
            openGeometry(tableToken, timestampType, partitionBy);
            return geometry.getLastWriteMicros(partitionIndex);
        }
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForNativePartition(
                path,
                timestampType,
                partitionBy,
                txReader.getPartitionTimestampByIndex(partitionIndex),
                txReader.getPartitionNameTxn(partitionIndex)
        );
        final int partitionDirLen = path.size();
        TableUtils.dFile(path, metadata.getColumnName(metadata.getTimestampIndex()), TableUtils.COLUMN_NAME_TXN_NONE);
        long lastModifiedMillis = ff.getLastModified(path.$());
        if (lastModifiedMillis <= 0) {
            // A timestamp column carrying a name txn of its own, after an ALTER that re-keyed it. The
            // directory's own stamp is coarser but still moves whenever the partition is rebuilt.
            lastModifiedMillis = ff.getLastModified(path.trimTo(partitionDirLen).$());
        }
        return lastModifiedMillis > 0 ? lastModifiedMillis * Micros.MILLI_MICROS : Long.MAX_VALUE;
    }

    /**
     * Whether {@code memoKey} sits in either generation of the clean-parquet memo. A hit means the partition
     * was footer-probed on an earlier sweep and found clean, so this sweep skips it for free.
     */
    private boolean isCleanParquetPartitionMemoized(long memoKey) {
        return cleanParquetPartitions.contains(memoKey) || retiringCleanParquetPartitions.contains(memoKey);
    }

    /**
     * Reports whether a Parquet partition is a compaction candidate: idle - not written to for {@link
     * #idleTimeoutMicros}, the same window the composite branch applies to {@code lastWriteMicros} - and either
     * holding ANY dead space or carrying a schema the table has moved on from (see {@link #isParquetSchemaStale}),
     * both read from the {@code _pm} standalone (no live {@link TableWriter}, no O3 commit in flight).
     */
    private boolean isParquetPartitionIdle(
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long nameTxn,
            long parquetFileSize,
            TableMetadata metadata,
            long nowMicros
    ) {
        final long memoKey = Hash.hashLong128_64(
                Hash.hashLong256_64(tableToken.getTableId(), partitionTimestamp, nameTxn, parquetFileSize),
                metadata.getMetadataVersion()
        );
        if (isCleanParquetPartitionMemoized(memoKey)) {
            return false;
        }
        if (probeBudget <= 0) {
            // This sweep has already spent its footer-probe budget. Leave the partition for a later sweep
            // rather than mmap-and-parse its _pm now, so one tick's parquet probe I/O stays bounded on this
            // single thread however many idle parquet partitions the instance holds. A partition deferred
            // here is not memoized, so the next sweep reconsiders it.
            return false;
        }
        probeBudget--;
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long lastModifiedMillis = ff.getLastModified(path.$());
        if (lastModifiedMillis > 0 && lastModifiedMillis * Micros.MILLI_MICROS > nowMicros - idleTimeoutMicros) {
            // Written to inside the idle window.
            return false;
        }
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForParquetPartitionMetadata(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long addr = ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetaReader);
        try {
            if (addr == 0 || !parquetMetaReader.resolveFooter(parquetFileSize)) {
                return false;
            }
            final long unusedBytes = parquetMetaReader.getUnusedBytes();
            final long actualParquetFileSize = parquetMetaReader.getParquetFileSize();
            if ((actualParquetFileSize > 0 && unusedBytes > 0) || isParquetSchemaStale(metadata, parquetMetaReader)) {
                return true;
            }
            rememberCleanParquetPartition(memoKey);
            return false;
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
     * Whether a swap for this LOGICAL partition is still in flight, which stands the sweep down on every one of its
     * folders - the main directory and all its splits - not just the one the swap was built from. At most one swap
     * per logical partition is ever outstanding, so folders that each deserve their own compaction take their turns
     * one sweep after another.
     * <p>
     * The record survives only while the exact writer instance that received the command remains live. The staging
     * directory itself is not a liveness signal: a terminal path may leave it behind, while a live command must
     * retain ownership even if the directory temporarily disappears.
     */
    private boolean isSwapPending(TableToken tableToken, long logicalPartitionTimestamp) {
        final int recordIndex = findPendingSwap(tableToken.getTableId(), logicalPartitionTimestamp);
        if (recordIndex < 0) {
            return false;
        }
        final int offset = recordIndex * IN_FLIGHT_STRIDE;
        if (inFlightSwaps.getQuick(offset + IN_FLIGHT_WRITER_ID_OFFSET) == engine.getWriterId(tableToken)) {
            return true;
        }
        removePendingSwap(recordIndex);
        return false;
    }

    /**
     * Hands the table a WAL apply notification when it still lags its sequencer. Taking the writer out of the pool
     * to land a swap blocks WAL apply, and the notification apply dropped while it waited is gone for good:
     * {@link io.questdb.cairo.wal.seq.SeqTxnTracker#notifyOnCommit} publishes only while a table is exactly caught
     * up, so no later commit re-sends one.
     */
    private void notifyWalApplyIfLagging(TableToken tableToken) {
        if (!tableToken.isWal()) {
            return;
        }
        SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tableToken);
        if (!tracker.isSuspended() && tracker.getWriterTxn() < tracker.getSeqTxn()) {
            engine.notifyWalTxnCommitted(tableToken);
        }
    }

    /**
     * Binds {@link #geometry} to the table being scanned, once per table: {@code of()} drops the resolution cache,
     * so a second call inside one table's scan would pay for every {@code _geometry} read twice.
     */
    private void openGeometry(TableToken tableToken, int timestampType, int partitionBy) {
        if (!isGeometryOpen) {
            geometry.of(ff, txReader, buildTableRoot(tableToken), timestampType, partitionBy, MemoryTag.NATIVE_TABLE_READER);
            isGeometryOpen = true;
        }
    }

    private void pruneDroppedTableSwaps() {
        liveTableIds.clear();
        for (int i = 0, n = tableTokenBucket.size(); i < n; i++) {
            liveTableIds.add(tableTokenBucket.get(i).getTableId());
        }
        for (int recordIndex = inFlightSwaps.size() / IN_FLIGHT_STRIDE - 1; recordIndex >= 0; recordIndex--) {
            final int offset = recordIndex * IN_FLIGHT_STRIDE;
            if (!liveTableIds.contains((int) inFlightSwaps.getQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET))) {
                removePendingSwap(recordIndex);
            }
        }
    }

    /**
     * Drops this table's in-flight records whose swap is done or gone: the writer instance that took the command
     * is no longer the table's, or the logical partition's {@code _txn} state has moved on - which is what landing
     * the swap does, and also what any ingestion into it does.
     */
    private void prunePendingSwaps(TableToken tableToken, int timestampType, int partitionBy) {
        int recordIndex = findPendingSwap(tableToken.getTableId(), Long.MIN_VALUE);
        recordIndex = recordIndex < 0 ? -recordIndex - 1 : recordIndex;
        final long liveWriterId = engine.getWriterId(tableToken);
        while (recordIndex * IN_FLIGHT_STRIDE < inFlightSwaps.size()) {
            final int offset = recordIndex * IN_FLIGHT_STRIDE;
            if (inFlightSwaps.getQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET) != tableToken.getTableId()) {
                break;
            }
            if (liveWriterId < 0 || inFlightSwaps.getQuick(offset + IN_FLIGHT_WRITER_ID_OFFSET) != liveWriterId) {
                removePendingSwap(recordIndex);
                continue;
            }
            final long logicalPartitionTimestamp = inFlightSwaps.getQuick(offset + IN_FLIGHT_LOGICAL_TIMESTAMP_OFFSET);
            final int lo = findLogicalPartitionRunStart(txReader, logicalPartitionTimestamp);
            if (lo < 0) {
                removePendingSwap(recordIndex);
                continue;
            }
            openGeometry(tableToken, timestampType, partitionBy);
            collectFolders(txReader, geometry, lo, findLogicalPartitionRunEnd(txReader, lo, logicalPartitionTimestamp));
            if (inFlightSwaps.getQuick(offset + IN_FLIGHT_STATE_OFFSET) != logicalPartitionState(folders)) {
                removePendingSwap(recordIndex);
                continue;
            }
            recordIndex++;
        }
    }

    /**
     * Hands {@code command} to the table's writer: an idle writer applies it inline on this thread, a busy one
     * takes it onto its own command queue, and the logical partition is then noted as having a swap in flight.
     */
    private void publishCommand(
            TableToken tableToken,
            long logicalPartitionTimestamp,
            long state,
            AsyncWriterCommand command,
            String what,
            TimestampDriver timestampDriver,
            long partitionTimestamp
    ) {
        boolean applied;
        publishedWriterId = -1;
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command, writerIdSink)) {
            applied = writer != null;
            if (applied) {
                command.apply(writer, true);
            } else {
                // The pool captured this id before publishing while its close fence held the writer live.
                rememberPendingSwap(tableToken.getTableId(), logicalPartitionTimestamp, state, publishedWriterId, clock.getTicks());
            }
        }
        LOG.info().$(what).$(" handed over [table=").$(tableToken)
                .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                .$(", appliedInline=").$(applied)
                .I$();
        if (applied) {
            notifyWalApplyIfLagging(tableToken);
        }
    }

    /**
     * The state of one logical partition as {@code reader}'s own snapshot sees it, for comparing against the
     * state the scan decided on. The "no folder at all" answer is the state of an empty run.
     */
    private long readerLogicalPartitionState(TableReader reader, long logicalPartitionTimestamp) {
        final TxReader txFile = reader.getTxFile();
        final int lo = findLogicalPartitionRunStart(txFile, logicalPartitionTimestamp);
        if (lo < 0) {
            folders.clear();
            return logicalPartitionState(folders);
        }
        collectFolders(txFile, reader.getGeometry(), lo, findLogicalPartitionRunEnd(txFile, lo, logicalPartitionTimestamp));
        return logicalPartitionState(folders);
    }

    /**
     * Records a clean parquet partition's fingerprint. When the active generation fills to half the memo
     * bound it is retired and the previously retired one dropped, evicting the oldest half of the memo
     * instead of wiping it whole. Swapping the two sets and clearing the reused one keeps the sweep path
     * allocation-free. A wholesale clear here would collapse the hit rate for an instance holding more clean
     * parquet partitions than the memo bound, re-probing them all on the next sweep.
     */
    private void rememberCleanParquetPartition(long memoKey) {
        if (cleanParquetPartitions.size() >= memoCapacity / 2) {
            final LongHashSet retired = retiringCleanParquetPartitions;
            retiringCleanParquetPartitions = cleanParquetPartitions;
            cleanParquetPartitions = retired;
            cleanParquetPartitions.clear();
        }
        cleanParquetPartitions.add(memoKey);
    }

    private void rememberPendingSwap(
            int tableId,
            long logicalPartitionTimestamp,
            long state,
            long writerId,
            long nowMicros
    ) {
        assert writerId > 0;
        int recordIndex = findPendingSwap(tableId, logicalPartitionTimestamp);
        if (recordIndex < 0) {
            recordIndex = -recordIndex - 1;
            inFlightSwaps.insert(recordIndex * IN_FLIGHT_STRIDE, IN_FLIGHT_STRIDE);
        }
        final int offset = recordIndex * IN_FLIGHT_STRIDE;
        inFlightSwaps.setQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET, tableId);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_LOGICAL_TIMESTAMP_OFFSET, logicalPartitionTimestamp);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_STATE_OFFSET, state);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_EXPIRY_OFFSET, nowMicros + MAX_IN_FLIGHT_MICROS);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_WRITER_ID_OFFSET, writerId);
    }

    private void removePendingSwap(int recordIndex) {
        inFlightSwaps.removeIndexBlock(recordIndex * IN_FLIGHT_STRIDE, IN_FLIGHT_STRIDE);
    }

    /**
     * Decides one LOGICAL partition - the run of {@code _txn} entries {@code [lo, hi)} sharing one logical start -
     * and dispatches at most one command for it.
     *
     * @return false when the sweep's budget ran out and the whole sweep has to stop
     */
    private boolean scanLogicalPartition(
            TableToken tableToken,
            TableMetadata metadata,
            int timestampType,
            int partitionBy,
            int lo,
            int hi,
            long logicalPartitionTimestamp,
            long nowMicros,
            long nowInTableUnits
    ) {
        final int partitionCount = txReader.getPartitionCount();
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
        // The END of the logical partition, not of one split inside it: every folder of it is a candidate,
        // and the run as a whole is what has to sit outside the idle window.
        final long upperBound = hi < partitionCount
                ? txReader.getPartitionTimestampByIndex(hi)
                : txReader.getMaxTimestamp();
        if (upperBound > nowInTableUnits - timestampDriver.fromMicros(squashIdleTimeoutMicros)) {
            // Still inside the idle window on the cheaper of the two thresholds: only the newest partitions
            // can plausibly take O3 writes, so this alone rules out most candidates with no extra I/O.
            return true;
        }
        if (isSwapPending(tableToken, logicalPartitionTimestamp)) {
            // A swap for this logical partition is already queued; it owns a staging directory, and its
            // folders are all about to be replaced by the one it lands. Nothing else may run here.
            return true;
        }

        // Whether the whole logical partition can be merged into one folder. It needs at least two folders -
        // a lone folder has no split to fold in and takes the single-folder path below - and every one of
        // them idle past the squash threshold.
        boolean isMergeable = hi - lo > 1
                // Never the ACTIVE logical partition: its files carry the WAL lag rows past the live ones,
                // which no piece accounts for and a copy built off a reader snapshot would drop. The
                // writer's own squash folds that one on commit. The writer re-checks this.
                && hi < partitionCount;
        for (int partitionIndex = lo; isMergeable && partitionIndex < hi; partitionIndex++) {
            isMergeable = !txReader.isPartitionReadOnly(partitionIndex)
                    && !txReader.isPartitionRemote(partitionIndex)
                    // A parquet folder is not rows this native copy can read; it is compacted on its own.
                    && !txReader.isPartitionParquet(partitionIndex)
                    && folderLastWriteMicros(tableToken, metadata, timestampType, partitionBy, partitionIndex)
                    <= nowMicros - squashIdleTimeoutMicros;
        }

        if (isMergeable) {
            long liveRows = 0;
            for (int partitionIndex = lo; partitionIndex < hi; partitionIndex++) {
                liveRows += txReader.getPartitionSize(partitionIndex);
            }
            if (liveRows == 0) {
                return true;
            }
            if (!chargeDispatch(estimateCompactionIoBytes(metadata, liveRows))) {
                return false;
            }
            openGeometry(tableToken, timestampType, partitionBy);
            collectFolders(txReader, geometry, lo, hi);
            dispatchMerge(tableToken, logicalPartitionTimestamp, logicalPartitionState(folders));
            return true;
        }

        // Some folder is still too fresh to squash, or the logical partition holds something the merge
        // cannot carry. Fall back to compacting single folders, which is a stricter, older threshold.
        for (int partitionIndex = lo; partitionIndex < hi; partitionIndex++) {
            if (txReader.isPartitionReadOnly(partitionIndex) || txReader.isPartitionRemote(partitionIndex)) {
                // Not this job's partition to rewrite. READ ONLY is an operator ATTACH or an
                // Enterprise freeze ahead of the cold switch. REMOTE means a durable copy exists
                // outside this instance, tracked by the very nameTxn and parquet file size the swap
                // reassigns; a remotely served partition has no local data.parquet left to map at all.
                continue;
            }
            final boolean isComposite = txReader.isPartitionComposite(partitionIndex);
            // The offset-3 word is a parquet file size only for a parquet partition; on a native one
            // it is a seqTxn stamp or a geometry pointer, and reading it as a size asserts. Ask the
            // format first rather than inferring it from a non-positive size.
            final long parquetFileSize = txReader.isPartitionParquet(partitionIndex)
                    ? txReader.getPartitionParquetFileSize(partitionIndex)
                    : -1L;
            if (!isComposite && parquetFileSize <= 0) {
                // A plain native folder holds no dead space of its own. Its only waste is being a separate
                // directory, which only the whole-partition merge above reclaims.
                continue;
            }
            // Dispatch by the folder's OWN start: the logical start resolves to the main folder, which for a
            // split is the wrong directory entirely.
            final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
            if (isComposite) {
                openGeometry(tableToken, timestampType, partitionBy);
                if (geometry.getLastWriteMicros(partitionIndex) > nowMicros - idleTimeoutMicros) {
                    continue;
                }
                final boolean isMakePlain = PartitionCompactionPolicy.isMakePlainShape(txReader, geometry, partitionIndex);
                final long estimatedIoBytes = isMakePlain
                        ? 0
                        : estimateCompactionIoBytes(metadata, txReader.getPartitionSize(partitionIndex));
                if (!chargeDispatch(estimatedIoBytes)) {
                    return false;
                }
                collectFolders(txReader, geometry, lo, hi);
                final long state = logicalPartitionState(folders);
                if (isMakePlain) {
                    // Already one piece at row 0: MAKE-PLAIN and TRIM-FILES reach REWRITE's result in
                    // place, with no copy at all. This is the shape a writer leaves behind when it has
                    // to defer the trim - for a reader or a checkpoint - and then stops ingesting, so
                    // its own per-commit retry never comes round again.
                    dispatchMakePlain(tableToken, logicalPartitionTimestamp, partitionTimestamp, state);
                } else {
                    dispatchComposite(tableToken, logicalPartitionTimestamp, partitionTimestamp, state);
                }
            } else {
                if (hi - lo > 1 || partitionTimestamp != logicalPartitionTimestamp) {
                    // TableWriter.swapCompactedParquetPartition floors the timestamp it is handed to the
                    // logical partition start, so it can only ever address a parquet folder that IS the whole
                    // logical partition. A parquet folder sharing its period with a split is left alone rather
                    // than swapped through a lookup that resolves to a different directory.
                    continue;
                }
                final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
                if (!isParquetPartitionIdle(
                        tableToken,
                        timestampType,
                        partitionBy,
                        partitionTimestamp,
                        nameTxn,
                        parquetFileSize,
                        metadata,
                        nowMicros
                )) {
                    continue;
                }
                if (!chargeDispatch(estimateCompactionIoBytes(metadata, txReader.getPartitionSize(partitionIndex)))) {
                    return false;
                }
                openGeometry(tableToken, timestampType, partitionBy);
                collectFolders(txReader, geometry, lo, hi);
                dispatchParquet(tableToken, logicalPartitionTimestamp, partitionTimestamp, logicalPartitionState(folders));
            }
            // One command per logical partition per sweep: it takes the in-flight slot, and every other
            // folder here would only be told to wait for it.
            return true;
        }
        return true;
    }

    /**
     * Opens {@code tableToken}'s {@code _txn} standalone and walks its attached partitions, one LOGICAL
     * partition - the run of entries sharing one logical start - at a time.
     */
    private void scanTable(TableToken tableToken, long nowMicros) {
        if (!tableToken.isWal()) {
            // The sweep serves WAL tables only. A non-WAL writer holds its transaction open across ticks -
            // TableUpdateDetails.commitIfMaxUncommittedRowsCountReached() ticks an ILP-over-TCP writer every
            // cairo.writer.tick.rows.count rows without committing first - so a swap built off a reader
            // snapshot can always be handed to a writer carrying rows that snapshot never saw. The writer's
            // own per-commit compaction still runs there; only this out-of-band path stands down. The check
            // reads one final field of the token, ahead of the _txn stat and the metadata open, so a non-WAL
            // table costs nothing per sweep.
            return;
        }
        path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
        if (!ff.exists(path.$())) {
            return;
        }
        // Ahead of the _txn snapshot: draining a stuck command moves the table on, and the snapshot has to be
        // taken after that rather than be invalidated by it.
        drainExpiredSwaps(tableToken, nowMicros);

        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            final int timestampType = metadata.getTimestampType();
            final int partitionBy = metadata.getPartitionBy();
            if (!PartitionBy.isPartitioned(partitionBy)) {
                return;
            }

            isGeometryOpen = false;
            txReader.ofRO(path.$(), timestampType, partitionBy);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            prunePendingSwaps(tableToken, timestampType, partitionBy);

            final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            final long nowInTableUnits = timestampDriver.fromMicros(nowMicros);

            final int partitionCount = txReader.getPartitionCount();
            int lo = 0;
            while (lo < partitionCount) {
                final long logicalPartitionTimestamp = txReader.getLogicalPartitionTimestamp(
                        txReader.getPartitionTimestampByIndex(lo)
                );
                int hi = lo + 1;
                while (hi < partitionCount
                        && txReader.getLogicalPartitionTimestamp(txReader.getPartitionTimestampByIndex(hi)) == logicalPartitionTimestamp) {
                    hi++;
                }
                if (sweepDispatchCount > 0 && clock.getTicks() >= sweepDeadline) {
                    isBudgetExhausted = true;
                    return;
                }
                if (!scanLogicalPartition(
                        tableToken,
                        metadata,
                        timestampType,
                        partitionBy,
                        lo,
                        hi,
                        logicalPartitionTimestamp,
                        nowMicros,
                        nowInTableUnits
                )) {
                    return;
                }
                lo = hi;
            }
        }
    }

    /**
     * Writes a logical partition merge's staging directory path into {@code sink}: {@code
     * <logicalPartition>.<firstFolderNameTxn>.merging<folderCount>}. A marker of its own, because the writer's
     * startup purge tests a merge's staging directory for liveness against the whole run of folders, not against
     * one folder's generation - see {@code TableWriter.removeMergingPartitionDirIfStale}.
     */
    private void setMergeStagingPath(
            Path sink,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long logicalPartitionTimestamp,
            long firstFolderNameTxn,
            int folderCount
    ) {
        sink.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForNativePartition(sink, timestampType, partitionBy, logicalPartitionTimestamp, firstFolderNameTxn);
        sink.put(TableUtils.MERGING_DIR_MARKER).put(folderCount);
    }

    /**
     * Writes a partition's staging directory path into {@code sink}: {@code
     * <partition>.<nameTxn>.compacting<generation>}.
     */
    private void setStagingPath(
            Path sink,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long srcNameTxn,
            long generation
    ) {
        sink.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForNativePartition(sink, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
        sink.put(TableUtils.COMPACTING_DIR_MARKER).put(generation);
    }

    /**
     * One tick starts at a configuration-seeded random table and stops before starting a dispatch that would exceed
     * either budget. The first dispatch always runs, so one partition larger than the byte budget cannot starve.
     */
    private void sweep(long nowMicros) {
        isBudgetExhausted = false;
        isIoDispatchStarted = false;
        probeBudget = maxProbesPerSweep;
        remainingIoBudget = Math.max(0, ioBudget);
        sweepDispatchCount = 0;
        sweepDeadline = timeBudgetMicros >= Long.MAX_VALUE - nowMicros
                ? Long.MAX_VALUE
                : nowMicros + Math.max(0, timeBudgetMicros);
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        pruneDroppedTableSwaps();
        final int n = tableTokenBucket.size();
        if (n == 0) {
            return;
        }
        int i = rnd.nextPositiveInt() % n;
        for (int visited = 0; visited < n && !isBudgetExhausted; visited++) {
            final TableToken tableToken = tableTokenBucket.get(i);
            i = i + 1 < n ? i + 1 : 0;
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

    @Override
    protected boolean runSerially() {
        if (checkInterval < 0) {
            // A negative cairo.partition.compaction.check.interval disables the background sweep;
            // writer-side compaction is unaffected. Zero still means "sweep on every call".
            return false;
        }
        final long t = clock.getTicks();
        if (last + checkInterval < t) {
            try {
                sweep(t);
            } finally {
                // Measure the interval from completion. A slow sweep must not make the next one immediately
                // eligible and turn background reclamation into a continuous workload.
                last = clock.getTicks();
            }
        }
        return false;
    }
}
