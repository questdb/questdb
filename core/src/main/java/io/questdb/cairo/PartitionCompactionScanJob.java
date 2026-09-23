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
 * Periodically scans every WAL table for idle composite (multi-piece) or Parquet-format partitions and dispatches the
 * appropriate compaction entry point. Non-WAL tables are out of scope - see {@code scanTable}.
 * <p>
 * A swap this job hands to a busy writer's command queue takes ownership of the staging directory the build filled.
 * An in-flight record suppresses rebuilding while the writer instance that received the command remains live and the
 * partition generation stays unchanged.
 */
public class PartitionCompactionScanJob extends SynchronizedJob implements Closeable {
    private static final int IN_FLIGHT_GENERATION_OFFSET = 3;
    private static final int IN_FLIGHT_NAME_TXN_OFFSET = 2;
    private static final int IN_FLIGHT_PARTITION_TIMESTAMP_OFFSET = 1;
    private static final int IN_FLIGHT_STRIDE = 5;
    private static final int IN_FLIGHT_TABLE_ID_OFFSET = 0;
    private static final int IN_FLIGHT_WRITER_ID_OFFSET = 4;
    private static final Log LOG = LogFactory.getLog(PartitionCompactionScanJob.class);
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
    private final PartitionGeometry geometry = new PartitionGeometry();
    private final long idleTimeoutMicros;
    // Sorted by (tableId, partitionTimestamp), five longs per record.
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
        // threshold the per-commit path would have applied.
        this.idleTimeoutMicros = configuration.getPartitionCompactionIdleTimeout();
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
     * Builds the REWRITE off a {@link TableReader} snapshot ({@link #buildCompactedComposite}), then publishes a {@link
     * CompositePartitionSwapCommand} the way {@link #dispatchParquet} publishes its own.
     */
    private void dispatchComposite(TableToken tableToken, long partitionTimestamp) {
        final CompositePartitionSwapCommand command;
        final long srcNameTxn;
        final TimestampDriver timestampDriver;
        final long writerTxn;
        try (TableReader reader = engine.getReader(tableToken)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !reader.getTxFile().isPartitionComposite(partitionIndex)) {
                return;
            }
            timestampDriver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            reader.getGeometry().resolve(partitionIndex);
            srcNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
            writerTxn = reader.getGeometry().getWriterTxn(partitionIndex);
            if (isSwapPending(tableToken, partitionTimestamp, srcNameTxn, writerTxn)) {
                // The copy for this exact generation is already staged and its swap already queued.
                LOG.debug().$("composite partition REWRITE already staged, skipping [table=").$(tableToken)
                        .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                        .$(", nameTxn=").$(srcNameTxn)
                        .$(", generation=").$(writerTxn)
                        .I$();
                return;
            }
            // Hoisted: every value the chain below prints is read before the ring slot is taken.
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
        boolean applied;
        publishedWriterId = -1;
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command, writerIdSink)) {
            applied = writer != null;
            if (applied) {
                command.apply(writer, true);
            } else {
                // The pool captured this id before publishing while its close fence held the writer live.
                rememberPendingSwap(tableToken.getTableId(), partitionTimestamp, srcNameTxn, writerTxn, publishedWriterId);
            }
        }
        LOG.info().$("composite partition REWRITE handed over [table=").$(tableToken)
                .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                .$(", swappedInline=").$(applied)
                .I$();
        if (applied) {
            notifyWalApplyIfLagging(tableToken);
        }
    }

    /**
     * Asks the table's writer to run MAKE-PLAIN and TRIM-FILES on one partition, in place - the same {@link
     * CompositePartitionSwapCommand} {@link #dispatchComposite} sends, in its MAKE-PLAIN mode. Nothing is staged and
     * nothing is copied, so there is no pending-swap record to keep: a command that lands on a partition that has
     * moved on is simply dropped by the writer, and the next sweep decides again.
     */
    private void dispatchMakePlain(TableToken tableToken, long partitionTimestamp) {
        final CompositePartitionSwapCommand command = new CompositePartitionSwapCommand();
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
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
        boolean applied;
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
            applied = writer != null;
            if (applied) {
                command.apply(writer, true);
            }
            // Queued onto a busy writer instead: it applies the command on its own thread, via tick(). A busy
            // writer is also one whose own per-commit compaction is running, so either path is fine.
        }
        LOG.info().$("composite partition MAKE-PLAIN handed over [table=").$(tableToken)
                .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                .$(", appliedInline=").$(applied)
                .I$();
        if (applied) {
            notifyWalApplyIfLagging(tableToken);
        }
    }

    /**
     * Compacts the partition off a {@link TableReader} snapshot ({@link #buildCompactedParquet}), then publishes a
     * {@link ParquetPartitionSwapCommand} exactly as {@link #dispatchComposite} does.
     */
    private void dispatchParquet(TableToken tableToken, long partitionTimestamp) {
        final ParquetPartitionSwapCommand command;
        final long parquetFileSize;
        final long srcNameTxn;
        final TimestampDriver timestampDriver;
        try (TableReader reader = engine.getReader(tableToken)) {
            final TxReader txFile = reader.getTxFile();
            final int partitionIndex = txFile.getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !txFile.isPartitionParquet(partitionIndex)) {
                return;
            }
            timestampDriver = ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType());
            srcNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            parquetFileSize = txFile.getPartitionParquetFileSize(partitionIndex);
            if (isSwapPending(tableToken, partitionTimestamp, srcNameTxn, parquetFileSize)) {
                LOG.debug().$("parquet partition rebuild already staged, skipping [table=").$(tableToken)
                        .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                        .$(", nameTxn=").$(srcNameTxn)
                        .I$();
                return;
            }
            LOG.info().$("compaction sweep is rebuilding a parquet partition [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", nameTxn=").$(srcNameTxn)
                    .$(", parquetFileSize=").$(parquetFileSize)
                    .I$();
            command = buildCompactedParquet(tableToken, reader, partitionIndex, partitionTimestamp);
        }
        boolean applied;
        publishedWriterId = -1;
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command, writerIdSink)) {
            applied = writer != null;
            if (applied) {
                command.apply(writer, true);
            } else {
                rememberPendingSwap(tableToken.getTableId(), partitionTimestamp, srcNameTxn, parquetFileSize, publishedWriterId);
            }
        }
        LOG.info().$("parquet partition rebuild handed over [table=").$(tableToken)
                .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                .$(", swappedInline=").$(applied)
                .I$();
        if (applied) {
            notifyWalApplyIfLagging(tableToken);
        }
    }

    private int findPendingSwap(int tableId, long partitionTimestamp) {
        int lo = 0;
        int hi = inFlightSwaps.size() / IN_FLIGHT_STRIDE - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int offset = mid * IN_FLIGHT_STRIDE;
            final long midTableId = inFlightSwaps.getQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET);
            final long midPartitionTimestamp = inFlightSwaps.getQuick(offset + IN_FLIGHT_PARTITION_TIMESTAMP_OFFSET);
            if (midTableId < tableId || midTableId == tableId && midPartitionTimestamp < partitionTimestamp) {
                lo = mid + 1;
            } else if (midTableId > tableId || midPartitionTimestamp > partitionTimestamp) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -lo - 1;
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
     * Suppresses a rebuild only while the exact writer instance that received the command remains live and the
     * partition identity still matches. The staging directory itself is not a liveness signal: a terminal path may
     * leave it behind, while a live command must retain ownership even if the directory temporarily disappears.
     */
    private boolean isSwapPending(
            TableToken tableToken,
            long partitionTimestamp,
            long srcNameTxn,
            long generation
    ) {
        final int recordIndex = findPendingSwap(tableToken.getTableId(), partitionTimestamp);
        if (recordIndex < 0) {
            return false;
        }
        final int offset = recordIndex * IN_FLIGHT_STRIDE;
        final boolean isSameGeneration = inFlightSwaps.getQuick(offset + IN_FLIGHT_NAME_TXN_OFFSET) == srcNameTxn
                && inFlightSwaps.getQuick(offset + IN_FLIGHT_GENERATION_OFFSET) == generation;
        final boolean isSameWriter = inFlightSwaps.getQuick(offset + IN_FLIGHT_WRITER_ID_OFFSET)
                == engine.getWriterId(tableToken);
        if (isSameGeneration && isSameWriter) {
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

    private void prunePendingSwaps(TableToken tableToken, int timestampType, int partitionBy) {
        int recordIndex = findPendingSwap(tableToken.getTableId(), Long.MIN_VALUE);
        recordIndex = recordIndex < 0 ? -recordIndex - 1 : recordIndex;
        final long liveWriterId = engine.getWriterId(tableToken);
        boolean isGeometryOpen = false;
        String tableRoot = null;
        while (recordIndex * IN_FLIGHT_STRIDE < inFlightSwaps.size()) {
            final int offset = recordIndex * IN_FLIGHT_STRIDE;
            if (inFlightSwaps.getQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET) != tableToken.getTableId()) {
                break;
            }
            if (liveWriterId < 0 || inFlightSwaps.getQuick(offset + IN_FLIGHT_WRITER_ID_OFFSET) != liveWriterId) {
                removePendingSwap(recordIndex);
                continue;
            }

            final long partitionTimestamp = inFlightSwaps.getQuick(offset + IN_FLIGHT_PARTITION_TIMESTAMP_OFFSET);
            final int partitionIndex = txReader.getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0) {
                removePendingSwap(recordIndex);
                continue;
            }
            final long srcNameTxn = txReader.getPartitionNameTxn(partitionIndex);
            final long generation;
            if (txReader.isPartitionComposite(partitionIndex)) {
                if (!isGeometryOpen) {
                    tableRoot = buildTableRoot(tableToken);
                    geometry.of(ff, txReader, tableRoot, timestampType, partitionBy, MemoryTag.NATIVE_TABLE_READER);
                    isGeometryOpen = true;
                }
                geometry.resolve(partitionIndex);
                generation = geometry.getWriterTxn(partitionIndex);
            } else if (txReader.isPartitionParquet(partitionIndex)) {
                generation = txReader.getPartitionParquetFileSize(partitionIndex);
            } else {
                removePendingSwap(recordIndex);
                continue;
            }
            if (inFlightSwaps.getQuick(offset + IN_FLIGHT_NAME_TXN_OFFSET) != srcNameTxn
                    || inFlightSwaps.getQuick(offset + IN_FLIGHT_GENERATION_OFFSET) != generation) {
                removePendingSwap(recordIndex);
                continue;
            }
            recordIndex++;
        }
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
            long partitionTimestamp,
            long srcNameTxn,
            long generation,
            long writerId
    ) {
        assert writerId > 0;
        int recordIndex = findPendingSwap(tableId, partitionTimestamp);
        if (recordIndex < 0) {
            recordIndex = -recordIndex - 1;
            inFlightSwaps.insert(recordIndex * IN_FLIGHT_STRIDE, IN_FLIGHT_STRIDE);
        }
        final int offset = recordIndex * IN_FLIGHT_STRIDE;
        inFlightSwaps.setQuick(offset + IN_FLIGHT_TABLE_ID_OFFSET, tableId);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_PARTITION_TIMESTAMP_OFFSET, partitionTimestamp);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_NAME_TXN_OFFSET, srcNameTxn);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_GENERATION_OFFSET, generation);
        inFlightSwaps.setQuick(offset + IN_FLIGHT_WRITER_ID_OFFSET, writerId);
    }

    private void removePendingSwap(int recordIndex) {
        inFlightSwaps.removeIndexBlock(recordIndex * IN_FLIGHT_STRIDE, IN_FLIGHT_STRIDE);
    }

    /**
     * Opens {@code tableToken}'s {@code _txn} standalone and walks its attached partitions.
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

        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            final int timestampType = metadata.getTimestampType();
            final int partitionBy = metadata.getPartitionBy();
            if (!PartitionBy.isPartitioned(partitionBy)) {
                return;
            }

            txReader.ofRO(path.$(), timestampType, partitionBy);
            TableUtils.safeReadTxn(txReader, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
            prunePendingSwaps(tableToken, timestampType, partitionBy);

            final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            final long nowInTableUnits = timestampDriver.fromMicros(nowMicros);
            final long idleTimeoutInTableUnits = timestampDriver.fromMicros(idleTimeoutMicros);

            String tableRoot = null;
            final int partitionCount = txReader.getPartitionCount();
            for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                if (sweepDispatchCount > 0 && clock.getTicks() >= sweepDeadline) {
                    isBudgetExhausted = true;
                    return;
                }
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

                final long partitionTimestamp = txReader.getLogicalPartitionTimestamp(
                        txReader.getPartitionTimestampByIndex(partitionIndex)
                );
                if (isComposite) {
                    if (tableRoot == null) {
                        tableRoot = buildTableRoot(tableToken);
                        geometry.of(ff, txReader, tableRoot, timestampType, partitionBy, MemoryTag.NATIVE_TABLE_READER);
                    }
                    if (geometry.getLastWriteMicros(partitionIndex) > nowMicros - idleTimeoutMicros) {
                        continue;
                    }
                    final long srcNameTxn = txReader.getPartitionNameTxn(partitionIndex);
                    final long writerTxn = geometry.getWriterTxn(partitionIndex);
                    if (isSwapPending(tableToken, partitionTimestamp, srcNameTxn, writerTxn)) {
                        continue;
                    }
                    final boolean isMakePlain = PartitionCompactionPolicy.isMakePlainShape(txReader, geometry, partitionIndex);
                    final long estimatedIoBytes = isMakePlain
                            ? 0
                            : estimateCompactionIoBytes(metadata, txReader.getPartitionSize(partitionIndex));
                    if (!chargeDispatch(estimatedIoBytes)) {
                        return;
                    }
                    if (isMakePlain) {
                        // Already one piece at row 0: MAKE-PLAIN and TRIM-FILES reach REWRITE's result in
                        // place, with no copy at all. This is the shape a writer leaves behind when it has
                        // to defer the trim - for a reader or a checkpoint - and then stops ingesting, so
                        // its own per-commit retry never comes round again.
                        dispatchMakePlain(tableToken, partitionTimestamp);
                    } else {
                        dispatchComposite(tableToken, partitionTimestamp);
                    }
                } else {
                    final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
                    if (isSwapPending(tableToken, partitionTimestamp, nameTxn, parquetFileSize)
                            || !isParquetPartitionIdle(
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
                        return;
                    }
                    dispatchParquet(tableToken, partitionTimestamp);
                }
            }
        }
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
