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
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

import java.io.Closeable;

/**
 * Periodically scans every WAL table for idle composite (multi-piece) or Parquet-format partitions and dispatches the
 * appropriate compaction entry point. Non-WAL tables are out of scope - see {@code scanTable}.
 */
public class PartitionCompactionScanJob extends SynchronizedJob implements Closeable {
    // Caps how many partitions one sweep hands out: the first sweep after an upgrade can find every
    // qualifying partition of every table at once. The next interval picks up the rest.
    private static final int MAX_DISPATCH_PER_SWEEP = 32;
    // Bounds the clean-parquet memo.
    private static final int MAX_MEMO_SIZE = 100_000;
    private static final Log LOG = LogFactory.getLog(PartitionCompactionScanJob.class);
    // Bounds how long a pending-swap record can sit unclaimed, and with it how long a queued swap keeps
    // suppressing a re-dispatch. isSwapPending consults the staging directory only while the record is
    // still on the list; once expirePendingSwaps drops it the fingerprint is simply absent and the check
    // says "not pending", whatever the staging directory still holds. So the TTL is the backstop for a
    // swap a writer never picked up: too short and the sweep re-does work already queued, too long and a
    // stuck swap blocks the partition from being reconsidered.
    private static final long PENDING_SWAP_MEMO_TTL_MICROS = 60 * Micros.MINUTE_MICROS;
    private final long checkInterval;
    // Fingerprints of parquet partitions already found to hold no dead space AND no stale schema. Any write
    // to a partition changes its nameTxn or its file size, and any DDL changes the metadata version, so
    // neither a changed partition nor a changed schema can match its own stale entry.
    private final LongHashSet cleanParquetPartitions = new LongHashSet();
    private final Clock clock;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final PartitionGeometry geometry = new PartitionGeometry();
    private final long idleTimeoutMicros;
    private final Path other = new Path();
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private final Path path = new Path();
    // (fingerprint, queuedAtMicros) pairs for swaps handed to a BUSY writer's queue.
    private final LongList pendingSwaps = new LongList();
    private final Utf8StringSink sidecarName = new Utf8StringSink();
    private final FindVisitor sidecarVisitor = this::copyParquetPartitionSidecar;
    private final TableUtils.SymbolTableProviderFromReader symbolTableProvider = new TableUtils.SymbolTableProviderFromReader();
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final TxReader txReader;
    private int dispatchBudget;
    private long last = 0;
    private int sidecarDstLen;
    private int sidecarSrcLen;
    // Where the next sweep starts its walk over the table list. A sweep that runs out of budget part way
    // through leaves this pointing at the first table it did not reach.
    private int sweepStartTableIndex;

    public PartitionCompactionScanJob(CairoEngine engine, FilesFacade ff, Clock clock) {
        this.engine = engine;
        this.ff = ff;
        this.clock = clock;
        this.configuration = engine.getConfiguration();
        this.checkInterval = configuration.getPartitionCompactionCheckInterval() * 1000;
        // The same key PartitionCompactionPolicy's AGE rule reads, so this job's idle gate matches the
        // threshold the per-commit path would have applied.
        this.idleTimeoutMicros = configuration.getPartitionCompactionIdleTimeout();
        this.txReader = new TxReader(ff);
    }

    public PartitionCompactionScanJob(CairoEngine engine) {
        this(engine, engine.getConfiguration().getFilesFacade(), engine.getConfiguration().getMicrosecondClock());
    }

    @Override
    public void close() {
        cleanParquetPartitions.clear();
        pendingSwaps.clear();
        geometry.close();
        other.close();
        parquetMetaReader.clear();
        path.close();
        txReader.close();
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
            last = t;
            sweep(t);
        }
        return false;
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
        final FrameFactory frameFactory = engine.getFrameFactory();

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
     * The table's root directory as a {@link String}, the shape {@link PartitionGeometry#of} needs.
     */
    private String buildTableRoot(TableToken tableToken) {
        try (Path root = new Path()) {
            root.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            return root.toString();
        }
    }

    /**
     * Builds the REWRITE off a {@link TableReader} snapshot ({@link #buildCompactedComposite}), then publishes a {@link
     * CompositePartitionSwapCommand} the way {@link #dispatchParquet} publishes its own.
     */
    private void dispatchComposite(TableToken tableToken, long partitionTimestamp, long nowMicros) {
        final CompositePartitionSwapCommand command;
        final long fingerprint;
        try (TableReader reader = engine.getReader(tableToken)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !reader.getTxFile().isPartitionComposite(partitionIndex)) {
                return;
            }
            reader.getGeometry().resolve(partitionIndex);
            final long srcNameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
            final long writerTxn = reader.getGeometry().getWriterTxn(partitionIndex);
            fingerprint = Hash.hashLong256_64(
                    tableToken.getTableId(),
                    partitionTimestamp,
                    srcNameTxn,
                    writerTxn
            );
            setStagingPath(
                    other,
                    tableToken,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    partitionTimestamp,
                    srcNameTxn,
                    writerTxn
            );
            if (isSwapPending(fingerprint, other)) {
                // The copy for this exact generation is already staged and its swap already queued.
                return;
            }
            command = buildCompactedComposite(tableToken, reader, partitionIndex, partitionTimestamp);
        }
        if (command == null) {
            return;
        }
        boolean applied;
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
            applied = writer != null;
            if (applied) {
                command.apply(writer, true);
            } else {
                // Queued onto a busy writer: it applies the swap on its own thread, via tick().
                pendingSwaps.add(fingerprint, nowMicros);
            }
        }
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
        try (TableReader reader = engine.getReader(tableToken)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !reader.getTxFile().isPartitionComposite(partitionIndex)) {
                return;
            }
            reader.getGeometry().resolve(partitionIndex);
            if (!PartitionCompactionPolicy.isMakePlainShape(reader.getTxFile(), reader.getGeometry(), partitionIndex)) {
                // The sweep read a _txn snapshot without holding anything; this reader is the current one.
                return;
            }
            command.ofMakePlain(
                    tableToken,
                    tableToken.getTableId(),
                    partitionTimestamp,
                    reader.getTxFile().getPartitionNameTxn(partitionIndex),
                    reader.getGeometry().getWriterTxn(partitionIndex),
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
        if (applied) {
            notifyWalApplyIfLagging(tableToken);
        }
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
     * Compacts the partition off a {@link TableReader} snapshot ({@link #buildCompactedParquet}), then publishes a
     * {@link ParquetPartitionSwapCommand} exactly as {@link #dispatchComposite} does.
     */
    private void dispatchParquet(TableToken tableToken, long partitionTimestamp, long nowMicros) {
        final ParquetPartitionSwapCommand command;
        final long fingerprint;
        try (TableReader reader = engine.getReader(tableToken)) {
            final TxReader txFile = reader.getTxFile();
            final int partitionIndex = txFile.getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !txFile.isPartitionParquet(partitionIndex)) {
                return;
            }
            final long srcNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            final long parquetFileSize = txFile.getPartitionParquetFileSize(partitionIndex);
            fingerprint = Hash.hashLong256_64(
                    tableToken.getTableId(),
                    partitionTimestamp,
                    srcNameTxn,
                    parquetFileSize
            );
            setStagingPath(
                    other,
                    tableToken,
                    reader.getMetadata().getTimestampType(),
                    reader.getPartitionedBy(),
                    partitionTimestamp,
                    srcNameTxn,
                    parquetFileSize
            );
            if (isSwapPending(fingerprint, other)) {
                return;
            }
            command = buildCompactedParquet(tableToken, reader, partitionIndex, partitionTimestamp);
        }
        boolean applied;
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
            applied = writer != null;
            if (applied) {
                command.apply(writer, true);
            } else {
                pendingSwaps.add(fingerprint, nowMicros);
            }
        }
        if (applied) {
            notifyWalApplyIfLagging(tableToken);
        }
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

    private void expirePendingSwaps(long nowMicros) {
        for (int i = pendingSwaps.size() - 2; i >= 0; i -= 2) {
            if (pendingSwaps.getQuick(i + 1) < nowMicros - PENDING_SWAP_MEMO_TTL_MICROS) {
                pendingSwaps.removeIndexBlock(i, 2);
            }
        }
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
        if (cleanParquetPartitions.contains(memoKey)) {
            return false;
        }
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
            if (cleanParquetPartitions.size() >= MAX_MEMO_SIZE) {
                cleanParquetPartitions.clear();
            }
            cleanParquetPartitions.add(memoKey);
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
     * Reports whether a swap for this exact generation is already staged and queued on a busy writer, so this sweep
     * must leave the partition alone. The record alone does not settle it, {@code stagingDir} does: that directory is
     * the queued command's own input, so rebuilding into it would have the copy's files renamed and truncated
     * underneath it mid-append.
     */
    private boolean isSwapPending(long fingerprint, Path stagingDir) {
        for (int i = 0, n = pendingSwaps.size(); i < n; i += 2) {
            if (pendingSwaps.getQuick(i) == fingerprint) {
                if (ff.exists(stagingDir.$())) {
                    return true;
                }
                pendingSwaps.removeIndexBlock(i, 2);
                return false;
            }
        }
        return false;
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

            final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
            final long nowInTableUnits = timestampDriver.fromMicros(nowMicros);
            final long idleTimeoutInTableUnits = timestampDriver.fromMicros(idleTimeoutMicros);

            String tableRoot = null;
            final int partitionCount = txReader.getPartitionCount();
            for (int partitionIndex = 0; partitionIndex < partitionCount && dispatchBudget > 0; partitionIndex++) {
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

                final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
                if (isComposite) {
                    if (tableRoot == null) {
                        tableRoot = buildTableRoot(tableToken);
                        geometry.of(ff, txReader, tableRoot, timestampType, partitionBy, MemoryTag.NATIVE_TABLE_READER);
                    }
                    if (geometry.getLastWriteMicros(partitionIndex) > nowMicros - idleTimeoutMicros) {
                        continue;
                    }
                    dispatchBudget--;
                    if (PartitionCompactionPolicy.isMakePlainShape(txReader, geometry, partitionIndex)) {
                        // Already one piece at row 0: MAKE-PLAIN and TRIM-FILES reach REWRITE's result in
                        // place, with no copy at all. This is the shape a writer leaves behind when it has
                        // to defer the trim - for a reader or a checkpoint - and then stops ingesting, so
                        // its own per-commit retry never comes round again.
                        dispatchMakePlain(tableToken, partitionTimestamp);
                    } else {
                        dispatchComposite(tableToken, partitionTimestamp, nowMicros);
                    }
                } else {
                    final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
                    if (!isParquetPartitionIdle(tableToken, timestampType, partitionBy, partitionTimestamp, nameTxn, parquetFileSize, metadata, nowMicros)) {
                        continue;
                    }
                    dispatchBudget--;
                    dispatchParquet(tableToken, partitionTimestamp, nowMicros);
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
     * One tick: walks the table list from where the last sweep stopped, handing out at most {@link
     * #MAX_DISPATCH_PER_SWEEP} dispatches in total.
     * <p>
     * The walk resumes rather than restarting because {@code getTableTokens} hands back a stable order - it
     * walks a hash map whose bin order is a pure function of the table directory names, and {@code
     * ObjHashSet.get} reads that walk back out of a dense list in the order it went in. Restarting at index 0
     * every tick therefore let one table holding more qualifying partitions than the budget spend all of it,
     * tick after tick, and the tables behind it were never opened at all - not compacted late, but not
     * scanned - for as long as that table's backlog lasted. Resuming walks the whole ring instead, so every
     * table gets its turn while the per-tick bound stays exactly as it was.
     */
    private void sweep(long nowMicros) {
        dispatchBudget = MAX_DISPATCH_PER_SWEEP;
        expirePendingSwaps(nowMicros);
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        final int n = tableTokenBucket.size();
        if (n == 0) {
            return;
        }
        int i = sweepStartTableIndex < n ? sweepStartTableIndex : 0;
        for (int visited = 0; visited < n && dispatchBudget > 0; visited++) {
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
        // A sweep that got all the way round leaves this where it started, so a system under no dispatch
        // pressure keeps the order it has always had. A table created or dropped in between shifts the
        // indices, which can repeat or skip one table for a single tick - one interval's delay at worst,
        // and the walk still covers the ring.
        sweepStartTableIndex = i;
    }
}
