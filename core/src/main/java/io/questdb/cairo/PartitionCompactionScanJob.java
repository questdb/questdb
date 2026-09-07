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
 * Periodically scans every table for idle composite (multi-piece) or Parquet-format partitions and dispatches the
 * appropriate compaction entry point.
 */
public class PartitionCompactionScanJob extends SynchronizedJob implements Closeable {
    // Caps how many partitions one sweep hands out: the first sweep after an upgrade can find every
    // qualifying partition of every table at once. The next interval picks up the rest.
    private static final int MAX_DISPATCH_PER_SWEEP = 32;
    // Bounds the clean-parquet memo. Reached only by a database with tens of thousands of parquet
    // partitions; dropping the whole set just costs one more footer read per partition on the next sweep.
    private static final int MAX_MEMO_SIZE = 100_000;
    private static final Log LOG = LogFactory.getLog(PartitionCompactionScanJob.class);
    // Bounds how long a pending-swap record can sit unclaimed. List hygiene, not the interlock:
    // isSwapPending decides against the staging directory itself. Deliberately NOT idleTimeoutMicros,
    // which is configurable down to microseconds.
    private static final long PENDING_SWAP_MEMO_TTL_MICROS = 60 * Micros.MINUTE_MICROS;
    // Fingerprints of parquet partitions already found to hold no dead space. Any write to a partition
    // changes its nameTxn or its file size, so a changed partition cannot match its own stale entry.
    private final LongHashSet cleanParquetPartitions = new LongHashSet();
    private final long checkInterval;
    private final Clock clock;
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final PartitionGeometry geometry = new PartitionGeometry();
    private final long idleTimeoutMicros;
    private final Path other = new Path();
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    // (fingerprint, queuedAtMicros) pairs for swaps handed to a BUSY writer's queue. Without them the
    // next interval copies the whole partition again for a staging directory only one swap can use. The
    // fingerprint carries the source generation, so a partition that moved on is rebuilt at once.
    private final LongList pendingSwaps = new LongList();
    private final Path path = new Path();
    private final Utf8StringSink sidecarName = new Utf8StringSink();
    private final FindVisitor sidecarVisitor = this::copyParquetPartitionSidecar;
    private final TableUtils.SymbolTableProviderFromReader symbolTableProvider = new TableUtils.SymbolTableProviderFromReader();
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final TxReader txReader;
    private int dispatchBudget;
    private long last = 0;
    private int sidecarDstLen;
    private int sidecarSrcLen;

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
                // A build that never reached its swap. Rebuild rather than append onto a directory of
                // unknown completeness.
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
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
            if (writer != null) {
                command.apply(writer, true);
            } else {
                // Queued onto a busy writer: it applies the swap on its own thread, via tick().
                pendingSwaps.add(fingerprint, nowMicros);
            }
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
                // A build that never reached its swap. Rebuild rather than trust a directory of unknown
                // completeness.
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
        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, command)) {
            if (writer != null) {
                command.apply(writer, true);
            } else {
                pendingSwaps.add(fingerprint, nowMicros);
            }
        }
    }

    /**
     * Drops pending entries nothing will ever claim.
     */
    private void expirePendingSwaps(long nowMicros) {
        for (int i = pendingSwaps.size() - 2; i >= 0; i -= 2) {
            if (pendingSwaps.getQuick(i + 1) < nowMicros - PENDING_SWAP_MEMO_TTL_MICROS) {
                pendingSwaps.removeIndexBlock(i, 2);
            }
        }
    }

    /**
     * Reports whether a Parquet partition is a compaction candidate: idle - not written to for {@link
     * #idleTimeoutMicros}, the same window the composite branch applies to {@code lastWriteMicros} - and holding ANY
     * dead space, read from the {@code _pm} footer standalone (no live {@link TableWriter}, no O3 commit in flight).
     */
    private boolean isParquetPartitionIdle(
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long nameTxn,
            long parquetFileSize,
            long nowMicros
    ) {
        final long memoKey = Hash.hashLong256_64(tableToken.getTableId(), partitionTimestamp, nameTxn, parquetFileSize);
        if (cleanParquetPartitions.contains(memoKey)) {
            return false;
        }
        path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long lastModifiedMillis = ff.getLastModified(path.$());
        if (lastModifiedMillis > 0 && lastModifiedMillis * Micros.MILLI_MICROS > nowMicros - idleTimeoutMicros) {
            // Written to inside the idle window. Not memoised: the partition is dirty, and the next
            // sweep after the writes stop is the one meant to pick it up.
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
            if (actualParquetFileSize > 0 && unusedBytes > 0) {
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
     * Reports whether a swap for this exact generation is already staged and queued on a busy writer, so this sweep
     * must leave the partition alone.
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
                    dispatchComposite(tableToken, partitionTimestamp, nowMicros);
                } else {
                    final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
                    if (!isParquetPartitionIdle(tableToken, timestampType, partitionBy, partitionTimestamp, nameTxn, parquetFileSize, nowMicros)) {
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

    private void sweep(long nowMicros) {
        dispatchBudget = MAX_DISPATCH_PER_SWEEP;
        expirePendingSwaps(nowMicros);
        tableTokenBucket.clear();
        engine.getTableTokens(tableTokenBucket, false);
        for (int i = 0, n = tableTokenBucket.size(); i < n && dispatchBudget > 0; i++) {
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
