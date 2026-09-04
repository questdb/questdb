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
import io.questdb.std.FilesFacade;
import io.questdb.std.Hash;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
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
 * Both candidate kinds are dispatched via {@link CairoEngine#getWriterOrPublishCommand}: an idle writer
 * applies the command in-process, right here on this job's thread; a busy writer gets it queued onto its
 * own {@link io.questdb.tasks.TableWriterTask} command queue and applies it later, on its own thread, via
 * {@link TableWriter#tick()}.
 * <p>
 * A composite candidate's REWRITE is built off a {@link TableReader} snapshot - see
 * {@link #buildCompactedComposite} - so this job never holds the target table's writer for the copy
 * itself, only for the swap that follows ({@link TableWriter#swapCompactedCompositePartition}), which is
 * metadata-only and cheap regardless of how much data the partition holds. If the source partition's
 * generation moved between snapshot and swap - another commit landed on it in between - the swap is
 * rejected as stale and the staged directory discarded; the next sweep interval simply tries again from a
 * fresh snapshot. This is why {@link #dispatchComposite} is safe to retry unconditionally, unlike a
 * PUBLIC entry point a caller might invoke expecting an authoritative answer.
 */
public class PartitionCompactionScanJob extends SynchronizedJob implements Closeable {
    // Caps how many partitions one sweep hands out. The first sweep after an upgrade can find every
    // qualifying partition of every table at once; without a cap it queues the lot back to back and the
    // writers spend the next stretch doing nothing else. What the cap does not dispatch this pass, the
    // next interval picks up.
    private static final int MAX_DISPATCH_PER_SWEEP = 32;
    // Bounds the clean-parquet memo. Reached only by a database with tens of thousands of parquet
    // partitions; dropping the whole set just costs one more footer read per partition on the next sweep.
    private static final int MAX_MEMO_SIZE = 100_000;
    private static final Log LOG = LogFactory.getLog(PartitionCompactionScanJob.class);
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
    private final Path path = new Path();
    private final ObjHashSet<TableToken> tableTokenBucket = new ObjHashSet<>();
    private final TxReader txReader;
    private int dispatchBudget;
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
        cleanParquetPartitions.clear();
        geometry.close();
        other.close();
        parquetMetaReader.clear();
        path.close();
        txReader.close();
    }

    @Override
    protected boolean runSerially() {
        if (checkInterval < 0) {
            // The operator's off switch: a negative cairo.partition.compaction.check.interval disables
            // the background sweep entirely. Writer-side compaction (housekeep -> runCompaction) is
            // unaffected. Zero keeps its meaning of "sweep on every call".
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
     * Builds a composite partition's REWRITE off {@code reader}'s own snapshot - never touches or holds
     * the writer. Mirrors {@link TableWriter#compactPartition0}'s copy loop, sourced from {@code reader}'s
     * own {@link ColumnVersionReader}/{@link PartitionGeometry} instead of a writer's mutable ones, and
     * staged into a directory the writer has not agreed to yet - see
     * {@link TableUtils#COMPACTING_DIR_MARKER}.
     * <p>
     * {@code upcomingTableTxn} passed to {@link FrameAlgebra#append} is deliberately left unset
     * ({@code -1L}): the posting index this copy builds is throwaway anyway -
     * {@link TableWriter#swapCompactedCompositePartition} unconditionally reseals from the column data at
     * swap time - and this build holds no writer to know its eventual commit txn by.
     *
     * @return a command ready to publish, or {@code null} if the partition holds no live rows to keep -
     *         an all-dead composite partition needs no swap.
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

        other.of(configuration.getDbRoot()).concat(tableToken.getDirName());
        TableUtils.setPathForNativePartition(other, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
        other.put(TableUtils.COMPACTING_DIR_MARKER).put(writerTxn);

        final CompositePartitionSwapCommand command = new CompositePartitionSwapCommand();
        // Strictly before the build: of() resets the command, the recorder included, so arming it here
        // instead of after the copy is what keeps the tops the build is about to record.
        command.of(tableToken, tableToken.getTableId(), partitionTimestamp, srcNameTxn, writerTxn, liveRows);
        final ColumnTopRecorder columnTops = command.getColumnTops();
        Frame targetFrame = null;
        boolean built = false;
        try {
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
                // Best-effort: a partial build must not leave a half-written staging directory behind for
                // the next sweep to trip over. TableWriter#swapCompactedCompositePartition only ever
                // cleans up a staging directory it was actually handed via a command; one that never
                // finished building never produced one.
                if (ff.exists(other.$())) {
                    ff.rmdir(other, false);
                }
            }
        }

        return command;
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
     * Builds the REWRITE off a {@link TableReader} snapshot - see {@link #buildCompactedComposite} - then
     * publishes a {@link CompositePartitionSwapCommand} the same way {@link #dispatchParquet} publishes
     * its own command: {@link CairoEngine#getWriterOrPublishCommand} applies it directly on an idle
     * writer, or queues it onto a busy one's own {@link io.questdb.tasks.TableWriterTask} command queue,
     * applied later on the writer's own thread via {@link TableWriter#tick()}.
     * <p>
     * Unlike the old {@code compactPartitionNoCommit}-based dispatch this replaced, the writer is never
     * held for the copy itself - only for the swap, which is metadata-only. A stale swap (the source
     * partition's generation moved between snapshot and swap - see
     * {@link TableWriter#swapCompactedCompositePartition}) throws
     * {@link TableReferenceOutOfDateException}; left uncaught here, {@link #sweep}'s own per-table catch
     * already handles it - the next sweep interval builds a fresh snapshot and tries again.
     */
    private void dispatchComposite(TableToken tableToken, long partitionTimestamp) {
        final CompositePartitionSwapCommand command;
        try (TableReader reader = engine.getReader(tableToken)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTimestamp);
            if (partitionIndex < 0 || !reader.getTxFile().isPartitionComposite(partitionIndex)) {
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
            }
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
     * reports whether the partition holds ANY dead space.
     * <p>
     * Deliberately NOT {@link O3PartitionJob}'s {@code parquet.o3.rewrite.unused.*} thresholds. Those two
     * decide whether an IN-FLIGHT O3 commit should pay for a rewrite it did not plan on - a latency
     * tradeoff inside a user-visible write. This sweep has no such constraint: it runs on its own thread,
     * on its own schedule, on a partition nothing is writing to, so a partition sitting at 40% dead space
     * (below the 0.5 ratio) or wasting less than a gigabyte (below the absolute rule) is worth reclaiming
     * too. It converges rather than churns: once rewritten, a partition nobody writes to has zero dead
     * bytes, fails this test on every later sweep and is never dispatched again - one rewrite per
     * partition ever, not repeated work.
     * <p>
     * The negative answer is memoised on {@code (tableId, partitionTimestamp, nameTxn, parquetFileSize)},
     * because with the threshold at "any dead space" re-reading the footer of every already-clean
     * partition on every pass would otherwise BE this job's steady-state cost. Any write to a partition
     * changes its {@code nameTxn} or its file size, so a changed partition never matches its stale entry.
     */
    private boolean isParquetPartitionIdle(
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            long nameTxn,
            long parquetFileSize
    ) {
        final long memoKey = Hash.hashLong256_64(tableToken.getTableId(), partitionTimestamp, nameTxn, parquetFileSize);
        if (cleanParquetPartitions.contains(memoKey)) {
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
                    dispatchComposite(tableToken, partitionTimestamp);
                } else {
                    final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
                    if (!isParquetPartitionIdle(tableToken, timestampType, partitionBy, partitionTimestamp, nameTxn, parquetFileSize)) {
                        continue;
                    }
                    dispatchBudget--;
                    dispatchParquet(tableToken, partitionTimestamp);
                }
            }
        }
    }

    private void sweep(long nowMicros) {
        dispatchBudget = MAX_DISPATCH_PER_SWEEP;
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
