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

package io.questdb.griffin;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTaskJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeConverter;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.ColumnType.isVarSize;
import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;

public class ConvertOperatorImpl implements Closeable {
    private static final Log LOG = LogFactory.getLog(ConvertOperatorImpl.class);
    private final long appendPageSize;
    private final AtomicInteger asyncProcessingErrorCount = new AtomicInteger();
    private final ColumnVersionWriter columnVersionWriter;
    private final CairoConfiguration configuration;
    private final SOUnboundedCountDownLatch countDownLatch;
    private final FilesFacade ff;
    private final int fileOpenOpts;
    private final MessageBus messageBus;
    private final ColumnConversionOffsetSink noopConversionOffsetSink = new ColumnConversionOffsetSink() {
        @Override
        public void setDestSizes(long primarySize, long auxSize) {
        }

        @Override
        public void setSrcOffsets(long primaryOffset, long auxOffset) {
        }
    };
    private final Path path;
    private final LongList pieceRowCounts = new LongList();
    private final LongList pieceRowOffsets = new LongList();
    private final PurgingOperator purgingOperator;
    private final int rootLen;
    private final TableWriter tableWriter;
    private final Clock timer;
    private CharSequence columnName;
    private long fixedFd;
    private int partitionUpdated;
    private SymbolMapReaderImpl symbolMapReader;
    private SymbolMapper symbolMapper;
    private final TableWriter.ColumnTaskHandler cthConvertPartitionHandler = this::cthConvertPartitionHandler;
    private long varFd;

    public ConvertOperatorImpl(
            CairoConfiguration configuration,
            TableWriter tableWriter,
            ColumnVersionWriter columnVersionWriter,
            Path path,
            int rootLen,
            PurgingOperator purgingOperator,
            MessageBus messageBus
    ) {
        this.configuration = configuration;
        this.tableWriter = tableWriter;
        this.columnVersionWriter = columnVersionWriter;
        this.rootLen = rootLen;
        this.purgingOperator = purgingOperator;
        this.fileOpenOpts = configuration.getWriterFileOpenOpts();
        this.ff = configuration.getFilesFacade();
        this.path = path;
        this.appendPageSize = configuration.getDataAppendPageSize();
        this.messageBus = messageBus;
        this.countDownLatch = new SOUnboundedCountDownLatch();
        this.timer = configuration.getMicrosecondClock();
    }

    @Override
    public void close() {
    }

    public void convertColumn(
            @NotNull String columnName,
            int existingColIndex,
            int existingType,
            byte existingIndexType,
            int columnIndex,
            int newType
    ) {
        clear();
        partitionUpdated = 0;
        convertColumn0(columnName, existingColIndex, existingType, existingIndexType, columnIndex, newType);
    }

    public void finishColumnConversion() {
        if (partitionUpdated > 0 && asyncProcessingErrorCount.get() == 0 && !tableWriter.isDistressed()) {
            partitionUpdated = 0;
            purgingOperator.purge(
                    path.trimTo(rootLen),
                    tableWriter.getTableToken(),
                    tableWriter.getMetadata().getTimestampType(),
                    tableWriter.getPartitionBy(),
                    tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn()
                            || tableWriter.isCheckpointInProgress(),
                    tableWriter.getTruncateVersion(),
                    tableWriter.getTxn()
            );
        }
        clear();
    }

    private void clear() {
        purgingOperator.clear();
        Misc.free(symbolMapReader);
    }

    private void closeFds(long srcFixFd, long srcVarFd, long dstFixFd, long dstVarFd) {
        LOG.debug().$("closing fds[srcFixFd=").$(srcFixFd)
                .$(", srcVarFd=").$(srcVarFd)
                .$(", dstFixFd=").$(dstFixFd)
                .$(", dstVarFd=").$(dstVarFd)
                .I$();
        ff.close(srcFixFd);
        ff.close(srcVarFd);
        ff.close(dstFixFd);
        ff.close(dstVarFd);
    }

    private void consumeConversionTasks(RingQueue<ColumnTask> queue, int queuedCount, boolean checkStatus) {
        // This is work stealing, can run tasks from other table writers
        final Sequence subSeq = this.messageBus.getColumnTaskSubSeq();
        while (!countDownLatch.done(queuedCount)) {
            long cursor = subSeq.next();
            if (cursor > -1) {
                ColumnTaskJob.processColumnTask(queue.get(cursor), cursor, subSeq);
            } else {
                Os.pause();
            }
        }

        if (checkStatus && asyncProcessingErrorCount.get() > 0) {
            throw CairoException.critical(0)
                    .put("column conversion failed, see logs for details [table=").put(tableWriter.getTableToken())
                    .put(", tableDir=").put(tableWriter.getTableToken().getDirName())
                    .put(", column=").put(columnName)
                    .put(']');
        }
    }

    private void convertColumn0(
            @NotNull String columnName,
            int existingColIndex,
            int existingType,
            byte existingIndexType,
            int columnIndex,
            int newType
    ) {
        try {
            this.columnName = columnName;

            if (ColumnType.isSymbol(newType)) {
                if (symbolMapper == null) {
                    symbolMapper = new SymbolMapper();
                }
                symbolMapper.of(tableWriter, columnIndex);
            }

            if (ColumnType.isSymbol(existingType)) {
                if (symbolMapReader == null) {
                    symbolMapReader = new SymbolMapReaderImpl();
                }
                long existingSymbolTableNameTxn = columnVersionWriter.getSymbolTableNameTxn(existingColIndex);
                int symbolCount = tableWriter.getSymbolMapWriter(existingColIndex).getSymbolCount();
                symbolMapReader.of(configuration, path, columnName, existingSymbolTableNameTxn, symbolCount);
            }

            int queueCount = 0;
            countDownLatch.reset();
            asyncProcessingErrorCount.set(0);
            long start = timer.getTicks();
            long totalRows = 0;

            for (int partitionIndex = 0, n = tableWriter.getPartitionCount(); partitionIndex < n; partitionIndex++) {
                if (asyncProcessingErrorCount.get() == 0) {
                    try {
                        final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
                        // The conversion rewrites a FILE, so it spans the rows the file spans - E, not the
                        // live row count. A composite partition scatters its live rows over [0, E) and the
                        // gaps hold superseded images, so a walk that stops at the live count leaves live
                        // rows above it unconverted.
                        final long maxRow = Math.max(
                                tableWriter.getPartitionSize(partitionIndex),
                                tableWriter.getPartitionPhysicalRowCount(partitionIndex)
                        );

                        final long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, existingColIndex);
                        if (columnTop > -1) {
                            long rowCount = maxRow - columnTop;
                            long partitionNameTxn = tableWriter.getPartitionNameTxn(partitionIndex);

                            if (rowCount > 0) {
                                path.trimTo(rootLen);
                                TableUtils.setPathForNativePartition(
                                        path,
                                        tableWriter.getMetadata().getTimestampType(),
                                        tableWriter.getPartitionBy(),
                                        partitionTimestamp,
                                        partitionNameTxn
                                );
                                int pathTrimToLen = path.size();

                                long srcFixFd = -1, srcVarFd = -1, dstFixFd = -1, dstVarFd = -1;
                                try {
                                    openColumnsRO(columnName, partitionTimestamp, existingColIndex, existingType, pathTrimToLen);
                                    srcFixFd = this.fixedFd;
                                    srcVarFd = this.varFd;

                                    openColumnsRW(columnName, partitionTimestamp, columnIndex, newType, pathTrimToLen);
                                    dstFixFd = this.fixedFd;
                                    dstVarFd = this.varFd;

                                    LOG.info().$("converting column [at=").$safe(path.trimTo(pathTrimToLen))
                                            .$(", column=").$safe(columnName)
                                            .$(", from=").$(ColumnType.nameOf(existingType))
                                            .$(", to=").$(ColumnType.nameOf(newType))
                                            .$(", rowCount=").$(rowCount)
                                            .I$();
                                    totalRows += rowCount;
                                } catch (Throwable th) {
                                    closeFds(srcFixFd, srcVarFd, dstFixFd, dstVarFd);
                                    throw th;
                                }

                                // A composite partition's dead space (a relocated piece's old, superseded
                                // copy) must never be read - it can be short, missing, or simply garbage
                                // relative to what this conversion expects. Piece-walking is available for
                                // every direction this converter supports EXCEPT symbol and decimal: source
                                // and destination are each either a fixed-width type or STRING/VARCHAR, and
                                // "fixed" deliberately excludes SYMBOL (isFixedSize) since a symbol key is
                                // meaningless without the dictionary remap dispatchConvertColumnPartitionTask
                                // still routes it through.
                                final boolean srcPieceable = ColumnType.isFixedSize(existingType) || existingType == ColumnType.STRING || existingType == ColumnType.VARCHAR;
                                final boolean dstFixed = ColumnType.isFixedSize(newType);
                                final boolean dstVarStringy = newType == ColumnType.STRING || newType == ColumnType.VARCHAR;
                                final boolean pieceWalkable = srcPieceable && (dstFixed || dstVarStringy);
                                final int pieceCount = pieceWalkable
                                        ? tableWriter.getGeometry().getPieceCount(partitionIndex)
                                        : 1;
                                if (dstFixed && pieceWalkable && pieceCount > 1) {
                                    // For a fixed-width destination, walk the partition's own pieces
                                    // (its live sections) instead of the flat [columnTop, maxRow) range: convert
                                    // each piece from its own file position, and pad the gaps between them -
                                    // never reading them - so every piece keeps the same absolute row it had,
                                    // which is the address a reader still uses to find it.
                                    convertToFixedDestByPieces(
                                            partitionIndex, pieceCount, columnTop, maxRow,
                                            existingType, newType, srcFixFd, srcVarFd, dstFixFd
                                    );
                                    closeFds(srcFixFd, srcVarFd, dstFixFd, dstVarFd);
                                } else if (dstVarStringy && pieceWalkable && pieceCount > 1) {
                                    // Same piece walk, for a STRING/VARCHAR destination: the aux (index) vector
                                    // is padded like a live write would extend it, but the data vector is not -
                                    // a dead piece's rows contribute no bytes to the converted data file.
                                    convertToVarDestByPieces(
                                            partitionIndex, pieceCount, columnTop, maxRow,
                                            existingType, newType, srcFixFd, srcVarFd, dstFixFd, dstVarFd
                                    );
                                    closeFds(srcFixFd, srcVarFd, dstFixFd, dstVarFd);
                                } else if (dispatchConvertColumnPartitionTask(
                                        existingType, newType, srcFixFd, srcVarFd, dstFixFd, dstVarFd, rowCount, partitionTimestamp)
                                ) {
                                    queueCount++;
                                }
                            }

                            long existingColTxnVer = tableWriter.getColumnNameTxn(partitionTimestamp, existingColIndex);
                            purgingOperator.add(
                                    existingColIndex,
                                    columnName,
                                    existingType,
                                    existingIndexType,
                                    existingColTxnVer,
                                    partitionTimestamp,
                                    partitionNameTxn);
                            partitionUpdated++;
                        }
                        if (columnTop != tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1)) {
                            long partTs = tableWriter.getPartitionBy() != PartitionBy.NONE
                                    ? partitionTimestamp
                                    : TxReader.DEFAULT_PARTITION_TIMESTAMP;
                            columnVersionWriter.upsertColumnTop(partTs, columnIndex, columnTop > -1 ? columnTop : maxRow);
                        }
                    } catch (Throwable th) {
                        LOG.error().$("error converting column [at=").$(tableWriter.getTableToken())
                                .$(", column=").$safe(columnName).$(", from=").$(ColumnType.nameOf(existingType))
                                .$(", to=").$(ColumnType.nameOf(newType))
                                .$(", error=").$(th).I$();
                        asyncProcessingErrorCount.incrementAndGet();
                        // wait all async tasks to finish to exit the method at known state
                        consumeConversionTasks(messageBus.getColumnTaskQueue(), queueCount, false);
                        throw th;
                    }
                }
            }
            consumeConversionTasks(messageBus.getColumnTaskQueue(), queueCount, true);
            long elapsed = timer.getTicks() - start;
            LOG.info().$("completed column conversion [at=").$(tableWriter.getTableToken())
                    .$(", column=").$safe(columnName).$(", from=").$(ColumnType.nameOf(existingType))
                    .$(", to=").$(ColumnType.nameOf(newType))
                    .$(", partitions=").$(partitionUpdated)
                    .$(", rows=").$(totalRows)
                    .$(", elapsed=").$(elapsed / 1000).$("ms]").I$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    /**
     * Fills {@link #pieceRowOffsets} / {@link #pieceRowCounts} with this partition's own pieces, clipped
     * to the column's own span and sorted into file (row) order - pieces are recorded in ascending tsLo
     * order, not file-row order, because a merge-append relocates a piece to the tail, so the two
     * diverge, and a piece walk has to go in file order.
     */
    private void collectPieces(int partitionIndex, int pieceCount, long columnTop, long maxRow) {
        final PartitionGeometry geometry = tableWriter.getGeometry();
        pieceRowOffsets.clear();
        pieceRowCounts.clear();
        for (int p = 0; p < pieceCount; p++) {
            final long pieceRowOffset = geometry.getPieceRowOffset(partitionIndex, p);
            final long pieceRowCount = geometry.getPieceRowCount(partitionIndex, p);
            // Clip to what this column actually spans - a piece wholly below columnTop predates the
            // column entirely and contributes nothing, not even a gap (the walk below never starts
            // before columnTop in the first place).
            final long lo = Math.max(pieceRowOffset, columnTop);
            final long hi = Math.min(pieceRowOffset + pieceRowCount, maxRow);
            if (hi > lo) {
                pieceRowOffsets.add(lo);
                pieceRowCounts.add(hi - lo);
            }
        }
        sortPiecesByRowOffset(pieceRowOffsets, pieceRowCounts);
    }

    /**
     * Piece walk into a FIXED destination, from either a fixed or a STRING/VARCHAR source. Every dead
     * gap is padded with the destination type's null, regardless of source shape - a dead row costs the
     * same fixed-width slot whether it was ever going to be read from a fixed file or a var one.
     */
    private void convertToFixedDestByPieces(
            int partitionIndex,
            int pieceCount,
            long columnTop,
            long maxRow,
            int existingType,
            int newType,
            long srcFixFd,
            long srcVarFd,
            long dstFixFd
    ) {
        collectPieces(partitionIndex, pieceCount, columnTop, maxRow);
        final boolean srcIsVar = ColumnType.isVarSize(existingType);

        long cursor = columnTop;
        for (int i = 0, n = pieceRowOffsets.size(); i < n; i++) {
            final long pieceLo = pieceRowOffsets.getQuick(i);
            final long pieceRowCount = pieceRowCounts.getQuick(i);
            if (pieceLo > cursor) {
                ColumnTypeConverter.padFixedGap(cursor - columnTop, pieceLo - cursor, newType, dstFixFd, ff);
            }
            final long segmentOffset = pieceLo - columnTop;
            if (!srcIsVar) {
                ColumnTypeConverter.convertFixedToFixed(
                        pieceRowCount, segmentOffset, segmentOffset, srcFixFd, dstFixFd, existingType, newType, ff, noopConversionOffsetSink
                );
            } else if (existingType == ColumnType.STRING) {
                ColumnTypeConverter.convertStringToFixedPiece(segmentOffset, pieceRowCount, srcFixFd, srcVarFd, dstFixFd, newType, ff);
            } else {
                ColumnTypeConverter.convertVarcharToFixedPiece(segmentOffset, pieceRowCount, srcFixFd, srcVarFd, dstFixFd, newType, ff);
            }
            cursor = pieceLo + pieceRowCount;
        }
        if (cursor < maxRow) {
            ColumnTypeConverter.padFixedGap(cursor - columnTop, maxRow - cursor, newType, dstFixFd, ff);
        }
    }

    /**
     * Piece walk into a STRING/VARCHAR destination, from either a fixed or a STRING/VARCHAR source. The
     * gap padding (aux entries per dead row, dense data vector) is identical regardless of source shape -
     * {@link ColumnTypeConverter#padVarGap} only ever looks at the destination type.
     */
    private void convertToVarDestByPieces(
            int partitionIndex,
            int pieceCount,
            long columnTop,
            long maxRow,
            int existingType,
            int newType,
            long srcFixFd,
            long srcVarFd,
            long dstFixFd,
            long dstVarFd
    ) {
        collectPieces(partitionIndex, pieceCount, columnTop, maxRow);
        final boolean srcIsVar = ColumnType.isVarSize(existingType);

        if (newType == ColumnType.STRING) {
            ColumnTypeConverter.seedStringAuxVector(dstFixFd, ff);
        }

        long cursor = columnTop;
        for (int i = 0, n = pieceRowOffsets.size(); i < n; i++) {
            final long pieceLo = pieceRowOffsets.getQuick(i);
            final long pieceRowCount = pieceRowCounts.getQuick(i);
            if (pieceLo > cursor) {
                ColumnTypeConverter.padVarGap(cursor - columnTop, pieceLo - cursor, newType, dstFixFd, dstVarFd, ff);
            }
            final long segmentOffset = pieceLo - columnTop;
            if (!srcIsVar) {
                if (newType == ColumnType.VARCHAR) {
                    ColumnTypeConverter.convertFixedToVarcharPiece(segmentOffset, pieceRowCount, srcFixFd, existingType, dstFixFd, dstVarFd, ff, appendPageSize);
                } else {
                    ColumnTypeConverter.convertFixedToStringPiece(segmentOffset, pieceRowCount, srcFixFd, existingType, dstFixFd, dstVarFd, ff, appendPageSize);
                }
            } else if (existingType == ColumnType.STRING) {
                ColumnTypeConverter.convertStringToVarcharPiece(segmentOffset, pieceRowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, ff, appendPageSize);
            } else {
                ColumnTypeConverter.convertVarcharToStringPiece(segmentOffset, pieceRowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, ff, appendPageSize);
            }
            cursor = pieceLo + pieceRowCount;
        }
        if (cursor < maxRow) {
            ColumnTypeConverter.padVarGap(cursor - columnTop, maxRow - cursor, newType, dstFixFd, dstVarFd, ff);
        }
    }

    private static void sortPiecesByRowOffset(LongList rowOffsets, LongList rowCounts) {
        for (int i = 1, n = rowOffsets.size(); i < n; i++) {
            final long keyOffset = rowOffsets.getQuick(i);
            final long keyCount = rowCounts.getQuick(i);
            int j = i - 1;
            while (j >= 0 && rowOffsets.getQuick(j) > keyOffset) {
                rowOffsets.setQuick(j + 1, rowOffsets.getQuick(j));
                rowCounts.setQuick(j + 1, rowCounts.getQuick(j));
                j--;
            }
            rowOffsets.setQuick(j + 1, keyOffset);
            rowCounts.setQuick(j + 1, keyCount);
        }
    }

    private void cthConvertPartitionHandler(
            int existingType,
            int newType,
            long srcFixFd,
            long srcVarFd,
            long dstFixFd,
            long dstVarFd,
            long partitionTimestamp,
            long rowCount
    ) {
        try {
            if (asyncProcessingErrorCount.get() == 0) {

                SymbolTable symbolTable = ColumnType.isSymbol(existingType) ? symbolMapReader.newSymbolTableView() : null;
                boolean ok = ColumnTypeConverter.convertColumn(
                        0,
                        rowCount,
                        existingType,
                        srcFixFd,
                        srcVarFd,
                        symbolTable,
                        newType,
                        dstFixFd,
                        dstVarFd,
                        symbolMapper,
                        ff,
                        appendPageSize,
                        noopConversionOffsetSink
                );

                if (!ok) {
                    LOG.critical().$("failed to convert column, column is corrupt [at=")
                            .$(tableWriter.getTableToken())
                            .$(", column=").$safe(columnName)
                            .$(", from=").$(ColumnType.nameOf(existingType))
                            .$(", to=").$(ColumnType.nameOf(newType))
                            .$(", srcFixFd=").$(srcFixFd)
                            .$(", srcVarFd=").$(srcVarFd)
                            .$(", partition ").$ts(ColumnType.getTimestampDriver(tableWriter.getTimestampType()), partitionTimestamp)
                            .I$();
                    asyncProcessingErrorCount.incrementAndGet();
                }
            }
        } catch (Throwable th) {
            asyncProcessingErrorCount.incrementAndGet();
            LogRecord log = LOG.critical().$("failed to convert column, column is corrupt [at=")
                    .$(tableWriter.getTableToken())
                    .$(", column=").$safe(columnName)
                    .$(", from=").$(ColumnType.nameOf(existingType))
                    .$(", to=").$(ColumnType.nameOf(newType))
                    .$(", srcFixFd=").$(srcFixFd)
                    .$(", srcVarFd=").$(srcVarFd)
                    .$(", partition ").$ts(ColumnType.getTimestampDriver(tableWriter.getTimestampType()), partitionTimestamp);
            if (th instanceof CairoException) {
                log.$(", errno=").$(((CairoException) th).getErrno());
            }
            log.$(", ex=").$(th).I$();
        } finally {
            closeFds(srcFixFd, srcVarFd, dstFixFd, dstVarFd);
        }
    }

    private boolean dispatchConvertColumnPartitionTask(
            int existingType,
            int newType,
            long srcFixFd,
            long srcVarFd,
            long dstFixFd,
            long dstVarFd,
            long rowCount,
            long partitionTimestamp
    ) {
        if (!ColumnType.isSymbol(newType)) {
            final Sequence pubSeq = this.messageBus.getColumnTaskPubSeq();
            final RingQueue<ColumnTask> queue = this.messageBus.getColumnTaskQueue();
            long cursor = pubSeq.next();
            // Pass column index as -1 when it's designated timestamp column to o3 move method
            if (cursor > -1) {
                try {
                    final ColumnTask task = queue.get(cursor);
                    task.of(
                            countDownLatch,
                            existingType,
                            newType,
                            srcFixFd,
                            srcVarFd,
                            dstFixFd,
                            dstVarFd,
                            partitionTimestamp,
                            rowCount,
                            cthConvertPartitionHandler);
                    return true;
                } finally {
                    pubSeq.done(cursor);
                }
            }
        }

        // Cannot write in parallel to SYMBOL column type, fall back to single thread conversion
        cthConvertPartitionHandler(existingType, newType, srcFixFd, srcVarFd, dstFixFd, dstVarFd, partitionTimestamp, rowCount);
        return false;
    }


    private void openColumnsRO(CharSequence name, long partitionTimestamp, int columnIndex, int columnType, int pathTrimToLen) {
        long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        if (isVarSize(columnType)) {
            fixedFd = TableUtils.openRO(ff, iFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG);
            try {
                varFd = TableUtils.openRO(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG);
            } catch (Throwable e) {
                ff.close(fixedFd);
                throw e;
            }
        } else {
            fixedFd = TableUtils.openRO(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG);
            varFd = -1;
        }
    }

    private void openColumnsRW(CharSequence name, long partitionTimestamp, int columnIndex, int columnType, int pathTrimToLen) {
        long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        if (isVarSize(columnType)) {
            fixedFd = TableUtils.openRW(ff, iFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG, fileOpenOpts);
            try {
                varFd = TableUtils.openRW(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG, fileOpenOpts);
            } catch (Throwable e) {
                ff.close(fixedFd);
                throw e;
            }
        } else {
            fixedFd = TableUtils.openRW(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG, fileOpenOpts);
            varFd = -1;
        }
    }

    private static class SymbolMapper implements SymbolMapWriterLite {
        private int columnIndex;
        private TableWriter tableWriter;

        @Override
        public int resolveSymbol(CharSequence value) {
            return tableWriter.getSymbolIndexNoTransientCountUpdate(columnIndex, value);
        }

        void of(TableWriter tw, int columnIndex) {
            this.tableWriter = tw;
            this.columnIndex = columnIndex;
        }
    }
}
