/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
            boolean existingIndexed,
            int columnIndex,
            int newType
    ) {
        clear();
        partitionUpdated = 0;
        convertColumn0(columnName, existingColIndex, existingType, existingIndexed, columnIndex, newType);
    }

    public void finishColumnConversion() {
        if (partitionUpdated > 0 && asyncProcessingErrorCount.get() == 0 && !tableWriter.isDistressed()) {
            partitionUpdated = 0;
            purgingOperator.purge(
                    path.trimTo(rootLen),
                    tableWriter.getTableToken(),
                    tableWriter.getMetadata().getTimestampType(),
                    tableWriter.getPartitionBy(),
                    tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn(),
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
            boolean existingIndexed,
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
                long existingColNameTxn = columnVersionWriter.getDefaultColumnNameTxn(existingColIndex);
                int symbolCount = tableWriter.getSymbolMapWriter(existingColIndex).getSymbolCount();
                symbolMapReader.of(configuration, path, columnName, existingColNameTxn, symbolCount);
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
                        final long maxRow = tableWriter.getPartitionSize(partitionIndex);

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

                                if (dispatchConvertColumnPartitionTask(
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
                                    existingIndexed,
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
                boolean ok = ColumnTypeConverter.convertColumn(0, rowCount,
                        existingType, srcFixFd, srcVarFd, symbolTable,
                        newType, dstFixFd, dstVarFd, symbolMapper,
                        ff, appendPageSize, noopConversionOffsetSink);

                if (!ok) {
                    LOG.critical().$("failed to convert column, column is corrupt [at=")
                            .$(tableWriter.getTableToken())
                            .$(", column=").$safe(columnName).$(", from=").$(ColumnType.nameOf(existingType))
                            .$(", to=").$(ColumnType.nameOf(newType)).$(", srcFixFd=").$(srcFixFd)
                            .$(", srcVarFd=").$(srcVarFd).$(", partition ").$ts(ColumnType.getTimestampDriver(tableWriter.getTimestampType()), partitionTimestamp)
                            .I$();
                    asyncProcessingErrorCount.incrementAndGet();
                }
            }
        } catch (Throwable th) {
            asyncProcessingErrorCount.incrementAndGet();
            LogRecord log = LOG.critical().$("failed to convert column, column is corrupt [at=")
                    .$(tableWriter.getTableToken())
                    .$(", column=").$safe(columnName).$(", from=").$(ColumnType.nameOf(existingType))
                    .$(", to=").$(ColumnType.nameOf(newType))
                    .$(", srcFixFd=").$(srcFixFd).$(", srcVarFd=")
                    .$(srcVarFd).$(", partition ").$ts(ColumnType.getTimestampDriver(tableWriter.getTimestampType()), partitionTimestamp);
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
