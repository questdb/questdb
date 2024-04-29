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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

import static io.questdb.cairo.ColumnType.isVarSize;
import static io.questdb.cairo.TableUtils.dFile;
import static io.questdb.cairo.TableUtils.iFile;

public class ConvertOperatorImpl implements Closeable {
    private static final Log LOG = LogFactory.getLog(ConvertOperatorImpl.class);
    private final long appendPageSize;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final long fileOpenOpts;
    private final Path path;
    private final PurgingOperator purgingOperator;
    private final ColumnVersionWriter columnVersionWriter;
    private final int rootLen;
    private final TableWriter tableWriter;
    private final ColumnConversionOffsetSink noopConversionOffsetSink = new ColumnConversionOffsetSink() {
        @Override
        public void setDestSizes(long primarySize, long auxSize) {
        }

        @Override
        public void setSrcOffsets(long primaryOffset, long auxOffset) {
        }
    };

    private int partitionUpdated;
    private SymbolMapReaderImpl symbolMapReader;
    private SymbolMapper symbolMapper;

    public ConvertOperatorImpl(CairoConfiguration configuration, TableWriter tableWriter, ColumnVersionWriter columnVersionWriter, Path path, int rootLen, PurgingOperator purgingOperator) {
        this.configuration = configuration;
        this.tableWriter = tableWriter;
        this.columnVersionWriter = columnVersionWriter;
        this.rootLen = rootLen;
        this.purgingOperator = purgingOperator;
        this.fileOpenOpts = configuration.getWriterFileOpenOpts();
        this.ff = configuration.getFilesFacade();
        this.path = path;
        this.appendPageSize = configuration.getDataAppendPageSize();
    }

    @Override
    public void close() throws IOException {
    }

    public void convertColumn(@NotNull CharSequence columnName, int existingColIndex, int existingType, int columnIndex, int newType, ColumnVersionWriter columnVersionWriter) {
        clear();
        partitionUpdated = 0;
        convertColumn0(columnName, existingColIndex, existingType, columnIndex, newType);
    }

    public void finishColumnConversion() {
        if (partitionUpdated > -1) {
            partitionUpdated = 0;
            purgingOperator.purge(path.trimTo(rootLen), tableWriter.getTableToken(), tableWriter.getPartitionBy(), tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn(), tableWriter.getMetadata(), tableWriter.getTruncateVersion(), tableWriter.getTxn());
            clear();
        }
    }

    private void clear() {
        purgingOperator.clear();
        Misc.free(symbolMapReader);
    }

    private void closeFds(int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd) {
        ff.close(srcFixFd);
        ff.close(srcVarFd);
        ff.close(dstFixFd);
        ff.close(dstVarFd);
    }

    private void convertColumn0(@NotNull CharSequence columnName, int existingColIndex, int existingType, int columnIndex, int newType) {
        try {
            final SymbolMapper symbolMapperWriter;
            if (ColumnType.isSymbol(newType)) {
                if (symbolMapper == null) {
                    symbolMapper = new SymbolMapper();
                }
                symbolMapperWriter = symbolMapper;
                symbolMapperWriter.of(tableWriter, columnIndex);
            } else {
                symbolMapperWriter = null;
            }

            final SymbolTable symbolTable;
            if (ColumnType.isSymbol(existingType)) {
                if (symbolMapReader == null) {
                    symbolMapReader = new SymbolMapReaderImpl();
                }
                long existingColNameTxn = tableWriter.getDefaultColumnNameTxn(existingColIndex);
                int symbolCount = tableWriter.getSymbolMapWriter(existingColIndex).getSymbolCount();
                symbolMapReader.of(configuration, path, columnName, existingColNameTxn, symbolCount);
                symbolTable = symbolMapReader;
            } else {
                symbolTable = null;
            }

            for (int partitionIndex = 0, n = tableWriter.getPartitionCount(); partitionIndex < n; partitionIndex++) {
                final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
                final long maxRow = tableWriter.getPartitionSize(partitionIndex);

                final long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, existingColIndex);
                if (columnTop != -1) {
                    long rowCount = maxRow - columnTop;
                    long partitionNameTxn = tableWriter.getPartitionNameTxn(partitionIndex);

                    path.trimTo(rootLen);
                    TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, partitionNameTxn);
                    int pathTrimToLen = path.size();

                    int srcFixFd = -1, srcVarFd = -1, dstFixFd = -1, dstVarFd = -1;
                    try {
                        long srcFds = openColumnsRO(columnName, partitionTimestamp, existingColIndex, existingType, pathTrimToLen);
                        long dstFds = openColumnsRW(columnName, partitionTimestamp, columnIndex, newType, pathTrimToLen);

                        srcFixFd = Numbers.decodeLowInt(srcFds);
                        srcVarFd = Numbers.decodeHighInt(srcFds);
                        dstFixFd = Numbers.decodeLowInt(dstFds);
                        dstVarFd = Numbers.decodeHighInt(dstFds);

                        LOG.info().$("converting column [at=").$(path.trimTo(pathTrimToLen))
                                .$(", column=").$(columnName)
                                .$(", from=").$(ColumnType.nameOf(existingType))
                                .$(", to=").$(ColumnType.nameOf(newType))
                                .$(", rowCount=").$(rowCount).I$();

                        boolean ok = ColumnTypeConverter.convertColumn(0, rowCount, existingType, srcFixFd, srcVarFd, symbolTable, newType, dstFixFd, dstVarFd, symbolMapperWriter, ff, appendPageSize, noopConversionOffsetSink);
                        if (!ok) {
                            LOG.critical().$("failed to convert column, column is corrupt [at=").$(path.trimTo(pathTrimToLen))
                                    .$(", column=").$(columnName)
                                    .$(", from=").$(ColumnType.nameOf(existingType))
                                    .$(", to=").$(ColumnType.nameOf(newType))
                                    .$(", srcFixFd=").$(srcFixFd)
                                    .$(", srcVarFd=").$(srcVarFd).I$();
                            throw CairoException.nonCritical().put("Failed to convert column. Column data is corrupt [name=").put(columnName).put(']');
                        }
                    } finally {
                        closeFds(srcFixFd, srcVarFd, dstFixFd, dstVarFd);
                    }

                    long existingColTxnVer = tableWriter.getColumnNameTxn(partitionTimestamp, existingColIndex);
                    purgingOperator.add(existingColIndex, existingColTxnVer, partitionTimestamp, partitionNameTxn);
                    partitionUpdated++;
                }
                if (columnTop != tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1)) {
                    if (columnTop != -1) {
                        columnVersionWriter.upsertColumnTop(partitionTimestamp, columnIndex, columnTop);
                    } else {
                        columnVersionWriter.removeColumnTop(partitionTimestamp, columnIndex);
                    }
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private long openColumnsRO(CharSequence name, long partitionTimestamp, int columnIndex, int columnType, int pathTrimToLen) {
        long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        if (isVarSize(columnType)) {
            int fixedFd = TableUtils.openRO(ff, iFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG);
            int varFd = TableUtils.openRO(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG);
            return Numbers.encodeLowHighInts(fixedFd, varFd);
        } else {
            int fixedFd = TableUtils.openRO(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG);
            return Numbers.encodeLowHighInts(fixedFd, -1);
        }
    }

    private long openColumnsRW(CharSequence name, long partitionTimestamp, int columnIndex, int columnType, int pathTrimToLen) {
        long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        if (isVarSize(columnType)) {
            int fixedFd = TableUtils.openRW(ff, iFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG, fileOpenOpts);
            int varFd = TableUtils.openRW(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG, fileOpenOpts);
            return Numbers.encodeLowHighInts(fixedFd, varFd);
        } else {
            int fixedFd = TableUtils.openRW(ff, dFile(path.trimTo(pathTrimToLen), name, columnNameTxn), LOG, fileOpenOpts);
            return Numbers.encodeLowHighInts(fixedFd, -1);
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
