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
    private final FilesFacade ff;
    private final long fileOpenOpts;
    private final Path path;
    private final PurgingOperator purgingOperator;
    private final int rootLen;
    private final TableWriter tableWriter;
    private IndexBuilder indexBuilder;
    private int partitionUpdated;

    public ConvertOperatorImpl(CairoConfiguration configuration, TableWriter tableWriter, Path path, int rootLen, PurgingOperator purgingOperator) {
        this.tableWriter = tableWriter;
        this.rootLen = rootLen;
        this.purgingOperator = purgingOperator;
        this.indexBuilder = new IndexBuilder(configuration);
        this.fileOpenOpts = configuration.getWriterFileOpenOpts();
        this.ff = configuration.getFilesFacade();
        this.path = path;
        this.appendPageSize = configuration.getDataAppendPageSize();

    }

    @Override
    public void close() throws IOException {
        indexBuilder = Misc.free(indexBuilder);
    }

    public void convertColumn(@NotNull CharSequence columnName, int existingColIndex, int existingType, int columnIndex, int newType) {
        clear();
        partitionUpdated = 0;
        try {
            for (int partitionIndex = 0, n = tableWriter.getPartitionCount(); partitionIndex < n; partitionIndex++) {
                final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
                final long maxRow = tableWriter.getPartitionSize(partitionIndex);

                final long columnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1);
                if (columnTop != -1) {
                    long partitionNameTxn = tableWriter.getPartitionNameTxn(partitionIndex);

                    path.trimTo(rootLen);
                    TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, partitionNameTxn);
                    int pathTrimToLen = path.size();

                    long srcFds = openColumnsRO(columnName, partitionTimestamp, existingColIndex, existingType, pathTrimToLen);
                    long dstFds = openColumnsRW(columnName, partitionTimestamp, columnIndex, newType, pathTrimToLen);

                    int srcFixFd = Numbers.decodeLowInt(srcFds);
                    int srcVarFd = Numbers.decodeHighInt(srcFds);
                    int dstFixFd = Numbers.decodeLowInt(dstFds);
                    int dstVarFd = Numbers.decodeHighInt(dstFds);

                    if (columnTop != tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1)) {
                        tableWriter.upsertColumnVersion(partitionTimestamp, columnIndex, columnTop);
                    }

                    LOG.info().$("converting column [at=").$(path.trimTo(pathTrimToLen)).$(", column=").$(columnName).$(", from=").$(ColumnType.nameOf(existingType)).$(", to=").$(ColumnType.nameOf(newType)).I$();

                    ColumnTypeConverter.convertColumn(maxRow - columnTop, srcFixFd, srcVarFd, dstFixFd, dstVarFd, existingType, newType, ff, appendPageSize);

                    long existingColTxnVer = tableWriter.getColumnNameTxn(partitionTimestamp, existingColIndex);
                    purgingOperator.add(existingColIndex, existingColTxnVer, partitionTimestamp, partitionNameTxn);
                    partitionUpdated++;
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
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
}
