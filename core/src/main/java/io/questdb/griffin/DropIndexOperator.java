/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import java.io.IOException;

import static io.questdb.cairo.TableUtils.dFile;

public class DropIndexOperator extends UpdateOperator {
    private static final Log LOG = LogFactory.getLog(DropIndexOperator.class);

    private Path auxPath;
    private final int auxRootLen;

    public DropIndexOperator(CairoConfiguration configuration, MessageBus messageBus, TableWriter tableWriter) {
        super(configuration, messageBus, tableWriter);
        auxPath = new Path().of(configuration.getRoot());
        auxRootLen = auxPath.length();
        indexBuilder = Misc.free(indexBuilder);
    }

    @Override
    public void close() {
        super.close();
        auxPath = Misc.free(auxPath);
    }

    @Override
    public long executeUpdate(SqlExecutionContext sqlExecutionContext, UpdateOperation op) throws SqlException, ReaderOutOfDateException {
        throw new UnsupportedOperationException();
    }

    public void executeDropIndex(CharSequence tableName, CharSequence columnName, int columnIndex) {
        if (Os.type == Os.WINDOWS) {
            throw CairoException.instance(0).put("DROP INDEX is not supported on Windows");
        }
        updateColumnIndexes.clear();
        updateColumnIndexes.add(columnIndex);
        cleanupColumnVersions.clear();
        try {
            if (tableWriter.inTransaction()) {
                LOG.info()
                        .$("committing current transaction before DROP INDEX execution [txn=")
                        .$(tableWriter.getTxn())
                        .$(", table=")
                        .$(tableName)
                        .$(", column=")
                        .$(columnName)
                        .I$();
                tableWriter.commit();
            }

            final int partitionBy = tableWriter.getPartitionBy();
            for (int partitionIndex = 0, limit = tableWriter.getPartitionCount(); partitionIndex < limit; partitionIndex++) {
                final long partitionNameTxn = tableWriter.getPartitionNameTxn(partitionIndex);
                final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
                final long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                final long columnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1L);
                try {
                    // check that the column file exists
                    LPSZ srcDFile = partitionDFile(
                            path,
                            rootLen,
                            tableName,
                            partitionBy,
                            partitionTimestamp,
                            partitionNameTxn,
                            columnName,
                            columnNameTxn
                    );
                    if (!Files.exists(srcDFile)) {
                        throw CairoException.instance(0)
                                .put("Impossible! this file should exist: ")
                                .put(path.toString());
                    }
                    // add to cleanup tasks, the index will be removed in due time
                    cleanupColumnVersions.add(columnIndex, columnNameTxn, partitionTimestamp, partitionNameTxn);

                    // bump up column version, metadata will be updated later
                    tableWriter.upsertColumnVersion(partitionTimestamp, columnIndex, columnTop);
                    final long columnDropIndexTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);

                    // create hard link to column data
                    LPSZ hardLinkDFile = partitionDFile(
                            auxPath,
                            auxRootLen,
                            tableName,
                            partitionBy,
                            partitionTimestamp,
                            partitionNameTxn,
                            columnName,
                            columnDropIndexTxn
                    );
                    Files.hardLink(srcDFile, hardLinkDFile);
                } finally {
                    path.trimTo(rootLen);
                    auxPath.trimTo(auxRootLen);
                }
            }
        } catch (Throwable th) {
            LOG.error().$("could not update").$(th).$();
            tableWriter.rollbackUpdate();
            throw th;
        }
    }

    private static LPSZ partitionDFile(
            Path path,
            int rootLen,
            CharSequence tableName,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn,
            CharSequence columnName,
            long columnNameTxn
    ) {
        path.trimTo(rootLen);
        path.concat(tableName);
        TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
        return dFile(path, columnName, columnNameTxn);

    }
}
