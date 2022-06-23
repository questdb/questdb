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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.dFile;

public class DropIndexOperator implements Closeable {
    private static final Log LOG = LogFactory.getLog(DropIndexOperator.class);
    private final LongList cleanupColumnVersions = new LongList();
    private final MessageBus messageBus;
    private final FilesFacade ff;
    private final TableWriter tableWriter;
    private Path path;
    private final int rootLen;
    private Path auxPath;
    private final int auxRootLen;

    public DropIndexOperator(CairoConfiguration configuration, MessageBus messageBus, TableWriter tableWriter) {
        this.messageBus = messageBus;
        this.tableWriter = tableWriter;
        ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot());
        rootLen = path.length();
        auxPath = new Path().of(configuration.getRoot());
        auxRootLen = auxPath.length();
    }

    @Override
    public void close() {
        path = Misc.free(path);
        auxPath = Misc.free(auxPath);
    }

    public void executeDropIndex(CharSequence tableName, CharSequence columnName, int columnIndex) {
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

            cleanupColumnVersions.clear();
            final int partitionBy = tableWriter.getPartitionBy();
            for (int pIndex = 0, limit = tableWriter.getPartitionCount(); pIndex < limit; pIndex++) {

                final long partitionTimestamp = tableWriter.getPartitionTimestamp(pIndex);
                final long columnVersion = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                final long partitionNameTxn = tableWriter.getPartitionNameTxn(pIndex);
                final long columnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, -1L);

                try {
                    // add to cleanup tasks, the index will be removed in due time
                    cleanupColumnVersions.add(columnVersion, partitionTimestamp, partitionNameTxn, columnIndex);

                    // bump up column version, metadata will be updated later
                    tableWriter.upsertColumnVersion(partitionTimestamp, columnIndex, columnTop);
                    final long columnDropIndexTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);

                    // create hard link to column data
                    final int errno = ff.hardLink(
                            partitionDFile( // src
                                    path,
                                    rootLen,
                                    tableName,
                                    partitionBy,
                                    partitionTimestamp,
                                    partitionNameTxn,
                                    columnName,
                                    columnVersion
                            ),
                            partitionDFile( // hard link
                                    auxPath,
                                    auxRootLen,
                                    tableName,
                                    partitionBy,
                                    partitionTimestamp,
                                    partitionNameTxn,
                                    columnName,
                                    columnDropIndexTxn
                            )
                    );

                    if (errno < 0) {
                        throw CairoException.instance(errno)
                                .put("Cannot hardLink [src=")
                                .put(path)
                                .put(", hardLink=")
                                .put(auxPath)
                                .put(']');
                    }
                } finally {
                    path.trimTo(rootLen);
                    auxPath.trimTo(auxRootLen);
                }
            }
        } catch (Throwable th) {
            LOG.error().$("Could not DROP INDEX: ").$(th.getMessage()).$();
            tableWriter.rollbackUpdate();
            throw th;
        }
    }

    public void purgeOldColumnIndexVersions() {
        try {
            if (cleanupColumnVersions.size() < 4) {
                return;
            }

            final int columnIndex = (int) cleanupColumnVersions.getQuick(3);

            final TableWriterMetadata writerMetadata = tableWriter.getMetadata();
            final String tableName = tableWriter.getTableName();
            final CharSequence columnName = writerMetadata.getColumnName(columnIndex);
            final long dropIndexTxn = tableWriter.getTxn();

            if (tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn()) {
                LOG.info()
                        .$("there are readers of the index, Please run 'VACUUM TABLE \"")
                        .$(tableName)
                        .$("\"' [columnName=")
                        .$(columnName)
                        .$(", dropIndexTxn=")
                        .$(dropIndexTxn)
                        .I$();
                return;
            }

            // submit async
            final Sequence pubSeq = messageBus.getColumnPurgePubSeq();
            while (true) {
                long cursor = pubSeq.next();
                if (cursor > -1L) {
                    // columnVersion, partitionTimestamp, partitionNameTxn, columnIndex
                    cleanupColumnVersions.getAndSetQuick(3, 0L); // becomes rowIndex
                    ColumnPurgeTask task = messageBus.getColumnPurgeQueue().get(cursor);
                    task.of(
                            tableName,
                            columnName,
                            writerMetadata.getId(),
                            (int) tableWriter.getTruncateVersion(),
                            writerMetadata.getColumnType(columnIndex),
                            tableWriter.getPartitionBy(),
                            dropIndexTxn,
                            cleanupColumnVersions
                    );
                    pubSeq.done(cursor);
                    return;
                } else if (cursor == -1L) {
                    // Queue overflow
                    LOG.error()
                            .$("purge queue is full, Please run 'VACUUM TABLE \"")
                            .$(tableName)
                            .$("\"' [columnName=")
                            .$(columnName)
                            .$(", dropIndexTxn=")
                            .$(dropIndexTxn)
                            .I$();
                    return;
                }
            }
        } finally {
            cleanupColumnVersions.clear();
            path.trimTo(rootLen);
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
        setPathOnPartition(
                path,
                rootLen,
                tableName,
                partitionBy,
                partitionTimestamp,
                partitionNameTxn
        );
        return dFile(path, columnName, columnNameTxn);
    }

    private static LPSZ setPathOnPartition(
            Path path,
            int rootLen,
            CharSequence tableName,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn
    ) {
        path.trimTo(rootLen);
        path.concat(tableName);
        TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
        return path;
    }
}
