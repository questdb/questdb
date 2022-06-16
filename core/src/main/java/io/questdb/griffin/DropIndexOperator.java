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
    private final FilesFacade ff;
    private final LongList cleanupColumnIndexVersions = new LongList();
    private LongList cleanupColumnIndexVersionsAsync = new LongList();
    private final MessageBus messageBus;
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

            cleanupColumnIndexVersions.clear();
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
                    if (!ff.exists(srcDFile)) {
                        throw CairoException.instance(0)
                                .put("Impossible! this file should exist: ")
                                .put(path.toString());
                    }
                    // add to cleanup tasks, the index will be removed in due time
                    cleanupColumnIndexVersions.add(columnIndex, columnNameTxn, partitionTimestamp, partitionNameTxn);

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
                    if (ff.hardLink(srcDFile, hardLinkDFile) < 0) {
                        throw CairoException.instance(0)
                                .put("Cannot hard link ")
                                .put(srcDFile)
                                .put(" as ")
                                .put(hardLinkDFile);
                    }
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

    public void purgeOldColumnIndexVersions() {
        try {
            if (cleanupColumnIndexVersions.size() < 4) {
                return;
            }

            if (tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn()) {
                // there are readers of the index
                return;
            }

            final int columnIndex = (int) cleanupColumnIndexVersions.getQuick(0);
            final long columnNameTxn = cleanupColumnIndexVersions.getQuick(1);
            final long partitionTimestamp = cleanupColumnIndexVersions.getQuick(2);
            final long partitionNameTxn = cleanupColumnIndexVersions.getQuick(3);

            final TableWriterMetadata writerMetadata = tableWriter.getMetadata();
            final String tableName = tableWriter.getTableName();
            final CharSequence columnName = writerMetadata.getColumnName(columnIndex);
            final int partitionBy = tableWriter.getPartitionBy();

            LPSZ keysFile = partitionKFile(
                    path,
                    rootLen,
                    tableName,
                    partitionBy,
                    partitionTimestamp,
                    partitionNameTxn,
                    columnName,
                    columnNameTxn
            );
            LPSZ valuesFile = partitionVFile(
                    auxPath,
                    auxRootLen,
                    tableName,
                    partitionBy,
                    partitionTimestamp,
                    partitionNameTxn,
                    columnName,
                    columnNameTxn
            );
            boolean columnIndexPurged = ff.remove(keysFile) &&
                    !ff.exists(keysFile) &&
                    ff.remove(valuesFile) &&
                    !ff.exists(valuesFile);

            if (!columnIndexPurged) {
                // submit async
                final long dropIndexTxn = tableWriter.getTxn();
                cleanupColumnIndexVersionsAsync.clear();
                cleanupColumnIndexVersionsAsync.add(columnNameTxn, partitionTimestamp, partitionNameTxn, 0L);
                final Sequence pubSeq = messageBus.getColumnPurgePubSeq();

                while (true) {
                    long cursor = pubSeq.next();
                    if (cursor > -1L) {
                        ColumnPurgeTask task = messageBus.getColumnPurgeQueue().get(cursor);
                        task.of(
                                tableName,
                                columnName,
                                writerMetadata.getId(),
                                (int) tableWriter.getTruncateVersion(),
                                writerMetadata.getColumnType(columnIndex),
                                partitionBy,
                                dropIndexTxn,
                                cleanupColumnIndexVersionsAsync
                        );
                        pubSeq.done(cursor);
                        return;
                    } else if (cursor == -1L) {
                        // Queue overflow
                        LOG.error()
                                .$("cannot schedule column purge, purge queue is full. Please run 'VACUUM TABLE \"")
                                .$(tableName)
                                .$("\"' [columnName=")
                                .$(columnName)
                                .$(", updateTxn=")
                                .$(dropIndexTxn)
                                .I$();
                        return;
                    }
                }
            }
        } finally {
            path.trimTo(rootLen);
            cleanupColumnIndexVersions.clear();
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

    private static LPSZ partitionKFile(
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
        return BitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
    }

    private static LPSZ partitionVFile(
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
        return BitmapIndexUtils.valueFileName(path, columnName, columnNameTxn);
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
