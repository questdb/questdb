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

public class DropIndexOperator {
    private static final Log LOG = LogFactory.getLog(DropIndexOperator.class);
    private final LongList cleanupColumnVersions = new LongList();
    private final LongList rollbackColumnVersions = new LongList();
    private final MessageBus messageBus;
    private final FilesFacade ff;
    private final TableWriter tableWriter;
    private final Path path;
    private final Path other;
    private final int rootLen;

    public DropIndexOperator(FilesFacade ff, MessageBus messageBus, TableWriter tableWriter, Path path, Path other, int rootLen) {
        this.tableWriter = tableWriter;
        this.ff = ff;
        this.messageBus = messageBus;
        this.path = path;
        this.other = other;
        this.rootLen = rootLen;
    }

    public void executeDropIndex(CharSequence columnName, int columnIndex) {
        final int partitionBy = tableWriter.getPartitionBy();
        final int partitionCount = tableWriter.getPartitionCount();
        try {
            cleanupColumnVersions.clear();
            rollbackColumnVersions.clear();
            for (int pIndex = 0; pIndex < partitionCount; pIndex++) {
                final long pTimestamp = tableWriter.getPartitionTimestamp(pIndex);
                final long pVersion = tableWriter.getPartitionNameTxn(pIndex);
                final long columnVersion = tableWriter.getColumnNameTxn(pTimestamp, columnIndex);
                final long columnTop = tableWriter.getColumnTop(pTimestamp, columnIndex, -1L);

                // bump up column version, metadata will be updated later
                tableWriter.upsertColumnVersion(pTimestamp, columnIndex, columnTop);
                final long columnDropIndexVersion = tableWriter.getColumnNameTxn(pTimestamp, columnIndex);

                // create hard link to column data
                // src
                partitionDFile(path, rootLen, partitionBy, pTimestamp, pVersion, columnName, columnVersion);
                // hard link
                partitionDFile(other, rootLen, partitionBy, pTimestamp, pVersion, columnName, columnDropIndexVersion);
                if (-1 == ff.hardLink(path, other)) {
                    throw CairoException.instance(ff.errno())
                            .put("cannot hardLink [src=").put(path)
                            .put(", hardLink=").put(other)
                            .put(']');
                }

                // add to cleanup tasks, the index will be removed in due time
                cleanupColumnVersions.add(columnVersion, pTimestamp, pVersion, columnIndex);
                rollbackColumnVersions.add(columnDropIndexVersion, pTimestamp, pVersion, columnIndex);
            }
        } catch (Throwable th) {
            LOG.error().$("Could not DROP INDEX: ").$(th.getMessage()).$();
            tableWriter.rollbackUpdate();

            // cleanup successful links
            int limit = rollbackColumnVersions.size();
            if (limit / 4 < partitionCount) {
                for (int i = 0; i < limit; i += 4) {
                    final long columnDropIndexVersion = rollbackColumnVersions.getQuick(0);
                    final long pTimestamp = rollbackColumnVersions.getQuick(1);
                    final long partitionNameTxn = rollbackColumnVersions.getQuick(2);
                    partitionDFile(other, rootLen, partitionBy, pTimestamp, partitionNameTxn, columnName, columnDropIndexVersion);
                    if (!ff.remove(other)) {
                        LOG.info().$("Please remove this file \"").$(other).$('"').I$();
                    }
                }
            }
            throw th;
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
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
                        .$("there are readers of the index, Please run 'VACUUM TABLE \"").$(tableName)
                        .$("\"' [columnName=").$(columnName)
                        .$(", dropIndexTxn=").$(dropIndexTxn)
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

                    // TODO: check there are no readers and delete synchronously

                    // Queue overflow
                    LOG.error()
                            .$("purge queue is full, Please run 'VACUUM TABLE \"").$(tableName)
                            .$("\"' [columnName=").$(columnName)
                            .$(", dropIndexTxn=").$(dropIndexTxn)
                            .I$();
                    return;
                }
            }
        } finally {
            cleanupColumnVersions.clear();
        }
    }

    private static LPSZ partitionDFile(
            Path path,
            int rootLen,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn,
            CharSequence columnName,
            long columnNameTxn
    ) {
        setPathOnPartition(
                path,
                rootLen,
                partitionBy,
                partitionTimestamp,
                partitionNameTxn
        );
        return dFile(path, columnName, columnNameTxn);
    }

    private static LPSZ setPathOnPartition(
            Path path,
            int rootLen,
            int partitionBy,
            long partitionTimestamp,
            long partitionNameTxn
    ) {
        path.trimTo(rootLen);
        TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
        return path;
    }
}
