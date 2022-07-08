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
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;

public abstract class PurgingOperator {
    private final Log log;
    protected final FilesFacade ff;
    protected final CairoConfiguration configuration;
    protected final MessageBus messageBus;
    protected final TableWriter tableWriter;
    protected final Path path;
    protected final int rootLen;
    protected final IntList updateColumnIndexes = new IntList();
    protected final LongList cleanupColumnVersions = new LongList();
    private final LongList cleanupColumnVersionsAsync = new LongList();

    protected PurgingOperator(
            Log log,
            CairoConfiguration configuration,
            MessageBus messageBus,
            TableWriter tableWriter,
            Path path,
            int rootLen
    ) {
        this.log = log;
        this.configuration = configuration;
        this.messageBus = messageBus;
        this.ff = configuration.getFilesFacade();
        this.tableWriter = tableWriter;
        this.path = path;
        this.rootLen = rootLen;
    }

    public void purgeOldColumnVersions() {
        boolean anyReadersBeforeCommittedTxn = tableWriter.checkScoreboardHasReadersBeforeLastCommittedTxn();
        TableWriterMetadata writerMetadata = tableWriter.getMetadata();

        int pathTrimToLen = path.length();
        path.concat(tableWriter.getTableName());
        int pathTableLen = path.length();
        long updateTxn = tableWriter.getTxn();

        // Process updated column by column, one at the time
        for (int updatedCol = 0, nn = updateColumnIndexes.size(); updatedCol < nn; updatedCol++) {
            int processColumnIndex = updateColumnIndexes.getQuick(updatedCol);
            CharSequence columnName = writerMetadata.getColumnName(processColumnIndex);
            int columnType = writerMetadata.getColumnType(processColumnIndex);
            cleanupColumnVersionsAsync.clear();

            for (int i = 0, n = cleanupColumnVersions.size(); i < n; i += 4) {
                int columnIndex = (int) cleanupColumnVersions.getQuick(i);
                long columnVersion = cleanupColumnVersions.getQuick(i + 1);
                long partitionTimestamp = cleanupColumnVersions.getQuick(i + 2);
                long partitionNameTxn = cleanupColumnVersions.getQuick(i + 3);

                // Process updated column by column, one at a time
                if (columnIndex == processColumnIndex) {
                    boolean columnPurged = !anyReadersBeforeCommittedTxn;
                    if (!anyReadersBeforeCommittedTxn) {
                        path.trimTo(pathTableLen);
                        TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
                        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
                        int pathPartitionLen = path.length();
                        TableUtils.dFile(path, columnName, columnVersion);
                        if (!ff.remove(path.$())) {
                            columnPurged = false;
                        }
                        if (columnPurged && ColumnType.isVariableLength(columnType)) {
                            path.trimTo(pathPartitionLen);
                            TableUtils.iFile(path, columnName, columnVersion);
                            TableUtils.iFile(path.$(), columnName, columnVersion);
                            if (!ff.remove(path.$()) && ff.exists(path)) {
                                columnPurged = false;
                            }
                        }
                        if (columnPurged && writerMetadata.isColumnIndexed(columnIndex)) {
                            path.trimTo(pathPartitionLen);
                            BitmapIndexUtils.valueFileName(path, columnName, columnVersion);
                            if (!ff.remove(path.$()) && ff.exists(path)) {
                                columnPurged = false;
                            }

                            path.trimTo(pathPartitionLen);
                            BitmapIndexUtils.keyFileName(path, columnName, columnVersion);
                            if (!ff.remove(path.$()) && ff.exists(path)) {
                                columnPurged = false;
                            }
                        }
                    }

                    if (!columnPurged) {
                        cleanupColumnVersionsAsync.add(columnVersion, partitionTimestamp, partitionNameTxn, 0L);
                    }
                }
            }

            // if anything not purged, schedule async purge
            if (cleanupColumnVersionsAsync.size() > 0) {
                purgeColumnVersionAsync(
                        tableWriter.getTableName(),
                        columnName,
                        writerMetadata.getId(),
                        (int) tableWriter.getTruncateVersion(),
                        columnType,
                        tableWriter.getPartitionBy(),
                        updateTxn,
                        cleanupColumnVersionsAsync
                );
                log.info().$("column purge scheduled [table=").$(tableWriter.getTableName())
                        .$(", column=").$(columnName)
                        .$(", updateTxn=").$(updateTxn)
                        .I$();
            } else {
                log.info().$("columns purged locally [table=").$(tableWriter.getTableName())
                        .$(", column=").$(columnName)
                        .$(", newColumnVersion=").$(updateTxn - 1).I$();
            }
        }

        path.trimTo(pathTrimToLen);
    }

    private void purgeColumnVersionAsync(
            String tableName,
            CharSequence columnName,
            int tableId,
            int tableTruncateVersion,
            int columnType,
            int partitionBy,
            long updateTxn,
            LongList columnVersions
    ) {
        Sequence pubSeq = messageBus.getColumnPurgePubSeq();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                ColumnPurgeTask task = messageBus.getColumnPurgeQueue().get(cursor);
                task.of(tableName, columnName, tableId, tableTruncateVersion, columnType, partitionBy, updateTxn, columnVersions);
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                // Queue overflow
                log.error().$("cannot schedule column purge, purge queue is full. Please run 'VACUUM TABLE \"").$(tableName)
                        .$("\"' [columnName=").$(columnName)
                        .$(", updateTxn=").$(updateTxn)
                        .I$();
                return;
            }
        }
    }
}
