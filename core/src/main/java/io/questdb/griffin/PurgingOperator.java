/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;

public final class PurgingOperator {
    public static final long TABLE_ROOT_PARTITION = Long.MIN_VALUE + 1;
    private final LongList cleanupColumnVersions = new LongList();
    private final LongList cleanupColumnVersionsAsync = new LongList();
    private final FilesFacade ff;
    private final Log log;
    private final MessageBus messageBus;
    private final IntList updateColumnIndexes = new IntList();

    public PurgingOperator(
            Log log,
            CairoConfiguration configuration,
            MessageBus messageBus
    ) {
        this.log = log;
        this.messageBus = messageBus;
        this.ff = configuration.getFilesFacade();
    }

    public void add(int columnIndex, long columnVersion, long partitionTimestamp, long partitionNameTxn) {
        if (!updateColumnIndexes.contains(columnIndex)) {
            updateColumnIndexes.add(columnIndex);
        }
        cleanupColumnVersions.add(columnIndex, columnVersion, partitionTimestamp, partitionNameTxn);
    }

    public void clear() {
        updateColumnIndexes.clear();
        cleanupColumnVersions.clear();
    }

    public void purge(Path path, TableToken tableToken, int partitionBy, boolean asyncOnly, TableRecordMetadata tableMetadata, long truncateVersion, long txn) {
        int rootLen = path.length();

        try {
            // Process updated column by column, one at the time
            for (int updatedCol = 0, nn = updateColumnIndexes.size(); updatedCol < nn; updatedCol++) {
                int processColumnIndex = updateColumnIndexes.getQuick(updatedCol);
                CharSequence columnName = tableMetadata.getColumnName(processColumnIndex);
                int rawType = tableMetadata.getColumnType(processColumnIndex);
                int columnType = Math.abs(rawType);
                cleanupColumnVersionsAsync.clear();

                for (int i = 0, n = cleanupColumnVersions.size(); i < n; i += 4) {
                    int columnIndex = (int) cleanupColumnVersions.getQuick(i);
                    long columnVersion = cleanupColumnVersions.getQuick(i + 1);
                    long partitionTimestamp = cleanupColumnVersions.getQuick(i + 2);
                    long partitionNameTxn = cleanupColumnVersions.getQuick(i + 3);

                    // Process updated column by column, one at a time
                    if (columnIndex == processColumnIndex) {
                        boolean columnPurged = !asyncOnly;
                        if (!asyncOnly) {
                            if (partitionTimestamp != TABLE_ROOT_PARTITION) {
                                path.trimTo(rootLen);
                                TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, partitionNameTxn);
                                int pathPartitionLen = path.length();
                                TableUtils.dFile(path, columnName, columnVersion);
                                columnPurged = ff.remove(path.$()) || !ff.exists(path);

                                if (ColumnType.isVariableLength(columnType)) {
                                    TableUtils.iFile(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.remove(path.$()) || !ff.exists(path);
                                }

                                if (tableMetadata.isColumnIndexed(columnIndex)) {
                                    BitmapIndexUtils.valueFileName(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.remove(path.$()) || !ff.exists(path);
                                    BitmapIndexUtils.keyFileName(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.remove(path.$()) || !ff.exists(path);
                                }
                            } else {
                                // This is removal of symbol files from the table root directory
                                TableUtils.charFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged = ff.remove(path.$()) || !ff.exists(path);
                                TableUtils.offsetFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.remove(path.$()) || !ff.exists(path);
                                BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.remove(path.$()) || !ff.exists(path);
                                BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.remove(path.$()) || !ff.exists(path);
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
                            tableToken,
                            columnName,
                            tableMetadata.getTableId(),
                            (int) truncateVersion,
                            rawType,
                            partitionBy,
                            txn,
                            cleanupColumnVersionsAsync
                    );
                    log.info().$("column purge scheduled [table=").utf8(tableToken.getTableName())
                            .$(", column=").utf8(columnName)
                            .$(", updateTxn=").$(txn)
                            .I$();
                } else {
                    log.info().$("column complete [table=").utf8(tableToken.getTableName())
                            .$(", column=").utf8(columnName)
                            .$(", newColumnVersion=").$(txn - 1)
                            .I$();
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void purgeColumnVersionAsync(
            TableToken tableName,
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
                log.error().$("cannot schedule column purge, purge queue is full. Please run 'VACUUM TABLE \"").utf8(tableName.getTableName())
                        .$("\"' [columnName=").utf8(columnName)
                        .$(", updateTxn=").$(updateTxn)
                        .I$();
                return;
            }
            Os.pause();
        }
    }

}
