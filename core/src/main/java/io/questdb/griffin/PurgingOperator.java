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
import io.questdb.cairo.BitmapIndexUtils;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;

public final class PurgingOperator {
    public static final long TABLE_ROOT_PARTITION = Long.MIN_VALUE + 1;
    private final LongList cleanupColumnVersions = new LongList();
    private final ObjList<String> columnNames = new ObjList<>();
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

    public void add(
            int columnIndex,
            String columnName,
            int columnType,
            boolean isIndexed,
            long columnVersion,
            long partitionTimestamp,
            long partitionNameTxn
    ) {
        updateColumnIndexes.add(columnIndex);
        updateColumnIndexes.add(columnType);
        updateColumnIndexes.add(isIndexed ? 1 : 0);
        updateColumnIndexes.add(columnNames.size());
        columnNames.add(columnName);
        cleanupColumnVersions.add(columnIndex, columnVersion, partitionTimestamp, partitionNameTxn);
    }

    public void clear() {
        updateColumnIndexes.clear();
        cleanupColumnVersions.clear();
    }

    public void purge(
            Path path,
            TableToken tableToken,
            int timestampType,
            int partitionBy,
            boolean asyncOnly,
            long truncateVersion,
            long txn
    ) {
        int rootLen = path.size();

        try {
            // Process updated column by column, one at the time
            int cleanupVersionSize = cleanupColumnVersions.size();
            int lastColumnIndex = -1;
            final int intsPerEntry = 4;
            updateColumnIndexes.sortGroups(intsPerEntry);
            for (int updatedCol = 0, nn = updateColumnIndexes.size(); updatedCol < nn; updatedCol += intsPerEntry) {
                int processColumnIndex = updateColumnIndexes.getQuick(updatedCol);
                if (processColumnIndex == lastColumnIndex) {
                    // Skip duplicate column index
                    continue;
                }

                lastColumnIndex = processColumnIndex;
                int columnType = updateColumnIndexes.getQuick(updatedCol + 1);
                boolean isIndexed = updateColumnIndexes.getQuick(updatedCol + 2) == 1;
                int colNameIndex = updateColumnIndexes.getQuick(updatedCol + 3);
                String columnName = columnNames.getQuick(colNameIndex);

                for (int i = 0; i < cleanupVersionSize; i += 4) {
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
                                TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
                                int pathPartitionLen = path.size();
                                TableUtils.dFile(path, columnName, columnVersion);
                                columnPurged = ff.removeQuiet(path.$());

                                if (ColumnType.isVarSize(columnType)) {
                                    TableUtils.iFile(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.removeQuiet(path.$());
                                }

                                if (isIndexed) {
                                    BitmapIndexUtils.valueFileName(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.removeQuiet(path.$());
                                    BitmapIndexUtils.keyFileName(path.trimTo(pathPartitionLen), columnName, columnVersion);
                                    columnPurged &= ff.removeQuiet(path.$());
                                }
                            } else {
                                // This is removal of symbol files from the table root directory
                                TableUtils.charFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged = ff.removeQuiet(path.$());
                                TableUtils.offsetFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.removeQuiet(path.$());
                                BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.removeQuiet(path.$());
                                BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName, columnVersion);
                                columnPurged &= ff.removeQuiet(path.$());
                            }
                        }

                        if (!columnPurged) {
                            // Schedule for async purge
                            cleanupColumnVersions.add(columnVersion, partitionTimestamp, partitionNameTxn, 0);
                        }
                    }
                }

                // if anything not purged, schedule async purge
                if (cleanupColumnVersions.size() > cleanupVersionSize) {
                    purgeColumnVersionAsync(
                            tableToken,
                            columnName,
                            tableToken.getTableId(),
                            (int) truncateVersion,
                            columnType,
                            timestampType,
                            partitionBy,
                            txn,
                            cleanupColumnVersions,
                            cleanupVersionSize,
                            cleanupColumnVersions.size()
                    );
                    cleanupColumnVersions.setPos(cleanupVersionSize);

                    log.info().$("column purge scheduled [table=").$(tableToken)
                            .$(", column=").$safe(columnName)
                            .$(", updateTxn=").$(txn)
                            .I$();
                } else {
                    log.info().$("column purge complete [table=").$(tableToken)
                            .$(", column=").$safe(columnName)
                            .$(", newColumnVersion=").$(txn - 1)
                            .I$();
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void purgeColumnVersionAsync(
            TableToken tableToken,
            String columnName,
            int tableId,
            int tableTruncateVersion,
            int columnType,
            int timestampType,
            int partitionBy,
            long updateTxn,
            @Transient LongList columnVersions,
            int columnVersionsLo,
            int columnVersionsHi
    ) {
        Sequence pubSeq = messageBus.getColumnPurgePubSeq();
        while (true) {
            long cursor = pubSeq.next();
            if (cursor > -1L) {
                ColumnPurgeTask task = messageBus.getColumnPurgeQueue().get(cursor);
                task.of(
                        tableToken,
                        columnName,
                        tableId,
                        tableTruncateVersion,
                        columnType,
                        timestampType,
                        partitionBy,
                        updateTxn,
                        columnVersions,
                        columnVersionsLo,
                        columnVersionsHi
                );
                pubSeq.done(cursor);
                return;
            } else if (cursor == -1L) {
                // Queue overflow
                log.error().$("cannot schedule column purge, purge queue is full. Please run 'VACUUM TABLE \"").$safe(tableToken.getTableName())
                        .$("\"' [columnName=").$safe(columnName)
                        .$(", updateTxn=").$(updateTxn)
                        .I$();
                return;
            }
            Os.pause();
        }
    }

}
