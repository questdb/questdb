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

package io.questdb.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnVersionPurgeTask;

import java.io.Closeable;
import java.io.IOException;

public class ColumnVersionPurgeExecution implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnVersionPurgeExecution.class);
    private final Path path = new Path();
    private final int pathRootLen;
    private final FilesFacade filesFacade;
    private final TxnScoreboard txnScoreboard;
    private int pathTableLen;

    public ColumnVersionPurgeExecution(CairoConfiguration configuration) {
        this.filesFacade = configuration.getFilesFacade();
        path.of(configuration.getRoot());
        pathRootLen = path.length();
        txnScoreboard = new TxnScoreboard(filesFacade, configuration.getTxnScoreboardEntryCount());
    }

    @Override
    public void close() throws IOException {
        path.close();
        txnScoreboard.close();
    }

    public boolean tryCleanup(ColumnVersionPurgeTask task) {
        LOG.info().$("cleaning up column version [table=").$(task.getTableName())
                .$(", column=").$(task.getColumnName())
                .$(", tableId=").$(task.getTableId())
                .I$();

        setTablePath(task.getTableName());

        LongList columnVersionList = task.getUpdatedColumnVersions();
        boolean checkedTxn = false;
        try {
            for (int i = 0, n = columnVersionList.size(); i < n; i += ColumnVersionPurgeTask.BLOCK_SIZE) {
                long columnVersion = columnVersionList.getQuick(i);
                long partitionTimestamp = columnVersionList.getQuick(i + 1);
                long partitionTxnName = columnVersionList.getQuick(i + 2);

                setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                int pathTripToPartition = path.length();

                TableUtils.dFile(path, task.getColumnName(), columnVersion);
                if (!filesFacade.exists(path.$())) {
                    if (ColumnType.isVariableLength(task.getColumnType())) {
                        path.trimTo(pathTripToPartition);
                        TableUtils.iFile(path, task.getColumnName(), columnVersion);
                        if (!filesFacade.exists(path.$())) {
                            continue;
                        }
                    } else {
                        // File already deleted, move to the next partition
                        continue;
                    }
                }

                // Check that there are no readers before updateTxn
                if (!checkedTxn) {
                    checkedTxn = !checkScoreboardHasReadersBeforeUpdate(columnVersion, task);
                    if (!checkedTxn) {
                        return false;
                    }
                    // Re-set up path to .d file
                    setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                    TableUtils.dFile(path, task.getColumnName(), columnVersion);
                }

                // No readers looking at the column version, files can be deleted
                if (!filesFacade.remove(path.$()) && filesFacade.exists(path.$())) {
                    LOG.info().$("cannot delete file, will retry [path=").$(path).$(", errno=").$(filesFacade.errno()).$();
                    return false;
                }

                if (ColumnType.isVariableLength(task.getColumnType())) {
                    path.trimTo(pathTripToPartition);
                    TableUtils.iFile(path, task.getColumnName(), columnVersion);

                    if (!filesFacade.remove(path.$()) && filesFacade.exists(path.$())) {
                        LOG.info().$("cannot delete file, will retry [path=").$(path).$(", errno=").$(filesFacade.errno()).$();
                        return false;
                    }
                }

                // Check if it's symbol, try remove .k and .v files in the partition
                if (ColumnType.isSymbol(task.getColumnType())) {
                    path.trimTo(pathTripToPartition);
                    BitmapIndexUtils.keyFileName(path, task.getColumnName(), columnVersion);
                    filesFacade.remove(path.$());

                    path.trimTo(pathTripToPartition);
                    BitmapIndexUtils.valueFileName(path, task.getColumnName(), columnVersion);
                    filesFacade.remove(path.$());
                }
            }
        } finally {
            txnScoreboard.clear();
        }

        return true;
    }

    private boolean checkScoreboardHasReadersBeforeUpdate(long columnVersion, ColumnVersionPurgeTask task) {
        txnScoreboard.ofRO(path.trimTo(pathTableLen));
        long updateTxn = task.getUpdatedTxn();
        try {
            return !txnScoreboard.isRangeAvailable(columnVersion + 1, updateTxn);
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall writing because of that.
            // Schedule async purge and continue
            LOG.error().$("cannot lock last txn in scoreboard, column version purge will be retried [table=")
                    .$(task.getTableName())
                    .$(", txn=").$(updateTxn)
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno()).I$();
            return true;
        }
    }

    private void setTablePath(String tableName) {
        path.trimTo(pathRootLen).concat(tableName);
        pathTableLen = path.length();
    }

    private void setUpPartitionPath(int partitionBy, long partitionTimestamp, long partitionTxnName) {
        path.trimTo(pathTableLen);
        TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
        TableUtils.txnPartitionConditionally(path, partitionTxnName);
    }
}
