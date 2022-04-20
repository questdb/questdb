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
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnVersionPurgeTask;

import java.io.Closeable;
import java.io.IOException;

public class ColumnVersionPurgeExecution implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnVersionPurgeExecution.class);
    private final Path path = new Path();
    private final int pathRootLen;
    private final FilesFacade filesFacade;
    private final TableWriter cleanUpLogWriter;
    private final String updateCompleteColumnName;
    private final TxnScoreboard txnScoreboard;
    private final TxReader txReader;
    private final LongList completedRecordIds = new LongList();
    private final MicrosecondClock microClock;
    private final int updateCompleteColumnWriterIndex;
    private long longBytes;
    private int pathTableLen;
    private long cleanupLogOpenPartitionTimestamp = Long.MAX_VALUE;
    private long cleanupLogOpenPartitionFd = -1L;

    public ColumnVersionPurgeExecution(CairoConfiguration configuration, TableWriter cleanUpLogWriter, String updateCompleteColumnName) {
        this.filesFacade = configuration.getFilesFacade();
        this.cleanUpLogWriter = cleanUpLogWriter;
        this.updateCompleteColumnName = updateCompleteColumnName;
        this.updateCompleteColumnWriterIndex = cleanUpLogWriter.getMetadata().getColumnIndex(updateCompleteColumnName);
        path.of(configuration.getRoot());
        pathRootLen = path.length();
        txnScoreboard = new TxnScoreboard(filesFacade, configuration.getTxnScoreboardEntryCount());
        txReader = new TxReader(filesFacade);
        microClock = configuration.getMicrosecondClock();
        longBytes = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() throws IOException {
        Unsafe.free(longBytes, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        longBytes = 0;
        closeCleanupLogCompleteFile();
        path.close();
        txnScoreboard.close();
    }

    public boolean tryCleanup(ColumnVersionPurgeTask task) {
        try {
            boolean done = tryCleanup0(task);
            setCompleteTimestamp(completedRecordIds, microClock.getTicks());
            return done;
        } catch (CairoException ex) {
            // Can be some IO exception
            LOG.error().$("failed to clean column versions. ").$((Throwable) ex).$();
            return false;
        }
    }

    private boolean checkScoreboardHasReadersBeforeUpdate(long columnVersion, ColumnVersionPurgeTask task) {
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

    private void closeCleanupLogCompleteFile() {
        if (cleanupLogOpenPartitionFd != -1L) {
            filesFacade.close(cleanupLogOpenPartitionFd);
            cleanupLogOpenPartitionFd = -1L;
        }
    }

    private long openCompleteColumToWrite(int partitionIndex) {
        long partitionTimestamp = cleanUpLogWriter.getPartitionTimestamp(partitionIndex);
        if (cleanupLogOpenPartitionTimestamp != partitionTimestamp) {
            path.trimTo(pathRootLen);
            path.concat(cleanUpLogWriter.getTableName());
            long partitionNameTxn = cleanUpLogWriter.getPartitionNameTxn(partitionIndex);
            int partitionBy = cleanUpLogWriter.getPartitionBy();
            TableUtils.setPathForPartition(path, partitionBy, partitionTimestamp, false);
            TableUtils.txnPartitionConditionally(path, partitionNameTxn);
            long completeColumnNameTxn = cleanUpLogWriter.getColumnNameTxn(partitionTimestamp, updateCompleteColumnWriterIndex);
            TableUtils.dFile(path, updateCompleteColumnName, completeColumnNameTxn);
            closeCleanupLogCompleteFile();
            cleanupLogOpenPartitionFd = TableUtils.openRW(filesFacade, path.$(), LOG, cleanUpLogWriter.getConfiguration().getWriterFileOpenOpts());
            cleanupLogOpenPartitionTimestamp = partitionTimestamp;
        }
        return cleanupLogOpenPartitionFd;
    }

    private int readTableId(Path path) {
        final int INVALID_TABLE_ID = Integer.MIN_VALUE;
        long fd = filesFacade.openRO(path.trimTo(pathTableLen).concat(TableUtils.META_FILE_NAME).$());
        if (fd < 0) {
            return INVALID_TABLE_ID;
        }
        try {
            if (filesFacade.read(fd, longBytes, Integer.BYTES, TableUtils.META_OFFSET_TABLE_ID) != Integer.BYTES) {
                return INVALID_TABLE_ID;
            }
            return Unsafe.getUnsafe().getInt(longBytes);
        } finally {
            filesFacade.close(fd);
        }
    }

    private void setCompleteTimestamp(LongList completedRecordIds, long timeMicro) {
        // This is in-place update for known record ids of completed column in column version cleanup log table
        try {
            long fd = -1;
            long fileSize = -1;
            Unsafe.getUnsafe().putLong(longBytes, timeMicro);
            for (int rec = 0, n = completedRecordIds.size(); rec < n; rec++) {
                long recordId = completedRecordIds.getQuick(rec);
                int partitionIndex = Rows.toPartitionIndex(recordId);
                if (rec == 0) {
                    // Assumption is that all records belong to same partition
                    // this is how the records are added to the table in ColumnVersionPurgeJob
                    // e.g. all records about the same column updated have identical timestamp
                    fd = openCompleteColumToWrite(partitionIndex);
                    fileSize = filesFacade.length(fd);
                }
                long rowId = Rows.toLocalRowID(recordId);
                long offset = rowId * Long.BYTES;
                if (offset + Long.BYTES > fileSize) {
                    LOG.error().$("update column version cleanup failed [writeOffset=").$(offset)
                            .$(", fileSize=").$(fileSize)
                            .$(", path=").$(path).I$();
                    return;
                }
                if (filesFacade.write(fd, longBytes, Long.BYTES, rowId * Long.BYTES) != Long.BYTES) {
                    LOG.error().$("update column version cleanup failed [errno=").$(filesFacade.errno())
                            .$(", writeOffset=").$(offset)
                            .$(", fileSize=").$(fileSize)
                            .$(", path=").$(path).I$();
                    return;
                }
            }
        } catch (CairoException ex) {
            LOG.error().$("failed to update column_versions_purge_log table with complete time. ").$((Throwable) ex).$();
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

    private boolean tryCleanup0(ColumnVersionPurgeTask task) {
        LOG.info().$("cleaning up column version [table=").$(task.getTableName())
                .$(", column=").$(task.getColumnName())
                .$(", tableId=").$(task.getTableId())
                .I$();

        setTablePath(task.getTableName());

        LongList columnVersionList = task.getUpdatedColumnVersions();
        boolean tableInitied = false;
        long minUnlockedTxnRangeStarts = Long.MAX_VALUE;
        boolean allDone = true;
        try {
            completedRecordIds.clear();
            for (int i = 0, n = columnVersionList.size(); i < n; i += ColumnVersionPurgeTask.BLOCK_SIZE) {
                long columnVersion = columnVersionList.getQuick(i);
                long partitionTimestamp = columnVersionList.getQuick(i + 1);
                long partitionTxnName = columnVersionList.getQuick(i + 2);

                setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                int pathTripToPartition = path.length();
                TableUtils.dFile(path, task.getColumnName(), columnVersion);
                long updateRecordId = columnVersionList.getQuick(i + 3);

                if (!filesFacade.exists(path.$())) {
                    if (ColumnType.isVariableLength(task.getColumnType())) {
                        path.trimTo(pathTripToPartition);
                        TableUtils.iFile(path, task.getColumnName(), columnVersion);
                        if (!filesFacade.exists(path.$())) {
                            completedRecordIds.add(updateRecordId);
                            continue;
                        }
                    } else {
                        // Files already deleted, move to the next partition
                        completedRecordIds.add(updateRecordId);
                        continue;
                    }
                }

                // Check that there are no readers before updateTxn
                if (!tableInitied) {
                    txnScoreboard.ofRO(path.trimTo(pathTableLen));

                    int tableId = readTableId(path);
                    if (tableId != task.getTableId()) {
                        LOG.info().$("column version will not be deleted, detected table dropped [path=").$(path.trimTo(pathTableLen)).I$();
                        return true;
                    }

                    txReader.ofRO(path.trimTo(pathTableLen), task.getPartitionBy());
                    txReader.unsafeLoadAll();
                    if ((int) txReader.getTruncateVersion() != task.getTruncateVersion()) {
                        LOG.info().$("column version will not be deleted, detected table truncated [path=").$(path.trimTo(pathTableLen)).I$();
                        return true;
                    }

                    // Re-set up path to .d file
                    setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                    TableUtils.dFile(path, task.getColumnName(), columnVersion);

                    tableInitied = true;
                }

                if (columnVersion < minUnlockedTxnRangeStarts) {
                    if (checkScoreboardHasReadersBeforeUpdate(columnVersion, task)) {
                        // Reader lock still exists
                        allDone = false;
                        LOG.debug().$("column version locked in scoreboard, will not be deleted [path=").$(path).I$();
                        continue;
                    } else {
                        minUnlockedTxnRangeStarts = columnVersion;
                    }
                }

                LOG.info().$("column version to be deleted [path=").$(path).I$();

                // No readers looking at the column version, files can be deleted
                if (!filesFacade.remove(path.$()) && filesFacade.exists(path.$())) {
                    LOG.info().$("cannot delete file, will retry [path=").$(path).$(", errno=").$(filesFacade.errno()).$();
                    allDone = false;
                    continue;
                }

                if (ColumnType.isVariableLength(task.getColumnType())) {
                    path.trimTo(pathTripToPartition);
                    TableUtils.iFile(path, task.getColumnName(), columnVersion);

                    if (!filesFacade.remove(path.$()) && filesFacade.exists(path.$())) {
                        LOG.info().$("cannot delete file, will retry [path=").$(path).$(", errno=").$(filesFacade.errno()).$();
                        allDone = false;
                        continue;
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
                completedRecordIds.add(updateRecordId);
            }
        } finally {
            txnScoreboard.clear();
            txReader.clear();
        }

        return allDone;
    }
}
