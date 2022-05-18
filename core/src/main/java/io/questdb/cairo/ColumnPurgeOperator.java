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
import io.questdb.tasks.ColumnPurgeTask;

import java.io.Closeable;
import java.io.IOException;

public class ColumnPurgeOperator implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnPurgeOperator.class);
    private final Path path = new Path();
    private final int pathRootLen;
    private final FilesFacade ff;
    private final TableWriter purgeLogWriter;
    private final String updateCompleteColumnName;
    private TxnScoreboard txnScoreboard;
    private TxReader txReader;
    private final LongList completedRecordIds = new LongList();
    private final MicrosecondClock microClock;
    private final int updateCompleteColumnWriterIndex;
    private long longBytes;
    private int pathTableLen;
    private long purgeLogPartitionTimestamp = Long.MAX_VALUE;
    private long purgeLogPartitionFd = -1L;

    public ColumnPurgeOperator(CairoConfiguration configuration, TableWriter purgeLogWriter, String updateCompleteColumnName) {
        this.ff = configuration.getFilesFacade();
        this.purgeLogWriter = purgeLogWriter;
        this.updateCompleteColumnName = updateCompleteColumnName;
        this.updateCompleteColumnWriterIndex = purgeLogWriter.getMetadata().getColumnIndex(updateCompleteColumnName);
        path.of(configuration.getRoot());
        pathRootLen = path.length();
        txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount());
        txReader = new TxReader(ff);
        microClock = configuration.getMicrosecondClock();
        longBytes = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
    }

    public ColumnPurgeOperator(CairoConfiguration configuration) {
        this.ff = configuration.getFilesFacade();
        this.purgeLogWriter = null;
        this.updateCompleteColumnName = null;
        this.updateCompleteColumnWriterIndex = -1;
        path.of(configuration.getRoot());
        pathRootLen = path.length();
        txnScoreboard = null;
        txReader = null;
        microClock = configuration.getMicrosecondClock();
        longBytes = 0;
    }

    @Override
    public void close() throws IOException {
        if (longBytes != 0L) {
            Unsafe.free(longBytes, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            longBytes = 0;
        }
        closeCleanupLogCompleteFile();
        path.close();
        if (txnScoreboard != null) {
            txnScoreboard.close();
        }
    }

    public boolean tryCleanup(ColumnPurgeTask task) {
        try {
            boolean done = tryCleanup0(task, false);
            setCompleteTimestamp(completedRecordIds, microClock.getTicks());
            return done;
        } catch (Throwable ex) {
            // Can be some IO exception
            LOG.error().$("failed to clean column versions. ").$(ex).$();
            return false;
        }
    }

    public boolean tryCleanup(ColumnPurgeTask task, TableReader tableReader) {
        try {
            txReader = tableReader.getTxFile();
            txnScoreboard = tableReader.getTxnScoreboard();
            return tryCleanup0(task, true);
        } catch (Throwable ex) {
            // Can be some IO exception
            LOG.error().$("failed to clean column versions. ").$(ex).$();
            return false;
        }
    }

    private boolean checkScoreboardHasReadersBeforeUpdate(long columnVersion, ColumnPurgeTask task) {
        long updateTxn = task.getUpdatedTxn();
        try {
            return !txnScoreboard.isRangeAvailable(columnVersion + 1, updateTxn);
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall cleanup because of that, re-schedule another run instead
            LOG.error().$("cannot lock last txn in scoreboard, column version purge will be retried [table=")
                    .$(task.getTableName())
                    .$(", txn=").$(updateTxn)
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno()).I$();
            return true;
        }
    }

    private void closeCleanupLogCompleteFile() {
        if (purgeLogPartitionFd != -1L) {
            ff.close(purgeLogPartitionFd);
            purgeLogPartitionFd = -1L;
        }
    }

    private int readTableId(Path path) {
        final int INVALID_TABLE_ID = Integer.MIN_VALUE;
        long fd = ff.openRO(path.trimTo(pathTableLen).concat(TableUtils.META_FILE_NAME).$());
        if (fd < 0) {
            return INVALID_TABLE_ID;
        }
        try {
            if (ff.read(fd, longBytes, Integer.BYTES, TableUtils.META_OFFSET_TABLE_ID) != Integer.BYTES) {
                return INVALID_TABLE_ID;
            }
            return Unsafe.getUnsafe().getInt(longBytes);
        } finally {
            ff.close(fd);
        }
    }

    private void setCompleteTimestamp(LongList completedRecordIds, long timeMicro) {
        // This is in-place update for known record ids of completed column in column version cleanup log table
        try {
            long fileSize = -1;
            Unsafe.getUnsafe().putLong(longBytes, timeMicro);
            for (int rec = 0, n = completedRecordIds.size(); rec < n; rec++) {
                long recordId = completedRecordIds.getQuick(rec);
                int partitionIndex = Rows.toPartitionIndex(recordId);
                if (rec == 0) {
                    // Assumption is that all records belong to same partition
                    // this is how the records are added to the table in ColumnPurgeJob
                    // e.g. all records about the same column updated have identical timestamp
                    final long partitionTimestamp = purgeLogWriter.getPartitionTimestamp(partitionIndex);
                    if (purgeLogPartitionTimestamp != partitionTimestamp) {
                        reopenPurgeLogPartition(partitionIndex, partitionTimestamp);
                    }
                    fileSize = ff.length(purgeLogPartitionFd);
                }
                long rowId = Rows.toLocalRowID(recordId);
                long offset = rowId * Long.BYTES;
                if (offset + Long.BYTES > fileSize) {
                    LOG.error().$("update column version cleanup failed [writeOffset=").$(offset)
                            .$(", fileSize=").$(fileSize)
                            .$(", path=").$(path).I$();
                    return;
                }
                if (ff.write(purgeLogPartitionFd, longBytes, Long.BYTES, rowId * Long.BYTES) != Long.BYTES) {
                    LOG.error().$("update column version cleanup failed [errno=").$(ff.errno())
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

    private void reopenPurgeLogPartition(int partitionIndex, long partitionTimestamp) {
        path.trimTo(pathRootLen);
        path.concat(purgeLogWriter.getTableName());
        long partitionNameTxn = purgeLogWriter.getPartitionNameTxn(partitionIndex);
        TableUtils.setPathForPartition(
                path,
                purgeLogWriter.getPartitionBy(),
                partitionTimestamp,
                false
        );
        TableUtils.txnPartitionConditionally(path, partitionNameTxn);
        TableUtils.dFile(
                path,
                updateCompleteColumnName,
                purgeLogWriter.getColumnNameTxn(partitionTimestamp, updateCompleteColumnWriterIndex)
        );
        closeCleanupLogCompleteFile();
        purgeLogPartitionFd = TableUtils.openRW(ff, path.$(), LOG, purgeLogWriter.getConfiguration().getWriterFileOpenOpts());
        purgeLogPartitionTimestamp = partitionTimestamp;
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

    private boolean tryCleanup0(ColumnPurgeTask task, boolean tableInitialized) {
        LOG.info().$("cleaning up column version [table=").$(task.getTableName())
                .$(", column=").$(task.getColumnName())
                .$(", tableId=").$(task.getTableId())
                .I$();

        setTablePath(task.getTableName());

        final LongList updatedColumnInfo = task.getUpdatedColumnInfo();
        long minUnlockedTxnRangeStarts = Long.MAX_VALUE;
        boolean allDone = true;
        boolean tableCleanupNeeded = !tableInitialized;

        try {
            completedRecordIds.clear();
            for (int i = 0, n = updatedColumnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
                final long columnVersion = updatedColumnInfo.getQuick(i);
                final long partitionTimestamp = updatedColumnInfo.getQuick(i + 1);
                final long partitionTxnName = updatedColumnInfo.getQuick(i + 2);
                final long updateRecordId = updatedColumnInfo.getQuick(i + 3);

                setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                int pathTrimToPartition = path.length();

                TableUtils.dFile(path, task.getColumnName(), columnVersion);

                if (!ff.exists(path)) {
                    if (ColumnType.isVariableLength(task.getColumnType())) {
                        path.trimTo(pathTrimToPartition);
                        TableUtils.iFile(path, task.getColumnName(), columnVersion);
                        if (!ff.exists(path)) {
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
                if (!tableInitialized) {
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

                    tableInitialized = true;
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
                if (couldNotRemove(ff, path)) {
                    allDone = false;
                    continue;
                }

                if (ColumnType.isVariableLength(task.getColumnType())) {
                    path.trimTo(pathTrimToPartition);
                    TableUtils.iFile(path, task.getColumnName(), columnVersion);

                    if (couldNotRemove(ff, path)) {
                        allDone = false;
                        continue;
                    }
                }

                // Check if it's symbol, try remove .k and .v files in the partition
                if (ColumnType.isSymbol(task.getColumnType())) {
                    path.trimTo(pathTrimToPartition);
                    BitmapIndexUtils.keyFileName(path, task.getColumnName(), columnVersion);
                    if (couldNotRemove(ff, path)) {
                        allDone = false;
                        continue;
                    }

                    path.trimTo(pathTrimToPartition);
                    BitmapIndexUtils.valueFileName(path, task.getColumnName(), columnVersion);
                    if (couldNotRemove(ff, path)) {
                        allDone = false;
                        continue;
                    }
                }
                completedRecordIds.add(updateRecordId);
            }
        } finally {
            if (tableCleanupNeeded) {
                txnScoreboard.clear();
                txReader.clear();
            }
        }

        return allDone;
    }

    private static boolean couldNotRemove(FilesFacade ff, Path path) {
        if (ff.remove(path)) {
            return false;
        }

        final int errno = ff.errno();

        if (ff.exists(path)) {
            LOG.info().$("cannot delete file, will retry [path=").$(path).$(", errno=").$(errno).I$();
            return true;
        }

        // file did not exist, we don't care of the error
        return false;
    }
}
