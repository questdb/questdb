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

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class ColumnPurgeOperator implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnPurgeOperator.class);
    private final Path path = new Path();
    private final int pathRootLen;
    private final FilesFacade ff;
    private final TableWriter purgeLogWriter;
    private final String updateCompleteColumnName;
    private final LongList completedRowIds = new LongList();
    private final MicrosecondClock microClock;
    private final int updateCompleteColumnWriterIndex;
    private TxnScoreboard txnScoreboard;
    private TxReader txReader;
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
        longBytes = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_COLUMN_PURGE);
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
            Unsafe.free(longBytes, Long.BYTES, MemoryTag.NATIVE_COLUMN_PURGE);
            longBytes = 0;
        }
        closePurgeLogCompleteFile();
        Misc.free(path);
        txnScoreboard = Misc.free(txnScoreboard);
    }

    public boolean purge(ColumnPurgeTask task) {
        try {
            boolean done = purge0(task, ScoreboardUseMode.INTERNAL);
            setCompletionTimestamp(completedRowIds, microClock.getTicks());
            return done;
        } catch (Throwable ex) {
            // Can be some IO exception
            LOG.error().$("could not purge").$(ex).$();
            return false;
        }
    }

    public boolean purge(ColumnPurgeTask task, TableReader tableReader) {
        try {
            txReader = tableReader.getTxFile();
            txnScoreboard = tableReader.getTxnScoreboard();
            return purge0(task, ScoreboardUseMode.EXTERNAL);
        } catch (Throwable ex) {
            // Can be some IO exception
            LOG.error().$("could not purge").$(ex).$();
            return false;
        }
    }

    public void purgeExclusive(ColumnPurgeTask task) {
        try {
            purge0(task, ScoreboardUseMode.EXCLUSIVE);
        } catch (Throwable ex) {
            // Can be some IO exception
            LOG.error().$("could not purge").$(ex).$();
        }
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

    private boolean checkScoreboardHasReadersBeforeUpdate(long columnVersion, ColumnPurgeTask task) {
        long updateTxn = task.getUpdateTxn();
        try {
            return !txnScoreboard.isRangeAvailable(columnVersion + 1, updateTxn);
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall purge because of that, re-schedule another run instead
            LOG.error().$("cannot lock last txn in scoreboard, column purge will re-run [table=")
                    .$(task.getTableName())
                    .$(", txn=").$(updateTxn)
                    .$(", error=").$(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno()).I$();
            return true;
        }
    }

    private void closePurgeLogCompleteFile() {
        if (purgeLogPartitionFd != -1L) {
            ff.close(purgeLogPartitionFd);
            LOG.info().$("closed purge log complete file [fd=").$(purgeLogPartitionFd).I$();
            purgeLogPartitionFd = -1L;
        }
    }

    private boolean openScoreboardAndTxn(ColumnPurgeTask task, ScoreboardUseMode scoreboardUseMode) {
        if (scoreboardUseMode == ScoreboardUseMode.INTERNAL) {
            txnScoreboard.ofRO(path.trimTo(pathTableLen));
        }

        // In exclusive mode we still need to check that purge will delete column in correct table,
        // e.g. table is not truncated after the update happened
        if (scoreboardUseMode == ScoreboardUseMode.INTERNAL || scoreboardUseMode == ScoreboardUseMode.EXCLUSIVE) {
            int tableId = readTableId(path);
            if (tableId != task.getTableId()) {
                LOG.info().$("cannot purge orphan table [path=").$(path.trimTo(pathTableLen)).I$();
                return false;
            }

            path.trimTo(pathTableLen).concat(TXN_FILE_NAME);
            txReader.ofRO(path.$(), task.getPartitionBy());
            txReader.unsafeLoadAll();
            if (txReader.getTruncateVersion() != task.getTruncateVersion()) {
                LOG.info().$("cannot purge, purge request overlaps with truncate [path=").$(path.trimTo(pathTableLen)).I$();
                return false;
            }
        }

        return true;
    }

    private boolean purge0(ColumnPurgeTask task, final ScoreboardUseMode scoreboardMode) {

        LOG.info().$("purging [table=").$(task.getTableName())
                .$(", column=").$(task.getColumnName())
                .$(", tableId=").$(task.getTableId())
                .I$();

        setTablePath(task.getTableName());

        final LongList updatedColumnInfo = task.getUpdatedColumnInfo();
        long minUnlockedTxnRangeStarts = Long.MAX_VALUE;
        boolean allDone = true;
        boolean setupScoreboard = scoreboardMode != ScoreboardUseMode.EXTERNAL;

        try {
            completedRowIds.clear();
            for (int i = 0, n = updatedColumnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
                final long columnVersion = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_COLUMN_VERSION);
                final long partitionTimestamp = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP);
                final long partitionTxnName = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN);
                final long updateRowId = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_UPDATE_ROW_ID);

                setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                int pathTrimToPartition = path.length();

                TableUtils.dFile(path, task.getColumnName(), columnVersion);

                // perform existence check ahead of trying to remove files

                if (!ff.exists(path)) {
                    if (ColumnType.isVariableLength(task.getColumnType())) {
                        path.trimTo(pathTrimToPartition);
                        TableUtils.iFile(path, task.getColumnName(), columnVersion);
                        if (!ff.exists(path)) {
                            completedRowIds.add(updateRowId);
                            continue;
                        }
                    } else {
                        // Files already deleted, move to the next partition
                        completedRowIds.add(updateRowId);
                        continue;
                    }
                }

                if (setupScoreboard) {
                    // Setup scoreboard lazily because columns we're purging
                    // may not exist, including the entire table. Setting up
                    // scoreboard ahead of checking file existence would fail in those
                    // cases.
                    if (!openScoreboardAndTxn(task, scoreboardMode)) {
                        // current table state precludes us from purging its columns
                        // nothing to do here
                        completedRowIds.add(updateRowId);
                        continue;
                    }
                    // we would have mutated the path by checking state of the table
                    // we will have to re-setup that
                    setUpPartitionPath(task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                    TableUtils.dFile(path, task.getColumnName(), columnVersion);
                    setupScoreboard = false;
                }

                if (columnVersion < minUnlockedTxnRangeStarts) {
                    if (scoreboardMode != ScoreboardUseMode.EXCLUSIVE && checkScoreboardHasReadersBeforeUpdate(columnVersion, task)) {
                        // Reader lock still exists
                        allDone = false;
                        LOG.debug().$("cannot purge, version is in use [path=").$(path).I$();
                        continue;
                    } else {
                        minUnlockedTxnRangeStarts = columnVersion;
                    }
                }

                LOG.info().$("purging [path=").$(path).I$();

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
                completedRowIds.add(updateRowId);
            }
        } finally {
            if (scoreboardMode != ScoreboardUseMode.EXTERNAL) {
                Misc.free(txnScoreboard);
                Misc.free(txReader);
            }
        }

        return allDone;
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
        closePurgeLogCompleteFile();
        purgeLogPartitionFd = TableUtils.openRW(ff, path.$(), LOG, purgeLogWriter.getConfiguration().getWriterFileOpenOpts());
        purgeLogPartitionTimestamp = partitionTimestamp;
        LOG.info().$("reopened purge log complete file [path=").$(path).$(", fd=").$(purgeLogPartitionFd).I$();
    }

    private void setCompletionTimestamp(LongList completedRecordIds, long timeMicro) {
        // This is in-place update for known record ids of completed column in column version cleanup log table
        try {
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
                }
                long rowId = Rows.toLocalRowID(recordId);
                long offset = rowId * Long.BYTES;

                if (ff.write(purgeLogPartitionFd, longBytes, Long.BYTES, rowId * Long.BYTES) != Long.BYTES) {
                    int errno = ff.errno();
                    long length = ff.length(purgeLogPartitionFd);
                    LOG.error().$("could not mark record as purged [errno=").$(errno)
                            .$(", writeOffset=").$(offset)
                            .$(", fd=").$(purgeLogPartitionFd)
                            .$(", fileSize=").$(length).I$();
                    // Re-open of the file next run in case something went wrong.
                    purgeLogPartitionTimestamp = -1;
                }
            }
        } catch (CairoException ex) {
            LOG.error().$("could not update completion timestamp").$((Throwable) ex).$();
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

    private enum ScoreboardUseMode {
        INTERNAL,
        EXTERNAL,
        EXCLUSIVE
    }
}
