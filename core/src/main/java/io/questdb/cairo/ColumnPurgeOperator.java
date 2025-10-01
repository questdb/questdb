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

package io.questdb.cairo;

import io.questdb.griffin.PurgingOperator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnPurgeTask;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class ColumnPurgeOperator implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnPurgeOperator.class);
    private final LongList completedRowIds = new LongList();
    private final CairoEngine engine;
    private final FilesFacade ff;
    private final MicrosecondClock microClock;
    private final Path path;
    private final int pathRootLen;
    private final TableWriter purgeLogWriter;
    private final ScoreboardUseMode scoreboardUseMode;
    private final String updateCompleteColumnName;
    private final int updateCompleteColumnWriterIndex;
    private long longBytes;
    private int pathTableLen;
    private long purgeLogPartitionFd = -1;
    private long purgeLogPartitionTimestamp = Long.MAX_VALUE;
    private TxReader txReader;
    private TxnScoreboard txnScoreboard;

    public ColumnPurgeOperator(CairoEngine engine, TableWriter purgeLogWriter, String updateCompleteColumnName, ScoreboardUseMode scoreboardUseMode) {
        try {
            this.engine = engine;
            final CairoConfiguration configuration = engine.getConfiguration();
            this.ff = configuration.getFilesFacade();
            this.path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            path.of(configuration.getDbRoot());
            pathRootLen = path.size();
            this.purgeLogWriter = purgeLogWriter;
            this.updateCompleteColumnName = updateCompleteColumnName;
            this.updateCompleteColumnWriterIndex = purgeLogWriter.getMetadata().getColumnIndex(updateCompleteColumnName);
            txReader = new TxReader(ff);
            microClock = configuration.getMicrosecondClock();
            longBytes = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_SQL_COMPILER);
            this.scoreboardUseMode = scoreboardUseMode;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public ColumnPurgeOperator(CairoEngine engine) {
        try {
            this.engine = engine;
            final CairoConfiguration configuration = engine.getConfiguration();
            this.ff = configuration.getFilesFacade();
            this.path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            path.of(configuration.getDbRoot());
            pathRootLen = path.size();
            this.purgeLogWriter = null;
            this.updateCompleteColumnName = null;
            this.updateCompleteColumnWriterIndex = -1;
            txnScoreboard = null;
            txReader = null;
            microClock = configuration.getMicrosecondClock();
            longBytes = 0;
            scoreboardUseMode = ScoreboardUseMode.VACUUM_TABLE;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        if (longBytes != 0L) {
            Unsafe.free(longBytes, Long.BYTES, MemoryTag.NATIVE_SQL_COMPILER);
            longBytes = 0;
        }
        closePurgeLogCompleteFile();
        Misc.free(path);
        txnScoreboard = Misc.free(txnScoreboard);
    }

    public boolean purge(@NotNull ColumnPurgeTask task) {
        assert task.getTableToken() != null;
        assert scoreboardUseMode != ScoreboardUseMode.VACUUM_TABLE;
        boolean done = purge0(task);
        if (done && scoreboardUseMode == ScoreboardUseMode.BAU_QUEUE_PROCESSING) {
            setCompletionTimestamp(completedRowIds, microClock.getTicks());
        }
        return done;
    }

    public boolean purge(@NotNull ColumnPurgeTask task, @NotNull TableReader tableReader) {
        assert task.getTableToken() != null;
        assert scoreboardUseMode == ScoreboardUseMode.VACUUM_TABLE;
        txReader = tableReader.getTxFile();
        txnScoreboard = tableReader.getTxnScoreboard();
        return purge0(task);
    }

    private static boolean couldNotRemove(FilesFacade ff, LPSZ path) {
        if (ff.removeQuiet(path)) {
            return false;
        }

        final int errno = ff.errno();
        if (ff.exists(path)) {
            LOG.info().$("cannot delete file, will retry [path=").$(path).$(", errno=").$(errno).I$();
            return true;
        }

        // the file did not exist, we don't care of the error
        return false;
    }

    private boolean checkScoreboardHasReadersBeforeUpdate(long columnVersion, ColumnPurgeTask task) {
        long updateTxn = task.getUpdateTxn();
        try {
            return !txnScoreboard.isRangeAvailable(columnVersion + 1, updateTxn);
        } catch (CairoException ex) {
            // Scoreboard can be over allocated, don't stall purge because of that, re-schedule another run instead
            LOG.error().$("cannot lock last txn in scoreboard, column purge will re-run [table=")
                    .$(task.getTableToken())
                    .$(", txn=").$(updateTxn)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            return true;
        }
    }

    private void closePurgeLogCompleteFile() {
        if (ff.close(purgeLogPartitionFd)) {
            LOG.info().$("closed purge log complete file [fd=").$(purgeLogPartitionFd).I$();
            purgeLogPartitionFd = -1;
        }
    }

    private boolean openScoreboardAndTxn(ColumnPurgeTask task) {
        switch (scoreboardUseMode) {
            case BAU_QUEUE_PROCESSING:
                txnScoreboard = Misc.free(txnScoreboard);
                txnScoreboard = engine.getTxnScoreboard(task.getTableToken());
                // fall through
            case STARTUP_ONLY:
                TableToken updatedTableToken = engine.getTableTokenIfExists(task.getTableToken().getTableName());
                boolean tableChanged = updatedTableToken == null || updatedTableToken.getTableId() != task.getTableId();
                if (!tableChanged) {
                    int tableId = readTableId(path);
                    tableChanged = tableId != task.getTableId();
                }
                if (tableChanged) {
                    LOG.info().$("cannot purge orphan table [path=").$(path.trimTo(pathTableLen)).I$();
                    return false;
                }

                path.trimTo(pathTableLen).concat(TXN_FILE_NAME);
                txReader.ofRO(path.$(), task.getTimestampType(), task.getPartitionBy());
                txReader.unsafeLoadAll();
                if (txReader.getTruncateVersion() != task.getTruncateVersion()) {
                    LOG.info().$("cannot purge, purge request overlaps with truncate [path=").$(path.trimTo(pathTableLen)).I$();
                    return false;
                }
                return true;
            default:
                return false;
        }
    }

    private boolean purge0(ColumnPurgeTask task) {
        try {
            setTablePath(task.getTableToken());
            final LongList updatedColumnInfo = task.getUpdatedColumnInfo();
            long minUnlockedTxnRangeStarts = Long.MAX_VALUE;
            boolean allDone = true;
            boolean setupScoreboard = scoreboardUseMode != ScoreboardUseMode.VACUUM_TABLE;

            try {
                completedRowIds.clear();
                for (int i = 0, n = updatedColumnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
                    final long columnVersion = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_COLUMN_VERSION);
                    final long partitionTimestamp = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP);
                    final long partitionTxnName = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN);
                    final long updateRowId = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_UPDATE_ROW_ID);
                    int columnTypeRaw = task.getColumnType();
                    int columnType = Math.abs(columnTypeRaw);
                    // We don't know the type of the column, the files are found on the disk, but column
                    // does not exist in the table metadata (e.g., column was dropped)
                    boolean columnTypeRogue = columnTypeRaw == ColumnType.UNDEFINED;
                    boolean isSymbolRootFiles = (ColumnType.isSymbol(columnType) || columnTypeRogue)
                            && partitionTimestamp == PurgingOperator.TABLE_ROOT_PARTITION;

                    int pathTrimToPartition;
                    CharSequence columnName = task.getColumnName();
                    if (!isSymbolRootFiles) {
                        setUpPartitionPath(task.getTimestampType(), task.getPartitionBy(), partitionTimestamp, partitionTxnName);
                        pathTrimToPartition = path.size();
                        TableUtils.dFile(path, columnName, columnVersion);
                    } else {
                        path.trimTo(pathTableLen);
                        pathTrimToPartition = path.size();
                        TableUtils.charFileName(path, columnName, columnVersion);
                    }

                    // perform existence check ahead of trying to remove files
                    if (!ff.exists(path.$()) && !columnTypeRogue) {
                        if (ColumnType.isVarSize(columnType)) {
                            path.trimTo(pathTrimToPartition);
                            if (!ff.exists(TableUtils.iFile(path, columnName, columnVersion))) {
                                completedRowIds.add(updateRowId);
                                continue;
                            }
                        } else if (ColumnType.isSymbol(columnType)) {
                            // In the case of symbol root files, we need to check if .k and .v files exist in table root.
                            // In the case of symbol files in partition, we need to check if .k and .v files exist in partition
                            // that can be index files after index drop SQL.
                            if (!ff.exists(TableUtils.offsetFileName(path.trimTo(pathTrimToPartition), columnName, columnVersion))) {
                                if (!ff.exists(BitmapIndexUtils.keyFileName(path.trimTo(pathTrimToPartition), columnName, columnVersion))) {
                                    if (!ff.exists(BitmapIndexUtils.valueFileName(path.trimTo(pathTrimToPartition), columnName, columnVersion))) {
                                        completedRowIds.add(updateRowId);
                                        continue;
                                    }
                                }
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
                        if (!openScoreboardAndTxn(task)) {
                            // the current table state precludes us from purging its columns
                            // nothing to do here
                            completedRowIds.add(updateRowId);
                            continue;
                        }
                        // we would have mutated the path by checking the state of the table
                        // we will have to re-set up that
                        if (!isSymbolRootFiles) {
                            setUpPartitionPath(
                                    task.getTimestampType(),
                                    task.getPartitionBy(),
                                    partitionTimestamp,
                                    partitionTxnName
                            );
                        } else {
                            path.trimTo(pathTableLen);
                        }
                        pathTrimToPartition = path.size();
                        TableUtils.dFile(path, columnName, columnVersion);
                        setupScoreboard = false;
                    }

                    if (txReader.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp)) {
                        // txReader is either open because scoreboardMode == ScoreboardUseMode.EXTERNAL,
                        // or it was open by openScoreboardAndTxn
                        LOG.info().$("skipping purge of read-only partition [path=").$(path.$())
                                .$(", column=").$safe(columnName)
                                .I$();
                        completedRowIds.add(updateRowId);
                        continue;
                    }

                    if (columnVersion < minUnlockedTxnRangeStarts) {
                        if (scoreboardUseMode != ScoreboardUseMode.STARTUP_ONLY && checkScoreboardHasReadersBeforeUpdate(columnVersion, task)) {
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
                    if (couldNotRemove(ff, path.$())) {
                        allDone = false;
                        continue;
                    }

                    if (ColumnType.isVarSize(columnType) || columnTypeRogue) {
                        path.trimTo(pathTrimToPartition);
                        TableUtils.iFile(path, columnName, columnVersion);

                        if (couldNotRemove(ff, path.$())) {
                            allDone = false;
                            continue;
                        }
                    }

                    // Check if it's a symbol, try to remove .k and .v files in the partition
                    if (ColumnType.isSymbol(columnType) || columnTypeRogue) {
                        if (isSymbolRootFiles) {
                            path.trimTo(pathTrimToPartition);
                            if (couldNotRemove(ff, TableUtils.charFileName(path, columnName, columnVersion))) {
                                allDone = false;
                                continue;
                            }

                            path.trimTo(pathTrimToPartition);
                            if (couldNotRemove(ff, TableUtils.offsetFileName(path, columnName, columnVersion))) {
                                allDone = false;
                                continue;
                            }
                        }

                        path.trimTo(pathTrimToPartition);
                        if (couldNotRemove(ff, BitmapIndexUtils.keyFileName(path, columnName, columnVersion))) {
                            allDone = false;
                            continue;
                        }

                        path.trimTo(pathTrimToPartition);
                        if (couldNotRemove(ff, BitmapIndexUtils.valueFileName(path, columnName, columnVersion))) {
                            allDone = false;
                            continue;
                        }
                    }
                    completedRowIds.add(updateRowId);
                }
            } finally {
                if (scoreboardUseMode != ScoreboardUseMode.VACUUM_TABLE) {
                    txnScoreboard = Misc.free(txnScoreboard);
                    // txReader is a reusable object, do not NULL it
                    Misc.free(txReader);
                } else {
                    // even though we take these things from the reader, we must not re-use them on the next run
                    txnScoreboard = null;
                    txReader = null;
                }
            }

            return allDone;
        } catch (Throwable e) {
            // Can be some IO exception
            LOG.error().$("could not purge [ex=`").$(e).$("`]").$();
            return false;
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

    private void reopenPurgeLogPartition(int partitionIndex, long partitionTimestamp) {
        path.trimTo(pathRootLen);
        path.concat(purgeLogWriter.getTableToken());
        long partitionNameTxn = purgeLogWriter.getPartitionNameTxn(partitionIndex);
        TableUtils.setPathForNativePartition(
                path,
                purgeLogWriter.getMetadata().getTimestampType(),
                purgeLogWriter.getPartitionBy(),
                partitionTimestamp,
                partitionNameTxn
        );
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
        // This is an in-place update for known record ids of completed column in column version cleanup log table
        try {
            Unsafe.getUnsafe().putLong(longBytes, timeMicro);
            for (int rec = 0, n = completedRecordIds.size(); rec < n; rec++) {
                long recordId = completedRecordIds.getQuick(rec);
                int partitionIndex = Rows.toPartitionIndex(recordId);
                if (rec == 0) {
                    // The assumption is that all records belong to the same partition
                    // this is how the records are added to the table in ColumnPurgeJob,
                    // e.g., all records about the same column updated have identical timestamp
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
                            .$(", fileSize=").$(length)
                            .I$();
                    // Re-open of the file next run in case something went wrong.
                    purgeLogPartitionTimestamp = -1;
                }
            }
        } catch (CairoException ex) {
            LOG.error().$("could not update completion timestamp").$((Throwable) ex).$();
        }
    }

    private void setTablePath(TableToken tableName) {
        path.trimTo(pathRootLen).concat(tableName);
        pathTableLen = path.size();
    }

    private void setUpPartitionPath(int timestampType, int partitionBy, long partitionTimestamp, long partitionTxnName) {
        path.trimTo(pathTableLen);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionTxnName);
    }

    public enum ScoreboardUseMode {
        BAU_QUEUE_PROCESSING,
        VACUUM_TABLE,
        STARTUP_ONLY
    }
}
