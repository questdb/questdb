/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.PurgingOperator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import io.questdb.std.WeakMutableObjectPool;
import io.questdb.std.datetime.Clock;
import io.questdb.tasks.ColumnPurgeTask;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.PriorityQueue;

public class ColumnPurgeJob extends SynchronizedJob implements Closeable {
    private static final int COLUMN_NAME_COLUMN = 2;
    private static final int COLUMN_TYPE_COLUMN = 5;
    private static final int COLUMN_VERSION_COLUMN = 8;
    private static final Log LOG = LogFactory.getLog(ColumnPurgeJob.class);
    private static final int MAX_ERRORS = 11;
    private static final int PARTITION_BY_COLUMN = 6;
    private static final int PARTITION_NAME_COLUMN = 10;
    private static final int PARTITION_TIMESTAMP_COLUMN = 9;
    // The partition timestamp exactly as _txn has it, in the unit of the table timestamp. partition_timestamp holds it
    // in micros, which drops the sub-microsecond part of a TIMESTAMP_NS split partition. A log that a version before
    // this column created gains it when the job starts, and the rows that version wrote read NULL in it.
    private static final String PARTITION_TIMESTAMP_RAW_COLUMN_NAME = "partition_timestamp_raw";
    private static final int TABLE_ID_COLUMN = 3;
    private static final int TABLE_NAME_COLUMN = 1;
    private static final int TABLE_TRUNCATE_VERSION = 4;
    private static final int UPDATE_TXN_COLUMN = 7;
    private final Clock clock;
    private final RingQueue<ColumnPurgeTask> inQueue;
    private final Sequence inSubSequence;
    // the entries of a task that resolveMicrosPartitionTimestamps() located, reused across the tasks of the startup pass
    private final LongList resolvedColumnInfo = new LongList();
    private final long retryDelay;
    private final long retryDelayLimit;
    private final double retryDelayMultiplier;
    private final PriorityQueue<ColumnPurgeRetryTask> retryQueue;
    private final TableToken tableToken;
    private ColumnPurgeOperator columnPurgeOperator;
    private int inErrorCount;
    // the writer index of partition_timestamp_raw, or -1 when the log lacks the column
    private int partitionTimestampRawWriterIndex = -1;
    private SqlExecutionContextImpl sqlExecutionContext;
    private WeakMutableObjectPool<ColumnPurgeRetryTask> taskPool;
    private TableWriter writer;

    public ColumnPurgeJob(CairoEngine engine) throws SqlException {
        try {
            final CairoConfiguration configuration = engine.getConfiguration();
            this.clock = configuration.getMicrosecondClock();
            this.inQueue = engine.getMessageBus().getColumnPurgeQueue();
            this.inSubSequence = engine.getMessageBus().getColumnPurgeSubSeq();
            String tableName = configuration.getSystemTableNamePrefix() + "column_versions_purge_log";
            this.taskPool = new WeakMutableObjectPool<>(ColumnPurgeRetryTask::new, configuration.getColumnPurgeTaskPoolCapacity());
            this.retryQueue = new PriorityQueue<>(configuration.getColumnPurgeQueueCapacity(), ColumnPurgeJob::compareRetryTasks);
            this.retryDelayLimit = configuration.getColumnPurgeRetryDelayLimit();
            this.retryDelay = configuration.getColumnPurgeRetryDelay();
            this.retryDelayMultiplier = configuration.getColumnPurgeRetryDelayMultiplier();
            this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            this.sqlExecutionContext.with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                this.tableToken = sqlCompiler.query()
                        .$("CREATE TABLE IF NOT EXISTS \"")
                        .$(tableName)
                        .$("\" (" +
                                "ts timestamp, " + // 0
                                "table_name symbol, " + // 1
                                "column_name symbol, " + // 2
                                "table_id int, " + // 3
                                "truncate_version long, " + // 4
                                "columnType int, " + // 5
                                "table_partition_by int, " + // 6
                                "updated_txn long, " + // 7
                                "column_version long, " + // 8
                                "partition_timestamp timestamp, " + // 9
                                "partition_name_txn long," + // 10
                                "completed timestamp, " + // 11
                                PARTITION_TIMESTAMP_RAW_COLUMN_NAME + " long" + // 12
                                ") timestamp(ts) partition by MONTH BYPASS WAL"
                        )
                        .createTable(sqlExecutionContext);
            }

            this.writer = engine.getWriter(tableToken, TableUtils.SYSTEM_WRITER_LOCK_REASON);
            addPartitionTimestampRawColumn(engine);
            this.partitionTimestampRawWriterIndex = getPartitionTimestampRawIndex(writer.getMetadata());
            this.columnPurgeOperator = new ColumnPurgeOperator(
                    engine,
                    this.writer,
                    "completed",
                    ColumnPurgeOperator.ScoreboardUseMode.BAU_QUEUE_PROCESSING
            );
            processTableRecords(engine);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        this.writer = Misc.free(writer);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.columnPurgeOperator = Misc.free(columnPurgeOperator);
        this.taskPool = Misc.free(taskPool);
    }

    @TestOnly
    public String getLogTableName() {
        return tableToken.getTableName();
    }

    @TestOnly
    public int getOutstandingPurgeTasks() {
        return retryQueue.size();
    }

    private static int compareRetryTasks(ColumnPurgeRetryTask task1, ColumnPurgeRetryTask task2) {
        return Long.compare(task1.nextRunTimestamp, task2.nextRunTimestamp);
    }

    // The index of partition_timestamp_raw in the metadata of the log or of a query on it, or -1 when the log lacks
    // the column, or has a column of that name that the job did not create.
    private static int getPartitionTimestampRawIndex(RecordMetadata metadata) {
        final int index = metadata.getColumnIndexQuiet(PARTITION_TIMESTAMP_RAW_COLUMN_NAME);
        if (index > -1 && metadata.getColumnType(index) != ColumnType.LONG) {
            LOG.error().$("ignoring column of unexpected type in column purge log [column=").$(PARTITION_TIMESTAMP_RAW_COLUMN_NAME)
                    .$(", type=").$(ColumnType.nameOf(metadata.getColumnType(index)))
                    .I$();
            return -1;
        }
        return index;
    }

    // Whether a log row holds the partition timestamp of a TIMESTAMP_NS partition in micros only. Such a row lacks
    // partition_timestamp_raw, and toMicros() dropped the sub-microsecond part of a split partition, so the row
    // does not name its partition: see resolveMicrosPartitionTimestamps(). A TIMESTAMP partition, the table root
    // and the single partition of a non-partitioned table need no resolution.
    private static boolean isMicrosOnlyPartitionTimestamp(Record rec, int partitionTimestampRawIndex, ColumnPurgeRetryTask task, long partitionTimestamp) {
        return (partitionTimestampRawIndex < 0 || rec.getLong(partitionTimestampRawIndex) == Numbers.LONG_NULL)
                && ColumnType.isTimestampNano(task.getTimestampType())
                && PartitionBy.isPartitioned(task.getPartitionBy())
                && partitionTimestamp != PurgingOperator.TABLE_ROOT_PARTITION;
    }

    // The partition timestamp of a log row. A row without partition_timestamp_raw, which a version before the column
    // wrote, has it in partition_timestamp only: toMicros() dropped the sub-microsecond part of a TIMESTAMP_NS split
    // partition, which no one can restore, and moved the root partition marker of a TIMESTAMP_NS table.
    private static long readPartitionTimestamp(Record rec, int partitionTimestampRawIndex, TimestampDriver driver) {
        if (partitionTimestampRawIndex > -1) {
            final long partitionTimestamp = rec.getLong(partitionTimestampRawIndex);
            if (partitionTimestamp != Numbers.LONG_NULL) {
                return partitionTimestamp;
            }
        }
        final long partitionTimestampMicros = rec.getLong(PARTITION_TIMESTAMP_COLUMN);
        if (partitionTimestampMicros == driver.toMicros(PurgingOperator.TABLE_ROOT_PARTITION)) {
            return PurgingOperator.TABLE_ROOT_PARTITION;
        }
        return driver.fromMicros(partitionTimestampMicros);
    }

    // Adds partition_timestamp_raw to a log that a version before the column created. When the column cannot be
    // added, the job logs as that version did, and the next start tries again.
    private void addPartitionTimestampRawColumn(CairoEngine engine) {
        if (writer.getMetadata().getColumnIndexQuiet(PARTITION_TIMESTAMP_RAW_COLUMN_NAME) > -1) {
            return;
        }
        try {
            writer.addColumn(PARTITION_TIMESTAMP_RAW_COLUMN_NAME, ColumnType.LONG, null);
        } catch (CairoException | CairoError e) {
            LOG.error().$("could not add column to column purge log [table=").$(tableToken)
                    .$(", column=").$(PARTITION_TIMESTAMP_RAW_COLUMN_NAME)
                    .$(", error=").$((Throwable) e)
                    .I$();
        }
        if (writer.isDistressed()) {
            // TableWriter asks for a new instance after a failed column add, and its open repairs the metadata
            writer = Misc.free(writer);
            writer = engine.getWriter(tableToken, TableUtils.SYSTEM_WRITER_LOCK_REASON);
        }
    }

    private void calculateNextTimestamp(ColumnPurgeRetryTask task, long currentTime) {
        task.retryDelay = Math.min(retryDelayLimit, (long) (task.retryDelay * retryDelayMultiplier));
        task.nextRunTimestamp = currentTime + task.retryDelay;
    }

    private void commit() {
        try {
            if (writer != null) {
                writer.commit();
            }
        } catch (Throwable th) {
            LOG.error().$("error saving to column version house keeping log, cannot commit")
                    .$(", releasing writer and stop updating log [table=").$(tableToken)
                    .$(", error=").$(th)
                    .I$();
            writer = Misc.free(writer);
        }
    }

    private String internStrObj(CharSequenceObjHashMap<String> stringIntern, CharSequence sym) {
        String val = stringIntern.get(sym);
        if (val != null) {
            return val;
        }
        val = Chars.toString(sym);
        stringIntern.put(val, val);
        return val;
    }

    // Keeps the entries of the table root, whose marker the log keeps whole, see readPartitionTimestamp().
    private void keepTableRootEntries(LongList columnInfo) {
        for (int i = 0, n = columnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
            if (columnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP) == PurgingOperator.TABLE_ROOT_PARTITION) {
                resolvedColumnInfo.add(columnInfo, i, i + ColumnPurgeTask.BLOCK_SIZE);
            }
        }
    }

    // Process incoming queue and put it on priority queue with next timestamp to rerun
    private boolean processInQueue() {
        boolean useful = false;
        long microTime = clock.getTicks();
        while (true) {
            long cursor = inSubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                Os.pause();
                continue;
            }
            // -1 = queue is empty, all done
            if (cursor < 0) {
                break;
            }

            ColumnPurgeTask queueTask = inQueue.get(cursor);
            ColumnPurgeRetryTask purgeTaskRun = taskPool.pop();
            purgeTaskRun.copyFrom(queueTask, retryDelay, microTime + retryDelay);
            purgeTaskRun.timestamp = microTime++;
            inSubSequence.done(cursor);

            saveToStorage(purgeTaskRun);

            retryQueue.add(purgeTaskRun);
            useful = true;
        }

        if (useful) {
            commit();
        }
        return useful;
    }

    private void processTableRecords(CairoEngine engine) {
        RecordCursorFactory recordCursorFactory;
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            recordCursorFactory = sqlCompiler.query()
                    .$("SELECT * FROM \"")
                    .$(tableToken.getTableName())
                    .$("\" WHERE completed = null")
                    .compile(sqlExecutionContext).getRecordCursorFactory();
        } catch (Throwable e) {
            LOG.error().$("failed to reload column version purge tasks").$(e).$();
            return;
        }

        final int partitionTimestampRawIndex = getPartitionTimestampRawIndex(recordCursorFactory.getMetadata());
        long microTime = clock.getTicks();
        // Tasks a save point still holds. The log truncate below would forget them, so they are written back to
        // the log and retried by the running job.
        final ObjList<ColumnPurgeRetryTask> heldTasks = new ObjList<>();
        try {
            assert recordCursorFactory.supportsUpdateRowId(tableToken);
            int count = 0;

            try (
                    RecordCursor records = recordCursorFactory.getCursor(sqlExecutionContext);
                    // this is a startup-only activity where we purge columns from the log table
                    // to ensure purge operator does not have to deal with the complexity of switching operating
                    // modes dynamically, we create a new instance specifically for startup.
                    ColumnPurgeOperator columnPurgeOperator = new ColumnPurgeOperator(
                            engine,
                            this.writer,
                            "completed",
                            ColumnPurgeOperator.ScoreboardUseMode.STARTUP_ONLY
                    )
            ) {
                Record rec = records.getRecord();
                long lastTs = Long.MIN_VALUE;
                ColumnPurgeRetryTask task = null;

                CharSequenceObjHashMap<String> stringIntern = new CharSequenceObjHashMap<>();

                boolean taskInitialized = false;
                while (records.hasNext()) {
                    count++;
                    long ts = rec.getTimestamp(0);
                    if (ts != lastTs || task == null) {
                        if (task != null) {
                            if (taskInitialized) {
                                if (purgeOrHold(engine, columnPurgeOperator, task, heldTasks)) {
                                    task = taskPool.pop();
                                }
                                taskInitialized = false;
                            }
                        } else {
                            task = taskPool.pop();
                        }

                        lastTs = ts;
                        String tableName = internStrObj(stringIntern, rec.getSymA(TABLE_NAME_COLUMN));
                        String columnName = internStrObj(stringIntern, rec.getSymA(COLUMN_NAME_COLUMN));
                        int tableId = rec.getInt(TABLE_ID_COLUMN);
                        long truncateVersion = rec.getLong(TABLE_TRUNCATE_VERSION);
                        int columnType = rec.getInt(COLUMN_TYPE_COLUMN);
                        int partitionBy = rec.getInt(PARTITION_BY_COLUMN);
                        long updateTxn = rec.getLong(UPDATE_TXN_COLUMN);
                        TableToken token = engine.getTableTokenByDirName(tableName);

                        if (token == null || token.getTableId() != tableId) {
                            LOG.debug().$("table deleted, skipping [tableDir=").$safe(tableName).I$();
                            lastTs = Long.MIN_VALUE; // reset timestamp to force task initialization on the next iteration
                            continue;
                        }
                        int timestampType;
                        // Read metadata from the user table being purged, not from the
                        // purge-log table (this.tableToken), whose timestamp type may differ.
                        try (TableMetadata metadata = engine.getTableMetadata(token)) {
                            timestampType = metadata.getTimestampType();
                        }
                        // The purge log does not keep the index type of the purged column instance,
                        // and the live column, if any, may have another one: DROP INDEX, DROP COLUMN,
                        // RENAME COLUMN, ALTER COLUMN TYPE and an index change all queue purges.
                        // The POSTING removal takes the files of every index type, the POSTING
                        // files and then the BITMAP .k and .v files, and every file it takes is
                        // named with the column name txn of the purged instance.
                        final byte indexType = IndexType.POSTING;

                        taskInitialized = true;
                        task.of(
                                token,
                                columnName,
                                tableId,
                                truncateVersion,
                                columnType,
                                indexType,
                                timestampType,
                                partitionBy,
                                updateTxn,
                                retryDelay,
                                microTime
                        );
                        task.timestamp = ts;
                    }
                    long columnVersion = rec.getLong(COLUMN_VERSION_COLUMN);
                    long partitionTs = readPartitionTimestamp(rec, partitionTimestampRawIndex, task.getTimestampTypeDriver());
                    long partitionNameTxn = rec.getLong(PARTITION_NAME_COLUMN);
                    if (isMicrosOnlyPartitionTimestamp(rec, partitionTimestampRawIndex, task, partitionTs)) {
                        if (task.getColumnType() == ColumnType.NULL) {
                            // A retired _geometry generation. Its purge leaves the file alone when a commit at or
                            // after the update txn created it, and resolveMicrosPartitionTimestamps() raises that
                            // txn; a sibling's live generation can also have the same number. The file stays.
                            LOG.info().$("leaving retired geometry generation that the purge log names in microseconds [table=")
                                    .$(task.getTableToken())
                                    .$(", generation=").$(columnVersion)
                                    .$(", partitionTimestampMicros=").$(rec.getLong(PARTITION_TIMESTAMP_COLUMN))
                                    .$(", partitionNameTxn=").$(partitionNameTxn)
                                    .I$();
                            continue;
                        }
                        task.hasMicrosPartitionTimestamps = true;
                    }
                    task.appendColumnInfo(columnVersion, partitionTs, partitionNameTxn, rec.getUpdateRowId());
                }
                if (task != null) {
                    if (!taskInitialized || !purgeOrHold(engine, columnPurgeOperator, task, heldTasks)) {
                        taskPool.push(task);
                    }
                }
            }

            if (count > 0) {
                LOG.info().$("cleaned up rewritten column files [cleanCount=").$(count).I$();
            }

            if (writer != null) {
                boolean truncated = false;
                try {
                    writer.truncate();
                    truncated = true;
                } catch (Throwable th) {
                    LOG.error().$("failed to truncate column version purge log table").$(th).$();
                }
                if (truncated) {
                    retryHeldTasks(heldTasks);
                }
            }
        } catch (SqlException e) {
            LOG.error().$("failed to reload column version purge tasks").$((Throwable) e).$();
        } finally {
            Misc.free(recordCursorFactory);
            for (int i = 0, n = heldTasks.size(); i < n; i++) {
                taskPool.push(heldTasks.getQuick(i));
            }
        }
    }

    private boolean purge() {
        boolean useful = false;
        final long now = clock.getTicks() + 1;
        while (!retryQueue.isEmpty()) {
            ColumnPurgeRetryTask nextTask = retryQueue.peek();
            if (nextTask.nextRunTimestamp < now) {
                retryQueue.poll();
                useful = true;
                if (!columnPurgeOperator.purge(nextTask)) {
                    // Re-queue
                    calculateNextTimestamp(nextTask, now);
                    retryQueue.add(nextTask);
                } else {
                    taskPool.push(nextTask);
                }
            } else {
                // All reruns are in the future.
                return useful;
            }
        }
        return useful;
    }

    // Purges a task the startup pass read. When a save point holds the task, the task joins heldTasks, which owns it
    // from then on, and the method returns true. A task that fails for any other reason is not kept, so the log
    // truncate drops it. Both the tasks inside the read loop and the last task go through here.
    private boolean purgeOrHold(
            CairoEngine engine,
            ColumnPurgeOperator columnPurgeOperator,
            ColumnPurgeRetryTask task,
            ObjList<ColumnPurgeRetryTask> heldTasks
    ) {
        if (task.hasMicrosPartitionTimestamps) {
            resolveMicrosPartitionTimestamps(engine, task);
        }
        if (!columnPurgeOperator.purge(task) && columnPurgeOperator.isLastPurgeHeldByEpoch()) {
            heldTasks.add(task);
            return true;
        }
        return false;
    }

    // Locates the partitions of the task entries that the log keeps in micros only, see
    // isMicrosOnlyPartitionTimestamp(). Such an entry fits every partition that starts in its microsecond. The
    // partition it was written for may be gone - rewritten under another name txn, or squashed into a sibling - and
    // a sibling may still read the version. So the entry goes to each partition of the microsecond, in a snapshot of
    // the table, where the version is dead by the test VACUUM TABLE makes, and the task waits for the readers and save
    // points before the snapshot txn, as a VACUUM TABLE task does: the version may have died in a sibling after the
    // update that logged the entry. A version dead in the snapshot stays dead, so a save point can hold the task for
    // the running job. An entry that fits no partition leaves the task: its files, if any, are in a partition version
    // that the partition purge removes. When the snapshot fails, the entries leave the task, and VACUUM TABLE finds
    // their files. The table root entries stay as logged.
    private void resolveMicrosPartitionTimestamps(CairoEngine engine, ColumnPurgeRetryTask task) {
        task.hasMicrosPartitionTimestamps = false;
        final LongList columnInfo = task.getUpdatedColumnInfo();
        resolvedColumnInfo.clear();
        try (TableReader reader = engine.getReader(task.getTableToken())) {
            final long snapshotTxn = reader.getTxn();
            resolveMicrosPartitionTimestamps(reader, task, columnInfo, snapshotTxn);
            task.setUpdateTxn(Math.max(task.getUpdateTxn(), snapshotTxn));
        } catch (Throwable th) {
            LOG.error().$("could not read table to locate purge log entries kept in microseconds, leaving their files to VACUUM TABLE [table=")
                    .$(task.getTableToken())
                    .$(", column=").$safe(task.getColumnName())
                    .$(", error=").$(th)
                    .I$();
            resolvedColumnInfo.clear();
            keepTableRootEntries(columnInfo);
        }
        LOG.info().$("located partitions of purge log entries kept in microseconds [table=").$(task.getTableToken())
                .$(", column=").$safe(task.getColumnName())
                .$(", logged=").$(columnInfo.size() / ColumnPurgeTask.BLOCK_SIZE)
                .$(", located=").$(resolvedColumnInfo.size() / ColumnPurgeTask.BLOCK_SIZE)
                .$(", updateTxn=").$(task.getUpdateTxn())
                .I$();
        columnInfo.clear();
        columnInfo.add(resolvedColumnInfo);
    }

    private void resolveMicrosPartitionTimestamps(TableReader reader, ColumnPurgeRetryTask task, LongList columnInfo, long snapshotTxn) {
        final TimestampDriver driver = task.getTimestampTypeDriver();
        final TxReader txFile = reader.getTxFile();
        final TableReaderMetadata metadata = reader.getMetadata();
        final ColumnVersionReader columnVersionReader = reader.getColumnVersionReader();
        final int columnIndex = metadata.getColumnIndexQuiet(task.getColumnName());
        final int writerIndex = columnIndex > -1 ? metadata.getWriterIndex(columnIndex) : -1;
        final boolean isPostingColumn = columnIndex > -1 && IndexType.isPosting(metadata.getColumnIndexType(columnIndex));
        final boolean isSymbolColumn = columnIndex > -1 && ColumnType.isSymbol(metadata.getColumnType(columnIndex));
        for (int i = 0, n = columnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
            final long columnVersion = columnInfo.getQuick(i + ColumnPurgeTask.OFFSET_COLUMN_VERSION);
            final long loggedTimestamp = columnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP);
            final long updateRowId = columnInfo.getQuick(i + ColumnPurgeTask.OFFSET_UPDATE_ROW_ID);
            if (loggedTimestamp == PurgingOperator.TABLE_ROOT_PARTITION) {
                resolvedColumnInfo.add(columnInfo, i, i + ColumnPurgeTask.BLOCK_SIZE);
                continue;
            }
            // The logged timestamp is fromMicros() of the microsecond the log kept. A TIMESTAMP_NS designated timestamp
            // is never before 1970, where toMicros() floors, so the partitions of that microsecond start in the 1000
            // ticks from the logged timestamp.
            int partitionIndex = txFile.findAttachedPartitionIndexByLoTimestamp(loggedTimestamp);
            if (partitionIndex < 0) {
                partitionIndex = -partitionIndex - 1;
            }
            final long partitionTimestampHi = loggedTimestamp + driver.fromMicros(1) - 1;
            for (int partitionCount = txFile.getPartitionCount(); partitionIndex < partitionCount; partitionIndex++) {
                final long partitionTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
                if (partitionTimestamp > partitionTimestampHi) {
                    break;
                }
                // VACUUM TABLE's test: a dropped column has no live version, and a version at or past the snapshot
                // txn may belong to a transaction in flight
                final long latestColumnNameTxn = writerIndex > -1 ? columnVersionReader.getColumnNameTxn(partitionTimestamp, writerIndex) : snapshotTxn;
                if (columnVersion >= snapshotTxn || VacuumColumnVersions.isLiveVersion(columnVersion, latestColumnNameTxn, isPostingColumn, isSymbolColumn, partitionTimestamp)) {
                    LOG.info().$("keeping column version that a purge log entry kept in microseconds names, it is live or in flight [table=")
                            .$(task.getTableToken())
                            .$(", column=").$safe(task.getColumnName())
                            .$(", columnVersion=").$(columnVersion)
                            .$(", latestColumnVersion=").$(latestColumnNameTxn)
                            .$(", partitionTimestamp=").$(partitionTimestamp)
                            .I$();
                    continue;
                }
                resolvedColumnInfo.add(columnVersion, partitionTimestamp, txFile.getPartitionNameTxn(partitionIndex), updateRowId);
            }
        }
    }

    // Writes the held tasks back to the truncated log at their original timestamps, which keeps them unique and in
    // order, and queues them for the running job. The tasks the queue takes leave the list.
    private void retryHeldTasks(ObjList<ColumnPurgeRetryTask> heldTasks) {
        final int taskCount = heldTasks.size();
        if (taskCount == 0) {
            return;
        }
        final long now = clock.getTicks();
        for (int i = 0; i < taskCount; i++) {
            final ColumnPurgeRetryTask task = heldTasks.getQuick(i);
            task.retryDelay = retryDelay;
            task.nextRunTimestamp = now + retryDelay;
            saveToStorage(task);
        }
        commit();
        if (writer == null) {
            // The log lost the rows, so the running job could not mark them completed.
            LOG.error().$("could not keep column versions a save point holds, VACUUM TABLE removes them once the save points move on [taskCount=")
                    .$(taskCount).I$();
            return;
        }
        for (int i = 0; i < taskCount; i++) {
            retryQueue.add(heldTasks.getQuick(i));
        }
        heldTasks.clear();
        LOG.info().$("column versions a save point holds, retrying [taskCount=").$(taskCount).I$();
    }

    private void saveToStorage(ColumnPurgeRetryTask cleanTask) {
        if (writer != null) {
            try {
                LongList updatedColumnInfo = cleanTask.getUpdatedColumnInfo();
                for (int i = 0, n = updatedColumnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
                    TableWriter.Row row = writer.newRow(cleanTask.timestamp);
                    row.putSym(TABLE_NAME_COLUMN, cleanTask.getTableToken().getDirName());
                    row.putSym(COLUMN_NAME_COLUMN, cleanTask.getColumnName());
                    row.putInt(TABLE_ID_COLUMN, cleanTask.getTableId());
                    row.putLong(TABLE_TRUNCATE_VERSION, cleanTask.getTruncateVersion());
                    row.putInt(COLUMN_TYPE_COLUMN, cleanTask.getColumnType());
                    row.putInt(PARTITION_BY_COLUMN, cleanTask.getPartitionBy());
                    row.putLong(UPDATE_TXN_COLUMN, cleanTask.getUpdateTxn());
                    row.putLong(COLUMN_VERSION_COLUMN, updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_COLUMN_VERSION));
                    final long partitionTimestamp = updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP);
                    // partition_timestamp keeps micros, for display and for the versions before partition_timestamp_raw.
                    // The replay reads partition_timestamp_raw.
                    row.putTimestamp(PARTITION_TIMESTAMP_COLUMN, cleanTask.getTimestampTypeDriver().toMicros(partitionTimestamp));
                    if (partitionTimestampRawWriterIndex > -1) {
                        row.putLong(partitionTimestampRawWriterIndex, partitionTimestamp);
                    }
                    row.putLong(PARTITION_NAME_COLUMN, updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_NAME_TXN));
                    row.append();
                    updatedColumnInfo.setQuick(
                            i + ColumnPurgeTask.OFFSET_UPDATE_ROW_ID,
                            Rows.toRowID(writer.getPartitionCount() - 1, writer.getTransientRowCount() - 1)
                    );
                }
            } catch (Throwable th) {
                LOG.error().$("error saving to column version house keeping log, unable to insert")
                        .$(", releasing writer and stop updating log [table=").$(tableToken)
                        .$(", error=").$(th)
                        .I$();
                writer = Misc.free(writer);
            }
        }
    }

    @Override
    protected boolean runSerially() {
        if (inErrorCount >= MAX_ERRORS) {
            return false;
        }

        try {
            boolean useful = processInQueue();
            boolean cleanupUseful = purge();
            if (cleanupUseful) {
                LOG.debug().$("cleaned column version, outstanding tasks: ").$(retryQueue.size()).$();
            }
            inErrorCount = 0;
            return cleanupUseful || useful;
        } catch (Throwable th) {
            LOG.error().$("failed to clean up column versions").$(th).$();
            inErrorCount++;
            if (inErrorCount == MAX_ERRORS) {
                if (!retryQueue.isEmpty()) {
                    LOG.error().$("clean up column versions reached maximum error count and will be recycled. Some column version may be left behind.").$(th).$();
                    retryQueue.clear();
                    inErrorCount = 0;
                } else {
                    LOG.error().$("clean up column versions reached maximum error count and will be DISABLED. Restart QuestDB to re-enable the job.").$(th).$();
                    close();
                }
            }
            return false;
        }
    }

    static class ColumnPurgeRetryTask extends ColumnPurgeTask implements Mutable {
        // whether the startup pass read entries that the log keeps in micros only, see isMicrosOnlyPartitionTimestamp()
        public boolean hasMicrosPartitionTimestamps;
        public long nextRunTimestamp;
        public long retryDelay;
        public long timestamp;

        public void copyFrom(ColumnPurgeTask inTask, long retryDelay, long nextRunTimestamp) {
            this.hasMicrosPartitionTimestamps = false;
            this.retryDelay = retryDelay;
            this.nextRunTimestamp = nextRunTimestamp;
            super.copyFrom(inTask);
        }

        public void of(
                TableToken tableName,
                String columnName,
                int tableId,
                long truncateVersion,
                int columnType,
                byte indexType,
                int timestampType,
                int partitionBy,
                long updateTxn,
                long retryDelay,
                long microTime
        ) {
            super.of(tableName, columnName, tableId, truncateVersion, columnType, indexType, timestampType, partitionBy, updateTxn);
            this.hasMicrosPartitionTimestamps = false;
            this.retryDelay = retryDelay;
            nextRunTimestamp = microTime;
        }
    }
}
