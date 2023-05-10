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

package io.questdb.cairo;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.ColumnPurgeTask;
import org.jetbrains.annotations.Nullable;
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
    private static final int TABLE_ID_COLUMN = 3;
    private static final int TABLE_NAME_COLUMN = 1;
    private static final int TABLE_TRUNCATE_VERSION = 4;
    private static final int UPDATE_TXN_COLUMN = 7;
    private final MicrosecondClock clock;
    private final RingQueue<ColumnPurgeTask> inQueue;
    private final Sequence inSubSequence;
    private final long retryDelay;
    private final long retryDelayLimit;
    private final double retryDelayMultiplier;
    private final PriorityQueue<ColumnPurgeRetryTask> retryQueue;
    private final TableToken tableToken;
    private ColumnPurgeOperator columnPurgeOperator;
    private int inErrorCount;
    private SqlCompiler sqlCompiler;
    private SqlExecutionContextImpl sqlExecutionContext;
    private WeakMutableObjectPool<ColumnPurgeRetryTask> taskPool;
    private TableWriter writer;

    public ColumnPurgeJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache) throws SqlException {
        CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.inQueue = engine.getMessageBus().getColumnPurgeQueue();
        this.inSubSequence = engine.getMessageBus().getColumnPurgeSubSeq();
        String tableName = configuration.getSystemTableNamePrefix() + "column_versions_purge_log";
        this.taskPool = new WeakMutableObjectPool<>(ColumnPurgeRetryTask::new, configuration.getColumnPurgeTaskPoolCapacity());
        this.retryQueue = new PriorityQueue<>(configuration.getColumnPurgeQueueCapacity(), ColumnPurgeJob::compareRetryTasks);
        this.retryDelayLimit = configuration.getColumnPurgeRetryDelayLimit();
        this.retryDelay = configuration.getColumnPurgeRetryDelay();
        this.retryDelayMultiplier = configuration.getColumnPurgeRetryDelayMultiplier();
        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null
        );
        this.tableToken = this.sqlCompiler.query().$(
                        "CREATE TABLE IF NOT EXISTS \"")
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
                        "completed timestamp" + // 11
                        ") timestamp(ts) partition by MONTH BYPASS WAL"
                )
                .compile(sqlExecutionContext)
                .getTableToken();
        this.writer = engine.getWriter(tableToken, "QuestDB system");
        this.columnPurgeOperator = new ColumnPurgeOperator(configuration, this.writer, "completed");
        processTableRecords(engine);
    }

    @Override
    public void close() {
        this.writer = Misc.free(writer);
        this.sqlCompiler = Misc.free(sqlCompiler);
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
        try {
            CompiledQuery reloadQuery = sqlCompiler.query()
                    .$("SELECT * FROM \"")
                    .$(tableToken.getTableName())
                    .$("\" WHERE completed = null")
                    .compile(sqlExecutionContext);

            long microTime = clock.getTicks();
            try (RecordCursorFactory recordCursorFactory = reloadQuery.getRecordCursorFactory()) {
                assert recordCursorFactory.supportsUpdateRowId(tableToken);
                int count = 0;

                try (RecordCursor records = recordCursorFactory.getCursor(sqlExecutionContext)) {
                    Record rec = records.getRecord();
                    long lastTs = 0;
                    ColumnPurgeRetryTask taskRun = null;

                    CharSequenceObjHashMap<String> stringIntern = new CharSequenceObjHashMap<>();

                    while (records.hasNext()) {
                        count++;
                        long ts = rec.getTimestamp(0);
                        if (ts != lastTs || taskRun == null) {
                            if (taskRun != null) {
                                columnPurgeOperator.purgeExclusive(taskRun);
                            } else {
                                taskRun = taskPool.pop();
                            }

                            lastTs = ts;
                            String tableName = internStrObj(stringIntern, rec.getSym(TABLE_NAME_COLUMN));
                            String columnName = internStrObj(stringIntern, rec.getSym(COLUMN_NAME_COLUMN));
                            int tableId = rec.getInt(TABLE_ID_COLUMN);
                            long truncateVersion = rec.getLong(TABLE_TRUNCATE_VERSION);
                            int columnType = rec.getInt(COLUMN_TYPE_COLUMN);
                            int partitionBy = rec.getInt(PARTITION_BY_COLUMN);
                            long updateTxn = rec.getLong(UPDATE_TXN_COLUMN);
                            TableToken token = engine.getTableTokenByDirName(tableName, tableId);

                            if (token == null) {
                                LOG.debug().$("table deleted, skipping [tableDir=").utf8(tableName).I$();
                                continue;
                            }

                            taskRun.of(
                                    token,
                                    columnName,
                                    tableId,
                                    truncateVersion,
                                    columnType,
                                    partitionBy,
                                    updateTxn,
                                    retryDelay,
                                    microTime
                            );
                        }
                        long columnVersion = rec.getLong(COLUMN_VERSION_COLUMN);
                        long partitionTs = rec.getLong(PARTITION_TIMESTAMP_COLUMN);
                        long partitionNameTxn = rec.getLong(PARTITION_NAME_COLUMN);
                        taskRun.appendColumnInfo(columnVersion, partitionTs, partitionNameTxn, rec.getUpdateRowId());
                    }
                    if (taskRun != null) {
                        columnPurgeOperator.purgeExclusive(taskRun);
                        taskPool.push(taskRun);
                    }
                }

                if (count > 0) {
                    LOG.info().$("cleaned up rewritten column files [cleanCount=").$(count).I$();
                }
            }
            if (writer != null) {
                try {
                    writer.truncate();
                } catch (Throwable th) {
                    LOG.error().$("failed to truncate column version purge log table").$(th).$();
                }
            }
        } catch (SqlException e) {
            LOG.error().$("failed to reload column version purge tasks").$((Throwable) e).$();
        }
    }

    private boolean purge() {
        boolean useful = false;
        final long now = clock.getTicks() + 1;
        while (retryQueue.size() > 0) {
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

    private void saveToStorage(ColumnPurgeRetryTask cleanTask) {
        if (writer != null) {
            try {
                LongList updatedColumnInfo = cleanTask.getUpdatedColumnInfo();
                for (int i = 0, n = updatedColumnInfo.size(); i < n; i += ColumnPurgeTask.BLOCK_SIZE) {
                    TableWriter.Row row = writer.newRow(cleanTask.timestamp);
                    row.putSym(TABLE_NAME_COLUMN, cleanTask.getTableName().getDirName());
                    row.putSym(COLUMN_NAME_COLUMN, cleanTask.getColumnName());
                    row.putInt(TABLE_ID_COLUMN, cleanTask.getTableId());
                    row.putLong(TABLE_TRUNCATE_VERSION, cleanTask.getTruncateVersion());
                    row.putInt(COLUMN_TYPE_COLUMN, cleanTask.getColumnType());
                    row.putInt(PARTITION_BY_COLUMN, cleanTask.getPartitionBy());
                    row.putLong(UPDATE_TXN_COLUMN, cleanTask.getUpdateTxn());
                    row.putLong(COLUMN_VERSION_COLUMN, updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_COLUMN_VERSION));
                    row.putTimestamp(PARTITION_TIMESTAMP_COLUMN, updatedColumnInfo.getQuick(i + ColumnPurgeTask.OFFSET_PARTITION_TIMESTAMP));
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
                if (retryQueue.size() > 0) {
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
        public long nextRunTimestamp;
        public long retryDelay;
        public long timestamp;

        public void copyFrom(ColumnPurgeTask inTask, long retryDelay, long nextRunTimestamp) {
            this.retryDelay = retryDelay;
            this.nextRunTimestamp = nextRunTimestamp;
            super.copyFrom(inTask);
        }

        public void of(
                TableToken tableName,
                CharSequence columnName,
                int tableId,
                long truncateVersion,
                int columnType,
                int partitionBy,
                long updateTxn,
                long retryDelay,
                long microTime
        ) {
            super.of(tableName, columnName, tableId, truncateVersion, columnType, partitionBy, updateTxn);
            this.retryDelay = retryDelay;
            nextRunTimestamp = microTime;
        }
    }
}
