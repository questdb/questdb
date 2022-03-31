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

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
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
import io.questdb.tasks.ColumnVersionPurgeTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.PriorityQueue;

public class ColumnVersionPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnVersionPurgeJob.class);
    private final String tableName;
    private static final int TABLE_ID_COLUMN = 3;
    private static final int COLUMN_TYPE_COLUMN = 4;
    private static final int PARTITION_BY_COLUMN = 5;
    private static final int UPDATED_TXN_COLUMN = 6;
    private static final int COLUMN_VERSION_COLUMN = 7;
    private static final int PARTITION_TIMESTAMP_COLUMN = 8;
    private static final int PARTITION_NAME_COLUMN = 9;
    private ColumnVersionPurgeExecution cleanupExecution;
    private final RingQueue<ColumnVersionPurgeTask> inQueue;
    private final Sequence inSubSequence;
    private final MicrosecondClock clock;
    private final PriorityQueue<ColumnVersionPurgeTaskRun> houseKeepingRunQueue;
    private final ObjectPool<ColumnVersionPurgeTaskRun> taskPool;
    private final long maxWaitCapMs;
    private final double exponentialWaitMultiplier;
    private SqlExecutionContextImpl sqlExecutionContext;
    private TableWriter writer;
    private SqlCompiler sqlCompiler;
    private static final int TABLE_NAME_COLUMN = 1;
    private static final int COLUMN_NAME_COLUMN = 2;
    private boolean initliased;

    public ColumnVersionPurgeJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache) throws SqlException {
        CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.inQueue = engine.getMessageBus().getColumnVersionPurgeQueue();
        this.inSubSequence = engine.getMessageBus().getColumnVersionPurgeSubSeq();
        this.tableName = configuration.getSystemTableNamePrefix() + "_column_versions_purge_log";
        this.taskPool = new ObjectPool<>(ColumnVersionPurgeTaskRun::new, 128);
        this.houseKeepingRunQueue = new PriorityQueue<>(256, ColumnVersionPurgeJob::compareHouseKeepingTasks);
        this.maxWaitCapMs = configuration.getColumnVersionPurgeMaxTimeoutMicros();
        this.exponentialWaitMultiplier = configuration.getColumnVersionPurgeWaitExponent();

        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS \"" + getLogTableName() + "\" (" +
                        "ts timestamp, " + // 0
                        "table_name symbol, " + // 1
                        "column_name symbol, " + // 2
                        "table_id int, " + // 3
                        "columnType int, " + // 4
                        "table_partition_by int, " + // 5
                        "updated_txn long, " + // 6
                        "column_version long, " + // 7
                        "partition_timestamp timestamp, " + // 8
                        "partition_name_txn long," + // 9
                        "completed timestamp" + // 10
                        ") timestamp(ts) partition by MONTH",
                sqlExecutionContext);
        this.writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, getLogTableName(), "QuestDB system");
        this.cleanupExecution = new ColumnVersionPurgeExecution(configuration, this.writer, "completed");
    }

    public String getLogTableName() {
        return tableName;
    }

    private void commit() {
        try {
            writer.commit();
        } catch (Throwable th) {
            LOG.error().$("error saving to column version house keeping log, cannot commit [table=").$(getLogTableName()).$(", error=").$(th).I$();
            writer.rollback();
            writer = Misc.free(writer);
            throw th;
        }
    }

    @Override
    public void close() throws IOException {
        this.writer = Misc.free(this.writer);
        this.sqlCompiler = Misc.free(sqlCompiler);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.cleanupExecution = Misc.free(cleanupExecution);
    }

    private static int compareHouseKeepingTasks(ColumnVersionPurgeTaskRun task1, ColumnVersionPurgeTaskRun task2) {
        return Long.compare(task1.nextRunTimestamp, task2.nextRunTimestamp);
    }

    private void calculateNextTimestamp(ColumnVersionPurgeTaskRun task, long currentTime) {
        long totalWait = currentTime - task.lastRunTimestamp;
        task.nextRunTimestamp = currentTime + Math.min(maxWaitCapMs, Math.max(4L, (long) (totalWait * exponentialWaitMultiplier)));
    }

    // Process incoming queue and put it on priority queue with next timestamp to rerun
    private boolean processInQueue() {
        boolean any = false;
        long microTime = clock.getTicks();
        while (true) {
            long cursor = inSubSequence.next();
            // -2 = there was a contest for queue index and this thread has lost
            if (cursor < -1) {
                continue;
            }

            // -1 = queue is empty. All done.
            if (cursor < 0) {
                if (any) {
                    commit();
                }
                return any;
            }

            ColumnVersionPurgeTask queueTask = inQueue.get(cursor);
            ColumnVersionPurgeTaskRun purgeTaskRun = taskPool.next();
            purgeTaskRun.copyFrom(queueTask, microTime++);
            inSubSequence.done(cursor);

            saveToStorage(purgeTaskRun);

            calculateNextTimestamp(purgeTaskRun, microTime);
            houseKeepingRunQueue.add(purgeTaskRun);
            any = true;
        }
    }

    private boolean cleanup() {
        boolean useful = false;
        final long now = clock.getTicks();
        while (houseKeepingRunQueue.size() > 0) {
            ColumnVersionPurgeTaskRun next = houseKeepingRunQueue.peek();
            if (next.nextRunTimestamp <= now) {
                houseKeepingRunQueue.poll();
                useful = true;
                if (!tryClean(next)) {
                    // Re-queue
                    calculateNextTimestamp(next, now);
                    houseKeepingRunQueue.add(next);
                }
            } else {
                // All reruns are in the future.
                return useful;
            }
        }
        return useful;
    }

    private void putTasksFromTableToQueue() {
        try {
            CompiledQuery reloadQuery = this.sqlCompiler.compile(
                    "SELECT * FROM \"" + getLogTableName() + "\" WHERE ts > dateadd('d', -7, now()) and completed = null",
                    sqlExecutionContext
            );

            long microTime = clock.getTicks();
            try (RecordCursorFactory recordFactor = reloadQuery.getRecordCursorFactory()) {
                assert recordFactor.supportsUpdateRowId(tableName);
                try (RecordCursor records = recordFactor.getCursor(sqlExecutionContext)) {
                    io.questdb.cairo.sql.Record rec = records.getRecord();
                    long lastTs = Long.MIN_VALUE;
                    ColumnVersionPurgeTaskRun taskRun = null;

                    while (records.hasNext()) {
                        long ts = rec.getTimestamp(0);
                        if (ts != lastTs) {
                            if (taskRun != null) {
                                calculateNextTimestamp(taskRun, microTime);
                                houseKeepingRunQueue.add(taskRun);
                            }
                            taskRun = taskPool.next();
                            lastTs = ts;
                            String tableName = Chars.toString(rec.getSym(TABLE_NAME_COLUMN));
                            String columnName = Chars.toString(rec.getSym(COLUMN_NAME_COLUMN));
                            int tableId = rec.getInt(TABLE_ID_COLUMN);
                            int columnType = rec.getInt(COLUMN_TYPE_COLUMN);
                            int partitionBY = rec.getInt(PARTITION_BY_COLUMN);
                            long updatedTxn = rec.getLong(UPDATED_TXN_COLUMN);
                            taskRun.of(tableName, columnName, tableId, columnType, partitionBY, updatedTxn);
                        }
                        long columnVersion = rec.getLong(COLUMN_VERSION_COLUMN);
                        long partitionTs = rec.getLong(PARTITION_TIMESTAMP_COLUMN);
                        long partitionNameTxn = rec.getLong(PARTITION_NAME_COLUMN);
                        taskRun.appendColumnVersion(columnVersion, partitionTs, partitionNameTxn, rec.getUpdateRowId());
                    }
                    if (taskRun != null) {
                        calculateNextTimestamp(taskRun, microTime);
                        houseKeepingRunQueue.add(taskRun);
                    }
                }
            }
            if (houseKeepingRunQueue.size() == 0) {
                // No tasks to do. Cleanup the log table
                writer.truncate();
            }
        } catch (SqlException e) {
            LOG.error().$("failed to reload column version purge tasks").$((Throwable) e).$();
        }
    }

    @Override
    protected boolean runSerially() {
        if (!initliased) {
            putTasksFromTableToQueue();
            initliased = true;
        }
        boolean useful = processInQueue();
        boolean cleanupUseful = cleanup();
        if (cleanupUseful) {
            LOG.debug().$("cleaned column version, outstanding tasks: ").$(houseKeepingRunQueue.size()).$();
        }
        return cleanupUseful || useful;
    }

    private void saveToStorage(ColumnVersionPurgeTaskRun cleanTask) {
        if (writer != null) {
            try {
                LongList updatedColumnVersions = cleanTask.getUpdatedColumnVersions();
                for (int i = 0, n = updatedColumnVersions.size(); i < n; i += ColumnVersionPurgeTask.BLOCK_SIZE) {
                    TableWriter.Row row = writer.newRow(cleanTask.timestamp);
                    row.putSym(TABLE_NAME_COLUMN, cleanTask.getTableName());
                    row.putSym(COLUMN_NAME_COLUMN, cleanTask.getColumnName());
                    row.putInt(TABLE_ID_COLUMN, cleanTask.getTableId());
                    row.putInt(COLUMN_TYPE_COLUMN, cleanTask.getColumnType());
                    row.putInt(PARTITION_BY_COLUMN, cleanTask.getPartitionBy());
                    row.putLong(UPDATED_TXN_COLUMN, cleanTask.getUpdatedTxn());
                    row.putLong(COLUMN_VERSION_COLUMN,
                            updatedColumnVersions.getQuick(i + ColumnVersionPurgeTask.OFFSET_COLUMN_VERSION));
                    row.putTimestamp(PARTITION_TIMESTAMP_COLUMN,
                            updatedColumnVersions.getQuick(i + ColumnVersionPurgeTask.OFFSET_PARTITION_TIMESTAMP));
                    row.putLong(PARTITION_NAME_COLUMN,
                            updatedColumnVersions.getQuick(i + ColumnVersionPurgeTask.OFFSET_PARTITION_NAME_TXN));
                    row.append();
                    updatedColumnVersions.setQuick(i + 3, Rows.toRowID(writer.getPartitionCount() - 1, writer.getTransientRowCount() - 1));
                }
            } catch (Throwable th) {
                LOG.error().$("error saving to column version house keeping log, cannot append [table=").$(getLogTableName()).$(", error=").$(th).I$();
                writer.rollback();
                writer = Misc.free(writer);
                throw th;
            }
        }
    }

    private boolean tryClean(ColumnVersionPurgeTaskRun next) {
        return cleanupExecution.tryCleanup(next);
    }

    static class ColumnVersionPurgeTaskRun extends ColumnVersionPurgeTask implements Mutable {
        public long nextRunTimestamp;
        public long timestamp;
        public long attempt;
        public long lastRunTimestamp;

        public void copyFrom(ColumnVersionPurgeTask inTask, long microTime) {
            lastRunTimestamp = nextRunTimestamp = timestamp = microTime;
            attempt = 0;
            super.copyFrom(inTask);
        }
    }
}
