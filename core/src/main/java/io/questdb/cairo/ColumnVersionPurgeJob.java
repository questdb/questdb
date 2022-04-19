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
import java.util.PriorityQueue;

public class ColumnVersionPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnVersionPurgeJob.class);
    private final String tableName;
    private static final int TABLE_ID_COLUMN = 3;
    private static final int TABLE_TRUNCATE_VERSION = 4;
    private static final int COLUMN_TYPE_COLUMN = 5;
    private static final int PARTITION_BY_COLUMN = 6;
    private static final int UPDATED_TXN_COLUMN = 7;
    private static final int COLUMN_VERSION_COLUMN = 8;
    private static final int PARTITION_TIMESTAMP_COLUMN = 9;
    private static final int PARTITION_NAME_COLUMN = 10;
    private static final int MAX_ERRORS = 11;
    private final int lookbackCleanupDays;

    private ColumnVersionPurgeExecution cleanupExecution;
    private final RingQueue<ColumnVersionPurgeTask> inQueue;
    private final Sequence inSubSequence;
    private final MicrosecondClock clock;
    private final PriorityQueue<ColumnVersionPurgeTaskRun> houseKeepingRunQueue;
    private final ObjectPool<ColumnVersionPurgeTaskRun> taskPool;
    private final long maxWaitCapMicro;
    private final long startWaitMicro;
    private final double exponentialWaitMultiplier;
    private SqlExecutionContextImpl sqlExecutionContext;
    private TableWriter writer;
    private SqlCompiler sqlCompiler;
    private static final int TABLE_NAME_COLUMN = 1;
    private static final int COLUMN_NAME_COLUMN = 2;
    private boolean initialised;
    private int inErrorCount;

    public ColumnVersionPurgeJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache) throws SqlException {
        CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.inQueue = engine.getMessageBus().getColumnVersionPurgeQueue();
        this.inSubSequence = engine.getMessageBus().getColumnVersionPurgeSubSeq();
        this.tableName = configuration.getSystemTableNamePrefix() + "column_versions_purge_log";
        this.taskPool = new ObjectPool<>(ColumnVersionPurgeTaskRun::new, 128);
        this.houseKeepingRunQueue = new PriorityQueue<>(256, ColumnVersionPurgeJob::compareHouseKeepingTasks);
        this.maxWaitCapMicro = configuration.getColumnVersionPurgeMaxTimeoutMicros();
        this.startWaitMicro = configuration.getColumnVersionPurgeStartWaitTimeoutMicros();
        this.exponentialWaitMultiplier = configuration.getColumnVersionPurgeWaitExponent();
        this.lookbackCleanupDays = configuration.getColumnVersionCleanupLookbackDays();
        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (" +
                        "ts timestamp, " + // 0
                        "table_name symbol, " + // 1
                        "column_name symbol, " + // 2
                        "table_id int, " + // 3
                        "truncate_version int, " + // 4
                        "columnType int, " + // 5
                        "table_partition_by int, " + // 6
                        "updated_txn long, " + // 7
                        "column_version long, " + // 8
                        "partition_timestamp timestamp, " + // 9
                        "partition_name_txn long," + // 10
                        "completed timestamp" + // 11
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
            LOG.error().$("error saving to column version house keeping log, cannot commit [table=").$(tableName).$(", error=").$(th).I$();
            writer.rollback();
            writer = Misc.free(writer);
            throw th;
        }
    }

    @Override
    public void close() {
        this.writer = Misc.free(this.writer);
        this.sqlCompiler = Misc.free(sqlCompiler);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.cleanupExecution = Misc.free(cleanupExecution);
    }

    private static int compareHouseKeepingTasks(ColumnVersionPurgeTaskRun task1, ColumnVersionPurgeTaskRun task2) {
        return Long.compare(task1.nextRunTimestamp, task2.nextRunTimestamp);
    }

    private void calculateNextTimestamp(ColumnVersionPurgeTaskRun task, long currentTime) {
        task.waitToRun = Math.min(maxWaitCapMicro, (long) (task.waitToRun * exponentialWaitMultiplier));
        task.nextRunTimestamp = currentTime + task.waitToRun;
    }

    private boolean cleanup() {
        boolean useful = false;
        final long now = clock.getTicks() + 1;
        while (houseKeepingRunQueue.size() > 0) {
            ColumnVersionPurgeTaskRun next = houseKeepingRunQueue.peek();
            if (next.nextRunTimestamp < now) {
                houseKeepingRunQueue.poll();
                useful = true;
                if (!cleanupExecution.tryCleanup(next)) {
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
            purgeTaskRun.copyFrom(queueTask, startWaitMicro, microTime + startWaitMicro);
            purgeTaskRun.timestamp = microTime++;
            inSubSequence.done(cursor);

            saveToStorage(purgeTaskRun);

            houseKeepingRunQueue.add(purgeTaskRun);
            any = true;
        }
    }

    private void putTasksFromTableToQueue() {
        try {
            CompiledQuery reloadQuery = this.sqlCompiler.compile(
                    "SELECT * FROM \"" + tableName + "\" WHERE ts > dateadd('d', -" + lookbackCleanupDays + ", now()) and completed = null",
                    sqlExecutionContext
            );

            long microTime = clock.getTicks();
            try (RecordCursorFactory recordFactor = reloadQuery.getRecordCursorFactory()) {
                assert recordFactor.supportsUpdateRowId(tableName);
                try (RecordCursor records = recordFactor.getCursor(sqlExecutionContext)) {
                    io.questdb.cairo.sql.Record rec = records.getRecord();
                    long lastTs = 0;
                    ColumnVersionPurgeTaskRun taskRun = null;

                    while (records.hasNext()) {
                        long ts = rec.getTimestamp(0);
                        if (ts != lastTs || taskRun == null) {
                            if (taskRun != null) {
                                houseKeepingRunQueue.add(taskRun);
                            }
                            taskRun = taskPool.next();
                            lastTs = ts;
                            String tableName = Chars.toString(rec.getSym(TABLE_NAME_COLUMN));
                            String columnName = Chars.toString(rec.getSym(COLUMN_NAME_COLUMN));
                            int tableId = rec.getInt(TABLE_ID_COLUMN);
                            int truncateVersion = rec.getInt(TABLE_TRUNCATE_VERSION);
                            int columnType = rec.getInt(COLUMN_TYPE_COLUMN);
                            int partitionBY = rec.getInt(PARTITION_BY_COLUMN);
                            long updatedTxn = rec.getLong(UPDATED_TXN_COLUMN);
                            taskRun.of(tableName, columnName, tableId, truncateVersion, columnType, partitionBY, updatedTxn, startWaitMicro, microTime);
                        }
                        long columnVersion = rec.getLong(COLUMN_VERSION_COLUMN);
                        long partitionTs = rec.getLong(PARTITION_TIMESTAMP_COLUMN);
                        long partitionNameTxn = rec.getLong(PARTITION_NAME_COLUMN);
                        taskRun.appendColumnVersion(columnVersion, partitionTs, partitionNameTxn, rec.getUpdateRowId());
                    }
                    if (taskRun != null) {
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
        if (inErrorCount >= MAX_ERRORS) {
            return false;
        }

        if (!initialised) {
            putTasksFromTableToQueue();
            initialised = true;
        }

        try {
            boolean useful = processInQueue();
            boolean cleanupUseful = cleanup();
            if (cleanupUseful) {
                LOG.debug().$("cleaned column version, outstanding tasks: ").$(houseKeepingRunQueue.size()).$();
            }
            inErrorCount = 0;
            return cleanupUseful || useful;
        } catch (Throwable th) {
            LOG.error().$("failed to clean up column versions").$(th).$();
            inErrorCount++;
            if (inErrorCount == MAX_ERRORS) {
                if (houseKeepingRunQueue.size() > 0) {
                    LOG.error().$("clean up column versions reached maximum error count and will be recycled. Some column version may be left behind.").$(th).$();
                    houseKeepingRunQueue.clear();
                    inErrorCount = 0;
                } else {
                    LOG.error().$("clean up column versions reached maximum error count and will be DISABLED. Restart QuestDB to re-enable the job.").$(th).$();
                    close();
                }
            }
            return false;
        }
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
                    row.putInt(TABLE_TRUNCATE_VERSION, cleanTask.getTruncateVersion());
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
                LOG.error().$("error saving to column version house keeping log, cannot append [table=").$(tableName).$(", error=").$(th).I$();
                writer.rollback();
                throw th;
            }
        }
    }

    static class ColumnVersionPurgeTaskRun extends ColumnVersionPurgeTask implements Mutable {
        public long nextRunTimestamp;
        public long timestamp;
        public long waitToRun;

        public void copyFrom(ColumnVersionPurgeTask inTask, long waitToRun, long nextRunTimestamp) {
            this.waitToRun = waitToRun;
            this.nextRunTimestamp = nextRunTimestamp;
            super.copyFrom(inTask);
        }

        public void of(String tableName, CharSequence columnName, int tableId, int truncateVersion, int columnType, int partitionBy, long lastTxn, long waitToRun, long microTime) {
            super.of(tableName, columnName, tableId, truncateVersion, columnType, partitionBy, lastTxn);
            this.waitToRun = waitToRun;
            nextRunTimestamp = microTime;
        }
    }
}
