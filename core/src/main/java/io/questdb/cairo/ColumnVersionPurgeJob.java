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
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.tasks.ColumnVersionPurgeTask;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.PriorityQueue;

public class ColumnVersionPurgeJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(ColumnVersionPurgeJob.class);
    private final String tableName;
    private final ColumnVersionPurgeExecution cleanupExecution;
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

    public ColumnVersionPurgeJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache, ColumnVersionPurgeExecution cleanupExecution) throws SqlException {
        CairoConfiguration configuration = engine.getConfiguration();
        this.cleanupExecution = cleanupExecution;
        this.clock = configuration.getMicrosecondClock();
        this.inQueue = engine.getMessageBus().getColumnVersionPurgeQueue();
        this.inSubSequence = engine.getMessageBus().getColumnVersionPurgeSubSeq();
        this.tableName = configuration.getSystemTableNamePrefix() + "_column_versions_purge_log";
        this.taskPool = new ObjectPool<>(ColumnVersionPurgeTaskRun::new, 128);
        this.houseKeepingRunQueue = new PriorityQueue<>(256, ColumnVersionPurgeJob::compareHouseKeepingTasks);
        this.maxWaitCapMs = configuration.getColumnVersionPurgeMaxTimeoutMicros();
        this.exponentialWaitMultiplier = 20.0;

        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS " + getLogTableName() + " (" +
                        "ts timestamp, " + // 0
                        "table_name symbol, " + // 1
                        "column_name symbol, " + // 2
                        "table_id int, " + // 3
                        "columnType int, " + // 4
                        "table_partition_by int, " + // 5
                        "updated_txn long, " + // 6
                        "column_version long, " + // 7
                        "partition_timestamp timestamp, " + // 8
                        "partition_name_txn long" + // 9
                        ") timestamp(ts) partition by MONTH",
                sqlExecutionContext);
        this.writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, getLogTableName(), "QuestDB system");
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
    }

    private static int compareHouseKeepingTasks(ColumnVersionPurgeTaskRun task1, ColumnVersionPurgeTaskRun task2) {
        return Long.compare(task1.nextRunTimestamp, task2.nextRunTimestamp);
    }

    private void calculateNextTimestamp(ColumnVersionPurgeTaskRun task, long currentTime) {
        long totalWait = currentTime - task.lastRunTimestamp;
        task.nextRunTimestamp = Math.min(maxWaitCapMs, Math.max(4L, (long) (totalWait * exponentialWaitMultiplier))) + task.lastRunTimestamp;
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

    private boolean houseKeep() {
        boolean useful = false;
        final long now = clock.getTicks();
        while (houseKeepingRunQueue.size() > 0) {
            ColumnVersionPurgeTaskRun next = houseKeepingRunQueue.peek();
            if (next.nextRunTimestamp <= now) {
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

    private void saveToStorage(ColumnVersionPurgeTaskRun cleanTask) {
        if (writer != null) {
            try {
                LongList updatedColumnVersions = cleanTask.getUpdatedColumnVersions();
                for (int i = 0, n = updatedColumnVersions.size(); i < n; i += ColumnVersionPurgeTask.BLOCK_SIZE) {
                    TableWriter.Row row = writer.newRow(cleanTask.timestamp);
                    row.putSym(1, cleanTask.getTableName());
                    row.putSym(2, cleanTask.getColumnName());
                    row.putInt(3, cleanTask.getTableId());
                    row.putInt(4, cleanTask.getColumnType());
                    row.putInt(5, cleanTask.getPartitionBy());
                    row.putLong(6, cleanTask.getUpdatedTxn());
                    row.putLong(7,
                            updatedColumnVersions.getQuick(i + ColumnVersionPurgeTask.OFFSET_COLUMN_VERSION));
                    row.putTimestamp(8,
                            updatedColumnVersions.getQuick(i + ColumnVersionPurgeTask.OFFSET_PARTITION_TIMESTAMP));
                    row.putLong(9,
                            updatedColumnVersions.getQuick(i + ColumnVersionPurgeTask.OFFSET_PARTITION_NAME_TXN));
                    row.append();
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

    @Override
    protected boolean runSerially() {
        return processInQueue() || houseKeep();
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
