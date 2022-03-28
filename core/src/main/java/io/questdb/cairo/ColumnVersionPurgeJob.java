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
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
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
    public final String tableName;
    private final CairoConfiguration configuration;
    private final ColumnVersionPurgeExecution cleanupExecution;
    private final RingQueue<ColumnVersionPurgeTask> inQueue;
    private final SCSequence inSubSequence;
    private final MicrosecondClock clock;
    private final PriorityQueue<ColumnVersionPurgeTaskRun> houseKeepingRunQueue;
    private final ObjectPool<ColumnVersionPurgeTaskRun> taskPool;
    private final long maxWaitCapMs;
    private final double exponentialWaitMultiplier;
    private SqlExecutionContextImpl sqlExecutionContext;
    private TableWriter writer;
    private SqlCompiler sqlCompiler;

    public ColumnVersionPurgeJob(CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache, ColumnVersionPurgeExecution cleanupExecution) throws SqlException {
        this.configuration = engine.getConfiguration();
        this.cleanupExecution = cleanupExecution;
        this.clock = configuration.getMicrosecondClock();
        this.inQueue = engine.getMessageBus().getColumnVersionPurgeQueue();
        this.inSubSequence = engine.getMessageBus().getColumnVersionPurgeSubSeq();
        this.tableName = configuration.getSystemTableNamePrefix() + "_column_versions_house_keeping";
        this.taskPool = new ObjectPool<>(ColumnVersionPurgeTaskRun::new, 128);
        this.houseKeepingRunQueue = new PriorityQueue<>(256, ColumnVersionPurgeJob::compareHouseKeepingTasks);
        this.maxWaitCapMs = configuration.getColumnVersionPurgeMaxTimeoutMicros();
        this.exponentialWaitMultiplier = 20.0;

        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS " + tableName + " (ts timestamp, table_name symbol, column_name symbol, long table_id, long column_version) timestamp(ts) partition by MONTH",
                sqlExecutionContext);
        this.writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "QuestDB system");
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

    private boolean tryClean(ColumnVersionPurgeTaskRun next) {
        return cleanupExecution.tryCleanup(next.getTableName(), next.getColumnName(), next.getTableId(), next.getColumnVersion());
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

            ColumnVersionPurgeTask inTask = inQueue.get(cursor);
            ColumnVersionPurgeTaskRun housekeepingRun = taskPool.next().of(inTask, microTime++);
            inSubSequence.done(cursor);

            saveToStorage(housekeepingRun);

            calculateNextTimestamp(housekeepingRun, microTime);
            houseKeepingRunQueue.add(housekeepingRun);
            any = true;
        }
    }

    @Override
    protected boolean runSerially() {
        return processInQueue() || houseKeep();
    }

    private void saveToStorage(ColumnVersionPurgeTaskRun housekeepingRun) {
        if (writer != null) {
            try {
                TableWriter.Row row = writer.newRow(housekeepingRun.timestamp);
                row.putSym(1, housekeepingRun.getTableName());
                row.putSym(2, housekeepingRun.getColumnName());
                row.putLong(3, housekeepingRun.getTableId());
                row.putLong(4, housekeepingRun.getColumnVersion());
                row.append();
            } catch (Throwable th) {
                LOG.error().$("error saving to column version house keeping log, cannot append [table=").$(tableName).$(", error=").$(th).I$();
                writer.rollback();
                writer = Misc.free(writer);
                throw th;
            }
        }
    }

    static class ColumnVersionPurgeTaskRun extends ColumnVersionPurgeTask implements Mutable {
        public long nextRunTimestamp;
        public long timestamp;
        public long attempt;
        public long lastRunTimestamp;

        @Override
        public void clear() {
        }

        public ColumnVersionPurgeTaskRun of(ColumnVersionPurgeTask inTask, long microTime) {
            lastRunTimestamp = nextRunTimestamp = timestamp = microTime;
            attempt = 0;
            setTableName(inTask.getTableName());
            setColumnName(inTask.getColumnName());
            setColumnVersion(inTask.getColumnVersion());
            setTableId(inTask.getTableId());
            return this;
        }
    }
}
