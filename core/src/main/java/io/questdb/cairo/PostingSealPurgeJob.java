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
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.WeakMutableObjectPool;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.Path;
import io.questdb.tasks.PostingSealPurgeTask;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.PriorityQueue;

public class PostingSealPurgeJob extends SynchronizedJob implements Closeable {

    private static final int COLUMN_NAME_COLUMN = 3;
    private static final int FROM_TABLE_TXN_COLUMN = 9;
    private static final Log LOG = LogFactory.getLog(PostingSealPurgeJob.class);
    private static final int MAX_ERRORS = 11;
    private static final int PARTITION_BY_COLUMN = 8;
    private static final int PARTITION_NAME_TXN_COLUMN = 7;
    private static final int PARTITION_TIMESTAMP_COLUMN = 6;
    private static final int POSTING_COLUMN_NAME_TXN_COLUMN = 4;
    private static final int SEAL_TXN_COLUMN = 5;
    private static final int TABLE_ID_COLUMN = 2;
    private static final int TABLE_NAME_COLUMN = 1;
    private static final int TIMESTAMP_TYPE_COLUMN = 11;
    private static final int TO_TABLE_TXN_COLUMN = 10;
    private final MicrosecondClock clock;
    private final Path completedPath;
    private final int completedWriterIndex;
    private final FilesFacade ff;
    private final RingQueue<PostingSealPurgeTask> inQueue;
    private final SCSequence inSubSequence;
    private final int pathRootLen;
    private final long retryDelay;
    private final long retryDelayLimit;
    private final double retryDelayMultiplier;
    private final PriorityQueue<RetryEntry> retryQueue;
    private final TableToken tableToken;
    private long completedFd = -1;
    private long completedFdPartitionTimestamp = Long.MIN_VALUE;
    private int errorCount;
    private long longBuf;
    private PostingSealPurgeOperator operator;
    private SqlExecutionContextImpl sqlExecutionContext;
    private WeakMutableObjectPool<RetryEntry> taskPool;
    private TableWriter writer;

    public PostingSealPurgeJob(CairoEngine engine) throws SqlException {
        try {
            CairoConfiguration configuration = engine.getConfiguration();
            this.clock = configuration.getMicrosecondClock();
            this.ff = configuration.getFilesFacade();
            this.completedPath = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
            this.completedPath.of(configuration.getDbRoot());
            this.pathRootLen = completedPath.size();
            this.longBuf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_SQL_COMPILER);
            this.inQueue = engine.getMessageBus().getPostingSealPurgeQueue();
            this.inSubSequence = engine.getMessageBus().getPostingSealPurgeSubSeq();
            this.taskPool = new WeakMutableObjectPool<>(RetryEntry::new, configuration.getColumnPurgeTaskPoolCapacity());
            this.retryQueue = new PriorityQueue<>(configuration.getColumnPurgeQueueCapacity(), PostingSealPurgeJob::compareRetry);
            this.retryDelay = configuration.getColumnPurgeRetryDelay();
            this.retryDelayLimit = configuration.getColumnPurgeRetryDelayLimit();
            this.retryDelayMultiplier = configuration.getColumnPurgeRetryDelayMultiplier();
            this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
            this.sqlExecutionContext.with(
                    configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                    null,
                    null
            );
            String tableName = configuration.getSystemTableNamePrefix() + "posting_seal_purge_log";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                this.tableToken = compiler.query()
                        .$("CREATE TABLE IF NOT EXISTS \"")
                        .$(tableName)
                        .$("\" (" +
                                "ts timestamp, " +
                                "table_name symbol, " +
                                "table_id int, " +
                                "column_name symbol, " +
                                "posting_column_name_txn long, " +
                                "seal_txn long, " +
                                "partition_timestamp timestamp, " +
                                "partition_name_txn long, " +
                                "partition_by int, " +
                                "from_table_txn long, " +
                                "to_table_txn long, " +
                                "timestamp_type int, " +
                                "completed timestamp" +
                                ") timestamp(ts) partition by MONTH BYPASS WAL"
                        )
                        .createTable(sqlExecutionContext);
            }
            this.writer = engine.getWriter(tableToken, "QuestDB system");
            this.completedWriterIndex = writer.getMetadata().getColumnIndex("completed");
            this.operator = new PostingSealPurgeOperator(engine);
            recoverOpenTasks(engine);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        closeCompletedFd();
        Misc.free(completedPath);
        if (longBuf != 0L) {
            Unsafe.free(longBuf, Long.BYTES, MemoryTag.NATIVE_SQL_COMPILER);
            longBuf = 0L;
        }
        this.writer = Misc.free(writer);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
        this.operator = Misc.free(operator);
        this.taskPool = Misc.free(taskPool);
    }

    @TestOnly
    public String getLogTableName() {
        return tableToken == null ? null : tableToken.getTableName();
    }

    @TestOnly
    public int getOutstandingPurgeTasks() {
        return retryQueue.size();
    }

    private static int compareRetry(RetryEntry a, RetryEntry b) {
        return Long.compare(a.nextRunTime, b.nextRunTime);
    }

    private void calculateNextRunTime(RetryEntry entry, long now) {
        entry.retryDelay = Math.min(retryDelayLimit, (long) (entry.retryDelay * retryDelayMultiplier));
        entry.nextRunTime = now + entry.retryDelay;
    }

    private void closeCompletedFd() {
        if (completedFd != -1) {
            ff.close(completedFd);
            completedFd = -1;
            completedFdPartitionTimestamp = Long.MIN_VALUE;
        }
    }

    private void commit() {
        if (writer != null) {
            try {
                writer.commit();
            } catch (Throwable th) {
                LOG.error().$("posting seal purge: log commit failed, disabling writer [err=").$(th).I$();
                errorCount++;
                writer = Misc.free(writer);
            }
        }
    }

    private boolean ensureCompletedFdForPartition(long partitionTimestamp) {
        if (writer == null || completedWriterIndex < 0) {
            return false;
        }
        if (completedFd != -1 && completedFdPartitionTimestamp == partitionTimestamp) {
            return true;
        }
        closeCompletedFd();
        try {
            completedPath.trimTo(pathRootLen).concat(writer.getTableToken().getDirName());
            int partitionIndex = writer.getPartitionIndexByTimestamp(partitionTimestamp);
            if (partitionIndex < 0) {
                return false;
            }
            long partitionNameTxn = writer.getPartitionNameTxn(partitionIndex);
            TableUtils.setPathForNativePartition(
                    completedPath,
                    writer.getMetadata().getTimestampType(),
                    writer.getPartitionBy(),
                    partitionTimestamp,
                    partitionNameTxn
            );
            TableUtils.dFile(
                    completedPath,
                    "completed",
                    writer.getColumnNameTxn(partitionTimestamp, completedWriterIndex)
            );
            completedFd = TableUtils.openRW(ff, completedPath.$(), LOG, writer.getConfiguration().getWriterFileOpenOpts());
            completedFdPartitionTimestamp = partitionTimestamp;
            return true;
        } catch (Throwable th) {
            LOG.error().$("posting seal purge: cannot open completed-column file [err=").$(th).I$();
            errorCount++;
            closeCompletedFd();
            return false;
        }
    }

    private void markCompleted(long rowId, long completionTime) {
        if (rowId < 0 || writer == null || completedWriterIndex < 0) {
            return;
        }
        try {
            int partitionIndex = Rows.toPartitionIndex(rowId);
            long partitionTimestamp = writer.getPartitionTimestamp(partitionIndex);
            if (!ensureCompletedFdForPartition(partitionTimestamp)) {
                return;
            }
            long localRowId = Rows.toLocalRowID(rowId);
            long offset = localRowId * Long.BYTES;
            Unsafe.getUnsafe().putLong(longBuf, completionTime);
            if (ff.write(completedFd, longBuf, Long.BYTES, offset) != Long.BYTES) {
                LOG.error().$("posting seal purge: completed-column write failed [errno=").$(ff.errno())
                        .$(", offset=").$(offset)
                        .$(", fd=").$(completedFd)
                        .I$();
                errorCount++;
                closeCompletedFd();
            }
        } catch (Throwable th) {
            LOG.error().$("posting seal purge: failed to mark row complete [rowId=").$(rowId).$(", err=").$(th).I$();
            errorCount++;
            closeCompletedFd();
        }
    }

    private void persistTask(RetryEntry entry) {
        if (writer == null) {
            return;
        }
        try {
            TableToken tok = entry.getTableToken();
            TableWriter.Row row = writer.newRow(entry.scheduledAt);
            row.putSym(TABLE_NAME_COLUMN, tok.getDirName());
            row.putInt(TABLE_ID_COLUMN, tok.getTableId());
            row.putSym(COLUMN_NAME_COLUMN, entry.getIndexColumnName());
            row.putLong(POSTING_COLUMN_NAME_TXN_COLUMN, entry.getPostingColumnNameTxn());
            row.putLong(SEAL_TXN_COLUMN, entry.getSealTxn());
            row.putTimestamp(PARTITION_TIMESTAMP_COLUMN, entry.getPartitionTimestamp());
            row.putLong(PARTITION_NAME_TXN_COLUMN, entry.getPartitionNameTxn());
            row.putInt(PARTITION_BY_COLUMN, entry.getPartitionBy());
            row.putLong(FROM_TABLE_TXN_COLUMN, entry.getFromTableTxn());
            row.putLong(TO_TABLE_TXN_COLUMN, entry.getToTableTxn());
            row.putInt(TIMESTAMP_TYPE_COLUMN, entry.getTimestampType());
            row.append();
            entry.logRowId = Rows.toRowID(writer.getPartitionCount() - 1, writer.getTransientRowCount() - 1);
        } catch (Throwable th) {
            LOG.error().$("posting seal purge: failed to persist task, log writer disabled [err=").$(th).I$();
            errorCount++;
            writer = Misc.free(writer);
        }
    }

    private boolean processInQueue() {
        boolean useful = false;
        long now = clock.getTicks();
        while (writer != null) {
            long cursor = inSubSequence.next();
            if (cursor < -1) {
                Os.pause();
                continue;
            }
            if (cursor < 0) {
                break;
            }
            PostingSealPurgeTask src = inQueue.get(cursor);
            RetryEntry entry = taskPool.pop();
            // First attempt immediate; retryDelay seeded with config base so
            // the first-failure backoff multiplies from a non-zero value.
            entry.copyFrom(src, now, retryDelay, now);
            inSubSequence.done(cursor);
            persistTask(entry);
            retryQueue.add(entry);
            useful = true;
        }
        if (useful) {
            commit();
        }
        return useful;
    }

    private boolean processRetryQueue() {
        boolean useful = false;
        long now = clock.getTicks();
        while (!retryQueue.isEmpty()) {
            RetryEntry head = retryQueue.peek();
            if (head.nextRunTime > now) {
                return useful;
            }
            retryQueue.poll();
            useful = true;
            boolean done;
            try {
                done = operator.purge(head);
            } catch (Throwable th) {
                LOG.error().$("posting seal purge: operator threw, re-queuing [err=").$(th).I$();
                done = false;
            }
            // Refresh after the I/O so scheduling and completed timestamp
            // reflect the time purge actually took.
            now = clock.getTicks();
            if (done) {
                markCompleted(head.logRowId, now);
                head.clear();
                taskPool.push(head);
            } else {
                calculateNextRunTime(head, now);
                retryQueue.add(head);
            }
        }
        if (useful) {
            commit();
        }
        return useful;
    }

    private void recoverOpenTasks(CairoEngine engine) {
        RecordCursorFactory factory;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            factory = compiler.query()
                    .$("SELECT * FROM \"")
                    .$(tableToken.getTableName())
                    .$("\" WHERE completed = null")
                    .compile(sqlExecutionContext)
                    .getRecordCursorFactory();
        } catch (Throwable th) {
            LOG.advisory().$("posting seal purge: recovery query failed, starting empty [err=").$(th).I$();
            return;
        }
        int succeeded = 0;
        int failed = 0;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Record rec = cursor.getRecord();
            RetryEntry entry = taskPool.pop();
            try {
                while (cursor.hasNext()) {
                    CharSequence tableDirName = rec.getSymA(TABLE_NAME_COLUMN);
                    int tableId = rec.getInt(TABLE_ID_COLUMN);
                    TableToken token = engine.getTableTokenByDirName(tableDirName);
                    if (token == null || token.getTableId() != tableId) {
                        continue;
                    }
                    String columnName = io.questdb.std.Chars.toString(rec.getSymA(COLUMN_NAME_COLUMN));
                    entry.of(
                            token,
                            columnName,
                            rec.getLong(POSTING_COLUMN_NAME_TXN_COLUMN),
                            rec.getLong(SEAL_TXN_COLUMN),
                            rec.getTimestamp(PARTITION_TIMESTAMP_COLUMN),
                            rec.getLong(PARTITION_NAME_TXN_COLUMN),
                            rec.getInt(PARTITION_BY_COLUMN),
                            rec.getInt(TIMESTAMP_TYPE_COLUMN),
                            rec.getLong(FROM_TABLE_TXN_COLUMN),
                            rec.getLong(TO_TABLE_TXN_COLUMN)
                    );
                    boolean done;
                    try {
                        done = operator.purge(entry);
                    } catch (Throwable th) {
                        LOG.error().$("posting seal purge: recovery purge failed [err=").$(th).I$();
                        done = false;
                    }
                    if (done) {
                        succeeded++;
                    } else {
                        failed++;
                    }
                }
            } finally {
                entry.clear();
                taskPool.push(entry);
            }
            if (succeeded + failed > 0) {
                LOG.info().$("posting seal purge: recovery done [succeeded=").$(succeeded)
                        .$(", failed=").$(failed).I$();
            }
        } catch (Throwable th) {
            LOG.error().$("posting seal purge: recovery cursor failed [err=").$(th).I$();
        } finally {
            Misc.free(factory);
        }

        if (failed == 0 && succeeded > 0 && writer != null) {
            try {
                writer.truncate();
            } catch (Throwable th) {
                LOG.error().$("posting seal purge: failed to truncate log table [err=").$(th).I$();
            }
        }
    }

    @Override
    protected boolean runSerially() {
        if (errorCount >= MAX_ERRORS) {
            return false;
        }
        int before = errorCount;
        boolean queueUseful = false;
        boolean retryUseful = false;
        try {
            queueUseful = processInQueue();
            retryUseful = processRetryQueue();
        } catch (Throwable th) {
            LOG.error().$("posting seal purge: job loop failed [err=").$(th).I$();
            errorCount++;
        }
        if (errorCount == before) {
            errorCount = 0;
        } else if (errorCount >= MAX_ERRORS) {
            LOG.error().$("posting seal purge: too many errors, disabling job (restart QuestDB to re-enable)").$();
            close();
            return false;
        }
        return queueUseful || retryUseful;
    }

    static final class RetryEntry extends PostingSealPurgeTask {
        long logRowId = -1;
        long nextRunTime;
        long retryDelay;
        long scheduledAt;

        @Override
        public void clear() {
            super.clear();
            logRowId = -1;
            nextRunTime = 0L;
            retryDelay = 0L;
            scheduledAt = 0L;
        }

        void copyFrom(PostingSealPurgeTask src, long scheduledAt, long retryDelay, long nextRunTime) {
            this.scheduledAt = scheduledAt;
            this.retryDelay = retryDelay;
            this.nextRunTime = nextRunTime;
            of(
                    src.getTableToken(),
                    src.getIndexColumnName(),
                    src.getPostingColumnNameTxn(),
                    src.getSealTxn(),
                    src.getPartitionTimestamp(),
                    src.getPartitionNameTxn(),
                    src.getPartitionBy(),
                    src.getTimestampType(),
                    src.getFromTableTxn(),
                    src.getToTableTxn()
            );
        }
    }
}
