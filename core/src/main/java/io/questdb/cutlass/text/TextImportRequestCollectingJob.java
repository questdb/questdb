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

package io.questdb.cutlass.text;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class TextImportRequestCollectingJob extends SynchronizedJob implements Closeable {
    private static final Log LOG = LogFactory.getLog(TextImportRequestCollectingJob.class);
    private static final int TIMESTAMP_COLUMN = 0;
    private static final int TABLE_NAME_COLUMN = 1;
    private static final int FILE_NAME_COLUMN = 2;
    private static final int HEADER_COLUMN = 3;
    private static final int TIMESTAMP_COLUMN_NAME_COLUMN = 4;
    private static final int FIELD_DELIMITER_COLUMN = 5;
    private static final int TIMESTAMP_FORMAT_COLUMN = 6;
    private static final int TABLE_PARTITION_BY_COLUMN = 7;
    private static final int STATUS_COLUMN = 8;
    private static final int COMPLETED_COLUMN = 9;

    private final String backlogTableName;
    private final RingQueue<TextImportRequestTask> requestCollectingQueue;
    private final Sequence requestCollectingSubSeq;
    private final Sequence requestProcessingStatusSeq;
    private final RingQueue<TextImportRequestTask> requestProcessingQueue;
    private final Sequence requestProcessingPubSeq;

    private final MicrosecondClock clock;
    private SqlCompiler sqlCompiler;
    private TableWriter writer;
    private SqlExecutionContextImpl sqlExecutionContext;
    private final ArrayDeque<TextImportRequestTask> pendingRequests;

    private final WeakMutableObjectPool<TextImportRequestTask> taskPool;
    private final SCSequence eventSubSequence = new SCSequence();

    private final CharSequenceObjHashMap<CancellationToken> inProgressRequests;
    private final CharSequenceHashSet cancelledRequests;

    public TextImportRequestCollectingJob(final CairoEngine engine, @Nullable FunctionFactoryCache functionFactoryCache) throws SqlException {
        CairoConfiguration configuration = engine.getConfiguration();
        this.clock = configuration.getMicrosecondClock();
        this.backlogTableName = configuration.getSystemTableNamePrefix() + "parallel_text_import_log";

        this.requestCollectingQueue = engine.getMessageBus().getTextImportRequestCollectingQueue();
        this.requestCollectingSubSeq = engine.getMessageBus().getTextImportRequestCollectingSubSeq();

        this.requestProcessingQueue = engine.getMessageBus().getTextImportRequestProcessingQueue();
        this.requestProcessingPubSeq = engine.getMessageBus().getTextImportRequestProcessingPubSeq();
        this.requestProcessingStatusSeq = engine.getMessageBus().getTextImportRequestProcessingStatusSeq();

        this.sqlCompiler = new SqlCompiler(engine, functionFactoryCache, null);
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.sqlExecutionContext.with(AllowAllCairoSecurityContext.INSTANCE, null, null);
        this.sqlCompiler.compile(
                "CREATE TABLE IF NOT EXISTS \"" + backlogTableName + "\" (" +
                        "ts timestamp, " + // 0
                        "table_name symbol, " + // 1
                        "file_name symbol, " + // 2
                        "header boolean, " + // 3
                        "timestamp_column_name symbol, " + // 4
                        "field_delimiter byte, " + // 5
                        "timestamp_format symbol, " + // 6
                        "table_partition_by int, " + // 7
                        "status symbol, " + // 8
                        "completed timestamp" + // 9
                        ") timestamp(ts) partition by MONTH",
                sqlExecutionContext
        );
        this.writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, backlogTableName, "QuestDB system");
        final int requestCapacity = requestCollectingQueue.getCycle() * 2;
        this.pendingRequests = new ArrayDeque<>(requestCapacity);
        this.taskPool = new WeakMutableObjectPool<>(TextImportRequestTask::new, requestCapacity);
        this.inProgressRequests = new CharSequenceObjHashMap<>(requestCapacity);
        this.cancelledRequests = new CharSequenceHashSet();
        loadRequestsBacklog();
    }

    @Override
    public void close() throws IOException {
        this.writer = Misc.free(this.writer);
        this.sqlCompiler = Misc.free(sqlCompiler);
        this.sqlExecutionContext = Misc.free(sqlExecutionContext);
    }

    @Override
    protected boolean runSerially() {
        try {
            boolean requestUseful = processIncomingRequestsQueue();
            boolean dispatchUseful = dispatchPendingRequests();
            boolean statusUseful = processStatus();
            return statusUseful || dispatchUseful || requestUseful;
        } catch (Throwable th) {
            return false;
        }
    }

    private boolean processIncomingRequestsQueue() {
        boolean useful = false;
        while (true) {
            long cursor = requestCollectingSubSeq.next();

            if (cursor < 0) {
                break;
            }

            TextImportRequestTask task = requestCollectingQueue.get(cursor);
            String tableName = task.getTableName();
            if (task.getStatus() == TextImportTask.STATUS_CANCEL) {
                if (pendingRequests.contains(task)) {
                    cancelledRequests.add(tableName);
                } else {
                    CancellationToken token = inProgressRequests.get(tableName);
                    if (token != null) {
                        token.cancel();
                        inProgressRequests.remove(tableName);
                    } else {
                        LOG.error().$("cannot cancel import request for table[").$(tableName).$("]").$();
                    }
                }
            } else {
                if (!(pendingRequests.contains(task) || inProgressRequests.contains(tableName))) {
                    TextImportRequestTask newTask = taskPool.pop();
                    newTask.copyFrom(task);
                    appendToTableBacklog(newTask);
                    pendingRequests.add(newTask);
                }
            }

            requestCollectingSubSeq.done(cursor);
            useful = true;
        }

        commit();
        return useful;
    }

    private void commit() {
        try {
            if (writer != null) {
                writer.commit();
            }
        } catch (Throwable th) {
            LOG.error().$("error saving to parallel import house keeping log, cannot commit")
                    .$(", releasing writer and stop updating log [table=").$(backlogTableName)
                    .$(", error=").$(th)
                    .I$();
            writer = Misc.free(writer);
        }
    }

    private boolean dispatchPendingRequests() {
        boolean useful = false;
        while (pendingRequests.size() > 0) {
            TextImportRequestTask pendingTask = pendingRequests.peek();
            String tableName = pendingTask.getTableName();
            // do not dispatch cancelled requests
            if (cancelledRequests.contains(tableName)) {
                cancelledRequests.remove(tableName);
                pendingRequests.poll();
                taskPool.push(pendingTask);
                String statusName = TextImportTask.getStatusName(TextImportTask.STATUS_CANCEL);
                updateRequestStatus(tableName, statusName);
                continue;
            }
            long cursor = requestProcessingPubSeq.next();
            if (cursor > -1) {
                TextImportRequestTask task = requestProcessingQueue.get(cursor);
                task.copyFrom(pendingTask);
                task.setStatus(TextImportTask.STATUS_OK);
                CancellationToken token = task.getCancellationToken();
                token.reset();
                inProgressRequests.put(tableName, token);
                requestProcessingPubSeq.done(cursor);
                pendingRequests.poll();
                taskPool.push(pendingTask);
                useful = true;
            } else {
                break;
            }
        }
        return useful;
    }

    private boolean processStatus() {
        boolean useful = false;
        while (true) {
            long cursor = requestProcessingStatusSeq.next();
            if (cursor > -1) {
                TextImportRequestTask finishedTask = requestProcessingQueue.get(cursor);
                String tableName = finishedTask.getTableName();
                updateRequestStatus(tableName, TextImportTask.getStatusName(finishedTask.getStatus()));
                inProgressRequests.remove(tableName);
                if (finishedTask.getStatus() == TextImportTask.STATUS_CANCEL) {
                    cancelledRequests.remove(tableName);
                }
                requestProcessingStatusSeq.done(cursor);
                useful = true;
            } else {
                break;
            }
        }
        return useful;
    }

    private void updateRequestStatus(final CharSequence tableName, final CharSequence status) {
        CharSequence query = "UPDATE \"" + backlogTableName + "\" SET completed = now(), status = '" + status + "' WHERE table_name = '" + tableName + "' and completed = null";
        try {
            CompiledQuery cq = sqlCompiler.compile(query, sqlExecutionContext);
            try (UpdateOperation op = cq.getUpdateOperation();
                 OperationFuture fut = cq.getDispatcher().execute(op, sqlExecutionContext, eventSubSequence)
            ) {
                writer.tick();
                fut.await();
            }
        } catch (SqlException e) {
            LOG.error().$("failed to update parallel import log table").$((Throwable)e).$();
        }
    }

    private void loadRequestsBacklog() {
        try {
            CompiledQuery reloadQuery = sqlCompiler.compile(
                    "SELECT * FROM \"" + backlogTableName + "\" WHERE completed = null",
                    sqlExecutionContext
            );

            try (RecordCursorFactory recordCursorFactory = reloadQuery.getRecordCursorFactory()) {
                assert recordCursorFactory.supportsUpdateRowId(backlogTableName);
                try (RecordCursor records = recordCursorFactory.getCursor(sqlExecutionContext)) {
                    Record rec = records.getRecord();
                    while (records.hasNext()) {
                        String tableName = Chars.toString(rec.getSym(TABLE_NAME_COLUMN));
                        String fileName = Chars.toString(rec.getSym(FILE_NAME_COLUMN));
                        boolean headerFlag = rec.getBool(HEADER_COLUMN);
                        String timestampColumnName = Chars.toString(rec.getSym(TIMESTAMP_COLUMN_NAME_COLUMN));
                        byte delimiter = rec.getByte(FIELD_DELIMITER_COLUMN);
                        String timestampFormat = Chars.toString(rec.getSym(TIMESTAMP_FORMAT_COLUMN));
                        int partition_by = rec.getInt(TABLE_PARTITION_BY_COLUMN);

                        TextImportRequestTask task = taskPool.pop();
                        task.of(tableName, fileName, headerFlag, timestampColumnName, delimiter, timestampFormat, partition_by);
                        pendingRequests.add(task);
                    }
                }
            }
            if ( pendingRequests.size() == 0 && writer != null) {
                // No tasks to do. Cleanup the log table
                try {
                    writer.truncate();
                } catch (Throwable th) {
                    LOG.error().$("failed to truncate parallel import log table").$(th).$();
                }
            }
        } catch (SqlException e) {
            LOG.error().$("failed to reload parallel import tasks").$((Throwable) e).$();
        }
    }

    private void appendToTableBacklog(final TextImportRequestTask task) {
        if (writer != null) {
            try {
                TableWriter.Row row = writer.newRow(clock.getTicks());
                row.putSym(TABLE_NAME_COLUMN, task.getTableName());
                row.putSym(FILE_NAME_COLUMN, task.getFileName());
                row.putBool(HEADER_COLUMN, task.isHeaderFlag());
                row.putSym(TIMESTAMP_COLUMN_NAME_COLUMN, task.getTimestampColumnName());
                row.putByte(FIELD_DELIMITER_COLUMN, task.getDelimiter());
                row.putSym(TIMESTAMP_FORMAT_COLUMN, task.getTimestampFormat());
                row.putInt(TABLE_PARTITION_BY_COLUMN, task.getPartitionBy());
                row.append();
            } catch (Throwable th) {
                LOG.error().$("error saving to parallel import house keeping log, unable to insert")
                        .$(", releasing writer and stop updating log [table=").$(backlogTableName)
                        .$(", error=").$(th)
                        .I$();
                writer = Misc.free(writer);
            }
        }
    }
}
