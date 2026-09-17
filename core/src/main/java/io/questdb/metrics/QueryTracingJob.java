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

package io.questdb.metrics;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ValueHolderList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;
import java.io.IOException;

public class QueryTracingJob extends SynchronizedJob implements Closeable {
    public static final String COLUMN_EXECUTION_MICROS = "execution_micros";
    public static final String COLUMN_PRINCIPAL = "principal";
    public static final String COLUMN_QUERY_START = "query_start";
    public static final String COLUMN_QUERY_TEXT = "query_text";
    public static final String COLUMN_TS = "ts";
    public static final String TABLE_NAME = "_query_trace";
    // Writer lock reason used when the query-tracing job acquires its own table writer.
    public static final String WRITER_LOCK_REASON = "query_tracing";
    private static final int BATCH_LIMIT = 1024;
    private static final int INITIAL_CAPACITY = 128;
    private static final Log LOG = LogFactory.getLog(QueryTracingJob.class.getName());
    private final ValueHolderList<QueryTrace> buffer;
    private final int executionMicrosColumnIndex;
    private final int principalColumnIndex;
    private final int queryStartColumnIndex;
    private final int queryTextColumnIndex;
    private final ConcurrentQueue<QueryTrace> queue;
    private final TableWriter tableWriter;
    private final TimestampDriver timestampDriver;
    private final QueryTrace trace = new QueryTrace();
    private final Utf8StringSink utf8sink = new Utf8StringSink();


    public QueryTracingJob(CairoEngine engine) throws SqlException {
        this.queue = engine.getMessageBus().getQueryTraceQueue();
        this.buffer = new ValueHolderList<>(QueryTrace.ITEM_FACTORY, INITIAL_CAPACITY);
        final SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null
        );
        this.tableWriter = acquireTableWriter(engine, sqlExecutionContext);
        this.timestampDriver = ColumnType.getTimestampDriver(tableWriter.getTimestampType());
        final TableRecordMetadata metadata = tableWriter.getMetadata();
        this.queryTextColumnIndex = metadata.getColumnIndex(COLUMN_QUERY_TEXT);
        this.executionMicrosColumnIndex = metadata.getColumnIndex(COLUMN_EXECUTION_MICROS);
        this.principalColumnIndex = metadata.getColumnIndex(COLUMN_PRINCIPAL);
        this.queryStartColumnIndex = metadata.getColumnIndex(COLUMN_QUERY_START);
    }

    @Override
    public void close() throws IOException {
        tableWriter.close();
    }

    private static TableWriter acquireTableWriter(
            CairoEngine engine,
            SqlExecutionContextImpl sqlExecutionContext
    ) throws SqlException {
        TableToken tableToken;
        try {
            tableToken = engine.verifyTableName(TABLE_NAME);
        } catch (Exception recoverable) {
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                CompiledQuery query = sqlCompiler.query()
                        .$("CREATE TABLE IF NOT EXISTS '").$(TABLE_NAME).$("' (")
                        .$(COLUMN_TS).$(" TIMESTAMP, ")
                        .$(COLUMN_QUERY_TEXT).$(" VARCHAR, ")
                        .$(COLUMN_EXECUTION_MICROS).$(" LONG, ")
                        .$(COLUMN_PRINCIPAL).$(" VARCHAR, ")
                        .$(COLUMN_QUERY_START).$(" TIMESTAMP")
                        .$(") TIMESTAMP(").$(COLUMN_TS).$(") PARTITION BY HOUR TTL 1 DAY BYPASS WAL")
                        .compile(sqlExecutionContext);
                query.getOperation().execute(sqlExecutionContext, null);
                tableToken = engine.verifyTableName(TABLE_NAME);
            }
        }
        final TableWriter writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
        try {
            final TableRecordMetadata metadata = writer.getMetadata();
            if (metadata.getColumnIndexQuiet(COLUMN_QUERY_START) < 0) {
                final SecurityContext securityContext = sqlExecutionContext.getSecurityContext();
                writer.addColumn(COLUMN_QUERY_START, ColumnType.TIMESTAMP, securityContext);
            }
        } catch (Throwable th) {
            writer.close();
            throw th;
        }
        return writer;
    }

    private void convertClosedPartitionToParquet() {
        final int partitionCount = tableWriter.getPartitionCount();
        final long activePartitionTimestamp = tableWriter.getLogicalPartitionTimestamp(tableWriter.getMaxTimestamp());
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
            if (tableWriter.getLogicalPartitionTimestamp(partitionTimestamp) == activePartitionTimestamp) {
                continue;
            }
            if (!tableWriter.isPartitionParquet(partitionIndex)) {
                tableWriter.convertPartitionNativeToParquet(partitionTimestamp, null, Double.NaN);
                return;
            }
        }
    }

    private void putVarchar(TableWriter.Row row, int column, String value) {
        utf8sink.clear();
        utf8sink.put(value);
        row.putVarchar(column, utf8sink);
    }

    @Override
    protected boolean runSerially() {
        try {
            // Query tracing owns the writer for the lifetime of this job. Process non-structural
            // commands such as TTL changes that were published while the writer was busy.
            tableWriter.tick();
        } catch (Exception e) {
            LOG.error().$("Failed to process query trace table command").$(e).$();
        }

        buffer.clear();
        for (int i = 0; i < BATCH_LIMIT && queue.tryDequeue(buffer.peekNextHolder()); i++) {
            buffer.commitNextHolder();
        }
        if (buffer.size() == 0) {
            return false;
        }

        try {
            // Match server-assigned ingestion: all rows in this batch use the time at which
            // the tracing job persists them. Query start time is stored in a separate column.
            final long insertionTimestamp = timestampDriver.getTicks();
            for (int n = buffer.size(), i = 0; i < n; i++) {
                buffer.moveQuick(i, trace);
                final TableWriter.Row row = tableWriter.newRow(insertionTimestamp);
                putVarchar(row, queryTextColumnIndex, trace.queryText);
                row.putLong(executionMicrosColumnIndex, trace.executionNanos / Micros.MICRO_NANOS);
                putVarchar(row, principalColumnIndex, trace.principal);
                row.putTimestamp(queryStartColumnIndex, trace.queryStartTimestamp);
                row.append();
            }
            tableWriter.commit();
            trace.clear();
        } catch (Exception e) {
            LOG.error().$("Failed to save query trace").$(e).$();
            return false;
        }

        try {
            convertClosedPartitionToParquet();
        } catch (Exception e) {
            LOG.error().$("Failed to convert query trace partition to parquet").$(e).$();
        }
        return false;
    }
}
