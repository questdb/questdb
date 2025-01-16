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

package io.questdb.metrics;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.ValueHolderList;
import io.questdb.std.str.Utf8StringSink;

import java.io.Closeable;
import java.io.IOException;

public class QueryTracingJob extends SynchronizedJob implements Closeable {
    public static final String COLUMN_EXECUTION_MICROS = "execution_micros";
    public static final String COLUMN_QUERY_TEXT = "query_text";
    public static final String COLUMN_TS = "ts";
    public static final String TABLE_NAME = "query_trace";
    private static final int BATCH_LIMIT = 1024;
    private static final int INITIAL_CAPACITY = 128;
    private static final Log LOG = LogFactory.getLog(QueryTracingJob.class.getName());
    private final ValueHolderList<QueryTrace> buffer;
    private final CairoEngine engine;
    private final MemCappedQueryTraceQueue queue;
    private final SqlExecutionContextImpl sqlExecutionContext;
    private final QueryTrace trace = new QueryTrace();
    private final Utf8StringSink utf8sink = new Utf8StringSink();
    private TableWriter tableWriter;


    public QueryTracingJob(CairoEngine engine) {
        this.queue = engine.getMessageBus().getQueryTraceQueue();
        this.buffer = new ValueHolderList<>(MemCappedQueryTraceQueue.ITEM_FACTORY, INITIAL_CAPACITY);
        this.engine = engine;
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null
        );
    }

    @Override
    public void close() throws IOException {
        if (tableWriter != null) {
            tableWriter.close();
        }
    }

    private void init() throws SqlException {
        String fullName = engine.getConfiguration().getSystemTableNamePrefix() + TABLE_NAME;
        TableToken tableToken;
        try {
            tableToken = engine.verifyTableName(fullName);
        } catch (Exception recoverable) {
            try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
                CompiledQuery query = sqlCompiler.query()
                        .$("CREATE TABLE IF NOT EXISTS '").$(fullName).$("' (")
                        .$(COLUMN_TS).$(" TIMESTAMP, ")
                        .$(COLUMN_QUERY_TEXT).$(" VARCHAR, ")
                        .$(COLUMN_EXECUTION_MICROS).$(" LONG")
                        .$(") TIMESTAMP(").$(COLUMN_TS).$(") PARTITION BY HOUR TTL 1 DAY BYPASS WAL")
                        .compile(sqlExecutionContext);
                query.getOperation().execute(sqlExecutionContext, null);
                tableToken = engine.verifyTableName(fullName);
            }
        }
        tableWriter = engine.getWriter(tableToken, "query_tracing");
    }

    @Override
    protected boolean runSerially() {
        buffer.clear();
        for (int i = 0; i < BATCH_LIMIT && queue.tryDequeue(buffer.peekNextHolder()); i++) {
            buffer.commitNextHolder();
        }
        if (buffer.size() <= 0) {
            return false;
        }
        try {
            if (tableWriter == null) {
                init();
            }
            for (int n = buffer.size(), i = 0; i < n; i++) {
                buffer.moveQuick(i, trace);
                final TableWriter.Row row = tableWriter.newRow(trace.timestamp);
                utf8sink.clear();
                utf8sink.put(trace.queryText);
                row.putVarchar(1, utf8sink);
                row.putLong(2, trace.executionNanos);
                row.append();
            }
            tableWriter.commit();
            trace.clear();
        } catch (Exception e) {
            LOG.error().$("Failed to save query trace").$(e).$();
        }
        return false;
    }
}
