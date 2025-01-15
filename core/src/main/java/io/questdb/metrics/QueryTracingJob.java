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
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ValueHolderList;
import io.questdb.std.str.Utf8StringSink;

public class QueryTracingJob extends SynchronizedJob {
    public static final String TABLE_NAME = "query_trace";
    private static final int BATCH_LIMIT = 1024;
    private static final int INITIAL_CAPACITY = 128;
    private static final Log LOG = LogFactory.getLog(QueryTracingJob.class.getName());
    private final ValueHolderList<QueryTrace> buffer;
    private final CairoEngine engine;
    private final MemCappedQueryTraceQueue queue;
    private final QueryTrace trace = new QueryTrace();
    private final Utf8StringSink utf8sink = new Utf8StringSink();
    private TableToken tableToken;

    public QueryTracingJob(CairoEngine engine) {
        this.queue = engine.getMessageBus().getQueryTraceQueue();
        this.buffer = new ValueHolderList<>(MemCappedQueryTraceQueue.ITEM_FACTORY, INITIAL_CAPACITY);
        this.engine = engine;
    }

    public static void assignToPool(WorkerPool pool, CairoEngine engine) {
        QueryTracingJob job = new QueryTracingJob(engine);
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            pool.assign(i, job);
        }
    }

    private void init() throws SqlException {
        String fullName = engine.getConfiguration().getSystemTableNamePrefix() + TABLE_NAME;
        engine.execute("CREATE TABLE IF NOT EXISTS '" + fullName +
                "' (ts TIMESTAMP, query_text VARCHAR, execution_micros LONG)" +
                " TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL");
        tableToken = engine.verifyTableName(fullName);
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
        TableWriter tableWriter0;
        try {
            tableWriter0 = engine.getWriter(tableToken, "query_tracing");
        } catch (Exception recoverable) {
            try {
                init();
                tableWriter0 = engine.getWriter(tableToken, "query_tracing");
            } catch (Exception e) {
                LOG.error().$("Failed to save query trace").$(e).$();
                return false;
            }
        }
        try (TableWriter tableWriter = tableWriter0) {
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
        }
        return false;
    }
}
