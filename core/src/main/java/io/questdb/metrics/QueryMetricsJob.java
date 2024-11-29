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
import io.questdb.mp.AbstractQueueBatchConsumerJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ValueHolderList;
import io.questdb.std.datetime.microtime.MicrosecondClock;

import java.util.concurrent.TimeUnit;

public class QueryMetricsJob extends AbstractQueueBatchConsumerJob<QueryMetrics> {
    public static final String TABLE_NAME = "_query_metrics_";
    private static final long CLEANUP_INTERVAL_MICROS = TimeUnit.SECONDS.toMicros(10);
    private static final Log LOG = LogFactory.getLog(QueryMetricsJob.class.getName());
    private static final long METRICS_LIFETIME_MICROS = TimeUnit.MINUTES.toMicros(61);

    private final MicrosecondClock clock;
    private final CairoEngine engine;
    private final QueryMetrics metrics = new QueryMetrics();
    private long lastCleanupTs;
    private TableToken tableToken;

    public QueryMetricsJob(CairoEngine engine) {
        super(engine.getMessageBus().getQueryMetricsQueue());
        this.engine = engine;
        this.clock = engine.getConfiguration().getMicrosecondClock();
    }

    public static void assignToPool(CairoEngine engine, WorkerPool pool) {
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            pool.assign(i, new QueryMetricsJob(engine));
        }
    }

    private void discardOldData() {
        final long now = clock.getTicks();
        if (now - lastCleanupTs > CLEANUP_INTERVAL_MICROS) {
            try {
                engine.execute("ALTER TABLE " + TABLE_NAME + " DROP PARTITION WHERE ts < " +
                        (clock.getTicks() - METRICS_LIFETIME_MICROS));
                lastCleanupTs = now;
            } catch (SqlException e) {
                LOG.error().$("Failed to discard old query metrics").$((Throwable) e).$();
            }
        }
    }

    private void init() throws SqlException {
        engine.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
                " (ts TIMESTAMP, query VARCHAR, execution_micros LONG)" +
                " TIMESTAMP(ts) PARTITION BY HOUR");
        tableToken = engine.verifyTableName(TABLE_NAME);
    }

    private void insertData(ValueHolderList<QueryMetrics> metricsList) {
        try (TableWriter tableWriter = engine.getWriter(tableToken, "query_tracing")) {
            for (int n = metricsList.size(), i = 0; i < n; i++) {
                metricsList.getQuick(i, metrics);
                final TableWriter.Row row = tableWriter.newRow(metrics.timestamp);
                row.putStr(1, metrics.queryText);
                row.putLong(2, metrics.executionNanos);
                row.append();
            }
            tableWriter.commit();
        }
    }

    @Override
    protected boolean doRun(int workerId, ValueHolderList<QueryMetrics> metricsList, RunStatus runStatus) {
        discardOldData();
        try {
            insertData(metricsList);
        } catch (Exception firstException) {
            try {
                init();
                insertData(metricsList);
            } catch (Exception e) {
                LOG.error().$("Failed to save query metrics").$(e).$();
            }
        }
        return false;
    }
}
