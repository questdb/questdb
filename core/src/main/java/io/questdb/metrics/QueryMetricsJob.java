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
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueBatchConsumerJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.ValueHolderList;

public class QueryMetricsJob extends AbstractQueueBatchConsumerJob<QueryMetrics> {
    private static final Log LOG = LogFactory.getLog(QueryMetricsJob.class.getName());

    private final CairoEngine engine;
    private final QueryMetrics metrics = new QueryMetrics();

    public QueryMetricsJob(CairoEngine engine) {
        super(engine.getMessageBus().getQueryMetricsQueue());
        this.engine = engine;
    }

    public static void assignToPool(CairoEngine engine, WorkerPool pool) {
        for (int i = 0, n = pool.getWorkerCount(); i < n; i++) {
            pool.assign(i, new QueryMetricsJob(engine));
        }
    }

    private void init() throws SqlException {
        engine.execute("CREATE TABLE IF NOT EXISTS _query_metrics_ (ts TIMESTAMP, query VARCHAR, execution_micros LONG)");
    }

    @Override
    protected boolean canRun() {
        return engine.getConfiguration().isQueryMetricsEnabled();
    }

    @Override
    protected boolean doRun(int workerId, ValueHolderList<QueryMetrics> metricsList, RunStatus runStatus) {
        StringBuilder b = new StringBuilder("INSERT INTO _query_metrics_ VALUES ");
        String separator = "";
        for (int n = metricsList.size(), i = 0; i < n; i++) {
            metricsList.getQuick(i, metrics);
            b.append(separator).append('(')
                    .append(metrics.timestamp).append(", '")
                    .append(metrics.queryText).append("', ")
                    .append(metrics.executionNanos).append(')');
            separator = ", ";
        }
        try {
            engine.execute(b);
        } catch (SqlException e) {
            try {
                init();
                engine.execute(b);
            } catch (SqlException ex) {
                LOG.error().$("Failed to save query metrics").$((Throwable) e).$();
            }
        }
        return false;
    }
}
