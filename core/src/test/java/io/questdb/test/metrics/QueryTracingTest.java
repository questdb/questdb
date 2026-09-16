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

package io.questdb.test.metrics;


import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.metrics.QueryTrace;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.SCSequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.metrics.QueryTracingJob.*;

public class QueryTracingTest extends AbstractCairoTest {

    private static void enqueueTrace(long queryStartTimestamp, String queryText) {
        final QueryTrace trace = new QueryTrace();
        trace.executionNanos = 1_000;
        trace.principal = "admin";
        trace.queryStartTimestamp = queryStartTimestamp;
        trace.queryText = queryText;
        engine.getMessageBus().getQueryTraceQueue().enqueue(trace);
    }

    @Before
    public void setup() throws SqlException {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
        engine.getMessageBus().getQueryTraceQueue().clear();
        engine.execute("DROP TABLE IF EXISTS '" + TABLE_NAME + "'");
    }

    @Test
    public void testQueryTracing() throws Exception {
        try (WorkerPool workerPool = new TestWorkerPool(1);
             QueryTracingJob job = new QueryTracingJob(engine)
        ) {
            workerPool.assign(job);
            workerPool.start(LOG);
            String exampleQuery = "SELECT table_name FROM tables()";
            assertQuery(exampleQuery)
                    .noLeakCheck()
                    .returnsOnce("table_name\n");
            int sleepMillis = 100;
            while (true) {
                Thread.sleep(sleepMillis);
                try {
                    assertQuery(String.format("SELECT %s, %s, %s is not null started from %s WHERE %s='%s' LIMIT 1",
                            COLUMN_QUERY_TEXT,
                            COLUMN_PRINCIPAL,
                            COLUMN_QUERY_START,
                            TABLE_NAME,
                            COLUMN_QUERY_TEXT,
                            exampleQuery
                    ))
                            .noLeakCheck()
                            .returnsOnce(String.format("%s\t%s\tstarted\n%s\tadmin\ttrue\n", COLUMN_QUERY_TEXT, COLUMN_PRINCIPAL, exampleQuery));
                    break;
                } catch (SqlException | AssertionError e) {
                    if (sleepMillis >= 6400) {
                        throw e;
                    }
                    sleepMillis *= 2;
                }
            }
        }
    }

    @Test
    public void testQueryTracingConvertsClosedPartitionsToParquet() throws Exception {
        final long currentHour = Micros.floorHH(engine.getConfiguration().getMicrosecondClock().getTicks());
        final long firstHour = currentHour - 2 * Micros.HOUR_MICROS;
        engine.execute(
                "CREATE TABLE '" + TABLE_NAME + "' (" +
                        COLUMN_TS + " TIMESTAMP, " +
                        COLUMN_QUERY_TEXT + " VARCHAR, " +
                        COLUMN_EXECUTION_MICROS + " LONG, " +
                        COLUMN_PRINCIPAL + " VARCHAR, " +
                        COLUMN_QUERY_START + " TIMESTAMP" +
                        ") TIMESTAMP(" + COLUMN_TS + ") PARTITION BY HOUR TTL 1 DAY BYPASS WAL"
        );
        engine.execute(
                "INSERT INTO '" + TABLE_NAME + "' VALUES " +
                        "(cast(" + firstHour + " as timestamp), 'select 1', 1, 'admin', cast(" + firstHour + " as timestamp)), " +
                        "(cast(" + (firstHour + Micros.HOUR_MICROS) + " as timestamp), 'select 2', 1, 'admin', cast(" + (firstHour + Micros.HOUR_MICROS) + " as timestamp))"
        );

        try (QueryTracingJob job = new QueryTracingJob(engine)) {
            enqueueTrace(currentHour, "select 3");
            job.run();
            enqueueTrace(firstHour + Micros.HOUR_MICROS - 1, "late select");
            job.run();
        }

        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertEquals(3, reader.getPartitionCount());
            Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(0));
            Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(1));
            Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormat(2));
        }

        assertQuery("select query_text from " + TABLE_NAME)
                .returnsOnce(
                        "query_text\n" +
                                "select 1\n" +
                                "select 2\n" +
                                "select 3\n" +
                                "late select\n"
                );
        assertQuery(
                "select count() from " + TABLE_NAME +
                        " where query_text = 'late select'" +
                        " and query_start = cast(" + (firstHour + Micros.HOUR_MICROS - 1) + " as timestamp)" +
                        " and ts > query_start"
        ).returnsOnce("count\n1\n");
    }

    @Test
    public void testQueryStartTimestampDoesNotUseO3() throws Exception {
        node1.getConfigurationOverrides().setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
        final long currentHour = Micros.floorHH(engine.getConfiguration().getMicrosecondClock().getTicks());
        try (QueryTracingJob job = new QueryTracingJob(engine)) {
            for (int i = 0; i < 128; i++) {
                enqueueTrace(currentHour + i * 1_000L, "select " + i);
                job.run();
            }

            enqueueTrace(currentHour + 120_500L, "late query");
            job.run();
        }

        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertEquals(1, reader.getPartitionCount());
        }
    }

    @Test
    public void testQueryTracingMigratesExistingTable() throws Exception {
        engine.execute(
                "CREATE TABLE '" + TABLE_NAME + "' (" +
                        COLUMN_TS + " TIMESTAMP, " +
                        COLUMN_QUERY_TEXT + " VARCHAR, " +
                        COLUMN_EXECUTION_MICROS + " LONG, " +
                        COLUMN_PRINCIPAL + " VARCHAR" +
                        ") TIMESTAMP(" + COLUMN_TS + ") PARTITION BY HOUR TTL 1 DAY BYPASS WAL"
        );

        try (QueryTracingJob ignored = new QueryTracingJob(engine)) {
            // Opening the job upgrades the existing table while it owns the writer.
        }

        assertQuery("select \"column\", type from table_columns('" + TABLE_NAME + "') where \"column\" = '" + COLUMN_QUERY_START + "'")
                .returnsOnce("column\ttype\n" + COLUMN_QUERY_START + "\tTIMESTAMP\n");
    }

    @Test
    public void testQueryTracingProcessesTtlChange() throws Exception {
        try (
                QueryTracingJob job = new QueryTracingJob(engine);
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            final CompiledQuery query = compiler.compile("alter table " + TABLE_NAME + " set ttl 2 hours", sqlExecutionContext);
            try (OperationFuture future = query.execute(new SCSequence())) {
                Assert.assertEquals(OperationFuture.QUERY_NO_RESPONSE, future.await(0));
                job.run();
                future.await();
            }
        }

        final TableToken tableToken = engine.verifyTableName(TABLE_NAME);
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertEquals(2, reader.getMetadata().getTtlHoursOrMonths());
        }
    }
}
