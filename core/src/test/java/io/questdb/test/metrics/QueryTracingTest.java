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
import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.metrics.QueryTrace;
import io.questdb.metrics.QueryTracingJob;
import io.questdb.mp.WorkerPool;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.metrics.QueryTracingJob.*;

public class QueryTracingTest extends AbstractCairoTest {

    @Before
    public void setup() throws SqlException {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
        engine.execute("DROP TABLE IF EXISTS '" + TABLE_NAME + "'");
    }

    @Test
    public void testJobRecoversAfterAcquireFailure() throws Exception {
        final AtomicBoolean isMkdirsFailing = new AtomicBoolean(true);
        ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (isMkdirsFailing.get() && Utf8s.containsAscii(path, TABLE_NAME)) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        };
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            try (QueryTracingJob job = new QueryTracingJob(engine)) {
                enqueueTrace("SELECT 1");
                job.run();
                Assert.assertNull(engine.getTableTokenIfExists(TABLE_NAME));

                // clear the injected failure and step past the backoff
                isMkdirsFailing.set(false);
                setCurrentMicros(currentMicros + 2 * Micros.SECOND_MICROS);
                enqueueTrace("SELECT 2");
                job.run();

                final TableToken token = engine.getTableTokenIfExists(TABLE_NAME);
                Assert.assertNotNull(token);
            }
            assertQuery("SELECT " + COLUMN_QUERY_TEXT + " FROM '" + TABLE_NAME + "'")
                    .expectSize()
                    .returns(COLUMN_QUERY_TEXT + "\nSELECT 2\n");
        });
    }

    @Test
    public void testJobSurvivesWriterAcquireFailure() throws Exception {
        ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.containsAscii(path, TABLE_NAME)) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        };
        assertMemoryLeak(ff, () -> {
            setCurrentMicros(1_000_000L);
            // the constructor must not throw even though the table cannot be created
            try (QueryTracingJob job = new QueryTracingJob(engine)) {
                for (int i = 0; i < 4; i++) {
                    enqueueTrace("SELECT " + i);
                    Assert.assertFalse(job.run());
                }
                // the queue must be drained even though nothing could be written,
                // otherwise it grows without bound
                Assert.assertFalse(engine.getMessageBus().getQueryTraceQueue().tryDequeue(new QueryTrace()));
                Assert.assertNull(engine.getTableTokenIfExists(TABLE_NAME));
            }
        });
    }

    @Test
    public void testQueryTracing() throws Exception {
        try (WorkerPool workerPool = new WorkerPool(() -> 1);
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
                    assertQuery(String.format("SELECT %s, %s from %s WHERE %s='%s' LIMIT 1",
                            COLUMN_QUERY_TEXT,
                            COLUMN_PRINCIPAL,
                            TABLE_NAME,
                            COLUMN_QUERY_TEXT,
                            exampleQuery
                    ))
                            .noLeakCheck()
                            .returnsOnce(String.format("%s\t%s\n%s\tadmin\n", COLUMN_QUERY_TEXT, COLUMN_PRINCIPAL, exampleQuery));
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
    public void testTableNotCreatedWithoutTraces() throws Exception {
        // the table is created on the first trace, never at construction time. this is what makes
        // the hot-reloadable query.tracing.enabled flag gate the table: no traces are enqueued
        // while it is off, so nothing is ever created
        assertMemoryLeak(() -> {
            try (QueryTracingJob job = new QueryTracingJob(engine)) {
                Assert.assertFalse(job.run());
                Assert.assertNull(engine.getTableTokenIfExists(TABLE_NAME));
            }
        });
    }

    private static void enqueueTrace(String queryText) {
        final QueryTrace queryTrace = new QueryTrace();
        queryTrace.timestamp = currentMicros;
        queryTrace.queryText = queryText;
        queryTrace.principal = "admin";
        queryTrace.executionNanos = 1_000L;
        engine.getMessageBus().getQueryTraceQueue().enqueue(queryTrace);
    }
}
