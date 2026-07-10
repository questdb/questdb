/*******************************************************************************
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

package io.questdb.test.cutlass.http;

import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Runs the HTTP server with fiber-mode query execution enabled: every connection
 * operation executes as a QueryTask on a pooled fiber mounted by the network
 * pool's workers, acquiring a request-processor selector per step. Exercises the
 * production shape end-to-end over a real socket: the dispatch job launches the
 * task, the fiber runs the JSON query, a sleep() query freezes the fiber on a
 * timer wait and resumes through the pool's continuation queue to finish the
 * response.
 */
public class HttpQueryFiberTest extends AbstractTest {

    @Test
    public void testQueriesRunOnPooledFibers() throws Exception {
        TestUtils.assertMemoryLeak(() -> new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withQueryFiberEnabled(true))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        // a plain query end-to-end on a fiber
                        testHttpClient.assertGet(
                                "{\"query\":\"select 42 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[42]],\"count\":1}",
                                "select 42 x"
                        );
                        // a parking query: sleep() freezes the fiber on a timer wait;
                        // the timer fires and the frozen fiber resumes through the
                        // network pool's continuation queue to finish the response
                        final long sleepStart = System.nanoTime();
                        testHttpClient.assertGet(
                                "{\"query\":\"select count() cnt from sleep(0.25)\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "select count() cnt from sleep(0.25)"
                        );
                        final long sleepMillis = (System.nanoTime() - sleepStart) / 1_000_000;
                        Assert.assertTrue("sleep completed too early: " + sleepMillis + "ms", sleepMillis >= 240);
                        // the same connection keeps reusing its task and the pooled fiber
                        testHttpClient.assertGet(
                                "{\"query\":\"select 43 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[43]],\"count\":1}",
                                "select 43 x"
                        );
                    }
                }));
    }
}
