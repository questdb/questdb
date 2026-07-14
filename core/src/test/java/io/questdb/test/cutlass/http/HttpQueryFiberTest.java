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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.TableWriter;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    public void testBusyWriterRetryLaunchesRerunOnFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withQueryFiberEnabled(true))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    final int insertCount = 4;
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (x LONG)");

                        final SOCountDownLatch inserted = new SOCountDownLatch(insertCount);
                        final AtomicReference<Throwable> insertError = new AtomicReference<>();
                        final ObjList<Thread> threads = new ObjList<>();
                        try (TableWriter ignore = engine.getWriter(engine.verifyTableName("tab"), "test")) {
                            // the held writer turns every INSERT into a busy-writer retry:
                            // the dispatch job parks it in the WaitProcessor, and each due
                            // rerun launches the connection's task on a pooled fiber,
                            // re-parking with growing backoff while the writer stays busy
                            for (int i = 0; i < insertCount; i++) {
                                final int value = i;
                                Thread thread = new Thread(() -> {
                                    try (TestHttpClient insertClient = new TestHttpClient()) {
                                        insertClient.assertGet("{\"dml\":\"OK\"}", "INSERT INTO tab VALUES (" + value + ")");
                                    } catch (Throwable th) {
                                        insertError.set(th);
                                    } finally {
                                        inserted.countDown();
                                    }
                                });
                                thread.start();
                                threads.add(thread);
                            }
                            // several backoff cycles pass; no insert can complete
                            Os.sleep(300);
                            Assert.assertEquals(insertCount, inserted.getCount());
                        }
                        Assert.assertTrue(
                                "inserts did not complete after writer release",
                                inserted.await(TimeUnit.SECONDS.toNanos(10))
                        );
                        for (int i = 0, n = threads.size(); i < n; i++) {
                            threads.getQuick(i).join();
                        }
                        Assert.assertNull(insertError.get());
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT count() cnt FROM tab\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[4]],\"count\":1}",
                                "SELECT count() cnt FROM tab"
                        );
                    }
                }));
    }

    @Test
    public void testClientDisconnectWhileRetryParkedOnFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withQueryFiberEnabled(true))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE tab (x LONG)");
                        try (TableWriter ignore = engine.getWriter(engine.verifyTableName("tab"), "test")) {
                            final long fd = new SendAndReceiveRequestBuilder().connectAndSendRequest(
                                    "GET /query?query=INSERT+INTO+tab+VALUES+(42) HTTP/1.1\r\n"
                                            + "Host: localhost:9001\r\n"
                                            + "\r\n"
                            );
                            // the INSERT parks in the retry queue while the writer is busy
                            Os.sleep(300);
                            // the client vanishes while its retry is parked; nothing
                            // observes the dead socket until the rerun touches it
                            Net.close(fd);
                        }
                        // the rerun after release either lands the insert and hits the
                        // dead socket on the response write, or the breaker trips first;
                        // both paths must reap the connection (the fd-leak check at
                        // teardown asserts the cleanup) and leave the server serving
                        Os.sleep(500);
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT 1 x\",\"columns\":[{\"name\":\"x\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "SELECT 1 x"
                        );
                    }
                }));
    }

    @Test
    public void testCsvImportRetryResumesMultipartOnFiber() throws Exception {
        TestUtils.assertMemoryLeak(() -> new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withQueryFiberEnabled(true))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    try (TestHttpClient testHttpClient = new TestHttpClient()) {
                        testHttpClient.assertGet("{\"ddl\":\"OK\"}", "CREATE TABLE test (a LONG)");

                        final String boundary = "----WebKitFormBoundaryOsOAD9cPKyHuxyBV";
                        final String body = "--" + boundary + "\r\n"
                                + "Content-Disposition: form-data; name=\"data\"\r\n"
                                + "\r\n"
                                + "1\r\n"
                                + "--" + boundary + "--\r\n";
                        final String importRequest = "POST /upload?fmt=json&name=test HTTP/1.1\r\n"
                                + "Host: localhost:9001\r\n"
                                + "Connection: keep-alive\r\n"
                                + "Content-Length: " + body.length() + "\r\n"
                                + "Content-Type: multipart/form-data; boundary=" + boundary + "\r\n"
                                + "\r\n"
                                + body;

                        final SOCountDownLatch imported = new SOCountDownLatch(1);
                        final AtomicReference<Throwable> importError = new AtomicReference<>();
                        Thread thread;
                        try (TableWriter ignore = engine.getWriter(engine.verifyTableName("test"), "test")) {
                            // the held writer suspends the import mid-multipart: the parser
                            // state is saved on the context and every due rerun launches on
                            // a fiber, resuming multipart consumption once the writer frees
                            thread = new Thread(() -> {
                                try {
                                    new SendAndReceiveRequestBuilder().execute(importRequest, "HTTP/1.1 200 OK");
                                } catch (Throwable th) {
                                    importError.set(th);
                                } finally {
                                    imported.countDown();
                                }
                            });
                            thread.start();
                            Os.sleep(300);
                            Assert.assertEquals(1, imported.getCount());
                        }
                        Assert.assertTrue(
                                "import did not complete after writer release",
                                imported.await(TimeUnit.SECONDS.toNanos(10))
                        );
                        thread.join();
                        Assert.assertNull(importError.get());
                        testHttpClient.assertGet(
                                "{\"query\":\"SELECT count() cnt FROM test\",\"columns\":[{\"name\":\"cnt\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}",
                                "SELECT count() cnt FROM test"
                        );
                    }
                }));
    }

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
