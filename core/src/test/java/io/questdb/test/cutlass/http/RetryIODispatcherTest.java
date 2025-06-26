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

package io.questdb.test.cutlass.http;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableWriter;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Os;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.assertEventually;
import static io.questdb.test.tools.TestUtils.getSendDelayNetworkFacade;

// These tests verify retry behaviour of IODispatcher and HttpConnectionContext.
// They run the same test multiple times in order to test concurrent retry executions.
// If a test becomes unstable (fails sometimes on build server or local run), increase number of iterations
// to reproduce the failure.
public class RetryIODispatcherTest extends AbstractTest {
    private static final String ValidImportRequest = "POST /upload HTTP/1.1\r\n" +
            "Host: localhost:9001\r\n" +
            "User-Agent: curl/7.64.0\r\n" +
            "Accept: */*\r\n" +
            "Content-Length: 437760673\r\n" +
            "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
            "Expect: 100-continue\r\n" +
            "\r\n" +
            "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "[\r\n" +
            "  {\r\n" +
            "    \"name\": \"date\",\r\n" +
            "    \"type\": \"DATE\",\r\n" +
            "    \"pattern\": \"d MMMM y.\",\r\n" +
            "    \"locale\": \"ru-RU\"\r\n" +
            "  }\r\n" +
            "]\r\n" +
            "\r\n" +
            "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "Dispatching_base_num,Pickup_DateTime,DropOff_datetime,PUlocationID,DOlocationID\r\n" +
            "B00008,2017-02-01 00:30:00,,,\r\n" +
            "B00008,2017-02-01 00:40:00,,,\r\n" +
            "B00009,2017-02-01 00:30:00,,,\r\n" +
            "B00013,2017-02-01 00:11:00,,,\r\n" +
            "B00013,2017-02-01 00:41:00,,,\r\n" +
            "B00013,2017-02-01 00:00:00,,,\r\n" +
            "B00013,2017-02-01 00:53:00,,,\r\n" +
            "B00013,2017-02-01 00:44:00,,,\r\n" +
            "B00013,2017-02-01 00:05:00,,,\r\n" +
            "B00013,2017-02-01 00:54:00,,,\r\n" +
            "B00014,2017-02-01 00:45:00,,,\r\n" +
            "B00014,2017-02-01 00:45:00,,,\r\n" +
            "B00014,2017-02-01 00:46:00,,,\r\n" +
            "B00014,2017-02-01 00:54:00,,,\r\n" +
            "B00014,2017-02-01 00:45:00,,,\r\n" +
            "B00014,2017-02-01 00:45:00,,,\r\n" +
            "B00014,2017-02-01 00:45:00,,,\r\n" +
            "B00014,2017-02-01 00:26:00,,,\r\n" +
            "B00014,2017-02-01 00:55:00,,,\r\n" +
            "B00014,2017-02-01 00:47:00,,,\r\n" +
            "B00014,2017-02-01 00:05:00,,,\r\n" +
            "B00014,2017-02-01 00:58:00,,,\r\n" +
            "B00014,2017-02-01 00:33:00,,,\r\n" +
            "B00014,2017-02-01 00:45:00,,,\r\n" +
            "\r\n" +
            "--------------------------27d997ca93d2689d--";

    private final String ValidImportResponse = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "\r\n" +
            "0666\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|      Location:  |                          fhv_tripdata_2017-02.csv  |        Pattern  | Locale  |      Errors  |\r\n" +
            "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
            "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|   Rows handled  |                                                24  |                 |         |              |\r\n" +
            "|  Rows imported  |                                                24  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|              0  |                              Dispatching_base_num  |                  VARCHAR  |           0  |\r\n" +
            "|              1  |                                   Pickup_DateTime  |                     DATE  |           0  |\r\n" +
            "|              2  |                                  DropOff_datetime  |                  VARCHAR  |           0  |\r\n" +
            "|              3  |                                      PUlocationID  |                  VARCHAR  |           0  |\r\n" +
            "|              4  |                                      DOlocationID  |                  VARCHAR  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @Test
    public void testFailsWhenInvalidDataImportedLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(root)
                            .withWorkerCount(2)
                            .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                            .withCustomTextImportProcessor(((configuration, engine, workerCount) -> new TextImportProcessor(engine, configuration) {
                                @Override
                                public void onRequestRetry(HttpConnectionContext context) throws ServerDisconnectException {
                                    throw ServerDisconnectException.INSTANCE;
                                }
                            })),
                    0, ValidImportRequest, ValidImportResponse, false, true
            );
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testImportProcessedWhenClientDisconnectedLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            assertImportProcessedWhenClientDisconnected();
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    public void testImportRerunsExceedsRerunProcessingQueueSize(int startDelay) throws Exception {
        final int rerunProcessingQueueSize = 1;
        final int parallelCount = 4;

        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(getSendDelayNetworkFacade(startDelay))
                                .withRerunProcessingQueueSize(rerunProcessingQueueSize)
                )
                .run((engine, sqlExecutionContext) -> {
                    // create table and do 1 import
                    new SendAndReceiveRequestBuilder().execute(ValidImportRequest, ValidImportResponse);
                    TableWriter writer = lockWriter(engine, "fhv_tripdata_2017-02.csv");
                    final int validRequestRecordCount = 24;
                    final int insertCount = 4;
                    AtomicInteger failedImports = new AtomicInteger();
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    for (int i = 0; i < parallelCount; i++) {
                        int finalI = i;
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        new SendAndReceiveRequestBuilder()
                                                .execute(ValidImportRequest, ValidImportResponse);
                                    } catch (AssertionError e) {
                                        LOG.info().$("Server call succeeded but response is different from the expected one").$();
                                        failedImports.incrementAndGet();
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                                    }
                                }
                            } finally {
                                LOG.info().$("Stopped thread ").$(finalI).$();
                                countDownLatch.countDown();
                            }
                        }).start();
                    }

                    boolean finished = countDownLatch.await(100, TimeUnit.MILLISECONDS);
                    Assert.assertFalse(finished);

                    writer.close();
                    countDownLatch.await();

                    // check if we have parallelCount x insertCount records
                    LOG.info().$("Requesting row count").$();
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                            "93\r\n" +
                                    "{\"query\":\"select count(*) from \\\"fhv_tripdata_2017-02.csv\\\"\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + (parallelCount * insertCount + 1 - failedImports.get()) * validRequestRecordCount + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });
    }

    @Test
    public void testImportRerunsExceedsRerunProcessingQueueSizeLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            testImportRerunsExceedsRerunProcessingQueueSize(1000);
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    public void testImportWaitsWhenWriterLocked(
            HttpQueryTestBuilder httpQueryTestBuilder,
            int slowServerReceiveNetAfterSending,
            String importRequest,
            String importResponse,
            boolean failOnUnfinished,
            boolean allowFailures
    ) throws Exception {
        final int parallelCount = httpQueryTestBuilder.getWorkerCount();
        httpQueryTestBuilder
                .run((engine, sqlExecutionContext) -> {
                    // create table and do 1 import
                    new SendAndReceiveRequestBuilder().execute(ValidImportRequest, ValidImportResponse);

                    TableWriter writer = lockWriter(engine, "fhv_tripdata_2017-02.csv");
                    final int validRequestRecordCount = 24;
                    final int insertCount = 1;
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    AtomicInteger successRequests = new AtomicInteger();
                    for (int i = 0; i < parallelCount; i++) {
                        int finalI = i;
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        SendAndReceiveRequestBuilder sendAndReceiveRequestBuilder = new SendAndReceiveRequestBuilder()
                                                .withNetworkFacade(getSendDelayNetworkFacade(slowServerReceiveNetAfterSending))
                                                .withCompareLength(importResponse.length())
                                                .withClientLinger(60);
                                        sendAndReceiveRequestBuilder
                                                .execute(importRequest, importResponse);
                                        successRequests.incrementAndGet();
                                    } catch (AssertionError e) {
                                        if (allowFailures) {
                                            LOG.info().$("Failed execute insert http request. Comparison failed").$();
                                        } else {
                                            LOG.error().$("Failed execute insert http request. Comparison failed").$(e).$();
                                        }
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request.").$(e).$();
                                    }
                                }
                            } finally {
                                LOG.info().$("Stopped thread ").$(finalI).$();
                                countDownLatch.countDown();
                            }
                        }).start();
                    }

                    boolean finished = countDownLatch.await(100, TimeUnit.MILLISECONDS);

                    if (failOnUnfinished) {
                        // Cairo engine should not allow second writer to be opened on the same table
                        // Cairo is expected to have finished == false
                        Assert.assertFalse(finished);
                    }

                    writer.close();
                    if (!countDownLatch.await(50000, TimeUnit.MILLISECONDS)) {
                        Assert.fail("Imports did not finish within reasonable time");
                    }

                    if (!allowFailures) {
                        Assert.assertEquals(parallelCount, successRequests.get());
                    }

                    // check if we have parallelCount x insertCount  records
                    LOG.info().$("Requesting row count").$();
                    int rowsExpected = (successRequests.get() + 1) * validRequestRecordCount;
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                            (rowsExpected < 100 ? "92" : "93") + "\r\n" +
                                    "{\"query\":\"select count(*) from \\\"fhv_tripdata_2017-02.csv\\\"\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + rowsExpected + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                });
    }

    @Test
    public void testImportWaitsWhenWriterLockedLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(root)
                            .withWorkerCount(4)
                            .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder()
                                    .withNetwork(getSendDelayNetworkFacade(500))
                                    .withMultipartIdleSpinCount(10)
                            ),
                    500, ValidImportRequest, ValidImportResponse
                    , true, false
            );
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testImportWaitsWhenWriterLockedWithSlowPeerLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println(
                    "*************************************************************************************\n" +
                            "**************************         Run " + i + "            ********************************\n" +
                            "*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(root)
                            .withWorkerCount(2)
                            .withHttpServerConfigBuilder(
                                    new HttpServerConfigurationBuilder().withNetwork(getSendDelayNetworkFacade(500))
                            ),
                    0, ValidImportRequest, ValidImportResponse, true, true
            );
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testImportsCreateAsSelectAndDrop() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(4)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    for (int i = 0; i < 10; i++) {
                        System.out.println("*************************************************************************************\n" +
                                "**************************         Run " + i + "            ********************************\n" +
                                "*************************************************************************************");
                        SendAndReceiveRequestBuilder sendAndReceiveRequestBuilder = new SendAndReceiveRequestBuilder()
                                .withNetworkFacade(getSendDelayNetworkFacade(0))
                                .withCompareLength(ValidImportResponse.length());
                        sendAndReceiveRequestBuilder.execute(ValidImportRequest, ValidImportResponse);

                        if (i == 0) {
                            new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                    "GET /query?query=create+table+copy+as+(select+*+from+%22fhv_tripdata_2017-02.csv%22)&count=true HTTP/1.1\r\n",
                                    IODispatcherTest.JSON_DDL_RESPONSE
                            );
                        } else {
                            new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                    "GET /query?query=insert+into+copy+select+*+from+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                                    IODispatcherTest.INSERT_QUERY_RESPONSE
                            );
                        }

                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                "GET /query?query=drop+table+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                                IODispatcherTest.JSON_DDL_RESPONSE
                        );
                    }
                });
    }

    @Test
    public void testImportsHeaderIsNotFullyReceivedIntoReceiveBuffer() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withReceiveBufferSize(50)
                ).run((engine, sqlExecutionContext) -> new SendAndReceiveRequestBuilder()
                        .execute(
                                ValidImportRequest,
                                "HTTP/1.1 200 OK\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Transfer-Encoding: chunked\r\n" +
                                        "Content-Type: text/plain; charset=utf-8\r\n" +
                                        "\r\n" +
                                        "58\r\n" +
                                        "cannot parse import because of receive buffer is not big enough to parse table structure\r\n" +
                                        "00\r\n" +
                                        "\r\n"
                        )
                );
    }

    @Test
    public void testImportsWhenReceiveBufferIsSmallAndSenderSlow() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(root)
                            .withWorkerCount(2)
                            .withHttpServerConfigBuilder(
                                    new HttpServerConfigurationBuilder()
                                            .withReceiveBufferSize(256)
                            ),
                    200, ValidImportRequest, ValidImportResponse, false, true
            );
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testInsertWaitsExceedsRerunProcessingQueueSizeLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            assertInsertWaitsExceedsRerunProcessingQueueSize();
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testInsertWaitsWhenWriterLockedLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************\n" +
                    "**************************         Run " + i + "            ********************************\n" +
                    "*************************************************************************************");
            assertInsertWaitsWhenWriterLocked();
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testInsertsIsPerformedWhenWriterLockedAndDisconnectedLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println(
                    "*************************************************************************************\n" +
                            "**************************         Run " + i + "            ********************************\n" +
                            "*************************************************************************************");
            assertInsertsIsPerformedWhenWriterLockedAndDisconnected();
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
            Metrics.ENABLED.clear();
        }
    }

    @Test
    public void testRenameWaitsWhenWriterLocked() throws Exception {
        final int parallelCount = 2;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    // create table
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                            IODispatcherTest.JSON_DDL_RESPONSE
                    );

                    TableWriter writer = lockWriter(engine, "balances_x");
                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    new Thread(() -> {
                        try {
                            try {
                                // Rename table
                                new SendAndReceiveRequestBuilder()
                                        .withClientLinger(60)
                                        .executeWithStandardHeaders(
                                                "GET /query?query=rename+table+%27balances_x%27+to+%27balances_y%27&limit=0%2C1000&count=true HTTP/1.1\r\n",
                                                IODispatcherTest.JSON_DDL_RESPONSE
                                        );
                            } catch (Exception e) {
                                LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                            }
                        } finally {
                            LOG.info().$("Stopped rename table thread").$();
                            countDownLatch.countDown();
                        }
                    }).start();
                    boolean finished = countDownLatch.await(200, TimeUnit.MILLISECONDS);

                    // Cairo engine should not allow table rename while writer is opened
                    // Cairo is expected to have finished == false
                    Assert.assertFalse(finished);

                    writer.close();
                    Assert.assertTrue(
                            "Table rename did not complete within timeout after writer is released",
                            countDownLatch.await(5, TimeUnit.SECONDS)
                    );
                });
    }

    private void assertImportProcessedWhenClientDisconnected() throws Exception {
        final int parallelCount = 2;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    // create table and do 1 import
                    new SendAndReceiveRequestBuilder().execute(ValidImportRequest, ValidImportResponse);

                    TableWriter writer = lockWriter(engine, "fhv_tripdata_2017-02.csv");

                    final int validRequestRecordCount = 24;
                    final int insertCount = 1;
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    long[] fds = new long[parallelCount * insertCount];
                    Arrays.fill(fds, -1);
                    for (int i = 0; i < parallelCount; i++) {
                        final int threadI = i;
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        long fd = new SendAndReceiveRequestBuilder().connectAndSendRequest(ValidImportRequest);
                                        fds[threadI * insertCount + r] = fd;
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                                    }
                                }
                            } finally {
                                LOG.info().$("Stopped thread ").$(threadI).$();
                                countDownLatch.countDown();
                            }
                        }).start();
                    }
                    countDownLatch.await();
                    assertNRowsInserted(validRequestRecordCount);

                    for (long fd : fds) {
                        Assert.assertNotEquals(-1, fd);
                        NetworkFacadeImpl.INSTANCE.close(fd);
                    }

                    // Cairo engine should not allow second writer to be opened on the same table, all requests should wait for the writer to be available
                    writer.close();

                    int nRows = (parallelCount + 1) * validRequestRecordCount;
                    assertEventually(() -> assertNRowsInserted(nRows));
                });
    }

    private void assertInsertWaitsExceedsRerunProcessingQueueSize() throws Exception {
        final int rerunProcessingQueueSize = 1;
        final int parallelCount = 4;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withRerunProcessingQueueSize(rerunProcessingQueueSize))
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    // create table
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                            IODispatcherTest.JSON_DDL_RESPONSE
                    );

                    TableWriter writer = lockWriter(engine, "balances_x");

                    final int insertCount = rerunProcessingQueueSize * 10;
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    AtomicInteger fails = new AtomicInteger();
                    for (int i = 0; i < parallelCount; i++) {
                        final int threadI = i;
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        new SendAndReceiveRequestBuilder()
                                                .withClientLinger(60)
                                                .executeWithStandardHeaders(
                                                        "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(1%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                                                        IODispatcherTest.INSERT_QUERY_RESPONSE
                                                );
                                    } catch (AssertionError ase) {
                                        fails.incrementAndGet();
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e);
                                    }
                                }
                            } finally {
                                LOG.info().$("Stopped thread ").$(threadI).$();
                                countDownLatch.countDown();
                            }
                        }).start();
                    }

                    boolean finished = countDownLatch.await(200, TimeUnit.MILLISECONDS);
                    Assert.assertFalse(finished);

                    writer.close();
                    if (!countDownLatch.await(5000, TimeUnit.MILLISECONDS)) {
                        Assert.fail("Wait to process retries exceeded timeout");
                    }

                    // check if we have parallelCount x insertCount  records
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+balances_x&count=true HTTP/1.1\r\n",
                            "80\r\n" +
                                    "{\"query\":\"select count(*) from balances_x\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + (parallelCount * insertCount - fails.get()) + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });
    }

    private void assertInsertWaitsWhenWriterLocked() throws Exception {
        final int parallelCount = 2;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    // create table
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                            IODispatcherTest.JSON_DDL_RESPONSE
                    );

                    TableWriter writer = lockWriter(engine, "balances_x");

                    final int insertCount = 10;
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    for (int i = 0; i < parallelCount; i++) {
                        final int threadI = i;
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        new SendAndReceiveRequestBuilder()
                                                .withClientLinger(60)
                                                .executeWithStandardHeaders(
                                                        "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(1%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                                                        IODispatcherTest.INSERT_QUERY_RESPONSE
                                                );
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                                    }
                                }
                            } finally {
                                LOG.info().$("Stopped thread ").$(threadI).$();
                                countDownLatch.countDown();
                            }
                        }).start();
                    }

                    boolean finished = countDownLatch.await(200, TimeUnit.MILLISECONDS);

                    // Cairo engine should not allow second writer to be opened on the same table
                    // Cairo is expected to have finished == false
                    Assert.assertFalse(finished);

                    writer.close();
                    countDownLatch.await();

                    // check if we have parallelCount x insertCount  records
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+balances_x&count=true HTTP/1.1\r\n",
                            "80\r\n" +
                                    "{\"query\":\"select count(*) from balances_x\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + parallelCount * insertCount + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });
    }

    private void assertInsertsIsPerformedWhenWriterLockedAndDisconnected() throws Exception {
        final int parallelCount = 4;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    long nonInsertQueries = 0;

                    // create table
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                            IODispatcherTest.JSON_DDL_RESPONSE
                    );
                    nonInsertQueries++;

                    TableWriter writer = lockWriter(engine, "balances_x");
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    long[] fds = new long[parallelCount];
                    Arrays.fill(fds, -1);
                    Thread[] threads = new Thread[parallelCount];
                    for (int i = 0; i < parallelCount; i++) {
                        final int threadI = i;
                        threads[i] = new Thread(() -> {
                            try {
                                // insert one record
                                // await nothing
                                try {
                                    Os.sleep(threadI * 5);
                                    String request = "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(" + threadI +
                                            "%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n" + SendAndReceiveRequestBuilder.RequestHeaders;
                                    long fd = new SendAndReceiveRequestBuilder()
                                            .withClientLinger(60)
                                            .connectAndSendRequest(request);
                                    fds[threadI] = fd;
                                } catch (Exception e) {
                                    LOG.error().$("Failed execute insert http request. Server error ").$(e);
                                }
                            } finally {
                                LOG.info().$("Stopped thread ").$(threadI).$();
                                countDownLatch.countDown();
                            }
                        });
                        threads[i].start();
                    }
                    countDownLatch.await();

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=SELECT+1 HTTP/1.1\r\n",
                            "63\r\n" +
                                    "{\"query\":\"SELECT 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                    nonInsertQueries++;

                    final int maxWaitTimeMillis = 6000;
                    final int sleepMillis = 10;

                    final Metrics metrics = engine.getMetrics();
                    // wait for all insert queries to be initially handled
                    long startedInserts;
                    for (int i = 0; i < maxWaitTimeMillis / sleepMillis; i++) {
                        startedInserts = metrics.jsonQueryMetrics().startedQueriesCount() - nonInsertQueries;
                        if (startedInserts >= parallelCount) {
                            break;
                        }
                        Os.sleep(sleepMillis);
                    }
                    startedInserts = metrics.jsonQueryMetrics().startedQueriesCount() - nonInsertQueries;
                    Assert.assertTrue(
                            "expected at least " + parallelCount + "insert attempts, but got: " + startedInserts,
                            startedInserts >= parallelCount
                    );

                    for (int n = 0; n < fds.length; n++) {
                        Assert.assertNotEquals(-1, fds[n]);
                        NetworkFacadeImpl.INSTANCE.close(fds[n]);
                    }

                    writer.close();

                    // wait for all insert queries to be executed
                    long completeInserts;
                    for (int i = 0; i < maxWaitTimeMillis / sleepMillis; i++) {
                        completeInserts = metrics.jsonQueryMetrics().completedQueriesCount() - nonInsertQueries;
                        if (completeInserts == parallelCount) {
                            break;
                        }
                        Os.sleep(sleepMillis);
                    }
                    completeInserts = metrics.jsonQueryMetrics().completedQueriesCount() - nonInsertQueries;
                    Assert.assertEquals("expected all inserts to succeed", parallelCount, completeInserts);

                    // check that we have all the records inserted
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count()+from+balances_x&count=true HTTP/1.1\r\n",
                            "7e\r\n" +
                                    "{\"query\":\"select count() from balances_x\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + parallelCount + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });
    }

    @NotNull
    private TableWriter lockWriter(CairoEngine engine, String tableName) {
        TableWriter writer = null;
        for (int i = 0; i < 10; i++) {
            try {
                writer = TestUtils.getWriter(engine, tableName);
                break;
            } catch (EntryUnavailableException e) {
                Os.sleep(10);
            }
        }

        if (writer == null) {
            Assert.fail("Cannot lock writer in a reasonable time");
        }
        return writer;
    }

    protected void assertNRowsInserted(final int nRows) {
        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                "GET /query?query=select+count(*)+from+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                "92\r\n" +
                        "{\"query\":\"select count(*) from \\\"fhv_tripdata_2017-02.csv\\\"\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[" + nRows +
                        "]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n"
        );
    }
}
