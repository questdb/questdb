/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.http.processors.TextImportProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.ServerDisconnectException;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// These test the retry behaviour of IODispatcher, HttpConnectionContext
// They run the same test multiple times in order to test concurrent retry executions
// If a test becomes unstable (fails sometimes on build server or local run), increase number of iterations
// to reproduce failure.
public class RetryIODispatcherTest {
    private static final Log LOG = LogFactory.getLog(RetryIODispatcherTest.class);

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
            "|              0  |                              Dispatching_base_num  |                   STRING  |           0  |\r\n" +
            "|              1  |                                   Pickup_DateTime  |                     DATE  |           0  |\r\n" +
            "|              2  |                                  DropOff_datetime  |                   STRING  |           0  |\r\n" +
            "|              3  |                                      PUlocationID  |                   STRING  |           0  |\r\n" +
            "|              4  |                                      DOlocationID  |                   STRING  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testImportProcessedWhenClientDisconnectedLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            assertImportProcessedWhenClientDisconnected();
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testInsertWaitsExceedsRerunProcessingQueueSizeLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            assertInsertWaitsExceedsRerunProcessingQueueSize();
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testImportWaitsWhenWriterLockedLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(temp)
                            .withWorkerCount(4)
                            .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder()
                                    .withNetwork(getSendDelayNetworkFacade(500))
                                    .withMultipartIdleSpinCount(10)
                            ),
                    500, ValidImportRequest, ValidImportResponse
                    , true, false);
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testInsertWaitsWhenWriterLockedLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            assertInsertWaitsWhenWriterLocked();
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testInsertsIsPerformedWhenWriterLockedAndDisconnectedLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            assertInsertsIsPerformedWhenWriterLockedAndDisconnected();
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testImportWaitsWhenWriterLockedWithSlowPeerLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(temp)
                            .withWorkerCount(2)
                            .withHttpServerConfigBuilder(
                                    new HttpServerConfigurationBuilder().withNetwork(getSendDelayNetworkFacade(500))
                            ),
                    0, ValidImportRequest, ValidImportResponse, true, true);
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testFailsWhenInvalidDataImportedLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(temp)
                            .withWorkerCount(2)
                            .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                            .withCustomTextImportProcessor(((configuration, engine, messageBus, workerCount) -> new TextImportProcessor(engine) {
                                @Override
                                public void onRequestRetry(HttpConnectionContext context) throws ServerDisconnectException {
                                    throw ServerDisconnectException.INSTANCE;
                                }
                            })),
                    0, ValidImportRequest, ValidImportResponse, false, true);
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testImportsWhenReceiveBufferIsSmallAndSenderSlow() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWaitsWhenWriterLocked(new HttpQueryTestBuilder()
                            .withTempFolder(temp)
                            .withWorkerCount(2)
                            .withHttpServerConfigBuilder(
                                    new HttpServerConfigurationBuilder()
                                            .withReceiveBufferSize(256)
                            ),
                    200, ValidImportRequest, ValidImportResponse, false, true);
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testImportsHeaderIsNotFullyReceivedIntoReceiveBuffer() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withReceiveBufferSize(50)
                ).run((engine) -> new SendAndReceiveRequestBuilder()
                .withExpectDisconnect(true)
                .execute(ValidImportRequest,
                "HTTP/1.1 200 OK\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: text/plain; charset=utf-8\r\n" +
                        "\r\n" +
                        "58\r\n" +
                        "cannot parse import because of receive buffer is not big enough to parse table structure\r\n" +
                        "00\r\n" +
                        "\r\n"));
    }

    @Test
    public void testImportRerunsExceedsRerunProcessingQueueSizeLoop() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportRerunsExceedsRerunProcessingQueueSize(1000);
            temp.delete();
            temp.create();
        }
    }

    @Test
    public void testImportsCreateAsSelectAndDrop() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(4)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
                    for (int i = 0; i < 10; i++) {
                        System.out.println("*************************************************************************************");
                        System.out.println("**************************         Run " + i + "            ********************************");
                        System.out.println("*************************************************************************************");
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
                                    IODispatcherTest.JSON_DDL_RESPONSE
                            );
                        }

                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                "GET /query?query=drop+table+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                                IODispatcherTest.JSON_DDL_RESPONSE
                        );
                    }
                });
    }

    private void assertImportProcessedWhenClientDisconnected() throws Exception {
        final int parallelCount = 2;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
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
                                countDownLatch.countDown();
                            }
                            LOG.info().$("Stopped thread ").$(threadI).$();
                        }).start();
                    }
                    countDownLatch.await();
                    assertNRowsInserted(validRequestRecordCount);

                    for (int n = 0; n < fds.length; n++) {
                        Assert.assertNotEquals(fds[n], -1);
                        NetworkFacadeImpl.INSTANCE.close(fds[n]);
                    }

                    // Cairo engine should not allow second writer to be opened on the same table, all requests should wait for the writer to be available
                    writer.close();

                    for (int i = 0; i < 20; i++) {
                        try {
                            // check if we have parallelCount x insertCount  records
                            int nRows = (parallelCount + 1) * validRequestRecordCount;
                            assertNRowsInserted(nRows);
                            return;
                        } catch (ComparisonFailure e) {
                            if (i < 9) {
                                Thread.sleep(50);
                            } else {
                                throw e;
                            }
                        }
                    }

                });
    }

    protected void assertNRowsInserted(final int nRows) throws InterruptedException {
        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                "GET /query?query=select+count(*)+from+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                "83\r\n" +
                        "{\"query\":\"select count(*) from \\\"fhv_tripdata_2017-02.csv\\\"\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[" + nRows +
                        "]],\"count\":1}\r\n" +
                        "00\r\n" +
                        "\r\n");
    }

    private void assertInsertWaitsExceedsRerunProcessingQueueSize() throws Exception {
        final int rerunProcessingQueueSize = 1;
        final int parallelCount = 4;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder().withRerunProcessingQueueSize(rerunProcessingQueueSize))
                .withTelemetry(false)
                .run(engine -> {
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
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                                "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(1%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                                                IODispatcherTest.JSON_DDL_RESPONSE
                                        );
                                    } catch (AssertionError ase) {
                                        fails.incrementAndGet();
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e);
                                    }
                                }
                            } finally {
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
                            "71\r\n" +
                                    "{\"query\":\"select count(*) from balances_x\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[" + (parallelCount * insertCount - fails.get()) + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });

    }

    private void assertInsertWaitsWhenWriterLocked() throws Exception {
        final int parallelCount = 2;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run(engine -> {
                    // create table
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                            IODispatcherTest.JSON_DDL_RESPONSE
                    );

                    TableWriter writer = lockWriter(engine, "balances_x");

                    final int insertCount = 10;
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    for (int i = 0; i < parallelCount; i++) {
                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    // insert one record
                                    try {
                                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                                "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(1%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                                                IODispatcherTest.JSON_DDL_RESPONSE
                                        );
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                                    }
                                }
                            } finally {
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
                            "71\r\n" +
                                    "{\"query\":\"select count(*) from balances_x\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[" + parallelCount * insertCount + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                });
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
                .run((engine) -> {
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
                                                .withCompareLength(importResponse.length());
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
                                countDownLatch.countDown();
                            }
                            LOG.info().$("Stopped thread ").$(finalI).$();
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
                            (rowsExpected < 100 ? "83" : "84") + "\r\n" +
                                    "{\"query\":\"select count(*) from \\\"fhv_tripdata_2017-02.csv\\\"\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[" + rowsExpected + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n");

                });
    }

    private void assertInsertsIsPerformedWhenWriterLockedAndDisconnected() throws Exception {
        final int parallelCount = 4;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run(engine -> {
                    // create table
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=%0A%0A%0Acreate+table+balances_x+(%0A%09cust_id+int%2C+%0A%09balance_ccy+symbol%2C+%0A%09balance+double%2C+%0A%09status+byte%2C+%0A%09timestamp+timestamp%0A)&limit=0%2C1000&count=true HTTP/1.1\r\n",
                            IODispatcherTest.JSON_DDL_RESPONSE
                    );

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
                                    Thread.sleep(threadI * 5);
                                    String request = "GET /query?query=%0A%0Ainsert+into+balances_x+(cust_id%2C+balance_ccy%2C+balance%2C+timestamp)+values+(" + threadI +
                                            "%2C+%27USD%27%2C+1500.00%2C+6000000001)&limit=0%2C1000&count=true HTTP/1.1\r\n" + SendAndReceiveRequestBuilder.RequestHeaders;
                                    long fd = new SendAndReceiveRequestBuilder().connectAndSendRequest(request);
                                    fds[threadI] = fd;
                                } catch (Exception e) {
                                    LOG.error().$("Failed execute insert http request. Server error ").$(e);
                                }
                            } finally {
                                countDownLatch.countDown();
                            }
                        });
                        threads[i].start();
                    }
                    countDownLatch.await();
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=SELECT+1 HTTP/1.1\r\n",
                            "54\r\n" +
                                    "{\"query\":\"SELECT 1\",\"columns\":[{\"name\":\"1\",\"type\":\"INT\"}],\"dataset\":[[1]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n");
                    for (int n = 0; n < fds.length; n++) {
                        Assert.assertNotEquals(fds[n], -1);
                        NetworkFacadeImpl.INSTANCE.close(fds[n]);
                    }

                    writer.close();

                    // check if we have parallelCount x insertCount  records
                    int waitCount = 1000 / 50 * parallelCount;
                    for (int i = 0; i < waitCount; i++) {

                        try {
                            new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                    "GET /query?query=select+count()+from+balances_x&count=true HTTP/1.1\r\n",
                                    "6f\r\n" +
                                            "{\"query\":\"select count() from balances_x\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[" + parallelCount + "]],\"count\":1}\r\n" +
                                            "00\r\n" +
                                            "\r\n"
                            );
                            return;
                        } catch (ComparisonFailure e) {
                            if (i < waitCount - 1) {
                                Thread.sleep(50);
                            } else {
                                throw e;
                            }

                        }
                    }
                });
    }

    public void testImportRerunsExceedsRerunProcessingQueueSize(int startDelay) throws Exception {
        final int rerunProcessingQueueSize = 1;
        final int parallelCount = 4;

        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withNetwork(getSendDelayNetworkFacade(startDelay))
                                .withRerunProcessingQueueSize(rerunProcessingQueueSize)
                )
                .run(engine -> {
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
                                countDownLatch.countDown();
                            }
                            LOG.info().$("Stopped thread ").$(finalI).$();
                        }).start();
                    }

                    boolean finished = countDownLatch.await(100, TimeUnit.MILLISECONDS);
                    Assert.assertFalse(finished);

                    writer.close();

                    if (!countDownLatch.await(5000, TimeUnit.MILLISECONDS)) {
                        Assert.fail("Imports did not finish within reasonable time");
                    }

                    // check if we have parallelCount x insertCount  records
                    LOG.info().$("Requesting row count").$();
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+%22fhv_tripdata_2017-02.csv%22&count=true HTTP/1.1\r\n",
                            "84\r\n" +
                                    "{\"query\":\"select count(*) from \\\"fhv_tripdata_2017-02.csv\\\"\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"dataset\":[[" + (parallelCount * insertCount + 1 - failedImports.get()) * validRequestRecordCount + "]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n");

                });
    }

    @Test
    public void testRenameWaitsWhenWriterLocked() throws Exception {
        final int parallelCount = 2;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run(engine -> {
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
                                new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                        "GET /query?query=rename+table+%27balances_x%27+to+%27balances_y%27&limit=0%2C1000&count=true HTTP/1.1\r\n",
                                        IODispatcherTest.JSON_DDL_RESPONSE
                                );
                            } catch (Exception e) {
                                LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                            }
                        } finally {
                            countDownLatch.countDown();
                        }
                    }).start();
                    boolean finished = countDownLatch.await(200, TimeUnit.MILLISECONDS);

                    // Cairo engine should not allow table rename while writer is opened
                    // Cairo is expected to have finished == false
                    Assert.assertFalse(finished);

                    writer.close();
                    Assert.assertTrue("Table rename did not complete within timeout after writer is released",
                            countDownLatch.await(500, TimeUnit.MILLISECONDS));
                });
    }

    @NotNull
    private NetworkFacade getSendDelayNetworkFacade(int startDelayDelayAfter) {
        return new NetworkFacadeImpl() {
            final AtomicInteger totalSent = new AtomicInteger();

            @Override
            public int send(long fd, long buffer, int bufferLen) {
                if (startDelayDelayAfter == 0) {
                    return super.send(fd, buffer, bufferLen);
                }

                int sentNow = totalSent.get();
                if (bufferLen > 0) {
                    if (sentNow >= startDelayDelayAfter) {
                        totalSent.set(0);
                        return 0;
                    }

                    int result = super.send(fd, buffer, Math.min(bufferLen, startDelayDelayAfter - sentNow));
                    totalSent.addAndGet(result);
                    return result;
                }
                return 0;
            }
        };
    }

    @NotNull
    private TableWriter lockWriter(CairoEngine engine, String tableName) throws InterruptedException {
        TableWriter writer = null;
        for (int i = 0; i < 10; i++) {
            try {
                writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "testing");
                break;
            } catch (EntryUnavailableException e) {
                Thread.sleep(10);
            }
        }

        if (writer == null) {
            Assert.fail("Cannot lock writer in a reasonable time");
        }
        return writer;
    }

}
