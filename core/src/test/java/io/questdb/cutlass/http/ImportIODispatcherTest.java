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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ImportIODispatcherTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private static final Log LOG = LogFactory.getLog(ImportIODispatcherTest.class);
    private static final String RequestFooter = "\r\n" +
            "--------------------------27d997ca93d2689d--";

    private static final String Request1Header = "POST /upload?name=trips HTTP/1.1\r\n" +
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
            "    \"name\": \"Col1\",\r\n" +
            "    \"type\": \"STRING\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"PickupDateTime\",\r\n" +
            "    \"type\": \"TIMESTAMP\",\r\n" +
            "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
            "  }\r\n" +
            "]\r\n" +
            "\r\n" +
            "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "Col1,Pickup_DateTime,DropOff_datetime\r\n";

    private static final String ValidImportRequest1 = Request1Header +
            "B00008,2017-02-01 00:30:00,\r\n" +
            "B00008,2017-02-01 00:40:00,\r\n" +
            "B00009,2017-02-01 00:50:00,\r\n" +
            "B00013,2017-02-01 00:51:00,\r\n" +
            "B00013,2017-02-01 01:41:00,\r\n" +
            "B00013,2017-02-01 02:00:00,\r\n" +
            "B00013,2017-02-01 03:53:00,\r\n" +
            "B00013,2017-02-01 04:44:00,\r\n" +
            "B00013,2017-02-01 05:05:00,\r\n" +
            "B00013,2017-02-01 06:54:00,\r\n" +
            "B00014,2017-02-01 07:45:00,\r\n" +
            "B00014,2017-02-01 08:45:00,\r\n" +
            "B00014,2017-02-01 09:46:00,\r\n" +
            "B00014,2017-02-01 10:54:00,\r\n" +
            "B00014,2017-02-01 11:45:00,\r\n" +
            "B00014,2017-02-01 11:45:00,\r\n" +
            "B00014,2017-02-01 11:45:00,\r\n" +
            "B00014,2017-02-01 12:26:00,\r\n" +
            "B00014,2017-02-01 12:55:00,\r\n" +
            "B00014,2017-02-01 13:47:00,\r\n" +
            "B00014,2017-02-01 14:05:00,\r\n" +
            "B00014,2017-02-01 14:58:00,\r\n" +
            "B00014,2017-02-01 15:33:00,\r\n" +
            "B00014,2017-02-01 15:45:00,\r\n" +
            RequestFooter;

    private static final String Request2Header = "POST /upload?name=trips HTTP/1.1\r\n" +
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
            "    \"name\": \"Col1\",\r\n" +
            "    \"type\": \"STRING\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"Col2\",\r\n" +
            "    \"type\": \"STRING\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"Col3\",\r\n" +
            "    \"type\": \"STRING\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"Col4\",\r\n" +
            "    \"type\": \"STRING\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"PickupDateTime\",\r\n" +
            "    \"type\": \"TIMESTAMP\",\r\n" +
            "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
            "  }\r\n" +
            "]\r\n" +
            "\r\n" +
            "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"data\"; filename=\"table2.csv\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "Co1,Col2,Col3,Col4,PickupDateTime\r\n";

    private static final String ValidImportRequest2 = Request2Header +
            "B00008,,,,2017-02-01 00:30:00\r\n" +
            "B00008,,,,2017-02-01 00:40:00\r\n" +
            "B00009,,,,2017-02-01 00:50:00\r\n" +
            "B00013,,,,2017-02-01 00:51:00\r\n" +
            "B00013,,,,2017-02-01 01:41:00\r\n" +
            "B00013,,,,2017-02-01 02:00:00\r\n" +
            "B00013,,,,2017-02-01 03:53:00\r\n" +
            "B00013,,,,2017-02-01 04:44:00\r\n" +
            "B00013,,,,2017-02-01 05:05:00\r\n" +
            "B00013,,,,2017-02-01 06:54:00\r\n" +
            "B00014,,,,2017-02-01 07:45:00\r\n" +
            "B00014,,,,2017-02-01 08:45:00\r\n" +
            "B00014,,,,2017-02-01 09:46:00\r\n" +
            "B00014,,,,2017-02-01 10:54:00\r\n" +
            "B00014,,,,2017-02-01 11:45:00\r\n" +
            "B00014,,,,2017-02-01 11:45:00\r\n" +
            "B00014,,,,2017-02-01 11:45:00\r\n" +
            "B00014,,,,2017-02-01 12:26:00\r\n" +
            "B00014,,,,2017-02-01 12:55:00\r\n" +
            "B00014,,,,2017-02-01 13:47:00\r\n" +
            "B00014,,,,2017-02-01 14:05:00\r\n" +
            "B00014,,,,2017-02-01 14:58:00\r\n" +
            "B00014,,,,2017-02-01 15:33:00\r\n" +
            "B00014,,,,2017-02-01 15:45:00\r\n" +
            RequestFooter;

    private final String ValidImportResponse1 = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "\r\n" +
            "057c\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |\r\n" +
            "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
            "|      Timestamp  |                                              NONE  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|   Rows handled  |                                                24  |                 |         |              |\r\n" +
            "|  Rows imported  |                                                24  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|              0  |                                              Col1  |                   STRING  |           0  |\r\n" +
            "|              1  |                                    PickupDateTime  |                TIMESTAMP  |           0  |\r\n" +
            "|              2  |                                   DropOffDatetime  |                   STRING  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";

    private final String ValidImportResponse2 = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "\r\n" +
            "0666\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |\r\n" +
            "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
            "|      Timestamp  |                                    PickupDateTime  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|   Rows handled  |                                                24  |                 |         |              |\r\n" +
            "|  Rows imported  |                                                24  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|              0  |                                              Col1  |                   STRING  |           0  |\r\n" +
            "|              1  |                                              Col2  |                   STRING  |           0  |\r\n" +
            "|              2  |                                              Col3  |                   STRING  |           0  |\r\n" +
            "|              3  |                                              Col4  |                   STRING  |           0  |\r\n" +
            "|              4  |                                    PickupDateTime  |                TIMESTAMP  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";

    private final String WarningValidImportResponse1 = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: text/plain; charset=utf-8\r\n" +
            "\r\n" +
            "057c\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |\r\n" +
            "|   Partition by  |                                              NONE  |                 |         |  From Table  |\r\n" +
            "|      Timestamp  |                                              NONE  |                 |         |  From Table  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|   Rows handled  |                                                24  |                 |         |              |\r\n" +
            "|  Rows imported  |                                                24  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|              0  |                                              Col1  |                   STRING  |           0  |\r\n" +
            "|              1  |                                    PickupDateTime  |                TIMESTAMP  |           0  |\r\n" +
            "|              2  |                                   DropOffDatetime  |                   STRING  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";

    private final String WarningValidImportResponse1Json = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: application/json; charset=utf-8\r\n" +
            "\r\n" +
            "0170\r\n" +
            "{\"status\":\"OK\"," +
            "\"location\":\"trips\"," +
            "\"rowsRejected\":0," +
            "\"rowsImported\":24," +
            "\"header\":false," +
            "\"warnings\":[" +
            "\"Existing table timestamp column is used\"," +
            "\"Existing table PartitionBy is used\"]," +
            "\"columns\":[" +
            "{\"name\":\"Col1\",\"type\":\"STRING\",\"size\":0,\"errors\":0}," +
            "{\"name\":\"PickupDateTime\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0}," +
            "{\"name\":\"DropOffDatetime\",\"type\":\"STRING\",\"size\":0,\"errors\":0}" +
            "]}\r\n" +
            "00\r\n" +
            "\r\n";

    private final String DdlCols1 = "(Col1+STRING,PickupDateTime+TIMESTAMP,DropOffDatetime+STRING)";
    private final String DdlCols2 = "(Col1+STRING,Col2+STRING,Col3+STRING,Col4+STRING,PickupDateTime+TIMESTAMP)+timestamp(PickupDateTime)";

    @Test
    public void testImportWithWrongTimestampSpecifiedLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWithWrongTimestampSpecified();
            temp.delete();
            temp.create();
        }
    }

    private void testImportWithWrongTimestampSpecified() throws Exception {
        final int parallelCount = 2;
        final int insertCount = 9;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    AtomicInteger success = new AtomicInteger();
                    String[] reqeusts = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] response = new String[]{ValidImportResponse1, ValidImportResponse2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    for (int i = 0; i < parallelCount; i++) {
                        final int thread = i;
                        final String respTemplate = response[i];
                        final String requestTemplate = reqeusts[i];
                        final String ddlCols = ddl[i];
                        final String tableName = "trip" + i;

                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                "GET /query?query=CREATE+TABLE+" + tableName + ddlCols + "; HTTP/1.1\r\n",
                                "0c\r\n" +
                                        "{\"ddl\":\"OK\"}\r\n" +
                                        "00\r\n" +
                                        "\r\n");

                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    try {
                                        String timestamp = "";
                                        if (r > 0 && thread > 0) {
                                            timestamp = "&timestamp=PickupDateTime";
                                        }
                                        String request = requestTemplate
                                                .replace("POST /upload?name=trips HTTP", "POST /upload?name=" + tableName + timestamp + " HTTP")
                                                .replace("2017-02-01", "2017-02-0" + (r + 1));

                                        String resp = respTemplate;
                                        resp = resp.replace("trips", tableName);
                                        new SendAndReceiveRequestBuilder().execute(request, resp);
                                        success.incrementAndGet();
                                    } catch (Exception e) {
                                        LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                                    }
                                }
                            } finally {
                                countDownLatch.countDown();
                            }
                        }).start();
                    }

                    final int totalImports = parallelCount * insertCount;
                    boolean finished = countDownLatch.await(200 * totalImports, TimeUnit.MILLISECONDS);
                    Assert.assertTrue(
                            "Import is not finished in reasonable time, check server errors",
                            finished);
                    Assert.assertEquals(
                            "Expected successful import count does not match actual imports",
                            totalImports,
                            success.get());
                });
    }

    @Test
    public void testImportLocksTable() throws Exception {
        final String tableName = "trips";
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
                    AtomicBoolean locked = new AtomicBoolean(false);
                    engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                        if (event == PoolListener.EV_LOCK_SUCCESS && Chars.equalsNc(name, tableName)) {
                            try (Path path = new Path()) {
                                if (engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, tableName) == TableUtils.TABLE_DOES_NOT_EXIST) {
                                    locked.set(true);
                                }
                            }
                        }
                    });

                    new SendAndReceiveRequestBuilder().execute(ValidImportRequest1, ValidImportResponse1);
                    Assert.assertTrue("Engine must be locked on table creation from upload", locked.get());
                });
    }


    @Test
    public void testImportWitNocacheSymbolsLoop() throws Exception {
        for (int i = 0; i < 2; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWitNocacheSymbols();
            temp.delete();
            temp.create();
        }
    }

    private void testImportWitNocacheSymbols() throws Exception {
        final int parallelCount = 2;
        final int insertCount = 1;
        final int importRowCount = 10;
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    AtomicInteger success = new AtomicInteger();

                    String ddl1 = "(Col1+SYMBOL+NOCACHE+INDEX,PickupDateTime+TIMESTAMP," +
                            "DropOffDatetime+SYMBOL)+timestamp(PickupDateTime)";
                    String ddl2 = "(Col1+SYMBOL+NOCACHE+INDEX,Col2+STRING,Col3+STRING,Col4+STRING,PickupDateTime+TIMESTAMP)" +
                            "+timestamp(PickupDateTime)";
                    String[] ddl = new String[]{ddl1, ddl2};
                    String[] headers = new String[]{Request1Header, Request2Header};

                    String[][] csvTemplate = {
                            new String[]{"SYM-%d", "2017-02-01 00:00:00", "SYM-2-%s"},
                            new String[]{"SYM-%d", "%d", "%d", "", "2017-02-01 00:00:00"}
                    };

                    for (int i = 0; i < parallelCount; i++) {
                        final String ddlCols = ddl[i];
                        final String tableName = "trip" + i;
                        final int tableIndex = i;

                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                "GET /query?query=CREATE+TABLE+" + tableName + ddlCols + "; HTTP/1.1\r\n",
                                "0c\r\n" +
                                        "{\"ddl\":\"OK\"}\r\n" +
                                        "00\r\n" +
                                        "\r\n");

                        new Thread(() -> {
                            try {
                                try {
                                    for (int batch = 0; batch < 2; batch++) {
                                        int low = importRowCount / 2 * batch;
                                        int hi = importRowCount / 2 * (batch + 1);
                                        String requestTemplate =
                                                headers[tableIndex].replace("POST /upload?name=trips ", "POST /upload?name=" + tableName + " ")
                                                        + GenerateImportCsv(low, hi, csvTemplate[tableIndex])
                                                        + RequestFooter;

                                        new SendAndReceiveRequestBuilder()
                                                .withCompareLength(15)
                                                .execute(requestTemplate, "HTTP/1.1 200 OK");
                                    }

                                    new SendAndReceiveRequestBuilder().withExpectDisconnect(false).executeMany(httpClient -> {
                                        for (int row = 0; row < importRowCount; row++) {
                                            final String request = "SELECT+Col1+FROM+" + tableName + "+WHERE+Col1%3D%27SYM-" + row + "%27; ";

                                            httpClient.executeWithStandardHeaders(
                                                    "GET /query?query=" + request + "HTTP/1.1\r\n",
                                                    "8" + (stringLen(row) * 2) + "\r\n" +
                                                            "{\"query\":\"SELECT Col1 FROM " + tableName + " WHERE Col1='SYM-"
                                                            + row +
                                                            "';\",\"columns\":[{\"name\":\"Col1\",\"type\":\"SYMBOL\"}]," +
                                                            "\"dataset\":[[\"SYM-" + row + "\"]],\"count\":1}\r\n"
                                                            + "00\r\n"
                                                            + "\r\n");
                                        }
                                    });

                                    success.incrementAndGet();
                                } catch (Exception e) {
                                    LOG.error().$("Failed execute insert http request. Server error ").$(e).$();
                                }
                            } finally {
                                countDownLatch.countDown();
                            }
                        }).start();
                    }

                    final int totalImports = parallelCount * insertCount;
                    boolean finished = countDownLatch.await(Math.max(10 * importRowCount, 2000) * totalImports, TimeUnit.MILLISECONDS);
                    Assert.assertTrue(
                            "Import is not finished in reasonable time, check server errors",
                            finished);
                    Assert.assertEquals(
                            "Expected successful import count does not match actual imports",
                            totalImports,
                            success.get());
                });
    }

    @Test
    public void testImportWithWrongPartitionBy() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
                    String[] reqeusts = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = reqeusts[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n");

                    String request = requestTemplate
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&partitionBy=DAY&timestamp=PickupDateTime HTTP");

                    new SendAndReceiveRequestBuilder().execute(request, WarningValidImportResponse1);
                });
    }

    @Test
    public void testImportWithWrongPartitionByJson() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine) -> {
                    String[] reqeusts = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = reqeusts[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n");

                    String request = requestTemplate
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&fmt=json&partitionBy=DAY&timestamp=PickupDateTime HTTP");

                    new SendAndReceiveRequestBuilder().execute(request, WarningValidImportResponse1Json);
                });
    }

    private static int stringLen(int number) {
        int length = 1;
        long temp = 10;
        while (temp <= number) {
            length++;
            temp *= 10;
        }
        return length;
    }

    private String GenerateImportCsv(int low, int hi, String... columnTemplates) {
        StringBuilder csvStringBuilder = new StringBuilder();
        for (int i = low; i < hi; i++) {
            for (int j = 0; j < columnTemplates.length; j++) {
                final String template = columnTemplates[j];
                csvStringBuilder.append(String.format(template, i));
                if (j < columnTemplates.length - 1) {
                    csvStringBuilder.append(',');
                } else {
                    csvStringBuilder.append("\r\n");
                }
            }
        }
        return csvStringBuilder.toString();
    }
}
