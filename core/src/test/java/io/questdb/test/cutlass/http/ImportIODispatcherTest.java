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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxnScoreboardV1;
import io.questdb.cairo.TxnScoreboardV2;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.tools.TestUtils.getSendDelayNetworkFacade;

public class ImportIODispatcherTest extends AbstractTest {
    private static final Log LOG = LogFactory.getLog(ImportIODispatcherTest.class);
    private static final String PostHeader = "POST /upload?name=trips HTTP/1.1\r\n" +
            "Host: localhost:9001\r\n" +
            "User-Agent: curl/7.64.0\r\n" +
            "Accept: */*\r\n" +
            "Content-Length: 437760673\r\n" +
            "Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r\n" +
            "Expect: 100-continue\r\n" +
            "\r\n";
    private static final String REQUEST_FOOTER = "\r\n" +
            "--------------------------27d997ca93d2689d--";
    private static final String Request1DataHeader = "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"data\"; filename=\"fhv_tripdata_2017-02.csv\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "Col1,Pickup_DateTime,DropOff_datetime\r\n";
    private static final String Request1SchemaPart = "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"schema\"; filename=\"schema.json\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "[\r\n" +
            "  {\r\n" +
            "    \"name\": \"Col1\",\r\n" +
            "    \"type\": \"STRING\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"Pickup_DateTime\",\r\n" +
            "    \"type\": \"TIMESTAMP\",\r\n" +
            "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
            "  },\r\n" +
            "  {\r\n" +
            "    \"name\": \"DropOff_datetime\",\r\n" +
            "    \"type\": \"VARCHAR\"\r\n" +
            "  }\r\n" +
            "]\r\n" +
            "\r\n";
    private static final String Request1Header = PostHeader +
            Request1SchemaPart +
            Request1DataHeader;
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
            REQUEST_FOOTER;
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
            "    \"name\": \"Pickup_DateTime\",\r\n" +
            "    \"type\": \"TIMESTAMP\",\r\n" +
            "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
            "  }\r\n" +
            "]\r\n" +
            "\r\n" +
            "--------------------------27d997ca93d2689d\r\n" +
            "Content-Disposition: form-data; name=\"data\"; filename=\"table2.csv\"\r\n" +
            "Content-Type: application/octet-stream\r\n" +
            "\r\n" +
            "Col1,Col2,Col3,Col4,Pickup_DateTime\r\n";
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
            REQUEST_FOOTER;
    private final String DdlCols1 = "(Col1+STRING,Pickup_DateTime+TIMESTAMP,DropOff_datetime+VARCHAR)";
    private final String DdlCols2 = "(Col1+STRING,Col2+STRING,Col3+STRING,Col4+STRING,Pickup_DateTime+TIMESTAMP)+timestamp(Pickup_DateTime)";
    private final String ImportCreateParamRequestFalse = ValidImportRequest1
            .replace(
                    "POST /upload?name=trips HTTP",
                    "POST /upload?name=trips&timestamp=Pickup_DateTime&createTable=false HTTP"
            );
    private final String ImportCreateParamRequestTrue = ValidImportRequest1
            .replace(
                    "POST /upload?name=trips HTTP",
                    "POST /upload?name=trips&timestamp=Pickup_DateTime&createTable=true HTTP"
            );
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
            "|              1  |                                   Pickup_DateTime  |                TIMESTAMP  |           0  |\r\n" +
            "|              2  |                                  DropOff_datetime  |                  VARCHAR  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    private final String ValidImportResponse1Json = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: application/json; charset=utf-8\r\n" +
            "\r\n" +
            "0148\r\n" +
            "{\"status\":\"OK\"," +
            "\"location\":\"trips\"," +
            "\"rowsRejected\":0," +
            "\"rowsImported\":24," +
            "\"header\":true," +
            "\"partitionBy\":\"DAY\"," +
            "\"timestamp\":\"Pickup_DateTime\"," +
            "\"columns\":[" +
            "{\"name\":\"Col1\",\"type\":\"STRING\",\"size\":0,\"errors\":0}," +
            "{\"name\":\"Pickup_DateTime\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0}," +
            "{\"name\":\"DropOff_datetime\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0}" +
            "]}\r\n" +
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
            "|      Timestamp  |                                   Pickup_DateTime  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|   Rows handled  |                                                24  |                 |         |              |\r\n" +
            "|  Rows imported  |                                                24  |                 |         |              |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "|              0  |                                              Col1  |                   STRING  |           0  |\r\n" +
            "|              1  |                                              Col2  |                   STRING  |           0  |\r\n" +
            "|              2  |                                              Col3  |                   STRING  |           0  |\r\n" +
            "|              3  |                                              Col4  |                   STRING  |           0  |\r\n" +
            "|              4  |                                   Pickup_DateTime  |                TIMESTAMP  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    private final String ValidImportResponse2Json = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: application/json; charset=utf-8\r\n" +
            "\r\n" +
            "012b\r\n" +
            "{\"status\":\"OK\"," +
            "\"location\":\"trips\"," +
            "\"rowsRejected\":0," +
            "\"rowsImported\":24," +
            "\"header\":true," +
            "\"partitionBy\":\"NONE\"," +
            "\"columns\":[" +
            "{\"name\":\"Col1\",\"type\":\"STRING\",\"size\":0,\"errors\":0}," +
            "{\"name\":\"Pickup_DateTime\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0}," +
            "{\"name\":\"DropOff_datetime\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0}" +
            "]}\r\n" +
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
            "|              1  |                                   Pickup_DateTime  |                TIMESTAMP  |           0  |\r\n" +
            "|              2  |                                  DropOff_datetime  |                  VARCHAR  |           0  |\r\n" +
            "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
            "\r\n" +
            "00\r\n" +
            "\r\n";
    private final String ImportCreateParamResponse = WarningValidImportResponse1
            .replace(
                    "\r\n" +
                            "|   Partition by  |                                              NONE  |                 |         |  From Table  |\r\n" +
                            "|      Timestamp  |                                              NONE  |                 |         |  From Table  |",
                    "\r\n" +
                            "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                            "|      Timestamp  |                                   Pickup_DateTime  |                 |         |              |"
            );
    private final String WarningValidImportResponse1Json = "HTTP/1.1 200 OK\r\n" +
            "Server: questDB/1.0\r\n" +
            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
            "Transfer-Encoding: chunked\r\n" +
            "Content-Type: application/json; charset=utf-8\r\n" +
            "\r\n" +
            "0187\r\n" +
            "{\"status\":\"OK\"," +
            "\"location\":\"trips\"," +
            "\"rowsRejected\":0," +
            "\"rowsImported\":24," +
            "\"header\":true," +
            "\"partitionBy\":\"NONE\"," +
            "\"warnings\":[" +
            "\"Existing table timestamp column is used\"," +
            "\"Existing table PartitionBy is used\"]," +
            "\"columns\":[" +
            "{\"name\":\"Col1\",\"type\":\"STRING\",\"size\":0,\"errors\":0}," +
            "{\"name\":\"Pickup_DateTime\",\"type\":\"TIMESTAMP\",\"size\":8,\"errors\":0}," +
            "{\"name\":\"DropOff_datetime\",\"type\":\"VARCHAR\",\"size\":0,\"errors\":0}" +
            "]}\r\n" +
            "00\r\n" +
            "\r\n";
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    private SqlExecutionContext sqlExecutionContext;

    @Test
    public void testImportDesignatedTsFromSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    setupSql(engine);
                    final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                    engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                        if (event == PoolListener.EV_RETURN && Chars.equals("syms", name.getTableName())) {
                            waitForData.countDown();
                        }
                    });
                    new SendAndReceiveRequestBuilder().execute(
                            "POST /upload?name=syms&timestamp=ts1 HTTP/1.1\r\n" +
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
                                    "    \"name\": \"col1\",\r\n" +
                                    "    \"type\": \"SYMBOL\",\r\n" +
                                    "    \"index\": \"false\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"col2\",\r\n" +
                                    "    \"type\": \"SYMBOL\",\r\n" +
                                    "    \"index\": \"true\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"ts1\",\r\n" +
                                    "    \"type\": \"TIMESTAMP\",\r\n" +
                                    "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"ts2\",\r\n" +
                                    "    \"type\": \"TIMESTAMP\",\r\n" +
                                    "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"ts3\",\r\n" +
                                    "    \"type\": \"TIMESTAMP\",\r\n" +
                                    "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
                                    "  }\r\n" +
                                    "]\r\n" +
                                    "\r\n" +
                                    "--------------------------27d997ca93d2689d\r\n" +
                                    "Content-Disposition: form-data; name=\"data\"; filename=\"table2.csv\"\r\n" +
                                    "Content-Type: application/octet-stream\r\n" +
                                    "\r\n" +
                                    "col1,col2,ts1,ts2,ts3\r\n" +
                                    "sym1,sym2,,2017-02-01 00:30:00,2017-02-01 00:30:01\r\n" +
                                    "\r\n" +
                                    "--------------------------27d997ca93d2689d--",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: text/plain; charset=utf-8\r\n" +
                                    "\r\n" +
                                    "0666\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "|      Location:  |                                              syms  |        Pattern  | Locale  |      Errors  |\r\n" +
                                    "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                                    "|      Timestamp  |                                               ts1  |                 |         |              |\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "|   Rows handled  |                                                 1  |                 |         |              |\r\n" +
                                    "|  Rows imported  |                                                 0  |                 |         |              |\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "|              0  |                                              col1  |                   SYMBOL  |           0  |\r\n" +
                                    "|              1  |                                              col2  |         (idx/256) SYMBOL  |           0  |\r\n" +
                                    "|              2  |                                               ts1  |                TIMESTAMP  |           1  |\r\n" +
                                    "|              3  |                                               ts2  |                TIMESTAMP  |           0  |\r\n" +
                                    "|              4  |                                               ts3  |                TIMESTAMP  |           0  |\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                    if (!waitForData.await(TimeUnit.SECONDS.toNanos(30L))) {
                        Assert.fail();
                    }

                    try (TableReader reader = newOffPoolReader(engine.getConfiguration(), "syms", engine)) {
                        TableReaderMetadata meta = reader.getMetadata();
                        Assert.assertEquals(5, meta.getColumnCount());
                        Assert.assertEquals(2, meta.getTimestampIndex());
                        Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("col1"));
                        Assert.assertFalse(meta.isColumnIndexed(0));
                        Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("col2"));
                        Assert.assertTrue(meta.isColumnIndexed(1));
                        Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType("ts1"));
                        Assert.assertFalse(meta.isColumnIndexed(2));
                        Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType("ts2"));
                        Assert.assertFalse(meta.isColumnIndexed(3));
                        Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType("ts3"));
                        Assert.assertFalse(meta.isColumnIndexed(4));
                    }
                });
    }

    @Test
    public void testImportDropReimportDifferentSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
                    executor.execute(
                            ValidImportRequest1.replace("VARCHAR", "SYMBOL"),
                            ValidImportResponse1.replace("VARCHAR", "SYMBOL").replace(" SYMBOL", "  SYMBOL")
                    );

                    executor.executeWithStandardHeaders(
                            "GET /query?query=select+*+from+trips HTTP/1.1\r\n",
                            "051b\r\n" +
                                    "{\"query\":\"select * from trips\",\"columns\":[{\"name\":\"Col1\",\"type\":\"STRING\"},{\"name\":\"Pickup_DateTime\",\"type\":\"TIMESTAMP\"},{\"name\":\"DropOff_datetime\",\"type\":\"SYMBOL\"}],\"timestamp\":-1,\"dataset\":[[\"B00008\",\"2017-02-01T00:30:00.000000Z\",null],[\"B00008\",\"2017-02-01T00:40:00.000000Z\",null],[\"B00009\",\"2017-02-01T00:50:00.000000Z\",null],[\"B00013\",\"2017-02-01T00:51:00.000000Z\",null],[\"B00013\",\"2017-02-01T01:41:00.000000Z\",null],[\"B00013\",\"2017-02-01T02:00:00.000000Z\",null],[\"B00013\",\"2017-02-01T03:53:00.000000Z\",null],[\"B00013\",\"2017-02-01T04:44:00.000000Z\",null],[\"B00013\",\"2017-02-01T05:05:00.000000Z\",null],[\"B00013\",\"2017-02-01T06:54:00.000000Z\",null],[\"B00014\",\"2017-02-01T07:45:00.000000Z\",null],[\"B00014\",\"2017-02-01T08:45:00.000000Z\",null],[\"B00014\",\"2017-02-01T09:46:00.000000Z\",null],[\"B00014\",\"2017-02-01T10:54:00.000000Z\",null],[\"B00014\",\"2017-02-01T11:45:00.000000Z\",null],[\"B00014\",\"2017-02-01T11:45:00.000000Z\",null],[\"B00014\",\"2017-02-01T11:45:00.000000Z\",null],[\"B00014\",\"2017-02-01T12:26:00.000000Z\",null],[\"B00014\",\"2017-02-01T12:55:00.000000Z\",null],[\"B00014\",\"2017-02-01T13:47:00.000000Z\",null],[\"B00014\",\"2017-02-01T14:05:00.000000Z\",null],[\"B00014\",\"2017-02-01T14:58:00.000000Z\",null],[\"B00014\",\"2017-02-01T15:33:00.000000Z\",null],[\"B00014\",\"2017-02-01T15:45:00.000000Z\",null]],\"count\":24}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    executor.executeWithStandardHeaders(
                            "GET /query?query=drop+table+trips HTTP/1.1\r\n",
                            "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    String request2 = ValidImportRequest2
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&timestamp=Pickup_DateTime&overwrite=true HTTP");

                    executor.execute(request2, ValidImportResponse2);

                    // This will try to hit the same execution plan as the first SELECT query
                    // but because table metadata changes, the old query plan is not valid anymore
                    // and produces NPE if used
                    executor.executeWithStandardHeaders(
                            "GET /query?query=select+*+from+trips HTTP/1.1\r\n",
                            "063e\r\n" +
                                    "{\"query\":\"select * from trips\",\"columns\":[{\"name\":\"Col1\",\"type\":\"STRING\"},{\"name\":\"Col2\",\"type\":\"STRING\"},{\"name\":\"Col3\",\"type\":\"STRING\"},{\"name\":\"Col4\",\"type\":\"STRING\"},{\"name\":\"Pickup_DateTime\",\"type\":\"TIMESTAMP\"}],\"timestamp\":4,\"dataset\":[[\"B00008\",null,null,null,\"2017-02-01T00:30:00.000000Z\"],[\"B00008\",null,null,null,\"2017-02-01T00:40:00.000000Z\"],[\"B00009\",null,null,null,\"2017-02-01T00:50:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T00:51:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T01:41:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T02:00:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T03:53:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T04:44:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T05:05:00.000000Z\"],[\"B00013\",null,null,null,\"2017-02-01T06:54:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T07:45:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T08:45:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T09:46:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T10:54:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T11:45:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T11:45:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T11:45:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T12:26:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T12:55:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T13:47:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T14:05:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T14:58:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T15:33:00.000000Z\"],[\"B00014\",null,null,null,\"2017-02-01T15:45:00.000000Z\"]],\"count\":24}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                }));
    }

    @Test
    public void testImportDropReimportDifferentSchemaTextQuery() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    setupSql(engine);
                    engine.execute(
                            "create table trips as (" +
                                    "select cast('b' as SYMBOL) Col1, " +
                                    "timestamp_sequence(0, 100000L) Pickup_DateTime," +
                                    "timestamp_sequence(100000000L, 10000L)" +
                                    "from long_sequence(1)" +
                                    ")", this.sqlExecutionContext
                    );

                    new SendAndReceiveRequestBuilder().executeMany(executor -> {
                        executor.execute(
                                "GET /exp?query=select+*+from+trips HTTP/1.1\r\n"
                                        + SendAndReceiveRequestBuilder.RequestHeaders,
                                "HTTP/1.1 200 OK\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Transfer-Encoding: chunked\r\n" +
                                        "Content-Type: text/csv; charset=utf-8\r\n" +
                                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                        "Keep-Alive: timeout=5, max=10000\r\n" +
                                        "\r\n" +
                                        "70\r\n" +
                                        "\"Col1\",\"Pickup_DateTime\",\"timestamp_sequence\"\r\n" +
                                        "\"b\",\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:01:40.000000Z\"\r\n" +
                                        "\r\n" +
                                        "00\r\n" +
                                        "\r\n"
                        );
                        executor.executeWithStandardHeaders(
                                "GET /query?query=drop+table+trips HTTP/1.1\r\n",
                                "0c\r\n" +
                                        "{\"ddl\":\"OK\"}\r\n" +
                                        "00\r\n" +
                                        "\r\n"
                        );

                        String request2 = ValidImportRequest2
                                .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&timestamp=Pickup_DateTime&overwrite=true HTTP");
                        executor.execute(request2, ValidImportResponse2);

                        // This will try to hit the same execution plan as the first SELECT query
                        // but because table metadata changes, the old query plan is not valid anymore
                        // and produces NPE if used
                        executor.execute(
                                "GET /exp?query=select+*+from+trips HTTP/1.1\r\n"
                                        + SendAndReceiveRequestBuilder.RequestHeaders,
                                "HTTP/1.1 200 OK\r\n" +
                                        "Server: questDB/1.0\r\n" +
                                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                        "Transfer-Encoding: chunked\r\n" +
                                        "Content-Type: text/csv; charset=utf-8\r\n" +
                                        "Content-Disposition: attachment; filename=\"questdb-query-0.csv\"\r\n" +
                                        "Keep-Alive: timeout=5, max=10000\r\n" +
                                        "\r\n" +
                                        "0437\r\n" +
                                        "\"Col1\",\"Col2\",\"Col3\",\"Col4\",\"Pickup_DateTime\"\r\n" +
                                        "\"B00008\",,,,\"2017-02-01T00:30:00.000000Z\"\r\n" +
                                        "\"B00008\",,,,\"2017-02-01T00:40:00.000000Z\"\r\n" +
                                        "\"B00009\",,,,\"2017-02-01T00:50:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T00:51:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T01:41:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T02:00:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T03:53:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T04:44:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T05:05:00.000000Z\"\r\n" +
                                        "\"B00013\",,,,\"2017-02-01T06:54:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T07:45:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T08:45:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T09:46:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T10:54:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T11:45:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T11:45:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T11:45:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T12:26:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T12:55:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T13:47:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T14:05:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T14:58:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T15:33:00.000000Z\"\r\n" +
                                        "\"B00014\",,,,\"2017-02-01T15:45:00.000000Z\"\r\n" +
                                        "\r\n" +
                                        "00\r\n" +
                                        "\r\n"
                        );
                    });
                });
    }

    @Test
    public void testImportLocksTable() throws Exception {
        final String tableName = "trips";
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    AtomicBoolean locked = new AtomicBoolean(false);
                    engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                        if (event == PoolListener.EV_LOCK_SUCCESS && Chars.equalsNc(name.getTableName(), tableName)) {
                            try (Path path = new Path()) {
                                if (engine.getTableStatus(path, tableName) == TableUtils.TABLE_RESERVED) {
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
    public void testImportMisDetectsTimestampColumn() throws Exception {
        testImportMisDetectsTimestampColumn(new HttpServerConfigurationBuilder(), 1000000);
    }

    @Test
    public void testImportMisDetectsTimestampColumnSlowPeer() throws Exception {
        testImportMisDetectsTimestampColumn(new HttpServerConfigurationBuilder().withNetwork(getSendDelayNetworkFacade(50)), 10);
    }

    @Test
    public void testImportOverSameConnectionWithDifferentFormats() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
                    String requestJson = ValidImportRequest1
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&fmt=json HTTP");
                    new SendAndReceiveRequestBuilder().execute(requestJson, ValidImportResponse2Json);
                    new SendAndReceiveRequestBuilder().execute(ValidImportRequest1, ValidImportResponse1);
                }));
    }

    @Test
    public void testImportSymbolIndexedFromSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    setupSql(engine);
                    final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                    engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                        if (event == PoolListener.EV_RETURN && Chars.equals("syms", name.getTableName())) {
                            waitForData.countDown();
                        }
                    });
                    new SendAndReceiveRequestBuilder().execute(
                            "POST /upload?name=syms&timestamp=ts HTTP/1.1\r\n" +
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
                                    "    \"name\": \"col1\",\r\n" +
                                    "    \"type\": \"SYMBOL\",\r\n" +
                                    "    \"index\": \"true\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"col2\",\r\n" +
                                    "    \"type\": \"SYMBOL\",\r\n" +
                                    "    \"index\": \"false\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"col3\",\r\n" +
                                    "    \"type\": \"SYMBOL\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"col4\",\r\n" +
                                    "    \"type\": \"STRING\",\r\n" +
                                    "    \"index\": \"true\"\r\n" +
                                    "  },\r\n" +
                                    "  {\r\n" +
                                    "    \"name\": \"ts\",\r\n" +
                                    "    \"type\": \"TIMESTAMP\",\r\n" +
                                    "    \"pattern\": \"yyyy-MM-dd HH:mm:ss\"\r\n" +
                                    "  }\r\n" +
                                    "]\r\n" +
                                    "\r\n" +
                                    "--------------------------27d997ca93d2689d\r\n" +
                                    "Content-Disposition: form-data; name=\"data\"; filename=\"table2.csv\"\r\n" +
                                    "Content-Type: application/octet-stream\r\n" +
                                    "\r\n" +
                                    "col1,col2,col3,col4,ts\r\n" +
                                    "sym1,sym2,,string here,2017-02-01 00:30:00\r\n" +
                                    "\r\n" +
                                    "--------------------------27d997ca93d2689d--",
                            "HTTP/1.1 200 OK\r\n" +
                                    "Server: questDB/1.0\r\n" +
                                    "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                    "Transfer-Encoding: chunked\r\n" +
                                    "Content-Type: text/plain; charset=utf-8\r\n" +
                                    "\r\n" +
                                    "0666\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "|      Location:  |                                              syms  |        Pattern  | Locale  |      Errors  |\r\n" +
                                    "|   Partition by  |                                              NONE  |                 |         |              |\r\n" +
                                    "|      Timestamp  |                                                ts  |                 |         |              |\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "|   Rows handled  |                                                 1  |                 |         |              |\r\n" +
                                    "|  Rows imported  |                                                 1  |                 |         |              |\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "|              0  |                                              col1  |         (idx/256) SYMBOL  |           0  |\r\n" +
                                    "|              1  |                                              col2  |                   SYMBOL  |           0  |\r\n" +
                                    "|              2  |                                              col3  |                   SYMBOL  |           0  |\r\n" +
                                    "|              3  |                                              col4  |                   STRING  |           0  |\r\n" +
                                    "|              4  |                                                ts  |                TIMESTAMP  |           0  |\r\n" +
                                    "+-----------------------------------------------------------------------------------------------------------------+\r\n" +
                                    "\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                    if (!waitForData.await(TimeUnit.SECONDS.toNanos(30L))) {
                        Assert.fail();
                    }

                    try (TableReader reader = newOffPoolReader(engine.getConfiguration(), "syms", engine)) {
                        TableReaderMetadata meta = reader.getMetadata();
                        Assert.assertEquals(5, meta.getColumnCount());
                        Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("col1"));
                        Assert.assertTrue(meta.isColumnIndexed(0));
                        Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("col2"));
                        Assert.assertFalse(meta.isColumnIndexed(1));
                        Assert.assertEquals(ColumnType.SYMBOL, meta.getColumnType("col3"));
                        Assert.assertFalse(meta.isColumnIndexed(2));
                        Assert.assertEquals(ColumnType.STRING, meta.getColumnType("col4"));
                        Assert.assertFalse(meta.isColumnIndexed(3));
                        Assert.assertEquals(ColumnType.TIMESTAMP, meta.getColumnType("ts"));
                        Assert.assertFalse(meta.isColumnIndexed(4));
                    }
                });
    }

    @Test
    public void testImportWitNocacheSymbolsLoop() throws Exception {
        for (int i = 0; i < 2; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWithNoCacheSymbols();
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testImportWithCreateFalseAndNoTable() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root).withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> new SendAndReceiveRequestBuilder().withExpectSendDisconnect(true).execute(ImportCreateParamRequestFalse, ImportCreateParamResponse));
    }

    @Test()
    public void testImportWithCreateTrueAndNoTable() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root).withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    new SendAndReceiveRequestBuilder().execute(ImportCreateParamRequestTrue, ImportCreateParamResponse);
                    drainWalQueue(engine);
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select count() from trips",
                            Misc.getThreadLocalSink(),
                            "count\n24\n"
                    );
                });
    }

    @Test
    public void testImportWithDedupEnabled() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {

                    engine.execute(
                            "create table trips (" +
                                    "Col1 STRING," +
                                    "Pickup_DateTime TIMESTAMP," +
                                    "DropOff_datetime VARCHAR" +
                                    ") timestamp(Pickup_DateTime) partition by DAY WAL",
                            sqlExecutionContext
                    );

                    String request = ValidImportRequest1
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&timestamp=Pickup_DateTime HTTP");

                    String response = WarningValidImportResponse1
                            .replace(
                                    "\r\n" +
                                            "|   Partition by  |                                              NONE  |                 |         |  From Table  |\r\n" +
                                            "|      Timestamp  |                                              NONE  |                 |         |  From Table  |",
                                    "\r\n" +
                                            "|   Partition by  |                                               DAY  |                 |         |              |\r\n" +
                                            "|      Timestamp  |                                   Pickup_DateTime  |                 |         |              |"
                            );
                    new SendAndReceiveRequestBuilder().execute(request, response);

                    drainWalQueue(engine);
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select count() from trips",
                            Misc.getThreadLocalSink(),
                            "count\n24\n"
                    );

                    engine.execute("alter table trips dedup upsert keys(Pickup_DateTime)", sqlExecutionContext);

                    // resend the same request
                    new SendAndReceiveRequestBuilder().execute(request, response);
                    drainWalQueue(engine);

                    // check deduplication worked
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select count() from trips",
                            Misc.getThreadLocalSink(),
                            "count\n24\n"
                    );
                });
    }

    @Test
    public void testImportWithTimestampAndPartitionByOverwriteJson() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = requests[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    String request = requestTemplate
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&fmt=json&partitionBy=DAY&timestamp=Pickup_DateTime&overwrite=true HTTP");

                    new SendAndReceiveRequestBuilder().execute(request, ValidImportResponse1Json);
                });
    }

    @Test
    public void testImportWithWrongPartitionBy() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = requests[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    String request = requestTemplate
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&partitionBy=DAY&timestamp=Pickup_DateTime HTTP");

                    new SendAndReceiveRequestBuilder().execute(request, WarningValidImportResponse1);
                });
    }

    @Test
    public void testImportWithWrongPartitionByJson() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = requests[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            "0c\r\n" +
                                    "{\"ddl\":\"OK\"}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    String request = requestTemplate
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&fmt=json&partitionBy=DAY&timestamp=Pickup_DateTime HTTP");

                    new SendAndReceiveRequestBuilder().execute(request, WarningValidImportResponse1Json);
                });
    }

    @Test
    public void testImportWithWrongTimestampSpecifiedLoop() throws Exception {
        for (int i = 0; i < 5; i++) {
            System.out.println("*************************************************************************************");
            System.out.println("**************************         Run " + i + "            ********************************");
            System.out.println("*************************************************************************************");
            testImportWithWrongTimestampSpecified();
            TestUtils.removeTestPath(root);
            TestUtils.createTestPath(root);
        }
    }

    @Test
    public void testPartitionDeletedUnlocksTxn() throws Exception {
        // Simulate file-not-found on partition re-open on reader reload
        // so that JsonQueryProcessor gets error like here

        // I i.q.c.h.p.QueryCache hit [thread=questdb-worker-2, sql=select count(*) from xyz where x > 0;]
        // I i.q.c.h.p.JsonQueryProcessorState [27] execute-cached [skip: 0, stop: 9223372036854775807]
        // I i.q.c.TableReader open partition /tmp/junit5370415536490581256/xyz/1970-01-01 [rowCount=1, partitionNameTxn=-1, transientRowCount=10000000, partitionIndex=0, partitionCount=1]
        // E i.q.c.h.p.JsonQueryProcessorState [27] internal error [q=`select count(*) from xyz where x > 0;`, ex=io.questdb.cairo.CairoException: [0] File not found: /tmp/junit5370415536490581256/xyz/1970-01-01/x.d
        AtomicLong count = new AtomicLong();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.endsWithAscii(path, "x.d") && count.incrementAndGet() == 4) {
                    return false;
                }
                return Files.exists(path);
            }
        };

        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withFilesFacade(ff)
                .run((engine, sqlExecutionContext) -> {
                    setupSql(engine);
                    engine.execute("create table xyz as (select x, timestamp_sequence(0, " + Micros.DAY_MICROS + ") ts from long_sequence(1)) timestamp(ts) Partition by DAY ", this.sqlExecutionContext);

                    // Cache query plan
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+xyz+where+x+%3E+0; HTTP/1.1\r\n",
                            "85\r\n" +
                                    "{\"query\":\"select count(*) from xyz where x > 0;\",\"columns\":[{\"name\":\"count\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );

                    // Add new commit
                    engine.execute("insert into xyz select x, timestamp_sequence(" + Micros.DAY_MICROS + ", 1) ts from long_sequence(10) ", this.sqlExecutionContext);

                    // Here fail expected
                    new SendAndReceiveRequestBuilder().withCompareLength(20).executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+xyz+where+x+%3E+0; HTTP/1.1\r\n" + SendAndReceiveRequestBuilder.RequestHeaders,
                            "8e\r\n" +
                                    "{\"query\":\"select count(*) from xyz where x > 0;\",\"error\":\"File not found: "
                    );

                    // Check that txn_scoreboard is fully unlocked, e.g., no reader scoreboard leaks after the failure
                    TableToken tableToken = engine.verifyTableName("xyz");
                    try (TxnScoreboard txnScoreboard = engine.getTxnScoreboard(tableToken)) {
                        long min = getMin(txnScoreboard);
                        Assert.assertTrue(2 == min || min == -1);
                        Assert.assertTrue(txnScoreboard.acquireTxn(10, 3));
                        Assert.assertTrue(txnScoreboard.isTxnAvailable(2));
                    }
                });
    }

    private static long getMin(TxnScoreboard scoreboard) {
        if (scoreboard instanceof TxnScoreboardV2) {
            return ((TxnScoreboardV2) scoreboard).getMin();
        } else {
            return ((TxnScoreboardV1) scoreboard).getMin();
        }
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

    private String generateImportCsv(int low, int hi, String... columnTemplates) {
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

    private void setupSql(CairoEngine engine) {
        sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, new BindVariableServiceImpl(engine.getConfiguration()));
    }

    private void testImportMisDetectsTimestampColumn(HttpServerConfigurationBuilder serverConfigBuilder, int rowCount) throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(serverConfigBuilder)
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    setupSql(engine);
                    engine.execute(
                            "create table trips(" +
                                    "timestamp TIMESTAMP," +
                                    "str STRING," +
                                    "i STRING" +
                                    ") timestamp(timestamp)", this.sqlExecutionContext
                    );
                    String request = PostHeader.replace("name=trips", "name=trips&skipLev=true") +
                            Request1DataHeader +
                            generateImportCsv(0, rowCount, "aaaaaaaaaaaaaaaaa,22222222222222222222,33333333333333333") +
                            Request1SchemaPart;

                    new SendAndReceiveRequestBuilder()
                            .withExpectSendDisconnect(true)
                            .execute(
                                    request,
                                    "HTTP/1.1 200 OK\r\n" +
                                            "Server: questDB/1.0\r\n" +
                                            "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                            "Transfer-Encoding: chunked\r\n" +
                                            "Content-Type: text/plain; charset=utf-8\r\n" +
                                            "\r\n" +
                                            "12\r\n" +
                                            "not a timestamp ''\r\n" +
                                            "00\r\n" +
                                            "\r\n"
                            );

                    engine.execute(
                            "insert into trips values (" +
                                    "'2021-07-20T00:01:00', 'ABC', 'DEF'" +
                                    ")", this.sqlExecutionContext
                    );
                });
    }

    private void testImportWithNoCacheSymbols() throws Exception {
        final int parallelCount = 2;
        final int insertCount = 1;
        final int importRowCount = 10;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    AtomicInteger success = new AtomicInteger();

                    String ddl1 = "(Col1+SYMBOL+NOCACHE+INDEX,Pickup_DateTime+TIMESTAMP," +
                            "DropOff_datetime+SYMBOL)+timestamp(Pickup_DateTime)";
                    String ddl2 = "(Col1+SYMBOL+NOCACHE+INDEX,Col2+STRING,Col3+STRING,Col4+STRING,Pickup_DateTime+TIMESTAMP)" +
                            "+timestamp(Pickup_DateTime)";
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
                                        "\r\n"
                        );

                        new Thread(() -> {
                            try {
                                try {
                                    for (int batch = 0; batch < 2; batch++) {
                                        int low = importRowCount / 2 * batch;
                                        int hi = importRowCount / 2 * (batch + 1);
                                        String requestTemplate =
                                                headers[tableIndex].replace("POST /upload?name=trips ", "POST /upload?name=" + tableName + " ")
                                                        + generateImportCsv(low, hi, csvTemplate[tableIndex])
                                                        + REQUEST_FOOTER;

                                        new SendAndReceiveRequestBuilder()
                                                .withCompareLength(15)
                                                .execute(requestTemplate, "HTTP/1.1 200 OK");
                                    }

                                    new SendAndReceiveRequestBuilder().executeMany(httpClient -> {
                                        for (int row = 0; row < importRowCount; row++) {
                                            final String request = "SELECT+Col1+FROM+" + tableName + "+WHERE+Col1%3D%27SYM-" + row + "%27; ";

                                            httpClient.executeWithStandardHeaders(
                                                    "GET /query?query=" + request + "HTTP/1.1\r\n",
                                                    "9" + ((stringLen(row) * 2) - 1) + "\r\n" +
                                                            "{\"query\":\"SELECT Col1 FROM " + tableName + " WHERE Col1='SYM-"
                                                            + row +
                                                            "';\",\"columns\":[{\"name\":\"Col1\",\"type\":\"SYMBOL\"}],\"timestamp\":-1," +
                                                            "\"dataset\":[[\"SYM-" + row + "\"]],\"count\":1}\r\n"
                                                            + "00\r\n"
                                                            + "\r\n"
                                            );
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
                            finished
                    );
                    Assert.assertEquals(
                            "Expected successful import count does not match actual imports",
                            totalImports,
                            success.get()
                    );
                });
    }

    private void testImportWithWrongTimestampSpecified() throws Exception {
        final int parallelCount = 2;
        final int insertCount = 9;
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(parallelCount)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    CountDownLatch countDownLatch = new CountDownLatch(parallelCount);
                    AtomicInteger success = new AtomicInteger();
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] response = new String[]{ValidImportResponse1, ValidImportResponse2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    for (int i = 0; i < parallelCount; i++) {
                        final int thread = i;
                        final String respTemplate = response[i];
                        final String requestTemplate = requests[i];
                        final String ddlCols = ddl[i];
                        final String tableName = "trip" + i;

                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                "GET /query?query=CREATE+TABLE+" + tableName + ddlCols + "; HTTP/1.1\r\n",
                                "0c\r\n" +
                                        "{\"ddl\":\"OK\"}\r\n" +
                                        "00\r\n" +
                                        "\r\n"
                        );

                        new Thread(() -> {
                            try {
                                for (int r = 0; r < insertCount; r++) {
                                    try {
                                        String timestamp = "";
                                        if (r > 0 && thread > 0) {
                                            timestamp = "&timestamp=Pickup_DateTime";
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
                            finished
                    );
                    Assert.assertEquals(
                            "Expected successful import count does not match actual imports",
                            totalImports,
                            success.get()
                    );
                });
    }
}
