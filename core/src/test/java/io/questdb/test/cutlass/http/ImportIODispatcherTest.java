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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxnScoreboard;
import io.questdb.cairo.TxnScoreboardV2;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.view.ViewCompileParker;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.getSendDelayNetworkFacade;

public class ImportIODispatcherTest extends AbstractTest {
    private static final Log LOG = LogFactory.getLog(ImportIODispatcherTest.class);
    private static final String PostHeader = """
            POST /upload?name=trips HTTP/1.1\r
            Host: localhost:9001\r
            User-Agent: curl/7.64.0\r
            Accept: */*\r
            Content-Length: 437760673\r
            Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r
            Expect: 100-continue\r
            \r
            """;
    private static final String REQUEST_FOOTER = "\r\n" +
            "--------------------------27d997ca93d2689d--";
    private static final String Request1DataHeader = """
            --------------------------27d997ca93d2689d\r
            Content-Disposition: form-data; name="data"; filename="fhv_tripdata_2017-02.csv"\r
            Content-Type: application/octet-stream\r
            \r
            Col1,Pickup_DateTime,DropOff_datetime\r
            """;
    private static final String Request1SchemaPart = """
            --------------------------27d997ca93d2689d\r
            Content-Disposition: form-data; name="schema"; filename="schema.json"\r
            Content-Type: application/octet-stream\r
            \r
            [\r
              {\r
                "name": "Col1",\r
                "type": "STRING"\r
              },\r
              {\r
                "name": "Pickup_DateTime",\r
                "type": "TIMESTAMP",\r
                "pattern": "yyyy-MM-dd HH:mm:ss"\r
              },\r
              {\r
                "name": "DropOff_datetime",\r
                "type": "VARCHAR"\r
              }\r
            ]\r
            \r
            """;
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
    private static final String Request2Header = """
            POST /upload?name=trips HTTP/1.1\r
            Host: localhost:9001\r
            User-Agent: curl/7.64.0\r
            Accept: */*\r
            Content-Length: 437760673\r
            Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r
            Expect: 100-continue\r
            \r
            --------------------------27d997ca93d2689d\r
            Content-Disposition: form-data; name="schema"; filename="schema.json"\r
            Content-Type: application/octet-stream\r
            \r
            [\r
              {\r
                "name": "Col1",\r
                "type": "STRING"\r
              },\r
              {\r
                "name": "Col2",\r
                "type": "STRING"\r
              },\r
              {\r
                "name": "Col3",\r
                "type": "STRING"\r
              },\r
              {\r
                "name": "Col4",\r
                "type": "STRING"\r
              },\r
              {\r
                "name": "Pickup_DateTime",\r
                "type": "TIMESTAMP",\r
                "pattern": "yyyy-MM-dd HH:mm:ss"\r
              }\r
            ]\r
            \r
            --------------------------27d997ca93d2689d\r
            Content-Disposition: form-data; name="data"; filename="table2.csv"\r
            Content-Type: application/octet-stream\r
            \r
            Col1,Col2,Col3,Col4,Pickup_DateTime\r
            """;
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
    // Backfill CSV rows, {designated timestamp, cnt}, in file order. A REFRESH LIMIT 1 HOUR view over a base row at
    // 02:00 takes backfill only into the 1h buckets that end at or before 01:00: rows at 01:00 and 01:30 fall in its
    // managed zone, the others in its frozen zone. With maxUncommittedRows=2 the import commits every two rows it
    // appends, then the rest.
    private static final String[][] BACKFILL_ROWS_ALL_FROZEN = {
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T00:01:43.000000Z", "4"},
            {"1970-01-01T00:01:44.000000Z", "5"},
            {"1970-01-01T00:01:45.000000Z", "6"}
    };
    // the latest frozen instant in the first batch, the first managed one in the second
    private static final String[][] BACKFILL_ROWS_BOUNDARY = {
            {"1970-01-01T00:59:59.999999Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T01:00:00.000000Z", "7"},
            {"1970-01-01T00:01:43.000000Z", "4"},
            {"1970-01-01T00:01:44.000000Z", "5"}
    };
    // a row without its designated timestamp before the batch with the managed-zone row
    private static final String[][] BACKFILL_ROWS_EMPTY_TIMESTAMP = {
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"", "9"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T01:30:00.000000Z", "7"},
            {"1970-01-01T00:01:43.000000Z", "4"}
    };
    private static final String[][] BACKFILL_ROWS_FOLLOW_UP = {
            {"1970-01-01T00:30:00.000000Z", "11"},
            {"1970-01-01T00:30:01.000000Z", "12"}
    };
    private static final String[][] BACKFILL_ROWS_MANAGED_IN_FIRST_BATCH = {
            {"1970-01-01T01:30:00.000000Z", "7"},
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T00:01:43.000000Z", "4"},
            {"1970-01-01T00:01:44.000000Z", "5"}
    };
    // the last batch is full, so an intermediate commit takes it, not the final one
    private static final String[][] BACKFILL_ROWS_MANAGED_IN_LAST_FULL_BATCH = {
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T00:01:43.000000Z", "4"},
            {"1970-01-01T01:30:00.000000Z", "7"}
    };
    // the last batch is a single row, which the final commit takes
    private static final String[][] BACKFILL_ROWS_MANAGED_IN_LAST_PARTIAL_BATCH = {
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T01:30:00.000000Z", "7"}
    };
    private static final String[][] BACKFILL_ROWS_MANAGED_IN_MIDDLE_BATCH = {
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T01:30:00.000000Z", "7"},
            {"1970-01-01T00:01:43.000000Z", "4"},
            {"1970-01-01T00:01:44.000000Z", "5"}
    };
    private static final String MULTIPART_BOUNDARY = "------------------------27d997ca93d2689d";
    // Four rows that an import sends in two writes: the first ends inside the second row, so the server takes the
    // structure, the writer and the first row before the test changes the target.
    private static final String[][] SPLIT_UPLOAD_ROWS = {
            {"1970-01-01T00:01:40.000000Z", "1"},
            {"1970-01-01T00:01:41.000000Z", "2"},
            {"1970-01-01T00:01:42.000000Z", "3"},
            {"1970-01-01T00:01:43.000000Z", "4"}
    };
    private static final String SPLIT_UPLOAD_SECOND_WRITE_START = ".000000Z,2\r\n";
    // the view's own maxUncommittedRows under the test configuration, above the row count of every case
    private static final int VIEW_MAX_UNCOMMITTED_ROWS = 1000;
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
    private final String ValidImportResponse1 = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: text/plain; charset=utf-8\r
            \r
            057c\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |\r
            |   Partition by  |                                              NONE  |                 |         |              |\r
            |      Timestamp  |                                              NONE  |                 |         |              |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |   Rows handled  |                                                24  |                 |         |              |\r
            |  Rows imported  |                                                24  |                 |         |              |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |              0  |                                              Col1  |                   STRING  |           0  |\r
            |              1  |                                   Pickup_DateTime  |                TIMESTAMP  |           0  |\r
            |              2  |                                  DropOff_datetime  |                  VARCHAR  |           0  |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            \r
            00\r
            \r
            """;
    private final String ValidImportResponse1Json = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: application/json; charset=utf-8\r
            \r
            0148\r
            {"status":"OK",\
            "location":"trips",\
            "rowsRejected":0,\
            "rowsImported":24,\
            "header":true,\
            "partitionBy":"DAY",\
            "timestamp":"Pickup_DateTime",\
            "columns":[\
            {"name":"Col1","type":"STRING","size":0,"errors":0},\
            {"name":"Pickup_DateTime","type":"TIMESTAMP","size":8,"errors":0},\
            {"name":"DropOff_datetime","type":"VARCHAR","size":0,"errors":0}\
            ]}\r
            00\r
            \r
            """;
    private final String ValidImportResponse2 = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: text/plain; charset=utf-8\r
            \r
            0666\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |\r
            |   Partition by  |                                              NONE  |                 |         |              |\r
            |      Timestamp  |                                   Pickup_DateTime  |                 |         |              |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |   Rows handled  |                                                24  |                 |         |              |\r
            |  Rows imported  |                                                24  |                 |         |              |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |              0  |                                              Col1  |                   STRING  |           0  |\r
            |              1  |                                              Col2  |                   STRING  |           0  |\r
            |              2  |                                              Col3  |                   STRING  |           0  |\r
            |              3  |                                              Col4  |                   STRING  |           0  |\r
            |              4  |                                   Pickup_DateTime  |                TIMESTAMP  |           0  |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            \r
            00\r
            \r
            """;
    private final String ValidImportResponse2Json = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: application/json; charset=utf-8\r
            \r
            012b\r
            {"status":"OK",\
            "location":"trips",\
            "rowsRejected":0,\
            "rowsImported":24,\
            "header":true,\
            "partitionBy":"NONE",\
            "columns":[\
            {"name":"Col1","type":"STRING","size":0,"errors":0},\
            {"name":"Pickup_DateTime","type":"TIMESTAMP","size":8,"errors":0},\
            {"name":"DropOff_datetime","type":"VARCHAR","size":0,"errors":0}\
            ]}\r
            00\r
            \r
            """;
    private final String WarningValidImportResponse1 = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: text/plain; charset=utf-8\r
            \r
            057c\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |      Location:  |                                             trips  |        Pattern  | Locale  |      Errors  |\r
            |   Partition by  |                                              NONE  |                 |         |  From Table  |\r
            |      Timestamp  |                                              NONE  |                 |         |  From Table  |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |   Rows handled  |                                                24  |                 |         |              |\r
            |  Rows imported  |                                                24  |                 |         |              |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            |              0  |                                              Col1  |                   STRING  |           0  |\r
            |              1  |                                   Pickup_DateTime  |                TIMESTAMP  |           0  |\r
            |              2  |                                  DropOff_datetime  |                  VARCHAR  |           0  |\r
            +-----------------------------------------------------------------------------------------------------------------+\r
            \r
            00\r
            \r
            """;
    private final String ImportCreateParamResponse = WarningValidImportResponse1
            .replace(
                    """
                            \r
                            |   Partition by  |                                              NONE  |                 |         |  From Table  |\r
                            |      Timestamp  |                                              NONE  |                 |         |  From Table  |""",
                    """
                            \r
                            |   Partition by  |                                              NONE  |                 |         |              |\r
                            |      Timestamp  |                                   Pickup_DateTime  |                 |         |              |"""
            );
    private final String WarningValidImportResponse1Json = """
            HTTP/1.1 200 OK\r
            Server: questDB/1.0\r
            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
            Transfer-Encoding: chunked\r
            Content-Type: application/json; charset=utf-8\r
            \r
            0187\r
            {"status":"OK",\
            "location":"trips",\
            "rowsRejected":0,\
            "rowsImported":24,\
            "header":true,\
            "partitionBy":"NONE",\
            "warnings":[\
            "Existing table timestamp column is used",\
            "Existing table PartitionBy is used"],\
            "columns":[\
            {"name":"Col1","type":"STRING","size":0,"errors":0},\
            {"name":"Pickup_DateTime","type":"TIMESTAMP","size":8,"errors":0},\
            {"name":"DropOff_datetime","type":"VARCHAR","size":0,"errors":0}\
            ]}\r
            00\r
            \r
            """;
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
                .run((engine, _) -> {
                    setupSql(engine);
                    final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                    engine.setPoolListener((_, _, name, event, _, _) -> {
                        if (event == PoolListener.EV_RETURN && Chars.equals("syms", name.getTableName())) {
                            waitForData.countDown();
                        }
                    });
                    new SendAndReceiveRequestBuilder().execute(
                            """
                                    POST /upload?name=syms&timestamp=ts1 HTTP/1.1\r
                                    Host: localhost:9001\r
                                    User-Agent: curl/7.64.0\r
                                    Accept: */*\r
                                    Content-Length: 437760673\r
                                    Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r
                                    Expect: 100-continue\r
                                    \r
                                    --------------------------27d997ca93d2689d\r
                                    Content-Disposition: form-data; name="schema"; filename="schema.json"\r
                                    Content-Type: application/octet-stream\r
                                    \r
                                    [\r
                                      {\r
                                        "name": "col1",\r
                                        "type": "SYMBOL",\r
                                        "index": "false"\r
                                      },\r
                                      {\r
                                        "name": "col2",\r
                                        "type": "SYMBOL",\r
                                        "index": "true"\r
                                      },\r
                                      {\r
                                        "name": "ts1",\r
                                        "type": "TIMESTAMP",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss"\r
                                      },\r
                                      {\r
                                        "name": "ts2",\r
                                        "type": "TIMESTAMP",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss"\r
                                      },\r
                                      {\r
                                        "name": "ts3",\r
                                        "type": "TIMESTAMP",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss"\r
                                      }\r
                                    ]\r
                                    \r
                                    --------------------------27d997ca93d2689d\r
                                    Content-Disposition: form-data; name="data"; filename="table2.csv"\r
                                    Content-Type: application/octet-stream\r
                                    \r
                                    col1,col2,ts1,ts2,ts3\r
                                    sym1,sym2,,2017-02-01 00:30:00,2017-02-01 00:30:01\r
                                    \r
                                    --------------------------27d997ca93d2689d--""",
                            """
                                    HTTP/1.1 200 OK\r
                                    Server: questDB/1.0\r
                                    Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                                    Transfer-Encoding: chunked\r
                                    Content-Type: text/plain; charset=utf-8\r
                                    \r
                                    0666\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |      Location:  |                                              syms  |        Pattern  | Locale  |      Errors  |\r
                                    |   Partition by  |                                              NONE  |                 |         |              |\r
                                    |      Timestamp  |                                               ts1  |                 |         |              |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |   Rows handled  |                                                 1  |                 |         |              |\r
                                    |  Rows imported  |                                                 0  |                 |         |              |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |              0  |                                              col1  |                   SYMBOL  |           0  |\r
                                    |              1  |                                              col2  |         (idx/256) SYMBOL  |           0  |\r
                                    |              2  |                                               ts1  |                TIMESTAMP  |           1  |\r
                                    |              3  |                                               ts2  |                TIMESTAMP  |           0  |\r
                                    |              4  |                                               ts3  |                TIMESTAMP  |           0  |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    \r
                                    00\r
                                    \r
                                    """
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
                .run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(executor -> {
                    executor.execute(
                            ValidImportRequest1.replace("VARCHAR", "SYMBOL"),
                            ValidImportResponse1.replace("VARCHAR", "SYMBOL").replace(" SYMBOL", "  SYMBOL")
                    );

                    executor.executeWithStandardHeaders(
                            "GET /query?query=select+*+from+trips HTTP/1.1\r\n",
                            """
                                    051b\r
                                    {"query":"select * from trips","columns":[{"name":"Col1","type":"STRING"},{"name":"Pickup_DateTime","type":"TIMESTAMP"},{"name":"DropOff_datetime","type":"SYMBOL"}],"timestamp":-1,"dataset":[["B00008","2017-02-01T00:30:00.000000Z",null],["B00008","2017-02-01T00:40:00.000000Z",null],["B00009","2017-02-01T00:50:00.000000Z",null],["B00013","2017-02-01T00:51:00.000000Z",null],["B00013","2017-02-01T01:41:00.000000Z",null],["B00013","2017-02-01T02:00:00.000000Z",null],["B00013","2017-02-01T03:53:00.000000Z",null],["B00013","2017-02-01T04:44:00.000000Z",null],["B00013","2017-02-01T05:05:00.000000Z",null],["B00013","2017-02-01T06:54:00.000000Z",null],["B00014","2017-02-01T07:45:00.000000Z",null],["B00014","2017-02-01T08:45:00.000000Z",null],["B00014","2017-02-01T09:46:00.000000Z",null],["B00014","2017-02-01T10:54:00.000000Z",null],["B00014","2017-02-01T11:45:00.000000Z",null],["B00014","2017-02-01T11:45:00.000000Z",null],["B00014","2017-02-01T11:45:00.000000Z",null],["B00014","2017-02-01T12:26:00.000000Z",null],["B00014","2017-02-01T12:55:00.000000Z",null],["B00014","2017-02-01T13:47:00.000000Z",null],["B00014","2017-02-01T14:05:00.000000Z",null],["B00014","2017-02-01T14:58:00.000000Z",null],["B00014","2017-02-01T15:33:00.000000Z",null],["B00014","2017-02-01T15:45:00.000000Z",null]],"count":24}\r
                                    00\r
                                    \r
                                    """
                    );

                    executor.executeWithStandardHeaders(
                            "GET /query?query=drop+table+trips HTTP/1.1\r\n",
                            """
                                    0c\r
                                    {"ddl":"OK"}\r
                                    00\r
                                    \r
                                    """
                    );

                    String request2 = ValidImportRequest2
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&timestamp=Pickup_DateTime&overwrite=true HTTP");

                    executor.execute(request2, ValidImportResponse2);

                    // This will try to hit the same execution plan as the first SELECT query
                    // but because table metadata changes, the old query plan is not valid anymore
                    // and produces NPE if used
                    executor.executeWithStandardHeaders(
                            "GET /query?query=select+*+from+trips HTTP/1.1\r\n",
                            """
                                    063e\r
                                    {"query":"select * from trips","columns":[{"name":"Col1","type":"STRING"},{"name":"Col2","type":"STRING"},{"name":"Col3","type":"STRING"},{"name":"Col4","type":"STRING"},{"name":"Pickup_DateTime","type":"TIMESTAMP"}],"timestamp":4,"dataset":[["B00008",null,null,null,"2017-02-01T00:30:00.000000Z"],["B00008",null,null,null,"2017-02-01T00:40:00.000000Z"],["B00009",null,null,null,"2017-02-01T00:50:00.000000Z"],["B00013",null,null,null,"2017-02-01T00:51:00.000000Z"],["B00013",null,null,null,"2017-02-01T01:41:00.000000Z"],["B00013",null,null,null,"2017-02-01T02:00:00.000000Z"],["B00013",null,null,null,"2017-02-01T03:53:00.000000Z"],["B00013",null,null,null,"2017-02-01T04:44:00.000000Z"],["B00013",null,null,null,"2017-02-01T05:05:00.000000Z"],["B00013",null,null,null,"2017-02-01T06:54:00.000000Z"],["B00014",null,null,null,"2017-02-01T07:45:00.000000Z"],["B00014",null,null,null,"2017-02-01T08:45:00.000000Z"],["B00014",null,null,null,"2017-02-01T09:46:00.000000Z"],["B00014",null,null,null,"2017-02-01T10:54:00.000000Z"],["B00014",null,null,null,"2017-02-01T11:45:00.000000Z"],["B00014",null,null,null,"2017-02-01T11:45:00.000000Z"],["B00014",null,null,null,"2017-02-01T11:45:00.000000Z"],["B00014",null,null,null,"2017-02-01T12:26:00.000000Z"],["B00014",null,null,null,"2017-02-01T12:55:00.000000Z"],["B00014",null,null,null,"2017-02-01T13:47:00.000000Z"],["B00014",null,null,null,"2017-02-01T14:05:00.000000Z"],["B00014",null,null,null,"2017-02-01T14:58:00.000000Z"],["B00014",null,null,null,"2017-02-01T15:33:00.000000Z"],["B00014",null,null,null,"2017-02-01T15:45:00.000000Z"]],"count":24}\r
                                    00\r
                                    \r
                                    """
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
                .run((engine, _) -> {
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
                                """
                                        HTTP/1.1 200 OK\r
                                        Server: questDB/1.0\r
                                        Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                                        Transfer-Encoding: chunked\r
                                        Content-Type: text/csv; charset=utf-8\r
                                        Content-Disposition: attachment; filename="questdb-query-0.csv"\r
                                        Keep-Alive: timeout=5, max=10000\r
                                        \r
                                        70\r
                                        "Col1","Pickup_DateTime","timestamp_sequence"\r
                                        "b","1970-01-01T00:00:00.000000Z","1970-01-01T00:01:40.000000Z"\r
                                        \r
                                        00\r
                                        \r
                                        """
                        );
                        executor.executeWithStandardHeaders(
                                "GET /query?query=drop+table+trips HTTP/1.1\r\n",
                                """
                                        0c\r
                                        {"ddl":"OK"}\r
                                        00\r
                                        \r
                                        """
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
                                """
                                        HTTP/1.1 200 OK\r
                                        Server: questDB/1.0\r
                                        Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                                        Transfer-Encoding: chunked\r
                                        Content-Type: text/csv; charset=utf-8\r
                                        Content-Disposition: attachment; filename="questdb-query-0.csv"\r
                                        Keep-Alive: timeout=5, max=10000\r
                                        \r
                                        0437\r
                                        "Col1","Col2","Col3","Col4","Pickup_DateTime"\r
                                        "B00008",,,,"2017-02-01T00:30:00.000000Z"\r
                                        "B00008",,,,"2017-02-01T00:40:00.000000Z"\r
                                        "B00009",,,,"2017-02-01T00:50:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T00:51:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T01:41:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T02:00:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T03:53:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T04:44:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T05:05:00.000000Z"\r
                                        "B00013",,,,"2017-02-01T06:54:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T07:45:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T08:45:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T09:46:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T10:54:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T11:45:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T11:45:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T11:45:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T12:26:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T12:55:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T13:47:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T14:05:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T14:58:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T15:33:00.000000Z"\r
                                        "B00014",,,,"2017-02-01T15:45:00.000000Z"\r
                                        \r
                                        00\r
                                        \r
                                        """
                        );
                    });
                });
    }

    @Test
    public void testImportIntoMatViewFailsAfterRefreshLimitRemovedMidImport() throws Exception {
        // An import keeps the writer it took when the backfill gate let it in. ALTER MATERIALIZED VIEW ... SET REFRESH
        // LIMIT 0 applies while the upload is under way: the import's next batch commit is refused with the gate's
        // message and the refusal fails the import, so no row lands and the view gains no WAL transaction. Once the
        // limit is back, the same upload goes through.
        getSimpleTester().run((engine, context) -> {
            // the materialized view state store
            engine.load();
            createBaseTable(engine, context, "base_price", "TIMESTAMP");
            createBackfillableMatViews(engine, context, "base_price", new ObjList<>("v"));
            final TableToken viewToken = engine.verifyTableName("v");
            final AtomicLong txnAfterAlter = new AtomicLong();
            final String request = importRequest("name=v&fmt=json&maxUncommittedRows=2", SPLIT_UPLOAD_ROWS);
            executeSplitUpload(
                    engine,
                    "v",
                    request,
                    () -> {
                        engine.execute("ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 0 HOUR", context);
                        drainWalAndMatViewQueues(engine);
                        txnAfterAlter.set(engine.getTableSequencerAPI().lastTxn(viewToken));
                    },
                    importErrorResponse(true, "cannot modify materialized view [view=v]")
            );
            drainWalAndMatViewQueues(engine);
            Assert.assertEquals(txnAfterAlter.get(), engine.getTableSequencerAPI().lastTxn(viewToken));
            assertQuery("SELECT ts, cnt FROM v")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tcnt
                            1970-01-01T02:00:00.000000Z\t1
                            """);

            engine.execute("ALTER MATERIALIZED VIEW v SET REFRESH LIMIT 1 HOUR", context);
            drainWalAndMatViewQueues(engine);
            new SendAndReceiveRequestBuilder().execute(request, importResponse(true, importOkJson("v", "TIMESTAMP", 4)));
            drainWalAndMatViewQueues(engine);
            assertQuery("SELECT ts, cnt FROM v")
                    .withEngine(engine)
                    .withContext(context)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tcnt
                            1970-01-01T00:01:40.000000Z\t1
                            1970-01-01T00:01:41.000000Z\t2
                            1970-01-01T00:01:42.000000Z\t3
                            1970-01-01T00:01:43.000000Z\t4
                            1970-01-01T02:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testImportIntoMatViewFailsAtRejectedBatch() throws Exception {
        // Each case imports into a fresh REFRESH LIMIT 1 HOUR view over a base row at 02:00 and commits every
        // maxUncommittedRows rows. The first batch the frozen-zone check rejects fails the import with the check's
        // message, in every atomicity mode and both response formats; the batches committed before it stay and no
        // row after it is written. MatViewBackfillModel derives the answer, the view's rows and its new WAL
        // transactions from the documented rule. The view then takes a clean import through its writer pool.
        final ImportAtomicity[] allModes = ImportAtomicity.values();
        final ImportAtomicity[] defaultMode = {ImportAtomicity.SKIP_COL};
        final ImportIntoMatViewCase[] cases = {
                new ImportIntoMatViewCase("first", BACKFILL_ROWS_MANAGED_IN_FIRST_BATCH, 2, true, false, allModes),
                new ImportIntoMatViewCase("middle", BACKFILL_ROWS_MANAGED_IN_MIDDLE_BATCH, 2, true, false, allModes),
                new ImportIntoMatViewCase("last", BACKFILL_ROWS_MANAGED_IN_LAST_FULL_BATCH, 2, true, false, allModes),
                new ImportIntoMatViewCase("boundary", BACKFILL_ROWS_BOUNDARY, 2, true, false, allModes),
                new ImportIntoMatViewCase("empty_ts", BACKFILL_ROWS_EMPTY_TIMESTAMP, 2, true, false, allModes),
                // the TIMESTAMP_NS view and the plain-text answer
                new ImportIntoMatViewCase("middle_ns", BACKFILL_ROWS_MANAGED_IN_MIDDLE_BATCH, 2, true, true, defaultMode),
                new ImportIntoMatViewCase("first_text", BACKFILL_ROWS_MANAGED_IN_FIRST_BATCH, 2, false, false, defaultMode),
                // controls, in one mode as the mode reaches neither path: the final commit of a single batch or of
                // the last one, and a clean import
                new ImportIntoMatViewCase("final_commit", BACKFILL_ROWS_MANAGED_IN_LAST_PARTIAL_BATCH, 2, true, false, defaultMode),
                new ImportIntoMatViewCase("one_batch", BACKFILL_ROWS_MANAGED_IN_FIRST_BATCH, 0, true, false, defaultMode),
                new ImportIntoMatViewCase("view_batch_size", BACKFILL_ROWS_MANAGED_IN_FIRST_BATCH, -1, true, false, defaultMode),
                new ImportIntoMatViewCase("clean", BACKFILL_ROWS_ALL_FROZEN, 2, true, false, defaultMode)
        };
        getSimpleTester().run(newChunkedTxnLogConfiguration(), (engine, context) -> {
            // the materialized view state store
            engine.load();
            createBaseTable(engine, context, "base_price", "TIMESTAMP");
            createBaseTable(engine, context, "base_price_ns", "TIMESTAMP_NS");
            // a fresh view for every import, all built together
            final ObjList<String> microsViews = new ObjList<>();
            final ObjList<String> nanosViews = new ObjList<>();
            for (ImportIntoMatViewCase importCase : cases) {
                for (ImportAtomicity atomicity : importCase.atomicities) {
                    (importCase.isNanos ? nanosViews : microsViews).add(importCase.getView(atomicity));
                }
            }
            createBackfillableMatViews(engine, context, "base_price", microsViews);
            createBackfillableMatViews(engine, context, "base_price_ns", nanosViews);

            for (ImportIntoMatViewCase importCase : cases) {
                for (ImportAtomicity atomicity : importCase.atomicities) {
                    final String view = importCase.getView(atomicity);
                    final TableToken viewToken = engine.verifyTableName(view);
                    int maxUncommittedRows = importCase.maxUncommittedRows;
                    String maxUncommittedRowsParam = "&maxUncommittedRows=" + maxUncommittedRows;
                    if (maxUncommittedRows < 0) {
                        try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
                            maxUncommittedRows = metadata.getMaxUncommittedRows();
                        }
                        Assert.assertEquals(VIEW_MAX_UNCOMMITTED_ROWS, maxUncommittedRows);
                        maxUncommittedRowsParam = "";
                    }
                    final long txnBeforeImports = engine.getTableSequencerAPI().lastTxn(viewToken);
                    final MatViewBackfillModel model = new MatViewBackfillModel(view, importCase.isNanos);
                    new SendAndReceiveRequestBuilder().execute(
                            importRequest(
                                    "name=" + view + (importCase.isJson ? "&fmt=json" : "") + maxUncommittedRowsParam + atomicity.urlParam,
                                    importCase.rows
                            ),
                            model.replayImport(importCase.rows, maxUncommittedRows, atomicity == ImportAtomicity.ABORT, importCase.isJson)
                    );
                    drainWalAndMatViewQueues(engine);
                    assertMatViewBackfill(engine, context, model, txnBeforeImports);

                    // the writer the import returned to the pool takes the next import
                    new SendAndReceiveRequestBuilder().execute(
                            importRequest("name=" + view + "&fmt=json&maxUncommittedRows=2", BACKFILL_ROWS_FOLLOW_UP),
                            model.replayImport(BACKFILL_ROWS_FOLLOW_UP, 2, false, true)
                    );
                    drainWalAndMatViewQueues(engine);
                    assertMatViewBackfill(engine, context, model, txnBeforeImports);
                }
            }
        });
    }

    @Test
    public void testImportIntoNonWalTableFailsAtFailedO3Commit() throws Exception {
        // A non-WAL table whose intermediate O3 commit fails on storage: the import fails at that commit with its
        // error, in every atomicity mode, rather than going on while the writer's next commit silently rolls the
        // failed batch back. The import's writer returns to the pool, which rolls the batch back, so the table keeps
        // only its own row and takes the same upload afterwards.
        final AtomicReference<String> failingTable = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                // the first open of a cnt column file of the armed table, which the O3 merge makes
                final String table = failingTable.get();
                if (table != null
                        && Utf8s.containsAscii(name, Files.SEPARATOR + table + Files.SEPARATOR)
                        && Utf8s.endsWithAscii(name, Files.SEPARATOR + "cnt.d")
                        && failingTable.compareAndSet(table, null)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        getSimpleTester().withFilesFacade(ff).run((engine, context) -> {
            for (ImportAtomicity atomicity : ImportAtomicity.values()) {
                final String table = "t_" + atomicity.name().toLowerCase();
                engine.execute("CREATE TABLE " + table + " (ts TIMESTAMP, cnt LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", context);
                engine.execute("INSERT INTO " + table + " VALUES ('1970-01-01T05:00:00.000000Z', 99)", context);
                final String request = importRequest(
                        "name=" + table + "&fmt=json&maxUncommittedRows=2" + atomicity.urlParam,
                        BACKFILL_ROWS_MANAGED_IN_FIRST_BATCH
                );
                // every imported row is older than the table's row, so the first batch commit merges into its partition
                failingTable.set(table);
                new SendAndReceiveRequestBuilder().execute(
                        request,
                        importErrorResponse(true, "bulk update failed and will be rolled back")
                );
                Assert.assertNull("the import did not reach the O3 merge", failingTable.get());
                Assert.assertEquals(0, engine.getBusyWriterCount());
                assertQuery("SELECT ts, cnt FROM " + table)
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tcnt
                                1970-01-01T05:00:00.000000Z\t99
                                """);

                new SendAndReceiveRequestBuilder().execute(request, importResponse(true, importOkJson(table, "TIMESTAMP", 5)));
                assertQuery("SELECT ts, cnt FROM " + table)
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tcnt
                                1970-01-01T00:01:41.000000Z\t2
                                1970-01-01T00:01:42.000000Z\t3
                                1970-01-01T00:01:43.000000Z\t4
                                1970-01-01T00:01:44.000000Z\t5
                                1970-01-01T01:30:00.000000Z\t7
                                1970-01-01T05:00:00.000000Z\t99
                                """);
            }
        });
    }

    @Test
    public void testImportIntoTableFailsAtRenameMidImport() throws Exception {
        // A WAL table renamed while an import uploads into it: the import's next commit, an intermediate batch commit
        // or the final one, finds the table under another name. The import answers with that error in every
        // atomicity mode, rather than dropping the connection or failing later with a distressed writer, and imports
        // no row; the renamed table takes the same upload afterwards.
        getSimpleTester().run((engine, context) -> {
            final String[] params = {
                    "&maxUncommittedRows=2",
                    "&maxUncommittedRows=0",
                    "&maxUncommittedRows=2" + ImportAtomicity.ABORT.urlParam
            };
            for (String param : params) {
                engine.execute("CREATE TABLE t (ts TIMESTAMP, cnt LONG) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
                executeSplitUpload(
                        engine,
                        "t",
                        importRequest("name=t&fmt=json" + param, SPLIT_UPLOAD_ROWS),
                        () -> engine.execute("RENAME TABLE t TO t2", context),
                        importErrorResponse(true, "cached query plan cannot be used because table schema has changed [table=t]")
                );
                drainWalQueue(engine);
                assertQuery("SELECT ts, cnt FROM t2")
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tcnt\n");

                new SendAndReceiveRequestBuilder().execute(
                        importRequest("name=t2&fmt=json" + param, SPLIT_UPLOAD_ROWS),
                        importResponse(true, importOkJson("t2", "TIMESTAMP", 4))
                );
                drainWalQueue(engine);
                assertQuery("SELECT ts, cnt FROM t2")
                        .withEngine(engine)
                        .withContext(context)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("""
                                ts\tcnt
                                1970-01-01T00:01:40.000000Z\t1
                                1970-01-01T00:01:41.000000Z\t2
                                1970-01-01T00:01:42.000000Z\t3
                                1970-01-01T00:01:43.000000Z\t4
                                """);
                engine.execute("DROP TABLE t2", context);
                drainWalQueue(engine);
            }
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
                .run((engine, _) -> {
                    AtomicBoolean locked = new AtomicBoolean(false);
                    engine.setPoolListener((_, _, name, event, _, _) -> {
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
                .run((_, _) -> new SendAndReceiveRequestBuilder().executeMany(_ -> {
                    String requestJson = ValidImportRequest1
                            .replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&fmt=json HTTP");
                    new SendAndReceiveRequestBuilder().execute(requestJson, ValidImportResponse2Json);
                    new SendAndReceiveRequestBuilder().execute(ValidImportRequest1, ValidImportResponse1);
                }));
    }

    @Test
    public void testImportOverwriteWaitsForViewCompile() throws Exception {
        // An import with overwrite=true drops the existing non-WAL table and creates it again from
        // the file. An ALTER of the table queued the recompile of the view over it, and the run
        // holds the table's reader when the import drops the table: the drop waits for the run
        // instead of failing the import as busy.
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withAlterTableStartWaitTimeout(Long.MAX_VALUE)
                .run((engine, sqlExecutionContext) -> {
                    // loads the view state store and its compile queue, as the server does at startup
                    engine.load();
                    engine.execute("CREATE TABLE trips (a INT)", sqlExecutionContext);
                    engine.execute("INSERT INTO trips VALUES (1), (2)", sqlExecutionContext);
                    Assert.assertFalse(engine.verifyTableName("trips").isWal());
                    engine.execute("CREATE VIEW v AS (SELECT * FROM trips)", sqlExecutionContext);
                    drainViewQueue(engine);
                    engine.execute("ALTER TABLE trips ADD COLUMN b INT", sqlExecutionContext);

                    final LogCapture capture = new LogCapture();
                    try {
                        ViewCompileParker.ofQueueDrain(engine, "trips", PoolListener.SRC_READER).executeWhileHolding(
                                capture,
                                "trips",
                                () -> new SendAndReceiveRequestBuilder().execute(
                                        ValidImportRequest1.replace("POST /upload?name=trips HTTP", "POST /upload?name=trips&overwrite=true HTTP"),
                                        ValidImportResponse1
                                ),
                                null
                        );
                    } finally {
                        capture.stop();
                    }
                    assertQuery("select count() from trips")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n24\n");
                    // creating the table again queued the compile of the view
                    drainViewQueue(engine);
                    final StringSink viewColumns = new StringSink();
                    try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("v"))) {
                        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                            viewColumns.put(metadata.getColumnName(i)).put(' ');
                        }
                    }
                    TestUtils.assertEquals("Col1 Pickup_DateTime DropOff_datetime ", viewColumns);
                });
    }

    @Test
    public void testImportSymbolIndexedFromSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, _) -> {
                    setupSql(engine);
                    final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                    engine.setPoolListener((_, _, name, event, _, _) -> {
                        if (event == PoolListener.EV_RETURN && Chars.equals("syms", name.getTableName())) {
                            waitForData.countDown();
                        }
                    });
                    new SendAndReceiveRequestBuilder().execute(
                            """
                                    POST /upload?name=syms&timestamp=ts HTTP/1.1\r
                                    Host: localhost:9001\r
                                    User-Agent: curl/7.64.0\r
                                    Accept: */*\r
                                    Content-Length: 437760673\r
                                    Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r
                                    Expect: 100-continue\r
                                    \r
                                    --------------------------27d997ca93d2689d\r
                                    Content-Disposition: form-data; name="schema"; filename="schema.json"\r
                                    Content-Type: application/octet-stream\r
                                    \r
                                    [\r
                                      {\r
                                        "name": "col1",\r
                                        "type": "SYMBOL",\r
                                        "index": "true"\r
                                      },\r
                                      {\r
                                        "name": "col2",\r
                                        "type": "SYMBOL",\r
                                        "index": "false"\r
                                      },\r
                                      {\r
                                        "name": "col3",\r
                                        "type": "SYMBOL"\r
                                      },\r
                                      {\r
                                        "name": "col4",\r
                                        "type": "STRING",\r
                                        "index": "true"\r
                                      },\r
                                      {\r
                                        "name": "ts",\r
                                        "type": "TIMESTAMP",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss"\r
                                      }\r
                                    ]\r
                                    \r
                                    --------------------------27d997ca93d2689d\r
                                    Content-Disposition: form-data; name="data"; filename="table2.csv"\r
                                    Content-Type: application/octet-stream\r
                                    \r
                                    col1,col2,col3,col4,ts\r
                                    sym1,sym2,,string here,2017-02-01 00:30:00\r
                                    \r
                                    --------------------------27d997ca93d2689d--""",
                            """
                                    HTTP/1.1 200 OK\r
                                    Server: questDB/1.0\r
                                    Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                                    Transfer-Encoding: chunked\r
                                    Content-Type: text/plain; charset=utf-8\r
                                    \r
                                    0666\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |      Location:  |                                              syms  |        Pattern  | Locale  |      Errors  |\r
                                    |   Partition by  |                                              NONE  |                 |         |              |\r
                                    |      Timestamp  |                                                ts  |                 |         |              |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |   Rows handled  |                                                 1  |                 |         |              |\r
                                    |  Rows imported  |                                                 1  |                 |         |              |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |              0  |                                              col1  |         (idx/256) SYMBOL  |           0  |\r
                                    |              1  |                                              col2  |                   SYMBOL  |           0  |\r
                                    |              2  |                                              col3  |                   SYMBOL  |           0  |\r
                                    |              3  |                                              col4  |                   STRING  |           0  |\r
                                    |              4  |                                                ts  |                TIMESTAMP  |           0  |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    \r
                                    00\r
                                    \r
                                    """
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
    public void testImportTsFromSchema() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, _) -> {
                    setupSql(engine);
                    final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                    engine.setPoolListener((_, _, name, event, _, _) -> {
                        if (event == PoolListener.EV_RETURN && Chars.equals("syms", name.getTableName())) {
                            waitForData.countDown();
                        }
                    });
                    new SendAndReceiveRequestBuilder().execute(
                            """
                                    POST /upload?name=syms&timestamp=ts1 HTTP/1.1\r
                                    Host: localhost:9001\r
                                    User-Agent: curl/7.64.0\r
                                    Accept: */*\r
                                    Content-Length: 437760673\r
                                    Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r
                                    Expect: 100-continue\r
                                    \r
                                    --------------------------27d997ca93d2689d\r
                                    Content-Disposition: form-data; name="schema"; filename="schema.json"\r
                                    Content-Type: application/octet-stream\r
                                    \r
                                    [\r
                                      {\r
                                        "name": "col1",\r
                                        "type": "SYMBOL",\r
                                        "index": "false"\r
                                      },\r
                                      {\r
                                        "name": "col2",\r
                                        "type": "SYMBOL",\r
                                        "index": "true"\r
                                      },\r
                                      {\r
                                        "name": "ts1",\r
                                        "type": "TIMESTAMP",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss.SSSUUUNNNz"\r
                                      },\r
                                      {\r
                                        "name": "ts2",\r
                                        "type": "TIMESTAMP_NS",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss"\r
                                      },\r
                                      {\r
                                        "name": "ts3",\r
                                        "type": "TIMESTAMP_NS",\r
                                        "pattern": "yyyy-MM-dd HH:mm:ss"\r
                                      }\r
                                    ]\r
                                    \r
                                    --------------------------27d997ca93d2689d\r
                                    Content-Disposition: form-data; name="data"; filename="table2.csv"\r
                                    Content-Type: application/octet-stream\r
                                    \r
                                    col1,col2,ts1,ts2,ts3\r
                                    sym1,sym2,2017-02-01 00:29:00.123456789z,2017-02-01 00:30:00,2017-02-01 00:30:01\r
                                    \r
                                    --------------------------27d997ca93d2689d--""",
                            """
                                    HTTP/1.1 200 OK\r
                                    Server: questDB/1.0\r
                                    Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                                    Transfer-Encoding: chunked\r
                                    Content-Type: text/plain; charset=utf-8\r
                                    \r
                                    0666\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |      Location:  |                                              syms  |        Pattern  | Locale  |      Errors  |\r
                                    |   Partition by  |                                              NONE  |                 |         |              |\r
                                    |      Timestamp  |                                               ts1  |                 |         |              |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |   Rows handled  |                                                 1  |                 |         |              |\r
                                    |  Rows imported  |                                                 1  |                 |         |              |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    |              0  |                                              col1  |                   SYMBOL  |           0  |\r
                                    |              1  |                                              col2  |         (idx/256) SYMBOL  |           0  |\r
                                    |              2  |                                               ts1  |                TIMESTAMP  |           0  |\r
                                    |              3  |                                               ts2  |             TIMESTAMP_NS  |           0  |\r
                                    |              4  |                                               ts3  |             TIMESTAMP_NS  |           0  |\r
                                    +-----------------------------------------------------------------------------------------------------------------+\r
                                    \r
                                    00\r
                                    \r
                                    """
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
                        Assert.assertEquals(ColumnType.TIMESTAMP_NANO, meta.getColumnType("ts2"));
                        Assert.assertFalse(meta.isColumnIndexed(3));
                        Assert.assertEquals(ColumnType.TIMESTAMP_NANO, meta.getColumnType("ts3"));
                        Assert.assertFalse(meta.isColumnIndexed(4));
                        TestUtils.assertReader("""
                                col1\tcol2\tts1\tts2\tts3
                                sym1\tsym2\t2017-02-01T00:29:00.123456Z\t2017-02-01T00:30:00.000000000Z\t2017-02-01T00:30:01.000000000Z
                                """, reader, new StringSink());
                    }
                });
    }

    @Test
    public void testImportUsesQuotedMultipartFilenameWithSemicolon() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    new SendAndReceiveRequestBuilder().execute("""
                            POST /imp?fmt=json&forceHeader=true&delimiter=%2C HTTP/1.1\r
                            Host: localhost:9001\r
                            User-Agent: curl/7.64.0\r
                            Accept: */*\r
                            Content-Length: 437760673\r
                            Content-Type: multipart/form-data; boundary=------------------------27d997ca93d2689d\r
                            Expect: 100-continue\r
                            \r
                            --------------------------27d997ca93d2689d\r
                            Content-Disposition: form-data; name="schema"; filename="schema.json"\r
                            Content-Type: application/octet-stream\r
                            \r
                            [{"name":"x","type":"INT"}]\r
                            --------------------------27d997ca93d2689d\r
                            Content-Disposition: form-data; name="data"; filename="a;b.csv"\r
                            Content-Type: application/octet-stream\r
                            \r
                            x\r
                            1\r
                            --------------------------27d997ca93d2689d--""", """
                            HTTP/1.1 200 OK\r
                            Server: questDB/1.0\r
                            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                            Transfer-Encoding: chunked\r
                            Content-Type: application/json; charset=utf-8\r
                            \r
                            a3\r
                            {"status":"OK","location":"a;b.csv","rowsRejected":0,"rowsImported":1,"header":true,"partitionBy":"NONE","columns":[{"name":"x","type":"INT","size":4,"errors":0}]}\r
                            00\r
                            \r
                            """);

                    drainWalQueue(engine);
                    assertQuery("select x from \"a;b.csv\"")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .expectSize()
                            .returns("x\n1\n");
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
                .run((_, _) -> new SendAndReceiveRequestBuilder().withExpectSendDisconnect(true).execute(ImportCreateParamRequestFalse, ImportCreateParamResponse));
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
                    assertQuery("select count() from trips")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .expectSize()
                            .noRandomAccess()
                            .returns("count\n24\n");
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
                                    """
                                            \r
                                            |   Partition by  |                                              NONE  |                 |         |  From Table  |\r
                                            |      Timestamp  |                                              NONE  |                 |         |  From Table  |""",
                                    """
                                            \r
                                            |   Partition by  |                                               DAY  |                 |         |              |\r
                                            |      Timestamp  |                                   Pickup_DateTime  |                 |         |              |"""
                            );
                    new SendAndReceiveRequestBuilder().execute(request, response);

                    drainWalQueue(engine);
                    assertQuery("select count() from trips")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .returnsOnce("count\n24\n");

                    engine.execute("alter table trips dedup upsert keys(Pickup_DateTime)", sqlExecutionContext);

                    // resend the same request
                    new SendAndReceiveRequestBuilder().execute(request, response);
                    drainWalQueue(engine);

                    // check deduplication worked
                    assertQuery("select count() from trips")
                            .withEngine(engine)
                            .withContext(sqlExecutionContext)
                            .noLeakCheck()
                            .returnsOnce("count\n24\n");
                });
    }

    @Test
    public void testImportWithTimestampAndPartitionByOverwriteJson() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((_, _) -> {
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = requests[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            """
                                    0c\r
                                    {"ddl":"OK"}\r
                                    00\r
                                    \r
                                    """
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
                .run((_, _) -> {
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = requests[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            """
                                    0c\r
                                    {"ddl":"OK"}\r
                                    00\r
                                    \r
                                    """
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
                .run((_, _) -> {
                    String[] requests = new String[]{ValidImportRequest1, ValidImportRequest2};
                    String[] ddl = new String[]{DdlCols1, DdlCols2};

                    final String requestTemplate = requests[0];
                    final String ddlCols = ddl[0];

                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=CREATE+TABLE+trips" + ddlCols + "; HTTP/1.1\r\n",
                            """
                                    0c\r
                                    {"ddl":"OK"}\r
                                    00\r
                                    \r
                                    """
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
                .run((engine, _) -> {
                    setupSql(engine);
                    engine.execute("create table xyz as (select x, timestamp_sequence(0, " + Micros.DAY_MICROS + ") ts from long_sequence(1)) timestamp(ts) Partition by DAY ", this.sqlExecutionContext);

                    // Cache query plan
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=select+count(*)+from+xyz+where+x+%3E+0; HTTP/1.1\r\n",
                            """
                                    85\r
                                    {"query":"select count(*) from xyz where x > 0;","columns":[{"name":"count","type":"LONG"}],"timestamp":-1,"dataset":[[1]],"count":1}\r
                                    00\r
                                    \r
                                    """
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
                        long min = ((TxnScoreboardV2) txnScoreboard).getMin();
                        Assert.assertTrue(2 == min || min == -1);
                        Assert.assertTrue(txnScoreboard.acquireTxn(10, 3, 3));
                        Assert.assertTrue(txnScoreboard.isTxnAvailable(2));
                    }
                });
    }

    // Views of the base table with REFRESH LIMIT 1 HOUR: over the base row at 02:00 their frozen zone ends at 01:00.
    private static void createBackfillableMatViews(
            CairoEngine engine,
            SqlExecutionContext context,
            String baseTable,
            ObjList<String> views
    ) throws SqlException {
        for (int i = 0, n = views.size(); i < n; i++) {
            engine.execute("CREATE MATERIALIZED VIEW " + views.getQuick(i) + " AS (SELECT ts, count() cnt FROM " + baseTable + " SAMPLE BY 1h) PARTITION BY DAY", context);
        }
        drainWalAndMatViewQueues(engine);
        for (int i = 0, n = views.size(); i < n; i++) {
            engine.execute("ALTER MATERIALIZED VIEW " + views.getQuick(i) + " SET REFRESH LIMIT 1 HOUR", context);
        }
        drainWalAndMatViewQueues(engine);
        for (int i = 0, n = views.size(); i < n; i++) {
            Assert.assertTrue(engine.isBackfillableMatView(engine.verifyTableName(views.getQuick(i))));
        }
    }

    private static void createBaseTable(
            CairoEngine engine,
            SqlExecutionContext context,
            String baseTable,
            String timestampType
    ) throws SqlException {
        engine.execute("CREATE TABLE " + baseTable + " (ts " + timestampType + ", val INT) TIMESTAMP(ts) PARTITION BY DAY WAL", context);
        engine.execute("INSERT INTO " + baseTable + " VALUES ('1970-01-01T02:00:00.000000Z', 1)", context);
        drainWalQueue(engine);
    }

    // Sends the import request in two writes on one connection, the first ending inside the second CSV row. Runs the
    // action between the writes, once the import has taken the target's writer, then reads the answer.
    private static void executeSplitUpload(
            CairoEngine engine,
            String target,
            String request,
            SplitUploadAction betweenWrites,
            String expectedResponse
    ) throws Exception {
        final SOCountDownLatch importTookWriter = new SOCountDownLatch(1);
        final AtomicBoolean isWriterTaken = new AtomicBoolean();
        engine.setPoolListener((factoryType, _, tableToken, event, _, _) -> {
            if (tableToken != null && Chars.equals(target, tableToken.getTableName())) {
                if (factoryType == PoolListener.SRC_WAL_WRITER && (event == PoolListener.EV_GET || event == PoolListener.EV_CREATE)) {
                    isWriterTaken.set(true);
                } else if (factoryType == PoolListener.SRC_TABLE_METADATA && event == PoolListener.EV_RETURN && isWriterTaken.get()) {
                    // CairoTextWriter.prepareTable() reads the target's partitioning after it took the writer; the
                    // import makes no other engine call before it parses the rows
                    importTookWriter.countDown();
                }
            }
        });
        final int splitAt = request.indexOf(SPLIT_UPLOAD_SECOND_WRITE_START);
        final SendAndReceiveRequestBuilder client = new SendAndReceiveRequestBuilder();
        final long fd = client.connectAndSendRequest(request.substring(0, splitAt));
        try {
            Assert.assertTrue("the import did not take the writer", importTookWriter.await(TimeUnit.SECONDS.toNanos(30)));
            engine.setPoolListener(null);
            betweenWrites.run();
            final String secondWrite = request.substring(splitAt);
            final int bufSize = Math.max(expectedResponse.length(), secondWrite.length()) * 2;
            final long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
            try {
                client.executeExplicit(secondWrite, fd, expectedResponse, bufSize, buf, null);
            } finally {
                Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            engine.setPoolListener(null);
            NetworkFacadeImpl.INSTANCE.close(fd);
        }
    }

    private static String importErrorResponse(boolean isJson, String message) {
        return importResponse(isJson, isJson ? "{\"status\":\"" + message + "\"}" : message);
    }

    private static String importOkJson(String table, String timestampType, int rowsImported) {
        return "{\"status\":\"OK\"," +
                "\"location\":\"" + table + "\"," +
                "\"rowsRejected\":0," +
                "\"rowsImported\":" + rowsImported + "," +
                "\"header\":true," +
                "\"partitionBy\":\"DAY\"," +
                "\"timestamp\":\"ts\"," +
                "\"columns\":[" +
                "{\"name\":\"ts\",\"type\":\"" + timestampType + "\",\"size\":8,\"errors\":0}," +
                "{\"name\":\"cnt\",\"type\":\"LONG\",\"size\":8,\"errors\":0}" +
                "]}";
    }

    // A POST /imp of a CSV with the header ts,cnt and the rows, each {ts, cnt}.
    private static String importRequest(String urlQuery, String[][] rows) {
        final StringBuilder body = new StringBuilder()
                .append("--").append(MULTIPART_BOUNDARY).append("\r\n")
                .append("Content-Disposition: form-data; name=\"data\"; filename=\"backfill.csv\"\r\n")
                .append("Content-Type: application/octet-stream\r\n")
                .append("\r\n")
                .append("ts,cnt\r\n");
        for (String[] row : rows) {
            body.append(row[0]).append(',').append(row[1]).append("\r\n");
        }
        body.append("\r\n--").append(MULTIPART_BOUNDARY).append("--");
        return "POST /imp?" + urlQuery + " HTTP/1.1\r\n" +
                "Host: localhost:9001\r\n" +
                "User-Agent: curl/7.64.0\r\n" +
                "Accept: */*\r\n" +
                "Content-Length: " + body.length() + "\r\n" +
                "Content-Type: multipart/form-data; boundary=" + MULTIPART_BOUNDARY + "\r\n" +
                "\r\n" +
                body;
    }

    // The /imp answer: HTTP 200 with the body in one chunk, whose size goes in an even number of hex digits.
    private static String importResponse(boolean isJson, String body) {
        final String chunkSize = Integer.toHexString(body.getBytes(StandardCharsets.UTF_8).length);
        return "HTTP/1.1 200 OK\r\n" +
                "Server: questDB/1.0\r\n" +
                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                "Transfer-Encoding: chunked\r\n" +
                "Content-Type: " + (isJson ? "application/json" : "text/plain") + "; charset=utf-8\r\n" +
                "\r\n" +
                (chunkSize.length() % 2 == 0 ? "" : "0") + chunkSize + "\r\n" +
                body + "\r\n" +
                "00\r\n" +
                "\r\n";
    }

    // wal_transactions() reports the row count and the timestamp range of a WAL transaction only from the chunked
    // sequencer transaction log
    private static DefaultTestCairoConfiguration newChunkedTxnLogConfiguration() {
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public int getDefaultSeqPartTxnCount() {
                return 10;
            }
        };
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

    private void assertMatViewBackfill(
            CairoEngine engine,
            SqlExecutionContext context,
            MatViewBackfillModel model,
            long txnBeforeImports
    ) throws Exception {
        assertQuery("SELECT ts, cnt FROM " + model.view)
                .withEngine(engine)
                .withContext(context)
                .noLeakCheck()
                .timestamp("ts")
                .expectSize()
                .returns(model.getExpectedViewRows());
        assertQuery("SELECT minTimestamp, maxTimestamp, rowCount FROM wal_transactions('" + model.view + "') WHERE sequencerTxn > " + txnBeforeImports)
                .withEngine(engine)
                .withContext(context)
                .noLeakCheck()
                .noRandomAccess()
                .returns(model.getExpectedTxns());
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
                .run((engine, _) -> {
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
                                    """
                                            HTTP/1.1 200 OK\r
                                            Server: questDB/1.0\r
                                            Date: Thu, 1 Jan 1970 00:00:00 GMT\r
                                            Transfer-Encoding: chunked\r
                                            Content-Type: text/plain; charset=utf-8\r
                                            \r
                                            12\r
                                            not a timestamp ''\r
                                            00\r
                                            \r
                                            """
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
                .run((_, _) -> {
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
                                """
                                        0c\r
                                        {"ddl":"OK"}\r
                                        00\r
                                        \r
                                        """
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
                .run((_, _) -> {
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
                                """
                                        0c\r
                                        {"ddl":"OK"}\r
                                        00\r
                                        \r
                                        """
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

    // /imp's atomicity modes and the URL parameter that selects each; skipCol, the default, is the absent parameter.
    private enum ImportAtomicity {
        SKIP_COL(""),
        SKIP_ROW("&atomicity=skipRow"),
        ABORT("&atomicity=abort");

        private final String urlParam;

        ImportAtomicity(String urlParam) {
            this.urlParam = urlParam;
        }
    }

    @FunctionalInterface
    private interface SplitUploadAction {
        void run() throws Exception;
    }

    // maxUncommittedRows below zero leaves the parameter out, so the import takes the view's own.
    private record ImportIntoMatViewCase(
            String name,
            String[][] rows,
            int maxUncommittedRows,
            boolean isJson,
            boolean isNanos,
            ImportAtomicity... atomicities
    ) {
        String getView(ImportAtomicity atomicity) {
            return "v_" + name + '_' + atomicity.name().toLowerCase();
        }
    }

    /**
     * The frozen-zone rule of a view with REFRESH LIMIT 1 HOUR over a base table whose latest row is at 02:00, as
     * the view documents it: a backfill commit lands only when the 1h bucket of its latest row ends at or before the
     * boundary bucket floor, floor_1h(02:00 - 1h) = 01:00; otherwise the whole commit is refused. Replays imports into
     * the view commit by commit and keeps what the view holds afterwards: its rows and the WAL transactions the
     * imports added.
     */
    private static class MatViewBackfillModel {
        private static final long BOUNDARY_BUCKET_FLOOR = Micros.HOUR_MICROS;
        private static final DateTimeFormatter MICROS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
                .withZone(ZoneOffset.UTC);
        private final StringBuilder expectedTxns = new StringBuilder("minTimestamp\tmaxTimestamp\trowCount\n");
        private final boolean isNanos;
        private final String view;
        private final TreeMap<Long, Long> viewRows = new TreeMap<>();

        MatViewBackfillModel(String view, boolean isNanos) {
            this.view = view;
            this.isNanos = isNanos;
            // the row the refresh built from the base row at 02:00
            viewRows.put(2 * Micros.HOUR_MICROS, 1L);
        }

        String getExpectedTxns() {
            return expectedTxns.toString();
        }

        String getExpectedViewRows() {
            final StringBuilder rows = new StringBuilder("ts\tcnt\n");
            for (Map.Entry<Long, Long> row : viewRows.entrySet()) {
                rows.append(format(row.getKey(), isNanos)).append('\t').append(row.getValue()).append('\n');
            }
            return rows.toString();
        }

        // Replays an import of the rows in file order: commits every maxUncommittedRows appended rows, then the rest.
        // Returns the answer the import gets.
        String replayImport(String[][] rows, int maxUncommittedRows, boolean isAbort, boolean isJson) {
            final ArrayList<long[]> batch = new ArrayList<>();
            int skippedRowCount = 0;
            for (int line = 0; line < rows.length; line++) {
                if (rows[line][0].isEmpty()) {
                    // a row without its designated timestamp is a row error: the import skips the row, or in abort
                    // mode rolls back the open batch and fails with the row's position
                    if (isAbort) {
                        return importErrorResponse(isJson, "bad syntax [line=" + line + ", col=0]");
                    }
                    skippedRowCount++;
                    continue;
                }
                batch.add(new long[]{parseMicros(rows[line][0]), Long.parseLong(rows[line][1])});
                if (batch.size() == maxUncommittedRows) {
                    final String rejection = commit(batch);
                    if (rejection != null) {
                        return importErrorResponse(isJson, rejection);
                    }
                }
            }
            final String rejection = batch.isEmpty() ? null : commit(batch);
            if (rejection != null) {
                return importErrorResponse(isJson, rejection);
            }
            Assert.assertTrue("no case expects a text answer or skipped rows from a clean import", isJson && skippedRowCount == 0);
            return importResponse(true, importOkJson(view, isNanos ? "TIMESTAMP_NS" : "TIMESTAMP", rows.length));
        }

        private static String format(long micros, boolean isNanos) {
            final Instant instant = Instant.ofEpochSecond(Math.floorDiv(micros, Micros.SECOND_MICROS), Math.floorMod(micros, Micros.SECOND_MICROS) * 1000);
            return MICROS_FORMAT.format(instant) + (isNanos ? "000Z" : "Z");
        }

        private static long parseMicros(String timestamp) {
            final Instant instant = Instant.parse(timestamp);
            return instant.getEpochSecond() * Micros.SECOND_MICROS + instant.getNano() / 1000;
        }

        // Returns the refusal of the batch, or null when it lands.
        private String commit(ArrayList<long[]> batch) {
            long minTimestamp = Long.MAX_VALUE;
            long maxTimestamp = Long.MIN_VALUE;
            for (int i = 0, n = batch.size(); i < n; i++) {
                minTimestamp = Math.min(minTimestamp, batch.get(i)[0]);
                maxTimestamp = Math.max(maxTimestamp, batch.get(i)[0]);
            }
            final long bucketLo = maxTimestamp - Math.floorMod(maxTimestamp, Micros.HOUR_MICROS);
            final long bucketHi = bucketLo + Micros.HOUR_MICROS;
            if (bucketHi > BOUNDARY_BUCKET_FLOOR) {
                return "backfill row falls in or past the managed zone of materialized view [view=" + view
                        + ", maxRowTs=" + format(maxTimestamp, isNanos)
                        + ", rowBucket=[" + format(bucketLo, isNanos) + ", " + format(bucketHi, isNanos)
                        + "), boundaryBucketFloor=" + format(BOUNDARY_BUCKET_FLOOR, isNanos)
                        + "]; backfill bucket end must be at-or-before the boundary bucket floor";
            }
            for (int i = 0, n = batch.size(); i < n; i++) {
                viewRows.put(batch.get(i)[0], batch.get(i)[1]);
            }
            // wal_transactions() reports microseconds whatever the timestamp type of the table
            expectedTxns.append(format(minTimestamp, false)).append('\t')
                    .append(format(maxTimestamp, false)).append('\t')
                    .append(batch.size()).append('\n');
            batch.clear();
            return null;
        }
    }
}
