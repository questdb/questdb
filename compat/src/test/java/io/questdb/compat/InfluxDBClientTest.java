/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.compat;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.compat.InfluxDBUtils.assertRequestErrorContains;

public class InfluxDBClientTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAppendErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                serverMain.compile("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();

                    assertRequestErrorContains(influxDB, points, "ex_tbl b\\\"c=1024 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl; invalid column name: b\\\"c\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "ex_tbl b=1024 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: b; cast error from protocol type: FLOAT to column type: BYTE\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "ex_tbl b=1024i 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: b; line protocol value: 1024 is out bounds of column type: BYTE\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "ex_tbl i=1024.2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: i; cast error from protocol type: FLOAT to column type: INT\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "ex_tbl str=1024.2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: str; cast error from protocol type: FLOAT to column type: STRING\",\"line\":1,\"errorId\":");
                }
            }
        });
    }

    @Test
    public void testColumnsCanBeAddedWithoutCommit() throws Exception {
        int count = 10000;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS.getEnvVarName(), String.valueOf(count)
            )) {
                serverMain.start();
                serverMain.compile("create table wal_low_max_uncomitted(sym symbol, ts timestamp) " +
                        "timestamp(ts) partition by DAY WAL WITH maxUncommittedRows=100");
                List<String> lines = new ArrayList<>();
                String goodLine = "wal_low_max_uncomitted,sym=aaa\n";
                for (int i = 0; i < count; i++) {
                    lines.add(goodLine);
                }

                // New column added
                lines.add("wal_low_max_uncomitted i=123i\n");
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    // Bad line which should roll back the transaction
                    assertRequestErrorContains(influxDB, lines, "ailed to parse line protocol:errors encountered on line(s):" +
                            "\\nerror in line 10002: Could not parse entire line. Symbol value is missing: bla");
                }

                serverMain.waitWalTxnApplied("wal_low_max_uncomitted");
                serverMain.assertSql("SELECT count() FROM wal_low_max_uncomitted", "count\n0\n");
            }
        });
    }

    @Test
    public void testCreateTableError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                serverMain.compile("create table wal_not_here(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp)");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {

                    List<String> points = new ArrayList<>();
                    assertRequestErrorContains(influxDB, points, "badPo\"int,a3=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: badPo\\\"int; invalid table name\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,bad\"symbol=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: badPoint; invalid column name: bad\\\"symbol\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,symbol=2 bad\\\\column=1 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\nerror in line 1: table: badPoint; invalid column name: bad\\\\" +
                            "\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,symbol=2 bad/column=1 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\nerror in line 1: table: badPoint; invalid column name: bad/" +
                            "\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,symbol=2 colu+mn=1 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\nerror in line 1: table: badPoint; invalid column name: colu+mn" +
                            "\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,sym+bol=2 column=1 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\nerror in line 1: table: badPoint; invalid column name: sym+bol" +
                            "\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "wal_not_here a=1,b=1 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: wal_not_here; cannot insert in non-WAL table\",\"line\":1,\"errorId\":");

                }
            }
        });
    }

    @Test
    public void testErrorDoesNotFitResponseBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "4096",
                    PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "512"
            )) {
                serverMain.start();
                serverMain.compile("create table wal_not_here(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp)");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();

                    StringSink sink = new StringSink();
                    for (int i = 0; i < 1024; i++) {
                        sink.put("a");
                    }
                    sink.put(" f=123 1233456\n");

                    points.add(sink.toString());
                    assertRequestErrorContains(influxDB, points, sink.toString(),
                            "{\"code\":\"invalid\",\"message\":\"failed to parse line protocol:errors encountered on line(s):\\nerror in line 1: table: aaaa",
                            "\"line\":1,\"errorId\":\""
                    );

                }
            }
        });
    }

    @Test
    public void testInsertWithIlpHttp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.waitWalTxnApplied(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttpParallelManyTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 10_000;

                int threads = 5;
                ObjList<Thread> threadList = new ObjList<>();
                AtomicReference<Throwable> error = new AtomicReference<>();

                for (int i = 0; i < threads; i++) {
                    final int threadNo = i;
                    threadList.add(new Thread(() -> {
                        try {
                            sendIlp(tableName + threadNo, count, serverMain);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            error.set(e);
                        }
                    }));
                    threadList.getLast().start();
                }

                for (int i = 0; i < threads; i++) {
                    threadList.getQuick(i).join();
                }

                LOG.info().$("== all threads finished ==").$();

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }

                for (int i = 0; i < threads; i++) {
                    String tn = "h2o_feet" + i;
                    serverMain.waitWalTxnApplied(tn, 2);
                    serverMain.assertSql("SELECT count() FROM " + tn, "count\n" + count + "\n");
                    serverMain.assertSql("SELECT sum(water_level) FROM " + tn, "sum\n" + (count * (count - 1) / 2) + "\n");
                }
            }
        });
    }

    @Test
    public void testInsertWithIlpHttpParallelOneTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 10_000;

                int threads = 5;
                ObjList<Thread> threadList = new ObjList<>();
                AtomicReference<Throwable> error = new AtomicReference<>();

                for (int i = 0; i < threads; i++) {
                    threadList.add(new Thread(() -> {
                        try {
                            sendIlp(tableName, count, serverMain);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            error.set(e);
                        }
                    }));
                    threadList.getLast().start();
                }

                for (int i = 0; i < threads; i++) {
                    threadList.getQuick(i).join();
                }

                LOG.info().$("== all threads finished ==").$();

                if (error.get() != null) {
                    throw new RuntimeException(error.get());
                }

                serverMain.waitWalTxnApplied(tableName, threads * 2);
                serverMain.assertSql("SELECT count() FROM " + tableName, "count\n" + count * threads + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM " + tableName, "sum\n" + (count * (count - 1) / 2) * threads + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttpServerKeepAliveOff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_SERVER_KEEP_ALIVE.getEnvVarName(), "false"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.waitWalTxnApplied(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testLineDoesNotFitBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "512",
                    PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), "15"
            )) {
                serverMain.start();
                serverMain.compile("create table wal_not_here(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp)");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();

                    // Fail on first line
                    points.add("very_long_table_name_very_very_long,tag1=value1 " +
                            "very_long_field_name_very_very_long1=92827743.02924732," +
                            "very_long_field_name_very_very_long2=92827743.02924732," +
                            "very_long_field_name_very_very_long3=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field=92827791");

                    assertRequestErrorContains(influxDB, points, "{\"code\":\"request too large\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):unable to read data: ILP line does not fit QuestDB ILP buffer size\"," +
                            "\"line\":1,\"errorId\":");

                    // Fail on second line
                    points.add("very_long_table_name_very_very_long,tag1=value1 " +
                            "very_long_field_name_very_very_long1=92827743.02924732," +
                            "very_long_field_name_very_very_long2=92827743.02924732," +
                            "very_long_field_name_very_very_long3=92827743.02924732");
                    points.add("very_long_table_name_very_very_long,tag1=value1 " +
                            "very_long_field_name_very_very_long1=92827743.02924732," +
                            "very_long_field_name_very_very_long2=92827743.02924732," +
                            "very_long_field_name_very_very_long3=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field_name_very_very_long4=92827743.02924732," +
                            "very_long_field=92827791");

                    assertRequestErrorContains(influxDB, points, "{\"code\":\"request too large\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):unable to read data: ILP line does not fit QuestDB ILP buffer size\"," +
                            "\"line\":2,\"errorId\":");
                }
            }
        });
    }

    @Test
    public void testMalformedLines() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {

                    List<String> points = new ArrayList<>();
                    points.add("good_point,sym=a str=\"abdc\",num=1 1233456\n");
                    assertRequestErrorContains(influxDB, points, "badPoint a3 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 2: Could not parse entire line. Field value is missing: a3\"," +
                            "\"line\":2,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,bad,symbol=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Symbol value is missing: bad\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "good_point, nonasciibadꠇ,field=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Field value is missing: nonasciibadꠇ\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "good_point, nonjson\\\"bad,field=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Field value is missing: nonjson\\\"bad\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "good_point, bad,field=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Field value is missing: bad\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,a3 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Symbol value is missing: a3\"," +
                            "\"line\":1" +
                            ",\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,a3=4 1233456ab\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse timestamp: 1233456ab\"," +
                            "\"line\":1" +
                            ",\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint d=1024.2 123345689909909898798\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse timestamp: 123345689909909898798\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint d=10a24.2", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line, field value is invalid. Field: d; value: 10a24.2\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "badPoint,tag1=\"asdf\" d=1024.2", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line, tag value is invalid. Tag: tag1; value: \\\"asdf\\\"\",\"line\":1,\"errorId\":");
                }

                serverMain.assertSql("SELECT count() FROM good_point", "count\n0\n");
                serverMain.assertSql("select table_name from tables() where table_name='badPoint'", "table_name\n");
            }
        });
    }

    @Test
    public void testNoErrorLastLineNoLineBreak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                serverMain.compile("create table wal_not_here(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp)");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();
                    points.add("m1,tag1=value1 f1=1i,y=12i");
                    points.add("m1,tag1=value1 f1=1i,x=12i");
                    influxDB.write(points);
                }
                serverMain.waitWalTxnApplied("m1");
                serverMain.assertSql("SELECT count() FROM m1", "count\n2\n");
            }
        });
    }

    @Test
    public void testPing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.LINE_HTTP_PING_VERSION.getEnvVarName(), "v2.2.2"
            )) {
                serverMain.start();

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    influxDB.setLogLevel(InfluxDB.LogLevel.FULL);
                    Pong pong = influxDB.ping();
                    Assert.assertTrue(pong.isGood());
                    Assert.assertEquals(pong.getVersion(), "v2.2.2");
                }
            }
        });
    }

    @Test
    public void testRequestAtomicNoNewColumns() throws Exception {
        int count = 10000;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS.getEnvVarName(), String.valueOf(count)
            )) {
                serverMain.start();
                serverMain.compile("create table wal_low_max_uncomitted(sym symbol, i long, ts timestamp) " +
                        "timestamp(ts) partition by DAY WAL WITH maxUncommittedRows=" + count);
                List<String> lines = new ArrayList<>();
                String goodLine = "wal_low_max_uncomitted,sym=aaa\n";
                for (int i = 0; i < count; i++) {
                    lines.add(goodLine);
                }

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    // Bad line which should roll back the transaction
                    int totalCount = count + 1;
                    assertRequestErrorContains(influxDB, lines, "wal_low_max_uncomitted,bla i=aaa\n",
                            "{\"code\":\"invalid\",\"message\":\"failed to parse line protocol:errors encountered on line(s):" +
                                    "\\nerror in line " + totalCount + ": Could not parse entire line. Symbol value is missing: bla\"," +
                                    "\"line\":" + totalCount + ",\"errorId\":");
                }

                serverMain.waitWalTxnApplied("wal_low_max_uncomitted");
                serverMain.assertSql("SELECT count() FROM wal_low_max_uncomitted", "count\n0\n");
            }
        });
    }

    @Test
    public void testRequestNewColumnAddedInMiddleOfRequest() throws Exception {
        int count = 10000;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS.getEnvVarName(), String.valueOf(count)
            )) {
                serverMain.start();
                serverMain.compile("create table wal_tbl(sym symbol, ts timestamp) " +
                        "timestamp(ts) partition by DAY WAL WITH maxUncommittedRows=100");
                List<String> lines = new ArrayList<>();
                String goodLine = "wal_tbl,sym=aaa\n";
                for (int i = 0; i < count; i++) {
                    lines.add(goodLine);
                }

                // New column added
                String addColumnLine = "wal_tbl i=123i\n";
                for (int i = 0; i < count; i++) {
                    lines.add(addColumnLine);
                }

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    // Column is added
                    influxDB.write(lines);
                }

                serverMain.waitWalTxnApplied("wal_tbl");
                serverMain.assertSql("SELECT count() FROM wal_tbl", "count\n" + 2 * count + "\n");
            }
        });
    }

    @Test
    public void testRestrictedCreateColumnsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS.getEnvVarName(), "false"
            )) {
                serverMain.start();
                serverMain.compile("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();
                    assertRequestErrorContains(influxDB, points, "ex_tbl,a3=2 1222233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "ex_tbl2, d=2 1222233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl2; table does not exist, cannot create table, creating new columns is disabled\",\"line\":1,\"errorId\":");

                }
            }
        });
    }

    @Test
    public void testRestrictedCreateTableError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.LINE_AUTO_CREATE_NEW_COLUMNS.getEnvVarName(), "false",
                    PropertyKey.LINE_AUTO_CREATE_NEW_TABLES.getEnvVarName(), "false"
            )) {
                serverMain.start();
                serverMain.compile("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();
                    assertRequestErrorContains(influxDB, points, "ex_tbl,a3=2 1222233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled\",\"line\":1,\"errorId\":");

                    assertRequestErrorContains(influxDB, points, "ex_tbl2, d=2 1222233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol:errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl2; table does not exist, creating new tables is disabled\",\"line\":1,\"errorId\":");

                }
            }
        });
    }

    @Test
    public void testTimestampPrecisionSupport() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
                    influxDB.setLogLevel(InfluxDB.LogLevel.BASIC);

                    long microTime = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T04:00:00.000001Z");
                    List<String> points = new ArrayList<>();
                    points.add("m1,tag1=value1 f1=1i,y=12i " + microTime);
                    influxDB.write("db", "rp", InfluxDB.ConsistencyLevel.ANY, TimeUnit.MICROSECONDS, points);
                    points.clear();

                    long milliTime = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T05:00:00.001001Z") / 1000L;
                    points.add("m1,tag1=value1 f1=1i,y=12i " + milliTime);
                    influxDB.write("db", "rp", InfluxDB.ConsistencyLevel.ANY, TimeUnit.MILLISECONDS, points);
                    points.clear();

                    long nanoTime = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T06:00:00.000001") * 1000L;
                    points.add("m1,tag1=value1 f1=1i,y=12i " + nanoTime);
                    influxDB.write("db", "rp", InfluxDB.ConsistencyLevel.ANY, TimeUnit.NANOSECONDS, points);
                    points.clear();

                    long secondTime = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T07:00:01") / 1000L / 1000L;
                    points.add("m1,tag1=value1 f1=1i,y=12i " + secondTime);
                    influxDB.write("db", "rp", InfluxDB.ConsistencyLevel.ANY, TimeUnit.SECONDS, points);
                    points.clear();

                    long minuteTime = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T08:01") / 1000L / 1000L / 60L;
                    points.add("m1,tag1=value1 f1=1i,y=12i " + minuteTime);
                    influxDB.write("db", "rp", InfluxDB.ConsistencyLevel.ANY, TimeUnit.MINUTES, points);
                    points.clear();

                    long hourTime = IntervalUtils.parseFloorPartialTimestamp("2022-02-24T09") / 1000L / 1000L / 60L / 60L;
                    points.add("m1,tag1=value1 f1=1i,y=12i " + hourTime);
                    influxDB.write("db", "rp", InfluxDB.ConsistencyLevel.ANY, TimeUnit.HOURS, points);
                    points.clear();

                    // TimeUnit.DAYS is not supported by the InfluxDB client
                }

                serverMain.waitWalTxnApplied("m1");
                serverMain.assertSql("SELECT * FROM m1", "tag1\tf1\ty\ttimestamp\n" +
                        "value1\t1\t12\t2022-02-24T04:00:00.000001Z\n" +
                        "value1\t1\t12\t2022-02-24T05:00:00.001000Z\n" +
                        "value1\t1\t12\t2022-02-24T06:00:00.000001Z\n" +
                        "value1\t1\t12\t2022-02-24T07:00:01.000000Z\n" +
                        "value1\t1\t12\t2022-02-24T08:01:00.000000Z\n" +
                        "value1\t1\t12\t2022-02-24T09:00:00.000000Z\n");
            }
        });
    }

    private static void sendIlp(String tableName, int count, ServerMain serverMain) throws NumericException {
        long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
        int i = 0;

        try (final InfluxDB influxDB = InfluxDBUtils.getConnection(serverMain)) {
            BatchPoints batchPoints = BatchPoints
                    .database("test_db")
                    .tag("async", "true")
                    .build();

            String tableNameUpper = tableName.toUpperCase();

            if (count / 2 > 0) {
                for (; i < count / 2; i++) {
                    String tn = i % 2 == 0 ? tableName : tableNameUpper;
                    batchPoints.point(Point.measurement(tn)
                            .time(timestamp, TimeUnit.MICROSECONDS)
                            .tag("location", "santa_monica")
                            .addField("level description", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                            .addField("water_level", i)
                            .build());
                }
                influxDB.write(batchPoints);
            }

            BatchPoints batchPoints2 = BatchPoints
                    .database("test_db")
                    .tag("async", "true")
                    .build();
            for (; i < count; i++) {
                String tn = i % 2 == 0 ? tableName : tableNameUpper;
                batchPoints2.point(Point.measurement(tn)
                        .time(timestamp, TimeUnit.MICROSECONDS)
                        .tag("location", "santa_monica")
                        .addField("level description", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                        .addField("water_level", i)
                        .build());
            }

            influxDB.write(batchPoints2);
        }
    }
}
