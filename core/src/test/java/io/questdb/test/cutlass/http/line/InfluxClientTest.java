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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class InfluxClientTest extends AbstractBootstrapTest {
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

                try (final InfluxDB influxDB = getConnection(serverMain)) {
                    List<String> points = new ArrayList<>();

                    assertRequestError(influxDB, points, "ex_tbl b\\\"c=1024 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl; invalid column name: b\\\"c\",\"line\":1}");

                    assertRequestError(influxDB, points, "ex_tbl b=1024 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: b; cast error from protocol type: FLOAT to column type: BYTE\",\"line\":1}");

                    assertRequestError(influxDB, points, "ex_tbl b=1024i 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: b; line protocol value: 1024 is out bounds of column type: BYTE\",\"line\":1}");

                    assertRequestError(influxDB, points, "ex_tbl i=1024.2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: i; cast error from protocol type: FLOAT to column type: INT\",\"line\":1}");

                    assertRequestError(influxDB, points, "ex_tbl str=1024.2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: str; cast error from protocol type: FLOAT to column type: STRING\",\"line\":1}");
                }
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

                try (final InfluxDB influxDB = getConnection(serverMain)) {

                    List<String> points = new ArrayList<>();
                    assertRequestError(influxDB, points, "badPo\"int,a3=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: badPo\\\"int; invalid table name\",\"line\":1}");

                    assertRequestError(influxDB, points, "badPoint,bad\"symbol=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: badPoint; invalid column name: bad\\\"symbol\",\"line\":1}");

                    assertRequestError(influxDB, points, "wal_not_here a=1,b=1 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: wal_not_here; cannot insert in non-WAL table\",\"line\":1}");

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

                serverMain.waitWalTxnApplied(tableName, 2);
                serverMain.assertSql("SELECT count() FROM " + tableName, "count\n" + count * threads + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM " + tableName, "sum\n" + (count * (count - 1) / 2) * threads + "\n");
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
                try (final InfluxDB influxDB = getConnection(serverMain)) {

                    List<String> points = new ArrayList<>();
                    points.add("good_point,sym=a str=\"abdc\",num=1 1233456\n");
                    assertRequestError(influxDB, points, "badPoint a3 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 2: Could not parse entire line. Field value is missing: a3\"," +
                            "\"line\":2" +
                            "}");

                    assertRequestError(influxDB, points, "badPoint,bad,symbol=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Tag value is missing: bad\",\"line\":1}");

                    assertRequestError(influxDB, points, "good_point, bad,field=2 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Field value is missing: bad\",\"line\":1}");

                    assertRequestError(influxDB, points, "badPoint,a3 1233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse entire line. Tag value is missing: a3\"," +
                            "\"line\":1" +
                            "}");

                    assertRequestError(influxDB, points, "badPoint,a3=4 1233456ab\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse timestamp: 1233456ab\"," +
                            "\"line\":1" +
                            "}");

                    assertRequestError(influxDB, points, "badPoint d=1024.2 123345689909909898798\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: Could not parse timestamp: 123345689909909898798\",\"line\":1}");
                }

                serverMain.assertSql("SELECT count() FROM good_point", "count\n0\n");
                serverMain.assertSql("select table_name from tables() where table_name='badPoint'", "table_name\n");
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

                try (final InfluxDB influxDB = getConnection(serverMain)) {

                    List<String> points = new ArrayList<>();
                    assertRequestError(influxDB, points, "ex_tbl,a3=2 1222233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled\",\"line\":1}");

                    assertRequestError(influxDB, points, "ex_tbl2, d=2 1222233456\n", "{" +
                            "\"code\":\"invalid\"," +
                            "\"message\":\"failed to parse line protocol: errors encountered on line(s):\\n" +
                            "error in line 1: table: ex_tbl2; table does not exist, creating new tables is disabled\",\"line\":1}");

                }
            }
        });
    }

    private static void assertPointsRequestError(InfluxDB influxDB, List<String> points, String error) {
        try {
            influxDB.write(points);
            Assert.fail();
        } catch (InfluxDBException e) {
            TestUtils.assertEquals(error, e.getMessage());
        }
        points.clear();
    }

    private static void assertRequestError(InfluxDB influxDB, List<String> points, String line, String error) {
        points.add(line);
        assertPointsRequestError(influxDB, points, error);
    }

    @NotNull
    private static InfluxDB getConnection(ServerMain serverMain) {
        int httpPort = serverMain.getConfiguration().getHttpServerConfiguration().getDispatcherConfiguration().getBindPort();
        final String serverURL = "http://127.0.0.1:" + httpPort, username = "root", password = "root";
        return InfluxDBFactory.connect(serverURL, username, password);
    }

    private static void sendIlp(String tableName, int count, ServerMain serverMain) throws NumericException {
        long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
        int i = 0;

        try (final InfluxDB influxDB = getConnection(serverMain)) {

            BatchPoints batchPoints = BatchPoints
                    .database("test_db")
                    .tag("async", "true")
                    .build();

            if (count / 2 > 0) {
                for (; i < count / 2; i++) {
                    batchPoints.point(Point.measurement(tableName)
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
                batchPoints2.point(Point.measurement(tableName)
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
