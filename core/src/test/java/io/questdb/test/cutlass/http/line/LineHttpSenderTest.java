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

package io.questdb.test.cutlass.http.line;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.http.LineHttpSender;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.PropertyKey.LINE_HTTP_ENABLED;

public class LineHttpSenderTest extends AbstractBootstrapTest {

    public void assertSql(CairoEngine engine, CharSequence sql, CharSequence expectedResult) throws SqlException {
        StringSink sink = Misc.getThreadLocalSink();
        engine.print(sql, sink);
        if (!Chars.equals(sink, expectedResult)) {
            Assert.assertEquals(expectedResult, sink);
        }
    }

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
                serverMain.ddl("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, u uuid, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .stringColumn("u", "foo")
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: u; cast error from protocol type: STRING to column type: UUID"
                    );

                    sender.table("ex_tbl")
                            .doubleColumn("b", 1234)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: b; cast error from protocol type: FLOAT to column type: BYTE"
                    );

                    sender.table("ex_tbl")
                            .longColumn("b", 1024)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: b; line protocol value: 1024 is out bounds of column type: BYTE"
                    );

                    sender.table("ex_tbl")
                            .doubleColumn("i", 1024.2)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: i; cast error from protocol type: FLOAT to column type: INT"
                    );

                    sender.table("ex_tbl")
                            .doubleColumn("str", 1024.2)
                            .at(1233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: str; cast error from protocol type: FLOAT to column type: STRING"
                    );
                }
            }
        });
    }

    @Test
    public void testAutoFlush() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 1 + rnd.nextInt(5);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 100_000;
                int autoFlushRows = 1000;
                try (LineHttpSender sender = new LineHttpSender("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, autoFlushRows, null, null, null, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        if (i != 0 && i % autoFlushRows == 0) {
                            serverMain.awaitTable("table with space");
                            serverMain.assertSql("select count() from 'table with space'", "count\n" +
                                    i + "\n");
                        }
                        sender.table("table with space")
                                .symbol("tag1", "value" + i % 10)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    serverMain.awaitTable("table with space");
                    serverMain.assertSql("select count() from 'table with space'", "count\n" +
                            totalCount + "\n");
                }
            }
        });
    }

    @Test
    public void testCancelRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .autoFlushIntervalMillis(Integer.MAX_VALUE) // flush manually...
                        .build()) {

                    sender.cancelRow(); // this should be no-op

                    // this row should be inserted
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 3)
                            .at(Instant.parse("2024-09-09T14:38:26.361110Z"));

                    // this one is cancelled
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 1);
                    sender.cancelRow();

                    // and this one should be inserted again
                    sender.table(tableName)
                            .symbol("async", "true")
                            .doubleColumn("water_level", 2)
                            .at(Instant.parse("2024-09-09T14:28:26.361110Z"));

                    sender.flush();
                }

                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("SELECT * FROM h2o_feet",
                        "async\twater_level\ttimestamp\n" +
                                "true\t2.0\t2024-09-09T14:28:26.361110Z\n" +
                                "true\t3.0\t2024-09-09T14:38:26.361110Z\n");
            }
        });
    }

    @Test
    public void testFlushAfterTimeout() throws Exception {
        // this is a regression test
        // there was a bug that flushes due to interval did not increase row count
        // bug scenario:
        // 1. flush to empty the local buffer
        // 2. period of inactivity
        // 3. insert a new row. this should trigger a flush due to interval flush. but it did not increase the row count so it stayed 0
        // 4. the flush() saw rowCount = 0 so it acted as noop
        // 5. every subsequent insert would trigger a flush due to interval (since the previous flush() was a noop and did not reset the timer)  but rowCount would stay 0
        // 6. nothing would be flushed and rows would keep accumulating in the buffer
        // 6. eventually the HTTP buffer would grow up to the limit and throw an exception

        // this test is to make sure that the scenario above does not happen
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                String confString = "http::addr=localhost:" + httpPort + ";auto_flush_rows=1;auto_flush_interval=1;";
                try (Sender sender = Sender.fromConfig(confString)) {

                    // insert a row to trigger row-based flush and reset the interval timer
                    sender.table("table")
                            .symbol("tag1", "value")
                            .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                            .atNow();

                    // wait a bit so the next insert triggers interval flush
                    Os.sleep(100);

                    // insert more rows
                    for (int i = 0; i < 9; i++) {
                        sender.table("table")
                                .symbol("tag1", "value")
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }

                    serverMain.awaitTable("table");
                    serverMain.assertSql("select count() from 'table'", "count\n" +
                            10 + "\n");
                }
            }
        });
    }

    @Test
    public void testHttpWithDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 100;
                int autoFlushRows = 1000;
                String tableName = "accounts";

                try (LineHttpSender sender = new LineHttpSender("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, autoFlushRows, null, null, null, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        // Add new symbol column with each second row
                        sender.table(tableName)
                                .symbol("balance" + i / 2, String.valueOf(i))
                                .atNow();

                        sender.flush();
                    }
                }

                for (int i = 0; i < 10; i++) {
                    serverMain.ddl("drop table " + tableName);
                    assertSql(serverMain.getEngine(), "SELECT count() from tables() where table_name='" + tableName + "'", "count\n0\n");
                    serverMain.ddl("create table " + tableName + " (" +
                            "balance1 symbol capacity 16, " +
                            "balance10 symbol capacity 16, " +
                            "timestamp timestamp)" +
                            " timestamp(timestamp) partition by DAY WAL " +
                            " dedup upsert keys (balance1, balance10, timestamp)");
                    assertSql(serverMain.getEngine(), "SELECT count() FROM (table_columns('Accounts')) WHERE upsertKey=true AND ( column = 'timestamp' )", "count\n" + 1 + "\n");
                    Os.sleep(10);
                }
            }
        });
    }

    @Test
    public void testIlpWithHttpContextPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048",
                    PropertyKey.HTTP_CONTEXT_WEB_CONSOLE.getEnvVarName(), "context1"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                int count = 9250;

                sendIlp(tableName, count, serverMain);

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
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

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
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

                serverMain.awaitTxn(tableName, 2);
                serverMain.assertSql("SELECT count() FROM h2o_feet", "count\n" + count + "\n");
                serverMain.assertSql("SELECT sum(water_level) FROM h2o_feet", "sum\n" + (count * (count - 1) / 2) + "\n");
            }
        });
    }

    @Test
    public void testInsertWithIlpHttp_varcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                String tableName = "h2o_feet";
                serverMain.ddl("create table " + tableName + " (async symbol, location symbol, level varchar, water_level long, ts timestamp) timestamp(ts) partition by DAY WAL");

                int count = 10;

                String fullString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                        .build()
                ) {
                    for (int i = 0; i < count; i++) {
                        sender.table(tableName)
                                .symbol("async", "true")
                                .symbol("location", "santa_monica")
                                .stringColumn("level", fullString.substring(0, i % 10 + 10))
                                .longColumn("water_level", i)
                                .atNow();
                    }
                    sender.flush();
                }
                StringBuilder expectedString = new StringBuilder("level\n");
                for (int i = 0; i < count; i++) {
                    expectedString.append(fullString, 0, i % 10 + 10).append('\n');
                }

                serverMain.awaitTxn(tableName, 1);
                serverMain.assertSql("SELECT level FROM h2o_feet", expectedString.toString());
            }
        });
    }

    @Test
    public void testLineHttpDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    LINE_HTTP_ENABLED.getEnvVarName(), "false"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 1_000;
                try (LineHttpSender sender = new LineHttpSender("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, 100_000, null, null, null, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("table")
                                .longColumn("lcol1", i)
                                .atNow();
                    }
                    try {
                        sender.flush();
                        Assert.fail("Expected exception");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "http-status=404");
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer: HTTP endpoint does not support ILP.");
                    }
                }
            }
        });
    }

    @Test
    public void testNegativeDesignatedTimestampDoesNotRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .retryTimeoutMillis(Integer.MAX_VALUE) // high-enoung value so the test times out if retry is attempted
                        .build()
                ) {
                    sender.table("tab")
                            .longColumn("l", 1) // filler
                            .at(-1, ChronoUnit.MICROS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "error in line 1: table: tab, timestamp: -1; designated timestamp before 1970-01-01 is not allowed"
                    );
                }
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
                serverMain.ddl("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .symbol("a3", "3")
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled"
                    );

                    sender.table("ex_tbl2")
                            .doubleColumn("d", 2)
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl2; table does not exist, cannot create table, creating new columns is disabled"
                    );
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
                serverMain.ddl("create table ex_tbl(b byte, s short, f float, d double, str string, sym symbol, tss timestamp, " +
                        "i int, l long, ip ipv4, g geohash(4c), ts timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {
                    sender.table("ex_tbl")
                            .symbol("a3", "2")
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl, column: a3 does not exist, creating new columns is disabled"
                    );

                    sender.table("ex_tbl2")
                            .doubleColumn("d", 2)
                            .at(1222233456, ChronoUnit.NANOS);
                    flushAndAssertError(
                            sender,
                            "Could not flush buffer",
                            "http-status=400",
                            "error in line 1: table: ex_tbl2; table does not exist, creating new tables is disabled"
                    );
                }
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            int fragmentation = 1 + rnd.nextInt(5);
            LOG.info().$("=== fragmentation=").$(fragmentation).$();
            try (final TestServerMain serverMain = startWithEnvVariables(
                    DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), String.valueOf(fragmentation)
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int totalCount = 1_000_000;
                try (LineHttpSender sender = new LineHttpSender("localhost", httpPort, DefaultHttpClientConfiguration.INSTANCE, null, 100_000, null, null, null, 0, 0, Long.MAX_VALUE)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("table with space")
                                .symbol("tag1", "value" + i % 10)
                                .symbol("tag2", "value " + i % 10)
                                .stringColumn("scol1", "value" + i)
                                .stringColumn("scol2", "value" + i)
                                .longColumn("lcol 1", i)
                                .longColumn("lcol2", i)
                                .doubleColumn("dcol1", i)
                                .doubleColumn("dcol2", i)
                                .boolColumn("bcol1", i % 2 == 0)
                                .boolColumn("bcol2", i % 2 == 0)
                                .timestampColumn("tcol1", Instant.now())
                                .timestampColumn("tcol2", Instant.now())
                                .timestampColumn("tcol3", 1, ChronoUnit.HOURS)
                                .timestampColumn("tcol4", 10, ChronoUnit.HOURS)
                                .atNow();
                    }
                    sender.flush();
                }
                serverMain.awaitTable("table with space");
                serverMain.assertSql("select count() from 'table with space'", "count\n" +
                        totalCount + "\n");
            }
        });
    }

    @Test
    public void testTimestampUpperBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            TimestampFormatCompiler timestampFormatCompiler = new TimestampFormatCompiler();

            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                serverMain.ddl("create table tab (ts timestamp, ts2 timestamp) timestamp(ts) partition by DAY WAL");

                int port = serverMain.getHttpServerPort();
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + port)
                        .build()
                ) {

                    DateFormat format = timestampFormatCompiler.compile("yyyy-MM-dd HH:mm:ss.SSSUUU");
                    // technically, we the storage layer supports dates up to 294247-01-10T04:00:54.775807Z
                    // but DateFormat does reliably support only 4 digit years. thus we use 9999-12-31T23:59:59.999Z
                    // is the maximum date that can be reliably worked with.
                    long nonDsTs = format.parse("9999-12-31 23:59:59.999999", DateFormatUtils.EN_LOCALE);
                    long dsTs = format.parse("9999-12-31 23:59:59.999999", DateFormatUtils.EN_LOCALE);

                    // first try with ChronoUnit
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsTs, ChronoUnit.MICROS)
                            .at(dsTs, ChronoUnit.MICROS);
                    sender.flush();
                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", "ts\tts2\n" +
                            "9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z\n");


                    // now try with the Instant overloads of `at()` and `timestampColumn()`
                    Instant nonDsInstant = Instant.ofEpochSecond(nonDsTs / 1_000_000, (nonDsTs % 1_000_000) * 1_000);
                    Instant dsInstant = Instant.ofEpochSecond(dsTs / 1_000_000, (nonDsTs % 1_000_000) * 1_000);
                    sender.table("tab")
                            .timestampColumn("ts2", nonDsInstant)
                            .at(dsInstant);
                    sender.flush();

                    serverMain.awaitTable("tab");
                    serverMain.assertSql("SELECT * FROM tab", "ts\tts2\n" +
                            "9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z\n" +
                            "9999-12-31T23:59:59.999999Z\t9999-12-31T23:59:59.999999Z\n");
                }
            }
        });
    }

    private static void flushAndAssertError(Sender sender, String... errors) {
        try {
            sender.flush();
            Assert.fail("Expected exception");
        } catch (LineSenderException e) {
            for (String error : errors) {
                TestUtils.assertContains(e.getMessage(), error);
            }
        }
    }

    private static void sendIlp(String tableName, int count, ServerMain serverMain) throws NumericException {
        long timestamp = IntervalUtils.parseFloorPartialTimestamp("2023-11-27T18:53:24.834Z");
        int i = 0;

        int port = serverMain.getHttpServerPort();
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address("localhost:" + port)
                .autoFlushRows(Integer.MAX_VALUE) // we want to flush manually
                .autoFlushIntervalMillis(Integer.MAX_VALUE) // flush manually...
                .build()
        ) {
            if (count / 2 > 0) {
                String tableNameUpper = tableName.toUpperCase();
                for (; i < count / 2; i++) {
                    String tn = i % 2 == 0 ? tableName : tableNameUpper;
                    sender.table(tn)
                            .symbol("async", "true")
                            .symbol("location", "santa_monica")
                            .stringColumn("level", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                            .longColumn("water_level", i)
                            .at(timestamp, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            for (; i < count; i++) {
                String tableNameUpper = tableName.toUpperCase();
                String tn = i % 2 == 0 ? tableName : tableNameUpper;
                sender.table(tn)
                        .symbol("async", "true")
                        .symbol("location", "santa_monica")
                        .stringColumn("level", "below 3 feet asd fasd fasfd asdf asdf asdfasdf asdf asdfasdfas dfads".substring(0, i % 68))
                        .longColumn("water_level", i)
                        .at(timestamp, ChronoUnit.MICROS);
            }
            sender.flush();
        }
    }
}
