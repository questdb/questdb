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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.Decimal256;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V3;

public class LineDecimalTargetColumnTypeTest extends AbstractBootstrapTest {
    private static final long TIMESTAMP = 100000000000L;
    // zero passes the precision and scale bits that non-decimal types happen to carry, so it reaches the store
    private static final Decimal256 SNEAKY = Decimal256.fromLong(0, 0);
    private static final Decimal256 VALID = Decimal256.fromLong(12345, 2);
    private static final LogCapture capture = new LogCapture();

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testHttpDecimalIntoNonDecimalColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "http_guard", true);

                assertHttpRejected(serverMain, "http_guard", "g", "GEOHASH(1c)");
                assertHttpRejected(serverMain, "http_guard", "l", "LONG");
                assertHttpRejected(serverMain, "http_guard", "d", "DOUBLE");
                assertHttpRejected(serverMain, "http_guard", "arr", "DOUBLE[]");
                serverMain.assertSql("select count() from http_guard", "count\n0\n");

                try (Sender sender = httpSender(serverMain)) {
                    sender.table("http_guard").decimalColumn("dec", VALID).at(TIMESTAMP, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTxn("http_guard", 1);
                serverMain.assertSql("select dec from http_guard", "dec\n123.45\n");
            }
        });
    }

    @Test
    public void testTcpDecimalIntoNewColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "tcp_new_col", true);

                assertNewColumnRejected(serverMain, "tcp_new_col", "zero", SNEAKY);
                assertNewColumnRejected(serverMain, "tcp_new_col", "scaled", VALID);
                assertColumnsUnchanged(serverMain, "tcp_new_col");
                serverMain.assertSql("SELECT count() FROM tcp_new_col", "count\n0\n");

                sendOverTcp(serverMain, "tcp_new_col", "dec", VALID);
                serverMain.awaitTxn("tcp_new_col", 1);
                serverMain.assertSql("SELECT dec FROM tcp_new_col", "dec\n123.45\n");
            }
        });
    }

    @Test
    public void testTcpDecimalIntoNonDecimalColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "tcp_guard", true);

                assertTcpRejected(serverMain, "tcp_guard", "g", "GEOHASH(1c)");
                assertTcpRejected(serverMain, "tcp_guard", "l", "LONG");
                assertTcpRejected(serverMain, "tcp_guard", "d", "DOUBLE");
                assertTcpRejected(serverMain, "tcp_guard", "arr", "DOUBLE[]");

                sendOverTcp(serverMain, "tcp_guard", "dec", VALID);
                serverMain.awaitTxn("tcp_guard", 1);
                serverMain.assertSql("SELECT count() FROM tcp_guard", "count\n1\n");
                serverMain.assertSql("SELECT dec FROM tcp_guard", "dec\n123.45\n");
            }
        });
    }

    @Test
    public void testTcpNonWalDecimalIntoNewColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "tcp_new_col_nowal", false);

                assertNewColumnRejected(serverMain, "tcp_new_col_nowal", "zero", SNEAKY);
                assertNewColumnRejected(serverMain, "tcp_new_col_nowal", "scaled", VALID);
                assertColumnsUnchanged(serverMain, "tcp_new_col_nowal");
                serverMain.assertSql("SELECT count() FROM tcp_new_col_nowal", "count\n0\n");

                sendOverTcp(serverMain, "tcp_new_col_nowal", "dec", VALID);
                TestUtils.assertEventually(() -> serverMain.assertSql("SELECT count() FROM tcp_new_col_nowal", "count\n1\n"));
                serverMain.assertSql("SELECT dec FROM tcp_new_col_nowal", "dec\n123.45\n");
            }
        });
    }

    @Test
    public void testTcpNonWalDecimalIntoNonDecimalColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "tcp_guard_nowal", false);

                assertTcpRejected(serverMain, "tcp_guard_nowal", "g", "GEOHASH(1c)");
                assertTcpRejected(serverMain, "tcp_guard_nowal", "l", "LONG");
                assertTcpRejected(serverMain, "tcp_guard_nowal", "d", "DOUBLE");
                assertTcpRejected(serverMain, "tcp_guard_nowal", "arr", "DOUBLE[]");

                sendOverTcp(serverMain, "tcp_guard_nowal", "dec", VALID);
                TestUtils.assertEventually(() -> serverMain.assertSql("SELECT count() FROM tcp_guard_nowal", "count\n1\n"));
                serverMain.assertSql("SELECT dec FROM tcp_guard_nowal", "dec\n123.45\n");
            }
        });
    }

    /**
     * A decimal that ILP refuses must leave no trace: an auto-added column of a broken type would
     * survive the rejected row and poison every later write to that table.
     */
    private static void assertColumnsUnchanged(TestServerMain serverMain, String tableName) {
        serverMain.assertSql("SELECT \"column\", type FROM table_columns('" + tableName + "')", """
                column\ttype
                g\tGEOHASH(1c)
                l\tLONG
                d\tDOUBLE
                arr\tDOUBLE[]
                dec\tDECIMAL(10,2)
                ts\tTIMESTAMP
                """);
    }

    private static void assertHttpRejected(TestServerMain serverMain, String tableName, String columnName, String columnTypeName) {
        try (Sender sender = httpSender(serverMain)) {
            sender.table(tableName).decimalColumn(columnName, SNEAKY).at(TIMESTAMP, ChronoUnit.MICROS);
            sender.flush();
            Assert.fail("expected rejection of a decimal sent into a " + columnTypeName + " column");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "cast error from protocol type: DECIMAL to column type: " + columnTypeName);
        }
    }

    private static void assertNewColumnRejected(TestServerMain serverMain, String tableName, String columnName, Decimal256 value) {
        sendOverTcp(serverMain, tableName, columnName, value);
        awaitLogged("decimal columns cannot be created automatically [table=" + tableName + ", columnName=" + columnName + "]");
    }

    private static void assertTcpRejected(TestServerMain serverMain, String tableName, String columnName, String columnTypeName) {
        sendOverTcp(serverMain, tableName, columnName, SNEAKY);
        awaitLogged("table: " + tableName + ", column: " + columnName
                + "; cast error from protocol type: DECIMAL to column type: " + columnTypeName);
    }

    /**
     * ILP TCP reports errors by disconnecting, so the server log carries the rejection. Waiting for the
     * message also sequences it ahead of every later row, which stops a wrongly accepted row from
     * remaining in flight past the row-count assertions. waitFor() can return without a match on
     * timeout, so assertLogged() carries the assertion.
     */
    private static void awaitLogged(String message) {
        capture.waitFor(message);
        capture.assertLogged(message);
    }

    private static void createTable(TestServerMain serverMain, String tableName, boolean wal) {
        serverMain.execute("CREATE TABLE " + tableName + " (" +
                "g GEOHASH(1c), l LONG, d DOUBLE, arr DOUBLE[], dec DECIMAL(10, 2), ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY DAY " + (wal ? "WAL" : "BYPASS WAL"));
    }

    private static Sender httpSender(TestServerMain serverMain) {
        return Sender.builder(Sender.Transport.HTTP)
                .address("localhost:" + serverMain.getHttpServerPort())
                .autoFlushRows(Integer.MAX_VALUE)
                .retryTimeoutMillis(0)
                .build();
    }

    private static void sendOverTcp(TestServerMain serverMain, String tableName, String columnName, Decimal256 value) {
        try (Sender sender = Sender.builder(Sender.Transport.TCP)
                .address("localhost")
                .port(serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort())
                .protocolVersion(PROTOCOL_VERSION_V3)
                .build()
        ) {
            sender.table(tableName).decimalColumn(columnName, value).at(TIMESTAMP, ChronoUnit.MICROS);
            sender.flush();
        } catch (LineSenderException ignored) {
            // the server may close the connection before the flush completes
        }
    }

    private static TestServerMain startServer() {
        return startWithEnvVariables(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS.getEnvVarName(), "1");
    }
}
