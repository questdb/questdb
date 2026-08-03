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
import io.questdb.test.tools.TestUtils;
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

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
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
    public void testTcpDecimalIntoNonDecimalColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "tcp_guard", true);

                sendOverTcp(serverMain, "tcp_guard", "g", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard", "l", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard", "d", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard", "arr", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard", "dec", VALID);

                serverMain.awaitTxn("tcp_guard", 1);
                serverMain.assertSql("select dec from tcp_guard", "dec\n123.45\n");
            }
        });
    }

    @Test
    public void testTcpNonWalDecimalIntoNonDecimalColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startServer()) {
                createTable(serverMain, "tcp_guard_nowal", false);

                sendOverTcp(serverMain, "tcp_guard_nowal", "g", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard_nowal", "l", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard_nowal", "d", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard_nowal", "arr", SNEAKY);
                sendOverTcp(serverMain, "tcp_guard_nowal", "dec", VALID);

                TestUtils.assertEventually(() -> serverMain.assertSql("select dec from tcp_guard_nowal", "dec\n123.45\n"));
            }
        });
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

    /**
     * ILP TCP reports errors by disconnecting, so rejection is asserted by the row's absence.
     */
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
