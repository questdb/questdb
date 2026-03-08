/*******************************************************************************
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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LineHttpsSenderTest extends AbstractBootstrapTest {
    public static final char[] TRUSTSTORE_PASSWORD = "questdb".toCharArray();
    public static final String TRUSTSTORE_PATH = "/keystore/server.keystore";
    @Rule
    public TlsProxyRule tlsProxy = TlsProxyRule.toHostAndPort("localhost", HTTP_PORT);

    private static void assertTableExists(CairoEngine engine, CharSequence tableName) {
        try (Path path = new Path()) {
            assertEquals(TableUtils.TABLE_EXISTS, engine.getTableStatus(path, engine.getTableTokenIfExists(tableName)));
        }
    }

    private static void assertTableSizeEventually(CairoEngine engine, CharSequence tableName, long expectedSize) throws Exception {
        TestUtils.assertEventually(() -> {
            assertTableExists(engine, tableName);

            try (
                    TableReader reader = engine.getReader(tableName);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                long size = cursor.size();
                assertEquals(expectedSize, size);
            } catch (EntryLockedException e) {
                // if table is busy we want to fail this round and have the assertEventually() to retry later
                fail("table +" + tableName + " is locked");
            }
        });
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void simpleTest() throws Exception {
        String tableName = "simpleTest";

        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                long count = 100_000;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address(address)
                        .enableTls()
                        .advancedTls()
                        .disableCertificateValidation()
                        .build()) {
                    for (long i = 1; i <= count; i++) {
                        sender.table(tableName).longColumn("value", i).atNow();
                    }
                }
                long expectedSum = (count / 2) * (count + 1);
                double expectedAvg = expectedSum / (double) count;
                TestUtils.assertEventually(() -> serverMain.assertSql(
                        "select sum(value), max(value), min(value), round(avg(value), 3) from " + tableName,
                        "sum\tmax\tmin\tround\n"
                                + expectedSum + "\t" + count + "\t1\t" + expectedAvg + "\n"
                ));
            }
        });
    }

    @Test
    public void testAutoRecoveryAfterInfrastructureError() throws Exception {
        String tableName = "testAutoRecoveryAfterInfrastructureError";
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address(address)
                        .enableTls()
                        .advancedTls()
                        .disableCertificateValidation()
                        .build()) {
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush(); // make sure a connection is established
                    tlsProxy.killConnections();
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush();
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 2);
            }
        });
    }

    @Test
    public void testConfigString() throws Exception {
        String tableName = "testConfigString";

        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                long count = 10_000;
                try (Sender sender = Sender.fromConfig("https::addr=" + address + ";tls_verify=unsafe_off;")) {
                    for (long i = 1; i <= count; i++) {
                        sender.table(tableName).longColumn("value", i).atNow();
                    }
                }
                long expectedSum = (count / 2) * (count + 1);
                double expectedAvg = expectedSum / (double) count;
                TestUtils.assertEventually(() -> serverMain.assertSql(
                        "select sum(value), max(value), min(value), avg(value) from " + tableName,
                        "sum\tmax\tmin\tavg\n"
                                + expectedSum + "\t" + count + "\t1\t" + expectedAvg + "\n"
                ));
            }
        });
    }

    @Test
    public void testCustomTrustStore() throws Exception {
        String tableName = UUID.randomUUID().toString();
        String truststore = TestUtils.getTestResourcePath(TRUSTSTORE_PATH);
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address(address)
                        .enableTls()
                        .advancedTls().customTrustStore(truststore, TRUSTSTORE_PASSWORD)
                        .build()) {
                    for (int i = 0; i < 100_000; i++) {
                        sender.table(tableName).longColumn("value", 42).atNow();
                    }
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 100_000);
            }
        });
    }

    @Test
    public void testRecoveryAfterInfrastructureErrorExceededRetryLimit() throws Exception {
        String tableName = "testAutoRecoveryAfterInfrastructureError";
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address(address)
                        .enableTls()
                        .advancedTls().disableCertificateValidation()
                        .retryTimeoutMillis(500)
                        .build()) {
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush(); // make sure a connection is established

                    sender.table(tableName).longColumn("value", 42).atNow();
                    tlsProxy.killConnections();
                    tlsProxy.killAfterAccepting();
                    try {
                        sender.flush();
                        Assert.fail("should fail");
                    } catch (LineSenderException e) {
                        // expected message: Could not flush buffer: https://localhost:<ephemeral_port>/write?precision=n Connection Failed
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer: https://localhost:");
                        TestUtils.assertContains(e.getMessage(), "/write?precision=n Connection Failed");
                        sender.reset();
                    }
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 1);
            }
        });
    }

    @Test
    public void testRecoveryAfterStructuralError() throws Exception {
        String tableName = "testRecoveryAfterStructuralError";
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address(address)
                        .enableTls()
                        .advancedTls().disableCertificateValidation()
                        .build()) {
                    // create 'value' as a long column
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush();

                    // try to send a string value to 'value' column. this must fail.
                    sender.table(tableName).stringColumn("value", "42").atNow();
                    try {
                        sender.flush();
                        Assert.fail("should fail");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer");
                        TestUtils.assertContains(e.getMessage(), "http-status=400");
                        TestUtils.assertContains(e.getMessage(), "error in line 1: table: testRecoveryAfterStructuralError, column: value; cast error from protocol type: STRING to column type: LONG");
                    }
                    sender.reset();

                    // assert that we can still send new rows after a structural error
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush();
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 2);
            }
        });
    }

    @Test
    public void testServerNotTrusted() throws Exception {
        String tableName = UUID.randomUUID().toString();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address(address)
                        .enableTls()
                        .retryTimeoutMillis(1000)
                        .protocolVersion(PROTOCOL_VERSION_V1)
                        .build()
                ) {
                    try {
                        sender.table(tableName).longColumn("value", 42).atNow();
                        sender.flush();
                        fail("should fail, the server is not trusted");
                    } catch (LineSenderException ex) {
                        TestUtils.assertContains(ex.getMessage(), "Could not flush buffer");
                        sender.reset();
                    }
                }
            }
        });
    }

    @Test
    public void testServerNotTrustedAutoDetection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String address = "localhost:" + port;
                try {
                    Sender ignore = Sender.builder(Sender.Transport.HTTP)
                            .address(address)
                            .enableTls()
                            .retryTimeoutMillis(1000)
                            .build();
                    fail("should fail, the server is not trusted");
                } catch (LineSenderException ex) {
                    TestUtils.assertContains(ex.getMessage(), "Failed to detect server line protocol version");
                }
            }
        });
    }

}
