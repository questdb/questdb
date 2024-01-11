package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import static io.questdb.test.tools.TestUtils.assertEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LineHttpsSenderTest extends AbstractBootstrapTest {
    @Rule
    public TlsProxyRule tlsProxy = TlsProxyRule.toHostAndPort("localhost", HTTP_PORT);

    public static void assertTableExists(CairoEngine engine, CharSequence tableName) {
        try (Path path = new Path()) {
            assertEquals(TableUtils.TABLE_EXISTS, engine.getTableStatus(path, engine.getTableTokenIfExists(tableName)));
        }
    }

    public static void assertTableExistsEventually(CairoEngine engine, CharSequence tableName) {
        assertEventually(() -> assertTableExists(engine, tableName));
    }

    public static void assertTableSizeEventually(CairoEngine engine, CharSequence tableName, long expectedSize) {
        TestUtils.assertEventually(() -> {
            assertTableExists(engine, tableName);

            try (TableReader reader = engine.getReader(tableName)) {
                long size = reader.getCursor().size();
                assertEquals(expectedSize, size);
            } catch (EntryLockedException e) {
                // if table is busy we want to fail this round and have the assertEventually() to retry later
                fail("table +" + tableName + " is locked");
            }
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void simpleTest() throws Exception {
        String tableName = UUID.randomUUID().toString();

        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                serverMain.start();
                int port = tlsProxy.getListeningPort();
                String url = "https://localhost:" + port;
                try (Sender sender = Sender.builder().url(url).advancedTls().disableCertificateValidation().build()) {
                    for (int i = 0; i < 100_000; i++) {
                        sender.table(tableName).longColumn("value", 42).atNow();
                    }
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 100_000);
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
                String url = "https://localhost:" + port;
                try (Sender sender = Sender.builder().url(url).advancedTls().disableCertificateValidation().build()) {
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush(); // make sure a connection is established
                    tlsProxy.killConnections();
                    sender.table(tableName).longColumn("value", 42).atNow();
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 2);
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
                String url = "https://localhost:" + port;
                try (Sender sender = Sender.builder().url(url).advancedTls().disableCertificateValidation().build()) {
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush(); // make sure a connection is established

                    sender.table(tableName).longColumn("value", 42).atNow();
                    tlsProxy.killConnections();
                    tlsProxy.killAfterAccepting();
                    try {
                        sender.flush();
                        Assert.fail("should fail");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "Could not flush buffer: Error while sending data to server.");
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
                String url = "https://localhost:" + port;
                try (Sender sender = Sender.builder().url(url).advancedTls().disableCertificateValidation().build()) {
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

                    // assert that we can still send new rows after a structural error
                    sender.table(tableName).longColumn("value", 42).atNow();
                    sender.flush();
                }
                assertTableSizeEventually(serverMain.getEngine(), tableName, 2);
            }
        });
    }

}
