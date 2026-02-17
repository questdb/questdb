package io.questdb.test.log;

import io.questdb.ServerMain;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.LogFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SqlLoggingTest extends AbstractCairoTest {
    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(QueryProgress.class);
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(QueryProgress.class);
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root)) {
                serverMain.start();

                // HTTP JSON test
                try (TestHttpClient httpClient = new TestHttpClient(HttpClientFactory.newPlainTextInstance())) {
                    final int port = serverMain.getHttpServerPort();
                    exec(httpClient, "{\"ddl\":\"OK\"}", "create table x(a int, ts timestamp) timestamp(ts) partition by day", port);
                    waitForRegex("fin.*?create table x");
                    exec(httpClient, "{\"dml\":\"OK\"}", "insert into x values (1,0)", port);
                    waitForRegex("fin.*?insert into x values");
                    serverMain.awaitTable("x");
                    exec(httpClient, "{\"query\":\"select count() from x\",\"columns\":[{\"name\":\"count()\",\"type\":\"LONG\"}],\"timestamp\":-1,\"dataset\":[[1]],\"count\":1}", "select count() from x", port);
                    waitForRegex("fin.*?select count\\(\\) from x");
                    exec(httpClient, "{\"ddl\":\"OK\"}", "alter table x add column c double", port);
                    waitForRegex("fin.*?alter table x add");
                    exec(httpClient, "{\"dml\":\"OK\",\"updated\":3}", "update x set c = 0.4", port);
                    waitForRegex("fin.*?update x set c");
                    serverMain.awaitTable("x");
                    exec(httpClient, "{\"ddl\":\"OK\"}", "rename table x to y", port);
                    waitForRegex("fin.*?rename table x to y");
                    exec(httpClient, "{\"ddl\":\"OK\"}", "drop table y", port);
                    waitForRegex("fin.*?drop table y");
                }
            }
            assertOnlyOnce("fin.*?create table x");
            assertOnlyOnce("fin.*?insert into x values");
            assertOnlyOnce("fin.*?select count\\(\\) from x");
            assertOnlyOnce("fin.*?alter table x add");
            assertOnlyOnce("fin.*?update x set c");
            assertOnlyOnce("fin.*?rename table x to y");
            assertOnlyOnce("fin.*?drop table y");
        });
    }

    private static void exec(TestHttpClient httpClient, String expectedResponse, String sql, int port) {
        httpClient.assertGet(
                "/exec",
                expectedResponse,
                sql,
                "localhost",
                port,
                null,
                null,
                null
        );
    }

    private void assertOnlyOnce(String regex) {
        capture.assertOnlyOnce(regex);
    }

    protected void waitForRegex(String regex) {
        capture.waitForRegex(regex);
    }
}
