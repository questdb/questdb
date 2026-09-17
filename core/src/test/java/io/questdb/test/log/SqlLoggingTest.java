package io.questdb.test.log;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.LogFactory;
import io.questdb.metrics.QueryTrace;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cutlass.http.TestHttpClient;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

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
    public void testCreateLiveView() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = createServerWithQueryProgressLogging()) {
                serverMain.start();

                try (TestHttpClient httpClient = new TestHttpClient(HttpClientFactory.newPlainTextInstance())) {
                    final int port = serverMain.getHttpServerPort();
                    exec(httpClient, "{\"ddl\":\"OK\"}", "create table trades(symbol symbol, price double, ts timestamp) timestamp(ts) partition by hour wal", port);
                    waitForRegex("fin.*?create table trades");
                    exec(
                            httpClient,
                            "{\"ddl\":\"OK\"}",
                            "create live view lv flush every 1s start from now as select symbol, price, ts, row_number() over w as rn"
                                    + " from trades window w as (partition by symbol order by ts anchor daily '00:00')",
                            port
                    );
                    waitForRegex("fin.*?create live view lv");
                }
            }
            assertOnlyOnce("fin.*?create live view lv");
        });
    }

    @Test
    public void testDisabledProgressLoggingPreservesErrorsAndReaderLeaks() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.LOG_SQL_QUERY_PROGRESS_ENABLED, false);
            final String successfulQuery = "select 1 /* progress-disabled-success */";
            final String failedQuery = "select 1 /* progress-disabled-error */";
            final String leakedQuery = "select 1 /* progress-disabled-leak */";
            final long beginNanos = configuration.getNanosecondClock().getTicks();

            QueryProgress.logStart(1, successfulQuery, sqlExecutionContext, false);
            QueryProgress.logEnd(1, successfulQuery, sqlExecutionContext, beginNanos);
            QueryProgress.logError(
                    CairoException.nonCritical().put("expected test failure"),
                    2,
                    failedQuery,
                    sqlExecutionContext,
                    beginNanos
            );

            execute("create table progress_disabled_reader_leak (x int)");
            try (TableReader reader = engine.getReader("progress_disabled_reader_leak")) {
                ObjList<TableReader> leakedReaders = new ObjList<>();
                leakedReaders.add(reader);
                QueryProgress.logEnd(3, leakedQuery, sqlExecutionContext, beginNanos, leakedReaders, null);
            }

            capture.drain();
            capture.assertNotLogged(successfulQuery);
            capture.assertLogged("err [id=2, sql=`" + failedQuery);
            capture.assertLogged("brk [id=3, sql=`" + leakedQuery);
        });
    }

    @Test
    public void testDisabledProgressLoggingStillTracesSuccessfulQueries() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.LOG_SQL_QUERY_PROGRESS_ENABLED, false);
            node1.setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
            engine.getMessageBus().getQueryTraceQueue().clear();

            final String query = "select 1 /* progress-disabled-trace */";
            final QueryTrace expected = new QueryTrace();
            expected.queryText = query;
            final long beginNanos = configuration.getNanosecondClock().getTicks();

            QueryProgress.logStart(4, query, sqlExecutionContext, false);
            QueryProgress.logEnd(4, query, sqlExecutionContext, beginNanos, null, expected);

            final QueryTrace actual = new QueryTrace();
            Assert.assertTrue(engine.getMessageBus().getQueryTraceQueue().tryDequeue(actual));
            Assert.assertEquals(query, actual.queryText);
            Assert.assertEquals(
                    sqlExecutionContext.getSecurityContext().getPrincipal().toString(),
                    actual.principal
            );
            capture.drain();
            capture.assertNotLogged(query);
        });
    }

    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = createServerWithQueryProgressLogging()) {
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
            // CREATE TABLE used to log "fin" twice: once from the keyword-executor's premature
            // completion log at compile time (id=-1, before the table exists), and once from
            // executeCreateTable() with the real query-registry id once the table is actually
            // created. The assertion above is the double-log guard; this one only pins WHICH
            // line survived. It cannot catch a double-log regression on its own -- \d+ never
            // matches the id=-1 sentinel, so it stays green whenever both lines are present.
            // It fails only if a fix dropped the real line and kept the sentinel.
            assertOnlyOnce("fin \\[id=\\d+, sql=`create table x");
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

    private static ServerMain createServerWithQueryProgressLogging() {
        return ServerMain.create(
                root,
                Map.of(PropertyKey.LOG_SQL_QUERY_PROGRESS_ENABLED.getEnvVarName(), "true")
        );
    }

    private void assertOnlyOnce(String regex) {
        capture.assertOnlyOnce(regex);
    }

    protected void waitForRegex(String regex) {
        capture.waitForRegex(regex);
    }
}
