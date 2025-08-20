package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.client.Sender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class LineHttpMultiUrlTest extends AbstractBootstrapTest {
    private static final String HOST = "0.0.0.0";
    private static final int PORT1 = 9020;
    private static final int PORT2 = 9030;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    public TestServerMain startInstancesWithoutConflict(String rootName, String host, int port, boolean readOnly) {
        return startWithEnvVariables(
                PropertyKey.HTTP_BIND_TO.getEnvVarName(), host + ":" + port,
                PropertyKey.HTTP_MIN_NET_BIND_TO.getEnvVarName(), host + ":" + port + 1,
                PropertyKey.PG_NET_BIND_TO.getEnvVarName(), host + ":" + port + 2,
                PropertyKey.LINE_TCP_NET_BIND_TO.getEnvVarName(), host + ":" + port + 3,
                PropertyKey.CAIRO_ROOT.getEnvVarName(), dbPath.parent().concat(rootName).toString(),
                PropertyKey.LINE_HTTP_ENABLED.getEnvVarName(), "true",
                PropertyKey.HTTP_SECURITY_READONLY.getEnvVarName(), String.valueOf(readOnly)
        );
    }

    @Test
    public void testFirstInstanceReadOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain1 = startInstancesWithoutConflict("server1", HOST, PORT1, true)) {
                serverMain1.start();
                try (final TestServerMain serverMain2 = startInstancesWithoutConflict("server2", HOST, PORT2, false)) {
                    dbPath.parent().$();
                    serverMain2.start();

                    try (Sender sender = Sender.builder(Sender.Transport.HTTP).address(HOST).port(PORT1).address(HOST).port(PORT2).build()) {
                        sender.table("line").symbol("sym1", "123").longColumn("field1", 123).at(123456789, ChronoUnit.MICROS);
                        sender.flush();
                        TableToken tt1 = serverMain1.getEngine().getTableTokenIfExists("line");
                        Assert.assertNull(tt1);
                        TableToken tt2 = serverMain2.getEngine().getTableTokenIfExists("line");
                        Assert.assertNotNull(tt2);

                        // sending again should succeed now
                        sender.table("line").symbol("sym1", "123").longColumn("field1", 123).at(123456789, ChronoUnit.MICROS);
                        sender.flush();

                        TestUtils.drainWalQueue(serverMain2.getEngine());
                        TestUtils.assertEventually(() -> {
                            serverMain2.assertSql("select count() FROM line;", "count\n2\n");
                        });
                    }
                }
            }
        });
    }
}
