package io.questdb.test.cutlass.line.http;

import io.questdb.cutlass.line.http.LineHttpSender;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE;
import static io.questdb.test.cutlass.http.line.IlpHttpUtils.getHttpPort;

public class LineHttpSenderTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
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
                int httpPort = getHttpPort(serverMain);

                int totalCount = 100;
                try (LineHttpSender sender = new LineHttpSender("localhost", httpPort, -1, false)) {
                    for (int i = 0; i < totalCount; i++) {
                        sender.table("m1")
                                .symbol("tag1", "value" + i)
                                .symbol("tag2", "value" + i)
                                .stringColumn("scol1", "value" + i)
                                .stringColumn("scol2", "value" + i)
                                .longColumn("lcol1", i)
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

                serverMain.waitWalTxnApplied("m1");
                serverMain.assertSql("select count() from m1", "count\n" +
                        totalCount + "\n");
            }
        });
    }
}
