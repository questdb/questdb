package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("FieldCanBeLocal")
public class LineHttpMultiUrlTest extends AbstractBootstrapTest {
    public static final Log LOG = LogFactory.getLog(LineHttpMultiUrlTest.class);
    private static final String HOST = "0.0.0.0";
    private static final int PORT1 = 9020;
    private static final int PORT2 = 9030;
    private static Rnd rnd;
    private static long s0;
    private static long s1;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        rnd = TestUtils.generateRandom(LOG);
        s0 = rnd.getSeed0();
        s1 = rnd.getSeed1();
    }

    @Test
    public void fuzzServersStartingAndStopping() throws Exception {
        Path.clearThreadLocals();
        TestUtils.assertMemoryLeak(() -> {
            int timeoutMillis = 30_000;
            int delayMillis = 2000;
            int jitterMillis = 150;

            AtomicLong t1Count = new AtomicLong();
            AtomicLong t2Count = new AtomicLong();
            AtomicLong t3Count = new AtomicLong();

            Thread t1 = new Thread(() -> createAServerAndOccasionallyBlipIt("server1", HOST, PORT1, false, timeoutMillis, delayMillis, jitterMillis, t1Count));
            Thread t2 = new Thread(() -> createAServerAndOccasionallyBlipIt("server2", HOST, PORT2, false, timeoutMillis, delayMillis, jitterMillis, t2Count));
            Thread t3 = new Thread(() -> sendToTwoPossibleServers(HOST, PORT1, HOST, PORT2, (int) (timeoutMillis * 0.9), delayMillis / 5, jitterMillis / 5, t3Count));
            t1.start();
            t2.start();
            t3.start();
            t3.join();
            t2.join();
            t1.join();


            Assert.assertEquals(t3Count.get(), t1Count.get() + t2Count.get());
            Path.clearThreadLocals();
        });
    }

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

    @Test
    public void testFirstServerIsDown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain2 = startInstancesWithoutConflict("server2", HOST, PORT2, false)) {
                serverMain2.start();
                try (Sender sender = Sender.fromConfig("http::addr=localhost:9020;addr=localhost:9030;")) {
                    sender.table("line").longColumn("foo", 123).atNow();
                    sender.flush();

                    TableToken tt2 = serverMain2.getEngine().getTableTokenIfExists("line");
                    Assert.assertNotNull(tt2);
                }
            }

        });
    }

    @Test
    public void testServersAllDown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.fromConfig("http::addr=localhost:9020;addr=localhost:9030;")) {
                sender.table("line").longColumn("foo", 123).atNow();
                sender.flush();
            } catch (LineSenderException e) {
                Assert.assertEquals(
                        "[61] Could not flush buffer: http://localhost:9030/write?precision=n Connection Failed: [61] could not connect to host [host=localhost, port=9030, errno=61]",
                        e.getMessage()
                );
                Assert.assertEquals(61, e.getErrno());
            }
        });
    }

    @Test
    public void testServersStartAndStop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            TestServerMain serverMain1 = startInstancesWithoutConflict("server1", HOST, PORT1, false);
            serverMain1.start();
            TestServerMain serverMain2 = startInstancesWithoutConflict("server2", HOST, PORT2, false);
            serverMain2.start();

            final @Nullable TxReader[] r1 = {null};
            final @Nullable TxReader[] r2 = {null};

            long expectedTotalRows = 100_000;
            try (Sender sender = Sender.fromConfig("http::addr=localhost:9020;addr=localhost:9030;auto_flush=off;")) {
                for (int i = 0; i < expectedTotalRows; i++) {
                    sender.table("line").longColumn("foo", i).atNow();

                    if (i == 10_000) {
                        serverMain2.close();
                        if (serverMain1.hasBeenClosed()) {
                            serverMain1 = startInstancesWithoutConflict("server1", HOST, PORT1, false);
                            serverMain1.start();
                        }
                    }

                    if (i == 20_000) {
                        serverMain1.close();
                        if (serverMain2.hasBeenClosed()) {
                            serverMain2 = startInstancesWithoutConflict("server2", HOST, PORT2, false);
                            serverMain2.start();
                        }
                    }

                    if (i % 100 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();

                if (serverMain1.hasBeenClosed()) {
                    serverMain1 = startInstancesWithoutConflict("server1", HOST, PORT1, false);
                    serverMain1.start();
                }

                if (serverMain2.hasBeenClosed()) {
                    serverMain2 = startInstancesWithoutConflict("server2", HOST, PORT2, false);
                    serverMain2.start();
                }

                TestUtils.drainWalQueue(serverMain1.getEngine());
                TestUtils.drainWalQueue(serverMain2.getEngine());


                TestServerMain finalServerMain1 = serverMain1;
                TestServerMain finalServerMain2 = serverMain2;
                TestUtils.assertEventually(() -> {
                    TableToken tt1 = finalServerMain1.getEngine().getTableTokenIfExists("line");
                    Assert.assertNotNull(tt1);
                    TableToken tt2 = finalServerMain2.getEngine().getTableTokenIfExists("line");
                    Assert.assertNotNull(tt2);


                    r1[0] = new TxReader(finalServerMain1.getEngine().getConfiguration().getFilesFacade());
                    r2[0] = new TxReader(finalServerMain2.getEngine().getConfiguration().getFilesFacade());


                    long rows1 = getRowCount(finalServerMain1.getEngine(), tt1, r1[0]);
                    long rows2 = getRowCount(finalServerMain2.getEngine(), tt1, r2[0]);

                    r1[0].close();
                    r2[0].close();

                    Assert.assertEquals(expectedTotalRows, rows1 + rows2);
                });
            } finally {
                if (!serverMain1.hasBeenClosed()) {
                    serverMain1.close();
                }
                if (!serverMain2.hasBeenClosed()) {
                    serverMain2.close();
                }
                Misc.free(r1);
                Misc.free(r2);
            }
        });
    }

    /**
     * Creates and destroys a server based on a delay and jitter.
     */
    private void createAServerAndOccasionallyBlipIt(String rootName, String host, int port, boolean readOnly, int timeoutMillis, int delayMillis, int jitterMillis, AtomicLong count) {
        long elapsedMillis = 0;
        @Nullable TestServerMain serverMain = null;

        assert delayMillis > jitterMillis;

        try {
            serverMain = startInstancesWithoutConflict(rootName, host, port, readOnly);

            while (elapsedMillis < timeoutMillis) {
                long startMillis = System.currentTimeMillis();

                if (serverMain.hasStarted()) {
                    // close it
                    serverMain.close();
                }

                if (serverMain.hasBeenClosed()) {
                    serverMain = startInstancesWithoutConflict(rootName, host, port, readOnly);
                }

                int jitter = rnd.nextIntSync(jitterMillis);
                jitter = (jitter >> 1) ^ (-(jitter & 1)); // zigzag conversion

                Os.sleep(delayMillis + jitter);
                long endMillis = System.currentTimeMillis();
                elapsedMillis += (endMillis - startMillis);
            }
        } finally {
            if (serverMain != null && serverMain.hasBeenClosed()) {
                serverMain = startInstancesWithoutConflict(rootName, host, port, readOnly);
                serverMain.start();
            }
            TestUtils.drainWalQueue(serverMain.getEngine());
            TableToken tt = null;
            for (int i = 0; i < 100_000; i++) {
                tt = serverMain.getEngine().getTableTokenIfExists("line");
                if (tt != null) {
                    break;
                }
            }
            try (TxReader txReader = new TxReader(serverMain.getEngine().getConfiguration().getFilesFacade())) {
                count.set(getRowCount(serverMain.getEngine(), serverMain.getEngine().getTableTokenIfExists("line"), txReader));
            }
            Misc.free(serverMain);
            Path.clearThreadLocals();
        }
    }

    private long getRowCount(CairoEngine engine, TableToken token, TxReader txReader) {
        try (TableMetadata tm = engine.getTableMetadata(token)) {
            int partitionBy = tm.getPartitionBy();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName());
                TableUtils.setTxReaderPath(txReader, path, partitionBy); // modifies path
                return txReader.unsafeLoadRowCount();
            }
        }
    }

    private void sendToTwoPossibleServers(String host1, int port1, String host2, int port2, int timeoutMillis, int delayMillis, int jitterMillis, AtomicLong count) {
        long elapsedMillis = 0;
        long nextFlushAt = delayMillis;

        int localCount = 0;
        assert delayMillis > jitterMillis;

        try (Sender sender = Sender.builder(Sender.Transport.HTTP).address(host1).port(port1).address(host2).port(port2).disableAutoFlush().build()) {
            while (elapsedMillis < timeoutMillis) {
                long startMillis = System.currentTimeMillis();

                sender.table("line").longColumn("foo", 123).atNow();
                localCount++;


                Os.sleep(10);

                if (elapsedMillis > nextFlushAt) {
                    sender.flush();
                    count.set(count.get() + localCount);

                    localCount = 0;
                    int jitter = rnd.nextIntSync(jitterMillis);
                    jitter = (jitter >> 1) ^ (-(jitter & 1)); // zigzag conversion

                    nextFlushAt = elapsedMillis + jitter + delayMillis;
                }

                long endMillis = System.currentTimeMillis();
                elapsedMillis += (endMillis - startMillis);
            }

            sender.flush();
            count.set(count.get() + localCount);
        }
        Path.clearThreadLocals();
    }
}
