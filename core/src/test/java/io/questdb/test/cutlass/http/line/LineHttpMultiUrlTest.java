package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LineHttpMultiUrlTest extends AbstractBootstrapTest {
    public static final Log LOG = LogFactory.getLog(LineHttpMultiUrlTest.class);
    private static final String HOST = "127.0.0.1";
    private static final int PORT1 = 9070;
    private static final int PORT2 = 9080;
    private static Rnd rnd;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        rnd = TestUtils.generateRandom(LOG);
    }

    @Test
    public void fuzzServersStartingAndStopping() throws Exception {
        String root1 = "server1" + System.currentTimeMillis();
        String root2 = "server2" + System.currentTimeMillis();
        Path.clearThreadLocals();
        TestUtils.assertMemoryLeak(() -> {
            int senderDurationMillis = 5_000;
            int upMillis = 1000;
            int downMillis = 1000;
            int jitterMillis = 100;

            AtomicLong t1Count = new AtomicLong(0);
            AtomicLong t2Count = new AtomicLong(0);
            AtomicLong t3Count = new AtomicLong(0);
            AtomicBoolean serversRunning = new AtomicBoolean(true);

            Thread t1 = new Thread(() -> {
                try {
                    createAServerAndOccasionallyBlipIt(root1, PORT1, false, serversRunning, upMillis, downMillis, jitterMillis, t1Count);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread t2 = new Thread(() -> {
                try {
                    createAServerAndOccasionallyBlipIt(root2, PORT2, false, serversRunning, upMillis, downMillis, jitterMillis, t2Count);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Thread t3 = new Thread(() -> sendToTwoPossibleServers(HOST, PORT1, HOST, PORT2, senderDurationMillis, t3Count));
            t1.start();
            t2.start();
            t3.start();

            t3.join();
            serversRunning.set(false);

            t2.join();
            t1.join();

            long c1 = t1Count.get();
            long c2 = t2Count.get();
            long c3 = t3Count.get();
            Assert.assertEquals("Observable rows do not match written rows. c1: " + c1 + ", c2: " + c2 + ", c3: " + c3, c3, c1 + c2);

            Path.clearThreadLocals();
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    public TestServerMain startInstance(String rootName, String host, int port, boolean readOnly, String name) {
        return startWithEnvVariables(
                PropertyKey.HTTP_BIND_TO.getEnvVarName(), host + ":" + port,
                PropertyKey.HTTP_MIN_NET_BIND_TO.getEnvVarName(), host + ":" + port + 1,
                PropertyKey.PG_NET_BIND_TO.getEnvVarName(), host + ":" + port + 2,
                PropertyKey.LINE_TCP_NET_BIND_TO.getEnvVarName(), host + ":" + port + 3,
                PropertyKey.CAIRO_ROOT.getEnvVarName(), dbPath + rootName,
                PropertyKey.LINE_HTTP_ENABLED.getEnvVarName(), "true",
                PropertyKey.HTTP_SECURITY_READONLY.getEnvVarName(), String.valueOf(readOnly),
                PropertyKey.DEBUG_DB_LOG_NAME.getEnvVarName(), name
        );
    }

    @Test
    public void testFirstInstanceReadOnly() throws Exception {
        String root1 = "server1" + System.currentTimeMillis();
        String root2 = "server2" + System.currentTimeMillis();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain1 = startInstance(root1, HOST, PORT1, true, null)) {
                serverMain1.start();
                try (final TestServerMain serverMain2 = startInstance(root2, HOST, PORT2, false, null)) {
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
        String root2 = "server2" + System.currentTimeMillis();
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain2 = startInstance(root2, HOST, PORT2, false, null)) {
                serverMain2.start();
                try (Sender sender = Sender.fromConfig("http::addr=" + HOST + ":" + PORT1 + ";addr=" + HOST + ":" + PORT2 + ";")) {
                    sender.table("line").longColumn("foo", 123).atNow();
                    sender.flush();

                    TableToken tt2 = serverMain2.getEngine().getTableTokenIfExists("line");
                    Assert.assertNotNull(tt2);
                }
            }
        });
    }

    @Test
    public void testFirstServerIsDownTCP() {
        try (Sender ignore = Sender.fromConfig("tcp::addr=" + HOST + ":" + PORT1 + ";addr=" + HOST + ":" + PORT2 + ";")) {
            Assert.fail();
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "only a single address (host:port) is supported for TCP transport");
        }
    }

    @Test
    public void testServersAllDown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender ignore = Sender.fromConfig("http::addr=" + HOST + ":" + PORT1 + ";addr=" + HOST + ":" + PORT2 + ";retry_timeout=500")) {
                Assert.fail();
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to detect server line protocol version");
            }
        });
    }

    @Test
    public void testServersStartAndStop() throws Exception {
        String root1 = "server1" + System.currentTimeMillis();
        String root2 = "server2" + System.currentTimeMillis();
        TestUtils.assertMemoryLeak(() -> {
            TestServerMain serverMain1 = startInstance(root1, HOST, PORT1, false, null);
            serverMain1.start();
            TestServerMain serverMain2 = startInstance(root2, HOST, PORT2, false, null);
            serverMain2.start();

            final @Nullable TxReader[] r1 = {null};
            final @Nullable TxReader[] r2 = {null};

            long expectedTotalRows = 100_000;
            try (Sender sender = Sender.fromConfig("http::addr=" + HOST + ":" + PORT1 + ";addr=" + HOST + ":" + PORT2 + ";" + "auto_flush=off;")) {
                for (int i = 0; i < expectedTotalRows; i++) {
                    sender.table("line").longColumn("foo", i).atNow();

                    if (i == 10_000) {
                        serverMain2.close();
                        if (serverMain1.hasBeenClosed()) {
                            serverMain1 = startInstance(root1, HOST, PORT1, false, null);
                            serverMain1.start();
                        }
                    }

                    if (i == 20_000) {
                        serverMain1.close();
                        if (serverMain2.hasBeenClosed()) {
                            serverMain2 = startInstance(root2, HOST, PORT2, false, null);
                            serverMain2.start();
                        }
                    }

                    if (i % 100 == 0) {
                        sender.flush();
                    }
                }

                sender.flush();

                if (serverMain1.hasBeenClosed()) {
                    serverMain1 = startInstance(root1, HOST, PORT1, false, null);
                    serverMain1.start();
                }

                if (serverMain2.hasBeenClosed()) {
                    serverMain2 = startInstance(root2, HOST, PORT2, false, null);
                    serverMain2.start();
                }

                Os.sleep(1000);

                TestUtils.drainWalQueue(serverMain1.getEngine());
                TestUtils.drainWalQueue(serverMain2.getEngine());

                Os.sleep(1000);

                TestServerMain finalServerMain1 = serverMain1;
                TestServerMain finalServerMain2 = serverMain2;


                TableToken tt1 = null;
                TableToken tt2 = null;
                for (int i = 0; i < 1_000; i++) {
                    tt1 = finalServerMain1.getEngine().getTableTokenIfExists("line");
                    tt2 = finalServerMain2.getEngine().getTableTokenIfExists("line");
                    if (tt1 != null && tt2 != null) {
                        break;
                    } else {
                        Os.sleep(10);
                    }
                }

                TableToken finalTt1 = tt1;
                TableToken finalTt2 = tt2;
                TestUtils.assertEventually(() -> {
                    r1[0] = new TxReader(finalServerMain1.getEngine().getConfiguration().getFilesFacade());
                    r2[0] = new TxReader(finalServerMain2.getEngine().getConfiguration().getFilesFacade());

                    long rows1 = getRowCount(finalServerMain1.getEngine(), finalTt1, r1[0]);
                    long rows2 = getRowCount(finalServerMain2.getEngine(), finalTt2, r2[0]);

                    r1[0].close();
                    r2[0].close();

                    System.out.println("Expected: " + expectedTotalRows + ", actual1: " + rows1 + ", actual2: " + rows2);
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

    private void assertThatWalIsDrained(CairoEngine engine, TableToken tt) throws Exception {
        TestUtils.assertEventually(() -> {
            try {
                SeqTxnTracker seqTxnTracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals(seqTxnTracker.getWriterTxn(), seqTxnTracker.getSeqTxn());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    /**
     * Creates and destroys a server based on a delay and jitter.
     */
    private void createAServerAndOccasionallyBlipIt(String rootName, int port, boolean readOnly, AtomicBoolean running, int upMillis, int downMillis, int jitterMillis, AtomicLong count) throws Exception {
        @Nullable TestServerMain serverMain = null;

        assert upMillis > jitterMillis;
        try {
            while (running.get()) {
                serverMain = startInstance(rootName, HOST, port, readOnly, "port=" + port);
                assert serverMain.hasStarted();

                int jitter = rnd.nextInt(jitterMillis) - jitterMillis / 2;
                Os.sleep(upMillis + jitter);
                serverMain.close();
                assert serverMain.hasBeenClosed();

                jitter = rnd.nextInt(jitterMillis) - jitterMillis / 2;
                Os.sleep(downMillis + jitter); // enough time to give the client a chance to reconnect to a different server
            }
        } finally {
            if (serverMain == null || serverMain.hasBeenClosed()) {
                serverMain = startInstance(rootName, HOST, port, readOnly, "port=" + port);
            }
            TestUtils.drainWalQueue(serverMain.getEngine());
            TableToken tt = serverMain.getEngine().getTableTokenIfExists("line");
            if (tt != null) {
                assertThatWalIsDrained(serverMain.getEngine(), tt);
                try (TxReader txReader = new TxReader(serverMain.getEngine().getConfiguration().getFilesFacade())) {
                    count.set(getRowCount(serverMain.getEngine(), serverMain.getEngine().getTableTokenIfExists("line"), txReader));
                }
            } else {
                LOG.error().$("table not found").$(); // useful for test troubleshooting
            }
            Misc.free(serverMain);
            Path.clearThreadLocals();
        }
    }

    private long getRowCount(CairoEngine engine, TableToken token, TxReader txReader) {
        try (TableMetadata tm = engine.getTableMetadata(token)) {
            int partitionBy = tm.getPartitionBy();
            int timestampType = tm.getTimestampType();

            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName());
                TableUtils.setTxReaderPath(txReader, path, timestampType, partitionBy);
                return txReader.unsafeLoadRowCount();
            }
        }
    }

    private void sendToTwoPossibleServers(String host1, int port1, String host2, int port2, int durationMillis, AtomicLong count) {
        long deadline = System.currentTimeMillis() + durationMillis;
        try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                .address(host1).port(port1)
                .address(host2).port(port2)
                .disableAutoFlush()
                .maxBackoffMillis(1)
                .build()) {
            while (System.currentTimeMillis() < deadline) {
                sender.table("line").longColumn("foo", 123).atNow();

                Os.sleep(1);
                sender.flush();

                count.incrementAndGet();
            }
        }
        Path.clearThreadLocals();
    }
}
