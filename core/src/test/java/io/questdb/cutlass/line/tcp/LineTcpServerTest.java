/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line.tcp;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderRecordCursor;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;

public class LineTcpServerTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpServerTest.class);
    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
    private final static String AUTH_KEY_ID2 = "testUser2";
    private final static PrivateKey AUTH_PRIVATE_KEY2 = AuthDb.importPrivateKey("lwJi3TSb4G6UcHxFJmPhOTWa4BLwJOOiK76wT6Uk7pI");
    private static final AtomicInteger N_TEST = new AtomicInteger();
    private static final long TEST_TIMEOUT_IN_MS = 120000;

    private Path path;
    private WorkerPool sharedWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
        private final int[] affinity = { -1, -1 };

        @Override
        public boolean haltOnError() {
            return true;
        }

        @Override
        public int getWorkerCount() {
            return 2;
        }

        @Override
        public int[] getWorkerAffinity() {
            return affinity;
        }
    });

    private final int bindIp = 0;
    private final int bindPort = 9002; // Dont clash with other tests since they may run in parallel
    private IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
        @Override
        public int getBindIPv4Address() {
            return bindIp;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }
    };
    private String authKeyId = null;
    private int msgBufferSize = 1024;
    private long minIdleMsBeforeWriterRelease = 30000;
    private LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration() {
        @Override
        public IODispatcherConfiguration getNetDispatcherConfiguration() {
            return ioDispatcherConfiguration;
        }

        @Override
        public int getWriterQueueCapacity() {
            return 4;
        }

        @Override
        public int getNetMsgBufferSize() {
            return msgBufferSize;
        }

        @Override
        public int getMaxMeasurementSize() {
            return 50;
        }

        @Override
        public int getNUpdatesPerLoadRebalance() {
            return 100;
        }

        @Override
        public int getMaxUncommittedRows() {
            return 50;
        }

        @Override
        public double getMaxLoadRatio() {
            // Always rebalance as long as there are more tables than threads;
            return 1;
        }

        @Override
        public String getAuthDbPath() {
            if (null == authKeyId) {
                return null;
            }
            URL u = getClass().getResource("authDb.txt");
            return u.getFile();
        }

        @Override
        public long getMaintenanceJobHysteresisInMs() {
            return 25;
        };

        @Override
        public long getMinIdleMsBeforeWriterRelease() {
            return minIdleMsBeforeWriterRelease;
        };

    };
    private CairoEngine engine;

    @Test
    public void testUnauthenticated() {
        test(null, null, 200, 1_000);
    }

    @Test
    public void testGoodAuthenticated() {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, 768, 1_000);
    }

    @Test(expected = NetworkError.class)
    public void testInvalidUser() {
        test(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2, 768, 100);
    }

    @Test(expected = NetworkError.class)
    public void testInvalidSignature() {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY2, 768, 100);
    }

    @Test
    public void testWriterRelease1() throws Exception {
        runInContext(() -> {
            String lineData = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            send(lineData);

            TableWriter writer = waitForWriterRelease("weather", 3);
            writer.close();

            lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            send(lineData);

            writer = waitForWriterRelease("weather", 6);
            writer.close();

            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t82.0\t2016-06-13T17:43:50.100400Z\n" +
                    "us-midwest\t83.0\t2016-06-13T17:43:50.100500Z\n" +
                    "us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease2() throws Exception {
        runInContext(() -> {
            String lineData = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            send(lineData);

            TableWriter writer = waitForWriterRelease("weather", 3);
            writer.truncate();
            writer.close();

            lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            send(lineData);

            writer = waitForWriterRelease("weather", 3);
            writer.close();

            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease3() throws Exception {
        runInContext(() -> {
            String lineData = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            send(lineData);

            TableWriter writer = waitForWriterRelease("weather", 3);
            writer.close();
            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "weather");

            lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            send(lineData);

            writer = waitForWriterRelease("weather", 3);
            writer.close();

            String expected = "location\ttemperature\ttimestamp\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease4() throws Exception {
        runInContext(() -> {
            String lineData = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            send(lineData);

            TableWriter writer = waitForWriterRelease("weather", 3);
            writer.close();
            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "weather");

            lineData = "weather,loc=us-midwest temp=85 1465839830102300200\n" +
                    "weather,loc=us-eastcoast temp=89 1465839830102400200\n" +
                    "weather,loc=us-westcost temp=82 1465839830102500200\n";
            send(lineData);

            writer = waitForWriterRelease("weather", 3);
            writer.close();

            String expected = "loc\ttemp\ttimestamp\n" +
                    "us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease5() throws Exception {
        runInContext(() -> {
            String lineData = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            send(lineData);

            TableWriter writer = waitForWriterRelease("weather", 3);
            writer.close();
            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "weather");

            lineData = "weather,location=us-midwest,source=sensor1 temp=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast,source=sensor2 temp=89 1465839830102400200\n" +
                    "weather,location=us-westcost,source=sensor1 temp=82 1465839830102500200\n";
            send(lineData);

            writer = waitForWriterRelease("weather", 3);
            writer.close();

            String expected = "location\tsource\ttemp\ttimestamp\n" +
                    "us-midwest\tsensor1\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\tsensor2\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\tsensor1\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    private void runInContext(Runnable r) throws Exception {
        minIdleMsBeforeWriterRelease = 250;
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                path = new Path(4096);
                LineTcpServerTest.this.engine = engine;
                LineTcpServer tcpServer = LineTcpServer.create(lineConfiguration, sharedWorkerPool, LOG, engine);
                sharedWorkerPool.start(LOG);

                r.run();

                sharedWorkerPool.halt();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                Misc.free(tcpServer);
            } finally {
                LineTcpServerTest.this.engine = null;
                path.close();
            }
        });
    }

    private void send(String lineData) {
        int ipv4address = Net.parseIPv4("127.0.0.1");
        long sockaddr = Net.sockaddr(ipv4address, bindPort);
        long fd = Net.socketTcp(true);
        if (Net.connect(fd, sockaddr) != 0) {
            throw NetworkError.instance(Os.errno(), "could not connect to ").ip(ipv4address);
        }
        byte[] lineDataBytes = lineData.getBytes(StandardCharsets.UTF_8);
        long bufaddr = Unsafe.malloc(lineDataBytes.length);
        for (int n = 0; n < lineDataBytes.length; n++) {
            Unsafe.getUnsafe().putByte(bufaddr + n, lineDataBytes[n]);
        }
        int rc = Net.send(fd, bufaddr, lineDataBytes.length);
        Unsafe.free(bufaddr, lineDataBytes.length);
        Net.close(fd);
        Net.freeSockAddr(sockaddr);

        Assert.assertEquals(lineDataBytes.length, rc);
    }

    private TableWriter waitForWriterRelease(String tableName, int nMinRows) {
        int status = TableUtils.TABLE_DOES_NOT_EXIST;
        int nRows = 0;
        long startEpochMillis = System.currentTimeMillis();
        while (true) {
            if (status != TableUtils.TABLE_EXISTS) {
                status = engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, tableName, 0, tableName.length());
            } else if (nRows < nMinRows) {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                    nRows = (int) reader.size();
                } catch (EntryLockedException e) {
                    //
                }
            } else {
                try {
                    return engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName);
                } catch (EntryUnavailableException ex) {
                    //
                }
            }

            long epochMillis = System.currentTimeMillis();
            if (epochMillis - startEpochMillis > 15000000) {
                Assert.fail("Timed out waiting for writer release");
            }
            try {
                Thread.sleep(lineConfiguration.getMaintenanceJobHysteresisInMs());
            } catch (InterruptedException e) {
                Assert.fail("Interrupted out waiting for writer release");
            }
        }
    }

    private void test(String authKeyId, PrivateKey authPrivateKey, int msgBufferSize, final int nRows) {
        int nTest = N_TEST.incrementAndGet();
        this.authKeyId = authKeyId;
        this.msgBufferSize = msgBufferSize;

        final String[] tables = { "weather1-" + nTest, "weather2-" + nTest, "weather3-" + nTest };
        final String[] locations = { "london", "paris", "rome" };

        final Random rand = new Random(0);
        final StringBuilder[] expectedSbs = new StringBuilder[tables.length];

        long startEpochMs = System.currentTimeMillis();
        try (CairoEngine engine = new CairoEngine(configuration)) {
            LineTcpServer tcpServer = LineTcpServer.create(lineConfiguration, sharedWorkerPool, LOG, engine);

            SOCountDownLatch tablesCreated = new SOCountDownLatch();
            tablesCreated.setCount(tables.length);
            Supplier<Path> pathSupplier = Path::new;
            sharedWorkerPool.assign(new SynchronizedJob() {
                private final ThreadLocal<Path> tlPath = ThreadLocal.withInitial(pathSupplier);

                @Override
                public boolean runSerially() {
                    int nTable = tables.length - tablesCreated.getCount();
                    if (nTable < tables.length) {
                        String tableName = tables[nTable];
                        int status = engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, tlPath.get(), tableName);
                        if (status == TableUtils.TABLE_EXISTS) {
                            tablesCreated.countDown();
                        }
                        return true;
                    }

                    return false;
                }
            });
            sharedWorkerPool.start(LOG);

            try {
                final LineProtoSender[] senders = new LineProtoSender[tables.length];
                for (int n = 0; n < senders.length; n++) {
                    if (null != authKeyId) {
                        AuthenticatedLineTCPProtoSender sender = new AuthenticatedLineTCPProtoSender(authKeyId, authPrivateKey, Net.parseIPv4("127.0.0.1"), bindPort, 4096);
                        sender.authenticate();
                        senders[n] = sender;
                    } else {
                        senders[n] = new LineTCPProtoSender(Net.parseIPv4("127.0.0.1"), bindPort, 4096);
                    }
                    StringBuilder sb = new StringBuilder((nRows + 1) * lineConfiguration.getMaxMeasurementSize());
                    sb.append("location\ttemp\ttimestamp\n");
                    expectedSbs[n] = sb;
                }

                long ts = Os.currentTimeMicros();
                StringSink tsSink = new StringSink();
                for (int nRow = 0; nRow < nRows; nRow++) {
                    int nTable = nRow < tables.length ? nRow : rand.nextInt(tables.length);
                    LineProtoSender sender = senders[nTable];
                    StringBuilder sb = expectedSbs[nTable];
                    String tableName = tables[nTable];
                    sender.metric(tableName);
                    String location = locations[rand.nextInt(locations.length)];
                    sb.append(location);
                    sb.append('\t');
                    sender.tag("location", location);
                    int temp = rand.nextInt(100);
                    sb.append(temp);
                    sb.append('\t');
                    sender.field("temp", temp);
                    tsSink.clear();
                    TimestampFormatUtils.appendDateTimeUSec(tsSink, ts);
                    sb.append(tsSink.toString());
                    sb.append('\n');
                    sender.$(ts * 1000);
                    sender.flush();
                    ts += rand.nextInt(1000);
                }

                for (int n = 0; n < senders.length; n++) {
                    LineProtoSender sender = senders[n];
                    sender.close();
                }

                boolean created = tablesCreated.await(TEST_TIMEOUT_IN_MS * 1_000_000);
                Assert.assertTrue(created);
                int nRowsWritten;
                do {
                    nRowsWritten = 0;
                    long timeTakenMs = System.currentTimeMillis() - startEpochMs;
                    if (timeTakenMs > TEST_TIMEOUT_IN_MS) {
                        LOG.error().$("after ").$(timeTakenMs).$("ms tables only had ").$(nRowsWritten).$(" rows out of ").$(nRows).$();
                        break;
                    }

                    Thread.yield();
                    for (int n = 0; n < tables.length; n++) {
                        String tableName = tables[n];
                        try (TableReader reader = new TableReader(configuration, tableName)) {
                            TableReaderRecordCursor cursor = reader.getCursor();
                            while (cursor.hasNext()) {
                                nRowsWritten++;
                            }
                        }
                    }
                } while (nRowsWritten < nRows);
                LOG.info().$(nRowsWritten).$(" rows written").$();
            } finally {
                sharedWorkerPool.halt();
                Misc.free(tcpServer);
            }
        }

        for (int n = 0; n < tables.length; n++) {
            String tableName = tables[n];
            LOG.info().$("checking table ").$(tableName).$();
            assertTable(expectedSbs[n].toString(), tableName);
        }
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            assertThat(expected, reader.getCursor(), reader.getMetadata(), true);
        }
    }
}
