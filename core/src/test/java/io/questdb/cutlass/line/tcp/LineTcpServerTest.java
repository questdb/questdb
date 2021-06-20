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

import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LineTcpServerTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(LineTcpServerTest.class);
    private final static String AUTH_KEY_ID1 = "testUser1";
    private final static PrivateKey AUTH_PRIVATE_KEY1 = AuthDb.importPrivateKey("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48");
    private final static String AUTH_KEY_ID2 = "testUser2";
    private final static PrivateKey AUTH_PRIVATE_KEY2 = AuthDb.importPrivateKey("lwJi3TSb4G6UcHxFJmPhOTWa4BLwJOOiK76wT6Uk7pI");
    private static final long TEST_TIMEOUT_IN_MS = 120000;
    private final WorkerPool sharedWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
        private final int[] affinity = {-1, -1};

        @Override
        public int[] getWorkerAffinity() {
            return affinity;
        }

        @Override
        public int getWorkerCount() {
            return 2;
        }

        @Override
        public boolean haltOnError() {
            return true;
        }
    });
    private final int bindPort = 9002; // Dont clash with other tests since they may run in parallel
    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
        @Override
        public int getBindIPv4Address() {
            return 0;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }
    };
    private int maxMeasurementSize = 50;
    private String authKeyId = null;
    private int msgBufferSize = 1024;
    private long minIdleMsBeforeWriterRelease = 30000;
    private int aggressiveReadRetryCount = 0;
    private final LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration() {
        @Override
        public IODispatcherConfiguration getNetDispatcherConfiguration() {
            return ioDispatcherConfiguration;
        }

        @Override
        public int getNetMsgBufferSize() {
            return msgBufferSize;
        }

        @Override
        public int getMaxMeasurementSize() {
            return maxMeasurementSize;
        }

        @Override
        public int getWriterQueueCapacity() {
            return 4;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return testMicrosClock;
        }

        @Override
        public int getNUpdatesPerLoadRebalance() {
            return 100;
        }

        @Override
        public double getMaxLoadRatio() {
            // Always rebalance as long as there are more tables than threads;
            return 1;
        }

        @Override
        public long getMaintenanceInterval() {
            return 25;
        }

        @Override
        public String getAuthDbPath() {
            if (null == authKeyId) {
                return null;
            }
            URL u = getClass().getResource("authDb.txt");
            assert u != null;
            return u.getFile();
        }

        @Override
        public long getWriterIdleTimeout() {
            return minIdleMsBeforeWriterRelease;
        }

        @Override
        public int getAggressiveReadRetryCount() {
            return aggressiveReadRetryCount;
        }
    };

    private Path path;

    @Test
    public void testGoodAuthenticated() throws Exception {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, 768, 1_000, false);
    }

    @Test(expected = NetworkError.class)
    public void testInvalidSignature() throws Exception {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY2, 768, 6_000, true);
    }

    @Test(expected = NetworkError.class)
    public void testInvalidUser() throws Exception {
        test(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2, 768, 6_000, true);
    }

    @Test
    public void testSomeWritersReleased() throws Exception {
        runInContext(() -> {
            String lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";

            int iterations = 8;
            int threadCount = 8;
            CharSequenceObjHashMap<SOUnboundedCountDownLatch> tableIndex = new CharSequenceObjHashMap<>();
            tableIndex.put("weather", new SOUnboundedCountDownLatch());
            for (int i = 1; i < threadCount; i++) {
                tableIndex.put("weather" + i, new SOUnboundedCountDownLatch());
            }

            // One engine hook for all writers
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                    tableIndex.get(name).countDown();
                }
            });

            try {
                sendAndWait(lineData, tableIndex, 1);
                SOCountDownLatch threadPushFinished = new SOCountDownLatch(threadCount - 1);
                for (int i = 1; i < threadCount; i++) {
                    final String threadTable = "weather" + i;
                    final String lineDataThread = lineData.replace("weather", threadTable);
                    sendNoWait(threadTable, lineDataThread);
                    new Thread(() -> {
                        try {
                            for (int n = 0; n < iterations; n++) {
                                Thread.sleep(minIdleMsBeforeWriterRelease - 50);
                                send(lineDataThread, threadTable, false);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            threadPushFinished.countDown();
                        }
                    }).start();
                }

                sendAndWait(lineData, tableIndex, 2);
                try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "weather")) {
                    w.truncate();
                }
                sendAndWait(lineData, tableIndex, 4);

                String header = "location\ttemperature\ttimestamp\n";
                String[] lines = {"us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n",
                        "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n",
                        "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n"};
                String expected = header + lines[0] + lines[1] + lines[2];
                assertTable(expected, "weather");

                // Concatenate iterations + 1 of identical insert results
                // to assert against weather 1-8 tables
                StringBuilder expectedSB = new StringBuilder(header);
                for (int l = 0; l < lines.length; l++) {
                    expectedSB.append(Chars.repeat(lines[l], iterations + 1));
                }

                // Wait async ILP send threads to finish.
                threadPushFinished.await();
                for (int i = 1; i < threadCount; i++) {
                    // Wait writer to be released and check.
                    String tableName = "weather" + i;
                    try {
                        try {
                            assertTable(expectedSB, tableName);
                        } catch (AssertionError e) {
                            int releasedCount = -tableIndex.get(tableName).getCount();
                            // Wait one more writer release before re-trying to compare
                            wait(tableIndex.get(tableName), releasedCount + 1, 20, minIdleMsBeforeWriterRelease);
                            assertTable(expectedSB, tableName);
                        }
                    } catch (Throwable err) {
                        LOG.error().$("Error '").$(err.getMessage()).$("' comparing table: ").$(tableName).$();
                        throw err;
                    }
                }
            } finally {
                // Clean engine hook
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                });
            }
        });
    }

    private void wait(SOUnboundedCountDownLatch latch, int value, long msTimeout, long iterations) {
        while (-latch.getCount() < value && iterations-- > 0) {
            try {
                Thread.sleep(msTimeout);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    @Test
    public void testUnauthenticated() throws Exception {
        test(null, null, 200, 1_000, false);
    }

    @Test
    public void testUnauthenticatedAggressive() throws Exception {
        aggressiveReadRetryCount = 1;
        test(null, null, 200, 1_000, false);
    }

    @Test
    public void testWriter17Fields() throws Exception {
        int defaultMeasurementSize = maxMeasurementSize;
        String lineData = "tableCRASH,tag_n_1=1,tag_n_2=2,tag_n_3=3,tag_n_4=4,tag_n_5=5,tag_n_6=6," +
                "tag_n_7=7,tag_n_8=8,tag_n_9=9,tag_n_10=10,tag_n_11=11,tag_n_12=12,tag_n_13=13," +
                "tag_n_14=14,tag_n_15=15,tag_n_16=16,tag_n_17=17 value=42.4 1619509249714000000\n";
        try {
            maxMeasurementSize = lineData.length();
            runInContext(() -> {
                send(lineData, "tableCRASH", true, false);

                String expected = "tag_n_1\ttag_n_2\ttag_n_3\ttag_n_4\ttag_n_5\ttag_n_6\ttag_n_7\ttag_n_8\ttag_n_9\ttag_n_10\ttag_n_11\ttag_n_12\ttag_n_13\ttag_n_14\ttag_n_15\ttag_n_16\ttag_n_17\tvalue\ttimestamp\n" +
                        "1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t42.400000000000006\t2021-04-27T07:40:49.714000Z\n";
                assertTable(expected, "tableCRASH");
            });
        } finally {
            maxMeasurementSize = defaultMeasurementSize;
        }
    }

    @Test
    public void testWriterAllLongs() throws Exception {
        currentMicros = 1;
        try (TableModel m = new TableModel(configuration, "messages", PartitionBy.MONTH)) {
            m.timestamp("ts")
                    .col("id", ColumnType.LONG)
                    .col("author", ColumnType.LONG)
                    .col("guild", ColumnType.LONG)
                    .col("channel", ColumnType.LONG)
                    .col("flags", ColumnType.BYTE);
            CairoTestUtils.createTableWithVersion(m, ColumnType.VERSION);
        }

        int defaultMeasurementSize = maxMeasurementSize;
        String lineData = "messages id=843530699759026177i,author=820703963477180437i,guild=820704412095479830i,channel=820704412095479833i,flags=6i\n";
        try {
            maxMeasurementSize = lineData.length();
            runInContext(() -> {
                send(lineData, "messages");
                String expected = "ts\tid\tauthor\tguild\tchannel\tflags\n" +
                        "1970-01-01T00:00:00.000001Z\t843530699759026177\t820703963477180437\t820704412095479830\t820704412095479833\t6\n";
                assertTable(expected, "messages");
            });
        } finally {
            maxMeasurementSize = defaultMeasurementSize;
        }
    }

    @Test
    public void testWriterRelease1() throws Exception {
        runInContext(() -> {

            String lineData = "weather,location=us-midwest temperature=82 1465839830100400200\n" +
                    "weather,location=us-midwest temperature=83 1465839830100500200\n" +
                    "weather,location=us-eastcoast temperature=81 1465839830101400200\n";
            send(lineData, "weather");

            lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            send(lineData, "weather");

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
            send(lineData, "weather");

            try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "weather")) {
                w.truncate();
            }

            lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            send(lineData, "weather");

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
            send(lineData, "weather");

            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "weather");

            lineData = "weather,location=us-midwest temperature=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast temperature=89 1465839830102400200\n" +
                    "weather,location=us-westcost temperature=82 1465839830102500200\n";
            send(lineData, "weather");

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
            send(lineData, "weather");

            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "weather");

            lineData = "weather,loc=us-midwest temp=85 1465839830102300200\n" +
                    "weather,loc=us-eastcoast temp=89 1465839830102400200\n" +
                    "weather,loc=us-westcost temp=82 1465839830102500200\n";
            send(lineData, "weather");

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
            send(lineData, "weather");
            engine.remove(AllowAllCairoSecurityContext.INSTANCE, path, "weather");

            lineData = "weather,location=us-midwest,source=sensor1 temp=85 1465839830102300200\n" +
                    "weather,location=us-eastcoast,source=sensor2 temp=89 1465839830102400200\n" +
                    "weather,location=us-westcost,source=sensor1 temp=82 1465839830102500200\n";
            send(lineData, "weather");

            String expected = "location\tsource\ttemp\ttimestamp\n" +
                    "us-midwest\tsensor1\t85.0\t2016-06-13T17:43:50.102300Z\n" +
                    "us-eastcoast\tsensor2\t89.0\t2016-06-13T17:43:50.102400Z\n" +
                    "us-westcost\tsensor1\t82.0\t2016-06-13T17:43:50.102500Z\n";
            assertTable(expected, "weather");
        });
    }

    private void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    private void runInContext(Runnable r) throws Exception {
        minIdleMsBeforeWriterRelease = 250;
        assertMemoryLeak(() -> {
            path = new Path(4096);
            try (LineTcpServer ignored = LineTcpServer.create(lineConfiguration, sharedWorkerPool, LOG, engine)) {
                sharedWorkerPool.assignCleaner(Path.CLEANER);
                sharedWorkerPool.start(LOG);
                try {
                    r.run();
                } catch (Throwable err) {
                    LOG.error().$("Stopping ILP worker pool because of an error").$();
                    throw err;
                } finally {
                    sharedWorkerPool.halt();
                    Path.clearThreadLocals();
                }
            } catch (Throwable err) {
                LOG.error().$("Stopping ILP server because of an error").$();
                throw err;
            } finally {
                Misc.free(path);
            }
        });
    }

    private void send(String lineData, String tableName, boolean wait) {
        send(lineData, tableName, wait, true);
    }

    private void send(String lineData, String tableName, boolean wait, boolean noLinger) {
        SOCountDownLatch releaseLatch = new SOCountDownLatch(1);
        if (wait) {
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (Chars.equals(tableName, name)) {
                    if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                        releaseLatch.countDown();
                    }
                }
            });
        }

        try {
            int ipv4address = Net.parseIPv4("127.0.0.1");
            long sockaddr = Net.sockaddr(ipv4address, bindPort);
            long fd = Net.socketTcp(true);
            try {
                TestUtils.assertConnect(fd, sockaddr, noLinger);
                byte[] lineDataBytes = lineData.getBytes(StandardCharsets.UTF_8);
                long bufaddr = Unsafe.malloc(lineDataBytes.length);
                try {
                    for (int n = 0; n < lineDataBytes.length; n++) {
                        Unsafe.getUnsafe().putByte(bufaddr + n, lineDataBytes[n]);
                    }
                    int rc = Net.send(fd, bufaddr, lineDataBytes.length);
                    Assert.assertEquals(lineDataBytes.length, rc);
                } finally {
                    Unsafe.free(bufaddr, lineDataBytes.length);
                }
            } finally {
                Net.close(fd);
                Net.freeSockAddr(sockaddr);
            }
            if (wait) {
                releaseLatch.await();
            }
        } finally {
            if (wait) {
                engine.setPoolListener(null);
            }
        }
    }

    private void send(String lineData, String tableName) {
        send(lineData, tableName, true);
    }

    private void sendAndWait(
            String lineData,
            CharSequenceObjHashMap<SOUnboundedCountDownLatch> tableIndex,
            int expectedReleaseCount
    ) {
        send(lineData, "weather", false);
        tableIndex.get("weather").await(expectedReleaseCount);
    }

    private void sendNoWait(String threadTable, String lineDataThread) {
        send(lineDataThread, threadTable, false);
    }

    private void test(
            String authKeyId,
            PrivateKey authPrivateKey,
            int msgBufferSize,
            final int nRows,
            boolean expectDisconnect
    ) throws Exception {
        this.authKeyId = authKeyId;
        this.msgBufferSize = msgBufferSize;
        assertMemoryLeak(() -> {
            final String[] locations = {"london", "paris", "rome"};

            final CharSequenceHashSet tables = new CharSequenceHashSet();
            tables.add("weather1");
            tables.add("weather2");
            tables.add("weather3");

            SOCountDownLatch tablesCreated = new SOCountDownLatch();
            tablesCreated.setCount(tables.size());

            final Rnd rand = new Rnd();
            final StringBuilder[] expectedSbs = new StringBuilder[tables.size()];

            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (factoryType == PoolListener.SRC_WRITER && event == PoolListener.EV_RETURN) {
                    if (tables.contains(name)) {
                        tablesCreated.countDown();
                    }
                }
            });

            minIdleMsBeforeWriterRelease = 100;
            try (LineTcpServer ignored = LineTcpServer.create(lineConfiguration, sharedWorkerPool, LOG, engine)) {
                long startEpochMs = System.currentTimeMillis();
                sharedWorkerPool.assignCleaner(Path.CLEANER);
                sharedWorkerPool.start(LOG);

                try {
                    final LineProtoSender[] senders = new LineProtoSender[tables.size()];
                    for (int n = 0; n < senders.length; n++) {
                        if (null != authKeyId) {
                            AuthenticatedLineTCPProtoSender sender = new AuthenticatedLineTCPProtoSender(
                                    authKeyId,
                                    authPrivateKey,
                                    Net.parseIPv4("127.0.0.1"),
                                    bindPort,
                                    4096
                            );
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
                        int nTable = nRow < tables.size() ? nRow : rand.nextInt(tables.size());
                        LineProtoSender sender = senders[nTable];
                        StringBuilder sb = expectedSbs[nTable];
                        CharSequence tableName = tables.get(nTable);
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
                        sb.append(tsSink);
                        sb.append('\n');
                        sender.$(ts * 1000);
                        sender.flush();
                        if (expectDisconnect) {
                            // To prevent all data being buffered before the expected disconnect slow sending
                            Thread.sleep(100);
                        }
                        ts += rand.nextInt(1000);
                    }

                    for (int n = 0; n < senders.length; n++) {
                        LineProtoSender sender = senders[n];
                        sender.close();
                    }

                    Assert.assertFalse(expectDisconnect);
                    boolean ready = tablesCreated.await(TimeUnit.MINUTES.toNanos(1));
                    if (!ready) {
                        throw new IllegalStateException("Timeout waiting for tables to be created");
                    }

                    int nRowsWritten;
                    do {
                        nRowsWritten = 0;
                        long timeTakenMs = System.currentTimeMillis() - startEpochMs;
                        if (timeTakenMs > TEST_TIMEOUT_IN_MS) {
                            LOG.error().$("after ").$(timeTakenMs).$("ms tables only had ").$(nRowsWritten).$(" rows out of ").$(nRows).$();
                            break;
                        }
                        Thread.yield();
                        for (int n = 0; n < tables.size(); n++) {
                            CharSequence tableName = tables.get(n);
                            while (true) {
                                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName)) {
                                    TableReaderRecordCursor cursor = reader.getCursor();
                                    while (cursor.hasNext()) {
                                        nRowsWritten++;
                                    }
                                    break;
                                } catch (EntryLockedException ex) {
                                    LOG.info().$("retrying read for ").$(tableName).$();
                                    LockSupport.parkNanos(1);
                                }
                            }
                        }
                    } while (nRowsWritten < nRows);
                    LOG.info().$(nRowsWritten).$(" rows written").$();
                } finally {
                    sharedWorkerPool.halt();
                }
            } finally {
                engine.setPoolListener(null);
            }

            for (int n = 0; n < tables.size(); n++) {
                CharSequence tableName = tables.get(n);
                LOG.info().$("checking table ").$(tableName).$();
                assertTable(expectedSbs[n], tableName);
            }
        });
    }

}
