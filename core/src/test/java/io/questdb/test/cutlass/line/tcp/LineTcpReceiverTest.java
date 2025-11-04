/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.line.AbstractLineSender;
import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.questdb.cutlass.auth.AuthUtils.EC_ALGORITHM;
import static io.questdb.test.tools.TestUtils.assertEventually;

public class LineTcpReceiverTest extends AbstractLineTcpReceiverTest {
    private final static Log LOG = LogFactory.getLog(LineTcpReceiverTest.class);
    private static final long TEST_TIMEOUT_IN_MS = 120000;
    private final boolean walEnabled;
    private Path path;

    public LineTcpReceiverTest() {
        Rnd rnd = TestUtils.generateRandom(AbstractCairoTest.LOG);
        this.walEnabled = TestUtils.isWal(rnd);
        this.timestampType = TestUtils.getTimestampType(rnd);
    }

    @Test
    public void generateKeys() throws NoSuchAlgorithmException {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(EC_ALGORITHM);

        KeyPair keys = keyPairGenerator.generateKeyPair();
        ECPublicKey publicKey = (ECPublicKey) keys.getPublic();

        String x = Base64.getUrlEncoder().encodeToString(
                publicKey.getW().getAffineX().toByteArray()
        );

        String y = Base64.getUrlEncoder().encodeToString(
                publicKey.getW().getAffineY().toByteArray()
        );

        System.out.println("x: " + x);
        System.out.println("y: " + y);

        ECPrivateKey privateKey = (ECPrivateKey) keys.getPrivate();
        System.out.println("s: " +
                Base64.getUrlEncoder().encodeToString(
                        privateKey.getS().toByteArray()
                )
        );

        System.out.printf("%s\tec-p-256-sha256\t%s\t%s%n", AUTH_KEY_ID1, x, y);
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        path = new Path();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        path.close();
        super.tearDown();
    }

    @Test
    public void testCaseInsensitiveTableName() throws Exception {
        autoCreateNewColumns = true;
        runInContext((receiver) -> {
            String tableName = "up";

            String lineData =
                    """
                            up out=1.0 631150000000000000
                            up out=2.0 631152000000000000
                            UP out=3.0 631160000000000000
                            up out=4.0 631170000000000000
                            """;

            // WAL ILP will create 2 WAL writer, because of different casing. In case of WAL need to wait for 2 writer releases.
            CountDownLatch released = new CountDownLatch(walEnabled ? 2 : 1);
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (name != null && Chars.equalsNc(name.getTableName(), tableName)) {
                    if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                        released.countDown();
                    }
                }
            });
            send("up", WAIT_NO_WAIT, () -> sendToSocket(lineData));

            released.await();
            if (walEnabled) {
                mayDrainWalQueue();
            }
            String expected = """
                    out\ttimestamp
                    1.0\t1989-12-31T23:26:40.000000Z
                    2.0\t1990-01-01T00:00:00.000000Z
                    3.0\t1990-01-01T02:13:20.000000Z
                    4.0\t1990-01-01T05:00:00.000000Z
                    """;
            assertTable(expected, "up");
        });
    }

    @Test
    public void testColumnTypeStaysTheSameWhileColumnAdded() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampNano(timestampType.getTimestampType()));
        final String tableName = "weather";
        final int numOfRows = 2000;

        TableModel m = new TableModel(configuration, tableName, PartitionBy.DAY);
        m.col("abcdef", ColumnType.SYMBOL).timestamp("ts");
        AbstractCairoTest.create(m);
        engine.releaseInactive();

        final SOCountDownLatch dataSent = new SOCountDownLatch(1);
        final SOCountDownLatch dataConsumed = new SOCountDownLatch(1);
        final AtomicInteger sendFailureCounter = new AtomicInteger();

        final byte awaitedFactoryType = walEnabled ? PoolListener.SRC_WAL_WRITER : PoolListener.SRC_WRITER;
        runInContext(receiver -> {
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (factoryType == awaitedFactoryType && event == PoolListener.EV_RETURN) {
                    if (Chars.equalsNc(name.getTableName(), tableName) && name.equals(engine.verifyTableName(tableName))) {
                        dataConsumed.countDown();
                    }
                }
            });

            new Thread(() -> {
                try (Socket socket = getSocket()) {
                    for (int i = 0; i < numOfRows; i++) {
                        String value = (i % 2 == 0) ? "\"test" + i + "\"" : String.valueOf(i);
                        sendToSocket(socket, tableName + ",abcdef=x col=" + value + "\n");
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    sendFailureCounter.incrementAndGet();
                } finally {
                    dataSent.countDown();
                }
            }).start();

            dataSent.await();
            Assert.assertEquals(0, sendFailureCounter.get());

            // this will wait until the writer is returned into the pool
            dataConsumed.await();
            mayDrainWalQueue();
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
            });

            try (TableReader reader = getReader(tableName)) {
                final int expectedNumOfRows = numOfRows / 2;
                Assert.assertEquals(reader.getMetadata().isWalEnabled(), walEnabled);
                Assert.assertEquals(expectedNumOfRows, reader.getTransientRowCount());
            } catch (Exception e) {
                Assert.fail("Reader failed [e=" + e + "]");
            }
        }, false, 250);
    }

    @Test
    public void testConcurrentWriteAndTruncate() throws Exception {
        Assume.assumeTrue(walEnabled);
        runInContext((receiver) -> {
            String tableName = "concurrent_test";
            final int iterations = 500;
            final SOCountDownLatch startLatch = new SOCountDownLatch(2);
            final SOCountDownLatch finishLatch = new SOCountDownLatch(2);
            final AtomicInteger errorCount = new AtomicInteger(0);
            String initialData = tableName + ",location=init_location,symbol=test temperature=20.0 1465839830100000000\n";
            send(initialData, tableName);
            mayDrainWalQueue();

            Thread ilpWriteThread = new Thread(() -> {
                try {
                    startLatch.countDown();
                    startLatch.await();

                    try (Socket socket = getSocket()) {
                        for (int i = 0; i < iterations; i++) {
                            try {
                                // Use duplicate timestamp and symbols to trigger metadata operations
                                String lineData = tableName + ",location=test_location,symbol=sym" + (i % 3) +
                                        " temperature=" + (20.0 + i) + " " +
                                        (1465839830102000000L + (i % 5)) + "\n";
                                sendToSocket(socket, lineData);

                                if (i % 50 == 0) {
                                    mayDrainWalQueue();
                                }

                                if (i % 20 == 0) {
                                    Os.sleep(1);
                                }
                            } catch (Throwable e) {
                                errorCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    finishLatch.countDown();
                }
            });

            Thread truncateThread = new Thread(() -> {
                try {
                    startLatch.countDown();
                    startLatch.await();
                    boolean drop = false;

                    for (int i = 0; i < iterations; i++) {
                        try {
                            // Alternate between truncate and drop operations
                            if (i % 15 == 0) {
                                TableToken tt = engine.getTableTokenIfExists(tableName);
                                if (tt != null) {
                                    try (TableWriterAPI writer = getTableWriterAPI(tableName)) {
                                        writer.truncateSoft();
                                    }
                                    mayDrainWalQueue();
                                    drop = true;
                                }
                            } else if (i % 25 == 0 && drop) {
                                TableToken tt = engine.getTableTokenIfExists(tableName);
                                if (tt != null) {
                                    engine.dropTableOrMatView(path, tt);
                                    drop = false;
                                }
                            }
                            Os.sleep(2);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    errorCount.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    finishLatch.countDown();
                }
            });

            ilpWriteThread.start();
            truncateThread.start();

            try {
                finishLatch.await();
                Assert.assertEquals(0, errorCount.get());
            } finally {
                ilpWriteThread.interrupt();
                truncateThread.interrupt();
            }
        });
    }

    @Test
    public void testCreationAttemptNonPartitionedTableWithWal() throws Exception {
        Assume.assumeTrue(walEnabled);
        partitionByDefault = PartitionBy.NONE;

        final String tableNonPartitioned = "weather_none";
        final String tablePartitioned = "weather_day";

        final boolean isMicro = ColumnType.isTimestampMicro(timestampType.getTimestampType());
        runInContext((receiver) -> {
            // Pre-create a partitioned table, so we can wait until it's created.
            TableModel m = new TableModel(configuration, tablePartitioned, PartitionBy.DAY);
            if (isMicro) {
                m.timestamp("ts").wal();
            } else {
                m.timestampNs("ts").wal();
            }

            TestUtils.createTable(engine, m);

            // Send non-partitioned table rows before the partitioned table ones.
            final String lineData = tableNonPartitioned + " windspeed=2.0 631150000000000000\n" +
                    tableNonPartitioned + " timetocycle=0.0,windspeed=3.0 631160000000000000\n" +
                    tablePartitioned + " windspeed=4.0 631170000000000000\n" +
                    tablePartitioned + " timetocycle=0.0,windspeed=3.0 631160000000000000\n";
            sendLinger(lineData, tablePartitioned, tableNonPartitioned);

            mayDrainWalQueue();

            // Verify that the partitioned table data has landed.
            Assert.assertTrue(isWalTable(tablePartitioned));
            String expected = isMicro
                    ? """
                    ts\twindspeed\ttimetocycle
                    1990-01-01T02:13:20.000000Z\t3.0\t0.0
                    1990-01-01T05:00:00.000000Z\t4.0\tnull
                    """
                    : """
                    ts\twindspeed\ttimetocycle
                    1990-01-01T02:13:20.000000000Z\t3.0\t0.0
                    1990-01-01T05:00:00.000000000Z\t4.0\tnull
                    """;
            assertTable(expected, tablePartitioned);

            // WAL is not supported on non-partitioned tables, so we create a non-WAL table as a fallback.
            Assert.assertFalse(isWalTable(tableNonPartitioned));
            expected = """
                    windspeed\ttimestamp\ttimetocycle
                    2.0\t1989-12-31T23:26:40.000000Z\tnull
                    3.0\t1990-01-01T02:13:20.000000Z\t0.0
                    """;
            assertTable(expected, tableNonPartitioned);
        });
    }

    @Test
    public void testCrossingSymbolBoundary() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String tableName = "punk";
        int count = 2100;
        int startSymbolCount = 2040;
        int writeIterations = 4;
        runInContext((receiver) -> {
            int symbolCount = startSymbolCount;
            for (int it = 0; it < writeIterations; it++) {
                final int iteration = it;
                final int maxIds = symbolCount++;
                send(tableName, WAIT_ENGINE_TABLE_RELEASE, () -> {
                    try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                        for (int i = 0; i < count; i++) {
                            String id = String.valueOf(i % maxIds);
                            sender.metric(tableName)
                                    .tag("id", id)
                                    .$((iteration * count + i) * 10_000_000L);
                        }
                        sender.flush();
                    }
                });
            }
        });
        mayDrainWalQueue();
        try (TableReader reader = getReader(tableName)) {
            Assert.assertEquals(reader.getMetadata().isWalEnabled(), walEnabled);
            Assert.assertEquals(count * writeIterations, reader.size());
        }
    }

    @Test
    public void testDateColumnAcceptsTimestamp() throws Exception {
        partitionByDefault = PartitionBy.NONE;
        String tableName = "date_column_accepts_timestamp";

        runInContext((receiver) -> {
            // Pre-create a partitioned table, so we can wait until it's created.
            TableModel m = new TableModel(configuration, tableName, PartitionBy.DAY);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                m.timestamp("ts").col("dt", ColumnType.DATE).noWal();
            } else {
                m.timestampNs("ts").col("dt", ColumnType.DATE).noWal();
            }
            AbstractCairoTest.create(m);

            final String lineData = tableName + " dt=631150000000000t 631150000000000000\n" +
                    tableName + " dt=631160000000000t 631160000000000000\n";
            sendLinger(lineData, tableName);

            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? """
                    ts\tdt
                    1989-12-31T23:26:40.000000Z\t1989-12-31T23:26:40.000Z
                    1990-01-01T02:13:20.000000Z\t1990-01-01T02:13:20.000Z
                    """
                    : """
                    ts\tdt
                    1989-12-31T23:26:40.000000000Z\t1989-12-31T23:26:40.000Z
                    1990-01-01T02:13:20.000000000Z\t1990-01-01T02:13:20.000Z
                    """;
            assertTable(expected, tableName);
        });
    }

    @Test
    public void testDropTable() throws Exception {
        Assume.assumeTrue(walEnabled && ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 2);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 2);
        String weather = "weather";
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int count = 1;

            @Override
            public long openRWNoCache(LPSZ name, int opts) {
                if (
                        Utf8s.endsWithAscii(name, Files.SEPARATOR + "wal1" + Files.SEPARATOR + "1.lock")
                                && Utf8s.containsAscii(name, weather)
                                && --count == 0
                ) {
                    dropWeatherTable();
                }
                return super.openRWNoCache(name, opts);
            }
        };

        runInContext(filesFacade, (receiver) -> {
            String lineData = weather + ",location=us-midwest temperature=82 1465839830100400200\n" +
                    weather + ",location=us-midwest temperature=83 1465839830100500200\n" +
                    weather + ",location=us-eastcoast temperature=81 1465839830101400200\n" +
                    weather + ",location=us-midwest,source=sensor1 temp=85 1465839830102300200\n" +
                    weather + ",location=us-eastcoast,source=sensor2 temp=89 1465839830102400200\n" +
                    weather + ",location=us-westcost,source=sensor1 temp=82 1465839830102500200\n" +
                    "done ok=t\n";

            // Wait for 2 WAL writer, dropped table WAL writer will not be returned to the pool
            sendWaitWalReleaseCount(lineData, 2);

            mayDrainWalQueue();

            // two of the three commits are lost
            String expected = """
                    location\tsource\ttemp\ttimestamp
                    us-eastcoast\tsensor2\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\tsensor1\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, weather);
        }, false, 250);
    }

    @Test
    public void testFieldValuesHasEqualsChar() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxMeasurementSize = 250;
        String lineData =
                """
                        tab ts_nsec=1111111111111111111i,raw_msg="_________________________________________________________________________________________________________ ____________" 1619509249714000000
                        tab ts_nsec=2222222222222222222i,raw_msg="_________________________________________________________________________________________________________ ____________" 1619509249714000000
                        tab ts_nsec=3333333333333333333i,raw_msg="_________________________________________________________________________________________________________ ____________" 1619509249714000000
                        tab ts_nsec=4444444444444444444i,raw_msg="_________________________________________________________________________________________________________ ____________" 1619509249714000000
                        tab ts_nsec=5555555555555555555i,raw_msg="_________________________________________________________________________________________________________ ____________" 1619509249714000000
                        tab ts_nsec=6666666666666666666i,raw_msg="_________________________________________________________________________________________________________ ____________" 1619509249714000000
                        """;
        runInContext((receiver) -> {
            sendLinger(lineData, "tab");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("tab"));
            }
            String expected = """
                    ts_nsec\traw_msg\ttimestamp
                    1111111111111111111\t_________________________________________________________________________________________________________ ____________\t2021-04-27T07:40:49.714000Z
                    2222222222222222222\t_________________________________________________________________________________________________________ ____________\t2021-04-27T07:40:49.714000Z
                    3333333333333333333\t_________________________________________________________________________________________________________ ____________\t2021-04-27T07:40:49.714000Z
                    4444444444444444444\t_________________________________________________________________________________________________________ ____________\t2021-04-27T07:40:49.714000Z
                    5555555555555555555\t_________________________________________________________________________________________________________ ____________\t2021-04-27T07:40:49.714000Z
                    6666666666666666666\t_________________________________________________________________________________________________________ ____________\t2021-04-27T07:40:49.714000Z
                    """;
            assertTable(expected, "tab");
        });
    }

    @Test
    public void testFieldsReducedNonPartitioned() throws Exception {
        // WAL is not supported on non-partitioned tables
        Assume.assumeFalse(walEnabled);

        TableModel m = new TableModel(configuration, "weather", PartitionBy.NONE);
        if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
            m.col("windspeed", ColumnType.DOUBLE).timestamp();
        } else {
            m.col("windspeed", ColumnType.DOUBLE).timestampNs();
        }

        AbstractCairoTest.create(m);

        String lineData =
                """
                        weather windspeed=2.0 631150000000000000
                        weather timetocycle=0.0,windspeed=3.0 631160000000000000
                        weather windspeed=4.0 631170000000000000
                        """;

        runInContext((receiver) -> {
            sendLinger(lineData, "weather");

            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? """
                    windspeed\ttimestamp\ttimetocycle
                    2.0\t1989-12-31T23:26:40.000000Z\tnull
                    3.0\t1990-01-01T02:13:20.000000Z\t0.0
                    4.0\t1990-01-01T05:00:00.000000Z\tnull
                    """
                    : """
                    windspeed\ttimestamp\ttimetocycle
                    2.0\t1989-12-31T23:26:40.000000000Z\tnull
                    3.0\t1990-01-01T02:13:20.000000000Z\t0.0
                    4.0\t1990-01-01T05:00:00.000000000Z\tnull
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testFieldsReducedO3() throws Exception {
        String lineData =
                """
                        weather windspeed=1.0 631152000000000000
                        weather windspeed=2.0 631150000000000000
                        weather timetocycle=0.0,windspeed=3.0 631160000000000000
                        weather windspeed=4.0 631170000000000000
                        """;

        runInContext((receiver) -> {
            sendLinger(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    windspeed\ttimestamp\ttimetocycle
                    2.0\t1989-12-31T23:26:40.000000Z\tnull
                    1.0\t1990-01-01T00:00:00.000000Z\tnull
                    3.0\t1990-01-01T02:13:20.000000Z\t0.0
                    4.0\t1990-01-01T05:00:00.000000Z\tnull
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testFieldsReducedO3VarLen() throws Exception {
        String lineData =
                """
                        weather dir="NA",windspeed=1.0 631152000000000000
                        weather dir="South",windspeed=2.0 631150000000000000
                        weather dir="North",windspeed=3.0,timetocycle=0.0 631160000000000000
                        weather dir="SSW",windspeed=4.0 631170000000000000
                        """;

        runInContext((receiver) -> {
            sendLinger(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    dir\twindspeed\ttimestamp\ttimetocycle
                    South\t2.0\t1989-12-31T23:26:40.000000Z\tnull
                    NA\t1.0\t1990-01-01T00:00:00.000000Z\tnull
                    North\t3.0\t1990-01-01T02:13:20.000000Z\t0.0
                    SSW\t4.0\t1990-01-01T05:00:00.000000Z\tnull
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testFirstRowIsCancelled() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            send("table", WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    lineTcpSender.disableValidation();
                    lineTcpSender
                            .metric("table")
                            .tag("tag/2", "value=\2") // Invalid column name, line is not saved
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 1")
                            .tag("tag=2", "значение 2")
                            .field("поле=3", "{\"ключ\": \"число\"}")
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 2")
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag=2", "value=\\2")
                            .$(Micros.DAY_MICROS * 1000L);
                    lineTcpSender.flush();
                }
            });

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("table"));
            }
            String expected = """
                    tag1\ttag=2\tполе=3\ttimestamp
                    value 1\tзначение 2\t{"ключ": "число"}\t1970-01-01T00:00:00.000000Z
                    value 2\t\t\t1970-01-01T00:00:00.000000Z
                    \tvalue=\\2\t\t1970-01-02T00:00:00.000000Z
                    """;
            assertTable(expected, "table");
        });
    }

    @Test
    public void testGoodAuthenticated() throws Exception {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1, 768, 1_000, false);
    }

    @Test(expected = LineSenderException.class)
    public void testInvalidSignature() throws Exception {
        test(AUTH_KEY_ID1, AUTH_PRIVATE_KEY2, 768, 6_000, true);
    }

    @Test(expected = LineSenderException.class)
    public void testInvalidUser() throws Exception {
        test(AUTH_KEY_ID2, AUTH_PRIVATE_KEY2, 768, 6_000, true);
    }

    @Test(expected = LineSenderException.class)
    public void testInvalidZeroSignature() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        test(AUTH_KEY_ID1, 768, 100, true, () -> {
            PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, Net.parseIPv4("127.0.0.1"), bindPort, 4096);
            AbstractLineTcpSender sender = new LineTcpSenderV2(channel, 4096, 127) {
                @Override
                protected byte[] signAndEncode(PrivateKey privateKey, byte[] challengeBytes) {
                    byte[] rawSignature = new byte[64];
                    return Base64.getEncoder().encode(rawSignature);
                }
            };
            sender.authenticate(AUTH_KEY_ID1, null);
            return sender;
        });
    }

    @Test
    public void testMetaDataSizeToHitExactly16K() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        final String tableName = "метеорологично_време";
        final int numOfColumns = 255;
        final Rnd rnd = new Rnd();

        final SOCountDownLatch finished = new SOCountDownLatch(1);
        // We set the minIdleMsBeforeWriterRelease interval to a rather large value
        // (1 sec) to prevent false positive WAL writer releases.
        runInContext(receiver -> {
            engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    if (Chars.equalsNc(name.getTableName(), tableName)
                            && name.equals(engine.verifyTableName(tableName))) {
                        finished.countDown();
                    }
                }
            });

            try (Socket socket = getSocket()) {
                // this loop adds columns to the table
                // after the 252nd column has been added the metadata size will be exactly 16k
                // adding an extra 2 columns, just in case
                for (int i = 1; i < numOfColumns; i++) {
                    sendToSocket(socket, tableName + ",abcdefghijklmnopqrs=x, " + rnd.nextString(13 - (int) Math.log10(i)) + i + "=32 " + i + "\n");
                }
                finished.await();
            } finally {
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                });
            }
        }, false, 1000);

        mayDrainWalQueue();
        try (TableReader reader = getReader(tableName)) {
            Assert.assertEquals(reader.getMetadata().isWalEnabled(), walEnabled);
            Assert.assertEquals(numOfColumns + 1, reader.getMetadata().getColumnCount());
        }
    }

    @Test
    public void testNewPartitionRowCancelledTwice() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampNano(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            send("table", WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    lineTcpSender.disableValidation();
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 1")
                            .tag("tag=2", "значение 2")
                            .field("поле=3", "{\"ключ\": \"число\"}")
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 2")
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag/2", "value=\2") // Invalid column name, last line is not saved
                            .$(Micros.DAY_MICROS * 1000L);
                    // Repeat
                    lineTcpSender
                            .metric("table")
                            .tag("tag/2", "value=\2") // Invalid column name, last line is not saved
                            .$(Micros.DAY_MICROS * 1000L);
                    lineTcpSender.flush();
                }
            });

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("table"));
            }
            String expected = """
                    tag1\ttag=2\tполе=3\ttimestamp
                    value 1\tзначение 2\t{"ключ": "число"}\t1970-01-01T00:00:00.000000Z
                    value 2\t\t\t1970-01-01T00:00:00.000000Z
                    """;
            assertTable(expected, "table");
        });
    }

    @Test
    public void testNoAutoCreateNewColumns() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        autoCreateNewColumns = false;
        runInContext((receiver) -> {
            // First, create a table and insert a few rows into it, so that we get some existing symbol keys.
            TableModel m = new TableModel(configuration, "up", PartitionBy.MONTH);
            m.timestamp("ts").col("sym", ColumnType.SYMBOL);
            if (walEnabled) {
                m.wal();
            }
            createTable(m);

            String lineData =
                    """
                            up out=1.0 631150000000000000
                            up out=2.0 631152000000000000
                            up out=3.0 631160000000000000
                            up out=4.0 631170000000000000
                            """;
            sendLinger(lineData, "up");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("up"));
            }
            String expected = "ts\tsym\n";
            assertTable(expected, "up");
        });
        autoCreateNewColumns = true;
    }

    @Test
    public void testQueueBufferOverflowDoesNotCrashVM() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        msgBufferSize = 64 * 1024;
        runInContext((receiver) -> {
            String lineData = "bybit_perpetual_btcusdt_ob_features,symbol=BTCUSDT,side=Buy,time_iso8601=2021-09-27_12:04:19.100000,trade_id=7d4676b9-08ae-5659-982c-629bd3a99f75,ask_ob=00000000000000000400000002000000fffffffff00500001000000000000a000c000600050008000a000000000104000c00000008000800000004000800000004000000010000000400000080faffff0000010e180000002000000004000000020000007805000028000000040000006c6973740000000014fbffff0000010004000000020000000000000001000000c4faffff0000010c14000000180000000400000001000000100000000200000031310000b8faffffecfaffff0000010d180000002000000004000000020000006804000014000000040000006974656d00000000e8faffff1cfbffff0000010e1c00000024000000040000000300000014040000540000002c0000000400000076616c7300000000b4fbffff00000100040000000300000000000000010000000200000068fbffff0000010510000000140000000400000000000000010000003500000058fbffff8cfbffff0000010c1400000018000000040000000100000010000000020000003131000080fbffffb4fbffff0000010d18000000200000000400000002000000f002000014000000040000006974656d00000000b0fbffffe4fbffff0000010e180000002000000004000000020000009c020000280000000400000076616c730000000078fcffff000001000400000002000000000000000100000028fcffff0000010c140000001800000004000000010000001000000002000000313000001cfcffff50fcffff0000010e180000002000000004000000020000000c02000028000000040000006974656d00000000e4fcffff000001000400000002000000000000000100000094fcffff0000010c1400000018000000040000000100000010000000020000003131000088fcffffbcfcffff0000010d180000002000000004000000020000001401000014000000040000006974656d00000000b8fcffffecfcffff0000010e20000000280000000400000004000000c00000008800000058000000300000000400000076616c730000000088fdffff0000010004000000040000000000000001000000020000000300000040fdffff0000010510000000140000000400000000000000010000003500000030fdffff64fdffff00000102100000001400000004000000000000000200000031360000d4ffffff000000012000000090fdffff00000102100000001c0000000400000000000000020000003135000008000c0008000700080000000000000120000000c4fdffff00000101100000001400000004000000000000000100000030000000b4fdffffe8fdffff0000010e180000002000000004000000020000005000000028000000040000006b657973000000007cfeffff00000100040000000200000000000000010000002cfeffff000001051000000014000000040000000000000001000000350000001cfeffff50feffff0000010110000000140000000400000000000000010000003000000040feffff74feffff0000010110000000140000000400000000000000010000003000000064feffff98feffff0000010110000000140000000400000000000000010000003000000088feffffbcfeffff0000010e180000002000000004000000020000005000000028000000040000006b6579730000000050ffffff000001000400000002000000000000000100000000ffffff00000105100000001400000004000000000000000100000035000000f0feffff24ffffff0000010110000000140000000400000000000000010000003000000014ffffff48ffffff0000010110000000140000000400000000000000010000003000000038ffffff6cffffff0000010e180000002800000004000000020000005800000030000000040000006b6579730000000008000c0006000800080000000000010004000000020000000000000001000000b8ffffff00000105100000001400000004000000000000000100000035000000a8ffffffdcffffff00000101100000001400000004000000000000000100000030000000ccffffff100014000800060007000c00000010001000000000000101100000001800000004000000000000000100000030000000040004000400000000000000ffffffff1805000014000000000000000c0016000600050008000c000c0000000003040018000000e00100000000000000000a0018000c00040008000a000000fc020000100000000100000000000000000000002e000000000000000000000001000000000000000800000000000000040000000000000010000000000000000000000000000000100000000000000008000000000000001800000000000000000000000000000018000000000000000200000000000000200000000000000008000000000000002800000000000000000000000000000028000000000000000c0000000000000038000000000000000c00000000000000480000000000000002000000000000005000000000000000080000000000000058000000000000000000000000000000580000000000000008000000000000006000000000000000000000000000000060000000000000000200000000000000680000000000000008000000000000007000000000000000000000000000000070000000000000000c0000000000000080000000000000000a000000000000009000000000000000020000000000000098000000000000000800000000000000a0000000000000000000000000000000a0000000000000000c00000000000000b0000000000000000400000000000000b8000000000000001000000000000000c8000000000000000000000000000000c8000000000000001400000000000000e0000000000000000000000000000000e0000000000000000800000000000000e800000000000000200000000000000008010000000000000000000000000000080100000000000024000000000000003001000000000000340000000000000068010000000000000800000000000000700100000000000020000000000000009001000000000000000000000000000090010000000000001000000000000000a0010000000000000000000000000000a0010000000000000800000000000000a8010000000000000000000000000000a8010000000000000c00000000000000b8010000000000001000000000000000c8010000000000000000000000000000c8010000000000000800000000000000d0010000000000000c00000000000000000000001e0000000100000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000200000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000002000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000080000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000200000000000000000000000000000002000000000000000000000000000000010000000000000000000000000000000100000000000000000000000000000000000000020000000101000000000000000000000100000000000000040000000c00000000000000646174615f7079747970655f000000000102000000000000000000000000000000000000020000000101000000000000000000000100000000000000060000000a00000000000000626c6f636b736178657300000000000001010000000000000000000001000000000000000200000004000000000000000101010100000000000000000100000002000000030000000000000002000000040000000600000008000000000000000101010101010101000000000100000002000000030000000400000005000000060000000700000000000000090000000e000000170000001c00000020000000280000002c0000003400000000000000706c6163656d656e74626c6f636b706c6163656d656e74626c6f636b646174615f7079747970655f646174615f7079747970655f00000000010101010203020300000000010000000200000003000000000000000000000001000000010000000000000001000000020000000300000000000000010000000000000008000000100000000000000070642e496e64657870642e496e646578000000000c00000070642e446174614672616d6500000000ffffffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffffffffb800000014000000000000000c001a000600050008000c000c000000000404002000000008000000000000000000000000000e002800070008000c00100014000e000000000000026000000028000000180000000000000000000000080000000000000000000000010000000800000000000000010000000c00000008001400080004000800000010000000010000000000000000000000000000000000000008000c0008000700080000000000000140000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000fffffffff800000014000000000000000c001a000600050008000c000c000000000404002000000040060000000000000000000000000e002800070008000c00100014000e000000000000038c0000003400000018000000000000000000000040060000000000000000000002000000080000000000000008000000000000000000000002000000300000000c0000000800100008000400080000000c000000c80000000000000000000000000000000800140008000400080000001000000001000000000000000000000000000000000006000800060006000000000002000000000000000000000000000000000000000000000000000000000000000000fa7e6abc7493883ffca9f1d24d62503f7b14ae47e17aa43f9a9999999999d93f9a9999999999c93fc3f5285c8fc2ed3f3bdf4f8d976e823f79e9263108ac7c3fec51b81e85ebc13ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62603f7b14ae47e17a843ffa7e6abc7493683ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62603f39b4c876be9f9a3ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62603ffca9f1d24d62603f9a9999999999a93f6abc74931804f03ffca9f1d24d62603ffca9f1d24d62503ffca9f1d24d62503ffa7e6abc7493683ffca9f1d24d62603ffa7e6abc7493783f79e9263108acac3f0ad7a3703d0ab73ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62603f48e17a14ae0720405a643bdf4f8da73ffa7e6abc7493883ffca9f1d24d62603f3bdf4f8d976e823ffca9f1d24d62503ffa7e6abc7493683ffca9f1d24d62603ffca9f1d24d62503ffca9f1d24d62603ffca9f1d24d62503ffca9f1d24d62503fb81e85eb51b88e3ffca9f1d24d62503f9a9999999999a93ffca9f1d24d62603ffca9f1d24d62503ffa7e6abc74131140fca9f1d24d62503ffa7e6abc7493683ffca9f1d24d62503f4260e5d022dbb93f1904560e2db2ad3f83c0caa145b61040fa7e6abc7493783ffa7e6abc7493883f79e9263108ac8c3ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62703ffca9f1d24d62503f7b14ae47e17a843ffca9f1d24d62503ffa7e6abc7493683ffca9f1d24d62503f39b4c876be9fba3f9a9999999999a93f39b4c876be9f8a3ffca9f1d24d62503ffca9f1d24d62503fba490c022b87863f62105839b4c80c40fca9f1d24d62503ffca9f1d24d62503ffa7e6abc7493683fee7c3f355ebae13ffca9f1d24d62503ffca9f1d24d62503ffca9f1d24d62503fbe9f1a2fdd24d63f39b4c876be9f9a3f9a9999999999a93fd9cef753e3a5bb3f7b14ae47e17a843f9cc420b07268913ffca9f1d24d62603ffca9f1d24d62503ffa7e6abc7493683f2b8716d9cef7a33f3bdf4f8d976eb23f4260e5d0220b4040fca9f1d24d62503ffca9f1d24d62503ff0a7c64b3789c13ffca9f1d24d62503f9a9999999999993fb81e85eb51b8be3f77be9f1a2fdd11407b14ae47e17ac43f39b4c876be9faa3ffca9f1d24d62503fc3f5285c8fc20d40ba490c022b87963ffca9f1d24d62503ffca9f1d24d62603ffca9f1d24d62503ffa7e6abc7493683f986e1283c0caed3ffca9f1d24d62503ffca9f1d24d62503f8b6ce7fba9f1a23fc976be9f1a2fbd3ffca9f1d24d62503ffa7e6abc7493783ffca9f1d24d62603f4260e5d022dbd13fdd2406819543eb3fb81e85eb51b89e3ffca9f1d24d62503ffa7e6abc7493683ffca9f1d24d62503fdbf97e6abc74e33ffca9f1d24d62503ffca9f1d24d62503f46b6f3fdd4780940fca9f1d24d62503fcdcccccccccc1140fca9f1d24d62703ffca9f1d24d62603ffca9f1d24d62503ffa7e6abc7493683fac1c5a643bdfe33fb81e85eb51b89e3f9a9999999999b93ffa7e6abc7493683f7f6abc749318f23f08ac1c5a643bbf3f295c8fc2f5281440b6f3fdd478e9e23ffca9f1d24d62603ffca9f1d24d62503f9eefa7c64b37d93ffca9f1d24d62503fc74b37894160fd3fa245b6f3fdd4b83fba490c022b87863f5a643bdf4f8d973ffca9f1d24d62503ffca9f1d24d62503ff6285c8fc2f5e83f3bdf4f8d976ef43fba490c022b87963f105839b4c876d63ffa7e6abc7493683fd9cef753e3a5f53f39b4c876be9fd23fba490c022b87863ffca9f1d24d62c03f2db29defa7c6f93f8fc2f5285c8ff03f3bdf4f8d976ef83f54e3a59bc4200b40f853e3a59bc40540e5d022dbf97e13403bdf4f8d976ef83ffca9f1d24d62503fd9cef753e3a5e73f75931804560ed53fec51b81e85ebe93f3d0ad7a3703dd23f79e9263108acac3fb4c876be9f1ad73f6de7fba9f1d2cd3f000000000000e83fb81e85eb51b89e3ff6285c8fc2f5c83ffca9f1d24d62503f7b14ae47e17a943f7b14ae47e17a943fe7fba9f1d24dd23fcdcccccccccce43f08ac1c5a643be33f39b4c876be9fe23f1f85eb51b81edd3f5839b4c876bee33fb81e85eb51b8d63f643bdf4f8d171040333333333333d33ffca9f1d24d62503f000000000000104000000000000010400000000000000840f853e3a59b441140ffffffffb800000014000000000000000c001a000600050008000c000c000000000404002000000008000000000000000000000000000e002800070008000c00100014000e000000000000026000000028000000180000000000000000000000080000000000000000000000010000000800000000000000010000000c00000008001400080004000800000010000000010000000000000000000000000000000000000008000c0008000700080000000000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000fffffffff800000014000000000000000c001a000600050008000c000c000000000404002000000040060000000000000000000000000e002800070008000c00100014000e000000000000038c0000003400000018000000000000000000000040060000000000000000000002000000400600000000000008000000000000000000000002000000300000000c0000000800100008000400080000000c000000c80000000000000000000000000000000800140008000400080000001000000001000000000000000000000000000000000006000800060006000000000002000000000000000000000000000000000000000000000000000000000000000000000000000061e54000000000e060e540000000009060e540000000005060e540000000004060e540000000003060e540000000002060e540000000001060e54000000000805fe54000000000005fe54000000000e05ee54000000000d05ee54000000000a05ee54000000000805ee54000000000005ee54000000000a05de54000000000905de54000000000805de54000000000e05ce54000000000c05ce54000000000805ce54000000000505ce54000000000405ce54000000000205ce54000000000105ce54000000000805be54000000000605be54000000000405be54000000000105be54000000000005be54000000000e05ae54000000000b05ae54000000000a05ae54000000000805ae54000000000205ae54000000000005ae54000000000f059e54000000000e059e54000000000d059e54000000000c059e540000000004059e540000000000059e540000000009058e540000000008058e540000000007058e54000000000e057e54000000000b057e540000000008057e540000000005057e540000000002057e540000000000057e54000000000e056e54000000000d056e54000000000c056e540000000008056e540000000001056e540000000000056e54000000000d055e54000000000b055e540000000009055e540000000005055e540000000004055e540000000002055e54000000000f054e54000000000e054e54000000000d054e54000000000c054e540000000008054e540000000000054e540000000009053e540000000008053e540000000006053e540000000005053e540000000002053e54000000000b052e540000000006052e540000000005052e54000000000c051e540000000008051e540000000002051e540000000001051e54000000000e050e540000000004050e540000000000050e54000000000e04fe54000000000d04fe54000000000c04fe54000000000804fe54000000000604fe54000000000104fe54000000000004fe54000000000c04ee54000000000b04ee54000000000904ee54000000000604ee54000000000204ee54000000000f04de54000000000e04de54000000000c04de54000000000a04de54000000000704de54000000000604de54000000000504de54000000000304de54000000000204de54000000000f04ce54000000000c04ce54000000000a04ce54000000000904ce54000000000804ce54000000000104ce54000000000004ce54000000000f04be54000000000e04be54000000000f04ae54000000000e04ae54000000000d04ae54000000000c04ae54000000000a04ae54000000000804ae54000000000404ae54000000000304ae54000000000104ae54000000000004ae54000000000a049e540000000009049e540000000006049e540000000004049e540000000007048e540000000006048e540000000005048e540000000003048e540000000002048e540000000008047e540000000007047e540000000002047e540000000001047e54000000000f046e54000000000e046e54000000000c046e54000000000a046e540000000006046e540000000005046e540000000004046e540000000001046e540000000000046e54000000000e045e54000000000d045e54000000000a045e540000000008045e540000000004045e540000000001045e540000000000045e54000000000a044e540000000009044e540000000007044e540000000004044e540000000001044e54000000000d043e540000000008043e540000000007043e540000000005043e540000000004043e540000000000043e54000000000f042e54000000000c042e54000000000a042e540000000009042e540000000008042e540000000007042e540000000006042e540000000005042e540000000001042e54000000000f041e54000000000e041e54000000000c041e54000000000b041e540000000008041e540000000004041e540000000003041e540000000002041e540000000000041e54000000000e040e54000000000d040e54000000000c040e54000000000b040e540000000008040e540000000007040e540000000004040e540000000003040e540000000002040e540000000001040e540000000000040e54000000000d03fe54000000000c03fe54000000000903fe54000000000003fe54000000000f03ee54000000000e03ee54000000000d03ee540f200000000000000800595e7000000000000008c1870616e6461732e636f72652e696e64657865732e62617365948c0a5f6e65775f496e64657894939468008c05496e6465789493947d94288c0464617461948c156e756d70792e636f72652e6d756c74696172726179948c0c5f7265636f6e7374727563749493948c056e756d7079948c076e6461727261799493944b0085944301629487945294284b014b028594680a8c0564747970659493948c024f3894898887945294284b038c017c944e4e4e4affffffff4affffffff4b3f749462895d94288c057072696365948c06416d6f756e7494657494628c046e616d65944e75869452942e8d0000000000000080059582000000000000008c1870616e6461732e636f72652e696e64657865732e62617365948c0a5f6e65775f496e6465789493948c1970616e6461732e636f72652e696e64657865732e72616e6765948c0a52616e6765496e6465789493947d94288c046e616d65944e8c057374617274944b008c0473746f70944bc88c0473746570944b0175869452942e,bid_ob=00000000000000000400000002000000fffffffff00500001000000000000a000c000600050008000a000000000104000c00000008000800000004000800000004000000010000000400000080faffff0000010e180000002000000004000000020000007805000028000000040000006c6973740000000014fbffff0000010004000000020000000000000001000000c4faffff0000010c14000000180000000400000001000000100000000200000031310000b8faffffecfaffff0000010d180000002000000004000000020000006804000014000000040000006974656d00000000e8faffff1cfbffff0000010e1c00000024000000040000000300000014040000540000002c0000000400000076616c7300000000b4fbffff00000100040000000300000000000000010000000200000068fbffff0000010510000000140000000400000000000000010000003500000058fbffff8cfbffff0000010c1400000018000000040000000100000010000000020000003131000080fbffffb4fbffff0000010d18000000200000000400000002000000f002000014000000040000006974656d00000000b0fbffffe4fbffff0000010e180000002000000004000000020000009c020000280000000400000076616c730000000078fcffff000001000400000002000000000000000100000028fcffff0000010c140000001800000004000000010000001000000002000000313000001cfcffff50fcffff0000010e180000002000000004000000020000000c02000028000000040000006974656d00000000e4fcffff000001000400000002000000000000000100000094fcffff0000010c1400000018000000040000000100000010000000020000003131000088fcffffbcfcffff0000010d180000002000000004000000020000001401000014000000040000006974656d00000000b8fcffffecfcffff0000010e20000000280000000400000004000000c00000008800000058000000300000000400000076616c730000000088fdffff0000010004000000040000000000000001000000020000000300000040fdffff0000010510000000140000000400000000000000010000003500000030fdffff64fdffff00000102100000001400000004000000000000000200000031360000d4ffffff000000012000000090fdffff00000102100000001c0000000400000000000000020000003135000008000c0008000700080000000000000120000000c4fdffff00000101100000001400000004000000000000000100000030000000b4fdffffe8fdffff0000010e180000002000000004000000020000005000000028000000040000006b657973000000007cfeffff00000100040000000200000000000000010000002cfeffff000001051000000014000000040000000000000001000000350000001cfeffff50feffff0000010110000000140000000400000000000000010000003000000040feffff74feffff0000010110000000140000000400000000000000010000003000000064feffff98feffff0000010110000000140000000400000000000000010000003000000088feffffbcfeffff0000010e180000002000000004000000020000005000000028000000040000006b6579730000000050ffffff000001000400000002000000000000000100000000ffffff00000105100000001400000004000000000000000100000035000000f0feffff24ffffff0000010110000000140000000400000000000000010000003000000014ffffff48ffffff0000010110000000140000000400000000000000010000003000000038ffffff6cffffff0000010e180000002800000004000000020000005800000030000000040000006b6579730000000008000c0006000800080000000000010004000000020000000000000001000000b8ffffff00000105100000001400000004000000000000000100000035000000a8ffffffdcffffff00000101100000001400000004000000000000000100000030000000ccffffff100014000800060007000c00000010001000000000000101100000001800000004000000000000000100000030000000040004000400000000000000ffffffff1805000014000000000000000c0016000600050008000c000c0000000003040018000000e00100000000000000000a0018000c00040008000a000000fc020000100000000100000000000000000000002e000000000000000000000001000000000000000800000000000000040000000000000010000000000000000000000000000000100000000000000008000000000000001800000000000000000000000000000018000000000000000200000000000000200000000000000008000000000000002800000000000000000000000000000028000000000000000c0000000000000038000000000000000c00000000000000480000000000000002000000000000005000000000000000080000000000000058000000000000000000000000000000580000000000000008000000000000006000000000000000000000000000000060000000000000000200000000000000680000000000000008000000000000007000000000000000000000000000000070000000000000000c0000000000000080000000000000000a000000000000009000000000000000020000000000000098000000000000000800000000000000a0000000000000000000000000000000a0000000000000000c00000000000000b0000000000000000400000000000000b8000000000000001000000000000000c8000000000000000000000000000000c8000000000000001400000000000000e0000000000000000000000000000000e0000000000000000800000000000000e800000000000000200000000000000008010000000000000000000000000000080100000000000024000000000000003001000000000000340000000000000068010000000000000800000000000000700100000000000020000000000000009001000000000000000000000000000090010000000000001000000000000000a0010000000000000000000000000000a0010000000000000800000000000000a8010000000000000000000000000000a8010000000000000c00000000000000b8010000000000001000000000000000c8010000000000000000000000000000c8010000000000000800000000000000d0010000000000000c00000000000000000000001e0000000100000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000200000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000002000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000080000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000080000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000200000000000000000000000000000002000000000000000000000000000000010000000000000000000000000000000100000000000000000000000000000000000000020000000101000000000000000000000100000000000000040000000c00000000000000646174615f7079747970655f000000000102000000000000000000000000000000000000020000000101000000000000000000000100000000000000060000000a00000000000000626c6f636b736178657300000000000001010000000000000000000001000000000000000200000004000000000000000101010100000000000000000100000002000000030000000000000002000000040000000600000008000000000000000101010101010101000000000100000002000000030000000400000005000000060000000700000000000000090000000e000000170000001c00000020000000280000002c0000003400000000000000706c6163656d656e74626c6f636b706c6163656d656e74626c6f636b646174615f7079747970655f646174615f7079747970655f00000000010101010203020300000000010000000200000003000000000000000000000001000000010000000000000001000000020000000300000000000000010000000000000008000000100000000000000070642e496e64657870642e496e646578000000000c00000070642e446174614672616d6500000000ffffffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffffffffb800000014000000000000000c001a000600050008000c000c000000000404002000000008000000000000000000000000000e002800070008000c00100014000e000000000000026000000028000000180000000000000000000000080000000000000000000000010000000800000000000000010000000c00000008001400080004000800000010000000010000000000000000000000000000000000000008000c0008000700080000000000000140000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000fffffffff800000014000000000000000c001a000600050008000c000c000000000404002000000040060000000000000000000000000e002800070008000c00100014000e000000000000038c0000003400000018000000000000000000000040060000000000000000000002000000080000000000000008000000000000000000000002000000300000000c0000000800100008000400080000000c000000c80000000000000000000000000000000800140008000400080000001000000001000000000000000000000000000000000006000800060006000000000002000000000000000000000000000000000000000000000000000000000000000000e9263108acfc4e400000000000001040713d0ad7a370cd3f39b4c876be9fba3f7f6abc749318c43f52b81e85eb51d03f7f6abc749318c43f08ac1c5a643bbf3f9cc420b07268b13f9a9999999999c93ff0a7c64b3789f93f068195438b6cfd3fc976be9f1a2fbd3f91ed7c3f351e3040fed478e926310a407d3f355eba490340e9263108ac1cea3fd122dbf97e6aec3f1904560e2db2cd3fa8c64b3789410240508d976e1283d03fd9cef753e3a5d33f7b14ae47e17a843f7b14ae47e17a843f75931804560ee93f39b4c876be9f9a3f9a9999999999d93ffa7e6abc7493a83f6abc74931804c63ffca9f1d24d62803f1904560e2db2f13f79e9263108ac7c3fcdcccccccccce43f986e1283c0caed3f2fdd24068195fd3fe17a14ae47e10740f6285c8fc2f5c83f273108ac1c5a08402b8716d9cef7f13fb81e85eb51b8ae3fe17a14ae47e1ba3f295c8fc2f528bc3ffca9f1d24d62503f736891ed7c3fb53ffed478e92631d03f5a643bdf4f8de33f2b8716d9cef7c33f333333333333b33fee7c3f355ebac93fdf4f8d976e12c33ffca9f1d24d62503fba490c022b87863fc1caa145b6f3bd3ffca9f1d24d62503f1283c0caa145e23fd122dbf97e6abc3f9a9999999999a93ffa7e6abc7493c83fdbf97e6abc74933fdbf97e6abcf410402b8716d9cef7a33f8b6ce7fba9f1d23f52b81e85eb5138404260e5d022dbd13fdf4f8d976e12d33f39b4c876be9f8a3fac1c5a643bdfcf3f7f6abc749318e03ffca9f1d24d62503f91ed7c3f355eba3ffca9f1d24d62503ffca9f1d24d62703fd34d62105839e03fe5d022dbf97eea3fc976be9f1a2fdd3fc520b072689100404260e5d022db014004560e2db21d114079e9263108ac7c3f4c37894160e5a03ffa7e6abc7493983f7b14ae47e17a943f9a9999999999a93ffa7e6abc7493b83f8d976e1283c0ca3fc3f5285c8fc2ed3fe9263108ac1caa3fb29defa7c64bc73f3bdf4f8d976eb23f3bdf4f8d97ee3d40cba145b6f3fda43f9a9999999999a93fb81e85eb51b88e3f4a0c022b8716a93f79e9263108accc3f4c37894160e5d83f4a0c022b8716b93ffca9f1d24d62803f39b4c876be9f9a3ffca9f1d24d62603ffca9f1d24d62703f1904560e2db2dd3f3bdf4f8d976e923ffca9f1d24d62503f7f6abc749318c43f9a9999999999b93ffca9f1d24d62603f508d976e1283f63f1d5a643bdf4fdd3f7b14ae47e17a743f333333333333b33fee7c3f355ebac93fcff753e3a59bc43fc3f5285c8fc2b53f000000000000f03fb81e85eb51b8d63fb29defa7c6cb1940000000000000e03f9a9999999999b93fdd2406819543d33f2db29defa7c60340736891ed7c3fb53ffca9f1d24d62503fe7fba9f1d24dc23f8195438b6ce7bb3ffed478e926f14040f853e3a59bc41b40643bdf4f8d972c407b14ae47e17a943fb81e85eb51b88e3f6abc74931804b63f9a9999999999c93fc3f5285c8fc2e93ffa7e6abc7493683f9eefa7c64b37d13f62105839b4c8c63fc976be9f1a2fcd3fb81e85eb51b89e3fba490c022b87863f3bdf4f8d976e923f39b4c876be9faa3ffca9f1d24d62503f333333333333c33f7b14ae47e17a743f333333333333d33f2fdd24068195d33f4a0c022b8716fb3f7d3f355eba49e83fba490c022b87863f931804560e2de23f5a643bdf4f8d973f333333333333d33f3bdf4f8d976e823ffca9f1d24d62903f7b14ae47e17a843f1b2fdd240681b53f39b4c876be9f9a3f9a9999999999a93f894160e5d022ab3f1b2fdd240681084079e9263108ac9c3ffa7e6abc7493783f79e9263108ac8c3f60e5d022dbf9ce3fe3a59bc420b0b23ffca9f1d24d62a03f7b14ae47e17a743f54e3a59bc420b03ffca9f1d24d62803ffca9f1d24d62703f54e3a59bc420d03f75931804560ecd3fec51b81e85ebc13ffa7e6abc74931d406abc74931804f23ffca9f1d24d62503f7b14ae47e17a843ffca9f1d24d62503f3bdf4f8d976ea23f21b0726891edbc3f8195438b6ce7d33ffca9f1d24d62503f333333333333b33fdf4f8d976e921640fca9f1d24d62503f4c37894160e5a03f7b14ae47e17a843f333333333333d33f7b14ae47e17a843f9cc420b07268a13f39b4c876be9faa3ffa7e6abc7493783f9cc420b07268b13f48e17a14ae47c13f08ac1c5a643bd73ffa7e6abc7493783f7d3f355eba49cc3f6abc74931804b63f7b14ae47e17a843fa69bc420b072c83fffffffffb800000014000000000000000c001a000600050008000c000c000000000404002000000008000000000000000000000000000e002800070008000c00100014000e000000000000026000000028000000180000000000000000000000080000000000000000000000010000000800000000000000010000000c00000008001400080004000800000010000000010000000000000000000000000000000000000008000c0008000700080000000000000140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000fffffffff800000014000000000000000c001a000600050008000c000c000000000404002000000040060000000000000000000000000e002800070008000c00100014000e000000000000038c0000003400000018000000000000000000000040060000000000000000000002000000400600000000000008000000000000000000000002000000300000000c0000000800100008000400080000000c000000c8000000000000000000000000000000080014000800040008000000100000000100000000000000000000000000000000000600080006000600000000000200000000000000000000000000000000000000000000000000000000000000000000000000c03ee54000000000b03ee54000000000203ee54000000000f03de54000000000e03de54000000000a03de54000000000603de54000000000403de54000000000303de54000000000103de54000000000f03ce54000000000a03ce54000000000903ce54000000000603be54000000000503be54000000000403be54000000000303be54000000000203be54000000000103be54000000000003be54000000000f03ae54000000000e03ae54000000000d03ae54000000000b03ae54000000000903ae54000000000703ae54000000000603ae54000000000403ae54000000000303ae54000000000203ae54000000000103ae54000000000003ae54000000000f039e54000000000e039e54000000000d039e54000000000c039e54000000000b039e54000000000a039e540000000009039e540000000007039e540000000005039e540000000004039e540000000002039e540000000000039e54000000000e038e54000000000d038e54000000000c038e54000000000a038e540000000008038e540000000007038e540000000006038e540000000004038e540000000003038e540000000001038e540000000000038e54000000000e037e54000000000c037e54000000000a037e540000000008037e540000000007037e540000000006037e540000000005037e540000000004037e540000000003037e54000000000e036e54000000000c036e54000000000b036e54000000000a036e540000000009036e540000000008036e540000000004036e540000000003036e540000000002036e540000000001036e540000000000036e54000000000d035e54000000000b035e54000000000a035e540000000009035e540000000008035e540000000006035e540000000005035e540000000004035e540000000003035e540000000002035e540000000001035e540000000000035e54000000000f034e54000000000e034e54000000000d034e54000000000c034e54000000000b034e54000000000a034e540000000008034e540000000004034e540000000003034e540000000002034e540000000000034e54000000000f033e54000000000e033e54000000000a033e540000000008033e540000000007033e540000000006033e540000000004033e540000000002033e540000000000033e54000000000e032e54000000000a032e540000000009032e540000000007032e540000000006032e540000000004032e540000000002032e540000000001032e540000000000032e54000000000f031e54000000000e031e54000000000d031e54000000000c031e54000000000b031e54000000000a031e540000000006031e540000000005031e540000000004031e540000000002031e540000000001031e540000000000031e54000000000d030e54000000000a030e540000000008030e540000000005030e540000000004030e540000000000030e54000000000f02fe54000000000e02fe54000000000c02fe54000000000b02fe54000000000a02fe54000000000702fe54000000000602fe54000000000402fe54000000000202fe54000000000e02ee54000000000c02ee54000000000a02ee54000000000802ee54000000000602ee54000000000402ee54000000000302ee54000000000202ee54000000000002ee54000000000f02de54000000000e02de54000000000c02de54000000000a02de54000000000902de54000000000802de54000000000502de54000000000402de54000000000302de54000000000102de54000000000002de54000000000f02ce54000000000a02ce54000000000702ce54000000000602ce54000000000502ce54000000000402ce54000000000202ce54000000000002ce54000000000e02be54000000000b02be54000000000802be54000000000602be54000000000502be54000000000402be54000000000302be54000000000202be54000000000102be54000000000002be54000000000e02ae54000000000d02ae54000000000c02ae54000000000b02ae54000000000802ae54000000000702ae54000000000402ae54000000000302ae54000000000202ae54000000000102ae54000000000002ae54000000000f029e54000000000e029e54000000000d029e54000000000c029e54000000000a029e540000000008029e540000000006029e540000000002029e540f200000000000000800595e7000000000000008c1870616e6461732e636f72652e696e64657865732e62617365948c0a5f6e65775f496e64657894939468008c05496e6465789493947d94288c0464617461948c156e756d70792e636f72652e6d756c74696172726179948c0c5f7265636f6e7374727563749493948c056e756d7079948c076e6461727261799493944b0085944301629487945294284b014b028594680a8c0564747970659493948c024f3894898887945294284b038c017c944e4e4e4affffffff4affffffff4b3f749462895d94288c057072696365948c06416d6f756e7494657494628c046e616d65944e75869452942e8d0000000000000080059582000000000000008c1870616e6461732e636f72652e696e64657865732e62617365948c0a5f6e65775f496e6465789493948c1970616e6461732e636f72652e696e64657865732e72616e6765948c0a52616e6765496e6465789493947d94288c046e616d65944e8c057374617274944b008c0473746f70944bc88c0473746570944b0175869452942e amount=0.2,time_unix=1632744259.1,price=43510.5,quote_asset_amount=8702.1,cum_ob_imbalance_lev_1=13.355802640722723,cum_ob_imbalance_lev_2=13.689135974056057,cum_ob_imbalance_lev_3=12.746635974056057,cum_ob_imbalance_lev_4=11.772635974056056,cum_ob_imbalance_lev_5=167.77263597405604,cum_ob_imbalance_lev_6=167.62263597405604,cum_ob_imbalance_lev_7=166.66166157684003,cum_ob_imbalance_lev_8=166.00532354867102,cum_ob_imbalance_lev_9=165.1155342455916,cum_ob_imbalance_lev_10=164.55509468515203,cum_ob_imbalance_lev_15=385.58929086480646,cum_ob_imbalance_lev_20=1332.4691370186524,ob_imbalance_lev_1=14.355802640722723,ob_imbalance_lev_2=1.3333333333333333,ob_imbalance_lev_3=0.0575,ob_imbalance_lev_4=0.026,ob_imbalance_lev_5=157.0,ob_imbalance_lev_6=0.8500000000000001,ob_imbalance_lev_7=0.03902560278399205,ob_imbalance_lev_8=0.3436619718309859,ob_imbalance_lev_9=0.11021069692058348,ob_imbalance_lev_10=0.43956043956043955,ob_imbalance_lev_15=163.7,ob_imbalance_lev_20=3.042666666666667,ob_usd_imbalance_lev_1=14.355637671317169,ob_usd_imbalance_lev_2=1.3332873679452706,ob_usd_imbalance_lev_3=0.0574914103168128,ob_usd_imbalance_lev_4=0.02599492094134951,ob_usd_imbalance_lev_5=156.95129433663095,ob_usd_imbalance_lev_6=0.8496679534905098,ob_usd_imbalance_lev_7=0.039008115985175736,ob_usd_imbalance_lev_8=0.3434882456503729,ob_usd_imbalance_lev_9=0.11015245204680738,ob_usd_imbalance_lev_10=0.43931299049517847,ob_usd_imbalance_lev_15=163.53827641267762,ob_usd_imbalance_lev_20=3.0392415784113274,ob_imbalance_ratio_lev_1=0.93487803774268,ob_imbalance_ratio_lev_2=0.5714285714285714,ob_imbalance_ratio_lev_3=0.05437352245862884,ob_imbalance_ratio_lev_4=0.025341130604288498,ob_imbalance_ratio_lev_5=0.9936708860759493,ob_imbalance_ratio_lev_6=0.45945945945945954,ob_imbalance_ratio_lev_7=0.037559808612440196,ob_imbalance_ratio_lev_8=0.2557651991614256,ob_imbalance_ratio_lev_9=0.09927007299270073,ob_imbalance_ratio_lev_10=0.3053435114503817,ob_imbalance_ratio_lev_15=0.9939283545840922,ob_imbalance_ratio_lev_20=0.7526385224274407,cum_ob_imbalance_ratio_lev_1=0.93487803774268,cum_ob_imbalance_ratio_lev_2=1.5063066091712514,cum_ob_imbalance_ratio_lev_3=1.5606801316298802,cum_ob_imbalance_ratio_lev_4=1.5860212622341687,cum_ob_imbalance_ratio_lev_5=2.579692148310118,cum_ob_imbalance_ratio_lev_6=3.0391516077695777,cum_ob_imbalance_ratio_lev_7=3.0767114163820177,cum_ob_imbalance_ratio_lev_8=3.3324766155434435,cum_ob_imbalance_ratio_lev_9=3.431746688536144,cum_ob_imbalance_ratio_lev_10=3.737090199986526,cum_ob_imbalance_ratio_lev_15=7.349269335985824,cum_ob_imbalance_ratio_lev_20=11.797897582698646\n";
            send(lineData, "bybit_perpetual_btcusdt_ob_features", WAIT_ILP_TABLE_RELEASE);
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        Assume.assumeTrue(walEnabled && ColumnType.isTimestampNano(timestampType.getTimestampType()));

        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 2);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 2);
        String weather = "weather";
        String meteorology = "meteorology";
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private final AtomicInteger count = new AtomicInteger(1);

            @Override
            public long openRWNoCache(LPSZ name, int opts) {
                if (
                        Utf8s.endsWithAscii(name, Files.SEPARATOR + "wal1" + Files.SEPARATOR + "1.lock")
                                && count.decrementAndGet() == 0
                ) {
                    mayDrainWalQueue();
                    renameTable(weather, meteorology);
                }
                return super.openRWNoCache(name, opts);
            }
        };

        runInContext(filesFacade, (receiver) -> {
            final String lineData = weather + ",location=west1 temperature=10 1465839830100400200\n" +
                    weather + ",location=west2 temperature=20 1465839830100500200\n" +
                    weather + ",location=east3 temperature=30 1465839830100600200\n" +
                    weather + ",location=west4,source=sensor1 temp=40 1465839830100700200\n" + // <- this is where the split should happen
                    weather + ",location=east5,source=sensor2 temp=50 1465839830100800200\n" +
                    weather + ",location=west6,source=sensor3 temp=60 1465839830100900200\n" +
                    weather + ",location=north,source=sensor4 temp=70 1465839830101000200\n" +
                    meteorology + ",location=south temperature=80 1465839830101000200\n";

            sendWaitWalReleaseCount(lineData, 4);

            mayDrainWalQueue();

            assertEventually(() -> {
                String expected = """
                        location\ttemperature\ttimestamp\tsource\ttemp
                        west1\t10.0\t2016-06-13T17:43:50.100400Z\t\tnull
                        west2\t20.0\t2016-06-13T17:43:50.100500Z\t\tnull
                        east3\t30.0\t2016-06-13T17:43:50.100600Z\t\tnull
                        west4\tnull\t2016-06-13T17:43:50.100700Z\tsensor1\t40.0
                        south\t80.0\t2016-06-13T17:43:50.101000Z\t\tnull
                        """;
                assertTable(expected, meteorology);

                expected = """
                        location\tsource\ttemp\ttimestamp
                        east5\tsensor2\t50.0\t2016-06-13T17:43:50.100800Z
                        west6\tsensor3\t60.0\t2016-06-13T17:43:50.100900Z
                        north\tsensor4\t70.0\t2016-06-13T17:43:50.101000Z
                        """;
                assertTable(expected, weather);
            });

        }, false, 250);
    }

    @Test
    public void testRenameTableSameMeta() throws Exception {
        Assume.assumeTrue(walEnabled && ColumnType.isTimestampNano(timestampType.getTimestampType()));

        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 2);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 2);
        String weather = "weather";
        String meteorology = "meteorology";
        FilesFacade filesFacade = new TestFilesFacadeImpl() {
            private int count = 1;

            @Override
            public long openRWNoCache(LPSZ name, int opts) {
                if (
                        Utf8s.endsWithAscii(name, Files.SEPARATOR + "wal1" + Files.SEPARATOR + "1.lock")
                                && Utf8s.containsAscii(name, weather)
                                && --count == 0
                ) {
                    renameTable(weather, meteorology);
                }
                return super.openRWNoCache(name, opts);
            }
        };

        runInContext(filesFacade, (receiver) -> {
            String lineData = weather + ",location=west1 temperature=10 1465839830100400200\n" +
                    weather + ",location=west2 temperature=20 1465839830100500200\n" +
                    weather + ",location=east3 temperature=30 1465839830100600200\n" +
                    weather + ",location=west4 temperature=40 1465839830100700200\n" +
                    weather + ",location=west5 temperature=50 1465839830100800200\n" +
                    weather + ",location=east6 temperature=60 1465839830100900200\n" +
                    weather + ",location=south7 temperature=70 1465839830101000200\n" +
                    meteorology + ",location=south8 temperature=80 1465839830101000200\n";

            sendWaitWalReleaseCount(lineData, 3);

            // two of the three commits go to the renamed table
            final String expected = """
                    location\ttemperature\ttimestamp
                    west1\t10.0\t2016-06-13T17:43:50.100400Z
                    west2\t20.0\t2016-06-13T17:43:50.100500Z
                    east3\t30.0\t2016-06-13T17:43:50.100600Z
                    west4\t40.0\t2016-06-13T17:43:50.100700Z
                    south8\t80.0\t2016-06-13T17:43:50.101000Z
                    """;

            assertEventually(
                    () -> {
                        drainWalQueue();
                        assertTable(expected, meteorology);
                    },
                    15
            );

            // last commit goes to the recreated table
            final String expected2 = """
                    location\ttemperature\ttimestamp
                    west5\t50.0\t2016-06-13T17:43:50.100800Z
                    east6\t60.0\t2016-06-13T17:43:50.100900Z
                    south7\t70.0\t2016-06-13T17:43:50.101000Z
                    """;

            assertEventually(
                    () -> {
                        drainWalQueue();
                        assertTable(expected2, weather);
                    },
                    15
            );

        }, false, 250);
    }

    @Test
    public void testShutdownWithDedicatedPoolsCloseIoPoolFirst() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        long preTestErrors = engine.getMetrics().healthMetrics().unhandledErrorsCount();

        assertMemoryLeak(() -> {
            WorkerPool writerPool = new TestWorkerPool("writer", 2, engine.getMetrics());
            WorkerPool ioPool = new TestWorkerPool("io", 2, engine.getMetrics());
            shutdownReceiverWhileSenderIsSendingData(ioPool, writerPool);

            Assert.assertEquals(0, engine.getMetrics().healthMetrics().unhandledErrorsCount() - preTestErrors);
        });
    }

    @Test
    public void testShutdownWithDedicatedPoolsCloseWriterPoolFirst() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        long preTestErrors = engine.getMetrics().healthMetrics().unhandledErrorsCount();

        assertMemoryLeak(() -> {
            WorkerPool writerPool = new TestWorkerPool("writer", 2, engine.getMetrics());
            WorkerPool ioPool = new TestWorkerPool("io", 2, engine.getMetrics());
            shutdownReceiverWhileSenderIsSendingData(ioPool, writerPool);

            Assert.assertEquals(0, engine.getMetrics().healthMetrics().unhandledErrorsCount() - preTestErrors);
        });
    }

    @Test
    public void testShutdownWithSharedPool() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        long preTestErrors = engine.getMetrics().healthMetrics().unhandledErrorsCount();

        assertMemoryLeak(() -> {
            WorkerPool sharedPool = new TestWorkerPool("shared", 2, engine.getMetrics());
            shutdownReceiverWhileSenderIsSendingData(sharedPool, sharedPool);

            Assert.assertEquals(0, engine.getMetrics().healthMetrics().unhandledErrorsCount() - preTestErrors);
        });
    }

    @Test
    public void testSomeWritersReleased() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            String lineData = """
                    weather,location=us-midwest temperature=85 1465839830102300200
                    weather,location=us-eastcoast temperature=89 1465839830102400200
                    weather,location=us-westcost temperature=82 1465839830102500200
                    """;

            int iterations = 8;
            int threadCount = 8;
            CharSequenceObjHashMap<SOUnboundedCountDownLatch> tableIndex = new CharSequenceObjHashMap<>();
            tableIndex.put("weather", new SOUnboundedCountDownLatch());
            for (int i = 1; i < threadCount; i++) {
                tableIndex.put("weather" + i, new SOUnboundedCountDownLatch());
            }

            // One engine hook for all writers
            engine.setPoolListener((factoryType, thread, token, event, segment, position) -> {
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    tableIndex.get(token.getTableName()).countDown();
                }
            });

            try {
                sendAndWait(lineData, tableIndex, 1);
                SOCountDownLatch threadPushFinished = new SOCountDownLatch(threadCount - 1);
                for (int i = 1; i < threadCount; i++) {
                    final String threadTable = "weather" + i;
                    final String lineDataThread = lineData.replace("weather", threadTable);
                    sendNoWait(lineDataThread, threadTable);
                    new Thread(() -> {
                        try {
                            for (int n = 0; n < iterations; n++) {
                                Os.sleep(minIdleMsBeforeWriterRelease - 50);
                                send(lineDataThread, threadTable, WAIT_NO_WAIT);
                            }
                        } catch (Exception e) {
                            e.printStackTrace(System.out);
                        } finally {
                            threadPushFinished.countDown();
                        }
                    }).start();
                }

                sendAndWait(lineData, tableIndex, 2);
                mayDrainWalQueue();
                try (TableWriterAPI w = getTableWriterAPI("weather")) {
                    w.truncateSoft();
                }
                // drainWalQueue() call opens TableWriter one more time
                int expectedReleases = walEnabled ? 5 : 4;
                sendAndWait(lineData, tableIndex, expectedReleases);
                mayDrainWalQueue();

                String header = "location\ttemperature\ttimestamp\n";
                String[] lines = {"us-midwest\t85.0\t2016-06-13T17:43:50.102300Z\n",
                        "us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z\n",
                        "us-westcost\t82.0\t2016-06-13T17:43:50.102500Z\n"};
                String expected = header + lines[0] + lines[1] + lines[2];
                assertTable(expected, "weather");
                if (walEnabled) {
                    Assert.assertTrue(isWalTable("weather"));
                }

                // Concatenate iterations + 1 of identical insert results
                // to assert against weather 1-8 tables
                StringBuilder expectedSB = new StringBuilder(header);
                for (String line : lines) {
                    expectedSB.append(Chars.repeat(line, iterations + 1));
                }

                // Wait async ILP send threads to finish.
                threadPushFinished.await();
                for (int i = 1; i < threadCount; i++) {
                    // Wait writer to be released and check.
                    String tableName = "weather" + i;
                    try {
                        try {
                            assertTable(expectedSB, tableName);
                            if (walEnabled) {
                                Assert.assertTrue(isWalTable(tableName));
                            }
                        } catch (AssertionError e) {
                            int releasedCount = -tableIndex.get(tableName).getCount();
                            // Wait one more writer release before re-trying to compare
                            wait(tableIndex.get(tableName), releasedCount + 3, minIdleMsBeforeWriterRelease);
                            mayDrainWalQueue();
                            assertTable(expectedSB, tableName);
                            if (walEnabled) {
                                Assert.assertTrue(isWalTable(tableName));
                            }
                        }
                    } catch (Throwable err) {
                        LOG.error().$("Error '").$(err.getMessage()).$("' comparing table: ").$safe(tableName).$();
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

    @Test
    public void testStringsWithTcpSenderWithNewLineChars() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            send("table", WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 1")
                            .tag("tag=2", "значение 2")
                            .field("поле=3", "{\"ключ\": \n \"число\", \r\n \"key2\": \"value2\"}\n")
                            .$(0);
                    lineTcpSender.flush();
                }
            });

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("table"));
            }
            String expected = """
                    tag1\ttag=2\tполе=3\ttimestamp
                    value 1\tзначение 2\t{"ключ":\s
                     "число", \r
                     "key2": "value2"}
                    \t1970-01-01T00:00:00.000000Z
                    """;
            assertTable(expected, "table");
        });
    }

    @Test
    public void testSymbolAddedInO3Mode() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxMeasurementSize = 4096;
        runInContext((receiver) -> {
            String lineData = """
                    plug,room=6A watts="3195" 1631817296977
                    plug,room=6A watts="3188" 1631817599910
                    plug,room=6A watts="3180" 1631817902842
                    plug,room=6A watts="469" 1631817902842
                    plug,label=Power,room=6A watts="475" 1631817478737
                    """;
            sendLinger(lineData, "plug");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("plug"));
            }
            String expected = """
                    room\twatts\ttimestamp\tlabel
                    6A\t3195\t1970-01-01T00:27:11.817296Z\t
                    6A\t475\t1970-01-01T00:27:11.817478Z\tPower
                    6A\t3188\t1970-01-01T00:27:11.817599Z\t
                    6A\t3180\t1970-01-01T00:27:11.817902Z\t
                    6A\t469\t1970-01-01T00:27:11.817902Z\t
                    """;
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testSymbolAddedInO3ModeFirstRow() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxMeasurementSize = 4096;
        runInContext((receiver) -> {
            String lineData = """
                    plug,room=6A watts="1" 2631819999000
                    plug,label=Power,room=6B watts="22" 1631817902842
                    """;
            sendLinger(lineData, "plug");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("plug"));
            }
            String expected = """
                    room\twatts\ttimestamp\tlabel
                    6B\t22\t1970-01-01T00:27:11.817902Z\tPower
                    6A\t1\t1970-01-01T00:43:51.819999Z\t
                    """;
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testSymbolAddedInO3ModeFirstRow2Lines() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxMeasurementSize = 4096;
        runInContext((receiver) -> {
            String lineData = """
                    plug,room=6A watts="1" 2631819999000
                    plug,label=Power,room=6B watts="22" 1631817902842
                    plug,label=Line,room=6C watts="333" 1531817902842
                    """;
            sendLinger(lineData, "plug");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("plug"));
            }
            String expected = """
                    room\twatts\ttimestamp\tlabel
                    6C\t333\t1970-01-01T00:25:31.817902Z\tLine
                    6B\t22\t1970-01-01T00:27:11.817902Z\tPower
                    6A\t1\t1970-01-01T00:43:51.819999Z\t
                    """;
            assertTable(expected, "plug");
        });
    }

    @Test
    public void testTableTableIdChangedOnRecreate() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute(
                    "create table weather as (" +
                            "select x as windspeed," +
                            "x*2 as timetocycle, " +
                            "cast(x as timestamp) as ts " +
                            "from long_sequence(2)) timestamp(ts)"
            );

            try (RecordCursorFactory cursorFactory = select("weather")) {
                try (RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)) {
                    println(cursorFactory, cursor);
                    TestUtils.assertEquals("""
                            windspeed\ttimetocycle\tts
                            1\t2\t1970-01-01T00:00:00.000001Z
                            2\t4\t1970-01-01T00:00:00.000002Z
                            """, sink);
                }

                execute("drop table weather");

                runInContext((receiver) -> {
                    String lineData =
                            """
                                    weather windspeed=1.0 631150000000000000
                                    weather windspeed=2.0 631152000000000000
                                    weather timetocycle=0.0,windspeed=3.0 631160000000000000
                                    weather windspeed=4.0 631170000000000000
                                    """;
                    sendLinger(lineData, "weather");
                });
                mayDrainWalQueue();
                if (walEnabled) {
                    Assert.assertTrue(isWalTable("weather"));
                }

                try (RecordCursor cursor = cursorFactory.getCursor(sqlExecutionContext)) {
                    println(cursorFactory, cursor);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }
        });
    }

    @Test
    public void testTcpIPv4() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table test (" +
                    "col ipv4, " +
                    "ts timestamp " +
                    ") timestamp(ts) partition by day");

            engine.releaseInactive();
            runInContext((receiver) -> {
                String lineData =
                        """
                                test col="1.1.1.1" 631150000000000000
                                test col="1.1.1.1" 31152000000000000
                                test col="1.1.1.1" 631160000000000000
                                test col="1.1.1.1" 631170000000000000
                                """;
                sendLinger(lineData, "test");
            });
            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("test"));
            }

            String expected = """
                    col\tts
                    1.1.1.1\t1970-12-27T13:20:00.000000Z
                    1.1.1.1\t1989-12-31T23:26:40.000000Z
                    1.1.1.1\t1990-01-01T02:13:20.000000Z
                    1.1.1.1\t1990-01-01T05:00:00.000000Z
                    """;
            assertTable(expected, "test");
        });
    }

    @Test
    public void testTcpIPv4Duplicate() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table test (" +
                    "col ipv4, " +
                    "ts timestamp " +
                    ") timestamp(ts) partition by day");

            engine.releaseInactive();
            runInContext((receiver) -> {
                String lineData =
                        """
                                test col="12.35.40.11",col="23.44.87.56" 631150000000000000
                                test col="23.45.09.12",col="32.11.35.67" 31152000000000000
                                test col="255.255.255.255",col="80.45.86.21" 631160000000000000
                                test col="34.54.23.89",col="22.54.68.90" 631170000000000000
                                """;
                sendLinger(lineData, "test");
            });
            mayDrainWalQueue();

            String expected = """
                    col\tts
                    23.45.9.12\t1970-12-27T13:20:00.000000Z
                    12.35.40.11\t1989-12-31T23:26:40.000000Z
                    255.255.255.255\t1990-01-01T02:13:20.000000Z
                    34.54.23.89\t1990-01-01T05:00:00.000000Z
                    """;
            assertTable(expected, "test");
        });
    }

    @Test
    public void testTcpIPv4MultiCol() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table test (" +
                    "col ipv4, " +
                    "coll ipv4, " +
                    "ts timestamp " +
                    ") timestamp(ts) partition by day");

            engine.releaseInactive();
            runInContext((receiver) -> {
                String lineData =
                        """
                                test col="12.35.40.11",coll="23.44.87.56" 631150000000000000
                                test col="23.45.09.12",coll="32.11.35.67" 31152000000000000
                                test col="255.255.255.255",coll="80.45.86.21" 631160000000000000
                                test col="34.54.23.89",coll="22.54.68.90" 631170000000000000
                                """;
                sendLinger(lineData, "test");
            });
            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("test"));
            }

            String expected = """
                    col\tcoll\tts
                    23.45.9.12\t32.11.35.67\t1970-12-27T13:20:00.000000Z
                    12.35.40.11\t23.44.87.56\t1989-12-31T23:26:40.000000Z
                    255.255.255.255\t80.45.86.21\t1990-01-01T02:13:20.000000Z
                    34.54.23.89\t22.54.68.90\t1990-01-01T05:00:00.000000Z
                    """;
            assertTable(expected, "test");
        });
    }

    @Test
    public void testTcpIPv4NoMagicNumbers() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table test (" +
                    "col ipv4, " +
                    "ts timestamp " +
                    ") timestamp(ts) partition by day");

            engine.releaseInactive();

            // Check that -1 and -2 are not magic numbers in ILP parsing
            runInContext((receiver) -> {
                String lineData =
                        """
                                test col="255.255.255.254" 631150000000000000
                                test col="255.255.255.255" 631150000000000000
                                """;
                sendLinger(lineData, "test");
            });
            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("test"));
            }

            String expected = """
                    col\tts
                    255.255.255.254\t1989-12-31T23:26:40.000000Z
                    255.255.255.255\t1989-12-31T23:26:40.000000Z
                    """;
            assertTable(expected, "test");
        });
    }

    @Test
    public void testTcpIPv4Null() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table test (" +
                    "col ipv4, " +
                    "ts timestamp " +
                    ") timestamp(ts) partition by day");

            engine.releaseInactive();
            runInContext((receiver) -> {
                String lineData =
                        """
                                test col="0.0.0.0" 631150000000000000
                                test col="0.0.0.0" 31152000000000000
                                test col="0.0.0.0" 631160000000000000
                                test col="0.0.0.0" 631170000000000000
                                """;
                sendLinger(lineData, "test");
            });
            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("test"));
            }

            String expected = """
                    col\tts
                    \t1970-12-27T13:20:00.000000Z
                    \t1989-12-31T23:26:40.000000Z
                    \t1990-01-01T02:13:20.000000Z
                    \t1990-01-01T05:00:00.000000Z
                    """;
            assertTable(expected, "test");
        });
    }

    @Test
    public void testTcpIPv4Null2() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        assertMemoryLeak(() -> {
            execute("create table test (" +
                    "col ipv4, " +
                    "ts timestamp " +
                    ") timestamp(ts) partition by day");

            engine.releaseInactive();
            runInContext((receiver) -> {
                String lineData =
                        """
                                test col="" 631150000000000000
                                test col="" 31152000000000000
                                test col="" 631160000000000000
                                test col="" 631170000000000000
                                """;
                sendLinger(lineData, "test");
            });
            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("test"));
            }

            String expected = """
                    col\tts
                    \t1970-12-27T13:20:00.000000Z
                    \t1989-12-31T23:26:40.000000Z
                    \t1990-01-01T02:13:20.000000Z
                    \t1990-01-01T05:00:00.000000Z
                    """;
            assertTable(expected, "test");
        });
    }

    @Test
    public void testTcpSenderManyLinesToForceBufferFlush() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        int rowCount = 100;
        maxMeasurementSize = 100;
        runInContext((receiver) -> {
            String tableName = "table";
            send(tableName, WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, 64)) {
                    for (int i = 0; i < rowCount; i++) {
                        lineTcpSender
                                .metric(tableName)
                                .tag("tag1", "value 1")
                                .field("tag2", Chars.repeat("value 2", 10))
                                .$(0);
                    }
                    lineTcpSender.flush();
                }
            });

            mayDrainWalQueue();
            try (TableReader reader = getReader(tableName)) {
                Assert.assertEquals(reader.getMetadata().isWalEnabled(), walEnabled);
                Assert.assertEquals(rowCount, reader.size());
            }
        });
    }

    @Test
    public void testTcpSenderWithSpaceInTableName() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            String tableName = "ta ble";
            send(tableName, WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    lineTcpSender
                            .metric(tableName)
                            .tag("tag1", "value 1")
                            .field("tag2", "value 2")
                            .$(0);
                    lineTcpSender.flush();
                }
            });

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable(tableName));
            }
            String expected = """
                    tag1\ttag2\ttimestamp
                    value 1\tvalue 2\t1970-01-01T00:00:00.000000Z
                    """;
            assertTable(expected, tableName);
        });
    }

    @Test
    public void testTimestampColumn() throws Exception {
        String table = "Timestamp";
        runInContext((receiver) -> {
            String lineData = table + ",location=us-midwest timestamp=1465839830100400t,temperature=82\n" +
                    table + ",location=us-midwest timestamp=1465839830100500t,temperature=83\n" +
                    table + ",location=us-eastcoast timestamp=1465839830101600t,temperature=81\n" +
                    table + ",location=us-midwest timestamp=1465839830102300t,temperature=85\n" +
                    table + ",location=us-eastcoast timestamp=1465839830102400t,temperature=89\n" +
                    table + ",location=us-eastcoast timestamp=1465839830102400t,temperature=80\n" +
                    table + ",location=us-westcost timestamp=1465839830102500t,temperature=82\n";

            sendLinger(lineData, table);

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable(table));
            }

            String expected = timestampType.getTimestampType() == ColumnType.TIMESTAMP_NANO
                    ? """
                    location\ttimestamp\ttemperature
                    us-midwest\t2016-06-13T17:43:50.100400000Z\t82.0
                    us-midwest\t2016-06-13T17:43:50.100500000Z\t83.0
                    us-eastcoast\t2016-06-13T17:43:50.101600000Z\t81.0
                    us-midwest\t2016-06-13T17:43:50.102300000Z\t85.0
                    us-eastcoast\t2016-06-13T17:43:50.102400000Z\t89.0
                    us-eastcoast\t2016-06-13T17:43:50.102400000Z\t80.0
                    us-westcost\t2016-06-13T17:43:50.102500000Z\t82.0
                    """
                    : """
                    location\ttimestamp\ttemperature
                    us-midwest\t2016-06-13T17:43:50.100400Z\t82.0
                    us-midwest\t2016-06-13T17:43:50.100500Z\t83.0
                    us-eastcoast\t2016-06-13T17:43:50.101600Z\t81.0
                    us-midwest\t2016-06-13T17:43:50.102300Z\t85.0
                    us-eastcoast\t2016-06-13T17:43:50.102400Z\t89.0
                    us-eastcoast\t2016-06-13T17:43:50.102400Z\t80.0
                    us-westcost\t2016-06-13T17:43:50.102500Z\t82.0
                    """;
            assertTable(expected, table);
        });
    }

    @Test
    // flapping test
    public void testUnauthenticated() throws Exception {
        test(null, null, 200, 1_000, false);
    }

    @Test
    public void testUnicodeTableName() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        byte[] utf8Bytes = "ल".getBytes(Files.UTF_8);
        Assert.assertEquals(3, utf8Bytes.length);

        TableModel m = new TableModel(configuration, "लаблअца", PartitionBy.DAY);
        m.col("символ", ColumnType.SYMBOL).indexed(true, 256)
                .col("поле", ColumnType.STRING)
                .timestamp("время");
        if (walEnabled) {
            m.wal();
        }
        TestUtils.createTable(engine, m);

        engine.releaseInactive();
        runInContext((receiver) -> {
            String lineData = "लаблअца поле=\"значение\" 1619509249714000000\n";
            sendLinger(lineData, "लаблअца");

            String lineData2 = "लаблअца,символ=значение2 поле=\"значение3\" 1619509249714000000\n";
            sendLinger(lineData2, "लаблअца");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("लаблअца"));
            }
            assertTable("""
                    символ\tполе\tвремя
                    \tзначение\t2021-04-27T07:40:49.714000Z
                    значение2\tзначение3\t2021-04-27T07:40:49.714000Z
                    """, "लаблअца");
        });
    }

    @Test
    public void testUnicodeTableNameExistingTable() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            String lineData = "लаблअца поле=\"значение\" 1619509249714000000\n";
            sendLinger(lineData, "लаблअца");

            String lineData2 = "लаблअца,символ=значение2 1619509249714000000\n";
            sendLinger(lineData2, "लаблअца");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("लаблअца"));
            }
            String expected = """
                    поле\ttimestamp\tсимвол
                    значение\t2021-04-27T07:40:49.714000Z\t
                    \t2021-04-27T07:40:49.714000Z\tзначение2
                    """;
            assertTable(expected, "लаблअца");
        });
    }

    @Test
    public void testWindowsAccessDenied() throws Exception {
        String lineData = "table_a,MessageType=B,SequenceNumber=1 Length=92i,test=1.5 1465839830100400000\n";

        runInContext((receiver) -> {
            TableModel m = new TableModel(configuration, "table_a", PartitionBy.DAY);
            if (ColumnType.isTimestampMicro(timestampType.getTimestampType())) {
                m = m.timestamp("ReceiveTime");
            } else {
                m = m.timestampNs("ReceiveTime");
            }
            m.col("SequenceNumber", ColumnType.SYMBOL).indexed(true, 256)
                    .col("MessageType", ColumnType.SYMBOL).indexed(true, 256)
                    .col("Length", ColumnType.INT);
            if (walEnabled) {
                m.wal();
            }
            TestUtils.createTable(engine, m);

            sendLinger(lineData, "table_a");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("table_a"));
            }
            String expected = ColumnType.isTimestampMicro(timestampType.getTimestampType())
                    ? """
                    ReceiveTime\tSequenceNumber\tMessageType\tLength\ttest
                    2016-06-13T17:43:50.100400Z\t1\tB\t92\t1.5
                    """
                    : """
                    ReceiveTime\tSequenceNumber\tMessageType\tLength\ttest
                    2016-06-13T17:43:50.100400000Z\t1\tB\t92\t1.5
                    """;
            assertTable(expected, "table_a");
        });
    }

    @Test
    public void testWithColumnAsReservedKeyword() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            String lineData =
                    """
                            up out=1.0 631150000000000000
                            up in=2.0 631152000000000000
                            up in=3.0 631160000000000000
                            up in=4.0 631170000000000000
                            """;
            sendLinger(lineData, "up");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("up"));
            }
            String expected =
                    """
                            out\ttimestamp\tin
                            1.0\t1989-12-31T23:26:40.000000Z\tnull
                            null\t1990-01-01T00:00:00.000000Z\t2.0
                            null\t1990-01-01T02:13:20.000000Z\t3.0
                            null\t1990-01-01T05:00:00.000000Z\t4.0
                            """;
            assertTable(expected, "up");
        });
    }

    @Test
    public void testWithInvalidColumn() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            String lineData =
                    """
                            up out=1.0 631150000000000000
                            up ..=2.0 631152000000000000
                            up ..=3.0 631160000000000000
                            up ..=4.0 631170000000000000
                            """;
            sendLinger(lineData, "up");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("up"));
            }
            String expected = """
                    out\ttimestamp
                    1.0\t1989-12-31T23:26:40.000000Z
                    """;
            assertTable(expected, "up");
        });
    }

    @Test
    public void testWithTcpSender() throws Exception {
        runInContext((receiver) -> {
            send("table", WAIT_ENGINE_TABLE_RELEASE, () -> {
                try (AbstractLineTcpSender lineTcpSender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
                    lineTcpSender.disableValidation();
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 1")
                            .tag("tag=2", "значение 2")
                            .field("поле=3", "{\"ключ\": \"число\"}")
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag1", "value 2")
                            .$(0);
                    lineTcpSender
                            .metric("table")
                            .tag("tag/2", "value=\2") // Invalid column name, last line is not saved
                            .$(Micros.DAY_MICROS * 1000L);
                    lineTcpSender.flush();
                }
            });

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("table"));
            }
            String expected = """
                    tag1\ttag=2\tполе=3\ttimestamp
                    value 1\tзначение 2\t{"ключ": "число"}\t1970-01-01T00:00:00.000000Z
                    value 2\t\t\t1970-01-01T00:00:00.000000Z
                    """;
            assertTable(expected, "table");
        });
    }

    @Test
    public void testWriter17Fields() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        maxMeasurementSize = 1024;
        String lineData = "tableCRASH,tag_n_1=1,tag_n_2=2,tag_n_3=3,tag_n_4=4,tag_n_5=5,tag_n_6=6," +
                "tag_n_7=7,tag_n_8=8,tag_n_9=9,tag_n_10=10,tag_n_11=11,tag_n_12=12,tag_n_13=13," +
                "tag_n_14=14,tag_n_15=15,tag_n_16=16,tag_n_17=17 value=42.4 1619509249714000000\n";
        runInContext((receiver) -> {
            sendLinger(lineData, "tableCRASH");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("tableCRASH"));
            }
            String expected = """
                    tag_n_1\ttag_n_2\ttag_n_3\ttag_n_4\ttag_n_5\ttag_n_6\ttag_n_7\ttag_n_8\ttag_n_9\ttag_n_10\ttag_n_11\ttag_n_12\ttag_n_13\ttag_n_14\ttag_n_15\ttag_n_16\ttag_n_17\tvalue\ttimestamp
                    1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13\t14\t15\t16\t17\t42.4\t2021-04-27T07:40:49.714000Z
                    """;
            assertTable(expected, "tableCRASH");
        });
    }

    @Test
    public void testWriterAllLongs() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        String lineData = "messages id=843530699759026177i,author=820703963477180437i,guild=820704412095479830i,channel=820704412095479833i,flags=6i 1000\n";
        runInContext((receiver) -> {
            TableModel m = new TableModel(configuration, "messages", PartitionBy.MONTH);
            m.timestamp("ts")
                    .col("id", ColumnType.LONG)
                    .col("author", ColumnType.LONG)
                    .col("guild", ColumnType.LONG)
                    .col("channel", ColumnType.LONG)
                    .col("flags", ColumnType.BYTE);
            if (walEnabled) {
                m.wal();
            }
            TestUtils.createTable(engine, m);

            send(lineData, "messages");
            String expected = """
                    ts\tid\tauthor\tguild\tchannel\tflags
                    1970-01-01T00:00:00.000001Z\t843530699759026177\t820703963477180437\t820704412095479830\t820704412095479833\t6
                    """;

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("messages"));
            }
            assertTable(expected, "messages");
        });
    }

    @Test
    public void testWriterCommitFails() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampNano(timestampType.getTimestampType()));
        // This test only makes sense for TableWriter.
        Assume.assumeFalse(walEnabled);

        runInContext((receiver) -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean rmdir(Path path, boolean lazy) {
                    return false;
                }
            };

            TableModel m = new TableModel(configuration, "table_a", PartitionBy.DAY);
            m.timestampNs("ReceiveTime")
                    .col("SequenceNumber", ColumnType.SYMBOL).indexed(true, 256)
                    .col("MessageType", ColumnType.SYMBOL).indexed(true, 256)
                    .col("Length", ColumnType.INT);
            AbstractCairoTest.create(m);

            String lineData = "table_a,MessageType=B,SequenceNumber=1 Length=92i,test=1.5 1465839830100400000\n";
            sendLinger(lineData, "table_a");

            String expected = "ReceiveTime\tSequenceNumber\tMessageType\tLength\n";
            assertTable(expected, "table_a");
        });
    }

    @Test
    public void testWriterInsertNewSymbolsIntoTableWithExistingSymbols() throws Exception {
        // This test only makes sense to WAL tables since it writes into the table from multiple threads.
        Assume.assumeTrue(walEnabled && ColumnType.isTimestampMicro(timestampType.getTimestampType()));

        // Here we make sure that TableUpdateDetails' and WalWriter's symbol caches to not clash with each other.
        final String tableName = "x";
        final int symbols = 1024;

        runInContext((receiver) -> {
            // First, create a table and insert a few rows into it, so that we get some existing symbol keys.
            TableModel m = new TableModel(configuration, tableName, PartitionBy.MONTH);
            m.timestamp("ts").col("sym", ColumnType.SYMBOL).wal();
            AbstractCairoTest.create(m);

            // Next, start inserting symbols on a background thread.
            final CountDownLatch halfDoneLatch = new CountDownLatch(1);
            final CountDownLatch doneLatch = new CountDownLatch(1);
            new Thread(() -> {
                try (TableWriterAPI writer = getTableWriterAPI(tableName)) {
                    for (int i = 0; i < symbols; i++) {
                        TableWriter.Row row = writer.newRow();
                        row.putSym(1, "sym" + i);
                        row.append();
                        writer.commit();

                        drainWalQueue();

                        if (i == (symbols / 2)) {
                            halfDoneLatch.countDown();
                        }
                    }
                }
                Path.clearThreadLocals();
                doneLatch.countDown();
            }).start();

            // Finally, ingest rows with same symbols, but opposite direction, into the table.
            final StringBuilder lineData = new StringBuilder();
            for (int i = symbols - 1; i > -1; i--) {
                lineData.append(tableName)
                        .append(",sym=sym")
                        .append(i)
                        .append('\n');
            }

            halfDoneLatch.await();
            sendLinger(lineData.toString(), tableName);
            doneLatch.await();

            assertEventually(() -> {
                mayDrainWalQueue();

                CharSequenceIntHashMap symbolCounts = new CharSequenceIntHashMap();
                try (
                        TableReader reader = getReader(tableName);
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    Assert.assertEquals(2 * symbols, cursor.size());
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        symbolCounts.inc(record.getSymA(1));
                    }
                }

                Assert.assertEquals(symbols, symbolCounts.size());
                for (int i = 0; i < symbols; i++) {
                    Assert.assertEquals("count should be 2 for sym" + i + " symbol", 2, symbolCounts.get("sym" + i));
                }
            });
        });
    }

    @Test
    public void testWriterRelease1() throws Exception {
        runInContext((receiver) -> {
            String lineData = """
                    weather,location=us-midwest temperature=82 1465839830100400200
                    weather,location=us-midwest temperature=83 1465839830100500200
                    weather,location=us-eastcoast temperature=81 1465839830101400200
                    """;
            send(lineData, "weather");

            lineData = """
                    weather,location=us-midwest temperature=85 1465839830102300200
                    weather,location=us-eastcoast temperature=89 1465839830102400200
                    weather,location=us-westcost temperature=82 1465839830102500200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t82.0\t2016-06-13T17:43:50.100400Z
                    us-midwest\t83.0\t2016-06-13T17:43:50.100500Z
                    us-eastcoast\t81.0\t2016-06-13T17:43:50.101400Z
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease2() throws Exception {
        runInContext((receiver) -> {
            String lineData = """
                    weather,location=us-midwest temperature=82 1465839830100400200
                    weather,location=us-midwest temperature=83 1465839830100500200
                    weather,location=us-eastcoast temperature=81 1465839830101400200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            try (TableWriterAPI w = getTableWriterAPI("weather")) {
                w.truncateSoft();
            }

            lineData = """
                    weather,location=us-midwest temperature=85 1465839830102300200
                    weather,location=us-eastcoast temperature=89 1465839830102400200
                    weather,location=us-westcost temperature=82 1465839830102500200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease3() throws Exception {
        runInContext((receiver) -> {
            String lineData = """
                    weather,location=us-midwest temperature=82 1465839830100400200
                    weather,location=us-midwest temperature=83 1465839830100500200
                    weather,location=us-eastcoast temperature=81 1465839830101400200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            dropWeatherTable();

            lineData = """
                    weather,location=us-midwest temperature=85 1465839830102300200
                    weather,location=us-eastcoast temperature=89 1465839830102400200
                    weather,location=us-westcost temperature=82 1465839830102500200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    location\ttemperature\ttimestamp
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease4() throws Exception {
        runInContext((receiver) -> {
            String lineData = """
                    weather,location=us-midwest temperature=82 1465839830100400200
                    weather,location=us-midwest temperature=83 1465839830100500200
                    weather,location=us-eastcoast temperature=81 1465839830101400200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            dropWeatherTable();

            lineData = """
                    weather,loc=us-midwest temp=85 1465839830102300200
                    weather,loc=us-eastcoast temp=89 1465839830102400200
                    weather,loc=us-westcost temp=82 1465839830102500200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    loc\ttemp\ttimestamp
                    us-midwest\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterRelease5() throws Exception {
        runInContext((receiver) -> {
            String lineData = """
                    weather,location=us-midwest temperature=82 1465839830100400200
                    weather,location=us-midwest temperature=83 1465839830100500200
                    weather,location=us-eastcoast temperature=81 1465839830101400200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            dropWeatherTable();

            lineData = """
                    weather,location=us-midwest,source=sensor1 temp=85 1465839830102300200
                    weather,location=us-eastcoast,source=sensor2 temp=89 1465839830102400200
                    weather,location=us-westcost,source=sensor1 temp=82 1465839830102500200
                    """;
            send(lineData, "weather");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("weather"));
            }
            String expected = """
                    location\tsource\ttemp\ttimestamp
                    us-midwest\tsensor1\t85.0\t2016-06-13T17:43:50.102300Z
                    us-eastcoast\tsensor2\t89.0\t2016-06-13T17:43:50.102400Z
                    us-westcost\tsensor1\t82.0\t2016-06-13T17:43:50.102500Z
                    """;
            assertTable(expected, "weather");
        });
    }

    @Test
    public void testWriterScientificDoubleNotation() throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        runInContext((receiver) -> {
            String lineData = "doubles d0=0,d1=1.23E-10,d2=1.23E-03,d3=1.23E10,d4=1.23E01,d5=1.23E+10,d6=1.23E+01,dNaN=NaN,dmNan=-NaN,dInf=Infinity,dmInf=-Infinity 0\n";
            send(lineData, "doubles");

            mayDrainWalQueue();
            if (walEnabled) {
                Assert.assertTrue(isWalTable("doubles"));
            }
            String expected = """
                    d0\td1\td2\td3\td4\td5\td6\tdNaN\tdmNan\tdInf\tdmInf\ttimestamp
                    0.0\t1.23E-10\t0.00123\t1.23E10\t12.3\t1.23E10\t12.3\tnull\tnull\tnull\tnull\t1970-01-01T00:00:00.000000Z
                    """;
            assertTable(expected, "doubles");
        });
    }

    private void dropWeatherTable() {
        engine.dropTableOrMatView(path, engine.verifyTableName("weather"));
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private void renameTable(CharSequence from, CharSequence to) {
        try (MemoryMARW mem = Vm.getCMARWInstance(); Path otherPath = new Path()) {
            engine.rename(securityContext, path, mem, from, otherPath, to);
        }
    }

    private void send(String lineData, String tableName, int wait) {
        send(tableName, wait, () -> sendToSocket(lineData));
    }

    private void send(String lineData, String tableName) {
        send(lineData, tableName, WAIT_ENGINE_TABLE_RELEASE);
    }

    private void sendAndWait(
            String lineData,
            CharSequenceObjHashMap<SOUnboundedCountDownLatch> tableIndex,
            int expectedReleaseCount
    ) {
        send(lineData, "weather", WAIT_NO_WAIT);
        tableIndex.get("weather").await(expectedReleaseCount);
    }

    private void sendLinger(String lineData, String... tableNames) {
        send(WAIT_ENGINE_TABLE_RELEASE, () -> sendToSocket(lineData), tableNames);
    }

    private void sendNoWait(String lineData, String tableName) {
        send(lineData, tableName, WAIT_NO_WAIT);
    }

    private void sendWaitWalReleaseCount(String lineData, int walReleaseCount) {
        SOCountDownLatch releaseLatch = new SOCountDownLatch(walReleaseCount);
        engine.setPoolListener((factoryType, thread, tableToken, event, segment, position) -> {
            if (factoryType == PoolListener.SRC_WAL_WRITER && event == PoolListener.EV_RETURN && tableToken != null) {
                LOG.info().$("=== released WAL writer === ").$(tableToken).$();
                releaseLatch.countDown();
            }
        });

        send(lineData, "dummy", WAIT_NO_WAIT);
        releaseLatch.await(10 * Micros.SECOND_MICROS * 1000L);
    }

    private void shutdownReceiverWhileSenderIsSendingData(WorkerPool ioPool, WorkerPool writerPool) throws SqlException {
        String tableName = "tab";
        LineTcpReceiver receiver = new LineTcpReceiver(lineConfiguration, engine, ioPool, writerPool);

        if (ioPool == writerPool) {
            WorkerPoolUtils.setupWriterJobs(ioPool, engine);
        }
        ioPool.start(LOG);
        if (ioPool != writerPool) {
            writerPool.start(LOG);
        }

        final SOCountDownLatch finished = new SOCountDownLatch(1);

        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, msgBufferSize)) {
            for (int i = 0; i < 1000; i++) {
                sender.metric(tableName)
                        .field("id", i)
                        .$(i * 1_000_000L);
                sender.flush();
            }

            new Thread(() -> {
                try {
                    ioPool.halt();

                    long start = System.currentTimeMillis();
                    while (engine.getMetrics().healthMetrics().unhandledErrorsCount() == 0) {
                        Os.sleep(10);
                        if (System.currentTimeMillis() - start > 1000) {
                            break;
                        }
                    }

                    if (writerPool != ioPool) (writerPool).halt();

                    receiver.close();
                } catch (Throwable e) {
                    LOG.errorW().$("Can't close pools or ilp receiver!").$(e).$();
                } finally {
                    finished.countDown();
                }
            }, "shutdown thread").start();

            int i = 1000;
            // run until throws exception or will be killed by CI
            //noinspection InfiniteLoopStatement
            while (true) {
                sender.metric(tableName)
                        .field("id", i)
                        .$(i * 1_000_000L);
                sender.flush();
                i++;
                Os.sleep(100);
            }
        } catch (LineSenderException lse) {
            //expected
        } finally {
            finished.await();
            Path.clearThreadLocals();
        }
    }

    private void test(
            String authKeyId,
            PrivateKey authPrivateKey,
            int msgBufferSize,
            final int nRows,
            boolean expectDisconnect
    ) throws Exception {
        Assume.assumeTrue(ColumnType.isTimestampMicro(timestampType.getTimestampType()));
        test(authKeyId, msgBufferSize, nRows, expectDisconnect,
                () -> {
                    AbstractLineSender sender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), bindPort, 4096);
                    if (authKeyId != null) {
                        sender.authenticate(authKeyId, authPrivateKey);
                    }
                    return sender;
                }
        );
    }

    private void test(
            String authKeyId,
            int msgBufferSize,
            final int nRows,
            boolean expectDisconnect,
            Supplier<AbstractLineSender> senderSupplier
    ) throws Exception {
        this.authKeyId = authKeyId;
        this.msgBufferSize = msgBufferSize;
        assertMemoryLeak(() -> {
            final String[] locations = {"x london", "paris", "rome"};

            final CharSequenceHashSet tables = new CharSequenceHashSet();
            tables.add("weather1");
            tables.add("weather2");
            tables.add("weather3");

            SOCountDownLatch tablesCreated = new SOCountDownLatch();
            tablesCreated.setCount(tables.size());

            final Rnd rand = new Rnd();
            final StringBuilder[] expectedSbs = new StringBuilder[tables.size()];

            engine.setPoolListener((factoryType, thread, token, event, segment, position) -> {
                if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                    if (tables.contains(token.getTableName())) {
                        tablesCreated.countDown();
                    }
                }
            });

            minIdleMsBeforeWriterRelease = 100;
            try (LineTcpReceiver ignored = new LineTcpReceiver(lineConfiguration, engine, sharedWorkerPool, sharedWorkerPool)) {
                long startEpochMs = System.currentTimeMillis();
                sharedWorkerPool.start(LOG);

                try {
                    final AbstractLineSender[] senders = new AbstractLineSender[tables.size()];
                    for (int n = 0; n < senders.length; n++) {
                        senders[n] = senderSupplier.get();
                        StringBuilder sb = new StringBuilder((nRows + 1) * lineConfiguration.getMaxMeasurementSize());
                        sb.append("location\ttemp\ttimestamp\n");
                        expectedSbs[n] = sb;
                    }

                    try {
                        long ts = Os.currentTimeMicros();
                        StringSink tsSink = new StringSink();
                        for (int nRow = 0; nRow < nRows; nRow++) {
                            int nTable = nRow < tables.size() ? nRow : rand.nextInt(tables.size());
                            AbstractLineSender sender = senders[nTable];
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
                            MicrosFormatUtils.appendDateTimeUSec(tsSink, ts);
                            sb.append(tsSink);
                            sb.append('\n');
                            sender.$(ts * 1000);
                            sender.flush();
                            if (expectDisconnect) {
                                // To prevent all data being buffered before the expected disconnect slow sending
                                Os.sleep(100);
                            }
                            ts += rand.nextInt(1000);
                        }
                    } finally {
                        for (AbstractLineSender sender : senders) {
                            sender.close();
                        }
                    }

                    Assert.assertFalse(expectDisconnect);
                    boolean ready = tablesCreated.await(TimeUnit.MINUTES.toNanos(1));
                    if (!ready) {
                        throw new IllegalStateException("Timeout waiting for tables to be created");
                    }
                    mayDrainWalQueue();

                    int nRowsWritten;
                    do {
                        nRowsWritten = 0;
                        long timeTakenMs = System.currentTimeMillis() - startEpochMs;
                        if (timeTakenMs > TEST_TIMEOUT_IN_MS) {
                            LOG.error().$("after ").$(timeTakenMs).$("ms tables only had ").$(nRowsWritten).$(" rows out of ").$(nRows).$();
                            break;
                        }
                        Os.pause();
                        for (int n = 0; n < tables.size(); n++) {
                            CharSequence tableName = tables.get(n);
                            while (true) {
                                try (
                                        TableReader reader = getReader(tableName);
                                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                                ) {
                                    while (cursor.hasNext()) {
                                        nRowsWritten++;
                                    }
                                    break;
                                } catch (EntryLockedException ex) {
                                    LOG.info().$("retrying read for ").$safe(tableName).$();
                                    Os.pause();
                                }
                            }
                            mayDrainWalQueue();
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
                LOG.info().$("checking table ").$safe(tableName).$();
                if (walEnabled) {
                    Assert.assertTrue(isWalTable(tableName));
                }
                assertTable(expectedSbs[n], tableName);
            }
        });
    }

    private void wait(SOUnboundedCountDownLatch latch, int value, long iterations) {
        while (-latch.getCount() < value && iterations-- > 0) {
            Os.sleep(20);
        }
    }
}
