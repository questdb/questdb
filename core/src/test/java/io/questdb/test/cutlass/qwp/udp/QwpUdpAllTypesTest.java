/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.qwp.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GeoHashes;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.cutlass.qwp.server.DefaultQwpUdpReceiverConfiguration;
import io.questdb.cutlass.qwp.server.LinuxMMQwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiver;
import io.questdb.cutlass.qwp.server.QwpUdpReceiverConfiguration;
import io.questdb.network.Net;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

@RunWith(Parameterized.class)
public class QwpUdpAllTypesTest extends AbstractCairoTest {

    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = 19_002;
    private static final QwpUdpReceiverConfiguration LOW_COMMIT_RATE_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getMaxUncommittedDatagrams() {
            return 1;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private static final QwpUdpReceiverConfiguration STRESS_CONF = new DefaultQwpUdpReceiverConfiguration() {
        @Override
        public int getMaxUncommittedDatagrams() {
            return 100_000;
        }

        @Override
        public int getPort() {
            return PORT;
        }

        @Override
        public int getReceiveBufferSize() {
            return 8 * 1024 * 1024;
        }

        @Override
        public boolean isOwnThread() {
            return false;
        }
    };
    private final ReceiverFactory receiverFactory;

    @SuppressWarnings("unused")
    public QwpUdpAllTypesTest(String name, ReceiverFactory factory) {
        this.receiverFactory = factory;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{"base", (ReceiverFactory) QwpUdpReceiver::new});
        if (Os.isLinux()) {
            params.add(new Object[]{"recvmmsg", (ReceiverFactory) LinuxMMQwpUdpReceiver::new});
        }
        return params;
    }

    @Test
    public void testAllTypesInOneRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_all (
                        b_bool BOOLEAN,
                        b_byte BYTE,
                        b_short SHORT,
                        b_char CHAR,
                        b_int INT,
                        b_float FLOAT,
                        b_long LONG,
                        b_date DATE,
                        b_double DOUBLE,
                        b_sym SYMBOL,
                        b_str VARCHAR,
                        b_uuid UUID,
                        b_l256 LONG256,
                        b_geo GEOHASH(6c),
                        b_dec64 DECIMAL(18, 2),
                        b_dec128 DECIMAL(38, 4),
                        b_dec256 DECIMAL(56, 5),
                        b_darr DOUBLE[],
                        b_ts TIMESTAMP,
                        b_tsns TIMESTAMP_NS,
                        timestamp TIMESTAMP NOT NULL
                    ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
                    """);

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_all", tb -> {
                    tb.getOrCreateColumn("b_bool", TYPE_BOOLEAN, false).addByte((byte) 1);
                    tb.getOrCreateColumn("b_byte", TYPE_BYTE, false).addByte((byte) 42);
                    tb.getOrCreateColumn("b_short", TYPE_SHORT, false).addShort((short) 1234);
                    tb.getOrCreateColumn("b_char", TYPE_CHAR, false).addShort((short) 'X');
                    tb.getOrCreateColumn("b_int", TYPE_INT, false).addInt(56_789);
                    tb.getOrCreateColumn("b_float", TYPE_FLOAT, false).addFloat(3.14f);
                    tb.getOrCreateColumn("b_long", TYPE_LONG, false).addLong(99_999L);
                    tb.getOrCreateColumn("b_date", TYPE_DATE, true).addLong(1_705_276_800_000L);
                    tb.getOrCreateColumn("b_double", TYPE_DOUBLE, false).addDouble(2.718);
                    tb.getOrCreateColumn("b_sym", TYPE_SYMBOL, false).addSymbol("abc");
                    tb.getOrCreateColumn("b_str", TYPE_VARCHAR, true).addString("hello");
                    tb.getOrCreateColumn("b_uuid", TYPE_UUID, true).addUuid(0x550e8400e29b41d4L, 0xa716446655440000L);
                    tb.getOrCreateColumn("b_l256", TYPE_LONG256, true).addLong256(
                            0x1111111111111111L, 0x2222222222222222L,
                            0x3333333333333333L, 0x4444444444444444L
                    );
                    tb.getOrCreateColumn("b_geo", TYPE_GEOHASH, true).addGeoHash(GeoHashes.fromString("s24se0"), 30);
                    tb.getOrCreateColumn("b_dec64", TYPE_DECIMAL64, true).addDecimal64(new Decimal64(12_345L, 2));
                    tb.getOrCreateColumn("b_dec128", TYPE_DECIMAL128, true).addDecimal128(Decimal128.fromLong(123_456_789L, 4));
                    tb.getOrCreateColumn("b_dec256", TYPE_DECIMAL256, true).addDecimal256(Decimal256.fromLong(9_999_912_345L, 5));
                    tb.getOrCreateColumn("b_darr", TYPE_DOUBLE_ARRAY, true).addDoubleArray(new double[]{1.0, 2.0, 3.0});
                    tb.getOrCreateColumn("b_ts", TYPE_TIMESTAMP, true).addLong(1_705_276_800_000_000L);
                    tb.getOrCreateColumn("b_tsns", TYPE_TIMESTAMP_NANOS, true).addLong(1_705_276_800_000_000_123L);
                    tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP).addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            b_bool\tb_byte\tb_short\tb_char\tb_int\tb_float\tb_long\tb_date\tb_double\tb_sym\tb_str\tb_uuid\tb_l256\tb_geo\tb_dec64\tb_dec128\tb_dec256\tb_darr\tb_ts\tb_tsns
                            true\t42\t1234\tX\t56789\t3.14\t99999\t2024-01-15T00:00:00.000Z\t2.718\tabc\thello\t\
                            550e8400-e29b-41d4-a716-446655440000\t\
                            0x4444444444444444333333333333333322222222222222221111111111111111\t\
                            s24se0\t123.45\t12345.6789\t99999.12345\t[1.0,2.0,3.0]\t\
                            2024-01-15T00:00:00.000000Z\t2024-01-15T00:00:00.000000123Z
                            """,
                    "SELECT b_bool, b_byte, b_short, b_char, b_int, b_float, b_long, b_date, b_double, " +
                            "b_sym, b_str, b_uuid, b_l256, b_geo, b_dec64, b_dec128, b_dec256, b_darr, b_ts, b_tsns FROM t_all"
            );
        });
    }

    @Test
    public void testBoolean() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_bool")
                            .boolColumn("flag", true)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.table("t_bool")
                            .boolColumn("flag", false)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    """
                            flag
                            true
                            false
                            """,
                    "SELECT flag FROM t_bool ORDER BY timestamp"
            );
        });
    }

    @Test
    public void testByteDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_byte (val BYTE, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_byte", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_BYTE, false);
                    col.addByte((byte) 42);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n42\n",
                    "SELECT val FROM t_byte"
            );
        });
    }

    @Test
    public void testCharDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_char (val CHAR, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_char", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_CHAR, false);
                    col.addShort((short) 'A');
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\nA\n",
                    "SELECT val FROM t_char"
            );
        });
    }

    @Test
    public void testDateDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_date (val DATE, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                // DATE is millis since epoch: 2024-01-15T00:00:00Z = 1705276800000
                sendDirectRow("t_date", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_DATE, true);
                    col.addLong(1_705_276_800_000L);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n2024-01-15T00:00:00.000Z\n",
                    "SELECT val FROM t_date"
            );
        });
    }

    @Test
    public void testDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_dec128")
                            .decimalColumn("val", Decimal128.fromLong(123_456_789L, 4))
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n12345.6789\n",
                    "SELECT val FROM t_dec128"
            );
        });
    }

    @Test
    public void testDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_dec256")
                            .decimalColumn("val", Decimal256.fromLong(9_999_912_345L, 5))
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n99999.12345\n",
                    "SELECT val FROM t_dec256"
            );
        });
    }

    @Test
    public void testDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_dec64")
                            .decimalColumn("val", new Decimal64(12_345L, 2))
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n123.45\n",
                    "SELECT val FROM t_dec64"
            );
        });
    }

    @Test
    public void testDoubleArray1D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_darr1 (vals DOUBLE[], timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_darr1")
                            .doubleArray("vals", new double[]{1.1, 2.2, 3.3})
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "vals\n[1.1,2.2,3.3]\n",
                    "SELECT vals FROM t_darr1"
            );
        });
    }

    @Test
    public void testDoubleArray2D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_darr2 (vals DOUBLE[][], timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_darr2")
                            .doubleArray("vals", new double[][]{{1.0, 2.0}, {3.0, 4.0}})
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "vals\n[[1.0,2.0],[3.0,4.0]]\n",
                    "SELECT vals FROM t_darr2"
            );
        });
    }

    @Test
    public void testDoubleArray3D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_darr3 (vals DOUBLE[][][], timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    double[][][] cube = {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
                    sender.table("t_darr3")
                            .doubleArray("vals", cube)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "vals\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n",
                    "SELECT vals FROM t_darr3"
            );
        });
    }

    @Test
    public void testDoubleViaSender() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_dbl")
                            .doubleColumn("val", 2.718281828)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n2.718281828\n",
                    "SELECT val FROM t_dbl"
            );
        });
    }

    @Test
    public void testFloatDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_float (val FLOAT, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_float", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_FLOAT, false);
                    col.addFloat(3.14f);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n3.14\n",
                    "SELECT val FROM t_float"
            );
        });
    }

    @Test
    public void testGeoHashDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_geo (val GEOHASH(6c), timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_geo", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_GEOHASH, true);
                    col.addGeoHash(GeoHashes.fromString("s24se0"), 30);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\ns24se0\n",
                    "SELECT val FROM t_geo"
            );
        });
    }

    @Test
    public void testIntDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_int (val INT, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_int", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_INT, false);
                    col.addInt(12_345);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n12345\n",
                    "SELECT val FROM t_int"
            );
        });
    }

    @Test
    public void testLong256Direct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_l256 (val LONG256, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_l256", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_LONG256, true);
                    col.addLong256(
                            0x1111111111111111L, 0x2222222222222222L,
                            0x3333333333333333L, 0x4444444444444444L
                    );
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n0x4444444444444444333333333333333322222222222222221111111111111111\n",
                    "SELECT val FROM t_l256"
            );
        });
    }

    @Test
    public void testLongViaSender() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_lng")
                            .longColumn("val", 9_876_543_210L)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n9876543210\n",
                    "SELECT val FROM t_lng"
            );
        });
    }

    @Test
    public void testShortDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_short (val SHORT, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_short", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_SHORT, false);
                    col.addShort((short) 1234);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n1234\n",
                    "SELECT val FROM t_short"
            );
        });
    }

    @Test
    public void testStress100KRows() throws Exception {
        // This test sends a lot of rows over UDP, chances are that some datagrams gets lost.
        // Instead of weakening assertion, we pre-setup the target table with deduplication enabled
        // and keep ingesting the same data in a loop. So if some datagrams are dropped by the OS,
        // we can still assert that the data are eventually ingested correctly since the Sender will keep resending the same rows until the test finishes.

        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_stress100k (tag SYMBOL, id LONG, val DOUBLE, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL DEDUP UPSERT KEYS (timestamp, id)");

            try (QwpUdpReceiver receiver = receiverFactory.create(STRESS_CONF, engine)) {
                // Drain concurrently with sending to minimize chances of kernel UDP buffer overflow
                // It can still happen during a hiccup. that's ok, the Sender thread retransmits and QuestDB server deduplicates
                AtomicBoolean stop = new AtomicBoolean(false);
                Thread drainThread = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            receiver.runSerially();
                            Os.pause();
                        }
                        // final drain pass
                        receiver.runSerially();
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "qwp-stress100k-drain");
                drainThread.start();

                Thread senderThread = new Thread(() -> {
                    try (QwpUdpSender sender = newSender(1500)) {
                        while (!stop.get()) {
                            for (int i = 0; i < 100_000 && !stop.get(); i++) {
                                sender.table("t_stress100k")
                                        .symbol("tag", "srv-" + (i % 10))
                                        .longColumn("id", i)
                                        .doubleColumn("val", i * 0.1)
                                        .at(1_000_000L + i, ChronoUnit.MICROS);
                            }
                            sender.flush();
                        }
                    }
                }, "qwp-stress100k-sender");
                senderThread.start();

                Throwable failure = null;
                try {
                    TestUtils.assertEventually(() -> {
                        drainWalQueue();
                        assertSql(
                                "count\n100000\n",
                                "SELECT count() FROM t_stress100k"
                        );
                    });
                } catch (Throwable th) {
                    failure = th;
                    throw th;
                } finally {
                    stop.set(true);
                    senderThread.join(10_000);
                    drainThread.join(10_000);

                    if (senderThread.isAlive()) {
                        AssertionError leak = new AssertionError("sender thread did not stop");
                        if (failure != null) {
                            failure.addSuppressed(leak);
                        } else {
                            throw leak;
                        }
                    }
                    if (drainThread.isAlive()) {
                        AssertionError leak = new AssertionError("drain thread did not stop");
                        if (failure != null) {
                            failure.addSuppressed(leak);
                        } else {
                            throw leak;
                        }
                    }
                }
            }

            assertSql(
                    "count\n100000\n",
                    "SELECT count() FROM t_stress100k"
            );
            assertSql(
                    "sum_id\n4999950000\n",
                    "SELECT sum(id) AS sum_id FROM t_stress100k"
            );
        });
    }

    @Test
    public void testStress1KRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(4000)) {
                    for (int i = 0; i < 1000; i++) {
                        sender.table("t_stress")
                                .symbol("tag", "srv-" + (i % 10))
                                .longColumn("id", i)
                                .doubleColumn("val", i * 0.1)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "count\n1000\n",
                    "SELECT count() FROM t_stress"
            );
            assertSql(
                    "sum_id\n499500\n",
                    "SELECT sum(id) AS sum_id FROM t_stress"
            );
        });
    }

    @Test
    public void testStringViaSender() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_str")
                            .stringColumn("msg", "hello world")
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "msg\nhello world\n",
                    "SELECT msg FROM t_str"
            );
            // stringColumn() must send the VARCHAR wire type (0x0F), so the
            // auto-created column type must be VARCHAR, not STRING.
            assertSql(
                    "column\ttype\nmsg\tVARCHAR\n",
                    "SELECT \"column\", type FROM table_columns('t_str') WHERE \"column\" = 'msg'"
            );
        });
    }

    @Test
    public void testSymbolDictionaryOverflow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender(400)) {
                    for (int i = 0; i < 200; i++) {
                        sender.table("t_sym_overflow")
                                .symbol("tag", "unique-" + i)
                                .longColumn("id", i)
                                .at(1_000_000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                drainReceiver(receiver);
                Assert.assertTrue(
                        "expected multiple datagrams, got " + receiver.getProcessedCount(),
                        receiver.getProcessedCount() > 1
                );
            }

            drainWalQueue();
            assertSql(
                    "count\n200\n",
                    "SELECT count() FROM t_sym_overflow"
            );
            assertSql(
                    "distinct_tags\n200\n",
                    "SELECT count_distinct(tag) AS distinct_tags FROM t_sym_overflow"
            );
            assertSql(
                    """
                            tag\tid
                            unique-0\t0
                            """,
                    "SELECT tag, id FROM t_sym_overflow ORDER BY timestamp LIMIT 1"
            );
            assertSql(
                    """
                            tag\tid
                            unique-199\t199
                            """,
                    "SELECT tag, id FROM t_sym_overflow ORDER BY timestamp DESC LIMIT 1"
            );
        });
    }

    @Test
    public void testTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_tscol")
                            .timestampColumn("created", 1_705_276_800_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "created\n2024-01-15T00:00:00.000000Z\n",
                    "SELECT created FROM t_tscol"
            );
        });
    }

    @Test
    public void testTimestampNanos() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                try (QwpUdpSender sender = newSender()) {
                    sender.table("t_tsnano")
                            .timestampColumn("created", 1_705_276_800_000_000_123L, ChronoUnit.NANOS)
                            .at(1_000_000L, ChronoUnit.MICROS);
                    sender.flush();
                }
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "created\n2024-01-15T00:00:00.000000123Z\n",
                    "SELECT created FROM t_tsnano"
            );
        });
    }

    @Test
    public void testUuidDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_uuid (val UUID, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                // UUID: 550e8400-e29b-41d4-a716-446655440000
                // hi = 0x550e8400e29b41d4, lo = 0xa716446655440000
                sendDirectRow("t_uuid", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_UUID, true);
                    col.addUuid(0x550e8400e29b41d4L, 0xa716446655440000L);
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\n550e8400-e29b-41d4-a716-446655440000\n",
                    "SELECT val FROM t_uuid"
            );
        });
    }

    @Test
    public void testVarcharDirect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_varchar (val VARCHAR, timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpUdpReceiver receiver = receiverFactory.create(LOW_COMMIT_RATE_CONF, engine)) {
                sendDirectRow("t_varchar", tb -> {
                    QwpTableBuffer.ColumnBuffer col = tb.getOrCreateColumn("val", TYPE_VARCHAR, true);
                    col.addString("hello varchar");
                    QwpTableBuffer.ColumnBuffer ts = tb.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    ts.addLong(1_000_000L);
                    tb.nextRow();
                });
                drainReceiver(receiver);
            }

            drainWalQueue();
            assertSql(
                    "val\nhello varchar\n",
                    "SELECT val FROM t_varchar"
            );
        });
    }

    private static void drainReceiver(QwpUdpReceiver receiver) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        boolean everReceived = false;
        while (System.nanoTime() < deadline) {
            boolean received = receiver.runSerially();
            if (received) {
                everReceived = true;
            } else if (everReceived) {
                break;
            }
            Os.pause();
        }
        Assert.assertTrue("timeout: receiver did not process any datagrams", everReceived);
    }

    private static QwpUdpSender newSender() {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0);
    }

    private static QwpUdpSender newSender(int maxDatagramSize) {
        return new QwpUdpSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 0, maxDatagramSize);
    }

    private static void sendDirectRow(String tableName, Consumer<QwpTableBuffer> populator) {
        try (QwpTableBuffer tb = new QwpTableBuffer(tableName)) {
            populator.accept(tb);
            sendTableBuffer(tb);
        }
    }

    private static void sendTableBuffer(QwpTableBuffer tb) {
        // Encode using QwpWebSocketEncoder (public API) then send via raw UDP
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(false);
            int len = encoder.encode(tb, false);

            long fd = Net.socketUdp();
            Assert.assertTrue("failed to open UDP socket", fd > -1);
            long mem = 0;
            long sockaddr = 0;
            try {
                mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                Unsafe.copyMemory(encoder.getBuffer().getBufferPtr(), mem, len);
                sockaddr = Net.sockaddr(LOCALHOST, PORT);
                int sent = Net.sendTo(fd, mem, len, sockaddr);
                Assert.assertEquals(len, sent);
            } finally {
                if (sockaddr != 0) {
                    Net.freeSockAddr(sockaddr);
                }
                if (mem != 0) {
                    Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
                }
                Net.close(fd);
            }
        }
    }

    @FunctionalInterface
    public interface ReceiverFactory {
        QwpUdpReceiver create(QwpUdpReceiverConfiguration config, CairoEngine engine);
    }
}
