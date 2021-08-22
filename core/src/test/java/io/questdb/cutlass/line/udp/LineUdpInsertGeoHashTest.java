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

package io.questdb.cutlass.line.udp;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public class LineUdpInsertGeoHashTest extends AbstractCairoTest {

    private static final LineUdpReceiverConfiguration RCVR_CONF = new DefaultLineUdpReceiverConfiguration();
    private static final int LOCALHOST = Net.parseIPv4("127.0.0.1");
    private static final int PORT = RCVR_CONF.getPort();
    private static final String TABLE_NAME = "tracking";
    private static final String COL_NAME = "geohash";

    @Test
    public void testByteSizedGeoHashes() throws Exception {
        assertGeoHash(5, 1, 9,
                "geohash\ttimestamp\n" +
                        "9\t1970-01-01T00:00:01.000000Z\n" +
                        "4\t1970-01-01T00:00:02.000000Z\n" +
                        "j\t1970-01-01T00:00:03.000000Z\n" +
                        "z\t1970-01-01T00:00:04.000000Z\n" +
                        "h\t1970-01-01T00:00:05.000000Z\n" +
                        "w\t1970-01-01T00:00:06.000000Z\n" +
                        "s\t1970-01-01T00:00:07.000000Z\n" +
                        "1\t1970-01-01T00:00:08.000000Z\n" +
                        "m\t1970-01-01T00:00:09.000000Z\n");
    }

    @Test
    public void testByteSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(4, 1, 3,
                "geohash\ttimestamp\n" +
                        "0100\t1970-01-01T00:00:01.000000Z\n" +
                        "0010\t1970-01-01T00:00:02.000000Z\n" +
                        "1000\t1970-01-01T00:00:03.000000Z\n");
    }

    @Test
    public void testShortSizedGeoHashes() throws Exception {
        assertGeoHash(15, 3, 10,
                "geohash\ttimestamp\n" +
                        "9v1\t1970-01-01T00:00:01.000000Z\n" +
                        "46s\t1970-01-01T00:00:02.000000Z\n" +
                        "jnw\t1970-01-01T00:00:03.000000Z\n" +
                        "zfu\t1970-01-01T00:00:04.000000Z\n" +
                        "hp4\t1970-01-01T00:00:05.000000Z\n" +
                        "wh4\t1970-01-01T00:00:06.000000Z\n" +
                        "s2z\t1970-01-01T00:00:07.000000Z\n" +
                        "1cj\t1970-01-01T00:00:08.000000Z\n" +
                        "mmt\t1970-01-01T00:00:09.000000Z\n" +
                        "71f\t1970-01-01T00:00:10.000000Z\n");
    }

    @Test
    public void testShortSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(13, 3, 10,
                "geohash\ttimestamp\n" +
                        "0100111011000\t1970-01-01T00:00:01.000000Z\n" +
                        "0010000110110\t1970-01-01T00:00:02.000000Z\n" +
                        "1000110100111\t1970-01-01T00:00:03.000000Z\n" +
                        "1111101110110\t1970-01-01T00:00:04.000000Z\n" +
                        "1000010101001\t1970-01-01T00:00:05.000000Z\n" +
                        "1110010000001\t1970-01-01T00:00:06.000000Z\n" +
                        "1100000010111\t1970-01-01T00:00:07.000000Z\n" +
                        "0000101011100\t1970-01-01T00:00:08.000000Z\n" +
                        "1001110011110\t1970-01-01T00:00:09.000000Z\n" +
                        "0011100001011\t1970-01-01T00:00:10.000000Z\n");
    }

    @Test
    public void testShortSizedGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(15, 2, 10,
                "geohash\ttimestamp\n");
    }

    @Test
    public void testIntSizedGeoHashes() throws Exception {
        assertGeoHash(30, 6, 8,
                "geohash\ttimestamp\n" +
                        "9v1s8h\t1970-01-01T00:00:01.000000Z\n" +
                        "46swgj\t1970-01-01T00:00:02.000000Z\n" +
                        "jnw97u\t1970-01-01T00:00:03.000000Z\n" +
                        "zfuqd3\t1970-01-01T00:00:04.000000Z\n" +
                        "hp4muv\t1970-01-01T00:00:05.000000Z\n" +
                        "wh4b6v\t1970-01-01T00:00:06.000000Z\n" +
                        "s2z2fy\t1970-01-01T00:00:07.000000Z\n" +
                        "1cjjwk\t1970-01-01T00:00:08.000000Z\n");
    }

    @Test
    public void testIntSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(29, 6, 8,
                "geohash\ttimestamp\n" +
                        "01001110110000111000010001000\t1970-01-01T00:00:01.000000Z\n" +
                        "00100001101100011100011111000\t1970-01-01T00:00:02.000000Z\n" +
                        "10001101001110001001001111101\t1970-01-01T00:00:03.000000Z\n" +
                        "11111011101101010110011000001\t1970-01-01T00:00:04.000000Z\n" +
                        "10000101010010010011110101101\t1970-01-01T00:00:05.000000Z\n" +
                        "11100100000010001010001101101\t1970-01-01T00:00:06.000000Z\n" +
                        "11000000101111100010011101111\t1970-01-01T00:00:07.000000Z\n" +
                        "00001010111000110001111001001\t1970-01-01T00:00:08.000000Z\n");
    }

    @Test
    public void testIntSizedGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(32, 5, 8,
                "geohash\ttimestamp\n");
    }

    @Test
    public void testLongSizedGeoHashes() throws Exception {
        assertGeoHash(60, 12, 7,
                "geohash\ttimestamp\n" +
                        "9v1s8hm7wpks\t1970-01-01T00:00:01.000000Z\n" +
                        "46swgj10r88k\t1970-01-01T00:00:02.000000Z\n" +
                        "jnw97u4yuquw\t1970-01-01T00:00:03.000000Z\n" +
                        "zfuqd3bf8hbu\t1970-01-01T00:00:04.000000Z\n" +
                        "hp4muv5tgg3q\t1970-01-01T00:00:05.000000Z\n" +
                        "wh4b6vntdq1c\t1970-01-01T00:00:06.000000Z\n" +
                        "s2z2fydsjq5n\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testLongSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(57, 12, 7,
                "geohash\ttimestamp\n" +
                        "010011101100001110000100010000100110011111100101011001011\t1970-01-01T00:00:01.000000Z\n" +
                        "001000011011000111000111110001000010000010111010000100010\t1970-01-01T00:00:02.000000Z\n" +
                        "100011010011100010010011111010001001111011010101101101011\t1970-01-01T00:00:03.000000Z\n" +
                        "111110111011010101100110000011010100111001000100000101011\t1970-01-01T00:00:04.000000Z\n" +
                        "100001010100100100111101011011001011100101111011110001110\t1970-01-01T00:00:05.000000Z\n" +
                        "111001000000100010100011011011101001100101100101100000101\t1970-01-01T00:00:06.000000Z\n" +
                        "110000001011111000100111011110011001100010001101100010110\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testLongSizedGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(60, 11, 7,
                "geohash\ttimestamp\n");
    }

    @Test
    public void testInsertByteGeoHashWrongChars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 1);
                    receiver.start();
                    sendGeoHashLine("@");
                    assertReader("geohash\ttimestamp\n");
                }
            }
        });
    }

    @Test
    public void testInsertShortGeoHashWrongChars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 9);
                    receiver.start();
                    sendGeoHashLine("sp-");
                    assertReader("geohash\ttimestamp\n");
                }
            }
        });
    }

    @Test
    public void testInsertIntGeoHashWrongChars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 31);
                    receiver.start();
                    sendGeoHashLine("sp018*");
                    assertReader("geohash\ttimestamp\n");
                }
            }
        });
    }

    @Test
    public void testInsertLongGeoHashWrongChars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 58);
                    receiver.start();
                    sendGeoHashLine("sp018sp0!18*");
                    assertReader("geohash\ttimestamp\n");
                }
            }
        });
    }

    @Test
    public void testInsertNullByteGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 1);
                    receiver.start();
                    sendGeoHashLine("");
                    assertReader("geohash\ttimestamp\n" +
                            "\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }

    @Test
    public void testInsertNullShortGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 15);
                    receiver.start();
                    sendGeoHashLine("");
                    assertReader("geohash\ttimestamp\n" +
                            "\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }

    @Test
    public void testInsertNullIntGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 30);
                    receiver.start();
                    sendGeoHashLine("");
                    assertReader("geohash\ttimestamp\n" +
                            "\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }

    @Test
    public void testInsertNullLongGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 30);
                    receiver.start();
                    sendGeoHashLine("");
                    assertReader("geohash\ttimestamp\n" +
                            "\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }

    private static void assertGeoHash(int columnBits, int lineGeoSizeChars, int numLines, String expected) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, columnBits);
                    receiver.start();
                    sendGeoHashLines(numLines, lineGeoSizeChars);
                    assertReader(expected);
                }
            }
        });
    }

    private static AbstractLineProtoReceiver createLineProtoReceiver(CairoEngine engine) {
        AbstractLineProtoReceiver lpr;
        if (Os.type == Os.LINUX_AMD64) {
            lpr = new LinuxMMLineProtoReceiver(RCVR_CONF, engine, null);
        } else {
            lpr = new LineProtoReceiver(RCVR_CONF, engine, null);
        }
        return lpr;
    }

    private static void createTable(CairoEngine engine, int bitsPrecision) {
        try (TableModel model = new TableModel(configuration, TABLE_NAME, PartitionBy.NONE)) {
            CairoTestUtils.create(model.col(COL_NAME, ColumnType.geohashWithPrecision(bitsPrecision)).timestamp());
        }
        try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, TABLE_NAME, "pleasure")) {
            writer.warmUp();
        }
    }

    private static void sendGeoHashLine(String value) {
        try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 256 * 1024, 1)) {
            sender.metric(TABLE_NAME).field(COL_NAME, value).$(1_000_000_000);
            sender.flush();
        }
    }

    private static void sendGeoHashLines(int numLines, int charsPrecision) {
        Supplier<String> rnd = randomGeoHashGenerator(charsPrecision);
        try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 256 * 1024, 1)) {
            for (int i = 0; i < numLines; i++) {
                sender.metric(TABLE_NAME).field(COL_NAME, rnd.get()).$((long) ((i + 1) * 1e9));
            }
            sender.flush();
        }
    }

    private static void assertReader(String expected) {
        int numLines = expected.split("[\n]").length - 1;
        try (TableReader reader = new TableReader(new DefaultCairoConfiguration(root), TABLE_NAME)) {
            for (int attempts = 28_02_78; attempts > 0; attempts--) {
                if (reader.size() >= numLines) {
                    break;
                }
                LockSupport.parkNanos(1);
                reader.reload();
            }
            TestUtils.assertReader(expected, reader, sink);
        }
    }

    private static Supplier<String> randomGeoHashGenerator(int chars) {
        final Rnd rnd = new Rnd();
        return () -> {
            double lat = rnd.nextDouble() * 180.0 - 90.0;
            double lng = rnd.nextDouble() * 360.0 - 180.0;
            try {
                StringSink sink = Misc.getThreadLocalBuilder();
                GeoHashes.toString(GeoHashes.fromCoordinates(lat, lng, chars * 5), chars, sink);
                return sink.toString();
            } catch (NumericException e) {
                throw new IllegalStateException("should never happen");
            }
        };
    }
}