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
    public void testByteGeoHashesWhenSchemaExists() throws Exception {
        assertInsert(5, 1, 9,
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
    public void testByteGeoHashesWhenSchemaExists2() throws Exception {
        assertInsert(4, 1, 3,
                "geohash\ttimestamp\n" +
                        "1001\t1970-01-01T00:00:01.000000Z\n" +
                        "0100\t1970-01-01T00:00:02.000000Z\n" +
                        "0001\t1970-01-01T00:00:03.000000Z\n");
    }

    @Test
    public void testByteGeoHashesWhenSchemaExists3() throws Exception {
        assertInsert(6, 1, 9,
                "geohash\ttimestamp\n" +
                        "001001\t1970-01-01T00:00:01.000000Z\n" +
                        "000100\t1970-01-01T00:00:02.000000Z\n" +
                        "010001\t1970-01-01T00:00:03.000000Z\n" +
                        "011111\t1970-01-01T00:00:04.000000Z\n" +
                        "010000\t1970-01-01T00:00:05.000000Z\n" +
                        "011100\t1970-01-01T00:00:06.000000Z\n" +
                        "011000\t1970-01-01T00:00:07.000000Z\n" +
                        "000001\t1970-01-01T00:00:08.000000Z\n" +
                        "010011\t1970-01-01T00:00:09.000000Z\n");
    }

    @Test
    public void testShortGeoHashesWhenSchemaExists1() throws Exception {
        assertInsert(15, 3, 10,
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
    public void testShortGeoHashesWhenSchemaExists2() throws Exception {
        assertInsert(13, 3, 10,
                "geohash\ttimestamp\n" +
                        "0000100111011\t1970-01-01T00:00:01.000000Z\n" +
                        "0000010000110\t1970-01-01T00:00:02.000000Z\n" +
                        "0001000110100\t1970-01-01T00:00:03.000000Z\n" +
                        "0001111101110\t1970-01-01T00:00:04.000000Z\n" +
                        "0001000010101\t1970-01-01T00:00:05.000000Z\n" +
                        "0001110010000\t1970-01-01T00:00:06.000000Z\n" +
                        "0001100000010\t1970-01-01T00:00:07.000000Z\n" +
                        "0000000101011\t1970-01-01T00:00:08.000000Z\n" +
                        "0001001110011\t1970-01-01T00:00:09.000000Z\n" +
                        "0000011100001\t1970-01-01T00:00:10.000000Z\n");
    }

    @Test
    public void testShortGeoHashesWhenSchemaExists3() throws Exception {
        assertInsert(15, 2, 10,
                "geohash\ttimestamp\n");
    }

    @Test
    public void testIntGeoHashesWhenSchemaExists1() throws Exception {
        assertInsert(30, 6, 8,
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
    public void testIntGeoHashesWhenSchemaExists2() throws Exception {
        assertInsert(29, 6, 8,
                "geohash\ttimestamp\n" +
                        "00000100111011000011100001000\t1970-01-01T00:00:01.000000Z\n" +
                        "00000010000110110001110001111\t1970-01-01T00:00:02.000000Z\n" +
                        "00001000110100111000100100111\t1970-01-01T00:00:03.000000Z\n" +
                        "00001111101110110101011001100\t1970-01-01T00:00:04.000000Z\n" +
                        "00001000010101001001001111010\t1970-01-01T00:00:05.000000Z\n" +
                        "00001110010000001000101000110\t1970-01-01T00:00:06.000000Z\n" +
                        "00001100000010111110001001110\t1970-01-01T00:00:07.000000Z\n" +
                        "00000000101011100011000111100\t1970-01-01T00:00:08.000000Z\n");
    }

    @Test
    public void testIntGeoHashesWhenSchemaExists3() throws Exception {
        assertInsert(32, 6, 8,
                "geohash\ttimestamp\n" +
                        "00010011101100001110000100010000\t1970-01-01T00:00:01.000000Z\n" +
                        "00001000011011000111000111110001\t1970-01-01T00:00:02.000000Z\n" +
                        "00100011010011100010010011111010\t1970-01-01T00:00:03.000000Z\n" +
                        "00111110111011010101100110000011\t1970-01-01T00:00:04.000000Z\n" +
                        "00100001010100100100111101011011\t1970-01-01T00:00:05.000000Z\n" +
                        "00111001000000100010100011011011\t1970-01-01T00:00:06.000000Z\n" +
                        "00110000001011111000100111011110\t1970-01-01T00:00:07.000000Z\n" +
                        "00000010101110001100011110010010\t1970-01-01T00:00:08.000000Z\n");
    }

    @Test
    public void testIntGeoHashesWhenSchemaExists4() throws Exception {
        assertInsert(32, 5, 8,
                "geohash\ttimestamp\n");
    }

    @Test
    public void testLongGeoHashesWhenSchemaExists1() throws Exception {
        assertInsert(60, 12, 7,
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
    public void testLongGeoHashesWhenSchemaExists2() throws Exception {
        assertInsert(57, 12, 7,
                "geohash\ttimestamp\n" +
                        "000100111011000011100001000100001001100111111001010110010\t1970-01-01T00:00:01.000000Z\n" +
                        "000010000110110001110001111100010000100000101110100001000\t1970-01-01T00:00:02.000000Z\n" +
                        "001000110100111000100100111110100010011110110101011011010\t1970-01-01T00:00:03.000000Z\n" +
                        "001111101110110101011001100000110101001110010001000001010\t1970-01-01T00:00:04.000000Z\n" +
                        "001000010101001001001111010110110010111001011110111100011\t1970-01-01T00:00:05.000000Z\n" +
                        "001110010000001000101000110110111010011001011001011000001\t1970-01-01T00:00:06.000000Z\n" +
                        "001100000010111110001001110111100110011000100011011000101\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testLongGeoHashesWhenSchemaExists3() throws Exception {
        assertInsert(60, 11, 7,
                "geohash\ttimestamp\n");
    }

    @Test
    public void testNullGeoHashWhenSchemaExists() throws Exception {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            if (b > 1) {
                setUp();
            }
            final int bits = b;
            TestUtils.assertMemoryLeak(() -> {
                try (CairoEngine engine = new CairoEngine(configuration)) {
                    try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                        createTable(engine, bits);
                        receiver.start();
                        sendGeoHashLine("");
                        assertReader("geohash\ttimestamp\n");
                    }
                }
            });
            tearDown();
        }
    }

    private static void assertInsert(int columnBits, int lineGeoSizeChars, int numLines, String expected) throws Exception {
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