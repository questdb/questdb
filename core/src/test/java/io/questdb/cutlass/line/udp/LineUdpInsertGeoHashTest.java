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
    private static final int BITS_PRECISION = 30; // ~±0.61km

    @Test
    public void testInsertValidGeoHashesWhenSchemaExists() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine);
                    receiver.start();
                    sendGeoHashLines(10);
                    assertReader("geohash\ttimestamp\n" +
                                    "9v1s8h\t1970-01-01T00:00:01.000000Z\n" +
                                    "46swgj\t1970-01-01T00:00:02.000000Z\n" +
                                    "jnw97u\t1970-01-01T00:00:03.000000Z\n" +
                                    "zfuqd3\t1970-01-01T00:00:04.000000Z\n" +
                                    "hp4muv\t1970-01-01T00:00:05.000000Z\n" +
                                    "wh4b6v\t1970-01-01T00:00:06.000000Z\n" +
                                    "s2z2fy\t1970-01-01T00:00:07.000000Z\n" +
                                    "1cjjwk\t1970-01-01T00:00:08.000000Z\n" +
                                    "mmt894\t1970-01-01T00:00:09.000000Z\n" +
                                    "71ftmp\t1970-01-01T00:00:10.000000Z\n",
                            10);
                }
            }
        });
    }

    @Test
    public void testInsertNullGeoHashWhenSchemaExists() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine);
                    receiver.start();
                    sendGeoHashLine("");
                    assertReader("geohash\ttimestamp\n" +
                                    "\t1970-01-01T00:00:01.000000Z\n",
                            1);
                }
            }
        });
    }

    @Test
    public void testInsertLowerPrecisionGeoHashWhenSchemaExists() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine);
                    receiver.start();
                    sendGeoHashLine("sp");
                    assertReader("geohash\ttimestamp\n", 0);
                }
            }
        });
    }

    @Test
    public void testInsertHigherPrecisionGeoHashWhenSchemaExists() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine);
                    receiver.start();
                    sendGeoHashLine("sp052w92p1p8");
                    assertReader("geohash\ttimestamp\n" +
                                    "sp052w\t1970-01-01T00:00:01.000000Z\n",
                            1);
                }
            }
        });
    }

    private static AbstractLineProtoReceiver createLineProtoReceiver(CairoEngine engine) {
        AbstractLineProtoReceiver lpr;
        switch (Os.type) {
            case Os.LINUX_AMD64:
                lpr = new LinuxMMLineProtoReceiver(RCVR_CONF, engine, null);
                break;
            default:
                lpr = new LineProtoReceiver(RCVR_CONF, engine, null);
        }
        return lpr;
    }

    private static void createTable(CairoEngine engine) {
        try (TableModel model = new TableModel(configuration, TABLE_NAME, PartitionBy.NONE)) {
            CairoTestUtils.create(model.col(COL_NAME, ColumnType.geohashWithPrecision(BITS_PRECISION)).timestamp());
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

    private static void sendGeoHashLines(int numLines) {
        Supplier<String> rnd = randomGeoHashGenerator(BITS_PRECISION);
        try (LineProtoSender sender = new LineProtoSender(NetworkFacadeImpl.INSTANCE, 0, LOCALHOST, PORT, 256 * 1024, 1)) {
            for (int i = 0; i < numLines; i++) {
                sender.metric(TABLE_NAME).field(COL_NAME, rnd.get()).$((long) ((i + 1) * 1e9));
            }
            sender.flush();
        }
    }

    private static void assertReader(String expected, int numLines) {
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

    private static Supplier<String> randomGeoHashGenerator(int bits) {
        if (bits == 0 || bits % 5 != 0) {
            throw new IllegalArgumentException();
        }
        final Rnd rnd = new Rnd();
        return () -> {
            double lat = rnd.nextDouble() * 180.0 - 90.0;
            double lng = rnd.nextDouble() * 360.0 - 180.0;
            try {
                StringSink sink = Misc.getThreadLocalBuilder();
                GeoHashes.toString(GeoHashes.fromCoordinates(lat, lng, bits), bits / 5, sink);
                return sink.toString();
            } catch (NumericException e) {
                throw new IllegalStateException("should never happen");
            }
        };
    }
}