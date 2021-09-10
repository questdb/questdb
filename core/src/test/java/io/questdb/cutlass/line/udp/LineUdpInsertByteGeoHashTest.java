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
import io.questdb.cutlass.line.LineProtoSender;
import io.questdb.test.tools.TestUtils;

public class LineUdpInsertByteGeoHashTest extends LineUdpInsertGeoHashTest {
    @Override
    public void testGeoHashes() throws Exception {
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

    @Override
    public void testGeoHashesTruncating() throws Exception {
        assertGeoHash(4, 1, 3,
                "geohash\ttimestamp\n" +
                        "0100\t1970-01-01T00:00:01.000000Z\n" +
                        "0010\t1970-01-01T00:00:02.000000Z\n" +
                        "1000\t1970-01-01T00:00:03.000000Z\n");
    }

    @Override
    public void testTableHasGeoHashMessageDoesNot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 4);
                    receiver.start();
                    try (LineProtoSender sender = createLineProtoSender()) {
                        sender.metric(tableName).field("carrots", "9").$(1000000000L);
                        sender.metric(tableName).field("carrots", "4").$(2000000000L);
                        sender.metric(tableName).field("carrots", "j").$(3000000000L);
                        sender.flush();
                    }
                    assertReader(tableName,
                            "geohash\ttimestamp\tcarrots\n" +
                                    "\t1970-01-01T00:00:01.000000Z\t9\n" +
                                    "\t1970-01-01T00:00:02.000000Z\t4\n" +
                                    "\t1970-01-01T00:00:03.000000Z\tj\n",
                            "carrots");
                }
            }
        });
    }

    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 4);
                    receiver.start();
                    sendGeoHashLine("9v1s8hm7wpkssv1h");
                    assertReader(tableName,
                            "geohash\ttimestamp\n" +
                                    "0100\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }

    @Override
    public void testGeoHashesNotEnoughPrecision() throws Exception {
        // TODO: there is no binary representation allowing to represent less than 5 bits
    }

    @Override
    public void testWrongCharGeoHashes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 1);
                    receiver.start();
                    sendGeoHashLine("@");
                    assertReader(tableName,
                            "geohash\ttimestamp\n" +
                                    "\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }

    @Override
    public void testNullGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    createTable(engine, 1);
                    receiver.start();
                    sendGeoHashLine("");
                    sendGeoHashLine("null");
                    assertReader(tableName,
                            "geohash\ttimestamp\n" +
                                    "\t1970-01-01T00:00:01.000000Z\n" +
                                    "\t1970-01-01T00:00:01.000000Z\n");
                }
            }
        });
    }
}
