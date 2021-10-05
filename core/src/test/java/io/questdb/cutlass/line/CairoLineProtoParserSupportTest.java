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

package io.questdb.cutlass.line;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.udp.AbstractLineProtoReceiver;
import io.questdb.cutlass.line.udp.LineUdpInsertTest;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class CairoLineProtoParserSupportTest extends LineUdpInsertTest {
    private static final String tableName = "table";
    private static final String targetColumnName = "column";
    private static final String locationColumnName = "location";

    @Test
    public void testPutByteBadValueIsTreatedAsNull() throws Exception {
        testColumnType(ColumnType.BYTE, 30,
                "column\tlocation\ttimestamp\n" +
                        "5\t\t1970-01-01T00:00:04.000000Z\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, Long.MAX_VALUE)
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, 300)
                            .$(2000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "not a number")
                            .field(locationColumnName, "sp052w12")
                            .$(3000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, 5)
                            .$(4000000000L);
                    sender.flush();
                });
    }

    @Test
    public void testPutShortBadValueIsTreatedAsNull() throws Exception {
        testColumnType(ColumnType.SHORT, 30,
                "column\tlocation\ttimestamp\n" +
                        "0\tsp052w\t1970-01-01T00:00:01.000000Z\n" +
                        "300\t\t1970-01-01T00:00:02.000000Z\n" +
                        "5\t\t1970-01-01T00:00:04.000000Z\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, Long.MAX_VALUE)
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, 300)
                            .$(2000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "not a number")
                            .field(locationColumnName, "sp052w12")
                            .$(3000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, 5)
                            .$(4000000000L);
                    sender.flush();
                });
    }

    @Test
    public void testPutIntBadValueIsTreatedAsNull() throws Exception {
        testColumnType(ColumnType.INT, 30,
                "column\tlocation\ttimestamp\n" +
                        "NaN\tsp052w\t1970-01-01T00:00:01.000000Z\n" +
                        "5\t\t1970-01-01T00:00:04.000000Z\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, Long.MAX_VALUE)
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, 300.12)
                            .$(2000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "not a number")
                            .field(locationColumnName, "sp052w12")
                            .$(3000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, 5)
                            .$(4000000000L);
                    sender.flush();
                });
    }

    @Test
    public void testPutFloatBadValueIsTreatedAsNull() throws Exception {
        testColumnType(ColumnType.FLOAT, 30,
                "column\tlocation\ttimestamp\n" +
                        "Infinity\tsp052w\t1970-01-01T00:00:01.000000Z\n" +
                        "3.1416\t\t1970-01-01T00:00:02.000000Z\n" +
                        "5.0000\t\t1970-01-01T00:00:05.000000Z\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, Double.MAX_VALUE)
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, 3.14159)
                            .$(2000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "not a number")
                            .field(locationColumnName, "sp052w12")
                            .$(3000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, 5)
                            .$(4000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, 5.0)
                            .$(5000000000L);
                    sender.flush();
                });
    }

    @Test
    public void testPutLong256BadValueIsTreatedAsNull() throws Exception {
        testColumnType(ColumnType.LONG256, 30,
                "column\tlocation\ttimestamp\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, Long.MAX_VALUE)
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, 300)
                            .$(2000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "not a number")
                            .field(locationColumnName, "sp052w12")
                            .$(3000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, 5)
                            .$(4000000000L);
                    sender.flush();
                });
    }

    @Test
    public void testPutBinaryBadValueIsTreatedAsNull() throws Exception {
        testColumnType(ColumnType.BINARY, 30,
                "column\tlocation\ttimestamp\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, Long.MAX_VALUE)
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, 5)
                            .$(4000000000L);
                    sender.flush();
                });
    }

    @Test
    public void testGetValueType() {
        Assert.assertEquals(ColumnType.NULL, CairoLineProtoParserSupport.getValueType(""));

        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("null"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("NULL"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("NulL"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("skull"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("skulL"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("1.6x"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("aa\"aa"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("tre"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("''"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("oX"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("0x"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("a"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("i"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("aflse"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("aTTTT"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("aFFF"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("e"));
        Assert.assertEquals(ColumnType.SYMBOL, CairoLineProtoParserSupport.getValueType("ox1"));

        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("t"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("T"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("f"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("F"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("true"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("false"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("FalSe"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.getValueType("tRuE"));

        Assert.assertEquals(ColumnType.STRING, CairoLineProtoParserSupport.getValueType("\"0x123a4\""));
        Assert.assertEquals(ColumnType.STRING, CairoLineProtoParserSupport.getValueType("\"0x123a4 looks \\\" like=long256,\\\n but tis not!\""));
        Assert.assertEquals(ColumnType.STRING, CairoLineProtoParserSupport.getValueType("\"0x123a4 looks like=long256, but tis not!\""));
        Assert.assertEquals(ColumnType.UNDEFINED, CairoLineProtoParserSupport.getValueType("\"0x123a4 looks \\\" like=long256,\\\n but tis not!")); // missing closing '"'
        Assert.assertEquals(ColumnType.UNDEFINED, CairoLineProtoParserSupport.getValueType("0x123a4 looks \\\" like=long256,\\\n but tis not!\"")); // wanted to be a string, missing opening '"'

        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.getValueType("0x123i"));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.getValueType("0x1i"));

        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("123i"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("1i"));

        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("1.45"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("1e-13"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("1.0"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("1"));
        Assert.assertEquals(ColumnType.TIMESTAMP, CairoLineProtoParserSupport.getValueType("123t"));

        Assert.assertEquals(ColumnType.UNDEFINED, CairoLineProtoParserSupport.getValueType("aaa\""));
        Assert.assertEquals(ColumnType.UNDEFINED, CairoLineProtoParserSupport.getValueType("\"aaa"));

        // in these edge examples, type is guessed as best as possible, later the parser would fail
        // (CairoLineProtoParserSupport.parseFieldValue):
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.getValueType("0x123a4i"));

        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("123a4i"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("oxi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("xi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("oXi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.getValueType("0xi"));

        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("123a4"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("0x1"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.getValueType("0x123a4"));
    }

    private void testColumnType(int columnType,
                                int geohashColumnBits,
                                String expected,
                                Consumer<LineProtoSender> senderConsumer) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (event == PoolListener.EV_RETURN && tableName.equals(name)) {
                        waitForData.countDown();
                    }
                });
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
                        CairoTestUtils.create(model
                                .col(targetColumnName, columnType)
                                .col(locationColumnName, ColumnType.getGeoHashTypeWithBits(geohashColumnBits))
                                .timestamp());
                    }
                    receiver.start();
                    try (LineProtoSender sender = createLineProtoSender()) {
                        senderConsumer.accept(sender);
                        sender.flush();
                    }
                    Os.sleep(250L);
                }
                if (!waitForData.await(TimeUnit.SECONDS.toNanos(30L))) {
                    Assert.fail();
                }
            }
            assertReader(tableName, expected);
        });
    }
}
