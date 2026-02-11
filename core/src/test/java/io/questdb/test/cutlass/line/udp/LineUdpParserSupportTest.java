/*******************************************************************************
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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.AbstractLineSender;
import io.questdb.cutlass.line.udp.AbstractLineProtoUdpReceiver;
import io.questdb.cutlass.line.udp.LineUdpParserSupport;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Os;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class LineUdpParserSupportTest extends LineUdpInsertTest {
    private static final String locationColumnName = "location";
    private static final String tableName = "table";
    private static final String targetColumnName = "column";

    @Test
    public void testGetValueType() {
        Assert.assertEquals(ColumnType.NULL, LineUdpParserSupport.getValueType(""));

        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("null"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("NULL"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("NulL"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("skull"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("skulL"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("1.6x"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("aa\"aa"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("tre"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("''"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("oX"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("0x"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("a"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("i"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("aflse"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("aTTTT"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("aFFF"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("e"));
        Assert.assertEquals(ColumnType.SYMBOL, LineUdpParserSupport.getValueType("ox1"));

        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("t"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("T"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("f"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("F"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("true"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("false"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("FalSe"));
        Assert.assertEquals(ColumnType.BOOLEAN, LineUdpParserSupport.getValueType("tRuE"));

        Assert.assertEquals(ColumnType.STRING, LineUdpParserSupport.getValueType("\"0x123a4\""));
        Assert.assertEquals(ColumnType.STRING, LineUdpParserSupport.getValueType("\"0x123a4 looks \\\" like=long256,\\\n but tis not!\""));
        Assert.assertEquals(ColumnType.STRING, LineUdpParserSupport.getValueType("\"0x123a4 looks like=long256, but tis not!\""));

        Assert.assertEquals(ColumnType.VARCHAR, LineUdpParserSupport.getValueType("\"0x123a4\"", false));
        Assert.assertEquals(ColumnType.VARCHAR, LineUdpParserSupport.getValueType("\"0x123a4 looks \\\" like=long256,\\\n but tis not!\"", false));
        Assert.assertEquals(ColumnType.VARCHAR, LineUdpParserSupport.getValueType("\"0x123a4 looks like=long256, but tis not!\"", false));

        Assert.assertEquals(ColumnType.UNDEFINED, LineUdpParserSupport.getValueType("\"0x123a4 looks \\\" like=long256,\\\n but tis not!")); // missing closing '"'
        Assert.assertEquals(ColumnType.UNDEFINED, LineUdpParserSupport.getValueType("0x123a4 looks \\\" like=long256,\\\n but tis not!\"")); // wanted to be a string, missing opening '"'

        Assert.assertEquals(ColumnType.LONG256, LineUdpParserSupport.getValueType("0x123i"));
        Assert.assertEquals(ColumnType.LONG256, LineUdpParserSupport.getValueType("0x1i"));

        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("123i"));
        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("1i"));
        Assert.assertEquals(ColumnType.INT, LineUdpParserSupport.getValueType("123i", ColumnType.DOUBLE, ColumnType.INT, true));
        Assert.assertEquals(ColumnType.INT, LineUdpParserSupport.getValueType("1i", ColumnType.FLOAT, ColumnType.INT, true));
        Assert.assertEquals(ColumnType.SHORT, LineUdpParserSupport.getValueType("123i", ColumnType.DOUBLE, ColumnType.SHORT, true));
        Assert.assertEquals(ColumnType.SHORT, LineUdpParserSupport.getValueType("1i", ColumnType.DOUBLE, ColumnType.SHORT, true));
        Assert.assertEquals(ColumnType.BYTE, LineUdpParserSupport.getValueType("123i", ColumnType.FLOAT, ColumnType.BYTE, true));
        Assert.assertEquals(ColumnType.BYTE, LineUdpParserSupport.getValueType("1i", ColumnType.DOUBLE, ColumnType.BYTE, true));

        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("1.45"));
        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("1e-13"));
        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("1.0"));
        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("1"));
        Assert.assertEquals(ColumnType.FLOAT, LineUdpParserSupport.getValueType("1.45", ColumnType.FLOAT, ColumnType.LONG, true));
        Assert.assertEquals(ColumnType.FLOAT, LineUdpParserSupport.getValueType("1e-13", ColumnType.FLOAT, ColumnType.INT, true));
        Assert.assertEquals(ColumnType.FLOAT, LineUdpParserSupport.getValueType("1.0", ColumnType.FLOAT, ColumnType.BYTE, true));
        Assert.assertEquals(ColumnType.FLOAT, LineUdpParserSupport.getValueType("1", ColumnType.FLOAT, ColumnType.LONG, true));

        Assert.assertEquals(ColumnType.TIMESTAMP, LineUdpParserSupport.getValueType("123t"));

        Assert.assertEquals(ColumnType.UNDEFINED, LineUdpParserSupport.getValueType("aaa\""));
        Assert.assertEquals(ColumnType.UNDEFINED, LineUdpParserSupport.getValueType("\"aaa"));

        // in these edge examples, type is guessed as best as possible, later the parser would fail
        // (LineUdpParserSupport.parseFieldValue):
        Assert.assertEquals(ColumnType.LONG256, LineUdpParserSupport.getValueType("0x123a4i"));

        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("123a4i"));
        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("oxi"));
        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("xi"));
        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("oXi"));
        Assert.assertEquals(ColumnType.LONG, LineUdpParserSupport.getValueType("0xi"));

        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("123a4"));
        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("0x1"));
        Assert.assertEquals(ColumnType.DOUBLE, LineUdpParserSupport.getValueType("0x123a4"));
    }

    @Test
    public void testPutBinaryBadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.BINARY,
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
                }
        );
    }

    @Test
    public void testPutByteBadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.BYTE,
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
                }
        );
    }

    @Test
    public void testPutFloatBadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.FLOAT,
                "column\tlocation\ttimestamp\n" +
                        "null\tsp052w\t1970-01-01T00:00:01.000000Z\n" +
                        "3.14159\t\t1970-01-01T00:00:02.000000Z\n" +
                        "5.0\t\t1970-01-01T00:00:05.000000Z\n",
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
                }
        );
    }

    @Test
    public void testPutIPv4BadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.IPv4,
                "column\tlocation\ttimestamp\n" +
                        "1.1.1.1\tsp052w\t1970-01-01T00:00:01.000000Z\n" +
                        "\t\t1970-01-01T00:00:02.000000Z\n" +
                        "12.25.6.8\tsp052w\t1970-01-01T00:00:03.000000Z\n" +
                        "\t\t1970-01-01T00:00:04.000000Z\n",
                (sender) -> {
                    sender.metric(tableName)
                            .field(targetColumnName, "1.1.1.1")
                            .field(locationColumnName, "sp052w")
                            .$(1000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "")
                            .$(2000000000);
                    sender.metric(tableName)
                            .field(targetColumnName, "12.25.6.8")
                            .field(locationColumnName, "sp052w12")
                            .$(3000000000L);
                    sender.metric(tableName)
                            .field(targetColumnName, "null")
                            .$(4000000000L);
                    sender.flush();
                }
        );
    }

    @Test
    public void testPutIntBadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.INT,
                "column\tlocation\ttimestamp\n" +
                        "null\tsp052w\t1970-01-01T00:00:01.000000Z\n" +
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
                }
        );
    }

    @Test
    public void testPutLong256BadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.LONG256,
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
                }
        );
    }

    @Test
    public void testPutShortBadValueIsTreatedAsNull() throws Exception {
        testColumnType(
                ColumnType.SHORT,
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
                }
        );
    }

    private void testColumnType(
            int columnType,
            String expected,
            Consumer<AbstractLineSender> senderConsumer
    ) throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final SOCountDownLatch waitForData = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (event == PoolListener.EV_RETURN && name.getTableName().equals(tableName)
                            && name.equals(engine.verifyTableName(tableName))) {
                        waitForData.countDown();
                    }
                });
                try (AbstractLineProtoUdpReceiver receiver = createLineProtoReceiver(engine)) {
                    TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE);
                    TestUtils.createTable(engine, model
                            .col(targetColumnName, columnType)
                            .col(locationColumnName, ColumnType.getGeoHashTypeWithBits(30))
                            .timestamp()
                    );
                    receiver.start();
                    try (AbstractLineSender sender = createLineProtoSender()) {
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
