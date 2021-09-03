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
import io.questdb.cutlass.line.udp.AbstractLineProtoReceiver;
import io.questdb.cutlass.line.udp.LineUdpInsertTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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
    public void testGuessValueType() {
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("e"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("t"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("T"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("f"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("F"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("true"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("false"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("FalSe"));
        Assert.assertEquals(ColumnType.STRING, CairoLineProtoParserSupport.guessValueType("\"0x123a4\""));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.guessValueType("0x123i"));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.guessValueType("0x1i"));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.guessValueType("0x1"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("123i"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("1i"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("1"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("1.45"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("1e-13"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("1.0"));

        // the goal of guessValueType is to guess the potential type of a token, and when
        // - UNDEFINED -> skip the whole line
        // - else -> attempt to parse according to guessed type, and on fail skip value (insert null)
        // guessValueType uses these rules:
        // - if consistently quoted with \" -> STRING
        // - if missing quote \" -> UNDEFINED
        // - if last char in {'e', 'E', 'f', 'F', 't', 'T'} -> BOOLEAN
        // - if last char in {'i', '0'..'9'} and no prefix '0x' -> LONG
        // - if last char in {'i', '0'..'9'} and prefix '0x' -> LONG256
        // - else if len > DOUBLE, otherwise UNDEFINED
        //
        // LineProtoSender.field appends 'i' to LONG256 and numeric non floating
        // LineProtoSender.field appends 't', 'f' to BOOLEAN
        // these are odd cases
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("tre"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("aflse"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("tRuE"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("aTTTT"));
        Assert.assertEquals(ColumnType.BOOLEAN, CairoLineProtoParserSupport.guessValueType("aFFF"));
        Assert.assertEquals(ColumnType.UNDEFINED, CairoLineProtoParserSupport.guessValueType("aaa\""));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.guessValueType("0x123a4"));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.guessValueType("0x123a4i"));
        Assert.assertEquals(ColumnType.LONG256, CairoLineProtoParserSupport.guessValueType("0x1"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("123a4i"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("oxi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("xi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("oXi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("0xi"));
        Assert.assertEquals(ColumnType.LONG, CairoLineProtoParserSupport.guessValueType("i"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("\"aaa"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("aa\"aa"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("123a4"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("oX"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("ox1"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("0x"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("a"));
        Assert.assertEquals(ColumnType.DOUBLE, CairoLineProtoParserSupport.guessValueType("''"));
    }

    private void testColumnType(int columnType,
                                int geohashColumnBits,
                                String expected,
                                Consumer<LineProtoSender> senderConsumer) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
                        CairoTestUtils.create(model
                                .col(targetColumnName, columnType)
                                .col(locationColumnName, ColumnType.geohashWithPrecision(geohashColumnBits))
                                .timestamp());
                    }
                    receiver.start();
                    try (LineProtoSender sender = createLineProtoSender()) {
                        senderConsumer.accept(sender);
                    }
                    assertReader(tableName, expected);
                }
            }
        });
    }
}
