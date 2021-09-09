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
import org.junit.Test;

public class LineUdpInsertOtherTypesTest extends LineUdpInsertTest {
    static final String tableName = "other";
    static final String targetColumnName = "value";

    @Test
    public void testInsertLong() throws Exception {
        assertType(ColumnType.LONG,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:05.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:06.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "9223372036854775807i",
                        "-9223372036854775807i",
                        "0",
                        "100",
                        "-0",
                        "-100",
                        "2147483647",
                        "-2147483647",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertTimestamp() throws Exception {
        assertType(ColumnType.TIMESTAMP,
                "value\ttimestamp\n" +
                        "1970-01-19T21:02:13.921000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:06.000000Z\n" +
                        "294247-01-10T04:00:54.775807Z\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "1630933921000i",
                        "1630933921000",
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "0i",
                        "",
                        "-0i",
                        "9223372036854775807i",
                        "NaN",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertDate() throws Exception {
        assertType(ColumnType.DATE,
                "value\ttimestamp\n" +
                        "2021-09-06T13:12:01.000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:06.000000Z\n" +
                        "292278994-08-17T07:12:55.807Z\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "1630933921000i",
                        "1630933921000",
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "0i",
                        "",
                        "-0i",
                        "9223372036854775807i",
                        "NaN",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertChar() throws Exception {
        assertType(ColumnType.CHAR,
                "value\ttimestamp\n" +
                        "1\t1970-01-01T00:00:01.000000Z\n" +
                        "1\t1970-01-01T00:00:02.000000Z\n" +
                        "N\t1970-01-01T00:00:05.000000Z\n" +
                        "N\t1970-01-01T00:00:06.000000Z\n",
                new CharSequence[]{
                        "\"1630933921000\"",
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "",
                        "-0i",
                        "\"NaN\"",
                        "\"N\"",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertInt() throws Exception {
        assertType(ColumnType.INT,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "NaN\t1970-01-01T00:00:05.000000Z\n" +
                        "NaN\t1970-01-01T00:00:06.000000Z\n" +
                        "2147483647\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483647\t1970-01-01T00:00:08.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "9223372036854775807i",
                        "-9223372036854775807i",
                        "2147483647i",
                        "-2147483647i",
                        "0",
                        "100",
                        "-0",
                        "-100",
                        "2147483647",
                        "-2147483647",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertShort() throws Exception {
        assertType(ColumnType.SHORT,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "32767\t1970-01-01T00:00:05.000000Z\n" +
                        "-32767\t1970-01-01T00:00:06.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "32767i",
                        "-32767i",
                        "0",
                        "100",
                        "-0",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertByte() throws Exception {
        assertType(ColumnType.BYTE,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "127\t1970-01-01T00:00:05.000000Z\n" +
                        "-127\t1970-01-01T00:00:06.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "127i",
                        "-127i",
                        "0",
                        "100",
                        "-0",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertLong256() throws Exception {
        assertType(ColumnType.LONG256,
                "value\ttimestamp\n" +
                        "0x1234\t1970-01-01T00:00:01.000000Z\n" +
                        "0x1234\t1970-01-01T00:00:02.000000Z\n",
                new CharSequence[]{
                        "0x1234i",
                        "0x1234",
                        ""
                });
    }

    @Test
    public void testInsertBoolean() throws Exception {
        assertType(ColumnType.BOOLEAN,
                "value\ttimestamp\n" +
                        "true\t1970-01-01T00:00:01.000000Z\n" +
                        "true\t1970-01-01T00:00:02.000000Z\n" +
                        "true\t1970-01-01T00:00:03.000000Z\n" +
                        "true\t1970-01-01T00:00:04.000000Z\n" +
                        "true\t1970-01-01T00:00:05.000000Z\n" +
                        "false\t1970-01-01T00:00:06.000000Z\n" +
                        "false\t1970-01-01T00:00:07.000000Z\n" +
                        "false\t1970-01-01T00:00:08.000000Z\n" +
                        "false\t1970-01-01T00:00:09.000000Z\n" +
                        "false\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "true",
                        "tRUe",
                        "TRUE",
                        "t",
                        "T",
                        "false",
                        "fALSe",
                        "FALSE",
                        "f",
                        "F",
                        "",
                        "e",
                });
    }

    @Test
    public void testInsertSymbol() throws Exception {
        assertType(ColumnType.SYMBOL,
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "yyy\t1970-01-01T00:00:04.000000Z\n" +
                        "A\t1970-01-01T00:00:06.000000Z\n",
                new CharSequence[]{
                        "e",
                        "xxx",
                        "paff",
                        "yyy",
                        "tt\"tt",
                        "A",
                        "@plant2",
                        ""
                });
    }

    @Test
    public void testInsertString() throws Exception {
        assertType(ColumnType.STRING,
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "tt\"tt\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "\"e\"",
                        "\"xxx\"",
                        "\"paff\"",
                        "\"paff",
                        "paff\"",
                        "yyy",
                        "\"tt\\\"tt\"",
                        "A",
                        "@plant2",
                        ""
                });
    }

    @Test
    public void testInsertDouble() throws Exception {
        assertType(ColumnType.DOUBLE,
                "value\ttimestamp\n" +
                        "0.425667788123\t1970-01-01T00:00:02.000000Z\n" +
                        "3.1415926535897936\t1970-01-01T00:00:03.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:05.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:06.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:07.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:08.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:09.000000Z\n" +
                        "123.0\t1970-01-01T00:00:10.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:11.000000Z\n",
                new CharSequence[]{
                        "1.6x",
                        "0.425667788123",
                        "3.14159265358979323846",
                        "1.35E-12",
                        "1.35e-12",
                        "1.35e12",
                        "1.35E12",
                        "-0.0035e3",
                        "-3.01e-43",
                        "123",
                        "-123",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertFloat() throws Exception {
        assertType(ColumnType.FLOAT,
                "value\ttimestamp\n" +
                        "0.4257\t1970-01-01T00:00:01.000000Z\n" +
                        "3.1416\t1970-01-01T00:00:02.000000Z\n" +
                        "0.0000\t1970-01-01T00:00:03.000000Z\n" +
                        "0.0000\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:05.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:06.000000Z\n" +
                        "-3.5000\t1970-01-01T00:00:07.000000Z\n" +
                        "-0.0000\t1970-01-01T00:00:08.000000Z\n" +
                        "123.0000\t1970-01-01T00:00:09.000000Z\n" +
                        "-123.0000\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "0.425667788123",
                        "3.14159265358979323846",
                        "1.35E-12",
                        "1.35e-12",
                        "1.35e12",
                        "1.35E12",
                        "-0.0035e3",
                        "-3.01e-43",
                        "123",
                        "-123",
                        "NaN",
                        "",
                        "1.6x"
                });
    }

    protected static void assertType(int columnType, String expected, CharSequence[] values) throws Exception {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            CairoTestUtils.create(model.col(targetColumnName, columnType).timestamp());
        }
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
                        CairoTestUtils.create(model.col(targetColumnName, columnType).timestamp());
                    }
                    receiver.start();
                    long ts = 0L;
                    try (LineProtoSender sender = createLineProtoSender()) {
                        for (int i = 0; i < values.length; i++) {
                            sender.metric(tableName).put(' ').put(targetColumnName).put('=').put(values[i]).$(ts += 1000000000);
                        }
                        sender.flush();
                    }
                    assertReader(tableName, expected);
                }
            }
        });
    }
}
