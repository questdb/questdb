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
    public void testInsertLongTableExists() throws Exception {
        assertType(ColumnType.LONG,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:05.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:06.000000Z\n" +
                        "NaN\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483647\t1970-01-01T00:00:13.000000Z\n" +
                        "NaN\t1970-01-01T00:00:15.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "9223372036854775807i",
                        "-9223372036854775807i",
                        "-9223372036854775808i", // NaN, same as null
                        "0", // ignored missing i
                        "100", // ignored missing i
                        "-0", // ignored missing i
                        "-100", // ignored missing i
                        "2147483647", // ignored missing i
                        "-2147483647i",
                        "", // ignored empty line
                        "null" // NaN
                });
    }

    @Test
    public void testInsertLongTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "NaN\t1970-01-01T00:00:05.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:06.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:07.000000Z\n" +
                        "NaN\t1970-01-01T00:00:08.000000Z\n" +
                        "2147483647\t1970-01-01T00:00:14.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "-9223372036854775808i",
                        "9223372036854775807i",
                        "-9223372036854775807i",
                        "null",
                        "\"null\"",
                        "0",
                        "100",
                        "-0",
                        "-100",
                        "2147483647i",
                        "-2147483647",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertTimestampTableExists() throws Exception {
        assertType(ColumnType.TIMESTAMP,
                "value\ttimestamp\n" +
                        "1970-01-19T21:02:13.921000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "\t1970-01-01T00:00:05.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:07.000000Z\n" +
                        "294247-01-10T04:00:54.775807Z\t1970-01-01T00:00:08.000000Z\n" +
                        "\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "1630933921000i",
                        "1630933921000", // discarded missing i
                        "\"1970-01-01T00:00:05.000000Z\"", // discarded bad type string
                        "0i",
                        "-9223372036854775808i", // NaN
                        "", // discarded empty line
                        "-0i",
                        "9223372036854775807i",
                        "NaN", // discarded bad value
                        "null", // null value
                        "1970-01-01T00:00:05.000000Z" // discarded bad type symbol
                });
    }

    @Test
    public void testInsertTimestampTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "1630933921000\t1970-01-01T00:00:01.000000Z\n" +
                        "0\t1970-01-01T00:00:04.000000Z\n" +
                        "0\t1970-01-01T00:00:06.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:07.000000Z\n" +
                        "NaN\t1970-01-01T00:00:09.000000Z\n" +
                        "NaN\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "1630933921000i", // no longer a timestamp, rather a long
                        "1630933921000", // discarded missing i
                        "\"1970-01-01T00:00:05.000000Z\"", // discarded bad type string
                        "0i",
                        "", // discarded empty line
                        "-0i",
                        "9223372036854775807i",
                        "NaN", // discarded bad value
                        "null", // null value
                        "-9223372036854775808i", // NaN
                        "1970-01-01T00:00:05.000000Z" // discarded bad type symbol
                });
    }

    @Test
    public void testInsertDateTableExists() throws Exception {
        assertType(ColumnType.DATE,
                "value\ttimestamp\n" +
                        "2021-09-06T13:12:01.000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:07.000000Z\n" +
                        "292278994-08-17T07:12:55.807Z\t1970-01-01T00:00:08.000000Z\n" +
                        "\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "1630933921000i",
                        "1630933921000",
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "0i",
                        "",
                        "-9223372036854775808i", // NaN
                        "-0i",
                        "9223372036854775807i",
                        "NaN",
                        "null",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertDateTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "1630933921000\t1970-01-01T00:00:01.000000Z\n" +
                        "NaN\t1970-01-01T00:00:03.000000Z\n" +
                        "0\t1970-01-01T00:00:05.000000Z\n" +
                        "0\t1970-01-01T00:00:07.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:08.000000Z\n" +
                        "NaN\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "1630933921000i",
                        "1630933921000",
                        "-9223372036854775808i", // NaN
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "0i",
                        "",
                        "-0i",
                        "9223372036854775807i",
                        "NaN",
                        "null",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertCharTableExists() throws Exception {
        assertType(ColumnType.CHAR,
                "value\ttimestamp\n" +
                        "1\t1970-01-01T00:00:01.000000Z\n" +
                        "1\t1970-01-01T00:00:02.000000Z\n" +
                        "N\t1970-01-01T00:00:05.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "N\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "\"1630933921000\"",
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "",
                        "-0i",
                        "\"NaN\"",
                        "null",
                        "\"N\"",
                        "0",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertCharTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "1630933921000\t1970-01-01T00:00:01.000000Z\n" +
                        "1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:02.000000Z\n" +
                        "NaN\t1970-01-01T00:00:05.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "N\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "\"1630933921000\"",
                        "\"1970-01-01T00:00:05.000000Z\"",
                        "",
                        "-0i",
                        "\"NaN\"",
                        "null",
                        "\"N\"",
                        "0",
                        "1970-01-01T00:00:05.000000Z"
                });
    }

    @Test
    public void testInsertIntTableExists() throws Exception {
        assertType(ColumnType.INT,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "NaN\t1970-01-01T00:00:05.000000Z\n" +
                        "NaN\t1970-01-01T00:00:06.000000Z\n" +
                        "2147483647\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483647\t1970-01-01T00:00:08.000000Z\n" +
                        "NaN\t1970-01-01T00:00:11.000000Z\n" +
                        "NaN\t1970-01-01T00:00:13.000000Z\n",
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
                        "-2147483648i",
                        "-2147483648",
                        "null",
                        "-0",
                        "-100",
                        "2147483647",
                        "-2147483647",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertIntTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:05.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:06.000000Z\n" +
                        "2147483647\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483647\t1970-01-01T00:00:08.000000Z\n" +
                        "-2147483648\t1970-01-01T00:00:11.000000Z\n" +
                        "NaN\t1970-01-01T00:00:13.000000Z\n",
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
                        "-2147483648i",
                        "-2147483648",
                        "null",
                        "-0",
                        "-100",
                        "2147483647",
                        "-2147483647",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertShortTableExists() throws Exception {
        assertType(ColumnType.SHORT,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "32767\t1970-01-01T00:00:05.000000Z\n" +
                        "-32767\t1970-01-01T00:00:06.000000Z\n" +
                        "0\t1970-01-01T00:00:07.000000Z\n" +
                        "0\t1970-01-01T00:00:08.000000Z\n" +
                        "0\t1970-01-01T00:00:10.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "32767i",
                        "-32767i",
                        "-2147483648i",
                        "2147483648i",
                        "2147483648",
                        "null",
                        "0",
                        "100",
                        "-0",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertShortTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "32767\t1970-01-01T00:00:05.000000Z\n" +
                        "-32767\t1970-01-01T00:00:06.000000Z\n" +
                        "NaN\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483648\t1970-01-01T00:00:08.000000Z\n" +
                        "2147483648\t1970-01-01T00:00:09.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "32767i",
                        "-32767i",
                        "null",
                        "-2147483648i",
                        "2147483648i",
                        "2147483648",
                        "0",
                        "100",
                        "-0",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertByteTableExists() throws Exception {
        assertType(ColumnType.BYTE,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "127\t1970-01-01T00:00:05.000000Z\n" +
                        "-127\t1970-01-01T00:00:06.000000Z\n" +
                        "0\t1970-01-01T00:00:09.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "127i",
                        "-127i",
                        "-2147483648i",
                        "0",
                        "null",
                        "100",
                        "-0",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertByteTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "127\t1970-01-01T00:00:05.000000Z\n" +
                        "-2147483648\t1970-01-01T00:00:06.000000Z\n" +
                        "-127\t1970-01-01T00:00:07.000000Z\n" +
                        "NaN\t1970-01-01T00:00:09.000000Z\n",
                new CharSequence[]{
                        "0i",
                        "100i",
                        "-0i",
                        "-100i",
                        "127i",
                        "-2147483648i",
                        "-127i",
                        "0",
                        "null",
                        "100",
                        "-0",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertLong256TableExists() throws Exception {
        assertType(ColumnType.LONG256,
                "value\ttimestamp\n" +
                        "\t1970-01-01T00:00:02.000000Z\n" +
                        "0x1234\t1970-01-01T00:00:04.000000Z\n" +
                        "0x1234\t1970-01-01T00:00:05.000000Z\n" +
                        "0x00\t1970-01-01T00:00:06.000000Z\n",
                new CharSequence[]{
                        "\"\"", // discarded, bad type string
                        "null", // actual null
                        "\"null\"", // discarded, bad type string
                        "0x1234i", // actual long256
                        "0x1234", // actual long256
                        "0x00", // actual long256 value 0
                        "" // discarded, empty line
                });
    }

    @Test
    public void testInsertLong256TableDoesNotExist1() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0x1234\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:03.000000Z\n" +
                        "0x00\t1970-01-01T00:00:04.000000Z\n" +
                        "0x1234\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "0x1234i", // long256
                        "", // discarded, empty line
                        "null", // actual null
                        "0x00", // actual long256 value 0
                        "\"null\"", // discarded, bad type string
                        "120i", // discarded, bad type long
                        "0x1234", // actual long256
                });
    }

    @Test
    public void testInsertLong256TableDoesNotExist2() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0x00\t1970-01-01T00:00:03.000000Z\n" +
                        "0x1234\t1970-01-01T00:00:05.000000Z\n",
                new CharSequence[]{
                        "", // discarded, empty line
                        "null", // discarded, no actual type clues to create col
                        "0x00", //actual long256 value 0
                        "\"null\"", // discarded, bad type string
                        "0x1234", // actual long256
                });
    }

    @Test
    public void testInsertLong256TableDoesNotExist3() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "null\t1970-01-01T00:00:02.000000Z\n",
                new CharSequence[]{
                        "", // discarded, empty line
                        "\"null\"", // actual string
                        "0x1234", //  discarded, bad type long256
                });
    }

    @Test
    public void testInsertBooleanTableExists() throws Exception {
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
                        "false\t1970-01-01T00:00:10.000000Z\n" +
                        "false\t1970-01-01T00:00:11.000000Z\n",
                new CharSequence[]{
                        "true",
                        "tRUe",
                        "TRUE",
                        "t",
                        "T",
                        "null",
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
    public void testInsertBooleanTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "true\t1970-01-01T00:00:01.000000Z\n" +
                        "true\t1970-01-01T00:00:02.000000Z\n" +
                        "true\t1970-01-01T00:00:03.000000Z\n" +
                        "true\t1970-01-01T00:00:04.000000Z\n" +
                        "true\t1970-01-01T00:00:05.000000Z\n" +
                        "false\t1970-01-01T00:00:06.000000Z\n" +
                        "false\t1970-01-01T00:00:07.000000Z\n" +
                        "false\t1970-01-01T00:00:08.000000Z\n" +
                        "false\t1970-01-01T00:00:09.000000Z\n" +
                        "false\t1970-01-01T00:00:10.000000Z\n" +
                        "false\t1970-01-01T00:00:11.000000Z\n",
                new CharSequence[]{
                        "true",
                        "tRUe",
                        "TRUE",
                        "t",
                        "T",
                        "null",
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
    public void testInsertSymbolTableExists() throws Exception {
        assertType(ColumnType.SYMBOL,
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "yyy\t1970-01-01T00:00:04.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "A\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "e",
                        "xxx",
                        "paff",
                        "yyy",
                        "tt\"tt",
                        "null",
                        "A",
                        "@plant2",
                        ""
                });
    }

    @Test
    public void testInsertSymbolTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "yyy\t1970-01-01T00:00:04.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "A\t1970-01-01T00:00:07.000000Z\n",
                new CharSequence[]{
                        "e",
                        "xxx",
                        "paff",
                        "yyy",
                        "tt\"tt",
                        "null",
                        "A",
                        "@plant2",
                        ""
                });
    }

    @Test
    public void testInsertStringTableExists() throws Exception {
        assertType(ColumnType.STRING,
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "tt\"tt\t1970-01-01T00:00:08.000000Z\n",
                new CharSequence[]{
                        "\"e\"",
                        "\"xxx\"",
                        "\"paff\"",
                        "\"paff",
                        "paff\"",
                        "null",
                        "yyy",
                        "\"tt\\\"tt\"",
                        "A",
                        "@plant2",
                        ""
                });
    }

    @Test
    public void testInsertStringTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "tt\"tt\t1970-01-01T00:00:08.000000Z\n",
                new CharSequence[]{
                        "\"e\"",
                        "\"xxx\"",
                        "\"paff\"",
                        "\"paff",
                        "paff\"",
                        "null",
                        "yyy",
                        "\"tt\\\"tt\"",
                        "A",
                        "@plant2",
                        ""
                });
    }


    @Test
    public void testInsertDoubleTableExists() throws Exception {
        assertType(ColumnType.DOUBLE,
                "value\ttimestamp\n" +
                        "1.7976931348623157E308\t1970-01-01T00:00:02.000000Z\n" +
                        "0.425667788123\t1970-01-01T00:00:03.000000Z\n" +
                        "3.1415926535897936\t1970-01-01T00:00:04.000000Z\n" +
                        "1.7976931348623157E308\t1970-01-01T00:00:05.000000Z\n" +
                        "1.7976931348623153E308\t1970-01-01T00:00:06.000000Z\n" +
                        "Infinity\t1970-01-01T00:00:07.000000Z\n" +
                        "-Infinity\t1970-01-01T00:00:08.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:09.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:10.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:11.000000Z\n" +
                        "NaN\t1970-01-01T00:00:12.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:13.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:14.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:15.000000Z\n" +
                        "123.0\t1970-01-01T00:00:16.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:17.000000Z\n",
                new CharSequence[]{
                        "1.6x",
                        "1.7976931348623157E308",
                        "0.425667788123",
                        "3.14159265358979323846",
                        "1.7976931348623156E308",
                        "1.7976931348623152E308",
                        "1.7976931348623152E312",
                        "-1.7976931348623152E312",
                        "1.35E-12",
                        "1.35e-12",
                        "1.35e12",
                        "null",
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
    public void testInsertDoubleTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "1.7976931348623157E308\t1970-01-01T00:00:01.000000Z\n" +
                        "0.425667788123\t1970-01-01T00:00:02.000000Z\n" +
                        "3.1415926535897936\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:05.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:06.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:07.000000Z\n" +
                        "NaN\t1970-01-01T00:00:08.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:09.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:10.000000Z\n" +
                        "3.1415926535897936\t1970-01-01T00:00:11.000000Z\n" +
                        "1.7976931348623153E308\t1970-01-01T00:00:12.000000Z\n" +
                        "Infinity\t1970-01-01T00:00:13.000000Z\n" +
                        "-Infinity\t1970-01-01T00:00:14.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:15.000000Z\n" +
                        "123.0\t1970-01-01T00:00:16.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:17.000000Z\n",
                new CharSequence[]{
                        "1.7976931348623156E308",
                        "0.425667788123",
                        "1.6x",
                        "3.14159265358979323846",
                        "1.35E-12",
                        "1.35e-12",
                        "1.35e12",
                        "null",
                        "1.35E12",
                        "-0.0035e3",
                        "3.14159265358979323846",
                        "1.7976931348623152E308",
                        "1.7976931348623152E312",
                        "-1.7976931348623152E312",
                        "-3.01e-43",
                        "123",
                        "-123",
                        "NaN",
                        ""
                });
    }

    @Test
    public void testInsertFloatTableExists() throws Exception {
        assertType(ColumnType.FLOAT,
                "value\ttimestamp\n" +
                        "0.4257\t1970-01-01T00:00:01.000000Z\n" +
                        "3.1416\t1970-01-01T00:00:02.000000Z\n" +
                        "0.0000\t1970-01-01T00:00:03.000000Z\n" +
                        "0.0000\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:05.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:06.000000Z\n" +
                        "NaN\t1970-01-01T00:00:07.000000Z\n" +
                        "3.4028235E38\t1970-01-01T00:00:08.000000Z\n" +
                        "Infinity\t1970-01-01T00:00:09.000000Z\n" +
                        "-Infinity\t1970-01-01T00:00:10.000000Z\n" +
                        "-3.5000\t1970-01-01T00:00:11.000000Z\n" +
                        "-0.0000\t1970-01-01T00:00:12.000000Z\n" +
                        "123.0000\t1970-01-01T00:00:13.000000Z\n" +
                        "-123.0000\t1970-01-01T00:00:14.000000Z\n",
                new CharSequence[]{
                        "0.425667788123",
                        "3.14159265358979323846",
                        "1.35E-12",
                        "1.35e-12",
                        "1.35e12",
                        "1.35E12",
                        "null",
                        "3.4028235E38",
                        "3.4028235E39",
                        "-3.4028235E39",
                        "-0.0035e3",
                        "-3.01e-43",
                        "123",
                        "-123",
                        "NaN",
                        "",
                        "1.6x"
                });
    }

    @Test
    public void testInsertFloatTableDoesnotExist() throws Exception {
        assertType(ColumnType.FLOAT,
                "value\ttimestamp\n" +
                        "0.4257\t1970-01-01T00:00:01.000000Z\n" +
                        "3.1416\t1970-01-01T00:00:02.000000Z\n" +
                        "0.0000\t1970-01-01T00:00:03.000000Z\n" +
                        "0.0000\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:05.000000Z\n" +
                        "3.4028235E38\t1970-01-01T00:00:06.000000Z\n" +
                        "Infinity\t1970-01-01T00:00:07.000000Z\n" +
                        "-Infinity\t1970-01-01T00:00:08.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:09.000000Z\n" +
                        "NaN\t1970-01-01T00:00:10.000000Z\n" +
                        "-3.5000\t1970-01-01T00:00:11.000000Z\n" +
                        "-0.0000\t1970-01-01T00:00:12.000000Z\n" +
                        "123.0000\t1970-01-01T00:00:13.000000Z\n" +
                        "-123.0000\t1970-01-01T00:00:14.000000Z\n",
                new CharSequence[]{
                        "0.425667788123",
                        "3.14159265358979323846",
                        "1.35E-12",
                        "1.35e-12",
                        "1.35e12",
                        "3.4028235E38",
                        "3.4028235E39",
                        "-3.4028235E39",
                        "1.35E12",
                        "null",
                        "-0.0035e3",
                        "-3.01e-43",
                        "123",
                        "-123",
                        "NaN",
                        "",
                        "1.6x"
                });
    }

    protected static void assertTypeNoTable(String expected, CharSequence[] values) throws Exception {
        assertType(ColumnType.UNDEFINED, expected, values);
    }

    protected static void assertType(int columnType, String expected, CharSequence[] values) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (AbstractLineProtoReceiver receiver = createLineProtoReceiver(engine)) {
                    if (columnType != ColumnType.UNDEFINED) {
                        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
                            CairoTestUtils.create(model.col(targetColumnName, columnType).timestamp());
                        }
                    }
                    receiver.start();
                    long ts = 0L;
                    try (LineProtoSender sender = createLineProtoSender()) {
                        for (int i = 0; i < values.length; i++) {
                            ((LineProtoSender) sender.metric(tableName).put(' ')
                                    .encodeUtf8(targetColumnName)) // this method belongs to a super class that returns this
                                    .put('=')
                                    .put(values[i]) // field method decorates this token, I want full control
                                    .$(ts += 1000000000);
                        }
                        sender.flush();
                    }
                    assertReader(tableName, expected);
                }
            }
        });
    }
}
