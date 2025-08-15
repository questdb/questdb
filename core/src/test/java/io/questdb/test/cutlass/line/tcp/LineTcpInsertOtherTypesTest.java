/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LineTcpInsertOtherTypesTest extends BaseLineTcpContextTest {
    private static final String TABLE = "other";
    private static final String TARGET_COLUMN_NAME = "value";

    private final boolean walEnabled;

    public LineTcpInsertOtherTypesTest(WalMode walMode) {
        this.walEnabled = (walMode == WalMode.WITH_WAL);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WalMode.WITH_WAL}, {WalMode.NO_WAL}
        });
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
    }

    @Test
    public void testInsertBooleanTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "true\t1970-01-01T00:00:01.000000Z\n" +
                        "true\t1970-01-01T00:00:02.000000Z\n" +
                        "true\t1970-01-01T00:00:03.000000Z\n" +
                        "true\t1970-01-01T00:00:04.000000Z\n" +
                        "true\t1970-01-01T00:00:05.000000Z\n" +
                        "false\t1970-01-01T00:00:07.000000Z\n" +
                        "false\t1970-01-01T00:00:08.000000Z\n" +
                        "false\t1970-01-01T00:00:09.000000Z\n" +
                        "false\t1970-01-01T00:00:10.000000Z\n" +
                        "false\t1970-01-01T00:00:11.000000Z\n" +
                        "false\t1970-01-01T00:00:12.000000Z\n",
                new CharSequence[]{
                        "true", // valid
                        "tRUe", // valid
                        "TRUE", // valid
                        "t", // valid
                        "T", // valid
                        "null", // discarded bad type symbol
                        "false", // valid
                        "fALSe", // valid
                        "FALSE", // valid
                        "f", // valid
                        "F", // valid
                        "", // valid null, equals false
                        "e", // discarded bad type symbol
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertBooleanTableExists() throws Exception {
        assertType(
                ColumnType.BOOLEAN,
                "value\ttimestamp\n" +
                        "true\t1970-01-01T00:00:01.000000Z\n" +
                        "true\t1970-01-01T00:00:02.000000Z\n" +
                        "true\t1970-01-01T00:00:03.000000Z\n" +
                        "true\t1970-01-01T00:00:04.000000Z\n" +
                        "true\t1970-01-01T00:00:05.000000Z\n" +
                        "false\t1970-01-01T00:00:07.000000Z\n" +
                        "false\t1970-01-01T00:00:08.000000Z\n" +
                        "false\t1970-01-01T00:00:09.000000Z\n" +
                        "false\t1970-01-01T00:00:10.000000Z\n" +
                        "false\t1970-01-01T00:00:11.000000Z\n" +
                        "false\t1970-01-01T00:00:12.000000Z\n",
                new CharSequence[]{
                        "true", // valid
                        "tRUe", // valid
                        "TRUE", // valid
                        "t", // valid
                        "T", // valid
                        "null", // discarded bad type symbol
                        "false", // valid
                        "fALSe", // valid
                        "FALSE", // valid
                        "f", // valid
                        "F", // valid
                        "", // valid null, equals false
                        "e", // valid
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertByteTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "127\t1970-01-01T00:00:05.000000Z\n" +
                        "-2147483648\t1970-01-01T00:00:06.000000Z\n" +
                        "-127\t1970-01-01T00:00:07.000000Z\n" +
                        "null\t1970-01-01T00:00:13.000000Z\n" +
                        "1\t1970-01-01T00:00:15.000000Z\n" +
                        "0\t1970-01-01T00:00:16.000000Z\n",
                new CharSequence[]{
                        "0i", // valid, taken as long, no way to make a short
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "127i", // valid
                        "-2147483648i", // valid
                        "-127i", // valid
                        "0", // discarded bad type double
                        "null", // discarded bad type symbol
                        "100", // discarded bad type double
                        "-0", // discarded bad type double
                        "NaN", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                },
                false
        );
    }

    @Test
    public void testInsertByteTableExists() throws Exception {
        assertType(
                ColumnType.BYTE,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "127\t1970-01-01T00:00:05.000000Z\n" +
                        "-128\t1970-01-01T00:00:06.000000Z\n" +
                        "0\t1970-01-01T00:00:14.000000Z\n" +
                        "1\t1970-01-01T00:00:16.000000Z\n" +
                        "0\t1970-01-01T00:00:17.000000Z\n",
                new CharSequence[]{
                        "0i", // valid
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "127i", // valid
                        "-128i", // valid
                        "-129i", // discarded bad size
                        "129i", // discarded bad size
                        "0", // discarded bad type double
                        "null", // discarded bad type symbol
                        "100", // discarded bad type double
                        "-0", // discarded bad type double
                        "NaN", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                },
                false
        );
    }

    @Test
    public void testInsertCharTableExists() throws Exception {
        assertType(
                ColumnType.CHAR,
                "value\ttimestamp\n" +
                        "1\t1970-01-01T00:00:02.000000Z\n" +
                        "\t1970-01-01T00:00:04.000000Z\n" +
                        "N\t1970-01-01T00:00:08.000000Z\n",
                new CharSequence[]{
                        "\"1630933921000\"", // discarded too long
                        "\"1\"", // valid
                        "\"1970-01-01T00:00:05.000000Z\"", // discarded too long
                        "", // valid null
                        "-0i", // discarded bad type long
                        "\"NaN\"", // discarded too long
                        "null", // discarded bad type symbol
                        "\"N\"", // valid
                        "0", // discarded bad type double
                        "0t", // discarded bad type timestamp
                        "1970-01-01T00:00:05.000000Z" // discarded bad type symbol
                },
                false
        );
    }

    @Test
    public void testInsertDateTableExists() throws Exception {
        assertType(
                ColumnType.DATE,
                // no literal representation for date, only longs and timestamps can be inserted
                "value\ttimestamp\n" +
                        "2021-09-06T13:12:01.000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "2021-09-06T13:12:01.000Z\t1970-01-01T00:00:02.000000Z\n" +
                        "2021-09-06T13:12:01.000Z\t1970-01-01T00:00:03.000000Z\n" +
                        "2021-09-06T13:12:01.000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:10.000000Z\n" +
                        "\t1970-01-01T00:00:11.000000Z\n" +
                        "\t1970-01-01T00:00:12.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:13.000000Z\n" +
                        "292278994-08-17T07:12:55.807Z\t1970-01-01T00:00:14.000000Z\n" +
                        "1970-01-01T00:00:00.000Z\t1970-01-01T00:00:18.000000Z\n",
                new CharSequence[]{
                        "1630933921000i", // valid
                        "1630933921000000t", // valid
                        "1630933921000000000n", // valid
                        "1630933921000m", // valid
                        "1630933921000", // discarded bad type double
                        "\"1970-01-01T00:00:05.000000Z\"", // discarded bad type string
                        "1970-01-01T00:\"00:05.00\"0000Z", // discarded bad type symbol
                        "\"1970-01-01T00:00:05.000000Z", // discarded bad string value
                        "1970-01-01T00:00:05.000000Z\"", // discarded bad string value
                        "0i", // valid
                        "-9223372036854775808i", // valid NaN, same as null
                        "", // valid null
                        "-0i", // valid
                        "9223372036854775807i", // valid
                        "NaN", // discarded bad type symbol
                        "null", // discarded bad type symbol
                        "1970-01-01T00:00:05.000000Z", // discarded bad type symbol
                        "0t", // valid
                },
                false
        );
    }

    @Test
    public void testInsertDoubleTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "1.7976931348623155E308\t1970-01-01T00:00:01.000000Z\n" +
                        "0.425667788123\t1970-01-01T00:00:02.000000Z\n" +
                        "3.141592653589793\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:05.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:06.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:07.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:09.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:10.000000Z\n" +
                        "3.141592653589793\t1970-01-01T00:00:11.000000Z\n" +
                        "1.7976931348623151E308\t1970-01-01T00:00:12.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:15.000000Z\n" +
                        "123.0\t1970-01-01T00:00:16.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:17.000000Z\n" +
                        "null\t1970-01-01T00:00:18.000000Z\n" +
                        "null\t1970-01-01T00:00:19.000000Z\n" +
                        "1.0\t1970-01-01T00:00:21.000000Z\n" +
                        "0.0\t1970-01-01T00:00:22.000000Z\n",
                new CharSequence[]{
                        "1.7976931348623156E308", // valid
                        "0.425667788123", // valid
                        "1.6x", // discarded bad type symbol
                        "3.14159265358979323846", // valid
                        "1.35E-12", // valid
                        "1.35e-12", // valid
                        "1.35e12", // valid
                        "null", // discarded bad type symbol
                        "1.35E12", // valid
                        "-0.0035e3", // valid
                        "3.14159265358979323846", // valid
                        "1.7976931348623152E308", // valid
                        "1.7976931348623152E312", // invalid - overflow
                        "-1.7976931348623152E312", // invalid - overflow
                        "-3.01e-43", // valid
                        "123", // valid
                        "-123", // valid
                        "NaN", // valid null
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1.0
                        "false", // valid, true casts down to 0.0
                },
                false
        );
    }

    @Test
    public void testInsertDoubleTableExists() throws Exception {
        assertType(
                ColumnType.DOUBLE,
                "value\ttimestamp\n" +
                        "1.7976931348623157E308\t1970-01-01T00:00:02.000000Z\n" +
                        "0.425667788123\t1970-01-01T00:00:03.000000Z\n" +
                        "3.141592653589793\t1970-01-01T00:00:04.000000Z\n" +
                        "1.7976931348623155E308\t1970-01-01T00:00:05.000000Z\n" +
                        "1.7976931348623151E308\t1970-01-01T00:00:06.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:09.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:10.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:11.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:13.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:14.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:15.000000Z\n" +
                        "123.0\t1970-01-01T00:00:16.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:17.000000Z\n" +
                        "null\t1970-01-01T00:00:18.000000Z\n" +
                        "null\t1970-01-01T00:00:19.000000Z\n" +
                        "1.0\t1970-01-01T00:00:21.000000Z\n" +
                        "0.0\t1970-01-01T00:00:22.000000Z\n",
                new CharSequence[]{
                        "1.6x", // discarded bad type symbol
                        "1.7976931348623157E308", // valid
                        "0.425667788123", // valid
                        "3.14159265358979323846", // valid
                        "1.7976931348623156E308", // valid
                        "1.7976931348623152E308", // valid
                        "1.7976931348623152E312", // invalid - overflow
                        "-1.7976931348623152E312", // invalid - overflow
                        "1.35E-12", // valid
                        "1.35e-12", // valid
                        "1.35e12", // valid
                        "null", // discarded bad type symbol
                        "1.35E12", // valid
                        "-0.0035e3", // valid
                        "-3.01e-43", // valid
                        "123", // valid
                        "-123", // valid
                        "NaN", // valid null
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1.0
                        "false", // valid, true casts down to 0.0
                },
                false
        );
    }

    @Test
    public void testInsertFloatTableDoesNotExist() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0.425667788123\t1970-01-01T00:00:01.000000Z\n" +
                        "3.141592653589793\t1970-01-01T00:00:02.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:03.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:05.000000Z\n" +
                        "3.4028235E38\t1970-01-01T00:00:06.000000Z\n" +
                        "3.4028235E39\t1970-01-01T00:00:07.000000Z\n" +
                        "-3.4028235E39\t1970-01-01T00:00:08.000000Z\n" +
                        "1.35E12\t1970-01-01T00:00:09.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:11.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:12.000000Z\n" +
                        "123.0\t1970-01-01T00:00:13.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:14.000000Z\n" +
                        "null\t1970-01-01T00:00:15.000000Z\n" +
                        "null\t1970-01-01T00:00:16.000000Z\n" +
                        "1.0\t1970-01-01T00:00:19.000000Z\n" +
                        "0.0\t1970-01-01T00:00:20.000000Z\n",
                new CharSequence[]{
                        "0.425667788123", // valid, but interpreted as double, cannot make float columns
                        "3.14159265358979323846", // valid
                        "1.35E-12", // valid
                        "1.35e-12", // valid
                        "1.35e12", // valid
                        "3.4028235E38", // valid
                        "3.4028235E39", // valid
                        "-3.4028235E39", // valid
                        "1.35E12", // valid
                        "null", // discarded bad type symbol
                        "-0.0035e3", // valid
                        "-3.01e-43", // valid
                        "123", // valid
                        "-123", // valid
                        "NaN", // valid null
                        "", // valid null
                        "1.6x", // discarded bad type symbol
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1.0
                        "false", // valid, true casts down to 0.0
                },
                false
        );
    }

    @Test
    public void testInsertFloatTableExists() throws Exception {
        assertType(
                ColumnType.FLOAT,
                "value\ttimestamp\n" +
                        "0.4256678\t1970-01-01T00:00:01.000000Z\n" +
                        "3.1415927\t1970-01-01T00:00:02.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:03.000000Z\n" +
                        "1.35E-12\t1970-01-01T00:00:04.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:05.000000Z\n" +
                        "1.35000005E12\t1970-01-01T00:00:06.000000Z\n" +
                        "3.4028235E38\t1970-01-01T00:00:08.000000Z\n" +
                        "null\t1970-01-01T00:00:09.000000Z\n" +
                        "null\t1970-01-01T00:00:10.000000Z\n" +
                        "-3.5\t1970-01-01T00:00:11.000000Z\n" +
                        "-3.01E-43\t1970-01-01T00:00:12.000000Z\n" +
                        "123.0\t1970-01-01T00:00:13.000000Z\n" +
                        "-123.0\t1970-01-01T00:00:14.000000Z\n" +
                        "null\t1970-01-01T00:00:15.000000Z\n" +
                        "null\t1970-01-01T00:00:16.000000Z\n" +
                        "null\t1970-01-01T00:00:17.000000Z\n" +
                        "1.0\t1970-01-01T00:00:20.000000Z\n" +
                        "0.0\t1970-01-01T00:00:21.000000Z\n",
                new CharSequence[]{
                        "0.425667788123", // valid
                        "3.14159265358979323846", // valid
                        "1.35E-12", // valid equals 0
                        "1.35e-12", // valid equals 0
                        "1.35e12", // valid
                        "1.35E12", // valid
                        "null", // discarded bad type symbol
                        "3.4028235E38", // valid
                        "3.4028235E39", // valid Infinity
                        "-3.4028235E39", // valid -Infinity
                        "-0.0035e3", // valid
                        "-3.01e-43", // valid
                        "123", // valid
                        "-123", // valid
                        "NaN", // valid null
                        "", // valid null
                        "NaN", // valid null
                        "1.6x", // discarded bad type symbol
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1.0
                        "false", // valid, true casts down to 0.0
                },
                false
        );
    }

    @Test
    public void testInsertIntTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:05.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:06.000000Z\n" +
                        "2147483647\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483647\t1970-01-01T00:00:08.000000Z\n" +
                        "-2147483648\t1970-01-01T00:00:11.000000Z\n" +
                        "null\t1970-01-01T00:00:19.000000Z\n" +
                        "1\t1970-01-01T00:00:21.000000Z\n" +
                        "0\t1970-01-01T00:00:22.000000Z\n",
                new CharSequence[]{
                        "0i", // valid
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "9223372036854775807i",
                        "-9223372036854775807i",
                        "2147483647i", // valid
                        "-2147483647i", // valid
                        "0", // discarded bad type double
                        "100", // discarded bad type double
                        "-2147483648i", // valid NaN same as null
                        "-2147483648", // discarded bad type double
                        "null", // discarded bad type symbol
                        "-0", // discarded bad type double
                        "-100", // discarded bad type double
                        "2147483647", // discarded bad type double
                        "-2147483647", // discarded bad type double
                        "NaN", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                },
                false
        );
    }

    @Test
    public void testInsertIntTableExists() throws Exception {
        assertType(
                ColumnType.INT,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "null\t1970-01-01T00:00:06.000000Z\n" +
                        "2147483647\t1970-01-01T00:00:07.000000Z\n" +
                        "-2147483647\t1970-01-01T00:00:08.000000Z\n" +
                        "null\t1970-01-01T00:00:11.000000Z\n" +
                        "1\t1970-01-01T00:00:14.000000Z\n" +
                        "0\t1970-01-01T00:00:15.000000Z\n" +
                        "null\t1970-01-01T00:00:21.000000Z\n",
                new CharSequence[]{
                        "0i", // valid
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "9223372036854775808i", // discarded bad value == Long.MIN_VALUE with no - sign, taken as symbol
                        "-9223372036854775808i", // valid NaN, same as null
                        "2147483647i", // valid
                        "-2147483647i", // valid
                        "0", // discarded bad type double
                        "100", // discarded bad type double
                        "-2147483648i", // valid NaN same as null
                        "-2147483648", // discarded out of range
                        "null", // discarded bad type symbol
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                        "-0", // discarded bad type double
                        "-100", // discarded bad type double
                        "2147483647", // discarded bad type double
                        "-2147483647", // discarded bad type double
                        "NaN", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertLong256TableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "0x1234\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:02.000000Z\n" +
                        "0x056789543288867543333668887654\t1970-01-01T00:00:05.000000Z\n",
                new CharSequence[]{
                        "0x1234i", // valid long256
                        "", // valid null
                        "null", // discarded bad type symbol
                        "0x00", // discarded bad type double
                        "0x56789543288867543333668887654i", // valid long256
                        "\"null\"", // discarded bad type string
                        "120i", // discarded bad type long
                        "0x1234", // discarded bad type double
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertLong256TableExists() throws Exception {
        assertType(
                ColumnType.LONG256,
                "value\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n" +
                        "0x1234\t1970-01-01T00:00:05.000000Z\n" +
                        "\t1970-01-01T00:00:08.000000Z\n",
                new CharSequence[]{
                        "", // valid null
                        "\"\"", // discarded bad type string
                        "null", // discarded bad type symbol
                        "\"null\"", // discarded bad type string
                        "0x1234i", // actual long256
                        "0x1234", // discarded bad type double
                        "0x00", // discarded bad type double
                        "", // valid null
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertLongTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:05.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:06.000000Z\n" +
                        "null\t1970-01-01T00:00:07.000000Z\n" +
                        "1\t1970-01-01T00:00:15.000000Z\n" +
                        "0\t1970-01-01T00:00:16.000000Z\n" +
                        "null\t1970-01-01T00:00:20.000000Z\n",
                new CharSequence[]{
                        "0i", // valid
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "9223372036854775807i", // valid
                        "-9223372036854775807i", // valid
                        "-9223372036854775808i", // valid NaN, same as null
                        "\"-9223372036854775808i and joy=yes\"", // discarded bad type string
                        "\"-9223372036854775808i and joy=yes", // discarded broken string
                        "-9223372036854775808i \\\nand joy=yes\"", // discarded broken string
                        "-9223372036854775808i and joy=yes\"", // discarded broken string
                        "-92233720368\"54775808i \"and joy=yes", // discarded bad type symbol
                        "0x12i", // discarded bad type long256
                        "0", // discarded bad type double
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                        "-0", // discarded bad type double
                        "-100", // discarded bad type double
                        "null", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertLongTableExists() throws Exception {
        assertType(
                ColumnType.LONG,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "9223372036854775807\t1970-01-01T00:00:05.000000Z\n" +
                        "-9223372036854775807\t1970-01-01T00:00:06.000000Z\n" +
                        "null\t1970-01-01T00:00:07.000000Z\n" +
                        "1\t1970-01-01T00:00:14.000000Z\n" +
                        "0\t1970-01-01T00:00:15.000000Z\n" +
                        "null\t1970-01-01T00:00:19.000000Z\n",
                new CharSequence[]{
                        "0i", // valid
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "9223372036854775807i", // valid
                        "-9223372036854775807i", // valid
                        "-9223372036854775808i", // valid NaN, same as null
                        "\"-9223372036854775808i and joy=yes\"", // discarded bad type string
                        "\"-9223372036854775808i and joy=yes", // discarded broken string
                        "-9223372036854775808i \\\nand joy=yes\"", // discarded broken string
                        "-92233720368\"54775808i \"and joy=yes", // discarded bad type symbol
                        "0x12i", // discarded bad type long256
                        "0", // discarded bad type double
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                        "-0", // discarded bad type double
                        "-100", // discarded bad type double
                        "null", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                },
                false
        );
    }

    @Test
    public void testInsertShortTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "32767\t1970-01-01T00:00:05.000000Z\n" +
                        "-32767\t1970-01-01T00:00:06.000000Z\n" +
                        "-2147483648\t1970-01-01T00:00:08.000000Z\n" +
                        "2147483648\t1970-01-01T00:00:09.000000Z\n" +
                        "null\t1970-01-01T00:00:15.000000Z\n" +
                        "1\t1970-01-01T00:00:17.000000Z\n" +
                        "0\t1970-01-01T00:00:18.000000Z\n",
                new CharSequence[]{
                        "0i", // valid, taken as long, no way to make a short
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "32767i", // valid
                        "-32767i", // valid
                        "null", // discarded bad type symbol
                        "-2147483648i", // valid
                        "2147483648i", // valid
                        "2147483648", // discarded bad type double
                        "0", // discarded bad type double
                        "100", // discarded bad type double
                        "-0", // discarded bad type double
                        "NaN", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                },
                false
        );
    }

    @Test
    public void testInsertShortTableExists() throws Exception {
        assertType(
                ColumnType.SHORT,
                "value\ttimestamp\n" +
                        "0\t1970-01-01T00:00:01.000000Z\n" +
                        "100\t1970-01-01T00:00:02.000000Z\n" +
                        "0\t1970-01-01T00:00:03.000000Z\n" +
                        "-100\t1970-01-01T00:00:04.000000Z\n" +
                        "32767\t1970-01-01T00:00:05.000000Z\n" +
                        "-32767\t1970-01-01T00:00:06.000000Z\n" +
                        "0\t1970-01-01T00:00:08.000000Z\n" +
                        "0\t1970-01-01T00:00:19.000000Z\n" +
                        "1\t1970-01-01T00:00:21.000000Z\n" +
                        "0\t1970-01-01T00:00:22.000000Z\n",
                new CharSequence[]{
                        "0i", // valid
                        "100i", // valid
                        "-0i", // valid equals 0
                        "-100i", // valid
                        "32767i", // valid
                        "-32767i", // valid
                        "9223372036854775808i", // discarded bad value == Long.MIN_VALUE with no - sign, taken as symbol
                        "-9223372036854775808i", // valid NaN, same as null, a short value of 0
                        "2147483647i", // discarded out of range
                        "-2147483647i", // discarded out of range
                        "-2147483648i", // discarded out of range
                        "2147483648i", // discarded out of range
                        "2147483648", // discarded bad type double
                        "null", // discarded bad type symbol
                        "0", // discarded bad type double
                        "100", // discarded bad type double
                        "-0", // discarded bad type double
                        "NaN", // discarded bad type symbol
                        "", // valid null
                        "0t", // discarded bad type timestamp
                        "true", // valid, true casts down to 1
                        "false", // valid, true casts down to 0
                },
                false
        );
    }

    @Test
    public void testInsertStringTableDoesNotExist() throws Exception {
        assertStringTypesNoTable(false);
    }

    @Test
    public void testInsertStringTableExists() throws Exception {
        assertStringTypes(false);
    }

    @Test
    public void testInsertSymbolTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "yyy\t1970-01-01T00:00:04.000000Z\n" +
                        "tt\"tt\t1970-01-01T00:00:05.000000Z\n" +
                        "null\t1970-01-01T00:00:06.000000Z\n" +
                        "A\t1970-01-01T00:00:07.000000Z\n" +
                        "@plant2\t1970-01-01T00:00:08.000000Z\n" +
                        "@plant\t1970-01-01T00:00:09.000000Z\n" +
                        "\"@plant\"\t1970-01-01T00:00:10.000000Z\n" +
                        "\t1970-01-01T00:00:11.000000Z\n",
                new CharSequence[]{
                        "e", // valid
                        "xxx", // valid
                        "paff", // valid
                        "yyy", // valid
                        "tt\"tt", // valid
                        "null", // valid
                        "A", // valid
                        "@plant2", // valid
                        "@plant", // valid
                        "\"@plant\"", // discarded bad type string
                        "" // valid null
                },
                true
        );
    }

    @Test
    public void testInsertSymbolTableExists() throws Exception {
        assertType(
                ColumnType.SYMBOL,
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "yyy\t1970-01-01T00:00:04.000000Z\n" +
                        "tt\"tt\t1970-01-01T00:00:05.000000Z\n" +
                        "null\t1970-01-01T00:00:06.000000Z\n" +
                        "A\t1970-01-01T00:00:07.000000Z\n" +
                        "@plant2\t1970-01-01T00:00:08.000000Z\n" +
                        "@plant\t1970-01-01T00:00:09.000000Z\n" +
                        "\"@plant\"\t1970-01-01T00:00:10.000000Z\n" +
                        "\t1970-01-01T00:00:11.000000Z\n" +
                        "\"abcd\t1970-01-01T00:00:12.000000Z\n",
                new CharSequence[]{
                        "e", // valid
                        "xxx", // valid
                        "paff", // valid
                        "yyy", // valid
                        "tt\"tt", // valid
                        "null", // valid
                        "A", // valid
                        "@plant2", // valid
                        "@plant", // valid
                        "\"@plant\"", // discarded bad type string
                        "", // valid null,
                        "\"abcd", //valid symbol
                },
                true
        );
    }

    @Test
    public void testInsertTimestamp() throws Exception {
        assertTimestamp(
                "value\ttimestamp\n" +
                        "0.0\t2021-09-06T13:12:01.000000Z\n" +
                        "1.0\t2021-09-06T13:12:01.000000Z\n" +
                        "2.0\t2021-09-06T13:12:01.000000Z\n" +
                        "3.0\t2021-09-06T13:12:01.000000Z\n",
                new CharSequence[]{
                        "1630933921000000000",
                        "1630933921000000000n",
                        "1630933921000000t",
                        "1630933921000m"
                }
        );
    }

    @Test
    public void testInsertTimestampNS() throws Exception {
        timestampType = TestTimestampType.NANO;
        assertTimestamp(
                "value\ttimestamp\n" +
                        "0.0\t2021-09-06T13:12:01.000000000Z\n" +
                        "1.0\t2021-09-06T13:12:01.000000000Z\n" +
                        "2.0\t2021-09-06T13:12:01.000000000Z\n" +
                        "3.0\t2021-09-06T13:12:01.000000000Z\n",
                new CharSequence[]{
                        "1630933921000000000",
                        "1630933921000000000n",
                        "1630933921000000t",
                        "1630933921000m"
                }
        );
        timestampType = TestTimestampType.MICRO;
    }

    @Test
    public void testInsertTimestampNSTableDoesNotExist() throws Exception {
        timestampType = TestTimestampType.NANO;
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "2021-09-06T13:12:01.000000000Z\t1970-01-01T00:00:01.000000000Z\n" +
                        "2021-09-06T13:12:01.000000000Z\t1970-01-01T00:00:02.000000000Z\n" +
                        "2021-09-06T13:12:01.000000000Z\t1970-01-01T00:00:03.000000000Z\n",
                new CharSequence[]{
                        "1630933921000000t",
                        "1630933921000000000n",
                        "1630933921000m"
                },
                false
        );
        timestampType = TestTimestampType.MICRO;
    }

    @Test
    public void testInsertTimestampNSTableExists() throws Exception {
        timestampType = TestTimestampType.NANO;
        assertType(
                ColumnType.TIMESTAMP_NANO,
                "value\ttimestamp\n" +
                        "1970-01-19T21:02:13.921000000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "2021-09-06T13:12:01.000000000Z\t1970-01-01T00:00:02.000000Z\n" +
                        "2021-09-06T13:12:01.000000000Z\t1970-01-01T00:00:03.000000Z\n" +
                        "2021-09-06T13:12:01.000000000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:10.000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:11.000000Z\n" +
                        "\t1970-01-01T00:00:12.000000Z\n" +
                        "\t1970-01-01T00:00:13.000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:14.000000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\t1970-01-01T00:00:15.000000Z\n" +
                        "2262-04-11T23:47:16.854775807Z\t1970-01-01T00:00:16.000000Z\n",
                new CharSequence[]{
                        "1630933921000000i", // valid
                        "1630933921000000t", // valid
                        "1630933921000000000n", // valid
                        "1630933921000m", // valid
                        "1630933921000", // discarded bad type double
                        "\"1970-01-01T00:00:05.000000Z\"", // discarded bad type string
                        "1970-01-01T00:\"00:05.00\"0000Z", // discarded bad type symbol
                        "\"1970-01-01T00:00:05.000000Z", // discarded bad string value
                        "1970-01-01T00:00:05.000000Z\"", // discarded bad string value
                        "0i", // valid
                        "0t", // valid
                        "-9223372036854775808i", // valid NaN, same as null
                        "", // valid null
                        "-0i", // valid
                        "-0t", // valid
                        "9223372036854775807i", // valid
                        "NaN", // discarded bad type symbol
                        "null", // discarded bad type symbol
                        "1970-01-01T00:00:05.000000Z", // discarded bad type symbol
                        "t", // discarded bad type boolean
                },
                false
        );
        timestampType = TestTimestampType.MICRO;
    }

    @Test
    public void testInsertTimestampTableDoesNotExist() throws Exception {
        assertTypeNoTable(
                "value\ttimestamp\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:02.000000Z\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:03.000000Z\n",
                new CharSequence[]{
                        "1630933921000000t",
                        "1630933921000000000n",
                        "1630933921000m"
                },
                false
        );
    }

    @Test
    public void testInsertTimestampTableExists() throws Exception {
        assertType(
                ColumnType.TIMESTAMP,
                "value\ttimestamp\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:01.000000Z\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:02.000000Z\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:03.000000Z\n" +
                        "2021-09-06T13:12:01.000000Z\t1970-01-01T00:00:04.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:10.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:11.000000Z\n" +
                        "\t1970-01-01T00:00:12.000000Z\n" +
                        "\t1970-01-01T00:00:13.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:14.000000Z\n" +
                        "1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:15.000000Z\n" +
                        "294247-01-10T04:00:54.775807Z\t1970-01-01T00:00:16.000000Z\n",
                new CharSequence[]{
                        "1630933921000000i", // valid
                        "1630933921000000t", // valid
                        "1630933921000000000n", // valid
                        "1630933921000m", // valid
                        "1630933921000", // discarded bad type double
                        "\"1970-01-01T00:00:05.000000Z\"", // discarded bad type string
                        "1970-01-01T00:\"00:05.00\"0000Z", // discarded bad type symbol
                        "\"1970-01-01T00:00:05.000000Z", // discarded bad string value
                        "1970-01-01T00:00:05.000000Z\"", // discarded bad string value
                        "0i", // valid
                        "0t", // valid
                        "-9223372036854775808i", // valid NaN, same as null
                        "", // valid null
                        "-0i", // valid
                        "-0t", // valid
                        "9223372036854775807i", // valid
                        "NaN", // discarded bad type symbol
                        "null", // discarded bad type symbol
                        "1970-01-01T00:00:05.000000Z", // discarded bad type symbol
                        "t", // discarded bad type boolean
                },
                false
        );
    }

    @Test
    public void testInsertVarcharTableDoesNotExist() throws Exception {
        assertStringTypesNoTable(true);
    }

    @Test
    public void testInsertVarcharTableExists() throws Exception {
        assertStringTypes(true);
    }

    private void assertStringTypes(boolean varchar) throws Exception {
        assertStringTypes(varchar ? ColumnType.VARCHAR : ColumnType.STRING);
    }

    private void assertStringTypes(int columnType) throws Exception {
        assertType(columnType,
                "value\ttimestamp\n" +
                        "e\t1970-01-01T00:00:01.000000Z\n" +
                        "xxx\t1970-01-01T00:00:02.000000Z\n" +
                        "paff\t1970-01-01T00:00:03.000000Z\n" +
                        "tt\"tt\t1970-01-01T00:00:11.000000Z\n" +
                        "tt\"tt\" \n =, ,=\"\t1970-01-01T00:00:12.000000Z\n" +
                        "\t1970-01-01T00:00:15.000000Z\n",
                new CharSequence[]{
                        "\"e\"", // 1, valid
                        "\"xxx\"", // 2, valid
                        "\"paff\"", // 3, valid
                        "\"paff", // 4, no quotes, invalid
                        "paff\"", // 5, no quotes, invalid
                        "null", // 6, no quotes, invalid
                        "yyy", // 7, no quotes, invalid
                        "\"tt\"tt\"", // 8, no quotes, invalid
                        "tt\"tt\"", // 9, no quotes, invalid
                        "\"tt\"tt", // 10, no quotes, invalid
                        "\"tt\\\"tt\"", // 11, valid
                        "\"tt\\\"tt\\\" \\\n =, ,=\\\"\"", // 12, valid
                        "A", // 13, no quotes, invalid
                        "@plant2", // 14, no quotes, invalid
                        "" // 15, valid null
                },
                false
        );
    }

    private void assertStringTypesNoTable(boolean varchar) throws Exception {
        useLegacyString = !varchar;
        assertStringTypes(ColumnType.UNDEFINED);
        useLegacyString = true; // restore default
    }

    private void assertTimestamp(String expected, CharSequence[] values) throws Exception {
        runInContext(() -> {
            sink.clear();
            for (int i = 0; i < values.length; i++) {
                sink.put(TABLE)
                        .put(' ').put(TARGET_COLUMN_NAME).put('=').put(i)
                        .put(' ').put(values[i])
                        .put('\n');
            }
            recvBuffer = sink.toString();
            do {
                handleContextIO0();
                Assert.assertFalse(disconnected);
            } while (!recvBuffer.isEmpty());
            closeContext();
            mayDrainWalQueue();
            try (TableReader reader = newOffPoolReader(new DefaultTestCairoConfiguration(root), TABLE)) {
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    private void assertType(int columnType, String expected, CharSequence[] values, boolean isTag) throws Exception {
        runInContext(() -> {
            if (columnType != ColumnType.UNDEFINED) {
                TableModel model = new TableModel(configuration, TABLE, PartitionBy.DAY);
                TestUtils.createTable(engine, model.col(TARGET_COLUMN_NAME, columnType).timestamp());
                if (walEnabled) {
                    Assert.assertTrue(isWalTable(TABLE));
                }
            }
            sink.clear();
            long ts = 0L;
            for (int i = 0; i < values.length; i++) {
                sink.put(TABLE)
                        .put(isTag ? ',' : ' ').put(TARGET_COLUMN_NAME).put('=').put(values[i])
                        .put(isTag ? "  " : " ").put(ts += 1000000000)
                        .put('\n');
            }
            recvBuffer = sink.toString();
            do {
                handleContextIO0();
                Assert.assertFalse(disconnected);
            } while (!recvBuffer.isEmpty());
            closeContext();
            mayDrainWalQueue();
            try (TableReader reader = newOffPoolReader(new DefaultTestCairoConfiguration(root), TABLE)) {
                TestUtils.assertReader(expected, reader, sink);
            }
        });
    }

    private void assertTypeNoTable(String expected, CharSequence[] values, boolean isTag) throws Exception {
        assertType(ColumnType.UNDEFINED, expected, values, isTag);
    }

    private void mayDrainWalQueue() {
        if (walEnabled) {
            drainWalQueue();
        }
    }
}
