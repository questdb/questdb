/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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

import io.questdb.cairo.ColumnType;
import org.junit.Test;

public class LineUdpInsertDecimalTest extends LineUdpInsertTest {
    static final String tableName = "decimal_test";
    static final String targetColumnName = "value";

    @Test
    public void testInsertDecimalTableExists() throws Exception {
        assertType(ColumnType.getDecimalType(8, 3), // Default precision/scale for auto-created decimal columns
                "value\ttimestamp\n" +
                        "123.45\t1970-01-01T00:00:01.000000Z\n" +
                        "-99.99\t1970-01-01T00:00:02.000000Z\n" +
                        "0.00\t1970-01-01T00:00:03.000000Z\n" +
                        "999.123456\t1970-01-01T00:00:04.000000Z\n" +
                        "12345.6789\t1970-01-01T00:00:05.000000Z\n" +
                        "1.0\t1970-01-01T00:00:06.000000Z\n" +
                        "null\t1970-01-01T00:00:11.000000Z\n",
                new String[]{
                        "123.45", // valid decimal literal
                        "-99.99", // valid negative decimal
                        "0.00", // valid zero decimal
                        "999.123456", // valid with more decimal places
                        "12345.6789", // valid larger number with decimals
                        "1", // valid integer with decimal suffix
                        "123.45", // discarded - not a decimal (no 'm' suffix)
                        "invalid", // discarded bad type symbol
                        "123.45i", // discarded bad type long
                        "true", // discarded bad type boolean
                        "", // valid null
                        "\"123.45\"", // discarded bad type string
                        "0t", // discarded bad type timestamp
                });
    }

    @Test
    public void testInsertDecimalHighPrecision() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "123456789012345.123456789\t1970-01-01T00:00:01.000000Z\n" +
                        "-999999999999999.999999999\t1970-01-01T00:00:02.000000Z\n" +
                        "0.000000001\t1970-01-01T00:00:03.000000Z\n" +
                        "99999999999.99999999\t1970-01-01T00:00:04.000000Z\n",
                new String[]{
                        "123456789012345.123456789m", // high precision decimal
                        "-999999999999999.999999999m", // high precision negative
                        "0.000000001m", // very small decimal
                        "99999999999.99999999m", // large number with precision
                        "1.23456789012345678901234567890123456789m", // exceeds precision, should be truncated
                });
    }

    @Test
    public void testInsertDecimalEdgeCases() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "0.0\t1970-01-01T00:00:01.000000Z\n" +
                        "-0.0\t1970-01-01T00:00:02.000000Z\n" +
                        "null\t1970-01-01T00:00:05.000000Z\n",
                new String[]{
                        "0m", // simple zero
                        "-0m", // negative zero
                        "0.0m", // explicit zero with decimal
                        "00.00m", // zero with leading zeros
                        "", // null value
                        "m", // invalid - just suffix without number
                        "123.45.67m", // invalid - multiple decimal points
                        "12a3.45m", // invalid - contains non-numeric characters
                        "123.45mm", // invalid - multiple suffixes
                });
    }

    @Test
    public void testInsertDecimalRangeValues() throws Exception {
        assertTypeNoTable("value\ttimestamp\n" +
                        "99.99\t1970-01-01T00:00:01.000000Z\n" +
                        "9999.99\t1970-01-01T00:00:02.000000Z\n" +
                        "9999999.99\t1970-01-01T00:00:03.000000Z\n" +
                        "999999999999999999.999999\t1970-01-01T00:00:04.000000Z\n" +
                        "12345678901234567890123456789.123456789\t1970-01-01T00:00:05.000000Z\n",
                new String[]{
                        "99.99m", // fits in DECIMAL8
                        "9999.99m", // fits in DECIMAL16
                        "9999999.99m", // fits in DECIMAL32
                        "999999999999999999.999999m", // fits in DECIMAL64
                        "12345678901234567890123456789.123456789m", // requires DECIMAL128 or DECIMAL256
                });
    }

    private static void assertType(int columnType, String expected, String[] values) throws Exception {
        assertType(tableName, targetColumnName, columnType, expected, sender -> {
            long ts = 0L;
            for (int i = 0, n = values.length; i < n; i++) {
                sender.metric(tableName).putAsciiInternal(' ')
                        .put(targetColumnName)
                        .putAsciiInternal('=')
                        .putAsciiInternal(values[i])
                        .$(ts += 1000000000);
            }
        });
    }

    private static void assertTypeNoTable(String expected, String[] values) throws Exception {
        assertType(ColumnType.UNDEFINED, expected, values);
    }
}