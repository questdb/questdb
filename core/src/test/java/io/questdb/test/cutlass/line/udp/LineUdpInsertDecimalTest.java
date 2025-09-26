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
import io.questdb.std.Decimal256;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class LineUdpInsertDecimalTest extends LineUdpInsertTest {
    static final String tableName = "decimal_test";
    static final String targetColumnName = "value";

    @Test
    public void testInsertDecimalBasic() throws Exception {
        assertDecimal("value\ttimestamp\n" +
                        "123.450\t1970-01-01T00:00:01.000000Z\n",
                ColumnType.DECIMAL_DEFAULT_TYPE,
                Decimal256.fromLong(12345, 2)
        );
    }

    @Test
    public void testInsertDecimalDefault() throws Exception {
        assertDecimal("value\ttimestamp\n" +
                        "123.450\t1970-01-01T00:00:01.000000Z\n",
                ColumnType.UNDEFINED,
                Decimal256.fromLong(12345, 2)
        );
    }

    @Test
    public void testInsertDecimalFromDouble() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getDecimalType(8, 3),
                "value\ttimestamp\n" +
                        "123.456\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, 123.456d).$(1_000_000_000)
        );
    }

    @Test
    public void testInsertDecimalFromLong() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getDecimalType(8, 2),
                "value\ttimestamp\n" +
                        "123.00\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, 123L).$(1_000_000_000)
        );

    }

    @Test
    public void testInsertDecimalFromString() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getDecimalType(8, 2),
                "value\ttimestamp\n" +
                        "123.45\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, "123.45").$(1_000_000_000)
        );
    }

    @Test
    public void testInsertDecimalLossyScaling() throws Exception {
        assertDecimal("value\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n",
                ColumnType.getDecimalType(8, 2),
                Decimal256.fromLong(123456, 3)
        );
    }

    @Test
    public void testInsertDecimalTooHighPrecision() throws Exception {
        assertDecimal("value\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n",
                ColumnType.getDecimalType(4, 0),
                Decimal256.fromLong(123456, 0)
        );
    }

    private void assertDecimal(String expected, int type, Decimal256 value) throws Exception {
        assertType(tableName,
                targetColumnName,
                type,
                expected,
                sender -> sender.metric(tableName).decimalColumn(targetColumnName, value).at(1_000_000_000, ChronoUnit.NANOS)
        );
    }
}