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

package io.questdb.test.griffin.engine.functions.math;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.math.LeastNumericFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LeastNumericFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testLeastNumericFunctionFactoryDecimalOverflow() throws Exception {
        assertException(
                "select least(123.456::decimal(76,73), 99999::int)",
                43,
                "inconvertible value: 99999 [INT -> DECIMAL(76,73)]"
        );
    }

    @Test
    public void testLeastNumericFunctionFactoryDecimals() throws Exception {
        assertSqlWithTypes("least\n6.000:DECIMAL(8,3)\n", "select least(12::decimal(4,0), 6::decimal(4,3), null::decimal(5,0))");
        assertSqlWithTypes("least\n6.000:DECIMAL(28,3)\n", "select least(12::decimal(24,0), 6::decimal(24,3), null::decimal(25,0))");
        assertSqlWithTypes("least\n6.000:DECIMAL(48,3)\n", "select least(12::decimal(44,0), 6::decimal(44,3), null::decimal(45,0))");
        assertSqlWithTypes("least\n2.0:DECIMAL(2,1)\n", "select least(null::decimal(1,0), 2::decimal(2,1), 3::decimal(1,0))");
        assertSqlWithTypes("least\n1.00:DECIMAL(4,2)\n", "select least(1::decimal(1,0), 12.34::decimal(4,2), 6.7::decimal(2,1))");
        assertSqlWithTypes("least\n0.000:DECIMAL(8,3)\n", "select least(123.456::decimal(6,3), 100::short, null::byte)");
        assertSqlWithTypes("least\n123.45600:DECIMAL(15,5)\n", "select least(123.456::decimal(6,3), 99999::int, 123456.78901::DECIMAL(11,5))");
    }

    @Test
    public void testLeastNumericFunctionFactoryAllNulls() throws Exception {
        assertSqlWithTypes("least\nnull:LONG\n", "select least(null::long, null::long)");
        assertSqlWithTypes("least\nnull:DOUBLE\n", "select least(null::double, null::double)");
    }

    @Test
    public void testLeastNumericFunctionFactoryBytes() throws Exception {
        assertSqlWithTypes("least\n1:BYTE\n", "select least(1::byte, 40::byte)");
        assertSqlWithTypes("least\n1:BYTE\n", "select least(1::byte, 4::byte, 3::byte, 12::byte, 8::byte)");
    }

    @Test
    public void testLeastNumericFunctionFactoryConversions() throws Exception {
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 4)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1, 4L)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1::short, 4L)");
        assertSqlWithTypes("least\n1:SHORT\n", "select least(1::short, 4::byte)");
        assertSqlWithTypes("least\n1.0:FLOAT\n", "select least(1::short, 4f)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(1::short, 4.0)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(1f, 4.0::double)");
        assertSqlWithTypes("least\n1.0:FLOAT\n", "select least(1f, 4::int)");
        // A short has a precision of 5 (32,768), when scaling it to match the decimal, we have a precision of 6 and a
        // scale of 1 -> DECIMAL(6, 1)
        assertSqlWithTypes("least\n1.0:DECIMAL(6,1)\n", "select least(1::decimal(2,1), 4::short)");
        // Doubles takes precedence over decimal has they have a much bigger range
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(4::decimal(2,1), 1d, null::decimal(1,0))");
    }

    @Test
    public void testLeastNumericFunctionFactoryDates() throws Exception {
        assertSqlWithTypes(
                "least\n2020-09-10T00:00:00.000Z:DATE\n",
                "select least('2020-09-10'::date, '2020-09-11'::date)"
        );
        assertSqlWithTypes(
                "least\n2020-09-03T00:00:00.000Z:DATE\n",
                "select least('2020-09-10'::date, '2020-09-11'::date, '2020-09-03'::date)"
        );
    }

    @Test
    public void testLeastNumericFunctionFactoryDoubles() throws Exception {
        assertSqlWithTypes("least\n5.3:DOUBLE\n", "select least(5.3, 9.2)");
        assertSqlWithTypes("least\n3.2:DOUBLE\n", "select least(5.3, 9.2, 6.5, 11.6, 3.2)");
    }

    @Test
    public void testLeastNumericFunctionFactoryFloats() throws Exception {
        assertSqlWithTypes("least\n5.3:FLOAT\n", "select least(5.3f, 9.2f)");
        assertSqlWithTypes("least\n3.2:FLOAT\n", "select least(5.3f, 9.2f, 6.5f, 11.6f, 3.2f)");
    }

    @Test
    public void testLeastNumericFunctionFactoryInts() throws Exception {
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 40)");
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 4, 3, 12, 8)");
    }

    @Test
    public void testLeastNumericFunctionFactoryLongs() throws Exception {
        assertSqlWithTypes("least\n1:LONG\n", "select least(1L, 40L)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1L, 4L, 3L, 12L, 8L)");
    }

    @Test
    public void testLeastNumericFunctionFactoryNulls() throws Exception {
        assertSqlWithTypes("least\n1:LONG\n", "select least(1L, null, 2L)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(null, 1L, 2L)");
        assertSqlWithTypes("least\n1:LONG\n", "select least(1L, 2L, null)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(1.0, null, 2.0)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(null, 1.0, 2.0)");
        assertSqlWithTypes("least\n1.0:DOUBLE\n", "select least(1.0, 2.0, null)");
        // verify that we've cleaned up the counter array after the NULL returned earlier
        assertSqlWithTypes("least\n1:INT\n", "select least(1, 2)");
    }

    @Test
    public void testLeastNumericFunctionFactoryShorts() throws Exception {
        assertSqlWithTypes("least\n1:SHORT\n", "select least(1::short, 40::short)");
        assertSqlWithTypes("least\n1:SHORT\n", "select least(1::short, 4::short, 3::short, 12::short, 8::short)");
    }

    @Test
    public void testLeastNumericFunctionFactoryTimestamps() throws Exception {
        assertSqlWithTypes(
                "least\n2020-09-10T20:00:00.000000Z:TIMESTAMP\n",
                "select least('2020-09-10T20:00:00.000000Z'::timestamp, '2020-09-10T20:01:00.000000Z'::timestamp)"
        );
        assertSqlWithTypes(
                "least\n2020-09-01T20:00:00.000000Z:TIMESTAMP\n",
                "select least('2020-09-10T20:00:00.000000Z'::timestamp, '2020-09-10T20:01:00.000000Z'::timestamp, '2020-09-01T20:00:00.000000Z'::timestamp, null)"
        );
        assertSqlWithTypes(
                "least\n2020-09-10T20:00:00.000000123Z:TIMESTAMP_NS\n",
                "select least('2020-09-10T20:00:00.000000123Z'::timestamp_ns, '2020-09-10T20:01:00.000000123Z'::timestamp_ns, '2020-09-11T20:00:00.000000789Z'::timestamp_ns, null)"
        );
        assertSqlWithTypes(
                "least\n2020-09-10T00:00:00.000000000Z:TIMESTAMP_NS\n",
                "select least('2020-09-10T00:00:00.000Z'::date, '2020-09-10T20:01:00.000000Z'::timestamp, '2020-09-11T20:00:00.000000789Z'::timestamp_ns, null)"
        );
        assertSqlWithTypes(
                "least\n" +
                        "1970-01-01T00:00:00.123456789Z:TIMESTAMP_NS\n",
                "select least('2020-09-10T00:00:00.000Z'::date, '2020-09-10T20:01:00.000000Z'::timestamp, '2020-09-11T20:00:00.000000789Z'::timestamp_ns, null, 123456789L)"
        );
        assertSqlWithTypes(
                "least\n" +
                        "2020-09-10T00:00:00.000000Z:TIMESTAMP\n",
                "select least('2020-09-10T00:00:00.000Z'::date, '2020-09-10T20:01:00.000000Z'::timestamp, '2020-09-11T20:00:00.000000Z'::timestamp, null, 123456789000000000L)"
        );
    }

    @Test
    public void testLeastNumericFunctionFactoryUnsupportedTypes() throws Exception {
        assertException("select least(5, 5.2, 'abc', 2)", 21, "unsupported type");
        assertException("select least(5, 5.2, 'abc'::varchar, 2)", 26, "unsupported type");
    }

    @Test
    public void testLeastNumericFunctionFactoryWith1Arg() throws Exception {
        assertSqlWithTypes("least\n40:LONG\n", "select least(40::long)");
        assertSqlWithTypes("least\n40.2:DOUBLE\n", "select least(40.2::double)");
    }

    @Test
    public void testLeastNumericFunctionFactoryWithData() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_int() a, rnd_int() b from long_sequence(20))");

            assertSqlWithTypes(
                    "least\n" +
                            "-1148479920:INT\n" +
                            "-727724771:INT\n" +
                            "-948263339:INT\n" +
                            "592859671:INT\n" +
                            "-847531048:INT\n" +
                            "-2041844972:INT\n" +
                            "-1575378703:INT\n" +
                            "806715481:INT\n" +
                            "1569490116:INT\n" +
                            "-409854405:INT\n" +
                            "1530831067:INT\n" +
                            "-1532328444:INT\n" +
                            "-1849627000:INT\n" +
                            "-1432278050:INT\n" +
                            "-1792928964:INT\n" +
                            "-1844391305:INT\n" +
                            "-1153445279:INT\n" +
                            "-1715058769:INT\n" +
                            "-1125169127:INT\n" +
                            "-1975183723:INT\n",
                    "select least(a, b) from x"
            );
        });
    }

    @Test
    public void testLeastNumericFunctionFactoryWithNoArgs() throws Exception {
        assertException("select least();", 7, "at least one argument is required ");
    }

    @Test
    public void testMultiLeastFunctionInSingleQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (timestamp TIMESTAMP, symbol SYMBOL, price DOUBLE, amount DOUBLE) TIMESTAMP(timestamp) PARTITION BY DAY;");
            execute(
                    "INSERT INTO x VALUES " +
                            "('2021-10-05T11:31:35.878Z', 'AAPL', 245, 123.4), " +
                            "('2021-10-05T12:31:35.878Z', 'AAPL', 245, 123.3), " +
                            "('2021-10-05T13:31:35.878Z', 'AAPL', 250, 123.1), " +
                            "('2021-10-05T14:31:35.878Z', 'AAPL', 250, 123.0);"
            );

            assertQuery(
                    "least\tleast1\n" +
                            "245.0\t123.2\n" +
                            "245.0\t123.2\n" +
                            "247.0\t123.1\n" +
                            "247.0\t123.0\n",
                    "select least(price, 247), least(amount, 123.2) from x"
            );
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LeastNumericFunctionFactory();
    }
}
