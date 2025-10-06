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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastDoubleToDecimalFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime cast
                    assertSql("QUERY PLAN\n" +
                            "VirtualRecord\n" +
                            "  functions: [value::DECIMAL(5,2)]\n" +
                            "    VirtualRecord\n" +
                            "      functions: [123.45]\n" +
                            "        long_sequence count: 1\n", "EXPLAIN WITH data AS (SELECT 123.45 AS value) SELECT cast(value as DECIMAL(5, 2)) FROM data");

                    // Constant folding
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123.45]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45 as DECIMAL(5, 2))");
                }
        );
    }

    @Test
    public void testCastInfinityAndNaN() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Cast positive infinity should return NULL
                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast('Infinity' as double) as DECIMAL(10,2))"
                    );

                    // Cast negative infinity should return NULL
                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast('-Infinity' as double) as DECIMAL(10,2))"
                    );

                    // Cast NaN should return NULL
                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast('NaN' as double) as DECIMAL(10,2))"
                    );
                }
        );
    }

    @Test
    public void testCastNull() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "\n",
                            "select cast(cast(null as double) as DECIMAL(10,2))"
                    );
                }
        );
    }

    @Test
    public void testCastPrecisionValidation() throws Exception {
        assertMemoryLeak(
                () -> {
                    // 1000.0 requires precision 5 (4 digits + 1 for scale) but only precision 4 allowed
                    assertException(
                            "select cast(1000.0 as DECIMAL(4,2))",
                            12,
                            "decimal '1000.0' requires precision of 6 but is limited to 4"
                    );

                    // 99999.0 should overflow DECIMAL(4)
                    assertException(
                            "select cast(99999.0 as DECIMAL(4,0))",
                            12,
                            "decimal '99999.0' requires precision of 5 but is limited to 4"
                    );
                }
        );
    }

    @Test
    public void testCastScaleValidation() throws Exception {
        assertMemoryLeak(
                () -> {
                    // This should fail because 123.456 has 3 decimal places but target scale is 2
                    assertException(
                            "select cast(123.456 as DECIMAL(5,2))",
                            12,
                            "decimal '123.456' has 3 decimal places but scale is limited to 2"
                    );

                    // This should fail because 0.001 has 3 decimal places but target scale is 2
                    assertException(
                            "select cast(0.001 as DECIMAL(4,2))",
                            12,
                            "decimal '0.001' has 3 decimal places but scale is limited to 2"
                    );
                }
        );
    }

    @Test
    public void testCastSimpleValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Simple positive double
                    assertSql(
                            "cast\n" +
                                    "123.45\n",
                            "select cast(123.45 as DECIMAL(5,2))"
                    );

                    // Simple negative double
                    assertSql(
                            "cast\n" +
                                    "-123.45\n",
                            "select cast(-123.45 as DECIMAL(5,2))"
                    );

                    // Zero
                    assertSql(
                            "cast\n" +
                                    "0.00\n",
                            "select cast(0.0 as DECIMAL(5,2))"
                    );

                    // Integer value with scale
                    assertSql(
                            "cast\n" +
                                    "100.00\n",
                            "select cast(100.0 as DECIMAL(5,2))"
                    );
                }
        );
    }

    @Test
    public void testLossy() throws Exception {
        assertMemoryLeak(() -> {
            // We allow lossy casts from double to decimal when it is explicit
            assertSql(
                    "cast\n" +
                            "123.45\n",
                    "WITH data AS (SELECT 123.456 AS value) SELECT cast(value as DECIMAL(5,2)) FROM data"
            );
        });
    }

    @Test
    public void testRuntimeCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime cast from double column
                    assertSql(
                            "value\tdecimal_value\n" +
                                    "123.45\t123.45\n" +
                                    "-67.89\t-67.89\n" +
                                    "0.0\t0.00\n" +
                                    "null\t\n",
                            "WITH data AS (SELECT 123.45 value UNION ALL SELECT -67.89 UNION ALL SELECT 0.0 UNION ALL SELECT cast('NaN' as double)) " +
                                    "SELECT value, cast(value as DECIMAL(5,2)) as decimal_value FROM data"
                    );

                    // Runtime cast from double column
                    assertSql(
                            """
                                    value\tdecimal_value
                                    123.45\t123.45
                                    -67.89\t-67.89
                                    0.0\t0.00
                                    null\t
                                    """,
                            """
                                    WITH data AS (
                                     SELECT 123.45 value UNION ALL
                                     SELECT -67.89 UNION ALL
                                     SELECT 0.0 UNION ALL
                                     SELECT cast('NaN' as double)
                                    )
                                    SELECT value, cast(value as DECIMAL(25,2)) as decimal_value FROM data
                                    """
                    );

                    // Runtime cast from double column
                    assertSql(
                            """
                                    value\tdecimal_value
                                    123.45\t123.45
                                    -67.89\t-67.89
                                    0.0\t0.00
                                    null\t
                                    """,
                            """
                                    WITH data AS (
                                     SELECT 123.45 value UNION ALL
                                     SELECT -67.89 UNION ALL
                                     SELECT 0.0 UNION ALL
                                     SELECT cast('NaN' as double)
                                    )
                                    SELECT value, cast(value as DECIMAL(55,2)) as decimal_value FROM data
                                    """
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflow() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime cast that would overflow precision
                    assertException(
                            "WITH data AS (SELECT 1000.0 AS value) SELECT cast(value as DECIMAL(4,2)) FROM data",
                            50,
                            "inconvertible value: `1000.0` [DOUBLE -> DECIMAL(4,2)]"
                    );

                    assertException(
                            "with data as (select -1e30d v) select cast(v as decimal(24,2)) from data",
                            43,
                            "inconvertible value: `-1.0E30` [DOUBLE -> DECIMAL(24,2)]"
                    );

                    assertException(
                            "with data as (select 1e36d v) select cast(v as decimal(42,12)) from data",
                            42,
                            "inconvertible value: `1.0E36` [DOUBLE -> DECIMAL(42,12)]"
                    );
                }
        );
    }
}