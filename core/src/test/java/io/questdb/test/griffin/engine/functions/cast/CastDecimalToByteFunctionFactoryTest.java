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

public class CastDecimalToByteFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastDecimalWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Truncates decimal part
                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.45m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "123\n",
                            "select cast(123.99m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.45m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123.99m as byte)"
                    );

                    // Zero with decimal places
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.99m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.01m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(-0.99m as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value with scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123.45]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123.45m AS value) SELECT cast(value as byte) FROM data");

                    // Runtime value without scale
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [123]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT 123m AS value) SELECT cast(value as byte) FROM data");

                    // Expression should be constant folded
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as byte)");
                }
        );
    }

    @Test
    public void testCastExplainsForDifferentDecimalTypes() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99m as DECIMAL(2)) AS value) SELECT cast(value as byte) FROM data");

                    // DECIMAL16 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [127]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(127m as DECIMAL(3)) AS value) SELECT cast(value as byte) FROM data");

                    // DECIMAL32 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [127]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(127m as DECIMAL(3)) AS value) SELECT cast(value as byte) FROM data");

                    // DECIMAL64 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [127]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(127m as DECIMAL(10)) AS value) SELECT cast(value as byte) FROM data");

                    // DECIMAL128 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [127]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(127m as DECIMAL(19)) AS value) SELECT cast(value as byte) FROM data");

                    // DECIMAL256 unscaled
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [127]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(127m as DECIMAL(40)) AS value) SELECT cast(value as byte) FROM data");

                    // With scale - tests ScaledDecimalFunction
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [value::byte]\n" +
                                    "    VirtualRecord\n" +
                                    "      functions: [99.50]\n" +
                                    "        long_sequence count: 1\n",
                            "EXPLAIN WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) AS value) SELECT cast(value as byte) FROM data");

                    // Constant folding for all decimal types
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99m as byte)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [127]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(127m as byte)");

                    // Constant folding with scale (truncation)
                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [123]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(123.45m as byte)");

                    assertSql("QUERY PLAN\n" +
                                    "VirtualRecord\n" +
                                    "  functions: [99]\n" +
                                    "    long_sequence count: 1\n",
                            "EXPLAIN SELECT cast(99.99m as byte)");
                }
        );
    }

    @Test
    public void testCastFromDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(cast(127m as DECIMAL(19)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-127\n",
                            "select cast(cast(-127m as DECIMAL(19)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(19)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(cast(127m as DECIMAL(4)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-127\n",
                            "select cast(cast(-127m as DECIMAL(4)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(4)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(cast(127m as DECIMAL(40)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-127\n",
                            "select cast(cast(-127m as DECIMAL(40)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(40)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(cast(127m as DECIMAL(9)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-127\n",
                            "select cast(cast(-127m as DECIMAL(9)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(9)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(cast(127m as DECIMAL(18)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-127\n",
                            "select cast(cast(-127m as DECIMAL(18)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(18)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastFromDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "99\n",
                            "select cast(cast(99m as DECIMAL(2)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(cast(-99m as DECIMAL(2)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(0m as DECIMAL(2)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(cast(null as DECIMAL(2)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastMaxByteValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max byte value from decimal
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(127m as byte)"
                    );

                    // Max byte value minus 1
                    assertSql(
                            "cast\n" +
                                    "126\n",
                            "select cast(126m as byte)"
                    );

                    // Min byte value + 1 (to avoid overflow with negation)
                    assertSql(
                            "cast\n" +
                                    "-127\n",
                            "select cast(-127m as byte)"
                    );

                    // With scale - truncated
                    assertSql(
                            "cast\n" +
                                    "127\n",
                            "select cast(127.99m as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastNegativeValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-123\n",
                            "select cast(-123m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(-99m as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for byte
                    assertException(
                            "select cast(cast(2147483647m as DECIMAL(19)) as byte)",
                            7,
                            "inconvertible value: 2147483647 [DECIMAL(19,0) -> BYTE]"
                    );

                    assertException(
                            "select cast(cast(-2147483648m as DECIMAL(19)) as byte)",
                            7,
                            "inconvertible value: -2147483648 [DECIMAL(19,0) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for byte
                    assertException(
                            "select cast(cast(128m as DECIMAL(3)) as byte)",
                            7,
                            "inconvertible value: 128 [DECIMAL(3,0) -> BYTE]"
                    );

                    assertException(
                            "select cast(cast(-129m as DECIMAL(3)) as byte)",
                            7,
                            "inconvertible value: -129 [DECIMAL(3,0) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for byte
                    assertException(
                            "select cast(cast(999999999999m as DECIMAL(40)) as byte)",
                            7,
                            "inconvertible value: 999999999999 [DECIMAL(40,0) -> BYTE]"
                    );

                    assertException(
                            "select cast(cast(-999999999999m as DECIMAL(40)) as byte)",
                            7,
                            "inconvertible value: -999999999999 [DECIMAL(40,0) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for byte
                    assertException(
                            "select cast(cast(1000m as DECIMAL(4)) as byte)",
                            7,
                            "inconvertible value: 1000 [DECIMAL(4,0) -> BYTE]"
                    );

                    assertException(
                            "select cast(cast(-1000m as DECIMAL(4)) as byte)",
                            7,
                            "inconvertible value: -1000 [DECIMAL(4,0) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Value too large for byte
                    assertException(
                            "select cast(cast(99999m as DECIMAL(18)) as byte)",
                            7,
                            "inconvertible value: 99999 [DECIMAL(18,0) -> BYTE]"
                    );

                    assertException(
                            "select cast(cast(-99999m as DECIMAL(18)) as byte)",
                            7,
                            "inconvertible value: -99999 [DECIMAL(18,0) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    // DECIMAL8 can hold max 99, which fits in BYTE (max 127)
                    assertSql(
                            "cast\n" +
                                    "99\n",
                            "select cast(cast(99m as DECIMAL(2)) as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-99\n",
                            "select cast(cast(-99m as DECIMAL(2)) as byte)"
                    );
                }
        );
    }

    @Test
    public void testCastZeroValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.0m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.00m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "0\n",
                            "select cast(0.000m as byte)"
                    );
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // No implicit cast from decimal to byte in arithmetic
                    // Must use explicit cast
                    assertSql(
                            "column\n" +
                                    "199\n",
                            "select cast(99m as byte) + cast(100 as byte)"
                    );

                    // Runtime conversion
                    assertSql(
                            "column\n" +
                                    "199\n",
                            "with data as (select 99m x) select cast(x as byte) + cast(100 as byte) from data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowScaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow with scaled decimal
                    assertException(
                            "WITH data AS (SELECT cast(128.5m as DECIMAL(4,1)) AS value) SELECT cast(value as byte) FROM data",
                            67,
                            "inconvertible value: 128.5 [DECIMAL(4,1) -> BYTE]"
                    );

                    assertException(
                            "WITH data AS (SELECT cast(-129.5m as DECIMAL(4,1)) AS value) SELECT cast(value as byte) FROM data",
                            68,
                            "inconvertible value: -129.5 [DECIMAL(4,1) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastOverflowUnscaled() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL16
                    assertException(
                            "WITH data AS (SELECT cast(128m as DECIMAL(3)) AS value) SELECT cast(value as byte) FROM data",
                            63,
                            "inconvertible value: 128 [DECIMAL(3,0) -> BYTE]"
                    );

                    // Runtime overflow for DECIMAL32
                    assertException(
                            "WITH data AS (SELECT cast(1000m as DECIMAL(5)) AS value) SELECT cast(value as byte) FROM data",
                            64,
                            "inconvertible value: 1000 [DECIMAL(5,0) -> BYTE]"
                    );

                    // Runtime overflow for DECIMAL64
                    assertException(
                            "WITH data AS (SELECT cast(99999m as DECIMAL(18)) AS value) SELECT cast(value as byte) FROM data",
                            66,
                            "inconvertible value: 99999 [DECIMAL(18,0) -> BYTE]"
                    );

                    // Runtime overflow for DECIMAL128
                    assertException(
                            "WITH data AS (SELECT cast(2147483647m as DECIMAL(19)) AS value) SELECT cast(value as byte) FROM data",
                            71,
                            "inconvertible value: 2147483647 [DECIMAL(19,0) -> BYTE]"
                    );

                    // Runtime overflow for DECIMAL256
                    assertException(
                            "WITH data AS (SELECT cast(999999999999m as DECIMAL(40)) AS value) SELECT cast(value as byte) FROM data",
                            73,
                            "inconvertible value: 999999999999 [DECIMAL(40,0) -> BYTE]"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "126.99\t126\n" +
                                    "-126.99\t-126\n" +
                                    "123.67\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(126.99m as DECIMAL(20,2)) value " +
                                    "UNION ALL SELECT cast(-126.99m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(123.67m as DECIMAL(20,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(20,2))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "99.50\t99\n" +
                                    "-99.50\t-99\n" +
                                    "12.99\t12\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(99.5m as DECIMAL(4,2)) value " +
                                    "UNION ALL SELECT cast(-99.5m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(12.99m as DECIMAL(4,2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4,2))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "126.9999999999\t126\n" +
                                    "-126.9999999999\t-126\n" +
                                    "123.1234567890\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(126.9999999999m as DECIMAL(40,10)) value " +
                                    "UNION ALL SELECT cast(-126.9999999999m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(123.1234567890m as DECIMAL(40,10)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40,10))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "126.999\t126\n" +
                                    "-126.999\t-126\n" +
                                    "123.678\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(126.999m as DECIMAL(6,3)) value " +
                                    "UNION ALL SELECT cast(-126.999m as DECIMAL(6,3)) " +
                                    "UNION ALL SELECT cast(123.678m as DECIMAL(6,3)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(6, 3))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "126.999999\t126\n" +
                                    "-126.999999\t-126\n" +
                                    "123.678901\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(126.999999m as DECIMAL(9,6)) value " +
                                    "UNION ALL SELECT cast(-126.999999m as DECIMAL(9,6)) " +
                                    "UNION ALL SELECT cast(123.678901m as DECIMAL(9,6)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9, 6))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "9.9\t9\n" +
                                    "-9.9\t-9\n" +
                                    "0.5\t0\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(9.9m as DECIMAL(2,1)) value " +
                                    "UNION ALL SELECT cast(-9.9m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(0.5m as DECIMAL(2,1)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2,1))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "127\t127\n" +
                                    "-127\t-127\n" +
                                    "123\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(127m as DECIMAL(19)) value " +
                                    "UNION ALL SELECT cast(-127m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(123m as DECIMAL(19)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(19))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "127\t127\n" +
                                    "-127\t-127\n" +
                                    "123\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(127m as DECIMAL(4)) value " +
                                    "UNION ALL SELECT cast(-127m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(123m as DECIMAL(4)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(4))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "127\t127\n" +
                                    "-127\t-127\n" +
                                    "123\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(127m as DECIMAL(40)) value " +
                                    "UNION ALL SELECT cast(-127m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(123m as DECIMAL(40)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(40))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "127\t127\n" +
                                    "-127\t-127\n" +
                                    "123\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(127m as DECIMAL(9)) value " +
                                    "UNION ALL SELECT cast(-127m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(123m as DECIMAL(9)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(9))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "127\t127\n" +
                                    "-127\t-127\n" +
                                    "123\t123\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(127m as DECIMAL(18)) value " +
                                    "UNION ALL SELECT cast(-127m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(123m as DECIMAL(18)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(18))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertSql(
                            "value\tbyte_value\n" +
                                    "99\t99\n" +
                                    "-99\t-99\n" +
                                    "0\t0\n" +
                                    "\t0\n",
                            "WITH data AS (SELECT cast(99m as DECIMAL(2)) value " +
                                    "UNION ALL SELECT cast(-99m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(0m as DECIMAL(2)) " +
                                    "UNION ALL SELECT cast(null as DECIMAL(2))) " +
                                    "SELECT value, cast(value as byte) as byte_value FROM data"
                    );
                }
        );
    }

    @Test
    public void testTruncationBehavior() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Positive values - truncate towards zero
                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.1m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.5m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "1\n",
                            "select cast(1.9m as byte)"
                    );

                    // Negative values - truncate towards zero
                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.1m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.5m as byte)"
                    );

                    assertSql(
                            "cast\n" +
                                    "-1\n",
                            "select cast(-1.9m as byte)"
                    );
                }
        );
    }
}