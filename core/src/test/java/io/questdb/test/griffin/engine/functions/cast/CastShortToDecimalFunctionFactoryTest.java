/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.cast;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CastShortToDecimalFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCastExplains() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime value needs scaling
                    assertSql("""
                            QUERY PLAN
                            VirtualRecord
                              functions: [value::DECIMAL(5,2)]
                                VirtualRecord
                                  functions: [123]
                                    long_sequence count: 1
                            """, "EXPLAIN WITH data AS (SELECT 123::short AS value) SELECT cast(value as DECIMAL(5, 2)) FROM data");

                    // Runtime value doesn't need scaling
                    assertSql("""
                            QUERY PLAN
                            VirtualRecord
                              functions: [value::DECIMAL(5,0)]
                                VirtualRecord
                                  functions: [123]
                                    long_sequence count: 1
                            """, "EXPLAIN WITH data AS (SELECT 123::short AS value) SELECT cast(value as DECIMAL(5, 0)) FROM data");

                    assertSql("""
                            QUERY PLAN
                            VirtualRecord
                              functions: [value::DECIMAL(26,0)]
                                VirtualRecord
                                  functions: [123]
                                    long_sequence count: 1
                            """, "EXPLAIN WITH data AS (SELECT 123::short AS value) SELECT cast(value as DECIMAL(26, 0)) FROM data");

                    assertSql("""
                            QUERY PLAN
                            VirtualRecord
                              functions: [value::DECIMAL(55,0)]
                                VirtualRecord
                                  functions: [123]
                                    long_sequence count: 1
                            """, "EXPLAIN WITH data AS (SELECT 123::short AS value) SELECT cast(value as DECIMAL(55, 0)) FROM data");

                    // Expression should be constant folded
                    assertSql("""
                                    QUERY PLAN
                                    VirtualRecord
                                      functions: [1.00]
                                        long_sequence count: 1
                                    """,
                            "EXPLAIN SELECT cast(1::short as DECIMAL(5, 2))");
                }
        );
    }

    @Test
    public void testCastHighScaleLowPrecision() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(0::short as DECIMAL(2,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.00
                                    """);

                    // Any non-zero value should overflow
                    assertQuery("select cast(1::short as DECIMAL(2,2))")
                            .fails(13, "inconvertible value: 1 [SHORT -> DECIMAL(2,2)]");

                    assertQuery("select cast(-1::short as DECIMAL(2,2))")
                            .fails(12, "inconvertible value: -1 [SHORT -> DECIMAL(2,2)]");
                }
        );
    }

    @Test
    public void testCastMaxShortValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Max short value to decimal with sufficient precision
                    assertQuery("select cast(32767::short as DECIMAL(5))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    // Min short value to decimal with sufficient precision
                    assertQuery("select cast(-32768::short as DECIMAL(5))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32768
                                    """);

                    // Max short with scale requires higher precision
                    assertQuery("select cast(32767::short as DECIMAL(7,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767.00
                                    """);
                }
        );
    }

    @Test
    public void testCastOverflowDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(10000::short as DECIMAL(4))")
                            .fails(17, "inconvertible value: 10000 [SHORT -> DECIMAL(4,0)]");

                    assertQuery("select cast(-10000::short as DECIMAL(4))")
                            .fails(12, "inconvertible value: -10000 [SHORT -> DECIMAL(4,0)]");
                }
        );
    }

    @Test
    public void testCastOverflowDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(128::short as DECIMAL(2))")
                            .fails(15, "inconvertible value: 128 [SHORT -> DECIMAL(2,0)]");

                    assertQuery("select cast(-129::short as DECIMAL(2))")
                            .fails(12, "inconvertible value: -129 [SHORT -> DECIMAL(2,0)]");
                }
        );
    }

    @Test
    public void testCastOverflowWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    // 100 with scale 2 requires precision of at least 5 (100.00)
                    assertQuery("select cast(100::short as DECIMAL(4,2))")
                            .fails(15, "inconvertible value: 100 [SHORT -> DECIMAL(4,2)]");

                    // 1000 with scale 3 requires precision of at least 7 (1000.000)
                    assertQuery("select cast(1000::short as DECIMAL(5,3))")
                            .fails(16, "inconvertible value: 1000 [SHORT -> DECIMAL(5,3)]");
                }
        );
    }

    @Test
    public void testCastSimpleValues() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(123::short as DECIMAL(5,0))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123
                                    """);

                    assertQuery("select cast(-123::short as DECIMAL(5,0))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -123
                                    """);

                    assertQuery("select cast(0::short as DECIMAL(5,0))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal128() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(32767::short as DECIMAL(19))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(-32768::short as DECIMAL(19))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32768
                                    """);

                    assertQuery("select cast(cast(null as short) as DECIMAL(19))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal16() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(9999::short as DECIMAL(4))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    9999
                                    """);

                    assertQuery("select cast(-9999::short as DECIMAL(4))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -9999
                                    """);

                    assertQuery("select cast(cast(null as short) as DECIMAL(4))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal256() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(32767::short as DECIMAL(40))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(-32768::short as DECIMAL(40))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32768
                                    """);

                    assertQuery("select cast(cast(null as short) as DECIMAL(40))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal32() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(32767::short as DECIMAL(9))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(-32768::short as DECIMAL(9))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32768
                                    """);

                    assertQuery("select cast(cast(null as short) as DECIMAL(9))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal64() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(32767::short as DECIMAL(18))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    32767
                                    """);

                    assertQuery("select cast(-32768::short as DECIMAL(18))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -32768
                                    """);

                    assertQuery("select cast(cast(null as short) as DECIMAL(18))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastToDecimal8() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(99::short as DECIMAL(2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    99
                                    """);

                    assertQuery("select cast(-99::short as DECIMAL(2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99
                                    """);

                    assertQuery("select cast(cast(null as short) as DECIMAL(2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0
                                    """);
                }
        );
    }

    @Test
    public void testCastWithScale() throws Exception {
        assertMemoryLeak(
                () -> {
                    assertQuery("select cast(123::short as DECIMAL(5,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    123.00
                                    """);

                    assertQuery("select cast(-99::short as DECIMAL(4,2))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    -99.00
                                    """);

                    // 0 is null for short
                    assertQuery("select cast(0::short as DECIMAL(5,3))")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    cast
                                    0.000
                                    """);
                }
        );
    }

    @Test
    public void testImplicitCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Constant folded
                    assertQuery("select 1234::short + 1234m")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    2468
                                    """);

                    // Runtime discovered
                    assertQuery("with data as (select 1234::short x) select x + 1234m from data")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column
                                    2468
                                    """);
                }
        );
    }

    @Test
    public void testRuntimeCastOverflow() throws Exception {
        assertMemoryLeak(
                () -> {
                    // Runtime overflow for DECIMAL(2)
                    assertQuery("WITH data AS (SELECT 128::short AS value) SELECT cast(value as DECIMAL(2)) FROM data")
                            .fails(54, "inconvertible value: 128 [SHORT -> DECIMAL(2,0)]");

                    // Runtime overflow for DECIMAL(4,2)
                    assertQuery("WITH data AS (SELECT 100::short AS value) SELECT cast(value as DECIMAL(4,2)) FROM data")
                            .fails(54, "inconvertible value: 100 [SHORT -> DECIMAL(4,2)]");
                }
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 99::short value UNION ALL SELECT -99::short UNION ALL SELECT 12::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(4,2)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                99\t99.00
                                -99\t-99.00
                                12\t12.00
                                0\t0.00
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 1000::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(25,10)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767.0000000000
                                -32768\t-32768.0000000000
                                1000\t1000.0000000000
                                0\t0.0000000000
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 1000::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(50,14)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767.00000000000000
                                -32768\t-32768.00000000000000
                                1000\t1000.00000000000000
                                0\t0.00000000000000
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 9999::short value UNION ALL SELECT -9999::short UNION ALL SELECT 500::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(7,3)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                9999\t9999.000
                                -9999\t-9999.000
                                500\t500.000
                                0\t0.000
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 1500::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(12,5)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767.00000
                                -32768\t-32768.00000
                                1500\t1500.00000
                                0\t0.00000
                                """)
        );
    }

    @Test
    public void testRuntimeCastScaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 9::short value UNION ALL SELECT -9::short UNION ALL SELECT 1::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(2,1)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                9\t9.0
                                -9\t-9.0
                                1\t1.0
                                0\t0.0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal128() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 1000::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(20)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767
                                -32768\t-32768
                                1000\t1000
                                0\t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal16() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 999::short value UNION ALL SELECT -999::short UNION ALL SELECT 50::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(3)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                999\t999
                                -999\t-999
                                50\t50
                                0\t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal256() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 1000::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(40)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767
                                -32768\t-32768
                                1000\t1000
                                0\t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal32() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 750::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(5)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767
                                -32768\t-32768
                                750\t750
                                0\t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal64() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 32767::short value UNION ALL SELECT -32768::short UNION ALL SELECT 1000::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(10)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                32767\t32767
                                -32768\t-32768
                                1000\t1000
                                0\t0
                                """)
        );
    }

    @Test
    public void testRuntimeCastUnscaledDecimal8() throws Exception {
        assertMemoryLeak(
                () -> assertQuery("WITH data AS (SELECT 99::short value UNION ALL SELECT -99::short UNION ALL SELECT 1::short UNION ALL SELECT 0::short) " +
                                "SELECT value, cast(value as DECIMAL(2)) as decimal_value FROM data")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                value\tdecimal_value
                                99\t99
                                -99\t-99
                                1\t1
                                0\t0
                                """)
        );
    }
}