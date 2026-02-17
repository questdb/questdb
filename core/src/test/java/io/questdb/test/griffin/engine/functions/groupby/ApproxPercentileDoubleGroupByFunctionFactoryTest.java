/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class ApproxPercentileDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testApprox0thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n1.0\n",
                    "select approx_percentile(x, 0) from test"
            );
        });
    }

    @Test
    public void testApprox100thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n103.9375\n",
                    "select approx_percentile(x, 1.0) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n51.9375\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileDoubleValuesWith5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n51.9375\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as float) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n51.9375\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    // test increasing level of precision
    @Test
    public void testApprox50thPercentileWithPrecision1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n511.9375\n",
                    "select approx_percentile(x, 0.5, 1) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n501.9921875\n",
                    "select approx_percentile(x, 0.5, 2) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.2490234375\n",
                    "select approx_percentile(x, 0.5, 3) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.01556396484375\n",
                    "select approx_percentile(x, 0.5, 4) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as double) x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.00194549560547\n",
                    "select approx_percentile(x, 0.5, 5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    """
                            approx_percentile
                            null
                            """,
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    """
                            approx_percentile
                            null
                            """,
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentilePackedAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (null), (null), (null)");
            assertSql(
                    """
                            approx_percentile
                            null
                            """,
                    "select approx_percentile(x, 0.5, 5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentilePackedEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            assertSql(
                    """
                            approx_percentile
                            null
                            """,
                    "select approx_percentile(x, 0.5, 5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentilePackedWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n",
                    "select approx_percentile(x, $1, 5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (null), (null), (null)");
            assertSql(
                    """
                            approx_percentile
                            1.0
                            """,
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileWithPercentileBindVariable() throws Exception {
        bindVariableService.setDouble(0, 0.5);
        assertMemoryLeak(() -> {
            execute("create table test as (select 5.0 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n",
                    "select approx_percentile(x, $1) from test"
            );
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "select approx_percentile(x::double, 1.1) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "select approx_percentile(x::double, -1) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentile3() throws Exception {
        assertException(
                "select approx_percentile(x::double, x::double) from long_sequence(1)",
                37,
                "percentile must be a constant"
        );
    }

    @Test
    public void testInvalidPercentilePacked1() throws Exception {
        assertException(
                "select approx_percentile(x::double, 1.1, 5) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentilePacked2() throws Exception {
        assertException(
                "select approx_percentile(x::double, -1, 5) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentilePacked3() throws Exception {
        assertException(
                "select approx_percentile(x::double, x::double, 5) from long_sequence(1)",
                37,
                "percentile must be a constant"
        );
    }

    @Test
    public void testInvalidPrecision1() throws Exception {
        assertException(
                "select approx_percentile(x::double, 0.5, 6) from long_sequence(1)",
                41,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testInvalidPrecision2() throws Exception {
        assertException(
                "select approx_percentile(x::double, 0.5, -1) from long_sequence(1)",
                41,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testThrowsOnNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0)");
            try {
                assertSql(
                        """
                                approx_percentile
                                1.0
                                """,
                        "select approx_percentile(x, 0.5) from test"
                );
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testThrowsOnNegativeValuesPacked() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x double)");
            execute("insert into test values (1.0), (-1.0)");
            try {
                assertSql(
                        """
                                approx_percentile
                                1.0
                                """,
                        "select approx_percentile(x, 0.5, 5) from test"
                );
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }
}
