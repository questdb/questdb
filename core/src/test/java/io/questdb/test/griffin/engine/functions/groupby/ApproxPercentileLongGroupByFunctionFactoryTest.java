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

public class ApproxPercentileLongGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testApprox0thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n1.0\n",
                    "select approx_percentile(x, 0) from test"
            );
        });
    }

    @Test
    public void testApprox100thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n103.0\n",
                    "select approx_percentile(x, 1.0) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileIntValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select cast(x as int) x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n51.0\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileLongValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n51.0\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileLongValuesWith5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n51.0\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    // test increasing level of precision
    @Test
    public void testApprox50thPercentileWithPrecision1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n511.0\n",
                    "select approx_percentile(x, 0.5, 1) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n501.0\n",
                    "select approx_percentile(x, 0.5, 2) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision3() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.0\n",
                    "select approx_percentile(x, 0.5, 3) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.0\n",
                    "select approx_percentile(x, 0.5, 4) from test"
            );
        });
    }

    @Test
    public void testApprox50thPercentileWithPrecision5() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as (select x from long_sequence(1000))");
            assertSql(
                    "approx_percentile\n500.0\n",
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
            execute("create table test as (select 5 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n",
                    "select approx_percentile(x, 0.5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
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
            execute("create table test (x long)");
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
            execute("create table test as (select 5 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n",
                    "select approx_percentile(x, $1, 5) from test"
            );
        });
    }

    @Test
    public void testApproxPercentileSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (null), (null), (null)");
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
            execute("create table test as (select 5 x from long_sequence(100))");
            assertSql(
                    "approx_percentile\n5.0\n",
                    "select approx_percentile(x, $1) from test"
            );
        });
    }

    @Test
    public void testInvalidPercentile1() throws Exception {
        assertException(
                "select approx_percentile(x, 1.1) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentile2() throws Exception {
        assertException(
                "select approx_percentile(x, -1) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentile3() throws Exception {
        assertException(
                "select approx_percentile(x, x) from long_sequence(1)",
                28,
                "percentile must be a constant"
        );
    }

    @Test
    public void testInvalidPercentilePacked1() throws Exception {
        assertException(
                "select approx_percentile(x, 1.1, 5) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentilePacked2() throws Exception {
        assertException(
                "select approx_percentile(x, -1, 5) from long_sequence(1)",
                7,
                "percentile must be between 0.0 and 1.0"
        );
    }

    @Test
    public void testInvalidPercentilePacked3() throws Exception {
        assertException(
                "select approx_percentile(x, x, 5) from long_sequence(1)",
                28,
                "percentile must be a constant"
        );
    }

    @Test
    public void testInvalidPrecision1() throws Exception {
        assertException(
                "select approx_percentile(x, 0.5, 6) from long_sequence(1)",
                33,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testInvalidPrecision2() throws Exception {
        assertException(
                "select approx_percentile(x, 0.5, -1) from long_sequence(1)",
                33,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testThrowsOnNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (x long)");
            execute("insert into test values (1), (-1)");
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
            execute("create table test (x long)");
            execute("insert into test values (1), (-1)");
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
