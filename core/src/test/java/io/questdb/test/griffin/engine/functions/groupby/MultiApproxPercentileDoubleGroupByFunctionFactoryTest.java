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
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MultiApproxPercentileDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertSql(
                    "approx_percentile\nnull\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95]) FROM test"
            );
        });
    }

    @Test
    public void testAllSameValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT 5.0 x FROM long_sequence(100))");
            assertSql(
                    "approx_percentile\n[5.0,5.0,5.0]\n",
                    "SELECT approx_percentile(x, ARRAY[0.25, 0.5, 0.75]) FROM test"
            );
        });
    }

    @Test
    public void testBasicMultiPercentile() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");
            assertSql(
                    "approx_percentile\n[51.9375,103.9375]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 1.0]) FROM test"
            );
        });
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");
            assertSql(
                    "approx_percentile\n[]\n",
                    "SELECT approx_percentile(x, ARRAY[]) FROM test"
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            assertSql(
                    "approx_percentile\nnull\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95]) FROM test"
            );
        });
    }

    @Test
    public void testGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (category SYMBOL, value DOUBLE)");
            execute("""
                    INSERT INTO test VALUES
                    ('A', 1.0), ('A', 2.0), ('A', 3.0), ('A', 4.0), ('A', 5.0),
                    ('B', 10.0), ('B', 20.0), ('B', 30.0), ('B', 40.0), ('B', 50.0)
                    """);
            String result = """
                    category\tapprox_percentile
                    A\t[3.0625,5.1875]
                    B\t[30.5,51.5]
                    """;
            assertSql(
                    result,
                    "SELECT category, approx_percentile(value, ARRAY[0.5, 1.0]) FROM test ORDER BY category"
            );
        });
    }

    @Test
    public void testInvalidPercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::double AS val FROM long_sequence(10))");
            try {
                assertSql(
                        "",
                        "SELECT approx_percentile(val, ARRAY[0.5, 1.5]::DOUBLE[]) FROM test"
                );
                Assert.fail("Expected CairoException for invalid percentile");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid percentile");
            }
        });
    }

    @Test
    public void testNegativePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x::double AS val FROM long_sequence(10))");
            try {
                assertSql(
                        "",
                        "SELECT approx_percentile(val, ARRAY[0.5, -1.5]::DOUBLE[]) FROM test"
                );
                Assert.fail("Expected CairoException for invalid percentile");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid percentile");
            }
        });
    }

    @Test
    public void testInvalidNegativePrecision() throws Exception {
        assertException(
                "SELECT approx_percentile(x::DOUBLE, ARRAY[0.5], -1) FROM long_sequence(1)",
                48,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testInvalidPrecision() throws Exception {
        assertException(
                "SELECT approx_percentile(x::DOUBLE, ARRAY[0.5], 6) FROM long_sequence(1)",
                48,
                "precision must be between 0 and 5"
        );
    }

    @Test
    public void testLongInputAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x LONG)");
            execute("INSERT INTO test VALUES (null), (null), (null)");
            assertSql(
                    "approx_percentile\nnull\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.99]) FROM test"
            );
        });
    }

    @Test
    public void testLongInputHighPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(1000))");
            assertSql(
                    "approx_percentile\n[500.0,950.0]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95], 3) FROM test"
            );
        });
    }

    @Test
    public void testLongInputVariant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(100))");
            assertSql(
                    "approx_percentile\n[51.0,103.0]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 1.0]) FROM test"
            );
        });
    }

    @Test
    public void testLongInputWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT x FROM long_sequence(1000))");
            assertSql(
                    "approx_percentile\n[501.0,951.0]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95], 2) FROM test"
            );
        });
    }

    @Test
    public void testMultiResultsMatchIndividual() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");

            String singleP50 = queryResult("SELECT approx_percentile(x, 0.5) FROM test");
            String singleP95 = queryResult("SELECT approx_percentile(x, 0.95) FROM test");
            String singleP100 = queryResult("SELECT approx_percentile(x, 1.0) FROM test");

            assertSql(
                    "approx_percentile\n[" + singleP50 + "," + singleP95 + "," + singleP100 + "]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95, 1.0]) FROM test"
            );
        });
    }

    @Test
    public void testSinglePercentileInArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(100))");
            assertSql(
                    "approx_percentile\n[51.9375]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5]) FROM test"
            );
        });
    }

    @Test
    public void testSomeNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (null), (null), (null)");
            assertSql(
                    "approx_percentile\n[1.0,1.0]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.99]) FROM test"
            );
        });
    }

    @Test
    public void testThrowsOnNegativeDoubleValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (x DOUBLE)");
            execute("INSERT INTO test VALUES (1.0), (-1.0)");
            try {
                assertSql(
                        "approx_percentile\n[1.0]\n",
                        "SELECT approx_percentile(x, ARRAY[0.5]) FROM test"
                );
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testWithHighPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(1000))");
            assertSql(
                    "approx_percentile\n[500.2490234375,950.4990234375]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95], 3) FROM test"
            );
        });
    }

    @Test
    public void testWithPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test AS (SELECT CAST(x AS DOUBLE) x FROM long_sequence(1000))");
            assertSql(
                    "approx_percentile\n[501.9921875,951.9921875]\n",
                    "SELECT approx_percentile(x, ARRAY[0.5, 0.95], 2) FROM test"
            );
        });
    }

    private String queryResult(String query) throws Exception {
        try (var factory = select(query); var cursor = factory.getCursor(sqlExecutionContext)) {
            if (cursor.hasNext()) {
                return String.valueOf(cursor.getRecord().getDouble(0));
            }
        }
        return "null";
    }
}
