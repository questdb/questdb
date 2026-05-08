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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ArrayAggDoubleArrayGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNullArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (null),
                    (null)
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "null\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testBufferGrowth() throws Exception {
        // Concatenate enough single-element arrays to force the underlying
        // pair buffer to outgrow INITIAL_CAPACITY (16) several times. Asserts
        // that growth preserves null elements and keeps insertion order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            StringBuilder insert = new StringBuilder("INSERT INTO tab VALUES\n");
            StringBuilder expected = new StringBuilder("agg\n[");
            for (int i = 0; i < 100; i++) {
                if (i > 0) {
                    insert.append(",\n");
                    expected.append(',');
                }
                if (i == 0 || i == 16 || i == 99) {
                    insert.append("(ARRAY[null::double])");
                    expected.append("null");
                } else {
                    insert.append("(ARRAY[").append(i).append(".0])");
                    expected.append(i).append(".0");
                }
            }
            execute(insert.toString());
            expected.append("]\n");
            assertQueryNoLeakCheck(
                    expected.toString(),
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testRenderCacheReuseOnRepeatedGetArray() throws Exception {
        // getArray() renders a fresh allocator-backed flat buffer the first time
        // a given group is read and caches the (srcPtr -> renderPtr) mapping per
        // instance. Project the aggregate alongside derivations so the outer
        // expression reads it more than once on the same group, exercising the
        // cache-hit path for the second and third reads.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', ARRAY[1.0, 2.0]),
                    ('a', ARRAY[3.0, 4.0, 5.0]),
                    ('b', ARRAY[10.0]),
                    ('b', ARRAY[20.0, 30.0])
                    """);
            assertQueryNoLeakCheck(
                    "grp\tagg\tcnt\tsum\n" +
                            "a\t[1.0,2.0,3.0,4.0,5.0]\t5\t15.0\n" +
                            "b\t[10.0,20.0,30.0]\t3\t60.0\n",
                    "SELECT grp, agg, array_count(agg) cnt, array_sum(agg) sum " +
                            "FROM (SELECT grp, array_agg(arr) agg FROM tab) ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testConcatenation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.0, 2.0]),
                    (ARRAY[3.0, 4.0]),
                    (ARRAY[5.0])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,2.0,3.0,4.0,5.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testDifferentSizedArrays() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.0]),
                    (ARRAY[2.0, 3.0, 4.0]),
                    (ARRAY[5.0, 6.0])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,2.0,3.0,4.0,5.0,6.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyArraysSkipped() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.0, 2.0]),
                    (ARRAY[]),
                    (ARRAY[3.0, 4.0])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,2.0,3.0,4.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "null\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupByCompoundKey() throws Exception {
        // GROUP BY a, b uses a composite map key encoding distinct from the
        // single-symbol path covered by testGroupByKeyed. Verify that the
        // per-group buffer pointer is correctly resolved through a multi-key
        // map and that elements within each composite group concatenate in
        // insertion order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (region SYMBOL, country SYMBOL, arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    ('eu', 'fr', ARRAY[1.0, 2.0]),
                    ('eu', 'de', ARRAY[10.0]),
                    ('eu', 'fr', ARRAY[3.0]),
                    ('na', 'us', ARRAY[100.0, 200.0]),
                    ('eu', 'de', ARRAY[20.0, 30.0])
                    """);
            assertQueryNoLeakCheck(
                    "region\tcountry\tagg\n" +
                            "eu\tde\t[10.0,20.0,30.0]\n" +
                            "eu\tfr\t[1.0,2.0,3.0]\n" +
                            "na\tus\t[100.0,200.0]\n",
                    "SELECT region, country, array_agg(arr) agg FROM tab GROUP BY region, country ORDER BY region, country",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', ARRAY[1.0, 2.0]),
                    ('a', ARRAY[3.0]),
                    ('b', ARRAY[10.0, 20.0]),
                    ('b', ARRAY[30.0, 40.0, 50.0])
                    """);
            assertQueryNoLeakCheck(
                    "grp\tagg\n" +
                            "a\t[1.0,2.0,3.0]\n" +
                            "b\t[10.0,20.0,30.0,40.0,50.0]\n",
                    "SELECT grp, array_agg(arr) agg FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByNullKey() throws Exception {
        // A NULL grouping key must form its own group rather than being silently
        // dropped or coerced into the empty-string group.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', ARRAY[1.0, 2.0]),
                    (null, ARRAY[3.0]),
                    ('a', ARRAY[4.0]),
                    (null, ARRAY[5.0, 6.0])
                    """);
            assertQueryNoLeakCheck(
                    "grp\tagg\n" +
                            "\t[3.0,5.0,6.0]\n" +
                            "a\t[1.0,2.0,4.0]\n",
                    "SELECT grp, array_agg(arr) agg FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMaxArrayElementCountExceeded() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_ARRAY_ELEMENT_COUNT, 5);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.0, 2.0, 3.0]),
                    (ARRAY[4.0, 5.0, 6.0])
                    """);
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(arr) FROM tab",
                    0,
                    "array_agg: array size exceeds configured maximum [maxArrayElementCount=5]"
            );
        });
    }

    @Test
    public void testMergeTimeCardinalityExceeded() throws Exception {
        // Run on an explicit 4-worker pool so per-worker subtotals stay below the
        // 49_999-element cap (each worker sees a fraction of 50_000 elements)
        // while the merged total reliably crosses it. This forces the merge-time
        // checkCapacityLimit() to fire rather than falling through to the
        // computeNext check.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_MAX_ARRAY_ELEMENT_COUNT, 49_999);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (arr DOUBLE[])", sqlExecutionContext);
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    int base = i * 5;
                    sb.append("(ARRAY[")
                            .append(base).append(".0,")
                            .append(base + 1).append(".0,")
                            .append(base + 2).append(".0,")
                            .append(base + 3).append(".0,")
                            .append(base + 4).append(".0])");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                try {
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "SELECT array_agg(arr) FROM tab",
                            sink,
                            ""
                    );
                    org.junit.Assert.fail("expected CairoException with maxArrayElementCount=49999");
                } catch (io.questdb.cairo.CairoException ex) {
                    TestUtils.assertContains(ex.getMessage(),
                            "array_agg: array size exceeds configured maximum [maxArrayElementCount=49999]");
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testMixedWithOtherAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[], val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', ARRAY[1.0, 2.0], 10.0),
                    ('a', ARRAY[3.0], 20.0),
                    ('b', ARRAY[4.0, 5.0], 30.0)
                    """);
            assertQueryNoLeakCheck(
                    "grp\tagg\tavg\n" +
                            "a\t[1.0,2.0,3.0]\t15.0\n" +
                            "b\t[4.0,5.0]\t30.0\n",
                    "SELECT grp, array_agg(arr) agg, avg(val) avg FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNonVanilla1DInput() throws Exception {
        // transpose(m)[1] selects the first row of a transposed 2x2 matrix, which
        // is a non-vanilla 1D view (stride=2 over the 4-element backing store).
        // copyArrayElements must apply the stride when reading; otherwise it
        // reads physical memory in order and silently drops the semantics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (m DOUBLE[][])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[[10.0, 20.0], [30.0, 40.0]]),
                    (ARRAY[[50.0, 60.0], [70.0, 80.0]])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[10.0,30.0,50.0,70.0]\n",
                    "SELECT array_agg(transpose(m)[1]) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNullArraysSkipped() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.0, 2.0]),
                    (null),
                    (ARRAY[3.0, 4.0])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,2.0,3.0,4.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNullElementsPreserved() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.0, null]),
                    (ARRAY[null, 4.0])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,null,null,4.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNullFirstThenNonNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (null),
                    (null),
                    (ARRAY[1.0, 2.0]),
                    (ARRAY[3.0])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,2.0,3.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testParallelCounts() throws Exception {
        // Run on an explicit 4-worker pool so the parallel merge path is reliably
        // exercised regardless of the test JVM's default worker count.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])", sqlExecutionContext);
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 1000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    sb.append("('g").append(i % 5).append("', ARRAY[").append(i).append(".0, ").append(i + 1).append(".0])");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                // 200 arrays * 2 elements each = 400 elements per group
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_count(array_agg(arr)) cnt, array_sum(array_agg(arr)) total FROM tab ORDER BY grp",
                        sink,
                        "grp\tcnt\ttotal\n" +
                                "g0\t400\t199200.0\n" +
                                "g1\t400\t199600.0\n" +
                                "g2\t400\t200000.0\n" +
                                "g3\t400\t200400.0\n" +
                                "g4\t400\t200800.0\n"
                );
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelOrdering() throws Exception {
        // Run on an explicit 4-worker pool so the merge-sort in merge() is
        // reliably exercised regardless of the test JVM's default worker count.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])", sqlExecutionContext);
                // 10 groups x 1000 single-element arrays. Row i goes to group g(i%10) with ARRAY[i.0].
                // Group gN receives elements N.0, (N+10).0, ..., (N+9990).0 in insertion order.
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    sb.append("('g").append(i % 10).append("', ARRAY[").append(i).append(".0])");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                // Build expected: group gN has elements N.0, (N+10).0, ..., (N+9990).0.
                StringBuilder expected = new StringBuilder("grp\tagg\n");
                for (int g = 0; g < 10; g++) {
                    expected.append('g').append(g).append('\t').append('[');
                    for (int j = 0; j < 1_000; j++) {
                        if (j > 0) {
                            expected.append(',');
                        }
                        expected.append(g + j * 10).append(".0");
                    }
                    expected.append("]\n");
                }
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_agg(arr) agg FROM tab ORDER BY grp",
                        sink,
                        expected
                );
            }, configuration, LOG);
        });
    }

    @Test
    public void testRejects2DInput() throws Exception {
        // array_agg(D[]) accepts only 1D input; the factory signature matches any
        // DOUBLE array dimensionality (element-type-only match), so the factory
        // must reject non-1D arrays explicitly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[][])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
                    """);
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(arr) FROM tab",
                    17,
                    "array is not one-dimensional"
            );
        });
    }

    @Test
    public void testRejects2DTransposedInput() throws Exception {
        // transpose() preserves dimensionality, so a transposed 2D is still 2D
        // and must be rejected at factory bind time just like a direct 2D input.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[][])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
                    """);
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(transpose(arr)) FROM tab",
                    17,
                    "array is not one-dimensional"
            );
        });
    }

    @Test
    public void testRejectsBindVariableArg() throws Exception {
        // The factory rejects array bind variables with weak dimensionality (PGWire
        // bind metadata declares array element type but not dim count). Calling
        // bindVariableService.define() with a weak-dim array type for the placeholder
        // mirrors the PGWire dispatch path that hits the dims == -1 branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            bindVariableService.define(0, ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, false), 0);
            assertExceptionNoLeakCheck(
                    "SELECT array_agg($1) FROM tab",
                    17,
                    "array bind variable argument is not supported"
            );
        });
    }

    @Test
    public void testRejectsDistinctModifier() throws Exception {
        // array_agg(DISTINCT x) is not supported. ExpressionParser only rewrites
        // DISTINCT for count() and string_agg(); for array_agg the keyword leaks
        // through to the function call and must be rejected with a clear error.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(DISTINCT arr) FROM tab",
                    26,
                    "dangling literal"
            );
        });
    }

    @Test
    public void testRejectsOrderByInsideAggregate() throws Exception {
        // array_agg(x ORDER BY y) is PostgreSQL syntax. QuestDB only handles ORDER BY
        // inside string_distinct_agg(), so for array_agg it must be rejected rather
        // than silently dropped, otherwise users would get a non-deterministic order
        // without any indication that ORDER BY was ignored.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[])");
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(arr ORDER BY ts) FROM tab",
                    21,
                    "dangling literal"
            );
        });
    }

    @Test
    public void testRejectsWindowOver() throws Exception {
        // array_agg is a GROUP BY function, not a window function. Using it with
        // OVER() must error rather than silently producing wrong results.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(arr) OVER () FROM tab",
                    7,
                    "non-window function called in window context"
            );
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0, 2.0]),
                    ('2024-01-01T00:30:00', ARRAY[3.0]),
                    ('2024-01-01T01:00:00', ARRAY[4.0, 5.0]),
                    ('2024-01-01T01:30:00', ARRAY[6.0])
                    """);
            assertQueryNoLeakCheck(
                    "ts\tagg\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]\n" +
                            "2024-01-01T01:00:00.000000Z\t[4.0,5.0,6.0]\n",
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h ALIGN TO CALENDAR",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSampleByFillLinearRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, grp SYMBOL, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', 'a', ARRAY[1.0])");
            assertExceptionNoLeakCheck(
                    "SELECT ts, grp, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(LINEAR)",
                    16,
                    "support for LINEAR fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFillNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0, 2.0]),
                    ('2024-01-01T00:30:00', ARRAY[3.0]),
                    ('2024-01-01T00:45:00', null),
                    ('2024-01-01T03:00:00', ARRAY[4.0, 5.0]),
                    ('2024-01-01T06:00:00', ARRAY[6.0])
                    """);
            // Two gaps (01:00-03:00 and 04:00-06:00) must be omitted.
            // Null array at 00:45 is skipped by array_agg, not by FILL(NONE).
            assertQueryNoLeakCheck(
                    "ts\tagg\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]\n" +
                            "2024-01-01T03:00:00.000000Z\t[4.0,5.0]\n" +
                            "2024-01-01T06:00:00.000000Z\t[6.0]\n",
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(NONE)",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0, 2.0]),
                    ('2024-01-01T02:00:00', ARRAY[3.0, 4.0])
                    """);
            assertQueryNoLeakCheck(
                    "ts\tagg\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0]\n" +
                            "2024-01-01T01:00:00.000000Z\tnull\n" +
                            "2024-01-01T02:00:00.000000Z\t[3.0,4.0]\n",
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(NULL)",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0, 2.0]),
                    ('2024-01-01T01:00:00', ARRAY[3.0]),
                    ('2024-01-01T04:00:00', ARRAY[4.0, 5.0])
                    """);
            assertQueryNoLeakCheck(
                    "ts\tagg\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]\n" +
                            "2024-01-01T02:00:00.000000Z\t[1.0,2.0,3.0]\n" +
                            "2024-01-01T04:00:00.000000Z\t[4.0,5.0]\n",
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 2h FILL(PREV)",
                    "ts"
            );
        });
    }

    @Test
    public void testSampleByFillValueRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, grp SYMBOL, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', 'a', ARRAY[1.0])");
            assertExceptionNoLeakCheck(
                    "SELECT ts, grp, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(42)",
                    16,
                    "support for VALUE fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFillValueRejectedNonKeyed() throws Exception {
        // Non-keyed SAMPLE BY goes through SqlOptimiser.rewriteSampleBy which converts
        // SAMPLE BY into GROUP BY + FillRangeRecordCursorFactory. The rewrite path must
        // still validate that the aggregate supports VALUE fill, otherwise the query
        // compiles and crashes at runtime when a gap triggers FillRangeRecord.getArray().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0]),
                    ('2024-01-01T02:00:00', ARRAY[2.0])
                    """);
            assertExceptionNoLeakCheck(
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(42)",
                    11,
                    "support for VALUE fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0, 3.0])");
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.0,2.0,3.0]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSpecialDoubleValues() throws Exception {
        // Verify that arbitrary IEEE 754 finite bit patterns round-trip unchanged
        // through the per-element (rowId, value) build buffer and the flat render
        // buffer produced by getArray(). Covers Double.MAX_VALUE, Double.MIN_NORMAL,
        // Double.MIN_VALUE (denormal), negative extremes, and signed zero.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[1.7976931348623157E308, -1.7976931348623157E308]),
                    (ARRAY[2.2250738585072014E-308, 4.9E-324]),
                    (ARRAY[0.0, -0.0, 3.141592653589793])
                    """);
            assertQueryNoLeakCheck(
                    "agg\n" +
                            "[1.7976931348623157E308,-1.7976931348623157E308,2.2250738585072014E-308,5.0E-324,0.0,-0.0,3.141592653589793]\n",
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testToPlan() throws Exception {
        // Pin the query plan output so a regression in toPlan() is caught.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            assertPlanNoLeakCheck(
                    "SELECT array_agg(arr) FROM tab",
                    """
                            Async Group By workers: 1
                              vectorized: false
                              values: [array_agg(arr)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """
            );
        });
    }
}
