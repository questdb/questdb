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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ArrayAggDoubleGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllNullInputs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (null),
                    (null),
                    (null)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [null,null,null]
                            """);
        });
    }

    @Test
    public void testBufferGrowthPreservesNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            // Insert >16 rows to force buffer growth past INITIAL_CAPACITY.
            // Place nulls at boundaries: first, at capacity boundary (16th), and last.
            execute("""
                    INSERT INTO tab VALUES
                    (null),
                    (1.0),
                    (2.0),
                    (3.0),
                    (4.0),
                    (5.0),
                    (6.0),
                    (7.0),
                    (8.0),
                    (9.0),
                    (10.0),
                    (11.0),
                    (12.0),
                    (13.0),
                    (14.0),
                    (null),
                    (16.0),
                    (-17.5),
                    (null)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [null,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,null,16.0,-17.5,null]
                            """);
        });
    }

    @Test
    public void testConstantInput() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (x INT)");
            execute("""
                    INSERT INTO tab VALUES
                    (1),
                    (2),
                    (3)
                    """);
            assertQuery("SELECT array_agg(42.0) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [42.0,42.0,42.0]
                            """);
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            null
                            """);
        });
    }

    @Test
    public void testGroupByCompoundKey() throws Exception {
        // GROUP BY a, b uses a composite map key encoding distinct from the
        // single-symbol path covered by testGroupByKeyed. Verify that the
        // per-group buffer pointer is correctly resolved through a multi-key
        // map and that ordering across groups is stable.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (region SYMBOL, country SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('eu', 'fr', 1.0),
                    ('eu', 'de', 2.0),
                    ('eu', 'fr', 3.0),
                    ('na', 'us', 10.0),
                    ('na', 'us', 20.0),
                    ('eu', 'de', 4.0)
                    """);
            assertQuery("SELECT region, country, array_agg(val) arr FROM tab GROUP BY region, country ORDER BY region, country")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            region\tcountry\tarr
                            eu\tde\t[2.0,4.0]
                            eu\tfr\t[1.0,3.0]
                            na\tus\t[10.0,20.0]
                            """);
        });
    }

    @Test
    public void testGroupByKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 1.0),
                    ('a', 2.0),
                    ('a', 3.0),
                    ('b', 10.0),
                    ('b', 20.0)
                    """);
            assertQuery("SELECT grp, array_agg(val) arr FROM tab ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tarr
                            a\t[1.0,2.0,3.0]
                            b\t[10.0,20.0]
                            """);
        });
    }

    @Test
    public void testGroupByNotKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.0),
                    (2.0),
                    (3.0),
                    (4.0),
                    (5.0)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,2.0,3.0,4.0,5.0]
                            """);
        });
    }

    @Test
    public void testGroupByNullKey() throws Exception {
        // A NULL grouping key must form its own group rather than being silently
        // dropped or coerced into the empty-string group.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 1.0),
                    (null, 2.0),
                    ('a', 3.0),
                    (null, 4.0)
                    """);
            assertQuery("SELECT grp, array_agg(val) arr FROM tab ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tarr
                            \t[2.0,4.0]
                            a\t[1.0,3.0]
                            """);
        });
    }

    @Test
    public void testGroupByNullKeyParallel() throws Exception {
        // NULL-key buckets must accumulate consistently across worker boundaries
        // and survive the parallel merge phase. Run on a 4-worker pool with small
        // page frames so the NULL group is touched by every worker, then assert
        // both element count and array_sum (order-independent value check) for
        // each group including NULL.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)", sqlExecutionContext);
                // 2000 rows: every 3rd has a NULL key (positions 0, 3, 6, ...).
                // grp values cycle through g0..g4 for non-null rows.
                // Each row contributes one element, so a group with N rows
                // contributes N elements with total = sum of row positions.
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                int nullRows = 0;
                long nullSum = 0;
                int[] grpRows = new int[5];
                long[] grpSums = new long[5];
                for (int i = 0; i < 2_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    if (i % 3 == 0) {
                        sb.append("(null, ").append(i).append(".0)");
                        nullRows++;
                        nullSum += i;
                    } else {
                        int g = (i % 5);
                        sb.append("('g").append(g).append("', ").append(i).append(".0)");
                        grpRows[g]++;
                        grpSums[g] += i;
                    }
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                StringBuilder expected = new StringBuilder("grp\tcnt\ttotal\n");
                expected.append('\t').append(nullRows).append('\t')
                        .append((double) nullSum).append('\n');
                for (int g = 0; g < 5; g++) {
                    expected.append('g').append(g).append('\t').append(grpRows[g])
                            .append('\t').append((double) grpSums[g]).append('\n');
                }
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_count(array_agg(val)) cnt, array_sum(array_agg(val)) total " +
                                "FROM tab ORDER BY grp",
                        sink,
                        expected
                );
            }, configuration, LOG);
        });
    }

    @Test
    public void testImplicitCastFromInt() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val INT)");
            execute("""
                    INSERT INTO tab VALUES
                    (1),
                    (2),
                    (3)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,2.0,3.0]
                            """);
        });
    }

    @Test
    public void testMaxArrayElementCountExceeded() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_ARRAY_ELEMENT_COUNT, 5);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.0), (2.0), (3.0), (4.0), (5.0), (6.0)
                    """);
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(val) FROM tab",
                    0,
                    "array_agg: array size exceeds configured maximum [maxArrayElementCount=5]"
            );
        });
    }

    @Test
    public void testMergeTimeCardinalityExceeded() throws Exception {
        // Run on an explicit 4-worker pool so per-worker counts stay below the
        // 9_999-element limit while the merged count crosses it. This reliably
        // exercises the capacity check inside merge() rather than falling back
        // to the computeNext check when the test JVM has only one shared worker.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_MAX_ARRAY_ELEMENT_COUNT, 9_999);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (val DOUBLE)", sqlExecutionContext);
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    sb.append("(").append(i).append(".0)");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                try {
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "SELECT array_agg(val) FROM tab",
                            sink,
                            ""
                    );
                    org.junit.Assert.fail("expected CairoException with maxArrayElementCount=9999");
                } catch (io.questdb.cairo.CairoException ex) {
                    TestUtils.assertContains(ex.getMessage(),
                            "array_agg: array size exceeds configured maximum [maxArrayElementCount=9999]");
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testMixedWithOtherAggregates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 10.0),
                    ('a', 20.0),
                    ('a', 30.0),
                    ('b', 100.0),
                    ('b', 200.0)
                    """);
            assertQuery("SELECT grp, array_agg(val) arr, avg(val) avg FROM tab ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tarr\tavg
                            a\t[10.0,20.0,30.0]\t20.0
                            b\t[100.0,200.0]\t150.0
                            """);
        });
    }

    @Test
    public void testMultipleArrayAggInSameQuery() throws Exception {
        // Two independent array_agg() calls in the same projection must produce
        // two independent build buffers per group; each function instance keeps
        // its own (srcPtr -> renderPtr) cache, and the two must not collide.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, x DOUBLE, y DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 1.0, 10.0),
                    ('a', 2.0, 20.0),
                    ('b', 3.0, 30.0)
                    """);
            assertQuery("SELECT grp, array_agg(x) xs, array_agg(y) ys FROM tab ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\txs\tys
                            a\t[1.0,2.0]\t[10.0,20.0]
                            b\t[3.0]\t[30.0]
                            """);
        });
    }

    @Test
    public void testNaNFromArithmeticRendersAsNull() throws Exception {
        // The NULL DOUBLE sentinel and an arithmetic NaN share the same IEEE 754 bit
        // pattern; both must render as null in array output. testNonFiniteInputsRenderAsNull
        // covers Infinity but not NaN produced via arithmetic - this guards the bit-pattern
        // round-trip path explicitly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.0),
                    (0.0/0.0),
                    (3.0)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("arr\n[1.0,null,3.0]\n");
        });
    }

    @Test
    public void testNestedReAggregationCrossVariant() throws Exception {
        // Single inner group + order-independent array_sum to avoid depending
        // on hash GROUP BY emission order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 1.0),
                    ('a', 2.5),
                    ('a', 3.0),
                    ('a', 4.5)
                    """);
            assertQuery("SELECT array_agg(arr) concat, array_sum(array_agg(arr)) sum FROM (" +
                    "  SELECT array_agg(val) arr FROM tab" +
                    ")")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            concat\tsum
                            [1.0,2.5,3.0,4.5]\t11.0
                            """);
        });
    }

    @Test
    public void testNonFiniteInputsRenderAsNull() throws Exception {
        // QuestDB DOUBLE[] rendering treats non-finite values as null. The buffer
        // preserves the bit pattern via Unsafe.putDouble/getDouble, but the array
        // sink converts NaN/+Inf/-Inf to the null literal during text output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.0),
                    ('Infinity'::DOUBLE),
                    (2.0),
                    ('-Infinity'::DOUBLE),
                    (3.0)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,null,2.0,null,3.0]
                            """);
        });
    }

    @Test
    public void testNullInputValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.0),
                    (null),
                    (3.0),
                    (null),
                    (5.0)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,null,3.0,null,5.0]
                            """);
        });
    }

    @Test
    public void testOrderPreserved() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (5.0),
                    (3.0),
                    (1.0),
                    (4.0),
                    (2.0)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [5.0,3.0,1.0,4.0,2.0]
                            """);
        });
    }

    @Test
    public void testParallelCounts() throws Exception {
        // Run on an explicit 4-worker pool so the parallel merge path is reliably
        // exercised regardless of the test JVM's default worker count. The page
        // frame split alone does not guarantee multi-worker dispatch.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)", sqlExecutionContext);
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    sb.append("('g").append(i % 10).append("', ").append(i).append(".0)");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_count(array_agg(val)) cnt, array_sum(array_agg(val)) total FROM tab ORDER BY grp",
                        sink,
                        """
                                grp\tcnt\ttotal
                                g0\t1000\t4995000.0
                                g1\t1000\t4996000.0
                                g2\t1000\t4997000.0
                                g3\t1000\t4998000.0
                                g4\t1000\t4999000.0
                                g5\t1000\t5000000.0
                                g6\t1000\t5001000.0
                                g7\t1000\t5002000.0
                                g8\t1000\t5003000.0
                                g9\t1000\t5004000.0
                                """
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
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)", sqlExecutionContext);
                // 10 groups x 1000 rows. Row i goes to group g(i%10) with value i.0.
                // Group gN receives values N, N+10, N+20, ..., N+9990 in insertion order.
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 10_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    sb.append("('g").append(i % 10).append("', ").append(i).append(".0)");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                // Build expected: each group gN has elements N, N+10, ..., N+9990.
                StringBuilder expected = new StringBuilder("grp\tarr\n");
                for (int g = 0; g < 10; g++) {
                    expected.append('g').append(g).append('\t').append('[');
                    for (int j = 0; j < 1000; j++) {
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
                        "SELECT grp, array_agg(val) arr FROM tab ORDER BY grp",
                        sink,
                        expected
                );
            }, configuration, LOG);
        });
    }

    @Test
    public void testReAggregationViaArrayCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 1.0),
                    ('a', 2.0),
                    ('a', 3.0),
                    ('b', 10.0),
                    ('b', 20.0)
                    """);
            assertQuery("SELECT grp, array_count(arr) cnt FROM " +
                    "(SELECT grp, array_agg(val) arr FROM tab) ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tcnt
                            a\t3
                            b\t2
                            """);
        });
    }

    @Test
    public void testRejectsDistinctModifier() throws Exception {
        // array_agg(DISTINCT x) is not supported. ExpressionParser only rewrites
        // DISTINCT for count() and string_agg(); for array_agg the keyword leaks
        // through to the function call and must be rejected with a clear error.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(DISTINCT val) FROM tab",
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
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE)");
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(val ORDER BY ts) FROM tab",
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
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(val) OVER () FROM tab",
                    7,
                    "non-window function called in window context"
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
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    ('a', 1.0),
                    ('a', 2.0),
                    ('a', 3.0),
                    ('b', 10.0),
                    ('b', 20.0)
                    """);
            assertQuery("SELECT grp, arr, array_count(arr) cnt, array_sum(arr) sum " +
                    "FROM (SELECT grp, array_agg(val) arr FROM tab) ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tarr\tcnt\tsum
                            a\t[1.0,2.0,3.0]\t3\t6.0
                            b\t[10.0,20.0]\t2\t30.0
                            """);
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T01:00:00', 2.0),
                    ('2024-01-01T02:00:00', 3.0),
                    ('2024-01-01T05:00:00', 4.0),
                    ('2024-01-01T05:30:00', 5.0),
                    ('2024-01-01T09:00:00', 6.0)
                    """);
            assertQuery("SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 3h ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T03:00:00.000000Z\t[4.0,5.0]
                            2024-01-01T09:00:00.000000Z\t[6.0]
                            """);
        });
    }

    @Test
    public void testSampleByAlignToCalendar() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T00:30:00', 2.0),
                    ('2024-01-01T01:00:00', 3.0),
                    ('2024-01-01T01:15:00', 4.0)
                    """);
            assertQuery("SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 1h ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T01:00:00.000000Z\t[3.0,4.0]
                            """);
        });
    }

    @Test
    public void testSampleByFillLinearRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, grp SYMBOL, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', 'a', 1.0)");
            final String sql = "SELECT ts, grp, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(LINEAR)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("LINEAR"),
                    "support for LINEAR fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFillLinearRejectedNonKeyed() throws Exception {
        // Mirror of testSampleByFillValueRejectedNonKeyed for FILL(LINEAR). Both fill
        // modes route through the same SqlOptimiser.rewriteSampleBy + rewriteSelectClause0
        // path that propagates fillValues onto groupByModel; either could regress
        // independently if the LINEAR-specific gate at SqlOptimiser.hasLinearFill changed.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T02:00:00', 2.0)
                    """);
            final String sql = "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(LINEAR)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("LINEAR"),
                    "support for LINEAR fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFillNone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T00:30:00', 2.0),
                    ('2024-01-01T03:00:00', 3.0),
                    ('2024-01-01T06:00:00', 4.0),
                    ('2024-01-01T06:30:00', 5.0),
                    ('2024-01-01T06:45:00', null)
                    """);
            // Two gaps (01:00-03:00 and 04:00-06:00) must be omitted.
            // Null at 06:45 is preserved in the array, not skipped by FILL(NONE).
            assertQuery("SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(NONE)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T03:00:00.000000Z\t[3.0]
                            2024-01-01T06:00:00.000000Z\t[4.0,5.0,null]
                            """);
        });
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T01:00:00', 2.0),
                    ('2024-01-01T04:00:00', 3.0)
                    """);
            assertQuery("SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 2h FILL(NULL)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T02:00:00.000000Z\tnull
                            2024-01-01T04:00:00.000000Z\t[3.0]
                            """);
        });
    }

    @Test
    public void testSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T01:00:00', 2.0),
                    ('2024-01-01T04:00:00', 3.0)
                    """);
            assertQuery("SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 2h FILL(PREV)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T02:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T04:00:00.000000Z\t[3.0]
                            """);
        });
    }

    @Test
    public void testSampleByFillValueRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, grp SYMBOL, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', 'a', 1.0)");
            final String sql = "SELECT ts, grp, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(42)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("42"),
                    "support for VALUE fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFillValueRejectedNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', 1.0),
                    ('2024-01-01T02:00:00', 2.0)
                    """);
            final String sql = "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(42)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("42"),
                    "support for VALUE fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFromToFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T02:00:00', 1.0),
                    ('2024-01-01T03:00:00', 2.0)
                    """);
            assertQuery("SELECT ts, array_agg(val) arr FROM tab "
                    + "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR")
                    .noLeakCheck()
                    .timestamp("ts")
                    .noRandomAccess()
                    .returns("""
                            ts\tarr
                            2024-01-01T00:00:00.000000Z\tnull
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\t[1.0]
                            2024-01-01T03:00:00.000000Z\t[2.0]
                            2024-01-01T04:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("INSERT INTO tab VALUES (42.0)");
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [42.0]
                            """);
        });
    }

    @Test
    public void testSpecialDoubleValues() throws Exception {
        // Verify that arbitrary IEEE 754 finite bit patterns round-trip unchanged
        // through the (rowId, value) build buffer and the flat render buffer
        // produced by getArray(). Covers Double.MAX_VALUE, Double.MIN_NORMAL,
        // Double.MIN_VALUE (denormal), negative extremes, and signed zero.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.7976931348623157E308),
                    (-1.7976931348623157E308),
                    (2.2250738585072014E-308),
                    (4.9E-324),
                    (0.0),
                    (-0.0),
                    (3.141592653589793)
                    """);
            assertQuery("SELECT array_agg(val) arr FROM tab")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.7976931348623157E308,-1.7976931348623157E308,2.2250738585072014E-308,5.0E-324,0.0,-0.0,3.141592653589793]
                            """);
        });
    }

    @Test
    public void testToPlan() throws Exception {
        // Pin the query plan output so a regression in toPlan() is caught.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            assertPlanNoLeakCheck(
                    "SELECT array_agg(val) FROM tab",
                    """
                            Async Group By workers: 1
                              vectorized: false
                              values: [array_agg(val)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tab
                            """
            );
        });
    }

    @Test
    public void testWithSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("""
                    INSERT INTO tab VALUES
                    (1.0),
                    (2.0),
                    (3.0)
                    """);
            assertQuery("SELECT arr FROM (SELECT array_agg(val) arr FROM tab)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            arr
                            [1.0,2.0,3.0]
                            """);
        });
    }
}
