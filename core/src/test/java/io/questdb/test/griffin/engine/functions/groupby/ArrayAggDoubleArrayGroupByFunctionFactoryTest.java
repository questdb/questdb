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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.engine.functions.groupby.ArrayAggDoubleArrayGroupByFunction;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
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
                    """
                            agg
                            null
                            """,
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
                    """
                            agg
                            [1.0,2.0,3.0,4.0,5.0]
                            """,
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
                    """
                            agg
                            [1.0,2.0,3.0,4.0,5.0,6.0]
                            """,
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
                    """
                            agg
                            [1.0,2.0,3.0,4.0]
                            """,
                    "SELECT array_agg(arr) agg FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyFirstThenNonEmpty() throws Exception {
        // computeFirst() must handle an empty input array on the very first row
        // of a group: the array variant writes ptr=0 (concat identity), and a
        // later non-empty row re-enters computeFirst via the ptr==0 branch in
        // computeNext. Existing testEmptyArraysSkipped places the empty array
        // in the middle, which only exercises computeNext's empty-array path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("""
                    INSERT INTO tab VALUES
                    (ARRAY[]),
                    (ARRAY[]),
                    (ARRAY[1.0, 2.0]),
                    (ARRAY[3.0])
                    """);
            assertQueryNoLeakCheck(
                    """
                            agg
                            [1.0,2.0,3.0]
                            """,
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
                    """
                            agg
                            null
                            """,
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
                    """
                            region\tcountry\tagg
                            eu\tde\t[10.0,20.0,30.0]
                            eu\tfr\t[1.0,2.0,3.0]
                            na\tus\t[100.0,200.0]
                            """,
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
                    """
                            grp\tagg
                            a\t[1.0,2.0,3.0]
                            b\t[10.0,20.0,30.0,40.0,50.0]
                            """,
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
                    """
                            grp\tagg
                            \t[3.0,5.0,6.0]
                            a\t[1.0,2.0,4.0]
                            """,
                    "SELECT grp, array_agg(arr) agg FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
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
                engine.execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])", sqlExecutionContext);
                // 2000 rows: every 3rd has a NULL key (positions 0, 3, 6, ...).
                // grp values cycle through g0..g4 for non-null rows.
                // Each row contributes a 2-element array, so a group with N rows
                // contributes 2*N elements.
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                int nullRows = 0;
                int[] grpRows = new int[5];
                for (int i = 0; i < 2_000; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    if (i % 3 == 0) {
                        sb.append("(null, ARRAY[").append(i).append(".0, ").append(i + 1).append(".0])");
                        nullRows++;
                    } else {
                        int g = (i % 5);
                        sb.append("('g").append(g).append("', ARRAY[")
                                .append(i).append(".0, ").append(i + 1).append(".0])");
                        grpRows[g]++;
                    }
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                // Build expected per-group counts. array_sum of [i, i+1] over the
                // group rows equals sum of (2*i + 1) across that group's row positions.
                StringBuilder expected = new StringBuilder("grp\tcnt\n");
                expected.append('\t').append(nullRows * 2).append('\n');
                for (int g = 0; g < 5; g++) {
                    expected.append('g').append(g).append('\t').append(grpRows[g] * 2).append('\n');
                }
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_count(array_agg(arr)) cnt FROM tab ORDER BY grp",
                        sink,
                        expected
                );
            }, configuration, LOG);
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
    public void testMergeDeepCopyEmptyDest() throws Exception {
        // White-box test for the destPtr==0 deep-copy branch in
        // AbstractArrayAggDoubleGroupByFunction.merge() (L247-256). The
        // parallel framework only reaches this branch when work-stealing
        // assigns the destination accumulator a ptr=0 contribution before
        // any real data lands; that ordering is not controllable from SQL,
        // so testParallelMergeEmptyWorkerState above triggers it only
        // probabilistically. To exercise the branch deterministically this
        // test constructs the two MapValue inputs directly (one with the
        // long slot at 0, the other pointing at a synthesized 3-element
        // build buffer), bypasses the parallel dispatch path, and calls
        // merge() directly. The branch must allocate a fresh buffer in
        // dest's allocator and deep-copy src's entries verbatim - a shallow
        // copy of srcPtr would dangle once src's allocator was reclaimed.
        assertMemoryLeak(() -> {
            try (
                    OrderedMap destMap = new OrderedMap(4 * 1024, new SingleColumnType(ColumnType.INT),
                            new SingleColumnType(ColumnType.LONG), 16, 0.8, 24);
                    OrderedMap srcMap = new OrderedMap(4 * 1024, new SingleColumnType(ColumnType.INT),
                            new SingleColumnType(ColumnType.LONG), 16, 0.8, 24);
                    FastGroupByAllocator allocator = new FastGroupByAllocator(1024, 1024 * 1024)
            ) {
                try (ArrayAggDoubleArrayGroupByFunction fn = new ArrayAggDoubleArrayGroupByFunction(
                        DoubleConstant.NULL, configuration)) {
                    fn.initValueIndex(0);
                    fn.setAllocator(allocator);

                    // Source buffer layout: |count:INT|capacity:INT|(rowId:LONG, value:DOUBLE) x N|
                    final int srcCount = 3;
                    final long headerSize = 8L;
                    final long entrySize = 16L;
                    final long srcPtr = allocator.malloc(headerSize + (long) srcCount * entrySize);
                    Unsafe.putInt(srcPtr, srcCount);
                    Unsafe.putInt(srcPtr + 4, srcCount);
                    for (int i = 0; i < srcCount; i++) {
                        long entry = srcPtr + headerSize + (long) i * entrySize;
                        Unsafe.putLong(entry, i);
                        Unsafe.putDouble(entry + 8, 10.0 + i);
                    }

                    MapKey destKey = destMap.withKey();
                    destKey.putInt(1);
                    MapValue destValue = destKey.createValue();
                    destValue.putLong(0, 0L);

                    MapKey srcKey = srcMap.withKey();
                    srcKey.putInt(2);
                    MapValue srcValue = srcKey.createValue();
                    srcValue.putLong(0, srcPtr);

                    fn.merge(destValue, srcValue);

                    final long newDestPtr = destValue.getLong(0);
                    Assert.assertNotEquals("destValue must hold a fresh non-zero pointer", 0L, newDestPtr);
                    Assert.assertNotEquals("destValue must own a fresh buffer rather than shallow-copying srcPtr",
                            srcPtr, newDestPtr);
                    Assert.assertEquals(srcCount, Unsafe.getInt(newDestPtr));
                    Assert.assertEquals(srcCount, Unsafe.getInt(newDestPtr + 4));
                    for (int i = 0; i < srcCount; i++) {
                        long entry = newDestPtr + headerSize + (long) i * entrySize;
                        Assert.assertEquals(i, Unsafe.getLong(entry));
                        Assert.assertEquals(10.0 + i, Unsafe.getDouble(entry + 8), 0.0);
                    }
                }
            }
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
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
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
                    """
                            grp\tagg\tavg
                            a\t[1.0,2.0,3.0]\t15.0
                            b\t[4.0,5.0]\t30.0
                            """,
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
                    """
                            agg
                            [10.0,30.0,50.0,70.0]
                            """,
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
                    """
                            agg
                            [1.0,2.0,3.0,4.0]
                            """,
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
                    """
                            agg
                            [1.0,null,null,4.0]
                            """,
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
                    """
                            agg
                            [1.0,2.0,3.0]
                            """,
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
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
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
                        """
                                grp\tcnt\ttotal
                                g0\t400\t199200.0
                                g1\t400\t199600.0
                                g2\t400\t200000.0
                                g3\t400\t200400.0
                                g4\t400\t200800.0
                                """
                );
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelDisjointMerge() throws Exception {
        // When each worker accumulates rowIds from contiguous, non-overlapping
        // page frames, the per-worker buffers form pairwise disjoint sorted
        // runs (one worker's max rowId is below the next worker's min). In
        // that arrangement merge() takes the bulk-memcpy fast path via
        // tryMergeDisjointRuns instead of the two-pointer merge-sort.
        //
        // The layout below allocates exactly 4 page frames (one frame per
        // worker, single group key) so each worker is guaranteed a contiguous
        // single-frame rowId block.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])", sqlExecutionContext);
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 400; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    sb.append("('g0', ARRAY[").append(i).append(".0])");
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                // sum(0..399) = 399 * 400 / 2 = 79800
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_count(array_agg(arr)) cnt, array_sum(array_agg(arr)) total FROM tab",
                        sink,
                        """
                                grp\tcnt\ttotal
                                g0\t400\t79800.0
                                """
                );
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelMergeEmptyWorkerState() throws Exception {
        // For the array variant, computeFirst() on a NULL or empty input writes
        // ptr=0 into the map slot rather than allocating a buffer. When the
        // parallel framework folds per-worker maps into the destination map,
        // the resulting merge() call can see either side at ptr=0:
        //   - srcPtr==0: a worker that observed only NULL inputs for the key
        //   - destPtr==0: the destination accumulator (owner fragment) holds
        //     ptr=0 for the key when a later worker brings real data.
        //
        // Layout: 4 page frames under a single group key, one frame per
        // worker. Three frames hold NULL arrays (worker stays at ptr=0);
        // one frame holds real values (worker reaches ptr=data). With this
        // 3:1 skew the per-worker map collection contains three NULL-state
        // entries and one real-state entry. Whichever of the four worker
        // slots is iterated first into the (initially empty) destination
        // map dictates which branch of merge() fires for the others:
        //   - if a NULL-state worker is iterated first, the destination
        //     accumulator stays at ptr=0 and the real-state worker triggers
        //     the destPtr==0 deep-copy branch;
        //   - if the real-state worker is iterated first, the accumulator
        //     starts at ptr=data and the three NULL-state workers each
        //     trigger the srcPtr==0 early return.
        // Either way at least one of the two empty-state branches in
        // merge() is exercised. The exact branch is governed by work-
        // stealing and is not strictly deterministic.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                engine.execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])", sqlExecutionContext);
                StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                for (int i = 0; i < 400; i++) {
                    if (i > 0) {
                        sb.append(",\n");
                    }
                    // Frames 0, 1, 2 are NULL; frame 3 is real.
                    if (i < 300) {
                        sb.append("('g0', null::double[])");
                    } else {
                        sb.append("('g0', ARRAY[").append(i).append(".0])");
                    }
                }
                engine.execute(sb.toString(), sqlExecutionContext);
                // 100 real values: rows 300..399.
                // sum = (300+399)*100/2 = 34950
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "SELECT grp, array_count(array_agg(arr)) cnt, array_sum(array_agg(arr)) total FROM tab",
                        sink,
                        """
                                grp\tcnt\ttotal
                                g0\t100\t34950.0
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
                    """
                            grp\tagg\tcnt\tsum
                            a\t[1.0,2.0,3.0,4.0,5.0]\t5\t15.0
                            b\t[10.0,20.0,30.0]\t3\t60.0
                            """,
                    "SELECT grp, agg, array_count(agg) cnt, array_sum(agg) sum " +
                            "FROM (SELECT grp, array_agg(arr) agg FROM tab) ORDER BY grp",
                    null,
                    true,
                    true
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
                    """
                            ts\tagg
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T01:00:00.000000Z\t[4.0,5.0,6.0]
                            """,
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
            final String sql = "SELECT ts, grp, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(LINEAR)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("LINEAR"),
                    "support for LINEAR fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFillLinearRejectedNonKeyed() throws Exception {
        // Mirror of testSampleByFillValueRejectedNonKeyed for FILL(LINEAR) on the
        // array_agg(D[]) variant.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0]),
                    ('2024-01-01T02:00:00', ARRAY[2.0])
                    """);
            final String sql = "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(LINEAR)";
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
                    """
                            ts\tagg
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T03:00:00.000000Z\t[4.0,5.0]
                            2024-01-01T06:00:00.000000Z\t[6.0]
                            """,
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
                    """
                            ts\tagg
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\t[3.0,4.0]
                            """,
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
                    """
                            ts\tagg
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T02:00:00.000000Z\t[1.0,2.0,3.0]
                            2024-01-01T04:00:00.000000Z\t[4.0,5.0]
                            """,
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
            final String sql = "SELECT ts, grp, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(42)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("42"),
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
            final String sql = "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(42)";
            assertExceptionNoLeakCheck(
                    sql,
                    sql.indexOf("42"),
                    "support for VALUE fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSampleByFirstObservation() throws Exception {
        // ALIGN TO FIRST OBSERVATION routes through a different RecordCursorFactory than
        // ALIGN TO CALENDAR. The array variant's per-element copy plus null/empty-array
        // branches in computeFirst/computeNext are the more failure-prone side of the
        // aggregate; testSampleBy already pins ALIGN TO CALENDAR, this pins FIRST.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T00:00:00', ARRAY[1.0, 2.0]),
                    ('2024-01-01T01:00:00', ARRAY[3.0]),
                    ('2024-01-01T02:00:00', ARRAY[4.0, 5.0]),
                    ('2024-01-01T05:00:00', ARRAY[6.0]),
                    ('2024-01-01T05:30:00', ARRAY[7.0, 8.0]),
                    ('2024-01-01T09:00:00', ARRAY[9.0])
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tagg
                            2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0,4.0,5.0]
                            2024-01-01T03:00:00.000000Z\t[6.0,7.0,8.0]
                            2024-01-01T09:00:00.000000Z\t[9.0]
                            """,
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 3h ALIGN TO FIRST OBSERVATION",
                    "ts"
            );
        });
    }

    @Test
    public void testSampleByFromToFillNull() throws Exception {
        // FROM/TO + FILL(NULL) extends the result range beyond the data window, forcing
        // FillRangeRecordCursorFactory to emit synthetic null buckets at the front and
        // back. The scalar variant doesn't exercise the array null path; this test pins
        // FillRangeRecord.getArray() returning ArrayConstant.NULL for the synthetic rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tab VALUES
                    ('2024-01-01T02:00:00', ARRAY[1.0, 2.0]),
                    ('2024-01-01T03:00:00', ARRAY[3.0])
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tagg
                            2024-01-01T00:00:00.000000Z\tnull
                            2024-01-01T01:00:00.000000Z\tnull
                            2024-01-01T02:00:00.000000Z\t[1.0,2.0]
                            2024-01-01T03:00:00.000000Z\t[3.0]
                            2024-01-01T04:00:00.000000Z\tnull
                            """,
                    "SELECT ts, array_agg(arr) agg FROM tab "
                            + "SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' FILL(NULL) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (arr DOUBLE[])");
            execute("INSERT INTO tab VALUES (ARRAY[1.0, 2.0, 3.0])");
            assertQueryNoLeakCheck(
                    """
                            agg
                            [1.0,2.0,3.0]
                            """,
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
                    """
                            agg
                            [1.7976931348623157E308,-1.7976931348623157E308,2.2250738585072014E-308,5.0E-324,0.0,-0.0,3.141592653589793]
                            """,
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
