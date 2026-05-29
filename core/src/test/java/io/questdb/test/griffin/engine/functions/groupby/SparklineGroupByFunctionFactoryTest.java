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
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SparklineGroupByFunctionFactoryTest extends AbstractCairoTest {

    // Shared fixtures for the work-stealing contention regression tests
    // (testParallelSparklineMatchesUnderContention and its keyed variant) - the
    // sparkline twin of TwapUnsortedRunReproTest. SparklineGroupByFunction got
    // the identical per-frame batch-descriptor rework as TwapGroupByFunction for
    // issue #7123: under cross-query work-stealing a per-slot buffer can
    // accumulate page frames out of rowId order, and SortedRunsMerge must permute
    // whole batches back into key order before the value sequence is rendered. A
    // regression there renders a deterministically wrong sparkline that the
    // assertSqlCursors(sql, sql) self-comparison tests cannot catch, so these
    // tests assert an independently-known exact string. Index L of LEVEL_CHARS is
    // the character value level L renders to under the explicit min=0, max=7.
    //
    // No duplicate-key tie variant (twap has one): sparkline sorts by the row id,
    // which is globally unique, so two batches can never tie on first key. That
    // (firstKey, lastKey) tie-break is unreachable through sparkline and is
    // covered directly at the primitive level by SortedRunsMergeTest.
    private static final int CONTENTION_ITERATIONS = 30;
    private static final int CONTENTION_THREADS = 8;
    private static final int FRAMES_PER_LEVEL = 12;
    private static final int KEY_COUNT = 5; // divides ROWS_PER_FRAME so keys interleave evenly across frames
    private static final int LEVELS = 8;
    private static final char[] LEVEL_CHARS = {'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'};
    private static final int NON_KEYED_WIDTH = LEVELS * FRAMES_PER_LEVEL; // one rendered char per page frame
    private static final int ROWS_PER_FRAME = 50; // == CAIRO_SQL_PAGE_FRAME_MAX_ROWS
    private static final int STEPS_PER_LEVEL = 50;

    private static Record recordOf(double value) {
        return new Record() {
            @Override
            public double getDouble(int col) {
                return value;
            }
        };
    }

    @Test
    public void testAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (NULL, 'a', '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO t VALUES (NULL, 'a', '2024-01-01T01:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            sparkline

                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, '2024-01-01T01:00:00.000000Z'),
                    (2.0, '2024-01-01T02:00:00.000000Z'),
                    (3.0, '2024-01-01T03:00:00.000000Z'),
                    (4.0, '2024-01-01T04:00:00.000000Z'),
                    (5.0, '2024-01-01T05:00:00.000000Z'),
                    (6.0, '2024-01-01T06:00:00.000000Z'),
                    (7.0, '2024-01-01T07:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▂▃▄▅▆▇█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testClampAboveMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (100.0, '2024-01-01T02:00:00.000000Z'),
                    (200.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // With max=100, the 200 value should clamp to top char
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄██
                            """,
                    "SELECT sparkline(val, 0.0, 100.0, 4) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testClampBelowMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (-50.0, '2024-01-01T00:00:00.000000Z'),
                    (0.0, '2024-01-01T01:00:00.000000Z'),
                    (50.0, '2024-01-01T02:00:00.000000Z'),
                    (100.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // With min=0, the -50 value should clamp to bottom char
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▁▄█
                            """,
                    "SELECT sparkline(val, 0.0, 100.0, 4) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testConcatResult() throws Exception {
        // The || operator consumes the sparkline output as a VARCHAR input
        // to another function. The concat factory reads the Utf8Sequence
        // returned by getVarcharA and copies into its own sink, so the
        // bytes must stay valid past the getVarcharA call that produced
        // them. A regression in the allocator-backed render buffer
        // (premature free, aliasing across groups) would surface here as
        // garbled output.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, 'up',   '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'up',   '2024-01-01T01:00:00.000000Z'),
                    (7.0, 'up',   '2024-01-01T02:00:00.000000Z'),
                    (7.0, 'down', '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'down', '2024-01-01T01:00:00.000000Z'),
                    (0.0, 'down', '2024-01-01T02:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            grp\tlabel
                            down\ttrend: █▄▁ end
                            up\ttrend: ▁▄█ end
                            """,
                    "SELECT grp, 'trend: ' || sparkline(val) || ' end' label FROM t ORDER BY grp",
                    null,
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testConstantValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (5.0, '2024-01-01T00:00:00.000000Z'),
                    (5.0, '2024-01-01T01:00:00.000000Z'),
                    (5.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // All same value -> min==max -> all top chars
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ███
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertQueryNoLeakCheck(
                    """
                            sparkline

                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testMixedNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (7.0, '2024-01-01T02:00:00.000000Z'),
                    (NULL, '2024-01-01T03:00:00.000000Z'),
                    (3.5, '2024-01-01T04:00:00.000000Z')
                    """);
            // NULLs are skipped; remaining values 0.0, 7.0, 3.5
            // min=0, max=7, so: 0->index 0, 7->index 7, 3.5->index 3
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁█▄
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLeadingNullsThenValues() throws Exception {
        // First row of the group is NULL, so computeFirst marks the group
        // as empty (count=0). The next non-NULL row drives computeNext down
        // the count<=0 branch which has to allocate the pair buffer itself.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (NULL, '2024-01-01T00:00:00.000000Z'),
                    (NULL, '2024-01-01T01:00:00.000000Z'),
                    (0.0,  '2024-01-01T02:00:00.000000Z'),
                    (3.5,  '2024-01-01T03:00:00.000000Z'),
                    (7.0,  '2024-01-01T04:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testOrderBySparkline() throws Exception {
        // ORDER BY on the sparkline result drives the sort comparator,
        // which fetches getVarcharA and getVarcharB from the same Function
        // instance. A and B must return independent views that don't
        // clobber each other - a regression in that invariant would
        // produce wrong or unstable orderings.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, 'up',    '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'up',    '2024-01-01T01:00:00.000000Z'),
                    (7.0, 'up',    '2024-01-01T02:00:00.000000Z'),
                    (7.0, 'down',  '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'down',  '2024-01-01T01:00:00.000000Z'),
                    (0.0, 'down',  '2024-01-01T02:00:00.000000Z'),
                    (5.0, 'flat',  '2024-01-01T00:00:00.000000Z'),
                    (5.0, 'flat',  '2024-01-01T01:00:00.000000Z'),
                    (5.0, 'flat',  '2024-01-01T02:00:00.000000Z')
                    """);
            // The sparklines produced are: up=▁▄█, down=█▄▁, flat=███.
            // UTF-8 bytewise: ▁=E2 96 81, ▄=E2 96 84, █=E2 96 88.
            // First-byte differs at index 2: ▁▄█ (0x81) < █▄▁ = ███ (0x88).
            // █▄▁ vs ███ diverge at index 5: 0x84 < 0x88 => █▄▁ < ███.
            assertQueryNoLeakCheck(
                    """
                            grp\tspark
                            up\t▁▄█
                            down\t█▄▁
                            flat\t███
                            """,
                    "SELECT grp, sparkline(val) spark FROM t ORDER BY spark",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testOrderBySparklineWithNullGroup() throws Exception {
        // ORDER BY sparkline with at least one group whose values are all
        // NULL. The comparator reads both sides via getVarcharA/B; when
        // either side is the all-NULL group, getVarcharB's early-return
        // null branch must fire.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (0.0,  'up',    '2024-01-01T00:00:00.000000Z'),
                    (7.0,  'up',    '2024-01-01T01:00:00.000000Z'),
                    (NULL, 'blank', '2024-01-01T00:00:00.000000Z'),
                    (NULL, 'blank', '2024-01-01T01:00:00.000000Z'),
                    (7.0,  'down',  '2024-01-01T00:00:00.000000Z'),
                    (0.0,  'down',  '2024-01-01T01:00:00.000000Z')
                    """);
            // Three groups: up=▁█, blank=<null>, down=█▁.
            // QuestDB's ORDER BY default sorts NULL first ascending.
            assertQueryNoLeakCheck(
                    """
                            grp\tspark
                            blank\t
                            up\t▁█
                            down\t█▁
                            """,
                    "SELECT grp, sparkline(val) spark FROM t ORDER BY spark",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testMultipleGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, 'up', '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'up', '2024-01-01T01:00:00.000000Z'),
                    (7.0, 'up', '2024-01-01T02:00:00.000000Z'),
                    (7.0, 'down', '2024-01-01T00:00:00.000000Z'),
                    (3.5, 'down', '2024-01-01T01:00:00.000000Z'),
                    (0.0, 'down', '2024-01-01T02:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            grp\tsparkline
                            down\t█▄▁
                            up\t▁▄█
                            """,
                    "SELECT grp, sparkline(val) FROM t ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testNegativeValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (-10.0, '2024-01-01T00:00:00.000000Z'),
                    (-5.0, '2024-01-01T01:00:00.000000Z'),
                    (0.0, '2024-01-01T02:00:00.000000Z'),
                    (5.0, '2024-01-01T03:00:00.000000Z'),
                    (10.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // min=-10, max=10, range=20
            // -10->0, -5->1, 0->3, 5->5, 10->7
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▂▄▆█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T00:30:00.000000Z'),
                    (3.5, '2024-01-01T01:00:00.000000Z'),
                    (3.5, '2024-01-01T01:30:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            ts\tsparkline
                            2024-01-01T00:00:00.000000Z\t▁█
                            2024-01-01T01:00:00.000000Z\t██
                            """,
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (42.0, '2024-01-01T00:00:00.000000Z')");
            // Single value -> min==max -> top char
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            █
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWidthSubsampling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (0.0, '2024-01-01T01:00:00.000000Z'),
                    (7.0, '2024-01-01T02:00:00.000000Z'),
                    (7.0, '2024-01-01T03:00:00.000000Z')
                    """);
            // 4 values sub-sampled to width=2: bucket1=avg(0,0)=0, bucket2=avg(7,7)=7
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁█
                            """,
                    "SELECT sparkline(val, NULL, NULL, 2) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWithExplicitMinMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (25.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (75.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // With min=0, max=100: 25->index 1, 50->index 3, 75->index 5
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▂▄▆
                            """,
                    "SELECT sparkline(val, 0.0, 100.0, 3) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testExplicitMinNullMax() throws Exception {
        // User supplies a real min but NULL for max. The min+max scan
        // must run to derive max from the data, then honor the user's
        // min value. This asymmetric case exercises the second clause
        // of the "is userMin or userMax NaN" test in renderForPtr.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (5.0, '2024-01-01T00:00:00.000000Z'),
                    (8.0, '2024-01-01T01:00:00.000000Z'),
                    (12.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // User min=0, auto max -> max=12, range=12.
            // 5/12*7 = 2.91 -> idx 2 -> ▃
            // 8/12*7 = 4.66 -> idx 4 -> ▅
            // 12/12*7 = 7 -> idx 7 -> █
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▃▅█
                            """,
                    "SELECT sparkline(val, 0.0, NULL, 3) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testNullMinExplicitMax() throws Exception {
        // Symmetric: NULL min, explicit max.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (2.0, '2024-01-01T00:00:00.000000Z'),
                    (6.0, '2024-01-01T01:00:00.000000Z'),
                    (10.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // Auto min=2, user max=10, range=8.
            // (2-2)/8*7 = 0 -> ▁
            // (6-2)/8*7 = 3.5 -> idx 3 -> ▄
            // (10-2)/8*7 = 7 -> █
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val, NULL, 10.0, 3) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testWithNullMinAutoMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (3.5, '2024-01-01T01:00:00.000000Z'),
                    (7.0, '2024-01-01T02:00:00.000000Z')
                    """);
            // NULL min -> auto (0.0), NULL max -> auto (7.0)
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val, NULL, NULL, 3) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testFactoryReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T01:00:00.000000Z')
                    """);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                try (RecordCursorFactory fact = compiler.compile("SELECT sparkline(val) FROM t", sqlExecutionContext).getRecordCursorFactory()) {
                    // execute factory multiple times to verify pool reuse
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "sparkline\n▁█\n",
                                    cursor,
                                    fact.getMetadata(),
                                    true,
                                    sink
                            );
                        }
                    }

                    // replace with larger data and re-execute
                    execute("TRUNCATE TABLE t");
                    execute("""
                            INSERT INTO t VALUES
                            (0.0, '2024-01-01T00:00:00.000000Z'),
                            (3.5, '2024-01-01T01:00:00.000000Z'),
                            (7.0, '2024-01-01T02:00:00.000000Z')
                            """);
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = fact.getCursor(sqlExecutionContext)) {
                            TestUtils.assertCursor(
                                    "sparkline\n▁▄█\n",
                                    cursor,
                                    fact.getMetadata(),
                                    true,
                                    sink
                            );
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testIntegerColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, '2024-01-01T00:00:00.000000Z'),
                    (3, '2024-01-01T01:00:00.000000Z'),
                    (7, '2024-01-01T02:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testLongColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val LONG, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0, '2024-01-01T00:00:00.000000Z'),
                    (50, '2024-01-01T01:00:00.000000Z'),
                    (100, '2024-01-01T02:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testShortColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val SHORT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0::SHORT, '2024-01-01T00:00:00.000000Z'),
                    (50::SHORT, '2024-01-01T01:00:00.000000Z'),
                    (100::SHORT, '2024-01-01T02:00:00.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testInvertedExplicitMinMax() throws Exception {
        // min > max is almost always a user error. Without the guard the
        // negative range silently clamps every value to min and renders as
        // an all-bottom line, which looks like valid output. Verify the
        // function fails fast instead.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (50.0, '2024-01-01T01:00:00.000000Z'),
                    (90.0, '2024-01-01T02:00:00.000000Z')
                    """);
            assertException(
                    "SELECT sparkline(val, 100.0, 0.0, 3) FROM t",
                    17,
                    "sparkline() min must not exceed max [min=100.0, max=0.0]"
            );
        });
    }

    @Test
    public void testWidthInvalidZero() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            assertException(
                    "SELECT sparkline(val, 0.0, 100.0, 0) FROM t",
                    34,
                    "width must be a positive integer"
            );
        });
    }

    @Test
    public void testNonConstantMinRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, bound DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertException(
                    "SELECT sparkline(val, bound, 100.0, 10) FROM t",
                    7,
                    "there is no matching function `sparkline` with the argument types: (DOUBLE, DOUBLE, DOUBLE, INT)"
            );
        });
    }

    @Test
    public void testRejectedInHorizonJoin() throws Exception {
        // A HORIZON JOIN groups its output by horizon offset (and join key), so
        // the group-by aggregator does not receive rows in ascending
        // designated-timestamp order. The join callsite reports the base as
        // non-ascending, and sparkline() - which appends rows in scan order and
        // treats each per-frame batch as already key-sorted - is rejected at
        // compile time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT t.sym, sparkline(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM 0s TO 2s STEP 1s AS h",
                    14,
                    "sparkline() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testRejectedInMultiHorizonJoin() throws Exception {
        // Same reasoning as the single-slave HORIZON JOIN, but routed through
        // the multi-slave code path (RANGE on the last HORIZON JOIN only).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT t.sym, sparkline(p.price) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN prices p2 ON (t.sym = p2.sym) RANGE FROM 0s TO 2s STEP 1s AS h",
                    14,
                    "sparkline() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testRejectedInWindowJoin() throws Exception {
        // A WINDOW JOIN aggregates slave rows within a time window around each
        // master row, so the aggregator does not see rows in ascending
        // designated-timestamp order. The join callsite reports the base as
        // non-ascending and sparkline() is rejected at compile time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT t.sym, sparkline(p.price) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym) RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING",
                    14,
                    "sparkline() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testRejectsDescendingScan() throws Exception {
        // The aggregate appends rows in scan order using rowId as the sort
        // key and treats each per-frame batch as already key-sorted. A
        // backward scan delivers rows in reverse rowId order within a page
        // frame, breaking that invariant and producing wrong output
        // silently. The compiler must reject such queries.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, 'a', '2024-01-01T00:00:00.000000Z'),
                    (2.0, 'a', '2024-01-01T01:00:00.000000Z')
                    """);
            // ORDER BY ts DESC inside the inner SELECT compiles to a backward
            // page-frame scan when paired with LIMIT - the inner SELECT
            // without LIMIT is dropped by the optimiser.
            assertExceptionNoLeakCheck(
                    "SELECT sparkline(val) FROM (SELECT * FROM t ORDER BY ts DESC LIMIT 10)",
                    7,
                    "sparkline() requires the base query to provide ascending designated timestamp order",
                    false
            );
            assertExceptionNoLeakCheck(
                    "SELECT grp, sparkline(val) FROM (SELECT * FROM t ORDER BY ts DESC LIMIT 10) GROUP BY grp",
                    12,
                    "sparkline() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testMaxValuesBoundary() throws Exception {
        // Set buffer to 30 bytes -> maxValues = 30 / 3 = 10.
        // Inserting 10 values must succeed; inserting 11 must throw.
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (1.0, '2024-01-01T01:00:00.000000Z'),
                    (2.0, '2024-01-01T02:00:00.000000Z'),
                    (3.0, '2024-01-01T03:00:00.000000Z'),
                    (4.0, '2024-01-01T04:00:00.000000Z'),
                    (5.0, '2024-01-01T05:00:00.000000Z'),
                    (6.0, '2024-01-01T06:00:00.000000Z'),
                    (7.0, '2024-01-01T07:00:00.000000Z'),
                    (8.0, '2024-01-01T08:00:00.000000Z'),
                    (9.0, '2024-01-01T09:00:00.000000Z')
                    """);
            // 10 values must fit exactly.
            // Values 0..9 with min=0,max=9,range=9: idx = round_down(v/9*7).
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▁▂▃▄▄▅▆▇█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
            // Adding the 11th must throw.
            execute("INSERT INTO t VALUES (10.0, '2024-01-01T10:00:00.000000Z')");
            assertException(
                    "SELECT sparkline(val) FROM t",
                    17,
                    "sparkline() result exceeds max size of 30 bytes"
            );
        });
    }

    @Test
    public void testGetVarcharBReusesARender() throws Exception {
        // When getVarcharB is called with a ptr that side A just rendered,
        // it must return a viewB pointing at A's already-rendered buffer
        // without allocating a fresh one. This path matters for sort
        // comparators that call cmp(r, r) (self-compare) or re-read the
        // same record on the B side after a preceding A side read.
        assertMemoryLeak(() -> {
            SparklineGroupByFunction function = new SparklineGroupByFunction(
                    "sparkline",
                    new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'},
                    DoubleColumn.newInstance(0),
                    null, null, null,
                    0, -1,
                    300
            );
            ArrayColumnTypes types = new ArrayColumnTypes();
            function.initValueTypes(types);
            function.initValueIndex(0);

            try (
                    GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB);
                    SimpleMapValue value = new SimpleMapValue(types.getColumnCount())
            ) {
                function.setAllocator(allocator);
                function.setEmpty(value);

                function.computeFirst(value, recordOf(1.0), 0);
                function.computeNext(value, recordOf(2.0), 1);
                function.computeNext(value, recordOf(3.0), 2);

                long allocatedBefore = allocator.allocated();
                Utf8Sequence a = function.getVarcharA(value);
                Assert.assertNotNull(a);
                long allocatedAfterA = allocator.allocated();
                Assert.assertTrue("A render should allocate a buffer",
                        allocatedAfterA > allocatedBefore);

                // B for the same ptr must reuse A's buffer: no new allocation.
                Utf8Sequence b = function.getVarcharB(value);
                Assert.assertNotNull(b);
                Assert.assertEquals("B must reuse A's buffer, not allocate again",
                        allocatedAfterA, allocator.allocated());

                // A and B must be distinct flyweight instances (not the
                // same reference) so alternating reads do not clobber.
                Assert.assertNotSame(a, b);
                TestUtils.assertEquals(a, b);
            }
        });
    }

    @Test
    public void testMergeRenderCapExceeded() throws Exception {
        // Direct unit test of the render-time cap. Each per-worker partial
        // stays under maxValues (no computeNext throw), but the merged
        // buffer exceeds it. Only getVarcharA() can detect this, and the
        // render-time check in SparklineGroupByFunction must throw. This
        // path is not reachable from any serial SQL execution.
        assertMemoryLeak(() -> {
            // 30 bytes -> maxValues = 10. Each partial gets 8 values
            // (under the cap); merged has 16 values, exceeding the cap.
            final int maxBufferLength = 30;
            // column 0 is a DOUBLE "value" column; the rowId is supplied
            // separately via computeFirst/computeNext's rowId parameter.
            SparklineGroupByFunction function = new SparklineGroupByFunction(
                    "sparkline",
                    new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'},
                    DoubleColumn.newInstance(0),
                    null, null, null,
                    0, -1,
                    maxBufferLength
            );
            ArrayColumnTypes types = new ArrayColumnTypes();
            function.initValueTypes(types);
            function.initValueIndex(0);

            try (
                    GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB);
                    SimpleMapValue dest = new SimpleMapValue(types.getColumnCount());
                    SimpleMapValue src = new SimpleMapValue(types.getColumnCount())
            ) {
                function.setAllocator(allocator);
                function.setEmpty(dest);
                function.setEmpty(src);

                // Fill dest with rowIds 0,2,4,...,14 (8 values, under cap)
                function.computeFirst(dest, recordOf(1.0), 0);
                for (int i = 1; i < 8; i++) {
                    function.computeNext(dest, recordOf(i + 1.0), i * 2);
                }
                // Fill src with rowIds 1,3,5,...,15 (8 values, under cap)
                function.computeFirst(src, recordOf(10.0), 1);
                for (int i = 1; i < 8; i++) {
                    function.computeNext(src, recordOf(i + 10.0), i * 2 + 1);
                }

                // Merge: dest now holds 16 entries, exceeding maxValues=10.
                function.merge(dest, src);
                Assert.assertEquals(16L, dest.getLong(1));

                // getVarcharA must throw with the buffer-size message.
                try {
                    function.getVarcharA(dest);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(),
                            "sparkline() result exceeds max size of 30 bytes");
                }
            }
        });
    }

    @Test
    public void testMergeEmptyDestCopiesSrc() throws Exception {
        // When dest is empty (e.g., a worker saw no values for this group),
        // merge must copy src into dest's allocator rather than aliasing
        // the raw pointer. Aliasing would be a use-after-free because the
        // src worker's allocator is reclaimed independently. Assert that
        // dest's pointer differs from src's pointer after merge.
        assertMemoryLeak(() -> {
            SparklineGroupByFunction function = new SparklineGroupByFunction(
                    "sparkline",
                    new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'},
                    DoubleColumn.newInstance(0),
                    null, null, null,
                    0, -1,
                    300
            );
            ArrayColumnTypes types = new ArrayColumnTypes();
            function.initValueTypes(types);
            function.initValueIndex(0);

            try (
                    GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB);
                    SimpleMapValue dest = new SimpleMapValue(types.getColumnCount());
                    SimpleMapValue src = new SimpleMapValue(types.getColumnCount())
            ) {
                function.setAllocator(allocator);
                function.setEmpty(dest);
                function.setEmpty(src);

                // src gets 3 values; dest stays empty.
                function.computeFirst(src, recordOf(1.0), 10);
                function.computeNext(src, recordOf(2.0), 20);
                function.computeNext(src, recordOf(3.0), 30);

                long srcPtr = src.getLong(0);
                Assert.assertEquals(0L, dest.getLong(0));

                function.merge(dest, src);

                long destPtr = dest.getLong(0);
                Assert.assertEquals(3L, dest.getLong(1));
                Assert.assertNotEquals("merge must copy src, not alias its pointer", srcPtr, destPtr);
            }
        });
    }

    @Test
    public void testMergeEmptySrcLeavesDest() throws Exception {
        // Symmetric case: empty src into non-empty dest is a no-op.
        assertMemoryLeak(() -> {
            SparklineGroupByFunction function = new SparklineGroupByFunction(
                    "sparkline",
                    new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'},
                    DoubleColumn.newInstance(0),
                    null, null, null,
                    0, -1,
                    300
            );
            ArrayColumnTypes types = new ArrayColumnTypes();
            function.initValueTypes(types);
            function.initValueIndex(0);

            try (
                    GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1MB);
                    SimpleMapValue dest = new SimpleMapValue(types.getColumnCount());
                    SimpleMapValue src = new SimpleMapValue(types.getColumnCount())
            ) {
                function.setAllocator(allocator);
                function.setEmpty(dest);
                function.setEmpty(src);

                function.computeFirst(dest, recordOf(1.0), 5);
                function.computeNext(dest, recordOf(2.0), 10);

                long destPtrBefore = dest.getLong(0);
                long destCountBefore = dest.getLong(1);
                function.merge(dest, src);
                Assert.assertEquals(destPtrBefore, dest.getLong(0));
                Assert.assertEquals(destCountBefore, dest.getLong(1));
            }
        });
    }

    @Test
    public void testMemoryLimitExceeded() throws Exception {
        // getStrFunctionMaxBufferLength() defaults to 1MB = 1_048_576 bytes.
        // Each value produces one 3-byte UTF-8 char, so maxValues = 349_525.
        // We can't easily insert 350K rows in a test, so override the config
        // to a small limit and verify the error.
        setProperty(PropertyKey.CAIRO_SQL_STR_FUNCTION_BUFFER_MAX_SIZE, 30);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, '2024-01-01T00:00:00.000000Z'),
                    (2.0, '2024-01-01T01:00:00.000000Z'),
                    (3.0, '2024-01-01T02:00:00.000000Z'),
                    (4.0, '2024-01-01T03:00:00.000000Z'),
                    (5.0, '2024-01-01T04:00:00.000000Z'),
                    (6.0, '2024-01-01T05:00:00.000000Z'),
                    (7.0, '2024-01-01T06:00:00.000000Z'),
                    (8.0, '2024-01-01T07:00:00.000000Z'),
                    (9.0, '2024-01-01T08:00:00.000000Z'),
                    (10.0, '2024-01-01T09:00:00.000000Z'),
                    (11.0, '2024-01-01T10:00:00.000000Z')
                    """);
            assertException(
                    "SELECT sparkline(val) FROM t",
                    17,
                    "sparkline() result exceeds max size of 30 bytes"
            );
        });
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T00:30:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (3.5, '2024-01-01T02:30:00.000000Z')
                    """);
            // Hour 01:00 has no data, FILL(NULL) should produce NULL for that bucket
            assertQueryNoLeakCheck(
                    """
                            ts\tsparkline
                            2024-01-01T00:00:00.000000Z\t▁█
                            2024-01-01T01:00:00.000000Z\t
                            2024-01-01T02:00:00.000000Z\t██
                            """,
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h FILL(NULL)",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (7.0, '2024-01-01T00:30:00.000000Z'),
                    (3.5, '2024-01-01T02:00:00.000000Z'),
                    (3.5, '2024-01-01T02:30:00.000000Z')
                    """);
            // Hour 01:00 has no data, FILL(PREV) should repeat hour 00's sparkline
            assertQueryNoLeakCheck(
                    """
                            ts\tsparkline
                            2024-01-01T00:00:00.000000Z\t▁█
                            2024-01-01T01:00:00.000000Z\t▁█
                            2024-01-01T02:00:00.000000Z\t██
                            """,
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h FILL(PREV)",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testInfinityLiteralsTreatedAsNull() throws Exception {
        // QuestDB's DOUBLE cast normalizes 'Infinity' and '-Infinity' literals
        // to NULL (see OrderByEncodeSortTest). sparkline already skips NaN,
        // so Inf literals are transparently ignored via that same path.
        // Lock this down - if QuestDB ever starts preserving real Inf values,
        // sparkline's effectiveMin/effectiveMax would emit all-min chars
        // (range becomes +Inf) and this test would flag the behavior change.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (0.0, '2024-01-01T00:00:00.000000Z'),
                    (5.0, '2024-01-01T01:00:00.000000Z'),
                    ('Infinity'::DOUBLE, '2024-01-01T02:00:00.000000Z'),
                    ('-Infinity'::DOUBLE, '2024-01-01T03:00:00.000000Z'),
                    (10.0, '2024-01-01T04:00:00.000000Z')
                    """);
            // Both Inf literals drop out; remaining values 0,5,10 render as 3 chars.
            assertQueryNoLeakCheck(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testPlanOneArg() throws Exception {
        // EXPLAIN the single-arg form so toPlan() runs with minFunc == null
        // and renders just "sparkline(val)" without the min/max/width suffix.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertPlanNoLeakCheck(
                    "SELECT sparkline(val) FROM t",
                    """
                            Async Group By workers: 1
                              vectorized: false
                              values: [sparkline(val)]
                              filter: null
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: t
                            """
            );
        });
    }

    @Test
    public void testPlanParallel() throws Exception {
        // sparkline declares supportsParallelism()==true. Under a multi-worker
        // pool the planner must pick the Async Group By path.
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C') sym, " +
                "rnd_double() val " +
                "FROM long_sequence(10000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, sparkline(val, 0.0, 1.0, 8) FROM tab GROUP BY sym ORDER BY sym";

                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        "EXPLAIN " + sql,
                        sink,
                        """
                                QUERY PLAN
                                Encode sort light
                                  keys: [sym]
                                    Async Group By workers: 4
                                      keys: [sym]
                                      values: [sparkline(val,0.0,1.0,8)]
                                      filter: null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                                """
                );
            }, configuration, LOG);
        }
    }

    @Test
    public void testParallelCorrectness() throws Exception {
        // Run under a 4-worker pool and assert the result matches the
        // single-threaded execution. If the parallel path is incorrectly
        // engaged, results will differ across runs.
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C','D','E') sym, " +
                "rnd_double() val " +
                "FROM long_sequence(500000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, sparkline(val, 0.0, 1.0, 8) FROM tab GROUP BY sym ORDER BY sym";
                // Assert cursor is self-consistent across two executions
                // and stable under the worker pool.
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testParallelChunky() throws Exception {
        // Large dataset over a time-bucketed SAMPLE BY. sparkline stores
        // every raw value in an on-heap list and caps at ~350k per group
        // (1MB / 3 bytes per char), so a wide GROUP BY on a 2M-row table
        // would overflow. SAMPLE BY 1m keeps per-bucket sizes small while
        // still producing thousands of groups - the intended usage pattern.
        // Exercises ObjList pool growth and the LIST_CLEAR_THRESHOLD=64
        // reset semantics when the factory is reused.
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_double() val, " +
                "timestamp_sequence('2024-01-01T00:00:00.000000Z', 60_000_000L) ts " +
                "FROM long_sequence(2_000_000)) TIMESTAMP(ts) PARTITION BY MONTH");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT ts, sparkline(val, 0.0, 1.0, 8) FROM tab SAMPLE BY 1h";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testParallelAllNullValues() throws Exception {
        execute("CREATE TABLE tab AS (" +
                "SELECT rnd_symbol('A','B','C','D','E') sym, " +
                "CAST(null AS double) val " +
                "FROM long_sequence(100000))");

        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "SELECT sym, sparkline(val) FROM tab GROUP BY sym ORDER BY sym";
                assertQueryNoLeakCheck(
                        compiler,
                        """
                                sym\tsparkline
                                A\t
                                B\t
                                C\t
                                D\t
                                E\t
                                """,
                        sql,
                        null,
                        sqlExecutionContext,
                        true,
                        true
                );
            }, configuration, LOG);
        }
    }

    @Test
    public void testParallelKeyedSparklineMatchesUnderContention() throws Exception {
        // Keyed counterpart of testParallelSparklineMatchesUnderContention: a
        // GROUP BY over a SYMBOL key drives the computeKeyedBatch reduce path,
        // and the low sharding threshold forces the sharded merge (compactInto)
        // path. Every key shares the same value sequence, so every group must
        // render the same staircase; a deviation means the per-frame batch
        // descriptors were not honoured when merging a key's partial results.
        // The one-arg sparkline renders one char per row, so every level value
        // lands on its own char and any out-of-order batch changes the string.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, ROWS_PER_FRAME);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        final int steps = LEVELS * STEPS_PER_LEVEL;
        final String expected = expectedStaircase(STEPS_PER_LEVEL);
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (key SYMBOL, val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                            sqlExecutionContext
                    );
                    StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                    boolean first = true;
                    for (int s = 0; s < steps; s++) {
                        final int level = s / STEPS_PER_LEVEL;
                        for (int k = 0; k < KEY_COUNT; k++) {
                            if (!first) {
                                sb.append(",\n");
                            }
                            first = false;
                            sb.append("('k").append(k).append("', ")
                                    .append((double) level).append(", ").append((long) s * 1000).append(')');
                        }
                    }
                    engine.execute(sb.toString(), sqlExecutionContext);

                    final CyclicBarrier barrier = new CyclicBarrier(CONTENTION_THREADS);
                    final SOCountDownLatch latch = new SOCountDownLatch(CONTENTION_THREADS);
                    final Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicInteger mismatches = new AtomicInteger();
                    final AtomicReference<String> sampleWrongValue = new AtomicReference<>(null);

                    for (int t = 0; t < CONTENTION_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            try {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < CONTENTION_ITERATIONS; iter++) {
                                    mismatches.addAndGet(countKeyedSparklineMismatches(
                                            engine, sqlExecutionContext, expected, sampleWrongValue));
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                latch.countDown();
                            }
                        }, "sparkline-keyed-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());
                    Assert.assertEquals(
                            "every group's sparkline() must render the exact staircase regardless of per-slot "
                                    + "frame ordering on the keyed (computeKeyedBatch + sharded merge) reduce path. "
                                    + "Wrong sample: " + sampleWrongValue.get(),
                            0, mismatches.get()
                    );
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testParallelSparklineMatchesUnderContention() throws Exception {
        // Forces the work-stealing contention that broke twap/sparkline in
        // issue #7123: tiny page frames plus a work-stealing threshold of 1 let
        // a per-slot buffer accumulate frames out of rowId order, so the render
        // path (compactInPlace) must sort the batches back before walking the
        // value sequence. The dataset is LEVELS value levels, each spanning
        // FRAMES_PER_LEVEL frames; rendering one char per frame yields a strict
        // staircase whose every character comes from a single level value, so any
        // batch left out of order changes the string. Unlike the assertSqlCursors
        // self-comparison tests, this asserts the exact, independently-known output.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, ROWS_PER_FRAME);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1_000_000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_BATCH_SIZE, 8);

        final int rows = NON_KEYED_WIDTH * ROWS_PER_FRAME;
        final String sql = "SELECT sparkline(val, 0.0, 7.0, " + NON_KEYED_WIDTH + ") FROM tab";
        final String expected = expectedStaircase(FRAMES_PER_LEVEL);
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 2)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            "CREATE TABLE tab (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR",
                            sqlExecutionContext
                    );
                    StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
                    for (int r = 0; r < rows; r++) {
                        if (r > 0) {
                            sb.append(",\n");
                        }
                        final int level = r / (FRAMES_PER_LEVEL * ROWS_PER_FRAME);
                        sb.append('(').append((double) level).append(", ").append((long) r * 1000).append(')');
                    }
                    engine.execute(sb.toString(), sqlExecutionContext);

                    final CyclicBarrier barrier = new CyclicBarrier(CONTENTION_THREADS);
                    final SOCountDownLatch latch = new SOCountDownLatch(CONTENTION_THREADS);
                    final Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
                    final AtomicInteger mismatches = new AtomicInteger();
                    final AtomicInteger threadsThatSawMismatches = new AtomicInteger();
                    final AtomicReference<String> sampleWrongValue = new AtomicReference<>(null);

                    for (int t = 0; t < CONTENTION_THREADS; t++) {
                        final int threadId = t;
                        new Thread(() -> {
                            int localMismatches = 0;
                            try {
                                TestUtils.await(barrier);
                                for (int iter = 0; iter < CONTENTION_ITERATIONS; iter++) {
                                    String observed = runSparkline(engine, sqlExecutionContext, sql);
                                    if (!expected.equals(observed)) {
                                        localMismatches++;
                                        sampleWrongValue.compareAndSet(null, observed);
                                    }
                                }
                            } catch (Throwable th) {
                                errors.put(threadId, th);
                            } finally {
                                if (localMismatches > 0) {
                                    threadsThatSawMismatches.incrementAndGet();
                                    mismatches.addAndGet(localMismatches);
                                }
                                latch.countDown();
                            }
                        }, "sparkline-repro-" + threadId).start();
                    }
                    latch.await();

                    for (Map.Entry<Integer, Throwable> e : errors.entrySet()) {
                        e.getValue().printStackTrace(System.out);
                    }
                    Assert.assertTrue("thread errors: " + errors, errors.isEmpty());
                    Assert.assertEquals(
                            "sparkline() must render the exact staircase on every iteration. Any deviation under "
                                    + "this contention setup would mean the compaction step in SortedRunsMerge either "
                                    + "was not invoked or failed to restore key order before the render walk. Observed "
                                    + "wrong sample: " + sampleWrongValue.get() + " across "
                                    + threadsThatSawMismatches.get() + " thread(s), " + mismatches.get()
                                    + " mismatches total over " + CONTENTION_THREADS + " threads x "
                                    + CONTENTION_ITERATIONS + " iterations.",
                            0, mismatches.get()
                    );
                }, configuration, LOG);
            }
        });
    }

    // Runs the keyed sparkline query and returns the number of groups whose
    // rendered sparkline deviates from the exact expected staircase; a wrong
    // group count also counts as a mismatch so a dropped group is caught.
    private static int countKeyedSparklineMismatches(
            CairoEngine engine,
            SqlExecutionContext ctx,
            String expected,
            AtomicReference<String> sampleWrongValue
    ) throws Exception {
        int mismatches = 0;
        int groups = 0;
        try (RecordCursorFactory factory = engine.select("SELECT key, sparkline(val) FROM tab", ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                groups++;
                final String observed = Utf8s.toString(record.getVarcharA(1));
                if (!expected.equals(observed)) {
                    mismatches++;
                    sampleWrongValue.compareAndSet(null, observed);
                }
            }
        }
        if (groups != KEY_COUNT) {
            mismatches++;
        }
        return mismatches;
    }

    // Builds the expected block-staircase: each of the LEVELS level characters
    // repeated charsPerLevel times. This is the rendered output for a correctly
    // key-ordered buffer, derived from the dataset shape rather than from the
    // render code path.
    private static String expectedStaircase(int charsPerLevel) {
        StringBuilder sb = new StringBuilder(LEVELS * charsPerLevel);
        for (char c : LEVEL_CHARS) {
            for (int i = 0; i < charsPerLevel; i++) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String runSparkline(CairoEngine engine, SqlExecutionContext ctx, String sql) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            if (!cursor.hasNext()) {
                return null;
            }
            return Utf8s.toString(record.getVarcharA(0));
        }
    }
}
