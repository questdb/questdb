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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.SparklineGroupByFunction;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SparklineGroupByFunctionFactoryTest extends AbstractCairoTest {

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
            assertSql(
                    """
                            sparkline
                            
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▂▃▄▅▆▇█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄██
                            """,
                    "SELECT sparkline(val, 0.0, 100.0, 4) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▁▄█
                            """,
                    "SELECT sparkline(val, 0.0, 100.0, 4) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ███
                            """,
                    "SELECT sparkline(val) FROM t"
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertSql(
                    """
                            sparkline
                            
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁█▄
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            grp\tspark
                            up\t▁▄█
                            down\t█▄▁
                            flat\t███
                            """,
                    "SELECT grp, sparkline(val) spark FROM t ORDER BY spark"
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
            assertSql(
                    """
                            grp\tspark
                            blank\t
                            up\t▁█
                            down\t█▁
                            """,
                    "SELECT grp, sparkline(val) spark FROM t ORDER BY spark"
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
            assertSql(
                    """
                            grp\tsparkline
                            down\t█▄▁
                            up\t▁▄█
                            """,
                    "SELECT grp, sparkline(val) FROM t ORDER BY grp"
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
            assertSql(
                    """
                            sparkline
                            ▁▂▄▆█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            ts\tsparkline
                            2024-01-01T00:00:00.000000Z\t▁█
                            2024-01-01T01:00:00.000000Z\t██
                            """,
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h"
            );
        });
    }

    @Test
    public void testSingleValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (val DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO t VALUES (42.0, '2024-01-01T00:00:00.000000Z')");
            // Single value -> min==max -> top char
            assertSql(
                    """
                            sparkline
                            █
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁█
                            """,
                    "SELECT sparkline(val, NULL, NULL, 2) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▂▄▆
                            """,
                    "SELECT sparkline(val, 0.0, 100.0, 3) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▃▅█
                            """,
                    "SELECT sparkline(val, 0.0, NULL, 3) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val, NULL, 10.0, 3) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val, NULL, NULL, 3) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            sparkline
                            ▁▁▂▃▄▄▅▆▇█
                            """,
                    "SELECT sparkline(val) FROM t"
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
            assertSql(
                    """
                            ts\tsparkline
                            2024-01-01T00:00:00.000000Z\t▁█
                            2024-01-01T01:00:00.000000Z\t
                            2024-01-01T02:00:00.000000Z\t██
                            """,
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h FILL(NULL)"
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
            assertSql(
                    """
                            ts\tsparkline
                            2024-01-01T00:00:00.000000Z\t▁█
                            2024-01-01T01:00:00.000000Z\t▁█
                            2024-01-01T02:00:00.000000Z\t██
                            """,
                    "SELECT ts, sparkline(val) FROM t SAMPLE BY 1h FILL(PREV)"
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
            assertSql(
                    """
                            sparkline
                            ▁▄█
                            """,
                    "SELECT sparkline(val) FROM t"
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
                TestUtils.assertSql(
                        engine,
                        sqlExecutionContext,
                        sql,
                        sink,
                        "sym\tsparkline\nA\t\nB\t\nC\t\nD\t\nE\t\n"
                );
            }, configuration, LOG);
        }
    }
}
