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
import io.questdb.test.AbstractCairoTest;
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[null,null,null]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testCompactionSentinelSkipPath() throws Exception {
        // The getArray() compaction step writes -1 into the capacity slot so subsequent
        // calls on the same group skip re-compaction. Project the aggregate alongside a
        // derivation so the outer expression reads it more than once on the same group,
        // forcing the second call to traverse the already-compacted buffer.
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
            assertQueryNoLeakCheck(
                    "grp\tarr\tcnt\tsum\n" +
                            "a\t[1.0,2.0,3.0]\t3\t6.0\n" +
                            "b\t[10.0,20.0]\t2\t30.0\n",
                    "SELECT grp, arr, array_count(arr) cnt, array_sum(arr) sum " +
                            "FROM (SELECT grp, array_agg(val) arr FROM tab) ORDER BY grp",
                    null,
                    true,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[42.0,42.0,42.0]\n",
                    "SELECT array_agg(42.0) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "null\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "grp\tarr\n" +
                            "a\t[1.0,2.0,3.0]\n" +
                            "b\t[10.0,20.0]\n",
                    "SELECT grp, array_agg(val) arr FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[1.0,2.0,3.0,4.0,5.0]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[1.0,2.0,3.0]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "grp\tarr\tavg\n" +
                            "a\t[10.0,20.0,30.0]\t20.0\n" +
                            "b\t[100.0,200.0]\t150.0\n",
                    "SELECT grp, array_agg(val) arr, avg(val) avg FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[1.0,null,3.0,null,5.0]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[5.0,3.0,1.0,4.0,2.0]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[null,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,null,16.0,-17.5,null]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "ts\tarr\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]\n" +
                            "2024-01-01T03:00:00.000000Z\t[4.0,5.0]\n" +
                            "2024-01-01T09:00:00.000000Z\t[6.0]\n",
                    "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 3h ALIGN TO FIRST OBSERVATION",
                    "ts"
            );
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
            assertQueryNoLeakCheck(
                    "ts\tarr\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0]\n" +
                            "2024-01-01T01:00:00.000000Z\t[3.0,4.0]\n",
                    "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 1h ALIGN TO CALENDAR",
                    "ts",
                    true,
                    true
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
            assertQueryNoLeakCheck(
                    "ts\tarr\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0]\n" +
                            "2024-01-01T03:00:00.000000Z\t[3.0]\n" +
                            "2024-01-01T06:00:00.000000Z\t[4.0,5.0,null]\n",
                    "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(NONE)",
                    "ts",
                    true,
                    true
            );
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
            assertQueryNoLeakCheck(
                    "ts\tarr\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0]\n" +
                            "2024-01-01T02:00:00.000000Z\tnull\n" +
                            "2024-01-01T04:00:00.000000Z\t[3.0]\n",
                    "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 2h FILL(NULL)",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testSampleByFillLinearRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, grp SYMBOL, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', 'a', 1.0)");
            assertExceptionNoLeakCheck(
                    "SELECT ts, grp, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(LINEAR)",
                    16,
                    "support for LINEAR fill is not yet implemented"
            );
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
            assertQueryNoLeakCheck(
                    "ts\tarr\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0]\n" +
                            "2024-01-01T02:00:00.000000Z\t[1.0,2.0]\n" +
                            "2024-01-01T04:00:00.000000Z\t[3.0]\n",
                    "SELECT ts, array_agg(val) arr FROM tab SAMPLE BY 2h FILL(PREV)",
                    "ts"
            );
        });
    }

    @Test
    public void testSampleByFillValueRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (ts TIMESTAMP, grp SYMBOL, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tab VALUES ('2024-01-01T00:00:00', 'a', 1.0)");
            assertExceptionNoLeakCheck(
                    "SELECT ts, grp, array_agg(val) arr FROM tab SAMPLE BY 1h FILL(42)",
                    16,
                    "support for VALUE fill is not yet implemented"
            );
        });
    }

    @Test
    public void testSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            execute("INSERT INTO tab VALUES (42.0)");
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[42.0]\n",
                    "SELECT array_agg(val) arr FROM tab",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testParallelCounts() throws Exception {
        // Shrink the page frame so the 10_000-row insert spans many frames,
        // forcing multi-worker dispatch and exercising the parallel merge path.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
            for (int i = 0; i < 10_000; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('g").append(i % 10).append("', ").append(i).append(".0)");
            }
            execute(sb.toString());
            assertQueryNoLeakCheck(
                    "grp\tcnt\ttotal\n" +
                            "g0\t1000\t4995000.0\n" +
                            "g1\t1000\t4996000.0\n" +
                            "g2\t1000\t4997000.0\n" +
                            "g3\t1000\t4998000.0\n" +
                            "g4\t1000\t4999000.0\n" +
                            "g5\t1000\t5000000.0\n" +
                            "g6\t1000\t5001000.0\n" +
                            "g7\t1000\t5002000.0\n" +
                            "g8\t1000\t5003000.0\n" +
                            "g9\t1000\t5004000.0\n",
                    "SELECT grp, array_count(array_agg(val)) cnt, array_sum(array_agg(val)) total FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testParallelOrdering() throws Exception {
        // Shrink the page frame so the 10_000-row insert spans many frames,
        // forcing multi-worker dispatch and exercising the merge-sort in merge().
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, val DOUBLE)");
            // 10 groups x 1000 rows. Row i goes to group g(i%10) with value i.0.
            // Group gN receives values N, N+10, N+20, ..., N+9990 in insertion order.
            StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
            for (int i = 0; i < 10_000; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('g").append(i % 10).append("', ").append(i).append(".0)");
            }
            execute(sb.toString());
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
            assertQueryNoLeakCheck(
                    expected.toString(),
                    "SELECT grp, array_agg(val) arr FROM tab ORDER BY grp",
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
        // Shrink the page frame so per-worker counts stay below the 9_999-element
        // limit while the merged count crosses it, exercising the capacity check
        // inside merge(). Without this, the 10_000-row insert fits in a single
        // page frame and only the computeNext check runs.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_SQL_MAX_ARRAY_ELEMENT_COUNT, 9_999);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (val DOUBLE)");
            StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
            for (int i = 0; i < 10_000; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("(").append(i).append(".0)");
            }
            execute(sb.toString());
            assertExceptionNoLeakCheck(
                    "SELECT array_agg(val) FROM tab",
                    0,
                    "array_agg: array size exceeds configured maximum [maxArrayElementCount=9999]"
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
            assertQueryNoLeakCheck(
                    "arr\n" +
                            "[1.0,2.0,3.0]\n",
                    "SELECT arr FROM (SELECT array_agg(val) arr FROM tab)",
                    null,
                    false,
                    true
            );
        });
    }
}
