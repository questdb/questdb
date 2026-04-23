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
            assertSql(
                    "ts\tagg\n" +
                            "2024-01-01T00:00:00.000000Z\t[1.0,2.0,3.0]\n" +
                            "2024-01-01T03:00:00.000000Z\t[4.0,5.0]\n" +
                            "2024-01-01T06:00:00.000000Z\t[6.0]\n",
                    "SELECT ts, array_agg(arr) agg FROM tab SAMPLE BY 1h FILL(NONE)"
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
                    true,
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
    public void testParallelCounts() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])");
            StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('g").append(i % 5).append("', ARRAY[").append(i).append(".0, ").append(i + 1).append(".0])");
            }
            execute(sb.toString());
            // 200 arrays * 2 elements each = 400 elements per group
            assertQueryNoLeakCheck(
                    "grp\tcnt\ttotal\n" +
                            "g0\t400\t199200.0\n" +
                            "g1\t400\t199600.0\n" +
                            "g2\t400\t200000.0\n" +
                            "g3\t400\t200400.0\n" +
                            "g4\t400\t200800.0\n",
                    "SELECT grp, array_count(array_agg(arr)) cnt, array_sum(array_agg(arr)) total FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testParallelOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (grp SYMBOL, arr DOUBLE[])");
            // 10 groups x 1000 single-element arrays. Row i goes to group g(i%10) with ARRAY[i.0].
            // Group gN receives elements N.0, (N+10).0, ..., (N+9990).0 in insertion order.
            StringBuilder sb = new StringBuilder("INSERT INTO tab VALUES\n");
            for (int i = 0; i < 10_000; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('g").append(i % 10).append("', ARRAY[").append(i).append(".0])");
            }
            execute(sb.toString());
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
            assertQueryNoLeakCheck(
                    expected.toString(),
                    "SELECT grp, array_agg(arr) agg FROM tab ORDER BY grp",
                    null,
                    true,
                    true
            );
        });
    }
}
