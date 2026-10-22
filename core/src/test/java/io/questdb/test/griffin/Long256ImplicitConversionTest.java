/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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


package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class Long256ImplicitConversionTest extends AbstractCairoTest {
    private static final String DDL = """
            CREATE TABLE t (k SYMBOL, l LONG256, i INT, j LONG, s SHORT, b BYTE)
            """;
    private static final String DML = """
            INSERT INTO t VALUES
                ('a', 0x8000000000000000000000000000000000000000000000000000000000000001, 1, 1, 1, 1),
                ('b', NULL, NULL, NULL, 0, 0),
                ('c', 0x01, 1, 1, 1, 1),
                ('d', 0xffffffffffffffff, -1, -1, -1, -1)
            """;

    @Test
    public void testArithmeticWithIntegerOperands() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(DML);
            assertQuery("SELECT k, l + 1, 1 + l, l + i FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tcolumn\tcolumn1\tcolumn2
                            a\t0x8000000000000000000000000000000000000000000000000000000000000002\t0x8000000000000000000000000000000000000000000000000000000000000002\t0x8000000000000000000000000000000000000000000000000000000000000002
                            b\t\t\t
                            c\t0x02\t0x02\t0x02
                            d\t0x010000000000000000\t0x010000000000000000\t0x01fffffffffffffffe
                            """);
        });
    }

    @Test
    public void testCallsWithoutLong256OverloadFail() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute("CREATE TABLE x (l LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertFails("SELECT min(l) FROM t", 7, "there is no matching function `min` with the argument types: (LONG256)");
            assertFails("SELECT max(l) FROM t", 7, "there is no matching function `max` with the argument types: (LONG256)");
            assertFails("SELECT avg(l) FROM t", 7, "there is no matching function `avg` with the argument types: (LONG256)");
            assertFails("SELECT abs(l) FROM t", 7, "there is no matching function `abs` with the argument types: (LONG256)");
            assertFails("SELECT mode(l) FROM t", 7, "there is no matching function `mode` with the argument types: (LONG256)");
            assertFails("SELECT bit_and(l) FROM t", 7, "there is no matching function `bit_and` with the argument types: (LONG256)");
            assertFails("SELECT approx_count_distinct(l) FROM t", 7, "there is no matching function `approx_count_distinct` with the argument types: (LONG256)");
            assertFails("SELECT lag(l) OVER () FROM t", 7, "there is no matching function `lag` with the argument types: (LONG256)");
            assertFails("SELECT first_value(l) OVER () FROM t", 7, "there is no matching function `first_value` with the argument types: (LONG256)");
            assertFails("SELECT max(l) OVER () FROM t", 7, "there is no matching function `max` with the argument types: (LONG256)");
            assertFails("SELECT -l FROM t", 7, "there is no matching operator `-` with the argument type: LONG256");
            assertFails("SELECT l - 1 FROM t", 9, "there is no matching operator `-` with the argument types: LONG256 - INT");
            assertFails("SELECT l * 2 FROM t", 9, "there is no matching operator `*` with the argument types: LONG256 * INT");
            assertFails("SELECT l & 1 FROM t", 9, "there is no matching operator `&` with the argument types: LONG256 & INT");
            assertFails("SELECT l::decimal(76, 0) FROM t", 8, "there is no matching function `cast` with the argument types: (LONG256, DECIMAL(76,0))");
            assertFails("SELECT l = (SELECT l FROM t LIMIT 1) FROM t", 9, "there is no matching operator `=` with the argument types: LONG256 = CURSOR");
            assertFails("SELECT l IN (i) FROM t", 13, "constant expected");
            assertFails("SELECT first(l), ts FROM x SAMPLE BY 1h FILL(1)", 45, "fill value of type INT cannot fill column of type LONG256");
        });
    }

    @Test
    public void testComparisonWithIntegerOperands() throws Exception {
        // An integer widens to LONG256 like an explicit ::long256 cast: a negative value keeps its two's
        // complement in the low 64 bits and zeroes the rest, so -1 compares as 0xffffffffffffffff.
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(DML);
            assertQuery("SELECT k, l = 1, l != 1, l > 5, l <= 1, 1 = l, l > -1, l = i, l = j, l = s, l = b FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tcolumn\tcolumn1\tcolumn2\tcolumn3\tcolumn4\tcolumn5\tcolumn6\tcolumn7\tcolumn8\tcolumn9
                            a\tfalse\ttrue\ttrue\tfalse\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse
                            b\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue\ttrue\tfalse\tfalse
                            c\ttrue\tfalse\tfalse\ttrue\ttrue\tfalse\ttrue\ttrue\ttrue\ttrue
                            d\tfalse\ttrue\ttrue\tfalse\tfalse\tfalse\ttrue\ttrue\ttrue\ttrue
                            """);
            assertQuery("SELECT k FROM t WHERE l = 1")
                    .noLeakCheck()
                    .returns("""
                            k
                            c
                            """);
            // An integer widens only to meet a LONG256 operand: a BYTE against text keeps its own comparison.
            assertQuery("SELECT b = 'abc' FROM t WHERE k = 'a'")
                    .noLeakCheck()
                    .returns("""
                            column
                            false
                            """);
        });
    }

    @Test
    public void testIn() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(DML);
            assertQuery("""
                    SELECT k, l IN (1, 2), l NOT IN (1, 2), l IN (-1, '0x8000000000000000000000000000000000000000000000000000000000000001'),
                    l IN (NULL), l IN (0x8000000000000000000000000000000000000000000000000000000000000001) FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tcolumn\tcolumn1\tcolumn2\tcolumn3\tcolumn4
                            a\tfalse\ttrue\ttrue\tfalse\ttrue
                            b\tfalse\ttrue\tfalse\ttrue\tfalse
                            c\ttrue\tfalse\tfalse\tfalse\tfalse
                            d\tfalse\ttrue\ttrue\tfalse\tfalse
                            """);
            assertQuery("SELECT k FROM t WHERE l IN (1)")
                    .noLeakCheck()
                    .withPlanContaining("Async Filter", "filter: l in [0x01]")
                    .returns("""
                            k
                            c
                            """);
            bindVariableService.clear();
            bindVariableService.setLong(0, 1);
            bindVariableService.setStr(1, "0x8000000000000000000000000000000000000000000000000000000000000001");
            assertQuery("SELECT k FROM t WHERE l IN ($1, $2)")
                    .noLeakCheck()
                    .returns("""
                            k
                            a
                            c
                            """);
            bindVariableService.clear();
            bindVariableService.setInt(0, -1);
            bindVariableService.setStr(1, null);
            assertQuery("SELECT k FROM t WHERE l IN ($1, $2)")
                    .noLeakCheck()
                    .returns("""
                            k
                            b
                            d
                            """);
            bindVariableService.clear();
            assertFails("SELECT l IN ('abc') FROM t", 13, "invalid LONG256 value [abc]");
            assertFails("SELECT l IN (1.5) FROM t", 13, "cannot compare LONG256 with type DOUBLE");
        });
    }

    @Test
    public void testInParallelFilter() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)))) {
                TestUtils.execute(pool, (ignore, compiler, ctx) -> {
                    // Odd rows carry a high word, so only the even rows match an integer by all 256 bits.
                    execute(compiler, """
                            CREATE TABLE p AS (
                                SELECT x, CASE WHEN x % 2 = 0 THEN x::long256 ELSE to_long256(x, 0, 0, 1) END AS l
                                FROM long_sequence(10_000)
                            )
                            """, ctx);
                    assertQuery("SELECT x FROM p WHERE l IN (1, 2, 3, 4, 9_999, 10_000)")
                            .noLeakCheck()
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .assertsPlanContaining("Async Filter");
                    assertQuery("SELECT x FROM p WHERE l IN (1, 2, 3, 4, 9_999, 10_000)")
                            .noLeakCheck()
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .returns("""
                                    x
                                    2
                                    4
                                    10000
                                    """);
                    ctx.getBindVariableService().clear();
                    ctx.getBindVariableService().setLong(0, 10_000);
                    assertQuery("SELECT x FROM p WHERE l IN (2, $1)")
                            .noLeakCheck()
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .assertsPlanContaining("Async Filter");
                    assertQuery("SELECT x FROM p WHERE l IN (2, $1)")
                            .noLeakCheck()
                            .withCompiler(compiler)
                            .withContext(ctx)
                            .returns("""
                                    x
                                    2
                                    10000
                                    """);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testNullIf() throws Exception {
        assertMemoryLeak(() -> {
            execute(DDL);
            execute(DML);
            assertQuery("""
                    SELECT k, nullif(l, l), nullif(l, 1), nullif(l, i),
                    nullif(l, 0x8000000000000000000000000000000000000000000000000000000000000001) FROM t
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\tnullif\tnullif1\tnullif2\tnullif3
                            a\t\t0x8000000000000000000000000000000000000000000000000000000000000001\t0x8000000000000000000000000000000000000000000000000000000000000001\t
                            b\t\t\t\t
                            c\t\t\t\t0x01
                            d\t\t0xffffffffffffffff\t\t0xffffffffffffffff
                            """);
        });
    }

    private void assertFails(String sql, int position, String message) throws Exception {
        assertQuery(sql).noLeakCheck().fails(position, message);
    }
}
