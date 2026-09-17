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

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * A NOT NULL INT or LONG column carries the type's full signed range. QuestDB spends MIN_VALUE on
 * the NULL marker only where a column can be null, so on a NOT NULL column 0x8000_0000 and
 * 0x8000_0000_0000_0000 are ordinary values that bit_or folds like any other bit pattern. The
 * tests below write them as {@code '-2147483648'::int} and {@code '-9223372036854775808'::long}.
 * For LONG that spelling is the only safe one: the bare literal {@code -9223372036854775808} parses
 * as DOUBLE and fails the implicit cast to LONG, while {@code -9223372036854775808::long} binds the
 * cast to the positive literal, which saturates to LONG_MAX before the unary minus and stores
 * -9223372036854775807. The INT tests use the same spelling so both widths read alike.
 */
public class BitOrGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBitOrLong() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (" +
                        "select 1::long as val from long_sequence(1) " +
                        "union all select 2::long as val from long_sequence(1) " +
                        "union all select 4::long as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        7
                        """);
    }

    @Test
    public void testBitOrLongAllSame() throws Exception {
        // 7 | 7 | 7 = 7
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (select 7::long as val from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        7
                        """);
    }

    @Test
    public void testBitOrLongWithGroupBy() throws Exception {
        assertQuery("select grp, bit_or(val) from tab order by grp")
                .ddl("create table tab as (" +
                        "select 'a' as grp, 5::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'a' as grp, 2::long as val from long_sequence(1) " +
                        "union all " +
                        "select 'b' as grp, 7::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'b' as grp, 8::long as val from long_sequence(1)" +
                        ")")
                .expectSize()
                .returns("""
                        grp\tbit_or
                        a\t7
                        b\t15
                        """);
    }

    @Test
    public void testBitOrInt() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (" +
                        "select 1::int as val from long_sequence(1) " +
                        "union all select 2::int as val from long_sequence(1) " +
                        "union all select 4::int as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        7
                        """);
    }

    @Test
    public void testBitOrIntWithNull() throws Exception {
        // 1 | 2 = 3 (nulls are skipped)
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (" +
                        "select 1::int as val from long_sequence(1) " +
                        "union all " +
                        "select null::int as val from long_sequence(2) " +
                        "union all " +
                        "select 2::int as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        3
                        """);
    }

    @Test
    public void testBitOrShort() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (" +
                        "select 1::short as val from long_sequence(1) " +
                        "union all select 2::short as val from long_sequence(1) " +
                        "union all select 4::short as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        7
                        """);
    }

    @Test
    public void testBitOrByte() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (" +
                        "select 1::byte as val from long_sequence(1) " +
                        "union all select 2::byte as val from long_sequence(1) " +
                        "union all select 4::byte as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        7
                        """);
    }

    @Test
    public void testBitOrConstant() throws Exception {
        assertQuery("select bit_or(42::long) from tab")
                .ddl("create table tab as (select x from long_sequence(5))")
                .expectSize()
                .returns("""
                        bit_or
                        42
                        """);
    }

    @Test
    public void testBitOrLongWithNull() throws Exception {
        // 1 | 2 = 3 (nulls are skipped)
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (" +
                        "select 1::long as val from long_sequence(1) " +
                        "union all " +
                        "select null::long as val from long_sequence(2) " +
                        "union all " +
                        "select 2::long as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        3
                        """);
    }

    @Test
    public void testBitOrLongEmptyTable() throws Exception {
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab (val long)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        null
                        """);
    }

    @Test
    public void testBitOrLongAllNull() throws Exception {
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (select null::long as val from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        null
                        """);
    }

    @Test
    public void testBitOrIntEmptyTable() throws Exception {
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab (val int)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        null
                        """);
    }

    @Test
    public void testBitOrIntAllNull() throws Exception {
        assertQuery("select bit_or(val) from tab")
                .ddl("create table tab as (select null::int as val from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_or
                        null
                        """);
    }

    @Test
    public void testBitOrLongParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrLongParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_long(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrIntParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_int(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrIntParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_int(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrIntNotNullEmptyGroupIsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nn (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO nn VALUES ('a', '-2147483648'::int), ('a', 1)");
            // An empty population never reaches the accumulator, so setEmpty()'s INT_NULL
            // survives and the group still reads back as NULL on a NOT NULL column.
            assertQuery("select bit_or(v) from nn where false")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            bit_or
                            null
                            """);
        });
    }

    @Test
    public void testBitOrIntNullableNullStaysAbsentInput() throws Exception {
        assertMemoryLeak(() -> {
            // On a nullable column 0x8000_0000 is still an absent input rather than data, so
            // bit_or keeps skipping it and the only contributing row leaves
            //   1 = 0x0000_0001  0000...0001
            execute("CREATE TABLE nullable (g SYMBOL, v INT)");
            execute("INSERT INTO nullable VALUES ('a', NULL), ('a', 1)");
            assertBitOrBothPlans("nullable", "1");
        });
    }

    @Test
    public void testBitOrIntNotNullSentinelKeyedBatch() throws Exception {
        // Pins the Async Group By plan, whose keyed aggregation runs through
        // computeKeyedBatch instead of computeFirst/computeNext. Group 'a' spans four
        // 2_048-row batches and its INT_MIN row sits in the second one, so the
        // accumulator holds INT_MIN on a map entry that is no longer a fresh batch entry.
        //   3_000 x 0     = 0x0000_0000 folds to 0x0000_0000
        //   ... | INT_MIN = 0x8000_0000, the accumulator IS the bit pattern a nullable column
        //                   would spend on NULL, and on this column it is data, not an empty slot
        //   ... | 1       = 0x8000_0001 = -2147483647, held by the 3_000 trailing ones
        // Group 'b' folds 100 sevens to 7 and never reaches that bit pattern.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO t SELECT 'a', 0::int FROM long_sequence(3_000)");
            execute("INSERT INTO t VALUES ('a', '-2147483648'::int)");
            execute("INSERT INTO t SELECT 'a', 1::int FROM long_sequence(3_000)");
            execute("INSERT INTO t SELECT 'b', 7::int FROM long_sequence(100)");
            assertQuery("select g, bit_or(v) from t order by g")
                    .noLeakCheck()
                    .withPlanContaining("Async Group By", "keys: [g]")
                    .expectSize()
                    .returns("""
                            g\tbit_or
                            a\t-2147483647
                            b\t7
                            """);
        });
    }

    @Test
    public void testBitOrIntNotNullSentinelOrderIndependent() throws Exception {
        // A NOT NULL INT column carries the full signed range, 0x8000_0000 included, and bit_or
        // folds that bit pattern like any other, so the answer does not depend on row order:
        //   INT_MIN = 0x8000_0000  1000...0000
        //   0       = 0x0000_0000  0000...0000
        //   1       = 0x0000_0001  0000...0001
        //
        //   INT_MIN | 0 = 0x8000_0000, the accumulator IS the bit pattern a nullable column
        //                 would spend on NULL, and on this column it is data, not an empty slot
        //   ...     | 1 = 0x8000_0001 = -2147483647
        // 1 | INT_MIN is the same 0x8000_0001, so the sentinel-first, sentinel-last and
        // sentinel-held tables all end at -2147483647.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sentinelFirst (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO sentinelFirst VALUES ('a', '-2147483648'::int), ('a', 1)");
            execute("CREATE TABLE sentinelLast (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO sentinelLast VALUES ('a', 1), ('a', '-2147483648'::int)");
            // The middle row keeps the accumulator at 0x8000_0000: INT_MIN | 0 == INT_MIN.
            execute("CREATE TABLE sentinelHeld (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO sentinelHeld VALUES ('a', '-2147483648'::int), ('a', 0), ('a', 1)");
            // The stored bits really are 0x8000_0000, not a pattern one off it.
            assertQuery("select v from sentinelHeld")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            -2147483648
                            0
                            1
                            """);
            assertBitOrBothPlans("sentinelFirst", "-2147483647");
            assertBitOrBothPlans("sentinelLast", "-2147483647");
            assertBitOrBothPlans("sentinelHeld", "-2147483647");
        });
    }

    @Test
    public void testBitOrLongNotNullEmptyGroupIsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nn (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO nn VALUES ('a', '-9223372036854775808'::long), ('a', 1)");
            // An empty population never reaches the accumulator, so setEmpty()'s LONG_NULL
            // survives and the group still reads back as NULL on a NOT NULL column.
            assertQuery("select bit_or(v) from nn where false")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            bit_or
                            null
                            """);
        });
    }

    @Test
    public void testBitOrLongNullableNullStaysAbsentInput() throws Exception {
        assertMemoryLeak(() -> {
            // On a nullable column 0x8000_0000_0000_0000 is still an absent input rather than
            // data, so bit_or keeps skipping it and the only contributing row leaves
            //   1 = 0x0000_0000_0000_0001  0000...0001
            execute("CREATE TABLE nullable (g SYMBOL, v LONG)");
            execute("INSERT INTO nullable VALUES ('a', NULL), ('a', 1)");
            assertBitOrBothPlans("nullable", "1");
        });
    }

    @Test
    public void testBitOrLongNotNullSentinelKeyedBatch() throws Exception {
        // Pins the Async Group By plan, whose keyed aggregation runs through
        // computeKeyedBatch instead of computeFirst/computeNext. Group 'a' spans four
        // 2_048-row batches and its LONG_MIN row sits in the second one, so the
        // accumulator holds LONG_MIN on a map entry that is no longer a fresh batch entry.
        //   3_000 x 0      = 0x0000_0000_0000_0000 folds to 0x0000_0000_0000_0000
        //   ... | LONG_MIN = 0x8000_0000_0000_0000, the accumulator IS the bit pattern a nullable
        //                    column would spend on NULL, and on this column it is data, not an
        //                    empty slot
        //   ... | 1        = 0x8000_0000_0000_0001 = -9223372036854775807, held by the 3_000
        //                    trailing ones
        // Group 'b' folds 100 sevens to 7 and never reaches that bit pattern.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO t SELECT 'a', 0::long FROM long_sequence(3_000)");
            execute("INSERT INTO t VALUES ('a', '-9223372036854775808'::long)");
            execute("INSERT INTO t SELECT 'a', 1::long FROM long_sequence(3_000)");
            execute("INSERT INTO t SELECT 'b', 7::long FROM long_sequence(100)");
            assertQuery("select g, bit_or(v) from t order by g")
                    .noLeakCheck()
                    .withPlanContaining("Async Group By", "keys: [g]")
                    .expectSize()
                    .returns("""
                            g\tbit_or
                            a\t-9223372036854775807
                            b\t7
                            """);
        });
    }

    @Test
    public void testBitOrLongNotNullSentinelParallelMerge() throws Exception {
        // Eight daily partitions become eight page frames, so four workers each fold their own
        // partial and the partials meet in GroupByFunction.merge(). merge()'s javadoc guarantees
        // neither operand is a new map value, so a LONG_MIN accumulator crossing a merge stays
        // data.
        //   4_000 x 0      = 0x0000_0000_0000_0000 folds to 0x0000_0000_0000_0000
        //   ... | LONG_MIN = 0x8000_0000_0000_0000, the accumulator IS the bit pattern a nullable
        //                    column would spend on NULL, and on this column it is data, not an
        //                    empty slot
        //   ... | 1        = 0x8000_0000_0000_0001 = -9223372036854775807
        // OR is associative and commutative, so that is the answer whatever the frame split.
        execute("CREATE TABLE t (g SYMBOL, v LONG NOT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t SELECT 'a', " +
                "CASE WHEN x = 4_001 THEN '-9223372036854775808'::long WHEN x <= 4_000 THEN 0::long ELSE 1::long END, " +
                "((x - 1) * 86_400_000)::timestamp FROM long_sequence(8_000)");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                // Pin the multi-frame shape: without several partitions there is nothing to merge.
                assertQuery("select count() from table_partitions('t')")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count
                                8
                                """);
                assertQuery("select bit_or(v) from t")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                bit_or
                                -9223372036854775807
                                """);
                assertQuery("select g, bit_or(v) from t order by g")
                        .withEngine(engine)
                        .withContext(sqlExecutionContext)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                g\tbit_or
                                a\t-9223372036854775807
                                """);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrLongNotNullSentinelOrderIndependent() throws Exception {
        // A NOT NULL LONG column carries the full signed range, 0x8000_0000_0000_0000 included,
        // and bit_or folds that bit pattern like any other, so the answer does not depend on
        // row order:
        //   LONG_MIN = 0x8000_0000_0000_0000  1000...0000
        //   0        = 0x0000_0000_0000_0000  0000...0000
        //   1        = 0x0000_0000_0000_0001  0000...0001
        //
        //   LONG_MIN | 0 = 0x8000_0000_0000_0000, the accumulator IS the bit pattern a nullable
        //                  column would spend on NULL, and on this column it is data, not an
        //                  empty slot
        //   ...      | 1 = 0x8000_0000_0000_0001 = -9223372036854775807
        // 1 | LONG_MIN is the same 0x8000_0000_0000_0001, so the sentinel-first, sentinel-last
        // and sentinel-held tables all end at -9223372036854775807.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sentinelFirst (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO sentinelFirst VALUES ('a', '-9223372036854775808'::long), ('a', 1)");
            execute("CREATE TABLE sentinelLast (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO sentinelLast VALUES ('a', 1), ('a', '-9223372036854775808'::long)");
            // The middle row keeps the accumulator at 0x8000_0000_0000_0000: LONG_MIN | 0 == LONG_MIN.
            execute("CREATE TABLE sentinelHeld (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO sentinelHeld VALUES ('a', '-9223372036854775808'::long), ('a', 0), ('a', 1)");
            // The stored bits really are 0x8000_0000_0000_0000, not a pattern one off it.
            assertQuery("select v from sentinelHeld")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            -9223372036854775808
                            0
                            1
                            """);
            assertBitOrBothPlans("sentinelFirst", "-9223372036854775807");
            assertBitOrBothPlans("sentinelLast", "-9223372036854775807");
            assertBitOrBothPlans("sentinelHeld", "-9223372036854775807");
        });
    }

    /**
     * Asserts {@code bit_or(v)} over {@code table} equals {@code expected} for the unkeyed and
     * the {@code GROUP BY g} form, under both the parallel plan (Async Group By, which aggregates
     * through {@code computeKeyedBatch}) and the serial plan (which aggregates through
     * {@code computeFirst}/{@code computeNext}). Every table used here has a single group 'a'.
     */
    private void assertBitOrBothPlans(String table, String expected) throws Exception {
        for (boolean parallelGroupBy : new boolean[]{true, false}) {
            sqlExecutionContext.setParallelGroupByEnabled(parallelGroupBy);
            try {
                assertQuery("select bit_or(v) from " + table)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("bit_or\n" + expected + "\n");
                assertQuery("select g, bit_or(v) from " + table + " order by g")
                        .noLeakCheck()
                        .expectSize()
                        .returns("g\tbit_or\na\t" + expected + "\n");
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(true);
            }
        }
    }
}
