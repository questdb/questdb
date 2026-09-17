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
 * 0x8000_0000_0000_0000 are ordinary values that bit_and folds like any other bit pattern. The
 * tests below write them as {@code '-2147483648'::int} and {@code '-9223372036854775808'::long}.
 * For LONG that spelling is the only safe one: the bare literal {@code -9223372036854775808} parses
 * as DOUBLE and fails the implicit cast to LONG, while {@code -9223372036854775808::long} binds the
 * cast to the positive literal, which saturates to LONG_MAX before the unary minus and stores
 * -9223372036854775807. The INT tests use the same spelling so both widths read alike.
 */
public class BitAndGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBitAndLong() throws Exception {
        // 7 & 3 & 5 = 1 (binary: 0111 & 0011 & 0101 = 0001)
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (" +
                        "select 7::long as val from long_sequence(1) " +
                        "union all select 3::long as val from long_sequence(1) " +
                        "union all select 5::long as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        1
                        """);
    }

    @Test
    public void testBitAndLongAllSame() throws Exception {
        // 7 & 7 & 7 = 7
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (select 7::long as val from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        7
                        """);
    }

    @Test
    public void testBitAndLongWithGroupBy() throws Exception {
        assertQuery("select grp, bit_and(val) from tab order by grp")
                .ddl("create table tab as (" +
                        "select 'a' as grp, 5::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'a' as grp, 2::long as val from long_sequence(1) " +
                        "union all " +
                        "select 'b' as grp, 7::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'b' as grp, 12::long as val from long_sequence(1)" +
                        ")")
                .expectSize()
                .returns("""
                        grp\tbit_and
                        a\t0
                        b\t4
                        """);
    }

    @Test
    public void testBitAndInt() throws Exception {
        // 7 & 3 & 5 = 1
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (" +
                        "select 7::int as val from long_sequence(1) " +
                        "union all select 3::int as val from long_sequence(1) " +
                        "union all select 5::int as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        1
                        """);
    }

    @Test
    public void testBitAndIntWithNull() throws Exception {
        // 7 & 3 = 3 (nulls are skipped)
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (" +
                        "select 7::int as val from long_sequence(2) " +
                        "union all " +
                        "select null::int as val from long_sequence(2) " +
                        "union all " +
                        "select 3::int as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        3
                        """);
    }

    @Test
    public void testBitAndShort() throws Exception {
        // 7 & 3 & 5 = 1
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (" +
                        "select 7::short as val from long_sequence(1) " +
                        "union all select 3::short as val from long_sequence(1) " +
                        "union all select 5::short as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        1
                        """);
    }

    @Test
    public void testBitAndByte() throws Exception {
        // 7 & 3 & 5 = 1
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (" +
                        "select 7::byte as val from long_sequence(1) " +
                        "union all select 3::byte as val from long_sequence(1) " +
                        "union all select 5::byte as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        1
                        """);
    }

    @Test
    public void testBitAndConstant() throws Exception {
        assertQuery("select bit_and(42::long) from tab")
                .ddl("create table tab as (select x from long_sequence(5))")
                .expectSize()
                .returns("""
                        bit_and
                        42
                        """);
    }

    @Test
    public void testBitAndLongWithNull() throws Exception {
        // 7 & 3 = 3 (nulls are skipped)
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (" +
                        "select 7::long as val from long_sequence(2) " +
                        "union all " +
                        "select null::long as val from long_sequence(2) " +
                        "union all " +
                        "select 3::long as val from long_sequence(1)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        3
                        """);
    }

    @Test
    public void testBitAndLongEmptyTable() throws Exception {
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab (val long)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        null
                        """);
    }

    @Test
    public void testBitAndLongAllNull() throws Exception {
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (select null::long as val from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        null
                        """);
    }

    @Test
    public void testBitAndIntEmptyTable() throws Exception {
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab (val int)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        null
                        """);
    }

    @Test
    public void testBitAndIntAllNull() throws Exception {
        assertQuery("select bit_and(val) from tab")
                .ddl("create table tab as (select null::int as val from long_sequence(5))")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        bit_and
                        null
                        """);
    }

    @Test
    public void testBitAndLongParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndLongParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_long(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndIntParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_int(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndIntParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_int(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, _, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndIntNotNullEmptyGroupIsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nn (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO nn VALUES ('a', -2), ('a', '-2147483648'::int), ('a', 1)");
            // An empty population never reaches the accumulator, so setEmpty()'s INT_NULL
            // survives and the group still reads back as NULL on a NOT NULL column.
            assertQuery("select bit_and(v) from nn where false")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            bit_and
                            null
                            """);
        });
    }

    @Test
    public void testBitAndIntNullableNullStaysAbsentInput() throws Exception {
        assertMemoryLeak(() -> {
            // On a nullable column 0x8000_0000 is still an absent input rather than data, so
            // bit_and keeps skipping it and folds -2 & 3:
            //   -2 = 0xFFFF_FFFE  1111...1110
            //   3  = 0x0000_0003  0000...0011
            //   -2 & 3 = 0x0000_0002 = 2
            // Bit 1 is what tells the two readings apart: folding 0x8000_0000 in as data would
            // clear it and leave 0x0000_0000.
            execute("CREATE TABLE nullable (g SYMBOL, v INT)");
            execute("INSERT INTO nullable VALUES ('a', -2), ('a', NULL), ('a', 3)");
            assertBitAndBothPlans("nullable", "2");
        });
    }

    @Test
    public void testBitAndIntNotNullSentinelKeyedBatch() throws Exception {
        // Pins the Async Group By plan, whose keyed aggregation runs through
        // computeKeyedBatch instead of computeFirst/computeNext. Group 'a' spans four
        // 2_048-row batches and its INT_MIN row sits in the second one, so the
        // accumulator holds INT_MIN on a map entry that is no longer a fresh batch entry.
        //   3_000 x -2    = 0xFFFF_FFFE folds to 0xFFFF_FFFE
        //   ... & INT_MIN = 0xFFFF_FFFE & 0x8000_0000 = 0x8000_0000, the accumulator IS the bit
        //                   pattern a nullable column would spend on NULL, and on this column it
        //                   is data, not an empty slot
        //   ... & 1       = 0x8000_0000 & 0x0000_0001 = 0, held by the 3_000 trailing ones
        // Group 'b' folds 100 sevens to 7 and never reaches that bit pattern.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO t SELECT 'a', -2::int FROM long_sequence(3_000)");
            execute("INSERT INTO t VALUES ('a', '-2147483648'::int)");
            execute("INSERT INTO t SELECT 'a', 1::int FROM long_sequence(3_000)");
            execute("INSERT INTO t SELECT 'b', 7::int FROM long_sequence(100)");
            assertQuery("select g, bit_and(v) from t order by g")
                    .noLeakCheck()
                    .withPlanContaining("Async Group By", "keys: [g]")
                    .expectSize()
                    .returns("""
                            g\tbit_and
                            a\t0
                            b\t7
                            """);
        });
    }

    @Test
    public void testBitAndIntNotNullSentinelOrderIndependent() throws Exception {
        // A NOT NULL INT column carries the full signed range, 0x8000_0000 included, and bit_and
        // folds that bit pattern like any other, so the answer does not depend on row order:
        //   -2      = 0xFFFF_FFFE  1111...1110
        //   INT_MIN = 0x8000_0000  1000...0000
        //   1       = 0x0000_0001  0000...0001
        //
        //   -2 & INT_MIN = 0x8000_0000, the accumulator IS the bit pattern a nullable column
        //                  would spend on NULL, and on this column it is data, not an empty slot
        //   ...     & 1  = 0x0000_0000 = 0
        // Any other order reaches 0 earlier: 1 & INT_MIN and -2 & 1 are both 0 already.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sentinelMiddle (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO sentinelMiddle VALUES ('a', -2), ('a', '-2147483648'::int), ('a', 1)");
            execute("CREATE TABLE sentinelLast (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO sentinelLast VALUES ('a', -2), ('a', 1), ('a', '-2147483648'::int)");
            execute("CREATE TABLE sentinelReversed (g SYMBOL, v INT NOT NULL)");
            execute("INSERT INTO sentinelReversed VALUES ('a', 1), ('a', '-2147483648'::int), ('a', -2)");
            // The stored bits really are 0x8000_0000, not a pattern one off it.
            assertQuery("select v from sentinelMiddle")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            -2
                            -2147483648
                            1
                            """);
            assertBitAndBothPlans("sentinelMiddle", "0");
            assertBitAndBothPlans("sentinelLast", "0");
            assertBitAndBothPlans("sentinelReversed", "0");
        });
    }

    @Test
    public void testBitAndLongNotNullEmptyGroupIsNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE nn (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO nn VALUES ('a', -2), ('a', '-9223372036854775808'::long), ('a', 1)");
            // An empty population never reaches the accumulator, so setEmpty()'s LONG_NULL
            // survives and the group still reads back as NULL on a NOT NULL column.
            assertQuery("select bit_and(v) from nn where false")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            bit_and
                            null
                            """);
        });
    }

    @Test
    public void testBitAndLongNullableNullStaysAbsentInput() throws Exception {
        assertMemoryLeak(() -> {
            // On a nullable column 0x8000_0000_0000_0000 is still an absent input rather than
            // data, so bit_and keeps skipping it and folds -2 & 3:
            //   -2 = 0xFFFF_FFFF_FFFF_FFFE  1111...1110
            //   3  = 0x0000_0000_0000_0003  0000...0011
            //   -2 & 3 = 0x0000_0000_0000_0002 = 2
            // Bit 1 is what tells the two readings apart: folding 0x8000_0000_0000_0000 in as
            // data would clear it and leave 0x0000_0000_0000_0000.
            execute("CREATE TABLE nullable (g SYMBOL, v LONG)");
            execute("INSERT INTO nullable VALUES ('a', -2), ('a', NULL), ('a', 3)");
            assertBitAndBothPlans("nullable", "2");
        });
    }

    @Test
    public void testBitAndLongNotNullSentinelKeyedBatch() throws Exception {
        // Pins the Async Group By plan, whose keyed aggregation runs through
        // computeKeyedBatch instead of computeFirst/computeNext. Group 'a' spans four
        // 2_048-row batches and its LONG_MIN row sits in the second one, so the
        // accumulator holds LONG_MIN on a map entry that is no longer a fresh batch entry.
        //   3_000 x -2     = 0xFFFF_FFFF_FFFF_FFFE folds to 0xFFFF_FFFF_FFFF_FFFE
        //   ... & LONG_MIN = 0x8000_0000_0000_0000, the accumulator IS the bit pattern a nullable
        //                    column would spend on NULL, and on this column it is data, not an
        //                    empty slot
        //   ... & 1        = 0x0000_0000_0000_0000 = 0, held by the 3_000 trailing ones
        // Group 'b' folds 100 sevens to 7 and never reaches that bit pattern.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO t SELECT 'a', -2::long FROM long_sequence(3_000)");
            execute("INSERT INTO t VALUES ('a', '-9223372036854775808'::long)");
            execute("INSERT INTO t SELECT 'a', 1::long FROM long_sequence(3_000)");
            execute("INSERT INTO t SELECT 'b', 7::long FROM long_sequence(100)");
            assertQuery("select g, bit_and(v) from t order by g")
                    .noLeakCheck()
                    .withPlanContaining("Async Group By", "keys: [g]")
                    .expectSize()
                    .returns("""
                            g\tbit_and
                            a\t0
                            b\t7
                            """);
        });
    }

    @Test
    public void testBitAndLongNotNullSentinelParallelMerge() throws Exception {
        // Eight daily partitions become eight page frames, so four workers each fold their own
        // partial and the partials meet in GroupByFunction.merge(). merge()'s javadoc guarantees
        // neither operand is a new map value, so a LONG_MIN accumulator crossing a merge stays
        // data.
        //   -1       = 0xFFFF_FFFF_FFFF_FFFF  1111...1111, the identity of AND
        //   -2       = 0xFFFF_FFFF_FFFF_FFFE  1111...1110
        //   LONG_MIN = 0x8000_0000_0000_0000  1000...0000
        //   LONG_MAX = 0x7FFF_FFFF_FFFF_FFFF  0111...1111
        // Three distinct partials come out of the eight frames:
        //   the day-4 frame holds -2, LONG_MIN, then -1s:
        //     -2 & LONG_MIN = 0x8000_0000_0000_0000, the accumulator IS the bit pattern a nullable
        //                     column would spend on NULL, and on this column it is data, not an
        //                     empty slot; the trailing -1s hold it there, so the partial is
        //                     0x8000_0000_0000_0000
        //   the day-7 frame holds LONG_MAX then -1s, so its partial is 0x7FFF_FFFF_FFFF_FFFF
        //   the other six frames hold -1 only, so their partials are 0xFFFF_FFFF_FFFF_FFFF
        //   -1 & LONG_MIN & LONG_MAX = 0x8000_0000_0000_0000 & 0x7FFF_FFFF_FFFF_FFFF = 0
        // Bit 63 is the only bit the LONG_MAX partial cannot clear on its own, so the answer
        // turns on the day-4 partial surviving the merge: drop it and the fold stops at
        // 0x7FFF_FFFF_FFFF_FFFF. AND is associative and commutative, so 0 is the answer whatever
        // the frame split.
        execute("CREATE TABLE t (g SYMBOL, v LONG NOT NULL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t SELECT 'a', " +
                "CASE WHEN x = 4_001 THEN -2::long " +
                "WHEN x = 4_002 THEN '-9223372036854775808'::long " +
                "WHEN x = 7_001 THEN '9223372036854775807'::long " +
                "ELSE -1::long END, " +
                "((x - 1) * 86_400_000)::timestamp FROM long_sequence(8_000)");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                // Pin the multi-frame shape: without several partitions there is nothing to merge.
                TestUtils.assertSql(compiler, sqlExecutionContext, "select count() from table_partitions('t')", sink, """
                        count
                        8
                        """);
                TestUtils.assertSql(compiler, sqlExecutionContext, "select bit_and(v) from t", sink, """
                        bit_and
                        0
                        """);
                TestUtils.assertSql(compiler, sqlExecutionContext, "select g, bit_and(v) from t order by g", sink, """
                        g\tbit_and
                        a\t0
                        """);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndLongNotNullSentinelOrderIndependent() throws Exception {
        // A NOT NULL LONG column carries the full signed range, 0x8000_0000_0000_0000 included,
        // and bit_and folds that bit pattern like any other, so the answer does not depend on
        // row order:
        //   -2       = 0xFFFF_FFFF_FFFF_FFFE  1111...1110
        //   LONG_MIN = 0x8000_0000_0000_0000  1000...0000
        //   1        = 0x0000_0000_0000_0001  0000...0001
        //
        //   -2 & LONG_MIN = 0x8000_0000_0000_0000, the accumulator IS the bit pattern a nullable
        //                   column would spend on NULL, and on this column it is data, not an
        //                   empty slot
        //   ...      & 1  = 0x0000_0000_0000_0000 = 0
        // Any other order reaches 0 earlier: 1 & LONG_MIN and -2 & 1 are both 0 already.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sentinelMiddle (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO sentinelMiddle VALUES ('a', -2), ('a', '-9223372036854775808'::long), ('a', 1)");
            execute("CREATE TABLE sentinelLast (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO sentinelLast VALUES ('a', -2), ('a', 1), ('a', '-9223372036854775808'::long)");
            execute("CREATE TABLE sentinelReversed (g SYMBOL, v LONG NOT NULL)");
            execute("INSERT INTO sentinelReversed VALUES ('a', 1), ('a', '-9223372036854775808'::long), ('a', -2)");
            // The stored bits really are 0x8000_0000_0000_0000, not a pattern one off it.
            assertQuery("select v from sentinelMiddle")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v
                            -2
                            -9223372036854775808
                            1
                            """);
            assertBitAndBothPlans("sentinelMiddle", "0");
            assertBitAndBothPlans("sentinelLast", "0");
            assertBitAndBothPlans("sentinelReversed", "0");
        });
    }

    /**
     * Asserts {@code bit_and(v)} over {@code table} equals {@code expected} for the unkeyed and
     * the {@code GROUP BY g} form, under both the parallel plan (Async Group By, which aggregates
     * through {@code computeKeyedBatch}) and the serial plan (which aggregates through
     * {@code computeFirst}/{@code computeNext}). Every table used here has a single group 'a'.
     */
    private void assertBitAndBothPlans(String table, String expected) throws Exception {
        for (boolean parallelGroupBy : new boolean[]{true, false}) {
            sqlExecutionContext.setParallelGroupByEnabled(parallelGroupBy);
            try {
                assertQuery("select bit_and(v) from " + table)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("bit_and\n" + expected + "\n");
                assertQuery("select g, bit_and(v) from " + table + " order by g")
                        .noLeakCheck()
                        .expectSize()
                        .returns("g\tbit_and\na\t" + expected + "\n");
            } finally {
                sqlExecutionContext.setParallelGroupByEnabled(true);
            }
        }
    }
}
