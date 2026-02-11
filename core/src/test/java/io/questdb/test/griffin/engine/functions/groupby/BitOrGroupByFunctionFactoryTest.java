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

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class BitOrGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBitOrLong() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery(
                """
                        bit_or
                        7
                        """,
                "select bit_or(val) from tab",
                "create table tab as (" +
                        "select 1::long as val from long_sequence(1) " +
                        "union all select 2::long as val from long_sequence(1) " +
                        "union all select 4::long as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrLongAllSame() throws Exception {
        // 7 | 7 | 7 = 7
        assertQuery(
                """
                        bit_or
                        7
                        """,
                "select bit_or(val) from tab",
                "create table tab as (select 7::long as val from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrLongWithGroupBy() throws Exception {
        assertQuery(
                """
                        grp\tbit_or
                        a\t7
                        b\t15
                        """,
                "select grp, bit_or(val) from tab order by grp",
                "create table tab as (" +
                        "select 'a' as grp, 5::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'a' as grp, 2::long as val from long_sequence(1) " +
                        "union all " +
                        "select 'b' as grp, 7::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'b' as grp, 8::long as val from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBitOrInt() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery(
                """
                        bit_or
                        7
                        """,
                "select bit_or(val) from tab",
                "create table tab as (" +
                        "select 1::int as val from long_sequence(1) " +
                        "union all select 2::int as val from long_sequence(1) " +
                        "union all select 4::int as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrIntWithNull() throws Exception {
        // 1 | 2 = 3 (nulls are skipped)
        assertQuery(
                """
                        bit_or
                        3
                        """,
                "select bit_or(val) from tab",
                "create table tab as (" +
                        "select 1::int as val from long_sequence(1) " +
                        "union all " +
                        "select null::int as val from long_sequence(2) " +
                        "union all " +
                        "select 2::int as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrShort() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery(
                """
                        bit_or
                        7
                        """,
                "select bit_or(val) from tab",
                "create table tab as (" +
                        "select 1::short as val from long_sequence(1) " +
                        "union all select 2::short as val from long_sequence(1) " +
                        "union all select 4::short as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrByte() throws Exception {
        // 1 | 2 | 4 = 7
        assertQuery(
                """
                        bit_or
                        7
                        """,
                "select bit_or(val) from tab",
                "create table tab as (" +
                        "select 1::byte as val from long_sequence(1) " +
                        "union all select 2::byte as val from long_sequence(1) " +
                        "union all select 4::byte as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrConstant() throws Exception {
        assertQuery(
                """
                        bit_or
                        42
                        """,
                "select bit_or(42::long) from tab",
                "create table tab as (select x from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBitOrLongWithNull() throws Exception {
        // 1 | 2 = 3 (nulls are skipped)
        assertQuery(
                """
                        bit_or
                        3
                        """,
                "select bit_or(val) from tab",
                "create table tab as (" +
                        "select 1::long as val from long_sequence(1) " +
                        "union all " +
                        "select null::long as val from long_sequence(2) " +
                        "union all " +
                        "select 2::long as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrLongEmptyTable() throws Exception {
        assertQuery(
                """
                        bit_or
                        null
                        """,
                "select bit_or(val) from tab",
                "create table tab (val long)",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrLongAllNull() throws Exception {
        assertQuery(
                """
                        bit_or
                        null
                        """,
                "select bit_or(val) from tab",
                "create table tab as (select null::long as val from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrIntEmptyTable() throws Exception {
        assertQuery(
                """
                        bit_or
                        null
                        """,
                "select bit_or(val) from tab",
                "create table tab (val int)",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrIntAllNull() throws Exception {
        assertQuery(
                """
                        bit_or
                        null
                        """,
                "select bit_or(val) from tab",
                "create table tab as (select null::int as val from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitOrLongParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrLongParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_long(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrIntParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_int(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitOrIntParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_int(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_or(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }
}
