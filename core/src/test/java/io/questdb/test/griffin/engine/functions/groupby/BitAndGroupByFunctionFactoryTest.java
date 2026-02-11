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

public class BitAndGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testBitAndLong() throws Exception {
        // 7 & 3 & 5 = 1 (binary: 0111 & 0011 & 0101 = 0001)
        assertQuery(
                """
                        bit_and
                        1
                        """,
                "select bit_and(val) from tab",
                "create table tab as (" +
                        "select 7::long as val from long_sequence(1) " +
                        "union all select 3::long as val from long_sequence(1) " +
                        "union all select 5::long as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndLongAllSame() throws Exception {
        // 7 & 7 & 7 = 7
        assertQuery(
                """
                        bit_and
                        7
                        """,
                "select bit_and(val) from tab",
                "create table tab as (select 7::long as val from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndLongWithGroupBy() throws Exception {
        assertQuery(
                """
                        grp\tbit_and
                        a\t0
                        b\t4
                        """,
                "select grp, bit_and(val) from tab order by grp",
                "create table tab as (" +
                        "select 'a' as grp, 5::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'a' as grp, 2::long as val from long_sequence(1) " +
                        "union all " +
                        "select 'b' as grp, 7::long as val from long_sequence(2) " +
                        "union all " +
                        "select 'b' as grp, 12::long as val from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testBitAndInt() throws Exception {
        // 7 & 3 & 5 = 1
        assertQuery(
                """
                        bit_and
                        1
                        """,
                "select bit_and(val) from tab",
                "create table tab as (" +
                        "select 7::int as val from long_sequence(1) " +
                        "union all select 3::int as val from long_sequence(1) " +
                        "union all select 5::int as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndIntWithNull() throws Exception {
        // 7 & 3 = 3 (nulls are skipped)
        assertQuery(
                """
                        bit_and
                        3
                        """,
                "select bit_and(val) from tab",
                "create table tab as (" +
                        "select 7::int as val from long_sequence(2) " +
                        "union all " +
                        "select null::int as val from long_sequence(2) " +
                        "union all " +
                        "select 3::int as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndShort() throws Exception {
        // 7 & 3 & 5 = 1
        assertQuery(
                """
                        bit_and
                        1
                        """,
                "select bit_and(val) from tab",
                "create table tab as (" +
                        "select 7::short as val from long_sequence(1) " +
                        "union all select 3::short as val from long_sequence(1) " +
                        "union all select 5::short as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndByte() throws Exception {
        // 7 & 3 & 5 = 1
        assertQuery(
                """
                        bit_and
                        1
                        """,
                "select bit_and(val) from tab",
                "create table tab as (" +
                        "select 7::byte as val from long_sequence(1) " +
                        "union all select 3::byte as val from long_sequence(1) " +
                        "union all select 5::byte as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndConstant() throws Exception {
        assertQuery(
                """
                        bit_and
                        42
                        """,
                "select bit_and(42::long) from tab",
                "create table tab as (select x from long_sequence(5))",
                null,
                true,
                true
        );
    }

    @Test
    public void testBitAndLongWithNull() throws Exception {
        // 7 & 3 = 3 (nulls are skipped)
        assertQuery(
                """
                        bit_and
                        3
                        """,
                "select bit_and(val) from tab",
                "create table tab as (" +
                        "select 7::long as val from long_sequence(2) " +
                        "union all " +
                        "select null::long as val from long_sequence(2) " +
                        "union all " +
                        "select 3::long as val from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndLongEmptyTable() throws Exception {
        assertQuery(
                """
                        bit_and
                        null
                        """,
                "select bit_and(val) from tab",
                "create table tab (val long)",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndLongAllNull() throws Exception {
        assertQuery(
                """
                        bit_and
                        null
                        """,
                "select bit_and(val) from tab",
                "create table tab as (select null::long as val from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndIntEmptyTable() throws Exception {
        assertQuery(
                """
                        bit_and
                        null
                        """,
                "select bit_and(val) from tab",
                "create table tab (val int)",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndIntAllNull() throws Exception {
        assertQuery(
                """
                        bit_and
                        null
                        """,
                "select bit_and(val) from tab",
                "create table tab as (select null::int as val from long_sequence(5))",
                null,
                false,
                true
        );
    }

    @Test
    public void testBitAndLongParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_long(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndLongParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_long(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndIntParallel() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, rnd_int(0, 255, 0) val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testBitAndIntParallelWithNulls() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, case when x % 3 = 0 then null else rnd_int(0, 255, 0) end val from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, bit_and(val) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }
}
