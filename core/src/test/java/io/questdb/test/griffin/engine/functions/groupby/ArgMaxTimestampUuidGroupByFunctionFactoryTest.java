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

import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Tests for arg_max(timestamp, uuid) - returns timestamp value at max uuid key.
 */
public class ArgMaxTimestampUuidGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testArgMaxAllNull() throws SqlException {
        execute("create table tab (value timestamp, key uuid)");
        execute("insert into tab values (null, null)");
        execute("insert into tab values (null, null)");
        assertSql("arg_max\n\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxParallelAllNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, cast(null as uuid) key from long_sequence(100000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSql(engine, sqlExecutionContext, sql, sink, "sym\targ_max\nA\t\nB\t\nC\t\nD\t\nE\t\n");
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelChunky() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, rnd_uuid4() key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelMergeNullDestValidSrc() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, case when x <= 1000000 then cast(null as uuid) else rnd_uuid4() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxParallelWithNullKeys() throws Exception {
        execute("create table tab as (select rnd_symbol('A','B','C','D','E') sym, timestamp_sequence(0, 1000) value, case when x % 2 = 0 then cast(null as uuid) else rnd_uuid4() end key from long_sequence(2000000))");
        try (WorkerPool pool = new WorkerPool(() -> 4)) {
            TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                String sql = "select sym, arg_max(value, key) from tab group by sym order by sym";
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
            }, configuration, LOG);
        }
    }

    @Test
    public void testArgMaxSimple() throws SqlException {
        execute("create table tab (value timestamp, key uuid)");
        execute("insert into tab values ('2023-01-01T00:00:00.000000Z', '11111111-1111-1111-1111-111111111111')");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 'ffffffff-ffff-ffff-ffff-ffffffffffff')");
        execute("insert into tab values ('2023-01-02T00:00:00.000000Z', '22222222-2222-2222-2222-222222222222')");
        // ffffffff-ffff-ffff-ffff-ffffffffffff is max uuid
        assertSql("arg_max\n2023-01-03T00:00:00.000000Z\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxWithGroupBy() throws SqlException {
        execute("create table tab (sym symbol, value timestamp, key uuid)");
        execute("insert into tab values ('A', '2023-01-01T00:00:00.000000Z', '11111111-1111-1111-1111-111111111111')");
        execute("insert into tab values ('A', '2023-01-03T00:00:00.000000Z', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')");
        execute("insert into tab values ('B', '2023-01-05T00:00:00.000000Z', 'ffffffff-ffff-ffff-ffff-ffffffffffff')");
        execute("insert into tab values ('B', '2023-01-04T00:00:00.000000Z', '22222222-2222-2222-2222-222222222222')");
        assertSql("sym\targ_max\nA\t2023-01-03T00:00:00.000000Z\nB\t2023-01-05T00:00:00.000000Z\n", "select sym, arg_max(value, key) from tab order by sym");
    }

    @Test
    public void testArgMaxWithNullKey() throws SqlException {
        execute("create table tab (value timestamp, key uuid)");
        execute("insert into tab values ('2023-01-01T00:00:00.000000Z', null)");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')");
        execute("insert into tab values ('2023-01-02T00:00:00.000000Z', '22222222-2222-2222-2222-222222222222')");
        assertSql("arg_max\n2023-01-03T00:00:00.000000Z\n", "select arg_max(value, key) from tab");
    }

    @Test
    public void testArgMaxWithNullValue() throws SqlException {
        execute("create table tab (value timestamp, key uuid)");
        execute("insert into tab values (null, 'ffffffff-ffff-ffff-ffff-ffffffffffff')");
        execute("insert into tab values ('2023-01-03T00:00:00.000000Z', '22222222-2222-2222-2222-222222222222')");
        assertSql("arg_max\n\n", "select arg_max(value, key) from tab");
    }
}
