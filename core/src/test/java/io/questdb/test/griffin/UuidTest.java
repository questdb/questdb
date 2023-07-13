/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.ImplicitCastException;
import io.questdb.griffin.SqlException;
import io.questdb.std.Uuid;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.fail;

public class UuidTest extends AbstractGriffinTest {

    @Test
    public void testBadConstantUuidWithExplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        try {
            assertCompile("insert into x values (cast ('a0eebc11-110b-11f8-116d' as uuid))");
            fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getMessage(), "invalid UUID constant");
        }
    }

    @Test
    public void testBadUuidWithImplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        try {
            assertCompile("insert into x values ('a0eebc11-110b-11f8-116d')");
            fail();
        } catch (ImplicitCastException e) {
            TestUtils.assertContains(e.getMessage(), "inconvertible value");
        }
    }

    @Test
    public void testCastingConstNullUUIDtoString() throws Exception {
        assertQuery("column\n" +
                        "true\n",
                "select cast (cast (null as uuid) as string) is null from long_sequence(1)", null, null, true, true);
    }

    @Test
    public void testCastingNullUUIDtoString() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values (null)");
        assertCompile("create table y (s string)");
        assertCompile("insert into y select cast (u as string) from x");
        assertQuery("column\n" +
                        "true\n",
                "select s is null from y", null, null, true, true);
    }

    @Test
    public void testCompareConstantNullStringWithUuid() throws Exception {
        assertQuery("column\n" +
                        "true\n",
                "select cast (null as string) = cast (null as uuid) from long_sequence(1)", null, null, true, true);
    }

    @Test
    public void testComparisonWithSymbols() throws Exception {
        // UUID is implicitly cast to String
        // and we can compare strings to symbols
        assertCompile("create table x (u UUID, s SYMBOL)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111', 'whatever')");
        assertCompile("insert into x values (null, null)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111', null)");
        assertCompile("insert into x values (null, 'whatever')");

        assertQuery("column\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n",
                "select u = s from x", null, null, true, true);
    }

    @Test
    public void testConcatFunction() throws Exception {
        assertCompile("create table x (u1 UUID, u2 UUID, u3 UUID)");
        assertCompile("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), null, null)");
        assertCompile("insert into x values (null, null, null)");
        assertCompile("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), null, '22222222-2222-2222-2222-222222222222')");

        assertQuery("concat\n" +
                        "11111111-1111-1111-1111-11111111111122222222-2222-2222-2222-22222222222233333333-3333-3333-3333-333333333333\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "\n" +
                        "11111111-1111-1111-1111-11111111111122222222-2222-2222-2222-222222222222\n",
                "select concat(u1, u2, u3) from x", null, null, true, true);
    }

    @Test
    public void testConstantComparison() throws Exception {
        assertQuery("column\n" +
                        "true\n",
                "select '11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)", null, null, true, true);

        assertQuery("column\n" +
                        "false\n",
                "select '11111111-1111-1111-1111-111111111111' = cast ('22222222-2222-2222-2222-222222222222' as uuid) from long_sequence(1)", null, null, true, true);
    }

    @Test
    public void testCountAggregation() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

        assertQuery("i\tcount\n" +
                        "0\t2\n" +
                        "1\t3\n",
                "select i, count() from x group by i order by i", null, null, true, true);
    }

    @Test
    public void testCountDistinctAggregation_keyed() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

        assertQuery("i\tcount_distinct\n" +
                        "0\t2\n" +
                        "1\t1\n",
                "select i, count_distinct(u) from x group by i order by i", null, null, true, true);
    }

    @Test
    public void testCountDistinctAggregation_nonkeyed() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

        assertQuery("count_distinct\n" +
                        "3\n",
                "select count_distinct(u) from x", null, null, false, true);
    }

    @Test
    public void testEqConstStringToUuid() throws Exception {
        assertQuery("column\tcolumn1\tcolumn2\tcolumn3\tcolumn4\n" +
                        "true\tfalse\ttrue\tfalse\tfalse\n",
                "select " +
                        "cast (null as string) = cast (null as uuid), " +
                        "cast (null as string) = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'not a uuid' = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'11111111-1111-1111-1111-111111111111' = rnd_uuid4()" +
                        "from long_sequence(1)", null, null, true, true);
    }

    @Test
    public void testEqVarBadStringToVarNullUuid() throws Exception {
        assertCompile("create table x (s STRING, u UUID)");
        assertCompile("insert into x values ('not a uuid', null)");
        assertQuery("column\n" +
                        "false\n",
                "select s = u from x", null, null, true, true);
    }

    @Test
    public void testEqVarStringToUuid() throws Exception {
        assertCompile("create table x (s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING)");
        assertCompile("insert into x values (null, null, '11111111-1111-1111-1111-111111111111', 'not a uuid', '11111111-1111-1111-1111-111111111111')");

        assertQuery("column\tcolumn1\tcolumn2\tcolumn3\tcolumn4\n" +
                        "true\tfalse\ttrue\tfalse\tfalse\n",
                "select " +
                        "s1 = cast (null as uuid), " +
                        "s2 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "s3 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "s4 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "s5 = rnd_uuid4() " +
                        "from x", null, null, true, true);
    }

    @Test
    public void testEqualityComparisonConstantOnLeft() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' = u", null, null, true, false);
    }

    @Test
    public void testEqualityComparisonExplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u = cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid)", null, null, true, false);
    }

    @Test
    public void testEqualityComparisonImplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", null, null, true, false);
    }

    @Test
    public void testExplicitCastWithEmptyStringConstant() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('' as uuid))");
        assertQuery("u\n" +
                        "\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testExplicitCastWithNullStringConstant() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast(null as string))");
        assertQuery("u\n" +
                        "\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testGroupByUuid() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values (1, '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values (2, '22222222-2222-2222-2222-222222222222')");
        assertCompile("insert into x values (3, '22222222-2222-2222-2222-222222222222')");

        assertQuery("u\tsum\n" +
                        "11111111-1111-1111-1111-111111111111\t1\n" +
                        "22222222-2222-2222-2222-222222222222\t5\n",
                "select u, sum(i) from x group by u", null, null, true, true);
    }

    @Test
    public void testIn() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values ('22222222-2222-2222-2222-222222222222')");
        assertCompile("insert into x values ('33333333-3333-3333-3333-333333333333')");
        assertCompile("insert into x values ('44444444-4444-4444-4444-444444444444')");
        assertCompile("insert into x values (null)");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n" +
                        "33333333-3333-3333-3333-333333333333\n",
                "select * from x where u in ('11111111-1111-1111-1111-111111111111', 'not uuid', '55555555-5555-5555-5555-555555555555', cast ('22222222-2222-2222-2222-222222222222' as UUID), cast ('33333333-3333-3333-3333-333333333333' as symbol))", null, true, false, true);
    }

    @Test
    public void testIn_constant() throws Exception {
        assertQuery("x\n" +
                        "1\n",
                "select * from long_sequence(1) where cast ('11111111-1111-1111-1111-111111111111' as uuid) in ('11111111-1111-1111-1111-111111111111')", null, true, false, true);

        assertQuery("x\n",
                "select * from long_sequence(1) where cast (null as uuid) in ('11111111-1111-1111-1111-111111111111')", null, false, false, true);
    }

    @Test
    public void testIn_null() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");

        assertFailure("select * from x where u in (cast (null as UUID))", null, 28, "NULL is not allowed in IN list");
        assertFailure("select * from x where u in (cast (null as String))", null, 28, "NULL is not allowed in IN list");
        assertFailure("select * from x where u in (null)", null, 28, "NULL is not allowed in IN list");
    }

    @Test
    public void testIn_unexpectedType() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");

        assertFailure("select * from x where u in (42)", null, 28, "STRING or UUID constant expected in IN list");
    }

    @Test
    public void testInsertAddUuidColumnAndThenO3Insert() throws Exception {
        // testing O3 insert when uuid columnTop > 0
        assertCompile("create table x (ts timestamp, i int) timestamp(ts) partition by MONTH");
        assertCompile("insert into x values ('2018-01-01', 1)");
        assertCompile("insert into x values ('2018-01-03', 1)");
        assertCompile("alter table x add column u uuid");
        assertCompile("insert into x values ('2018-01-02', 1, '00000000-0000-0000-0000-000000000000')");
        assertQuery("ts\ti\tu\n" +
                "2018-01-01T00:00:00.000000Z\t1\t\n" +
                "2018-01-02T00:00:00.000000Z\t1\t00000000-0000-0000-0000-000000000000\n" +
                "2018-01-03T00:00:00.000000Z\t1\t\n", "select * from x", "ts", true, true, true);
    }

    @Test
    public void testInsertExplicitNull() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (null)");
        assertQuery("u\n" +
                        "\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testInsertFromFunctionReturningLong() throws Exception {
        assertCompile("create table x (u UUID)");
        assertFailure("insert into x values (rnd_long())", null, 22, "inconvertible types");
    }

    @Test
    public void testInsertFromFunctionReturningString() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (rnd_str('11111111-1111-1111-1111-111111111111'))");
    }

    @Test
    public void testInsertNullByOmitting() throws Exception {
        assertCompile("create table x (i INT, u UUID, i2 INT)");
        assertCompile("insert into x (i, i2) values (42, 0)");
        assertQuery("i\tu\ti2\n" +
                        "42\t\t0\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testInsertNullUuidColumnIntoStringColumnImplicitCast() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values (null)");
        assertCompile("create table y (s string)");
        assertCompile("insert into y select u from x");
        assertQuery("column\n" +
                        "true\n",
                "select s is null from y", null, null, true, true);
    }

    @Test
    public void testInsertUuidColumnIntoIntColumnImplicitCast() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("create table y (i int)");
        assertFailure("insert into y select u from x", null, 21, "inconvertible types");
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnExplicitCast() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("create table y (s string)");
        assertCompile("insert into y select cast (u as string) from x");
        assertQuery("s\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from y", null, null, true, true);
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnImplicitCast() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("create table y (s string)");
        assertCompile("insert into y select u from x");
        assertQuery("s\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from y", null, null, true, true);
    }

    @Test
    public void testInsertWithExplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testInsertWithImplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertQuery("u\n" +
                        "a0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testLatestOn() throws Exception {
        assertCompile("create table x (ts timestamp, u uuid, i int) timestamp(ts) partition by DAY");
        assertCompile("insert into x values ('2020-01-01T00:00:00.000000Z', '00000000-0000-0000-0000-000000000001', 0)");
        assertCompile("insert into x values ('2020-01-02T00:01:00.000000Z', '00000000-0000-0000-0000-000000000001', 2)");
        assertCompile("insert into x values ('2020-01-02T00:01:00.000000Z', '00000000-0000-0000-0000-000000000002', 0)");

        assertQuery("ts\tu\ti\n" +
                        "2020-01-02T00:01:00.000000Z\t00000000-0000-0000-0000-000000000001\t2\n" +
                        "2020-01-02T00:01:00.000000Z\t00000000-0000-0000-0000-000000000002\t0\n",
                "select ts, u, i from x latest on ts partition by u", null, "ts", true, true);
    }

    @Test
    public void testLongExplicitCastAsUuid() throws Exception {
        assertCompile("create table x (l long)");
        assertCompile("insert into x values (42)");
        assertFailure("select cast(l as uuid) from x", null, 7, "unexpected argument for function");
    }

    @Test
    public void testLongsToUuid_constant() throws Exception {
        assertQuery(
                "uuid\n" +
                        "00000000-0000-0001-0000-000000000002\n",
                "select to_uuid(2, 1) as uuid from long_sequence(1)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testLongsToUuid_fromTable() throws Exception {
        assertCompile("create table x (lo long, hi long)");
        assertCompile("insert into x values (2, 1)");
        assertQuery(
                "uuid\n" +
                        "00000000-0000-0001-0000-000000000002\n",
                "select to_uuid(lo, hi) as uuid from x",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testLongsToUuid_nullConstant() throws Exception {
        assertQuery(
                "uuid\n" +
                        "\n",
                "select to_uuid(null, null) as uuid from long_sequence(1)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testLongsToUuid_nullFromTable() throws Exception {
        assertCompile("create table x (lo long, hi long)");
        assertCompile("insert into x values (null, null)");
        assertQuery(
                "uuid\n" +
                        "\n",
                "select to_uuid(lo, hi) as uuid from x",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testNegatedEqualityComparisonExplicitCast() throws Exception {
        Uuid uuid = new Uuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u != cast('11111111-1111-1111-1111-111111111111' as uuid)", null, null, true, false);
    }

    @Test
    public void testNegatedEqualityComparisonImplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u != '11111111-1111-1111-1111-111111111111'", null, null, true, false);
    }

    @Test
    public void testNonKeyedFirstAggregation() throws Exception {
        assertCompile("create table xxx(u uuid)");
        assertCompile("insert into xxx values ('54710940-38c0-4d93-92db-43b7cad84228')");
        assertCompile("insert into xxx values ('')");

        assertQuery("first\n" +
                        "54710940-38c0-4d93-92db-43b7cad84228\n",
                "select first(u) from xxx", null, null, false, true);
    }

    @Test
    public void testNonKeyedLastAggregation() throws Exception {
        assertCompile("create table xxx(u uuid)");
        assertCompile("insert into xxx values ('54710940-38c0-4d93-92db-43b7cad84228')");
        assertCompile("insert into xxx values ('')"); // empty string is implicitly cast to null

        assertQuery("last\n" +
                        "\n",
                "select last(u) from xxx", null, null, false, true);
    }

    @Test
    public void testO3_differentPartition() throws Exception {
        assertCompile("create table x (ts timestamp, u UUID) timestamp(ts) partition by DAY");
        assertCompile("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("insert into x values (to_timestamp('2010-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

        assertQuery("ts\tu\n" +
                        "2010-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11\n" +
                        "2018-01-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, "ts", true, true);
    }

    @Test
    public void testO3_samePartition() throws Exception {
        assertCompile("create table x (ts timestamp, u UUID) timestamp(ts) partition by YEAR");
        assertCompile("insert into x values (to_timestamp('2018-06', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

        assertQuery("ts\tu\n" +
                        "2018-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11\n" +
                        "2018-06-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, "ts", true, true);
    }

    @Test
    public void testOrderByUuid() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (2, '00000000-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000000000001')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000000000010')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000000000100')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000000001000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000000010000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000000100000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000001000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000010000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-000100000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-001000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-010000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0000-100000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0001-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0010-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-0100-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0000-1000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0001-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0010-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-0100-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0000-1000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0001-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0010-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-0100-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000000-1000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000001-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000010-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00000100-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00001000-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00010000-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '00100000-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '01000000-0000-0000-0000-000000000000')");
        assertCompile("insert into x values (1, '10000000-0000-0000-0000-000000000000')");

        assertQuery("i\tu\n" +
                        "2\t00000000-0000-0000-0000-000000000000\n" +
                        "1\t00000000-0000-0000-0000-000000000001\n" +
                        "1\t00000000-0000-0000-0000-000000000010\n" +
                        "1\t00000000-0000-0000-0000-000000000100\n" +
                        "1\t00000000-0000-0000-0000-000000001000\n" +
                        "1\t00000000-0000-0000-0000-000000010000\n" +
                        "1\t00000000-0000-0000-0000-000000100000\n" +
                        "1\t00000000-0000-0000-0000-000001000000\n" +
                        "1\t00000000-0000-0000-0000-000010000000\n" +
                        "1\t00000000-0000-0000-0000-000100000000\n" +
                        "1\t00000000-0000-0000-0000-001000000000\n" +
                        "1\t00000000-0000-0000-0000-010000000000\n" +
                        "1\t00000000-0000-0000-0000-100000000000\n" +
                        "1\t00000000-0000-0000-0001-000000000000\n" +
                        "1\t00000000-0000-0000-0010-000000000000\n" +
                        "1\t00000000-0000-0000-0100-000000000000\n" +
                        "1\t00000000-0000-0000-1000-000000000000\n" +
                        "1\t00000000-0000-0001-0000-000000000000\n" +
                        "1\t00000000-0000-0010-0000-000000000000\n" +
                        "1\t00000000-0000-0100-0000-000000000000\n" +
                        "1\t00000000-0000-1000-0000-000000000000\n" +
                        "1\t00000000-0001-0000-0000-000000000000\n" +
                        "1\t00000000-0010-0000-0000-000000000000\n" +
                        "1\t00000000-0100-0000-0000-000000000000\n" +
                        "1\t00000000-1000-0000-0000-000000000000\n" +
                        "1\t00000001-0000-0000-0000-000000000000\n" +
                        "1\t00000010-0000-0000-0000-000000000000\n" +
                        "1\t00000100-0000-0000-0000-000000000000\n" +
                        "1\t00001000-0000-0000-0000-000000000000\n" +
                        "1\t00010000-0000-0000-0000-000000000000\n" +
                        "1\t00100000-0000-0000-0000-000000000000\n" +
                        "1\t01000000-0000-0000-0000-000000000000\n" +
                        "1\t10000000-0000-0000-0000-000000000000\n",
                "select * from x order by u", null, null, true, true);

        assertQuery("i\tu\n" +
                        "1\t10000000-0000-0000-0000-000000000000\n" +
                        "1\t01000000-0000-0000-0000-000000000000\n" +
                        "1\t00100000-0000-0000-0000-000000000000\n" +
                        "1\t00010000-0000-0000-0000-000000000000\n" +
                        "1\t00001000-0000-0000-0000-000000000000\n" +
                        "1\t00000100-0000-0000-0000-000000000000\n" +
                        "1\t00000010-0000-0000-0000-000000000000\n" +
                        "1\t00000001-0000-0000-0000-000000000000\n" +
                        "1\t00000000-1000-0000-0000-000000000000\n" +
                        "1\t00000000-0100-0000-0000-000000000000\n" +
                        "1\t00000000-0010-0000-0000-000000000000\n" +
                        "1\t00000000-0001-0000-0000-000000000000\n" +
                        "1\t00000000-0000-1000-0000-000000000000\n" +
                        "1\t00000000-0000-0100-0000-000000000000\n" +
                        "1\t00000000-0000-0010-0000-000000000000\n" +
                        "1\t00000000-0000-0001-0000-000000000000\n" +
                        "1\t00000000-0000-0000-1000-000000000000\n" +
                        "1\t00000000-0000-0000-0100-000000000000\n" +
                        "1\t00000000-0000-0000-0010-000000000000\n" +
                        "1\t00000000-0000-0000-0001-000000000000\n" +
                        "1\t00000000-0000-0000-0000-100000000000\n" +
                        "1\t00000000-0000-0000-0000-010000000000\n" +
                        "1\t00000000-0000-0000-0000-001000000000\n" +
                        "1\t00000000-0000-0000-0000-000100000000\n" +
                        "1\t00000000-0000-0000-0000-000010000000\n" +
                        "1\t00000000-0000-0000-0000-000001000000\n" +
                        "1\t00000000-0000-0000-0000-000000100000\n" +
                        "1\t00000000-0000-0000-0000-000000010000\n" +
                        "1\t00000000-0000-0000-0000-000000001000\n" +
                        "1\t00000000-0000-0000-0000-000000000100\n" +
                        "1\t00000000-0000-0000-0000-000000000010\n" +
                        "1\t00000000-0000-0000-0000-000000000001\n" +
                        "2\t00000000-0000-0000-0000-000000000000\n",
                "select * from x order by u desc", null, null, true, true);
    }

    @Test
    public void testPostgresStyleLiteralCasting() throws Exception {
        assertQuery("column\n" +
                        "true\n",
                "select (uuid '11111111-1111-1111-1111-111111111111') = cast('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)", null, null, true, true);

    }

    @Test
    public void testRandomizedOrderBy() throws Exception {
        int count = 1000;

        assertCompile("create table x (u UUID)");
        UUID[] uuids = new UUID[count];
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.randomUUID();
            assertCompile("insert into x values ('" + uuid + "')");
            uuids[i] = uuid;
        }
        Arrays.sort(uuids);

        // test ascending
        StringBuilder expected = new StringBuilder("u\n");
        for (int i = 0; i < count; i++) {
            expected.append(uuids[i]).append("\n");
        }
        assertQuery(expected.toString(), "select * from x order by u", null, null, true, true);

        // test descending
        expected = new StringBuilder("u\n");
        for (int i = count - 1; i >= 0; i--) {
            expected.append(uuids[i]).append("\n");
        }
        assertQuery(expected.toString(), "select * from x order by u desc", null, null, true, true);
    }

    @Test
    public void testRndUuid() throws Exception {
        assertCompile("create table x as (select rnd_uuid4() from long_sequence(10))");
        assertQuery("rnd_uuid4\n" +
                        "0010cde8-12ce-40ee-8010-a928bb8b9650\n" +
                        "9f9b2131-d49f-4d1d-ab81-39815c50d341\n" +
                        "7bcd48d8-c77a-4655-b2a2-15ba0462ad15\n" +
                        "b5b2159a-2356-4217-965d-4c984f0ffa8a\n" +
                        "e8beef38-cd7b-43d8-9b2d-34586f6275fa\n" +
                        "322a2198-864b-4b14-b97f-a69eb8fec6cc\n" +
                        "980eca62-a219-40f1-a846-d7a3aa5aecce\n" +
                        "c1e63128-5c1a-4288-872b-fc5230158059\n" +
                        "716de3d2-5dcc-4d91-9fa2-397a5d8c84c4\n" +
                        "4b0f595f-143e-4d72-af1a-8266e7921e3b\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testStringOverloadFunction() throws Exception {
        assertCompile("create table x (u uuid)");

        assertCompile("insert into x values ('22222222-2222-2222-2222-222222222222')");

        assertQuery("length\n" +
                        "36\n",
                "select length(u) from x", null, null, true, true);
    }

    @Test
    public void testStringOverloadFunctionWithNull() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values (null)");
        assertQuery("length\n" +
                        "-1\n",
                "select length(u) from x", null, null, true, true);
    }

    @Test
    public void testTwoVarComparison() throws Exception {
        assertCompile("create table x (u1 UUID, u2 UUID)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111')");
        assertQuery("u1\tu2\n" +
                        "11111111-1111-1111-1111-111111111111\t11111111-1111-1111-1111-111111111111\n",
                "select * from x where u1 = u2", null, null, true, false);
    }

    @Test
    public void testTypeOf() throws Exception {
        assertQuery("typeOf\n" +
                        "UUID\n",
                "select typeOf(uuid '11111111-1111-1111-1111-111111111111') from long_sequence(1)", null, null, true, true);
    }

    @Test
    public void testUnionAllArbitraryStringWithUuid() throws Exception {
        assertCompile("create table x (u string)");
        assertCompile("create table y (u uuid)");

        assertCompile("insert into x values ('totally not a uuid')");
        assertCompile("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertQuery("u\n" +
                        "totally not a uuid\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUnionAllDups() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u UUID)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values ('11111111-1111-1111-1111-111111111111')");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from x union all select * from y", null, null, false, true);
    }

    @Test
    public void testUnionAllNull() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u UUID)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values (null)");
        assertCompile("insert into y values (null)");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "\n" +
                        "\n",
                "select * from x union all select * from y", null, null, false, true);
    }

    @Test
    public void testUnionAllSimple() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u UUID)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union all select * from y", null, null, false, true);
    }

    @Test
    public void testUnionAllStringWithUuid() throws Exception {
        assertCompile("create table x (u string)");
        assertCompile("create table y (u uuid)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUnionAllUuidWithArbitraryString() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("create table y (u string)");

        assertCompile("insert into x values ('22222222-2222-2222-2222-222222222222')");
        assertCompile("insert into y values ('totally not a uuid')");

        assertQuery("u\n" +
                        "22222222-2222-2222-2222-222222222222\n" +
                        "totally not a uuid\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUnionAllUuidWithString() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u string)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUnionDups() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u UUID)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values ('11111111-1111-1111-1111-111111111111')");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUnionNull() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u UUID)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values (null)");
        assertCompile("insert into y values (null)");

        // only one null is returned - dups null are eliminated
        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUnionSimple() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("create table y (u UUID)");

        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertQuery("u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y", null, null, false, false);
    }

    @Test
    public void testUpdateByUuid() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("update x set i = 42 where u = 'a0eebc11-110b-11f8-116d-11b9bd380a11'");
        assertQuery("i\tu\n" +
                        "42\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testUpdateUuid() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("update x set u = 'a0eebc11-4242-11f8-116d-11b9bd380a11' where i = 0");
        assertQuery("i\tu\n" +
                        "0\ta0eebc11-4242-11f8-116d-11b9bd380a11\n",
                "select * from x", null, null, true, true);
    }

    @Test
    public void testUuidExplicitCastAsLong() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertQuery("cast\n" +
                        "NaN\n",
                "select cast(u as long) from x", null, null, true, true);
    }
}
