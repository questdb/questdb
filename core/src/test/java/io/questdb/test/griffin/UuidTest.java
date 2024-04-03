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

import io.questdb.std.Uuid;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

public class UuidTest extends AbstractCairoTest {

    @Test
    public void testBadConstantUuidWithExplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        assertException(
                "insert into x values (cast ('a0eebc11-110b-11f8-116d' as uuid))",
                28,
                "invalid UUID constant"
        );
    }

    @Test
    public void testBadUuidWithImplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        assertException(
                "insert into x values ('a0eebc11-110b-11f8-116d')",
                0,
                "inconvertible value: `a0eebc11-110b-11f8-116d` [STRING -> UUID]"
        );
    }

    @Test
    public void testBindVariableInFilterInvalid() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (b uuid)");
            insert("insert into x values('11111111-1111-1111-1111-111111111111')");
            insert("insert into x values('22222222-2222-2222-2222-222222222222')");
            insert("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertSql(
                    "b\n",
                    "x where b = $1"
            );
        });
    }

    @Test
    public void testBindVariableInFilterInvalidNegated() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (b uuid)");
            insert("insert into x values('11111111-1111-1111-1111-111111111111')");
            insert("insert into x values('22222222-2222-2222-2222-222222222222')");
            insert("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertSql(
                    "b\n" +
                            "11111111-1111-1111-1111-111111111111\n" +
                            "22222222-2222-2222-2222-222222222222\n" +
                            "33333333-3333-3333-3333-333333333333\n",
                    "x where b != $1"
            );
        });
    }

    @Test
    public void testCastingConstNullUUIDtoString() throws Exception {
        assertSql(
                "column\n" +
                        "true\n",
                "select cast (cast (null as uuid) as string) is null from long_sequence(1)"
        );
    }

    @Test
    public void testCastingNullUUIDtoString() throws Exception {
        ddl("create table x (u uuid)");
        insert("insert into x values (null)");
        ddl("create table y (s string)");
        ddl("insert into y select cast (u as string) from x");
        assertSql(
                "column\n" +
                        "true\n", "select s is null from y"
        );
    }

    @Test
    public void testCompareConstantNullStringWithUuid() throws Exception {
        assertSql(
                "column\n" +
                        "true\n",
                "select cast (null as string) = cast (null as uuid) from long_sequence(1)"
        );
    }

    @Test
    public void testComparisonWithSymbols() throws Exception {
        // UUID is implicitly cast to String
        // and we can compare strings to symbols
        ddl("create table x (u UUID, s SYMBOL)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111', 'whatever')");
        insert("insert into x values (null, null)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111', null)");
        insert("insert into x values (null, 'whatever')");

        assertSql(
                "column\n" +
                        "true\n" +
                        "false\n" +
                        "true\n" +
                        "false\n" +
                        "false\n",
                "select u = s from x"
        );
    }

    @Test
    public void testConcatFunction() throws Exception {
        ddl("create table x (u1 UUID, u2 UUID, u3 UUID)");
        insert("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), null, null)");
        insert("insert into x values (null, null, null)");
        insert("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), null, '22222222-2222-2222-2222-222222222222')");

        assertSql(
                "concat\n" +
                        "11111111-1111-1111-1111-11111111111122222222-2222-2222-2222-22222222222233333333-3333-3333-3333-333333333333\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "\n" +
                        "11111111-1111-1111-1111-11111111111122222222-2222-2222-2222-222222222222\n",
                "select concat(u1, u2, u3) from x"
        );
    }

    @Test
    public void testConstantComparison() throws Exception {
        assertSql(
                "column\n" +
                        "true\n",
                "select '11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)"
        );

        assertSql(
                "column\n" +
                        "false\n",
                "select '11111111-1111-1111-1111-111111111111' = cast ('22222222-2222-2222-2222-222222222222' as uuid) from long_sequence(1)"
        );
    }

    @Test
    public void testConstantInFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (b uuid)");
            insert("insert into x values('11111111-1111-1111-1111-111111111111')");
            insert("insert into x values('22222222-2222-2222-2222-222222222222')");
            insert("insert into x values('33333333-3333-3333-3333-333333333333')");
            assertSql(
                    "b\n" +
                            "22222222-2222-2222-2222-222222222222\n",
                    "x where b = '22222222-2222-2222-2222-222222222222'"
            );
        });
    }

    @Test
    public void testCountAggregation() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

        assertSql(
                "i\tcount\n" +
                        "0\t2\n" +
                        "1\t3\n",
                "select i, count() from x group by i order by i"
        );
    }

    @Test
    public void testCountDistinctAggregation_keyed() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

        assertSql(
                "i\tcount_distinct\n" +
                        "0\t2\n" +
                        "1\t1\n",
                "select i, count_distinct(u) from x group by i order by i"
        );
    }

    @Test
    public void testCountDistinctAggregation_nonkeyed() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
        insert("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

        assertSql(
                "count_distinct\n" +
                        "3\n",
                "select count_distinct(u) from x"
        );
    }

    @Test
    public void testEqConstStringToUuid() throws Exception {
        assertSql(
                "column\tcolumn1\tcolumn2\tcolumn3\tcolumn4\n" +
                        "true\tfalse\ttrue\tfalse\tfalse\n",
                "select " +
                        "cast (null as string) = cast (null as uuid), " +
                        "cast (null as string) = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'not a uuid' = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'11111111-1111-1111-1111-111111111111' = rnd_uuid4()" +
                        "from long_sequence(1)"
        );
    }

    @Test
    public void testEqVarBadStringToVarNullUuid() throws Exception {
        ddl("create table x (s STRING, u UUID)");
        insert("insert into x values ('not a uuid', null)");
        assertSql(
                "column\n" +
                        "false\n",
                "select s = u from x"
        );
    }

    @Test
    public void testEqVarStringToUuid() throws Exception {
        ddl("create table x (s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING)");
        insert("insert into x values (null, null, '11111111-1111-1111-1111-111111111111', 'not a uuid', '11111111-1111-1111-1111-111111111111')");

        assertSql(
                "column\tcolumn1\tcolumn2\tcolumn3\tcolumn4\n" +
                        "true\tfalse\ttrue\tfalse\tfalse\n",
                "select " +
                        "s1 = cast (null as uuid), " +
                        "s2 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "s3 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "s4 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "s5 = rnd_uuid4() " +
                        "from x"
        );
    }

    @Test
    public void testEqualityComparisonConstantOnLeft() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertSql(
                "u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' = u"
        );
    }

    @Test
    public void testEqualityComparisonExplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertSql(
                "u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u = cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid)"
        );
    }

    @Test
    public void testEqualityComparisonImplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertSql(
                "u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"
        );
    }

    @Test
    public void testExplicitCastWithEmptyStringConstant() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (cast('' as uuid))");
        assertSql(
                "u\n" +
                        "\n",
                "select * from x"
        );
    }

    @Test
    public void testExplicitCastWithNullStringConstant() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (cast(null as string))");
        assertSql(
                "u\n" +
                        "\n",
                "x"
        );
    }

    @Test
    public void testGroupByUuid() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values (1, '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values (2, '22222222-2222-2222-2222-222222222222')");
        insert("insert into x values (3, '22222222-2222-2222-2222-222222222222')");

        assertSql(
                "u\tsum\n" +
                        "11111111-1111-1111-1111-111111111111\t1\n" +
                        "22222222-2222-2222-2222-222222222222\t5\n",
                "select u, sum(i) from x group by u order by u"
        );
    }

    @Test
    public void testIn() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into x values ('22222222-2222-2222-2222-222222222222')");
        insert("insert into x values ('33333333-3333-3333-3333-333333333333')");
        insert("insert into x values ('44444444-4444-4444-4444-444444444444')");
        insert("insert into x values (null)");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n" +
                        "33333333-3333-3333-3333-333333333333\n",
                "select * from x where u in ('11111111-1111-1111-1111-111111111111', 'not uuid', '55555555-5555-5555-5555-555555555555', cast ('22222222-2222-2222-2222-222222222222' as UUID), cast ('33333333-3333-3333-3333-333333333333' as symbol))"
        );
    }

    @Test
    public void testIn_constant() throws Exception {
        assertSql(
                "x\n" +
                        "1\n",
                "select * from long_sequence(1) where cast ('11111111-1111-1111-1111-111111111111' as uuid) in ('11111111-1111-1111-1111-111111111111')"
        );

        assertSql(
                "x\n",
                "select * from long_sequence(1) where cast (null as uuid) in ('11111111-1111-1111-1111-111111111111')"
        );
    }

    @Test
    public void testIn_null() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");

        assertException("select * from x where u in (cast (null as UUID))", 28, "NULL is not allowed in IN list");
        assertException("select * from x where u in (cast (null as String))", 28, "NULL is not allowed in IN list");
        assertException("select * from x where u in (null)", 28, "NULL is not allowed in IN list");
    }

    @Test
    public void testIn_unexpectedType() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");

        assertException("select * from x where u in (42)", 28, "STRING or UUID constant expected in IN list");
    }

    @Test
    public void testIndexedBindVariableInFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (b uuid)");
            insert("insert into x values('11111111-1111-1111-1111-111111111111')");
            insert("insert into x values('22222222-2222-2222-2222-222222222222')");
            insert("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "22222222-2222-2222-2222-222222222222");
            assertSql(
                    "b\n" +
                            "22222222-2222-2222-2222-222222222222\n",
                    "x where b = $1"
            );
        });
    }

    @Test
    public void testInsertAddUuidColumnAndThenO3Insert() throws Exception {
        // testing O3 insert when uuid columnTop > 0
        ddl("create table x (ts timestamp, i int) timestamp(ts) partition by MONTH");
        insert("insert into x values ('2018-01-01', 1)");
        insert("insert into x values ('2018-01-03', 1)");
        ddl("alter table x add column u uuid");
        insert("insert into x values ('2018-01-02', 1, '00000000-0000-0000-0000-000000000000')");
        assertSql(
                "ts\ti\tu\n" +
                        "2018-01-01T00:00:00.000000Z\t1\t\n" +
                        "2018-01-02T00:00:00.000000Z\t1\t00000000-0000-0000-0000-000000000000\n" +
                        "2018-01-03T00:00:00.000000Z\t1\t\n",
                "select * from x"
        );
    }

    @Test
    public void testInsertExplicitNull() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (null)");
        assertSql(
                "u\n" +
                        "\n",
                "select * from x"
        );
    }

    @Test
    public void testInsertFromFunctionReturningLong() throws Exception {
        ddl("create table x (u UUID)");
        assertException("insert into x values (rnd_long())", 22, "inconvertible types");
    }

    @Test
    public void testInsertFromFunctionReturningString() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (rnd_str('11111111-1111-1111-1111-111111111111'))");
        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "x"
        );
    }

    @Test
    public void testInsertNullByOmitting() throws Exception {
        ddl("create table x (i INT, u UUID, i2 INT)");
        insert("insert into x (i, i2) values (42, 0)");
        assertSql(
                "i\tu\ti2\n" +
                        "42\t\t0\n",
                "select * from x"
        );
    }

    @Test
    public void testInsertNullUuidColumnIntoStringColumnImplicitCast() throws Exception {
        ddl("create table x (u uuid)");
        insert("insert into x values (null)");
        ddl("create table y (s string)");
        ddl("insert into y select u from x");
        assertSql(
                "column\n" +
                        "true\n",
                "select s is null from y"
        );
    }

    @Test
    public void testInsertUuidColumnIntoIntColumnImplicitCast() throws Exception {
        ddl("create table x (u uuid)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        ddl("create table y (i int)");
        assertException("insert into y select u from x", 21, "inconvertible types");
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnExplicitCast() throws Exception {
        ddl("create table x (u uuid)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        ddl("create table y (s string)");
        ddl("insert into y select cast (u as string) from x");
        assertSql(
                "s\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from y"
        );
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnImplicitCast() throws Exception {
        ddl("create table x (u uuid)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        ddl("create table y (s string)");
        ddl("insert into y select u from x");
        assertSql(
                "s\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from y"
        );
    }

    @Test
    public void testInsertWithExplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertSql
                (
                        "u\n" +
                                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                        "select * from x"
                );
    }

    @Test
    public void testInsertWithImplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values ('a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertSql(
                "u\n" +
                        "a0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x"
        );
    }

    @Test
    public void testLatestOn() throws Exception {
        ddl("create table x (ts timestamp, u uuid, i int) timestamp(ts) partition by DAY");
        insert("insert into x values ('2020-01-01T00:00:00.000000Z', '00000000-0000-0000-0000-000000000001', 0)");
        insert("insert into x values ('2020-01-02T00:01:00.000000Z', '00000000-0000-0000-0000-000000000001', 2)");
        insert("insert into x values ('2020-01-02T00:01:00.000000Z', '00000000-0000-0000-0000-000000000002', 0)");

        assertSql(
                "ts\tu\ti\n" +
                        "2020-01-02T00:01:00.000000Z\t00000000-0000-0000-0000-000000000001\t2\n" +
                        "2020-01-02T00:01:00.000000Z\t00000000-0000-0000-0000-000000000002\t0\n",
                "select ts, u, i from x latest on ts partition by u"
        );
    }

    @Test
    public void testLongExplicitCastAsUuid() throws Exception {
        ddl("create table x (l long)");
        insert("insert into x values (42)");
        assertException("select cast(l as uuid) from x", 7, "unexpected argument for function");
    }

    @Test
    public void testLongsToUuid_constant() throws Exception {
        assertSql(
                "uuid\n" +
                        "00000000-0000-0001-0000-000000000002\n",
                "select to_uuid(2, 1) as uuid from long_sequence(1)"
        );
    }

    @Test
    public void testLongsToUuid_fromTable() throws Exception {
        ddl("create table x (lo long, hi long)");
        insert("insert into x values (2, 1)");
        assertSql(
                "uuid\n" +
                        "00000000-0000-0001-0000-000000000002\n",
                "select to_uuid(lo, hi) as uuid from x"
        );
    }

    @Test
    public void testLongsToUuid_nullConstant() throws Exception {
        assertSql(
                "uuid\n" +
                        "\n",
                "select to_uuid(null, null) as uuid from long_sequence(1)"
        );
    }

    @Test
    public void testLongsToUuid_nullFromTable() throws Exception {
        ddl("create table x (lo long, hi long)");
        insert("insert into x values (null, null)");
        assertSql(
                "uuid\n" +
                        "\n",
                "select to_uuid(lo, hi) as uuid from x"
        );
    }

    @Test
    public void testNamedBindVariableInFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (b uuid)");
            insert("insert into x values('11111111-1111-1111-1111-111111111111')");
            insert("insert into x values('22222222-2222-2222-2222-222222222222')");
            insert("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("uuid", "22222222-2222-2222-2222-222222222222");
            assertSql(
                    "b\n" +
                            "22222222-2222-2222-2222-222222222222\n",
                    "x where b = :uuid"
            );
        });
    }

    @Test
    public void testNegatedEqualityComparisonExplicitCast() throws Exception {
        Uuid uuid = new Uuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        ddl("create table x (u UUID)");
        insert("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertSql(
                "u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u != cast('11111111-1111-1111-1111-111111111111' as uuid)"
        );
    }

    @Test
    public void testNegatedEqualityComparisonImplicitCast() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertSql(
                "u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u != '11111111-1111-1111-1111-111111111111'"
        );
    }

    @Test
    public void testNonKeyedFirstAggregation() throws Exception {
        ddl("create table xxx(u uuid)");
        insert("insert into xxx values ('54710940-38c0-4d93-92db-43b7cad84228')");
        insert("insert into xxx values ('')");

        assertSql(
                "first\n" +
                        "54710940-38c0-4d93-92db-43b7cad84228\n",
                "select first(u) from xxx"
        );
    }

    @Test
    public void testNonKeyedLastAggregation() throws Exception {
        ddl("create table xxx(u uuid)");
        insert("insert into xxx values ('54710940-38c0-4d93-92db-43b7cad84228')");
        insert("insert into xxx values ('')"); // empty string is implicitly cast to null

        assertSql(
                "last\n" +
                        "\n",
                "select last(u) from xxx"
        );
    }

    @Test
    public void testO3_differentPartition() throws Exception {
        ddl("create table x (ts timestamp, u UUID) timestamp(ts) partition by DAY");
        insert("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        insert("insert into x values (to_timestamp('2010-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

        assertSql(
                "ts\tu\n" +
                        "2010-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11\n" +
                        "2018-01-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x"
        );
    }

    @Test
    public void testO3_samePartition() throws Exception {
        ddl("create table x (ts timestamp, u UUID) timestamp(ts) partition by YEAR");
        insert("insert into x values (to_timestamp('2018-06', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        insert("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

        assertSql(
                "ts\tu\n" +
                        "2018-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11\n" +
                        "2018-06-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x"
        );
    }

    @Test
    public void testOrderByUuid() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (2, '00000000-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000000000001')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000000000010')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000000000100')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000000001000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000000010000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000000100000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000001000000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000010000000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-000100000000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-001000000000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-010000000000')");
        insert("insert into x values (1, '00000000-0000-0000-0000-100000000000')");
        insert("insert into x values (1, '00000000-0000-0000-0001-000000000000')");
        insert("insert into x values (1, '00000000-0000-0000-0010-000000000000')");
        insert("insert into x values (1, '00000000-0000-0000-0100-000000000000')");
        insert("insert into x values (1, '00000000-0000-0000-1000-000000000000')");
        insert("insert into x values (1, '00000000-0000-0001-0000-000000000000')");
        insert("insert into x values (1, '00000000-0000-0010-0000-000000000000')");
        insert("insert into x values (1, '00000000-0000-0100-0000-000000000000')");
        insert("insert into x values (1, '00000000-0000-1000-0000-000000000000')");
        insert("insert into x values (1, '00000000-0001-0000-0000-000000000000')");
        insert("insert into x values (1, '00000000-0010-0000-0000-000000000000')");
        insert("insert into x values (1, '00000000-0100-0000-0000-000000000000')");
        insert("insert into x values (1, '00000000-1000-0000-0000-000000000000')");
        insert("insert into x values (1, '00000001-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '00000010-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '00000100-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '00001000-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '00010000-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '00100000-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '01000000-0000-0000-0000-000000000000')");
        insert("insert into x values (1, '10000000-0000-0000-0000-000000000000')");

        assertSql(
                "i\tu\n" +
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
                "select * from x order by u"
        );

        assertSql(
                "i\tu\n" +
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
                "select * from x order by u desc"
        );
    }

    @Test
    public void testPostgresStyleLiteralCasting() throws Exception {
        assertSql(
                "column\n" +
                        "true\n",
                "select (uuid '11111111-1111-1111-1111-111111111111') = cast('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)"
        );

    }

    @Test
    public void testRandomizedOrderBy() throws Exception {
        int count = 1000;

        ddl("create table x (u UUID)");
        UUID[] uuids = new UUID[count];
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.randomUUID();
            insert("insert into x values ('" + uuid + "')");
            uuids[i] = uuid;
        }
        Arrays.sort(uuids);

        // test ascending
        StringBuilder expected = new StringBuilder("u\n");
        for (int i = 0; i < count; i++) {
            expected.append(uuids[i]).append("\n");
        }
        assertSql(expected, "select * from x order by u");

        // test descending
        expected = new StringBuilder("u\n");
        for (int i = count - 1; i >= 0; i--) {
            expected.append(uuids[i]).append("\n");
        }
        assertSql(expected, "select * from x order by u desc");
    }

    @Test
    public void testRndUuid() throws Exception {
        ddl("create table x as (select rnd_uuid4() from long_sequence(10))");
        assertSql(
                "rnd_uuid4\n" +
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
                "select * from x"
        );
    }

    @Test
    public void testStrColumnInFilter() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (b uuid, a string)");
            insert("insert into x values('11111111-1111-1111-1111-111111111111','foobar')");
            insert("insert into x values('22222222-2222-2222-2222-222222222222','22222222-2222-2222-2222-222222222222')");
            insert("insert into x values('33333333-3333-3333-3333-333333333333','barbaz')");
            assertSql(
                    "b\ta\n" +
                            "22222222-2222-2222-2222-222222222222\t22222222-2222-2222-2222-222222222222\n",
                    "x where b = a"
            );
        });
    }

    @Test
    public void testStringOverloadFunction() throws Exception {
        ddl("create table x (u uuid)");

        insert("insert into x values ('22222222-2222-2222-2222-222222222222')");

        assertSql(
                "length\n" +
                        "36\n",
                "select length(u) from x"
        );
    }

    @Test
    public void testStringOverloadFunctionWithNull() throws Exception {
        ddl("create table x (u uuid)");
        insert("insert into x values (null)");
        assertSql(
                "length\n" +
                        "-1\n",
                "select length(u) from x"
        );
    }

    @Test
    public void testTwoVarComparison() throws Exception {
        ddl("create table x (u1 UUID, u2 UUID)");
        insert("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
        insert("insert into x values ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111')");
        assertSql(
                "u1\tu2\n" +
                        "11111111-1111-1111-1111-111111111111\t11111111-1111-1111-1111-111111111111\n",
                "select * from x where u1 = u2"
        );
    }

    @Test
    public void testTypeOf() throws Exception {
        assertSql(
                "typeOf\n" +
                        "UUID\n",
                "select typeOf(uuid '11111111-1111-1111-1111-111111111111') from long_sequence(1)"
        );
    }

    @Test
    public void testUnionAllArbitraryStringWithUuid() throws Exception {
        ddl("create table x (u string)");
        ddl("create table y (u uuid)");

        insert("insert into x values ('totally not a uuid')");
        insert("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertSql(
                "u\n" +
                        "totally not a uuid\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUnionAllDups() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u UUID)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values ('11111111-1111-1111-1111-111111111111')");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from x union all select * from y"
        );
    }

    @Test
    public void testUnionAllNull() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u UUID)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values (null)");
        insert("insert into y values (null)");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "\n" +
                        "\n",
                "select * from x union all select * from y"
        );
    }

    @Test
    public void testUnionAllSimple() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u UUID)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union all select * from y"
        );
    }

    @Test
    public void testUnionAllStringWithUuid() throws Exception {
        ddl("create table x (u string)");
        ddl("create table y (u uuid)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUnionAllUuidWithArbitraryString() throws Exception {
        ddl("create table x (u uuid)");
        ddl("create table y (u string)");

        insert("insert into x values ('22222222-2222-2222-2222-222222222222')");
        insert("insert into y values ('totally not a uuid')");

        assertSql(
                "u\n" +
                        "22222222-2222-2222-2222-222222222222\n" +
                        "totally not a uuid\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUnionAllUuidWithString() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u string)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUnionDups() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u UUID)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values ('11111111-1111-1111-1111-111111111111')");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUnionNull() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u UUID)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values (null)");
        insert("insert into y values (null)");

        // only one null is returned - dups null are eliminated
        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUnionSimple() throws Exception {
        ddl("create table x (u UUID)");
        ddl("create table y (u UUID)");

        insert("insert into x values ('11111111-1111-1111-1111-111111111111')");
        insert("insert into y values ('22222222-2222-2222-2222-222222222222')");

        assertSql(
                "u\n" +
                        "11111111-1111-1111-1111-111111111111\n" +
                        "22222222-2222-2222-2222-222222222222\n",
                "select * from x union select * from y"
        );
    }

    @Test
    public void testUpdateByUuid_nonPartitionedTable() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        update("update x set i = 42 where u = 'a0eebc11-110b-11f8-116d-11b9bd380a11'");
        assertSql(
                "i\tu\n" +
                        "42\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x"
        );
    }

    @Test
    public void testUpdateByUuid_partitionedTable() throws Exception {
        ddl("create table x (ts TIMESTAMP, i INT, u UUID) timestamp(ts) partition by DAY");
        insert("insert into x values (now(), 0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        update("update x set i = 42 where u = 'a0eebc11-110b-11f8-116d-11b9bd380a11'");
        assertSql(
                "i\tu\n" +
                        "42\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select i, u from x"
        );
    }

    @Test
    public void testUpdateUuid() throws Exception {
        ddl("create table x (i INT, u UUID)");
        insert("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        update("update x set u = 'a0eebc11-4242-11f8-116d-11b9bd380a11' where i = 0");
        assertSql(
                "i\tu\n" +
                        "0\ta0eebc11-4242-11f8-116d-11b9bd380a11\n",
                "select * from x"
        );
    }

    @Test
    public void testUpdateUuidWithVarchar() throws Exception {
        ddl("create table x (i INT, v VARCHAR, u UUID)");
        insert("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11', null)");
        update("update x set u = v where i = 0");
        assertSql(
                "i\tv\tu\n" +
                        "0\ta0eebc11-110b-11f8-116d-11b9bd380a11\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x"
        );
    }

    @Test
    public void testUuidExplicitCastAsLong() throws Exception {
        ddl("create table x (u UUID)");
        insert("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertSql(
                "cast\n" +
                        "NaN\n",
                "select cast(u as long) from x"
        );
    }
}
