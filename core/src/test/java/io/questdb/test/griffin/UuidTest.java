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

package io.questdb.test.griffin;

import io.questdb.std.Uuid;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

public class UuidTest extends AbstractCairoTest {

    @Test
    public void testBadConstantUuidWithExplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            assertQuery("insert into x values (cast ('a0eebc11-110b-11f8-116d' as uuid))")
                    .noLeakCheck()
                    .fails(28, "invalid UUID constant");
        });
    }

    @Test
    public void testBadUuidWithImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            assertQuery("insert into x values ('a0eebc11-110b-11f8-116d')")
                    .noLeakCheck()
                    .fails(0, "inconvertible value: `a0eebc11-110b-11f8-116d` [STRING -> UUID]");
        });
    }

    @Test
    public void testBindVariableInFilterInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertQuery("x where b = $1")
                    .noLeakCheck()
                    .returns("b\n");
        });
    }

    @Test
    public void testBindVariableInFilterInvalidNegated() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "foobar");
            assertQuery("x where b != $1")
                    .noLeakCheck()
                    .returns("""
                            b
                            11111111-1111-1111-1111-111111111111
                            22222222-2222-2222-2222-222222222222
                            33333333-3333-3333-3333-333333333333
                            """);
        });
    }

    @Test
    public void testCastVarcharToUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, cast(cast('11111111-1111-1111-1111-111111111111' as varchar) as uuid))");

            assertQuery("x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu
                            0\t11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testCastingConstNullUUIDtoString() throws Exception {
        assertMemoryLeak(() -> assertQuery("select cast (cast (null as uuid) as string) is null from long_sequence(1)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        true
                        """));
    }

    @Test
    public void testCastingNullUUIDtoString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values (null)");
            execute("create table y (s string)");
            execute("insert into y select cast (u as string) from x");
            assertQuery("select s is null from y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);
        });
    }

    @Test
    public void testCompareConstantNullStringWithUuid() throws Exception {
        assertMemoryLeak(() -> assertQuery("select cast (null as string) = cast (null as uuid) from long_sequence(1)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        true
                        """));
    }

    @Test
    public void testComparisonWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            // UUID is implicitly cast to String
            // and we can compare strings to symbols
            execute("create table x (u UUID, s SYMBOL)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111', 'whatever')");
            execute("insert into x values (null, null)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111', null)");
            execute("insert into x values (null, 'whatever')");

            assertQuery("select u = s from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            false
                            true
                            false
                            false
                            """);
        });
    }

    @Test
    public void testConcatFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u1 UUID, u2 UUID, u3 UUID)");
            execute("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), null, null)");
            execute("insert into x values (null, null, null)");
            execute("insert into x values (cast('11111111-1111-1111-1111-111111111111' as uuid), null, '22222222-2222-2222-2222-222222222222')");

            assertQuery("select concat(u1, u2, u3) from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            concat
                            11111111-1111-1111-1111-11111111111122222222-2222-2222-2222-22222222222233333333-3333-3333-3333-333333333333
                            11111111-1111-1111-1111-111111111111
                            
                            11111111-1111-1111-1111-11111111111122222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testConstantComparison() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select '11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);

            assertQuery("select '11111111-1111-1111-1111-111111111111' = cast ('22222222-2222-2222-2222-222222222222' as uuid) from long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            false
                            """);
        });
    }

    @Test
    public void testConstantInFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333')");
            assertQuery("x where b = '22222222-2222-2222-2222-222222222222'")
                    .noLeakCheck()
                    .returns("""
                            b
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testCopyVarcharToUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u VARCHAR)");
            execute("insert into x values (0, cast('11111111-1111-1111-1111-111111111111' as varchar))");
            execute("insert into x values (0, cast('22222222-2222-2222-2222-222222222222' as varchar))");
            execute("insert into x values (1, cast('33333333-3333-3333-3333-333333333333' as varchar))");
            execute("insert into x values (1, cast('33333333-3333-3333-3333-333333333333' as varchar))");
            execute("insert into x values (1, cast('33333333-3333-3333-3333-333333333333' as varchar))");

            String expected = """
                    i\tcount_distinct
                    0\t2
                    1\t1
                    """;
            assertQuery("select i, count_distinct(u) from x group by i order by i")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("select i, count(distinct u) from x group by i order by i")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testCountAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

            assertQuery("select i, count() from x group by i order by i")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tcount
                            0\t2
                            1\t3
                            """);
        });
    }

    @Test
    public void testCountDistinctAggregation_keyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

            String expected = """
                    i\tcount_distinct
                    0\t2
                    1\t1
                    """;
            assertQuery("select i, count_distinct(u) from x group by i order by i")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
            assertQuery("select i, count(distinct u) from x group by i order by i")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testCountDistinctAggregation_nonkeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values (0, '22222222-2222-2222-2222-222222222222')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");
            execute("insert into x values (1, '33333333-3333-3333-3333-333333333333')");

            String expected = """
                    count_distinct
                    3
                    """;
            assertQuery("select count_distinct(u) from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
            assertQuery("select count(distinct u) from x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns(expected);
        });
    }

    @Test
    public void testEqConstStringToUuid() throws Exception {
        assertMemoryLeak(() -> assertSql(
                """
                        column\tcolumn1\tcolumn2\tcolumn3\tcolumn4
                        true\tfalse\ttrue\tfalse\tfalse
                        """,
                "select " +
                        "cast (null as string) = cast (null as uuid), " +
                        "cast (null as string) = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'not a uuid' = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                        "'11111111-1111-1111-1111-111111111111' = rnd_uuid4()" +
                        "from long_sequence(1)"
        ));
    }

    @Test
    public void testEqVarBadStringToVarNullUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s STRING, u UUID)");
            execute("insert into x values ('not a uuid', null)");
            assertQuery("select s = u from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            false
                            """);
        });
    }

    @Test
    public void testEqVarStringToUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s1 STRING, s2 STRING, s3 STRING, s4 STRING, s5 STRING)");
            execute("insert into x values (null, null, '11111111-1111-1111-1111-111111111111', 'not a uuid', '11111111-1111-1111-1111-111111111111')");

            assertSql(
                    """
                            column\tcolumn1\tcolumn2\tcolumn3\tcolumn4
                            true\tfalse\ttrue\tfalse\tfalse
                            """,
                    "select " +
                            "s1 = cast (null as uuid), " +
                            "s2 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                            "s3 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                            "s4 = cast ('11111111-1111-1111-1111-111111111111' as uuid), " +
                            "s5 = rnd_uuid4() " +
                            "from x"
            );
        });
    }

    @Test
    public void testEqualityComparisonConstantOnLeft() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
            assertQuery("select * from x where 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' = u")
                    .noLeakCheck()
                    .returns("""
                            u
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """);
        });
    }

    @Test
    public void testEqualityComparisonExplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            assertQuery("select * from x where u = cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid)")
                    .noLeakCheck()
                    .returns("""
                            u
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """);
        });
    }

    @Test
    public void testEqualityComparisonImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
            assertQuery("select * from x where u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'")
                    .noLeakCheck()
                    .returns("""
                            u
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """);
        });
    }

    @Test
    public void testExplicitCastWithEmptyStringConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (cast('' as uuid))");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u
                            
                            """);
        });
    }

    @Test
    public void testExplicitCastWithNullStringConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (cast(null as string))");
            assertQuery("x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u
                            
                            """);
        });
    }

    @Test
    public void testGroupByUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values (1, '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values (2, '22222222-2222-2222-2222-222222222222')");
            execute("insert into x values (3, '22222222-2222-2222-2222-222222222222')");

            assertQuery("select u, sum(i) from x group by u order by u")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u\tsum
                            11111111-1111-1111-1111-111111111111\t1
                            22222222-2222-2222-2222-222222222222\t5
                            """);
        });
    }

    @Test
    public void testIn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values ('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values ('33333333-3333-3333-3333-333333333333')");
            execute("insert into x values ('44444444-4444-4444-4444-444444444444')");
            execute("insert into x values (null)");

            assertQuery("select * from x where u in ('11111111-1111-1111-1111-111111111111', '55555555-5555-5555-5555-555555555555', cast ('22222222-2222-2222-2222-222222222222' as UUID), cast ('33333333-3333-3333-3333-333333333333' as symbol))")
                    .noLeakCheck()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            22222222-2222-2222-2222-222222222222
                            33333333-3333-3333-3333-333333333333
                            """);
        });
    }

    @Test
    public void testIn_constant() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("select * from long_sequence(1) where cast ('11111111-1111-1111-1111-111111111111' as uuid) in ('11111111-1111-1111-1111-111111111111')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            1
                            """);

            assertQuery("select * from long_sequence(1) where cast (null as uuid) in ('11111111-1111-1111-1111-111111111111')")
                    .noLeakCheck()
                    .returns("x\n");
        });
    }

    @Test
    public void testIn_unexpectedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");

            assertQuery("select * from x where u in (42)")
                    .fails(28, "cannot compare UUID with type INT");
        });
    }

    @Test
    public void testIndexedBindVariableInFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "22222222-2222-2222-2222-222222222222");
            assertQuery("x where b = $1")
                    .noLeakCheck()
                    .returns("""
                            b
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testIndexedBindVariableInFilter2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "22222222-2222-2222-2222-222222222222");
            assertQuery("x where $1 = b")
                    .noLeakCheck()
                    .returns("""
                            b
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testInsertAddUuidColumnAndThenO3Insert() throws Exception {
        assertMemoryLeak(() -> {
            // testing O3 insert when uuid columnTop > 0
            execute("create table x (ts timestamp, i int) timestamp(ts) partition by MONTH");
            execute("insert into x values ('2018-01-01', 1)");
            execute("insert into x values ('2018-01-03', 1)");
            execute("alter table x add column u uuid");
            execute("insert into x values ('2018-01-02', 1, '00000000-0000-0000-0000-000000000000')");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\ti\tu
                            2018-01-01T00:00:00.000000Z\t1\t
                            2018-01-02T00:00:00.000000Z\t1\t00000000-0000-0000-0000-000000000000
                            2018-01-03T00:00:00.000000Z\t1\t
                            """);
        });
    }

    @Test
    public void testInsertExplicitNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (null)");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u
                            
                            """);
        });
    }

    @Test
    public void testInsertFromFunctionReturningLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            assertQuery("insert into x values (rnd_long())")
                    .noLeakCheck()
                    .fails(22, "inconvertible types");
        });
    }

    @Test
    public void testInsertFromFunctionReturningString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (rnd_str('11111111-1111-1111-1111-111111111111'))");
            assertQuery("x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testInsertNullByOmitting() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID, i2 INT)");
            execute("insert into x (i, i2) values (42, 0)");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu\ti2
                            42\t\t0
                            """);
        });
    }

    @Test
    public void testInsertNullUuidColumnIntoStringColumnImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values (null)");
            execute("create table y (s string)");
            execute("insert into y select u from x");
            assertQuery("select s is null from y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            true
                            """);
        });
    }

    @Test
    public void testInsertUuidColumnIntoIntColumnImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("create table y (i int)");
            assertQuery("insert into y select u from x")
                    .noLeakCheck()
                    .fails(21, "inconvertible types");
        });
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnExplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("create table y (s string)");
            execute("insert into y select cast (u as string) from x");
            assertQuery("select * from y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("create table y (s string)");
            execute("insert into y select u from x");
            assertQuery("select * from y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s
                            11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testInsertWithExplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """);
        });
    }

    @Test
    public void testInsertWithImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values ('a0eebc11-110b-11f8-116d-11b9bd380a11')");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            u
                            a0eebc11-110b-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, u uuid, i int) timestamp(ts) partition by DAY");
            execute("insert into x values ('2020-01-01T00:00:00.000000Z', '00000000-0000-0000-0000-000000000001', 0)");
            execute("insert into x values ('2020-01-02T00:01:00.000000Z', '00000000-0000-0000-0000-000000000001', 2)");
            execute("insert into x values ('2020-01-02T00:01:00.000000Z', '00000000-0000-0000-0000-000000000002', 0)");

            assertQuery("select ts, u, i from x latest on ts partition by u")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tu\ti
                            2020-01-02T00:01:00.000000Z\t00000000-0000-0000-0000-000000000001\t2
                            2020-01-02T00:01:00.000000Z\t00000000-0000-0000-0000-000000000002\t0
                            """);
        });
    }

    @Test
    public void testLongExplicitCastAsUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (l long)");
            execute("insert into x values (42)");
            assertQuery("select cast(l as uuid) from x")
                    .noLeakCheck()
                    .fails(7, "there is no matching function `cast` with the argument types: (LONG, UUID)");
        });
    }

    @Test
    public void testLongsToUuid_constant() throws Exception {
        assertMemoryLeak(() -> assertQuery("select to_uuid(2, 1) as uuid from long_sequence(1)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        uuid
                        00000000-0000-0001-0000-000000000002
                        """));
    }

    @Test
    public void testLongsToUuid_fromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (lo long, hi long)");
            execute("insert into x values (2, 1)");
            assertQuery("select to_uuid(lo, hi) as uuid from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            uuid
                            00000000-0000-0001-0000-000000000002
                            """);
        });
    }

    @Test
    public void testLongsToUuid_nullConstant() throws Exception {
        assertMemoryLeak(() -> assertQuery("select to_uuid(null, null) as uuid from long_sequence(1)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        uuid
                        
                        """));
    }

    @Test
    public void testLongsToUuid_nullFromTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (lo long, hi long)");
            execute("insert into x values (null, null)");
            assertQuery("select to_uuid(lo, hi) as uuid from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            uuid
                            
                            """);
        });
    }

    @Test
    public void testNamedBindVariableInFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333')");

            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr("uuid", "22222222-2222-2222-2222-222222222222");
            assertQuery("x where b = :uuid")
                    .noLeakCheck()
                    .returns("""
                            b
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testNegatedEqualityComparisonExplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            Uuid uuid = new Uuid();
            uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
            execute("create table x (u UUID)");
            execute("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            assertQuery("select * from x where u != cast('11111111-1111-1111-1111-111111111111' as uuid)")
                    .noLeakCheck()
                    .returns("""
                            u
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """);
        });
    }

    @Test
    public void testNegatedEqualityComparisonImplicitCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
            assertQuery("select * from x where u != '11111111-1111-1111-1111-111111111111'")
                    .noLeakCheck()
                    .returns("""
                            u
                            a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                            """);
        });
    }

    @Test
    public void testNonKeyedFirstAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xxx(u uuid)");
            execute("insert into xxx values ('54710940-38c0-4d93-92db-43b7cad84228')");
            execute("insert into xxx values ('')");

            assertQuery("select first(u) from xxx")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            first
                            54710940-38c0-4d93-92db-43b7cad84228
                            """);
        });
    }

    @Test
    public void testNonKeyedLastAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table xxx(u uuid)");
            execute("insert into xxx values ('54710940-38c0-4d93-92db-43b7cad84228')");
            execute("insert into xxx values ('')"); // empty string is implicitly cast to null

            assertQuery("select last(u) from xxx")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            last
                            
                            """);
        });
    }

    @Test
    public void testO3_differentPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, u UUID) timestamp(ts) partition by DAY");
            execute("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
            execute("insert into x values (to_timestamp('2010-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tu
                            2010-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11
                            2018-01-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testO3_samePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, u UUID) timestamp(ts) partition by YEAR");
            execute("insert into x values (to_timestamp('2018-06', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
            execute("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tu
                            2018-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11
                            2018-06-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testOrderByUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (2, '00000000-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000000000001')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000000000010')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000000000100')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000000001000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000000010000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000000100000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000001000000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000010000000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-000100000000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-001000000000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-010000000000')");
            execute("insert into x values (1, '00000000-0000-0000-0000-100000000000')");
            execute("insert into x values (1, '00000000-0000-0000-0001-000000000000')");
            execute("insert into x values (1, '00000000-0000-0000-0010-000000000000')");
            execute("insert into x values (1, '00000000-0000-0000-0100-000000000000')");
            execute("insert into x values (1, '00000000-0000-0000-1000-000000000000')");
            execute("insert into x values (1, '00000000-0000-0001-0000-000000000000')");
            execute("insert into x values (1, '00000000-0000-0010-0000-000000000000')");
            execute("insert into x values (1, '00000000-0000-0100-0000-000000000000')");
            execute("insert into x values (1, '00000000-0000-1000-0000-000000000000')");
            execute("insert into x values (1, '00000000-0001-0000-0000-000000000000')");
            execute("insert into x values (1, '00000000-0010-0000-0000-000000000000')");
            execute("insert into x values (1, '00000000-0100-0000-0000-000000000000')");
            execute("insert into x values (1, '00000000-1000-0000-0000-000000000000')");
            execute("insert into x values (1, '00000001-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '00000010-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '00000100-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '00001000-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '00010000-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '00100000-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '01000000-0000-0000-0000-000000000000')");
            execute("insert into x values (1, '10000000-0000-0000-0000-000000000000')");

            assertQuery("select * from x order by u")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu
                            2\t00000000-0000-0000-0000-000000000000
                            1\t00000000-0000-0000-0000-000000000001
                            1\t00000000-0000-0000-0000-000000000010
                            1\t00000000-0000-0000-0000-000000000100
                            1\t00000000-0000-0000-0000-000000001000
                            1\t00000000-0000-0000-0000-000000010000
                            1\t00000000-0000-0000-0000-000000100000
                            1\t00000000-0000-0000-0000-000001000000
                            1\t00000000-0000-0000-0000-000010000000
                            1\t00000000-0000-0000-0000-000100000000
                            1\t00000000-0000-0000-0000-001000000000
                            1\t00000000-0000-0000-0000-010000000000
                            1\t00000000-0000-0000-0000-100000000000
                            1\t00000000-0000-0000-0001-000000000000
                            1\t00000000-0000-0000-0010-000000000000
                            1\t00000000-0000-0000-0100-000000000000
                            1\t00000000-0000-0000-1000-000000000000
                            1\t00000000-0000-0001-0000-000000000000
                            1\t00000000-0000-0010-0000-000000000000
                            1\t00000000-0000-0100-0000-000000000000
                            1\t00000000-0000-1000-0000-000000000000
                            1\t00000000-0001-0000-0000-000000000000
                            1\t00000000-0010-0000-0000-000000000000
                            1\t00000000-0100-0000-0000-000000000000
                            1\t00000000-1000-0000-0000-000000000000
                            1\t00000001-0000-0000-0000-000000000000
                            1\t00000010-0000-0000-0000-000000000000
                            1\t00000100-0000-0000-0000-000000000000
                            1\t00001000-0000-0000-0000-000000000000
                            1\t00010000-0000-0000-0000-000000000000
                            1\t00100000-0000-0000-0000-000000000000
                            1\t01000000-0000-0000-0000-000000000000
                            1\t10000000-0000-0000-0000-000000000000
                            """);

            assertQuery("select * from x order by u desc")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu
                            1\t10000000-0000-0000-0000-000000000000
                            1\t01000000-0000-0000-0000-000000000000
                            1\t00100000-0000-0000-0000-000000000000
                            1\t00010000-0000-0000-0000-000000000000
                            1\t00001000-0000-0000-0000-000000000000
                            1\t00000100-0000-0000-0000-000000000000
                            1\t00000010-0000-0000-0000-000000000000
                            1\t00000001-0000-0000-0000-000000000000
                            1\t00000000-1000-0000-0000-000000000000
                            1\t00000000-0100-0000-0000-000000000000
                            1\t00000000-0010-0000-0000-000000000000
                            1\t00000000-0001-0000-0000-000000000000
                            1\t00000000-0000-1000-0000-000000000000
                            1\t00000000-0000-0100-0000-000000000000
                            1\t00000000-0000-0010-0000-000000000000
                            1\t00000000-0000-0001-0000-000000000000
                            1\t00000000-0000-0000-1000-000000000000
                            1\t00000000-0000-0000-0100-000000000000
                            1\t00000000-0000-0000-0010-000000000000
                            1\t00000000-0000-0000-0001-000000000000
                            1\t00000000-0000-0000-0000-100000000000
                            1\t00000000-0000-0000-0000-010000000000
                            1\t00000000-0000-0000-0000-001000000000
                            1\t00000000-0000-0000-0000-000100000000
                            1\t00000000-0000-0000-0000-000010000000
                            1\t00000000-0000-0000-0000-000001000000
                            1\t00000000-0000-0000-0000-000000100000
                            1\t00000000-0000-0000-0000-000000010000
                            1\t00000000-0000-0000-0000-000000001000
                            1\t00000000-0000-0000-0000-000000000100
                            1\t00000000-0000-0000-0000-000000000010
                            1\t00000000-0000-0000-0000-000000000001
                            2\t00000000-0000-0000-0000-000000000000
                            """);
        });
    }

    @Test
    public void testPostgresStyleLiteralCasting() throws Exception {
        assertMemoryLeak(() -> assertQuery("select (uuid '11111111-1111-1111-1111-111111111111') = cast('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        column
                        true
                        """));
    }

    @Test
    public void testRandomizedOrderBy() throws Exception {
        final int count = 1000;
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            UUID[] reference = new UUID[count];
            for (int i = 0; i < count; i++) {
                UUID uuid = UUID.randomUUID();
                execute("insert into x values ('" + uuid + "')");
                reference[i] = uuid;
            }
            // insert a null UUID too, we care about order of null UUIDs
            // they are considered lower than any non-null value
            execute("insert into x values (null)");

            // use string representation of UUIDs to generate reference
            // order since JDK has a bug in UUID.compareTo()
            // see https://bugs.openjdk.org/browse/JDK-7025832
            Arrays.sort(reference, Comparator.comparing(UUID::toString));

            // test ascending
            StringBuilder expected = new StringBuilder("u\n");
            // the null value must be first
            expected.append("\n");
            // then non-null UUIDs
            for (int i = 0; i < count; i++) {
                expected.append(reference[i]).append("\n");
            }
            assertQuery("select * from x order by u")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);

            // test descending
            expected = new StringBuilder("u\n");
            // first non-null UUIDs
            for (int i = count - 1; i >= 0; i--) {
                expected.append(reference[i]).append("\n");
            }
            // then null
            expected.append("\n");
            assertQuery("select * from x order by u desc")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testRndUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select rnd_uuid4() from long_sequence(10))");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            rnd_uuid4
                            0010cde8-12ce-40ee-8010-a928bb8b9650
                            9f9b2131-d49f-4d1d-ab81-39815c50d341
                            7bcd48d8-c77a-4655-b2a2-15ba0462ad15
                            b5b2159a-2356-4217-965d-4c984f0ffa8a
                            e8beef38-cd7b-43d8-9b2d-34586f6275fa
                            322a2198-864b-4b14-b97f-a69eb8fec6cc
                            980eca62-a219-40f1-a846-d7a3aa5aecce
                            c1e63128-5c1a-4288-872b-fc5230158059
                            716de3d2-5dcc-4d91-9fa2-397a5d8c84c4
                            4b0f595f-143e-4d72-af1a-8266e7921e3b
                            """);
        });
    }

    @Test
    public void testStrColumnInFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (b uuid, a string)");
            execute("insert into x values('11111111-1111-1111-1111-111111111111','foobar')");
            execute("insert into x values('22222222-2222-2222-2222-222222222222','22222222-2222-2222-2222-222222222222')");
            execute("insert into x values('33333333-3333-3333-3333-333333333333','barbaz')");
            assertQuery("x where b = a")
                    .noLeakCheck()
                    .returns("""
                            b\ta
                            22222222-2222-2222-2222-222222222222\t22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testStringOverloadFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values ('22222222-2222-2222-2222-222222222222')");
            assertQuery("select length(u) from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            length
                            36
                            """);
        });
    }

    @Test
    public void testStringOverloadFunctionWithNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("insert into x values (null)");
            assertQuery("select length(u) from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            length
                            -1
                            """);
        });
    }

    @Test
    public void testTwoVarComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u1 UUID, u2 UUID)");
            execute("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
            execute("insert into x values ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111')");
            assertQuery("select * from x where u1 = u2")
                    .noLeakCheck()
                    .returns("""
                            u1\tu2
                            11111111-1111-1111-1111-111111111111\t11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testTypeOf() throws Exception {
        assertMemoryLeak(() -> assertQuery("select typeOf(uuid '11111111-1111-1111-1111-111111111111') from long_sequence(1)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        typeOf
                        UUID
                        """));
    }

    @Test
    public void testUnionAllArbitraryStringWithUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u string)");
            execute("create table y (u uuid)");

            execute("insert into x values ('totally not a uuid')");
            execute("insert into y values ('22222222-2222-2222-2222-222222222222')");

            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            totally not a uuid
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testUnionAllDups() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u UUID)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values ('11111111-1111-1111-1111-111111111111')");

            assertQuery("select * from x union all select * from y")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testUnionAllNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u UUID)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values (null)");
            execute("insert into y values (null)");

            assertQuery("select * from x union all select * from y")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            
                            
                            """);
        });
    }

    @Test
    public void testUnionAllSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u UUID)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values ('22222222-2222-2222-2222-222222222222')");

            assertQuery("select * from x union all select * from y")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testUnionAllStringWithUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u string)");
            execute("create table y (u uuid)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values ('22222222-2222-2222-2222-222222222222')");

            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testUnionAllUuidWithArbitraryString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u uuid)");
            execute("create table y (u string)");

            execute("insert into x values ('22222222-2222-2222-2222-222222222222')");
            execute("insert into y values ('totally not a uuid')");

            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            22222222-2222-2222-2222-222222222222
                            totally not a uuid
                            """);
        });
    }

    @Test
    public void testUnionAllUuidWithString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u string)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values ('22222222-2222-2222-2222-222222222222')");

            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testUnionDups() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u UUID)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values ('11111111-1111-1111-1111-111111111111')");

            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            """);
        });
    }

    @Test
    public void testUnionNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u UUID)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values (null)");
            execute("insert into y values (null)");

            // only one null is returned - dups null are eliminated
            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            
                            """);
        });
    }

    @Test
    public void testUnionSimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("create table y (u UUID)");

            execute("insert into x values ('11111111-1111-1111-1111-111111111111')");
            execute("insert into y values ('22222222-2222-2222-2222-222222222222')");

            assertQuery("select * from x union select * from y")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            u
                            11111111-1111-1111-1111-111111111111
                            22222222-2222-2222-2222-222222222222
                            """);
        });
    }

    @Test
    public void testUpdateByUuid_nonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
            update("update x set i = 42 where u = 'a0eebc11-110b-11f8-116d-11b9bd380a11'");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu
                            42\ta0eebc11-110b-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testUpdateByUuid_partitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts TIMESTAMP, i INT, u UUID) timestamp(ts) partition by DAY");
            execute("insert into x values (now(), 0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
            update("update x set i = 42 where u = 'a0eebc11-110b-11f8-116d-11b9bd380a11'");
            assertQuery("select i, u from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu
                            42\ta0eebc11-110b-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testUpdateUuid() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, u UUID)");
            execute("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
            update("update x set u = 'a0eebc11-4242-11f8-116d-11b9bd380a11' where i = 0");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tu
                            0\ta0eebc11-4242-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testUpdateUuidWithVarchar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (i INT, v VARCHAR, u UUID)");
            execute("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11', null)");
            update("update x set u = v where i = 0");
            assertQuery("select * from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i\tv\tu
                            0\ta0eebc11-110b-11f8-116d-11b9bd380a11\ta0eebc11-110b-11f8-116d-11b9bd380a11
                            """);
        });
    }

    @Test
    public void testUuidExplicitCastAsLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (u UUID)");
            execute("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            assertQuery("select cast(u as long) from x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);
        });
    }
}
