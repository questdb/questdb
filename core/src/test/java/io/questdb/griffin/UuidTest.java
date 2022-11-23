/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import io.questdb.cairo.ImplicitCastException;
import io.questdb.std.MutableUuid;
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
                "select cast (cast (null as uuid) as string) is null from long_sequence(1)", null, null, true, true, true);
    }

    @Test
    public void testCastingConstUUIDtoString() throws Exception {
        assertQuery("column\n" +
                        "true\n",
                "select cast (cast ('11111111-1111-1111-1111-111111111111' as uuid) as string) = '11111111-1111-1111-1111-111111111111' from long_sequence(1)", null, null, true, true, true);
    }

    @Test
    public void testCastingNullUUIDtoString() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values (null)");
        assertCompile("create table y (s string)");
        assertCompile("insert into y select cast (u as string) from x");
        assertQuery("column\n" +
                        "true\n",
                "select s is null from y", null, null, true, true, true);
    }

    @Test
    public void testConstantComparison() throws Exception {
        assertQuery("column\n" +
                        "true\n",
                "select '11111111-1111-1111-1111-111111111111' = cast ('11111111-1111-1111-1111-111111111111' as uuid) from long_sequence(1)", null, null, true, true, true);

        assertQuery("column\n" +
                        "false\n",
                "select '11111111-1111-1111-1111-111111111111' = cast ('22222222-2222-2222-2222-222222222222' as uuid) from long_sequence(1)", null, null, true, true, true);
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
                "select i, count() from x group by i order by i", null, null, true, true, true);
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
                "select i, count_distinct(u) from x group by i order by i", null, null, true, true, true);
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
                "select count_distinct(u) from x", null, null, false, true, true);
    }

    @Test
    public void testEqualityComparisonConstantOnLeft() throws Exception {
        MutableUuid uuid = new MutableUuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' = u", null, null, true, true, false);
    }

    @Test
    public void testEqualityComparisonExplicitCast() throws Exception {
        MutableUuid uuid = new MutableUuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u = cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid)", null, null, true, true, false);
    }

    @Test
    public void testEqualityComparisonImplicitCast() throws Exception {
        MutableUuid uuid = new MutableUuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", null, null, true, true, false);
    }

    @Test
    public void testExplicitCastWithEmptyStringConstant() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('' as uuid))");
        assertQuery("u\n" +
                        "\n",
                "select * from x", null, null, true, true, true);
    }

    @Test
    public void testExplicitCastWithNullStringConstant() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast(null as string))");
        assertQuery("u\n" +
                        "\n",
                "select * from x", null, null, true, true, true);
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
                "select u, sum(i) from x group by u", null, null, true, true, true);
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
                "select * from x", null, null, true, true, true);
    }

    @Test
    public void testInsertNullByOmitting() throws Exception {
        assertCompile("create table x (i INT, u UUID, i2 INT)");
        assertCompile("insert into x (i, i2) values (42, 0)");
        assertQuery("i\tu\ti2\n" +
                        "42\t\t0\n",
                "select * from x", null, null, true, true, true);
    }

    @Test
    public void testInsertUuidColumnIntoStringColumnExplicitCast() throws Exception {
        assertCompile("create table x (u uuid)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111')");
        assertCompile("create table y (s string)");
        assertCompile("insert into y select cast (u as string) from x");
        assertQuery("s\n" +
                        "11111111-1111-1111-1111-111111111111\n",
                "select * from y", null, null, true, true, true);
    }

    @Test
    public void testInsertWithExplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x", null, null, true, true, true);
    }

    @Test
    public void testInsertWithImplicitCast() throws Exception {
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertQuery("u\n" +
                        "a0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, null, true, true, true);
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
                "select ts, u, i from x latest on ts partition by u", null, "ts", true, true, true);
    }

    @Test
    public void testNegatedEqualityComparisonExplicitCast() throws Exception {
        MutableUuid uuid = new MutableUuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u != cast('11111111-1111-1111-1111-111111111111' as uuid)", null, null, true, true, false);
    }

    @Test
    public void testNegatedEqualityComparisonImplicitCast() throws Exception {
        MutableUuid uuid = new MutableUuid();
        uuid.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assertCompile("create table x (u UUID)");
        assertCompile("insert into x values (cast('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' as uuid))");
        assertQuery("u\n" +
                        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\n",
                "select * from x where u != '11111111-1111-1111-1111-111111111111'", null, null, true, true, false);
    }

    @Test
    public void testO3_differentPartition() throws Exception {
        assertCompile("create table x (ts timestamp, u UUID) timestamp(ts) partition by DAY");
        assertCompile("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("insert into x values (to_timestamp('2010-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

        assertQuery("ts\tu\n" +
                        "2010-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11\n" +
                        "2018-01-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, "ts", true, true, true);
    }

    @Test
    public void testO3_samePartition() throws Exception {
        assertCompile("create table x (ts timestamp, u UUID) timestamp(ts) partition by YEAR");
        assertCompile("insert into x values (to_timestamp('2018-06', 'yyyy-MM'), 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("insert into x values (to_timestamp('2018-01', 'yyyy-MM'), 'a0eebc11-110b-4242-116d-11b9bd380a11')");

        assertQuery("ts\tu\n" +
                        "2018-01-01T00:00:00.000000Z\ta0eebc11-110b-4242-116d-11b9bd380a11\n" +
                        "2018-06-01T00:00:00.000000Z\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, "ts", true, true, true);
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
                "select * from x order by u", null, null, true, true, true);

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
                "select * from x order by u desc", null, null, true, true, true);
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
        assertQuery(expected.toString(), "select * from x order by u", null, null, true, true, true);

        // test descending
        expected = new StringBuilder("u\n");
        for (int i = count - 1; i >= 0; i--) {
            expected.append(uuids[i]).append("\n");
        }
        assertQuery(expected.toString(), "select * from x order by u desc", null, null, true, true, true);
    }

    @Test
    public void testRndUuid() throws Exception {
        assertCompile("create table x as (select rnd_uuid() from long_sequence(10))");
        assertQuery("rnd_uuid\n" +
                        "0010a928-bb8b-9650-0010-cde812ce60ee\n" +
                        "6b813981-5c50-d341-9f9b-2131d49fcd1d\n" +
                        "72a215ba-0462-ad15-7bcd-48d8c77aa655\n" +
                        "965d4c98-4f0f-fa8a-b5b2-159a23565217\n" +
                        "db2d3458-6f62-75fa-e8be-ef38cd7bb3d8\n" +
                        "797fa69e-b8fe-c6cc-322a-2198864beb14\n" +
                        "6846d7a3-aa5a-ecce-980e-ca62a219a0f1\n" +
                        "c72bfc52-3015-8059-c1e6-31285c1ab288\n" +
                        "9fa2397a-5d8c-84c4-716d-e3d25dcc2d91\n" +
                        "2f1a8266-e792-1e3b-4b0f-595f143e5d72\n",
                "select * from x", null, null, true, true, true);
    }

    @Test
    public void testTwoVarComparison() throws Exception {
        assertCompile("create table x (u1 UUID, u2 UUID)");
        assertCompile("insert into x values ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111111')");
        assertCompile("insert into x values ('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111')");
        assertQuery("u1\tu2\n" +
                        "11111111-1111-1111-1111-111111111111\t11111111-1111-1111-1111-111111111111\n",
                "select * from x where u1 = u2", null, null, true, true, false);
    }

    @Test
    public void testUpdateByUuid() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("update x set i = 42 where u = 'a0eebc11-110b-11f8-116d-11b9bd380a11'");
        assertQuery("i\tu\n" +
                        "42\ta0eebc11-110b-11f8-116d-11b9bd380a11\n",
                "select * from x", null, null, true, true, true);
    }

    @Test
    public void testUpdateUuid() throws Exception {
        assertCompile("create table x (i INT, u UUID)");
        assertCompile("insert into x values (0, 'a0eebc11-110b-11f8-116d-11b9bd380a11')");
        assertCompile("update x set u = 'a0eebc11-4242-11f8-116d-11b9bd380a11' where i = 0");
        assertQuery("i\tu\n" +
                        "0\ta0eebc11-4242-11f8-116d-11b9bd380a11\n",
                "select * from x", null, null, true, true, true);
    }
}
