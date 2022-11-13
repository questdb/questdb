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
