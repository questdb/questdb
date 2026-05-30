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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class Long128Tests extends AbstractCairoTest {

    @Test
    public void testFatJoinOnLong128Column() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table tab1 as " +
                            "(select" +
                            " to_long128(3 * x, 6 * x) ts, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                            " cast(x as int) i" +
                            " from long_sequence(20)" +
                            ")"
            );

            execute("create table tab2 as " +
                    "(select" +
                    " to_long128(x, 2 * x) ts, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                    " cast(x as int) i" +
                    " from long_sequence(20)" +
                    ")"
            );

            assertQueryFullFatNoLeakCheck(
                    """
                            ts\tts1\tts11\ti
                            00000000-0000-0006-0000-000000000003\t00000000-0000-0006-0000-000000000003\t2022-02-24T00:00:00.000000Z\t1
                            00000000-0000-000c-0000-000000000006\t00000000-0000-000c-0000-000000000006\t2022-02-24T00:00:01.000000Z\t2
                            00000000-0000-0012-0000-000000000009\t00000000-0000-0012-0000-000000000009\t2022-02-24T00:00:02.000000Z\t3
                            00000000-0000-0018-0000-00000000000c\t00000000-0000-0018-0000-00000000000c\t2022-02-24T00:00:03.000000Z\t4
                            00000000-0000-001e-0000-00000000000f\t00000000-0000-001e-0000-00000000000f\t2022-02-24T00:00:04.000000Z\t5
                            00000000-0000-0024-0000-000000000012\t00000000-0000-0024-0000-000000000012\t2022-02-24T00:00:05.000000Z\t6
                            """,
                    "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts = tab2.ts",
                    null,
                    false,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupByLong128Column() throws Exception {
        assertQuery("select ts, count() from tab1 order by ts")
                .ddl("create table tab1 as " +
                        "(select" +
                        " to_long128(x / 4, x / 2) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")")
                .expectSize()
                .returns("""
                        ts\tcount
                        00000000-0000-0000-0000-000000000000\t1
                        00000000-0000-0001-0000-000000000000\t2
                        00000000-0000-0002-0000-000000000001\t2
                        00000000-0000-0003-0000-000000000001\t2
                        00000000-0000-0004-0000-000000000002\t2
                        00000000-0000-0005-0000-000000000002\t2
                        00000000-0000-0006-0000-000000000003\t2
                        00000000-0000-0007-0000-000000000003\t2
                        00000000-0000-0008-0000-000000000004\t2
                        00000000-0000-0009-0000-000000000004\t2
                        00000000-0000-000a-0000-000000000005\t1
                        """);
    }

    @Test
    public void testGroupByLong128ColumnWithNulls() throws Exception {
        assertQuery("select ts, count() from tab1 order by ts")
                .ddl("create table tab1 as " +
                        "(select" +
                        " case when x % 2 = 0 then to_long128(x, 2 * x) else NULL end ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")")
                .expectSize()
                .returns("""
                        ts\tcount
                        \t10
                        00000000-0000-0004-0000-000000000002\t1
                        00000000-0000-0008-0000-000000000004\t1
                        00000000-0000-000c-0000-000000000006\t1
                        00000000-0000-0010-0000-000000000008\t1
                        00000000-0000-0014-0000-00000000000a\t1
                        00000000-0000-0018-0000-00000000000c\t1
                        00000000-0000-001c-0000-00000000000e\t1
                        00000000-0000-0020-0000-000000000010\t1
                        00000000-0000-0024-0000-000000000012\t1
                        00000000-0000-0028-0000-000000000014\t1
                        """);
    }

    @Test
    public void testJoinOnLong128Column() throws Exception {
        execute(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(3 * x, 6 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")"
        );
        engine.clear();

        assertQuery("select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts = tab2.ts")
                .ddl("create table tab2 as " +
                        "(select" +
                        " to_long128(x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")")
                .noRandomAccess()
                .returns("""
                        ts\tts1\tts11\ti
                        00000000-0000-0006-0000-000000000003\t00000000-0000-0006-0000-000000000003\t2022-02-24T00:00:00.000000Z\t1
                        00000000-0000-000c-0000-000000000006\t00000000-0000-000c-0000-000000000006\t2022-02-24T00:00:01.000000Z\t2
                        00000000-0000-0012-0000-000000000009\t00000000-0000-0012-0000-000000000009\t2022-02-24T00:00:02.000000Z\t3
                        00000000-0000-0018-0000-00000000000c\t00000000-0000-0018-0000-00000000000c\t2022-02-24T00:00:03.000000Z\t4
                        00000000-0000-001e-0000-00000000000f\t00000000-0000-001e-0000-00000000000f\t2022-02-24T00:00:04.000000Z\t5
                        00000000-0000-0024-0000-000000000012\t00000000-0000-0024-0000-000000000012\t2022-02-24T00:00:05.000000Z\t6
                        """);
    }

    @Test
    public void testJoinWithLong128ColumnOnPrimaryAndSecondary() throws Exception {
        execute(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x, x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")"
        );
        engine.clear();

        assertQuery("select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts1 = tab2.ts1")
                .ddl("create table tab2 as " +
                        "(select" +
                        " to_long128(x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")")
                .noRandomAccess()
                .returns("""
                        ts\tts1\tts11\ti
                        00000000-0000-0002-0000-000000000001\t00000000-0000-0001-0000-000000000001\t2022-02-24T00:00:00.000000Z\t1
                        00000000-0000-0004-0000-000000000002\t00000000-0000-0002-0000-000000000002\t2022-02-24T00:00:01.000000Z\t2
                        00000000-0000-0006-0000-000000000003\t00000000-0000-0003-0000-000000000003\t2022-02-24T00:00:02.000000Z\t3
                        00000000-0000-0008-0000-000000000004\t00000000-0000-0004-0000-000000000004\t2022-02-24T00:00:03.000000Z\t4
                        00000000-0000-000a-0000-000000000005\t00000000-0000-0005-0000-000000000005\t2022-02-24T00:00:04.000000Z\t5
                        00000000-0000-000c-0000-000000000006\t00000000-0000-0006-0000-000000000006\t2022-02-24T00:00:05.000000Z\t6
                        00000000-0000-000e-0000-000000000007\t00000000-0000-0007-0000-000000000007\t2022-02-24T00:00:06.000000Z\t7
                        00000000-0000-0010-0000-000000000008\t00000000-0000-0008-0000-000000000008\t2022-02-24T00:00:07.000000Z\t8
                        00000000-0000-0012-0000-000000000009\t00000000-0000-0009-0000-000000000009\t2022-02-24T00:00:08.000000Z\t9
                        00000000-0000-0014-0000-00000000000a\t00000000-0000-000a-0000-00000000000a\t2022-02-24T00:00:09.000000Z\t10
                        """);
    }

    @Test
    public void testLatestOn() throws Exception {
        execute("create table x (ts timestamp, l long128, i int) timestamp(ts) partition by DAY");
        execute("insert into x values ('2020-01-01T00:00:00.000000Z', to_long128(0, 0), 0)");
        execute("insert into x values ('2020-01-02T00:01:00.000000Z', to_long128(1, 1), 2)");
        execute("insert into x values ('2020-01-02T00:01:00.000000Z', to_long128(2, 2), 0)");

        assertSql(
                """
                        ts\tl\ti
                        2020-01-01T00:00:00.000000Z\t00000000-0000-0000-0000-000000000000\t0
                        2020-01-02T00:01:00.000000Z\t00000000-0000-0001-0000-000000000001\t2
                        2020-01-02T00:01:00.000000Z\t00000000-0000-0002-0000-000000000002\t0
                        """,
                "select ts, l, i from x latest on ts partition by l"
        );
    }

    @Test
    public void testLong128ValueNotSet() throws Exception {
        assertQuery("select ts, count() from tab1 order by ts")
                .ddl("create table tab1 as " +
                        "(select" +
                        " case when x % 2 = 0 then to_long128(x, 2 * x) else NULL end ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")")
                .mutateWith("insert into tab1(i) select 21 as i from long_sequence(1)")
                .expectSize()
                .returns("""
                        ts\tcount
                        \t10
                        00000000-0000-0004-0000-000000000002\t1
                        00000000-0000-0008-0000-000000000004\t1
                        00000000-0000-000c-0000-000000000006\t1
                        00000000-0000-0010-0000-000000000008\t1
                        00000000-0000-0014-0000-00000000000a\t1
                        00000000-0000-0018-0000-00000000000c\t1
                        00000000-0000-001c-0000-00000000000e\t1
                        00000000-0000-0020-0000-000000000010\t1
                        00000000-0000-0024-0000-000000000012\t1
                        00000000-0000-0028-0000-000000000014\t1
                        """, """
                        ts\tcount
                        \t11
                        00000000-0000-0004-0000-000000000002\t1
                        00000000-0000-0008-0000-000000000004\t1
                        00000000-0000-000c-0000-000000000006\t1
                        00000000-0000-0010-0000-000000000008\t1
                        00000000-0000-0014-0000-00000000000a\t1
                        00000000-0000-0018-0000-00000000000c\t1
                        00000000-0000-001c-0000-00000000000e\t1
                        00000000-0000-0020-0000-000000000010\t1
                        00000000-0000-0024-0000-000000000012\t1
                        00000000-0000-0028-0000-000000000014\t1
                        """);
    }

    @Test
    public void testOrderByLong128Column() throws Exception {
        assertQuery("select * from tab1 order by ts desc")
                .ddl("create table tab1 as " +
                        "(select" +
                        " to_long128(-x, x / 2) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")")
                .expectSize()
                .returns("""
                        ts\tts1\ti
                        00000000-0000-0005-ffff-fffffffffff6\t2022-02-24T00:00:09.000000Z\t10
                        00000000-0000-0004-ffff-fffffffffff8\t2022-02-24T00:00:07.000000Z\t8
                        00000000-0000-0004-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t9
                        00000000-0000-0003-ffff-fffffffffffa\t2022-02-24T00:00:05.000000Z\t6
                        00000000-0000-0003-ffff-fffffffffff9\t2022-02-24T00:00:06.000000Z\t7
                        00000000-0000-0002-ffff-fffffffffffc\t2022-02-24T00:00:03.000000Z\t4
                        00000000-0000-0002-ffff-fffffffffffb\t2022-02-24T00:00:04.000000Z\t5
                        00000000-0000-0001-ffff-fffffffffffe\t2022-02-24T00:00:01.000000Z\t2
                        00000000-0000-0001-ffff-fffffffffffd\t2022-02-24T00:00:02.000000Z\t3
                        00000000-0000-0000-ffff-ffffffffffff\t2022-02-24T00:00:00.000000Z\t1
                        """);
    }

    @Test
    public void testOrderByLong128ColumnAndOtherFields() throws Exception {
        assertQuery("select * from tab1 order by i, ts desc, ts1 asc")
                .ddl("create table tab1 as " +
                        "(select" +
                        " to_long128(-x, x / 2) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x / 2 as int) i" +
                        " from long_sequence(10)" +
                        ")")
                .expectSize()
                .returns("""
                        ts\tts1\ti
                        00000000-0000-0000-ffff-ffffffffffff\t2022-02-24T00:00:00.000000Z\t0
                        00000000-0000-0001-ffff-fffffffffffe\t2022-02-24T00:00:01.000000Z\t1
                        00000000-0000-0001-ffff-fffffffffffd\t2022-02-24T00:00:02.000000Z\t1
                        00000000-0000-0002-ffff-fffffffffffc\t2022-02-24T00:00:03.000000Z\t2
                        00000000-0000-0002-ffff-fffffffffffb\t2022-02-24T00:00:04.000000Z\t2
                        00000000-0000-0003-ffff-fffffffffffa\t2022-02-24T00:00:05.000000Z\t3
                        00000000-0000-0003-ffff-fffffffffff9\t2022-02-24T00:00:06.000000Z\t3
                        00000000-0000-0004-ffff-fffffffffff8\t2022-02-24T00:00:07.000000Z\t4
                        00000000-0000-0004-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t4
                        00000000-0000-0005-ffff-fffffffffff6\t2022-02-24T00:00:09.000000Z\t5
                        """);
    }

    @Test
    public void testReadLong128Column() throws Exception {
        assertQuery("select" +
                        " to_long128(timestamp_sequence('2022-02-24', 1000000L), x) ts," +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tts1\ti
                        00000000-0000-0001-0005-d8b84367a000\t2022-02-24T00:00:00.000000Z\t1
                        00000000-0000-0002-0005-d8b84376e240\t2022-02-24T00:00:01.000000Z\t2
                        00000000-0000-0003-0005-d8b843862480\t2022-02-24T00:00:02.000000Z\t3
                        00000000-0000-0004-0005-d8b8439566c0\t2022-02-24T00:00:03.000000Z\t4
                        00000000-0000-0005-0005-d8b843a4a900\t2022-02-24T00:00:04.000000Z\t5
                        00000000-0000-0006-0005-d8b843b3eb40\t2022-02-24T00:00:05.000000Z\t6
                        00000000-0000-0007-0005-d8b843c32d80\t2022-02-24T00:00:06.000000Z\t7
                        00000000-0000-0008-0005-d8b843d26fc0\t2022-02-24T00:00:07.000000Z\t8
                        00000000-0000-0009-0005-d8b843e1b200\t2022-02-24T00:00:08.000000Z\t9
                        00000000-0000-000a-0005-d8b843f0f440\t2022-02-24T00:00:09.000000Z\t10
                        """);
    }

    @Test
    public void testUpdateLong128ColumnToNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table testUpdateLong128ColumnToNull as " +
                    "(select" +
                    " to_long128(-x, x / 2) uuid, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                    " cast(x as int) i" +
                    " from long_sequence(10)" +
                    ")");

            execute("update testUpdateLong128ColumnToNull set uuid = null where i < 5");
            assertSql(
                    """
                            uuid\tts1\ti
                            \t2022-02-24T00:00:00.000000Z\t1
                            \t2022-02-24T00:00:01.000000Z\t2
                            \t2022-02-24T00:00:02.000000Z\t3
                            \t2022-02-24T00:00:03.000000Z\t4
                            00000000-0000-0002-ffff-fffffffffffb\t2022-02-24T00:00:04.000000Z\t5
                            00000000-0000-0003-ffff-fffffffffffa\t2022-02-24T00:00:05.000000Z\t6
                            00000000-0000-0003-ffff-fffffffffff9\t2022-02-24T00:00:06.000000Z\t7
                            00000000-0000-0004-ffff-fffffffffff8\t2022-02-24T00:00:07.000000Z\t8
                            00000000-0000-0004-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t9
                            00000000-0000-0005-ffff-fffffffffff6\t2022-02-24T00:00:09.000000Z\t10
                            """, "testUpdateLong128ColumnToNull"
            );

        });
    }

    @Test
    public void testWhereEquals() throws Exception {
        assertQuery("testWhereEquals where uuid = to_long128(-9, 9)")
                .ddl("create table testWhereEquals as (" +
                        " select to_long128(-x, x) uuid," +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")")
                .returns("""
                        uuid\tts1\ti
                        00000000-0000-0009-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t9
                        """);
    }
}
