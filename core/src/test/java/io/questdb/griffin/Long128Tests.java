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

import org.junit.Test;

public class Long128Tests extends AbstractGriffinTest {
    @Test
    public void testGroupByLong128Column() throws Exception {
        assertQuery("ts\tcount\n" +
                        "00000000-0000-0000-0000-000000000000\t1\n" +
                        "00000000-0000-0001-0000-000000000001\t2\n" +
                        "00000000-0000-0002-0000-000000000002\t2\n" +
                        "00000000-0000-0003-0000-000000000003\t2\n" +
                        "00000000-0000-0004-0000-000000000004\t2\n" +
                        "00000000-0000-0005-0000-000000000005\t2\n" +
                        "00000000-0000-0006-0000-000000000006\t2\n" +
                        "00000000-0000-0007-0000-000000000007\t2\n" +
                        "00000000-0000-0008-0000-000000000008\t2\n" +
                        "00000000-0000-0009-0000-000000000009\t2\n" +
                        "00000000-0000-000a-0000-00000000000a\t1\n",
                "select ts, count() from tab1",
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x / 2, x / 2) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByLong128ColumnWithNulls() throws Exception {
        assertQuery("ts\tcount\n" +
                        "\t10\n" +
                        "00000000-0000-0002-0000-000000000002\t1\n" +
                        "00000000-0000-0004-0000-000000000004\t1\n" +
                        "00000000-0000-0006-0000-000000000006\t1\n" +
                        "00000000-0000-0008-0000-000000000008\t1\n" +
                        "00000000-0000-000a-0000-00000000000a\t1\n" +
                        "00000000-0000-000c-0000-00000000000c\t1\n" +
                        "00000000-0000-000e-0000-00000000000e\t1\n" +
                        "00000000-0000-0010-0000-000000000010\t1\n" +
                        "00000000-0000-0012-0000-000000000012\t1\n" +
                        "00000000-0000-0014-0000-000000000014\t1\n",
                "select ts, count() from tab1",
                "create table tab1 as " +
                        "(select" +
                        " case when x % 2 = 0 then to_long128(x, x) else NULL end ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testLong128ValueNotSet() throws Exception {
        assertQuery("ts\tcount\n" +
                        "\t10\n" +
                        "00000000-0000-0002-0000-000000000002\t1\n" +
                        "00000000-0000-0004-0000-000000000004\t1\n" +
                        "00000000-0000-0006-0000-000000000006\t1\n" +
                        "00000000-0000-0008-0000-000000000008\t1\n" +
                        "00000000-0000-000a-0000-00000000000a\t1\n" +
                        "00000000-0000-000c-0000-00000000000c\t1\n" +
                        "00000000-0000-000e-0000-00000000000e\t1\n" +
                        "00000000-0000-0010-0000-000000000010\t1\n" +
                        "00000000-0000-0012-0000-000000000012\t1\n" +
                        "00000000-0000-0014-0000-000000000014\t1\n",
                "select ts, count() from tab1",
                "create table tab1 as " +
                        "(select" +
                        " case when x % 2 = 0 then to_long128(x, x) else NULL end ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                "insert into tab1(i) select 21 as i from long_sequence(1)",
                "ts\tcount\n" +
                        "\t11\n" +
                        "00000000-0000-0002-0000-000000000002\t1\n" +
                        "00000000-0000-0004-0000-000000000004\t1\n" +
                        "00000000-0000-0006-0000-000000000006\t1\n" +
                        "00000000-0000-0008-0000-000000000008\t1\n" +
                        "00000000-0000-000a-0000-00000000000a\t1\n" +
                        "00000000-0000-000c-0000-00000000000c\t1\n" +
                        "00000000-0000-000e-0000-00000000000e\t1\n" +
                        "00000000-0000-0010-0000-000000000010\t1\n" +
                        "00000000-0000-0012-0000-000000000012\t1\n" +
                        "00000000-0000-0014-0000-000000000014\t1\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testJoinOnLong128Column() throws Exception {
        compile(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(3 * x, 3 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")"
        );
        engine.clear();

        assertQuery("ts\tts1\tts11\ti\n" +
                        "00000000-0000-0006-0000-000000000006\t00000000-0000-0006-0000-000000000006\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "00000000-0000-000c-0000-00000000000c\t00000000-0000-000c-0000-00000000000c\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "00000000-0000-0012-0000-000000000012\t00000000-0000-0012-0000-000000000012\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "00000000-0000-0018-0000-000000000018\t00000000-0000-0018-0000-000000000018\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "00000000-0000-001e-0000-00000000001e\t00000000-0000-001e-0000-00000000001e\t2022-02-24T00:00:09.000000Z\t10\n" +
                        "00000000-0000-0024-0000-000000000024\t00000000-0000-0024-0000-000000000024\t2022-02-24T00:00:11.000000Z\t12\n",
                "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts = tab2.ts",
                "create table tab2 as " +
                        "(select" +
                        " to_long128(2 * x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testFatJoinOnLong128Column() throws Exception {
        compile(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(3 * x, 3 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")"
        );
        engine.clear();

        try {
            compiler.setFullFatJoins(true);
            assertQuery("ts\tts1\tts11\ti\n" +
                            "00000000-0000-0006-0000-000000000006\t00000000-0000-0006-0000-000000000006\t2022-02-24T00:00:01.000000Z\t2\n" +
                            "00000000-0000-000c-0000-00000000000c\t00000000-0000-000c-0000-00000000000c\t2022-02-24T00:00:03.000000Z\t4\n" +
                            "00000000-0000-0012-0000-000000000012\t00000000-0000-0012-0000-000000000012\t2022-02-24T00:00:05.000000Z\t6\n" +
                            "00000000-0000-0018-0000-000000000018\t00000000-0000-0018-0000-000000000018\t2022-02-24T00:00:07.000000Z\t8\n" +
                            "00000000-0000-001e-0000-00000000001e\t00000000-0000-001e-0000-00000000001e\t2022-02-24T00:00:09.000000Z\t10\n" +
                            "00000000-0000-0024-0000-000000000024\t00000000-0000-0024-0000-000000000024\t2022-02-24T00:00:11.000000Z\t12\n",
                    "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts = tab2.ts",
                    "create table tab2 as " +
                            "(select" +
                            " to_long128(2 * x, 2 * x) ts, " +
                            " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                            " cast(x as int) i" +
                            " from long_sequence(20)" +
                            ")",
                    null,
                    false
            );
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testJoinOnLong128ColumnCompact() throws Exception {
        compile(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(3 * x, 3 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")"
        );
        engine.clear();
        defaultMapType = "compact";

        assertQuery("ts\tts1\tts11\ti\n" +
                        "00000000-0000-0006-0000-000000000006\t00000000-0000-0006-0000-000000000006\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "00000000-0000-000c-0000-00000000000c\t00000000-0000-000c-0000-00000000000c\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "00000000-0000-0012-0000-000000000012\t00000000-0000-0012-0000-000000000012\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "00000000-0000-0018-0000-000000000018\t00000000-0000-0018-0000-000000000018\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "00000000-0000-001e-0000-00000000001e\t00000000-0000-001e-0000-00000000001e\t2022-02-24T00:00:09.000000Z\t10\n" +
                        "00000000-0000-0024-0000-000000000024\t00000000-0000-0024-0000-000000000024\t2022-02-24T00:00:11.000000Z\t12\n",
                "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts = tab2.ts",
                "create table tab2 as " +
                        "(select" +
                        " to_long128(2 * x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(20)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testJoinWithLong128ColumnOnPrimaryAndSecondary() throws Exception {
        compile(
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x, x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")"
        );
        engine.clear();

        assertQuery("ts\tts1\tts11\ti\n" +
                        "00000000-0000-0001-0000-000000000002\t00000000-0000-0001-0000-000000000001\t2022-02-24T00:00:00.000000Z\t1\n" +
                        "00000000-0000-0002-0000-000000000004\t00000000-0000-0002-0000-000000000002\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "00000000-0000-0003-0000-000000000006\t00000000-0000-0003-0000-000000000003\t2022-02-24T00:00:02.000000Z\t3\n" +
                        "00000000-0000-0004-0000-000000000008\t00000000-0000-0004-0000-000000000004\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "00000000-0000-0005-0000-00000000000a\t00000000-0000-0005-0000-000000000005\t2022-02-24T00:00:04.000000Z\t5\n" +
                        "00000000-0000-0006-0000-00000000000c\t00000000-0000-0006-0000-000000000006\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "00000000-0000-0007-0000-00000000000e\t00000000-0000-0007-0000-000000000007\t2022-02-24T00:00:06.000000Z\t7\n" +
                        "00000000-0000-0008-0000-000000000010\t00000000-0000-0008-0000-000000000008\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "00000000-0000-0009-0000-000000000012\t00000000-0000-0009-0000-000000000009\t2022-02-24T00:00:08.000000Z\t9\n" +
                        "00000000-0000-000a-0000-000000000014\t00000000-0000-000a-0000-00000000000a\t2022-02-24T00:00:09.000000Z\t10\n",
                "select tab2.ts, tab1.* from tab1 JOIN tab2 ON tab1.ts1 = tab2.ts1",
                "create table tab2 as " +
                        "(select" +
                        " to_long128(x, 2 * x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testOrderByLong128Column() throws Exception {
        assertQuery("ts\tts1\ti\n" +
                        "00000000-0000-0005-ffff-fffffffffff6\t2022-02-24T00:00:09.000000Z\t10\n" +
                        "00000000-0000-0004-ffff-fffffffffff8\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "00000000-0000-0004-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t9\n" +
                        "00000000-0000-0003-ffff-fffffffffffa\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "00000000-0000-0003-ffff-fffffffffff9\t2022-02-24T00:00:06.000000Z\t7\n" +
                        "00000000-0000-0002-ffff-fffffffffffc\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "00000000-0000-0002-ffff-fffffffffffb\t2022-02-24T00:00:04.000000Z\t5\n" +
                        "00000000-0000-0001-ffff-fffffffffffe\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "00000000-0000-0001-ffff-fffffffffffd\t2022-02-24T00:00:02.000000Z\t3\n" +
                        "00000000-0000-0000-ffff-ffffffffffff\t2022-02-24T00:00:00.000000Z\t1\n",
                "select * from tab1 order by ts desc",
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x / 2, -x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByLong128ColumnAndOtherFields() throws Exception {
        assertQuery("ts\tts1\ti\n" +
                        "00000000-0000-0000-ffff-ffffffffffff\t2022-02-24T00:00:00.000000Z\t0\n" +
                        "00000000-0000-0001-ffff-fffffffffffe\t2022-02-24T00:00:01.000000Z\t1\n" +
                        "00000000-0000-0001-ffff-fffffffffffd\t2022-02-24T00:00:02.000000Z\t1\n" +
                        "00000000-0000-0002-ffff-fffffffffffc\t2022-02-24T00:00:03.000000Z\t2\n" +
                        "00000000-0000-0002-ffff-fffffffffffb\t2022-02-24T00:00:04.000000Z\t2\n" +
                        "00000000-0000-0003-ffff-fffffffffffa\t2022-02-24T00:00:05.000000Z\t3\n" +
                        "00000000-0000-0003-ffff-fffffffffff9\t2022-02-24T00:00:06.000000Z\t3\n" +
                        "00000000-0000-0004-ffff-fffffffffff8\t2022-02-24T00:00:07.000000Z\t4\n" +
                        "00000000-0000-0004-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t4\n" +
                        "00000000-0000-0005-ffff-fffffffffff6\t2022-02-24T00:00:09.000000Z\t5\n",
                "select * from tab1 order by i, ts desc, ts1 asc",
                "create table tab1 as " +
                        "(select" +
                        " to_long128(x / 2, -x) ts, " +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x / 2 as int) i" +
                        " from long_sequence(10)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testReadLong128Column() throws SqlException {
        assertQuery(
                "ts\tts1\ti\n" +
                        "00000000-0000-0001-0005-d8b84367a000\t2022-02-24T00:00:00.000000Z\t1\n" +
                        "00000000-0000-0002-0005-d8b84376e240\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "00000000-0000-0003-0005-d8b843862480\t2022-02-24T00:00:02.000000Z\t3\n" +
                        "00000000-0000-0004-0005-d8b8439566c0\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "00000000-0000-0005-0005-d8b843a4a900\t2022-02-24T00:00:04.000000Z\t5\n" +
                        "00000000-0000-0006-0005-d8b843b3eb40\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "00000000-0000-0007-0005-d8b843c32d80\t2022-02-24T00:00:06.000000Z\t7\n" +
                        "00000000-0000-0008-0005-d8b843d26fc0\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "00000000-0000-0009-0005-d8b843e1b200\t2022-02-24T00:00:08.000000Z\t9\n" +
                        "00000000-0000-000a-0005-d8b843f0f440\t2022-02-24T00:00:09.000000Z\t10\n",
                "select" +
                        " to_long128(x, timestamp_sequence('2022-02-24', 1000000L)) ts," +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)",
                null,
                false,
                true
        );
    }

    @Test
    public void testWhereEquals() throws Exception {
        assertQuery(
                "uuid\tts1\ti\n" +
                        "00000000-0000-0009-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t9\n",
                "testWhereEquals where uuid = to_long128(9, -9)",
                "create table testWhereEquals as (" +
                        " select to_long128(x, -x) uuid," +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testUpdateLong128ColumnToNull() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table testUpdateLong128ColumnToNull as " +
                    "(select" +
                    " to_long128(x / 2, -x) uuid, " +
                    " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                    " cast(x as int) i" +
                    " from long_sequence(10)" +
                    ")");

            compile("update testUpdateLong128ColumnToNull set uuid = null where i < 5");
            assertSql(
                    "testUpdateLong128ColumnToNull",
                    "uuid\tts1\ti\n" +
                            "\t2022-02-24T00:00:00.000000Z\t1\n" +
                            "\t2022-02-24T00:00:01.000000Z\t2\n" +
                            "\t2022-02-24T00:00:02.000000Z\t3\n" +
                            "\t2022-02-24T00:00:03.000000Z\t4\n" +
                            "00000000-0000-0002-ffff-fffffffffffb\t2022-02-24T00:00:04.000000Z\t5\n" +
                            "00000000-0000-0003-ffff-fffffffffffa\t2022-02-24T00:00:05.000000Z\t6\n" +
                            "00000000-0000-0003-ffff-fffffffffff9\t2022-02-24T00:00:06.000000Z\t7\n" +
                            "00000000-0000-0004-ffff-fffffffffff8\t2022-02-24T00:00:07.000000Z\t8\n" +
                            "00000000-0000-0004-ffff-fffffffffff7\t2022-02-24T00:00:08.000000Z\t9\n" +
                            "00000000-0000-0005-ffff-fffffffffff6\t2022-02-24T00:00:09.000000Z\t10\n"
            );

        });
    }
}
