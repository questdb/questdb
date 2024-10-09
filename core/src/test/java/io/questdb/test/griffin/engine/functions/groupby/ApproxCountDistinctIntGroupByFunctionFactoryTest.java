/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ApproxCountDistinctIntGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery(
                "a\tapprox_count_distinct\n" +
                        "a\t1\n" +
                        "b\t1\n" +
                        "c\t1\n",
                "select a, approx_count_distinct(42) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDifferentPrecisionsDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (select * from (select rnd_int(1, 1000000, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)) timestamp(ts))");

            String expected = "count_distinct\n" +
                    "631884\n";
            assertQueryNoLeakCheck(
                    expected,
                    "select count_distinct(s) from x",
                    null,
                    false,
                    true
            );

            assertSql(expected, "select count(  distinct s) from x");

            long[] expectedEstimates = new long[]{
                    501129L,
                    544983L,
                    641215L,
                    643431L,
                    649396L,
                    664450L,
                    642849L,
                    628365L,
                    620599L,
                    624983L,
                    630138L,
                    631748L,
                    630012L,
                    631544L,
                    631788L
            };
            for (int precision = 4; precision <= 18; precision++) {
                assertQueryNoLeakCheck(
                        "approx_count_distinct" + precision + "\n" +
                                expectedEstimates[precision - 4] + "\n",
                        "select approx_count_distinct(s, " + precision + ") as approx_count_distinct" + precision + " from x",
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testDifferentPrecisionsSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (select * from (select rnd_int(1, 6, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))");

            assertQueryNoLeakCheck(
                    "count_distinct\n" +
                            "6\n",
                    "select count_distinct(s) from x",
                    null,
                    false,
                    true
            );

            long[] expectedEstimates = new long[]{
                    8L, 7L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L, 6L
            };
            for (int precision = 4; precision <= 18; precision++) {
                assertQueryNoLeakCheck(
                        "approx_count_distinct" + precision + "\n" +
                                expectedEstimates[precision - 4] + "\n",
                        "select approx_count_distinct(s, " + precision + ") as approx_count_distinct" + precision + " from x",
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testExpression() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tapprox_count_distinct\n" +
                    "a\t5\n" +
                    "b\t4\n" +
                    "c\t4\n";
            assertQueryNoLeakCheck(
                    expected,
                    "select a, approx_count_distinct(s * 42) from x order by a",
                    "create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_int(1, 8, 0) s from long_sequence(20)))",
                    null,
                    true,
                    true
            );
            // multiplication shouldn't affect the number of distinct values,
            // so the result should stay the same
            assertSql(expected, "select a, approx_count_distinct(s) from x order by a");
        });
    }

    @Test
    public void testGroupKeyedDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (" +
                    "select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_int(0, 100000, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)" +
                    ") timestamp(ts))");
            assertQueryNoLeakCheck(
                    "a\tcount_distinct\n" +
                            "a\t80974\n" +
                            "b\t81221\n" +
                            "c\t81204\n" +
                            "d\t81135\n" +
                            "e\t81116\n" +
                            "f\t81314\n",
                    "select a, count_distinct(s) from x order by a",
                    null,
                    true,
                    true
            );
            assertQueryNoLeakCheck(
                    "a\tapprox_count_distinct\n" +
                            "a\t80475\n" +
                            "b\t81142\n" +
                            "c\t80831\n" +
                            "d\t80586\n" +
                            "e\t80368\n" +
                            "f\t81112\n",
                    "select a, approx_count_distinct(s) from x order by a",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupKeyedSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (" +
                    "select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_int(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(20)" +
                    ") timestamp(ts))");
            assertQueryNoLeakCheck(
                    "a\tcount_distinct\n" +
                            "a\t2\n" +
                            "b\t1\n" +
                            "c\t2\n" +
                            "d\t4\n" +
                            "e\t4\n" +
                            "f\t4\n",
                    "select a, count_distinct(s) from x order by a",
                    null,
                    true,
                    true
            );
            assertQueryNoLeakCheck(
                    "a\tapprox_count_distinct\n" +
                            "a\t2\n" +
                            "b\t1\n" +
                            "c\t2\n" +
                            "d\t4\n" +
                            "e\t4\n" +
                            "f\t4\n",
                    "select a, approx_count_distinct(s) from x order by a",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testGroupNotKeyedDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (select * from (select rnd_int(1, 1000000, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)) timestamp(ts))");
            assertQueryNoLeakCheck(
                    "count_distinct\n" +
                            "631884\n",
                    "select count_distinct(s) from x",
                    null,
                    false,
                    true
            );
            assertQueryNoLeakCheck(
                    "approx_count_distinct\n" +
                            "630138\n",
                    "select approx_count_distinct(s) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupNotKeyedSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (select * from (select rnd_int(1, 6, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))");
            assertQueryNoLeakCheck(
                    "count_distinct\n" +
                            "6\n",
                    "select count_distinct(s) from x",
                    null,
                    false,
                    true
            );
            assertQueryNoLeakCheck(
                    "approx_count_distinct\n" +
                            "6\n",
                    "select approx_count_distinct(s) from x",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testGroupNotKeyedWithNullsDenseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (" +
                    "select * from (select rnd_int(1, 1000000, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(1000000)) timestamp(ts)" +
                    ") timestamp(ts) PARTITION BY YEAR");
            String expectedExact = "count_distinct\n" +
                    "631884\n";
            String expectedEstimated = "approx_count_distinct\n" +
                    "630138\n";

            assertQueryNoLeakCheck(expectedExact, "select count_distinct(s) from x", null, false, true);
            assertQueryNoLeakCheck(expectedEstimated, "select approx_count_distinct(s) from x", null, false, true);

            insert("insert into x values(cast(null as INT), '2021-05-21')");
            insert("insert into x values(cast(null as INT), '1970-01-01')");
            assertSql(expectedExact, "select count_distinct(s) from x");
            assertSql(expectedEstimated, "select approx_count_distinct(s) from x");
        });
    }

    @Test
    public void testGroupNotKeyedWithNullsSparseHLL() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x as (" +
                    "select * from (select rnd_int(1, 6, 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)" +
                    ") timestamp(ts) PARTITION BY YEAR");
            String expectedExact = "count_distinct\n" +
                    "6\n";
            String expectedEstimated = "approx_count_distinct\n" +
                    "6\n";

            assertQueryNoLeakCheck(expectedExact, "select count_distinct(s) from x", null, false, true);
            assertQueryNoLeakCheck(expectedEstimated, "select approx_count_distinct(s) from x", null, false, true);

            insert("insert into x values(cast(null as INT), '2021-05-21')");
            insert("insert into x values(cast(null as INT), '1970-01-01')");
            assertSql(expectedExact, "select count_distinct(s) from x");
            assertSql(expectedEstimated, "select approx_count_distinct(s) from x");
        });
    }

    @Test
    public void testInterpolation() throws Exception {
        assertQuery(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t1\n" +
                        "1970-01-01T00:00:01.000000Z\t1\n",
                "select ts, approx_count_distinct(s) from x sample by 1s fill(linear) limit 2",
                "create table x as (select * from (select rnd_int(0, 16, 0) s, timestamp_sequence(0, 60000000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                false
        );
    }

    @Test
    public void testNoValues() throws Exception {
        assertQuery(
                "approx_count_distinct\n" +
                        "0\n",
                "select approx_count_distinct(a) from x",
                "create table x (a int)",
                null,
                false,
                true
        );
    }

    @Test
    public void testNullConstant() throws Exception {
        assertQuery(
                "a\tapprox_count_distinct\n" +
                        "a\t0\n" +
                        "b\t0\n" +
                        "c\t0\n",
                "select a, approx_count_distinct(cast(null as INT)) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testPrecisionOutOfRange() throws Exception {
        assertException("select approx_count_distinct(x::int, 3) from long_sequence(1)", 7, "precision must be between 4 and 18");
        assertException("select approx_count_distinct(x::int, 19) from long_sequence(1)", 7, "precision must be between 4 and 18");
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t8\n" +
                        "1970-01-01T00:00:01.000000Z\t9\n" +
                        "1970-01-01T00:00:02.000000Z\t9\n" +
                        "1970-01-01T00:00:03.000000Z\t9\n" +
                        "1970-01-01T00:00:04.000000Z\t8\n" +
                        "1970-01-01T00:00:05.000000Z\t9\n" +
                        "1970-01-01T00:00:06.000000Z\t10\n" +
                        "1970-01-01T00:00:07.000000Z\t7\n" +
                        "1970-01-01T00:00:08.000000Z\t8\n" +
                        "1970-01-01T00:00:09.000000Z\t7\n",
                "select ts, approx_count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_int(0, 16, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.050000Z\t8\n" +
                        "1970-01-01T00:00:02.050000Z\t8\n",
                "with x as (select * from (select rnd_int(1, 8, 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(150)) timestamp(ts))\n" +
                        "select ts, approx_count_distinct(s) from x sample by 2s align to first observation"
        ));
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t8\n" +
                        "1970-01-01T00:00:01.000000Z\t6\n" +
                        "1970-01-01T00:00:02.000000Z\t7\n" +
                        "1970-01-01T00:00:03.000000Z\t8\n" +
                        "1970-01-01T00:00:04.000000Z\t6\n" +
                        "1970-01-01T00:00:05.000000Z\t7\n" +
                        "1970-01-01T00:00:06.000000Z\t7\n" +
                        "1970-01-01T00:00:07.000000Z\t6\n" +
                        "1970-01-01T00:00:08.000000Z\t7\n" +
                        "1970-01-01T00:00:09.000000Z\t5\n",
                "select ts, approx_count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_int(0, 8, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true
        );
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery(
                "a\tapprox_count_distinct\tts\n" +
                        "a\t5\t1970-01-01T00:00:00.000000Z\n" +
                        "f\t8\t1970-01-01T00:00:00.000000Z\n" +
                        "c\t9\t1970-01-01T00:00:00.000000Z\n" +
                        "e\t6\t1970-01-01T00:00:00.000000Z\n" +
                        "d\t7\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t5\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t6\t1970-01-01T00:00:05.000000Z\n" +
                        "c\t5\t1970-01-01T00:00:05.000000Z\n" +
                        "f\t6\t1970-01-01T00:00:05.000000Z\n" +
                        "e\t8\t1970-01-01T00:00:05.000000Z\n" +
                        "d\t7\t1970-01-01T00:00:05.000000Z\n" +
                        "a\t4\t1970-01-01T00:00:05.000000Z\n",
                "select a, approx_count_distinct(s), ts from x sample by 5s align to first observation",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_int(0, 12, 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }
}
