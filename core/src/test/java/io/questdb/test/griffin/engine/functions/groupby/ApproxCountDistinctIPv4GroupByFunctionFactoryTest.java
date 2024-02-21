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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ApproxCountDistinctIPv4GroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testConstant() throws Exception {
        assertQuery(
                "a\tapprox_count_distinct\n" +
                        "a\t1\n" +
                        "b\t1\n" +
                        "c\t1\n",
                "select a, approx_count_distinct('127.0.0.1'::ipv4) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testDifferentPrecisionsDenseHLL() throws Exception {
        compile("create table x as (select * from (select rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100000)) timestamp(ts))");

        assertQuery(
                "count_distinct\n" +
                        "99685\n",
                "select count_distinct(s) from x",
                null,
                false,
                true
        );

        for (int precision = 4; precision <= 18; precision++) {
            assertQuery(
                    "approx_count_distinct" + precision + "\n" +
                            "99152\n",
                    "select approx_count_distinct(s, " + precision + ") as approx_count_distinct" + precision + " from x",
                    null,
                    false,
                    true
            );
        }
    }

    @Test
    public void testDifferentPrecisionsSparseHLL() throws Exception {
        compile("create table x as (select * from (select rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))");

        assertQuery(
                "count_distinct\n" +
                        "100\n",
                "select count_distinct(s) from x",
                null,
                false,
                true
        );

        for (int precision = 4; precision <= 18; precision++) {
            assertQuery(
                    "approx_count_distinct" + precision + "\n" +
                            "100\n",
                    "select approx_count_distinct(s, " + precision + ") as approx_count_distinct" + precision + " from x",
                    null,
                    false,
                    true
            );
        }
    }

    @Test
    public void testExpression() throws Exception {
        final String expected = "a\tapprox_count_distinct\n" +
                "a\t6\n" +
                "b\t6\n" +
                "c\t8\n";
        assertQuery(
                expected,
                "select a, approx_count_distinct(s + 42) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a, rnd_ipv4('1.1.1.1/16', 0) s from long_sequence(20)))",
                null,
                true,
                true
        );
        // addition shouldn't affect the number of distinct values,
        // so the result should stay the same
        assertSql(expected, "select a, approx_count_distinct(s) from x order by a");
    }

    @Test
    public void testGroupKeyedDenseHLL() throws Exception {
        compile("create table x as (" +
                "select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(1000000)" +
                ") timestamp(ts))");
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t165309\n" +
                        "b\t166198\n" +
                        "c\t166121\n" +
                        "d\t165973\n" +
                        "e\t165557\n" +
                        "f\t165845\n",
                "select a, count_distinct(s) from x order by a",
                null,
                true,
                true
        );
        assertQuery(
                "a\tapprox_count_distinct\n" +
                        "a\t165044\n" +
                        "b\t164963\n" +
                        "c\t164909\n" +
                        "d\t166100\n" +
                        "e\t165568\n" +
                        "f\t166248\n",
                "select a, approx_count_distinct(s) from x order by a",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupKeyedSparseHLL() throws Exception {
        compile("create table x as (" +
                "select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_ipv4('1.1.1.1/16', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(20)" +
                ") timestamp(ts))");
        assertQuery(
                "a\tcount_distinct\n" +
                        "a\t2\n" +
                        "b\t1\n" +
                        "c\t2\n" +
                        "d\t4\n" +
                        "e\t5\n" +
                        "f\t6\n",
                "select a, count_distinct(s) from x order by a",
                null,
                true,
                true
        );
        assertQuery(
                "a\tapprox_count_distinct\n" +
                        "a\t2\n" +
                        "b\t1\n" +
                        "c\t2\n" +
                        "d\t4\n" +
                        "e\t5\n" +
                        "f\t6\n",
                "select a, approx_count_distinct(s) from x order by a",
                null,
                true,
                true
        );
    }

    @Test
    public void testGroupNotKeyedDenseHLL() throws Exception {
        compile("create table x as (select * from (select rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100000)) timestamp(ts))");
        assertQuery(
                "count_distinct\n" +
                        "99685\n",
                "select count_distinct(s) from x",
                null,
                false,
                true
        );
        assertQuery(
                "approx_count_distinct\n" +
                        "99152\n",
                "select approx_count_distinct(s) from x",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupNotKeyedSparseHLL() throws Exception {
        compile("create table x as (select * from (select rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))");
        assertQuery(
                "count_distinct\n" +
                        "100\n",
                "select count_distinct(s) from x",
                null,
                false,
                true
        );
        assertQuery(
                "approx_count_distinct\n" +
                        "100\n",
                "select approx_count_distinct(s) from x",
                null,
                false,
                true
        );
    }

    @Test
    public void testGroupNotKeyedWithNullsDenseHLL() throws Exception {
        compile("create table x as (" +
                "select * from (select rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(10, 100000) ts from long_sequence(1000000)) timestamp(ts)" +
                ") timestamp(ts) PARTITION BY YEAR");

        String expectedExact = "count_distinct\n" +
                "970716\n";
        String expectedEstimated = "approx_count_distinct\n" +
                "975818\n";

        assertQuery(
                expectedExact,
                "select count_distinct(s) from x",
                null,
                false,
                true
        );
        assertQuery(
                expectedEstimated,
                "select approx_count_distinct(s) from x",
                null,
                false,
                true
        );

        insert("insert into x values(cast(null as IPV4), '2021-05-21')");
        insert("insert into x values(cast(null as IPV4), '1970-01-01')");
        assertSql(expectedExact, "select count_distinct(s) from x");
        assertSql(expectedEstimated, "select approx_count_distinct(s) from x");
    }

    @Test
    public void testGroupNotKeyedWithNullsSparseHLL() throws Exception {
        compile("create table x as (" +
                "select * from (select rnd_ipv4('1.1.1.1/8', 0) s, timestamp_sequence(10, 100000) ts from long_sequence(100)) timestamp(ts)" +
                ") timestamp(ts) PARTITION BY YEAR");

        String expectedExact = "count_distinct\n" +
                "100\n";
        String expectedEstimated = "approx_count_distinct\n" +
                "100\n";

        assertQuery(
                expectedExact,
                "select count_distinct(s) from x",
                null,
                false,
                true
        );
        assertQuery(
                expectedEstimated,
                "select approx_count_distinct(s) from x",
                null,
                false,
                true
        );

        insert("insert into x values(cast(null as IPV4), '2021-05-21')");
        insert("insert into x values(cast(null as IPV4), '1970-01-01')");
        assertSql(expectedExact, "select count_distinct(s) from x");
        assertSql(expectedEstimated, "select approx_count_distinct(s) from x");
    }

    @Test
    public void testInterpolation() throws Exception {
        assertQuery(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t1\n" +
                        "1970-01-01T00:00:01.000000Z\t1\n",
                "select ts, approx_count_distinct(s) from x sample by 1s fill(linear) limit 2",
                "create table x as (select * from (select rnd_ipv4('1.1.1.1/28', 0) s, timestamp_sequence(0, 60000000) ts from long_sequence(100)) timestamp(ts))",
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
                "create table x (a ipv4)",
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
                "select a, approx_count_distinct(cast(null as IPV4)) from x order by a",
                "create table x as (select * from (select rnd_symbol('a','b','c') a from long_sequence(20)))",
                null,
                true,
                true
        );
    }

    @Test
    public void testPrecisionOutOfRange() throws Exception {
        assertException("select approx_count_distinct('127.0.0.1'::ipv4, 3) from long_sequence(1)", 7, "precision must be between 4 and 18");
        assertException("select approx_count_distinct('127.0.0.1'::ipv4, 19) from long_sequence(1)", 7, "precision must be between 4 and 18");
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t9\n" +
                        "1970-01-01T00:00:01.000000Z\t9\n" +
                        "1970-01-01T00:00:02.000000Z\t7\n" +
                        "1970-01-01T00:00:03.000000Z\t7\n" +
                        "1970-01-01T00:00:04.000000Z\t8\n" +
                        "1970-01-01T00:00:05.000000Z\t6\n" +
                        "1970-01-01T00:00:06.000000Z\t9\n" +
                        "1970-01-01T00:00:07.000000Z\t9\n" +
                        "1970-01-01T00:00:08.000000Z\t7\n" +
                        "1970-01-01T00:00:09.000000Z\t8\n",
                "select ts, approx_count_distinct(s) from x sample by 1s fill(linear)",
                "create table x as (select * from (select rnd_ipv4('1.1.1.1/28', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.050000Z\t16\n" +
                        "1970-01-01T00:00:02.050000Z\t16\n", "with x as (select * from (select rnd_ipv4('1.1.1.1/28', 0) s, timestamp_sequence(50000, 100000L/4) ts from long_sequence(150)) timestamp(ts))\n" +
                        "select ts, approx_count_distinct(s) from x sample by 2s"
        ));
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery(
                "ts\tapprox_count_distinct\n" +
                        "1970-01-01T00:00:00.000000Z\t9\n" +
                        "1970-01-01T00:00:01.000000Z\t9\n" +
                        "1970-01-01T00:00:02.000000Z\t7\n" +
                        "1970-01-01T00:00:03.000000Z\t7\n" +
                        "1970-01-01T00:00:04.000000Z\t8\n" +
                        "1970-01-01T00:00:05.000000Z\t6\n" +
                        "1970-01-01T00:00:06.000000Z\t9\n" +
                        "1970-01-01T00:00:07.000000Z\t9\n" +
                        "1970-01-01T00:00:08.000000Z\t7\n" +
                        "1970-01-01T00:00:09.000000Z\t8\n",
                "select ts, approx_count_distinct(s) from x sample by 1s fill(99)",
                "create table x as (select * from (select rnd_ipv4('1.1.1.1/28', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }

    @Test
    public void testSampleKeyed() throws Exception {
        assertQuery(
                "a\tapprox_count_distinct\tts\n" +
                        "a\t4\t1970-01-01T00:00:00.000000Z\n" +
                        "f\t6\t1970-01-01T00:00:00.000000Z\n" +
                        "c\t9\t1970-01-01T00:00:00.000000Z\n" +
                        "e\t6\t1970-01-01T00:00:00.000000Z\n" +
                        "d\t7\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t6\t1970-01-01T00:00:00.000000Z\n" +
                        "b\t7\t1970-01-01T00:00:05.000000Z\n" +
                        "c\t5\t1970-01-01T00:00:05.000000Z\n" +
                        "f\t7\t1970-01-01T00:00:05.000000Z\n" +
                        "e\t8\t1970-01-01T00:00:05.000000Z\n" +
                        "d\t7\t1970-01-01T00:00:05.000000Z\n" +
                        "a\t5\t1970-01-01T00:00:05.000000Z\n",
                "select a, approx_count_distinct(s), ts from x sample by 5s",
                "create table x as (select * from (select rnd_symbol('a','b','c','d','e','f') a, rnd_ipv4('1.1.1.1/28', 0) s, timestamp_sequence(0, 100000) ts from long_sequence(100)) timestamp(ts))",
                "ts",
                false
        );
    }
}
