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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ShortVectorAggregateTest extends AbstractCairoTest {

    @Test
    public void testAvgVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x from long_sequence(100000));");
            execute("create table abc as (select x, x::short y from temp)");

            assertSql(
                    "x\ty\n" +
                            "25.1492699999998\t25.1492699999998\n",
                    "select avg(x) x, avg(y) y from abc"
            );
        });
    }

    @Test
    public void testKeyedIntAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb') s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    "s\tx\ty\n" +
                            "aaa\t30.732091890705146\t30.732091890705146\n" +
                            "bbb\t-67.00160330280377\t-67.00160330280377\n",
                    "select s, avg(x) x, avg(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedIntMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb') s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    "s\tx\ty\n" +
                            "aaa\t32767\t32767\n" +
                            "bbb\t32765\t32765\n",
                    "select s, max(x) x, max(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedIntMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb') s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    "s\tx\ty\n" +
                            "aaa\t-32768\t-32768\n" +
                            "bbb\t-32768\t-32768\n",
                    "select s, min(x) x, min(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedIntSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb') s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    "s\tx\ty\n" +
                            "aaa\t1539770\t1539770\n" +
                            "bbb\t-3343179\t-3343179\n",
                    "select s, sum(x) x, sum(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedMicroHourAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0, 10000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t358.3847222222222\t358.3847222222222\n" +
                            "1\t157.76157407407408\t157.76157407407408\n" +
                            "2\t182.74652777777777\t182.74652777777777\n" +
                            "3\t-394.46550925925925\t-394.46550925925925\n" +
                            "4\t40.370370370370374\t40.370370370370374\n" +
                            "5\t243.6884259259259\t243.6884259259259\n" +
                            "6\t-215.55\t-215.55\n" +
                            "7\t209.96041666666667\t209.96041666666667\n" +
                            "8\t-28.809953703703705\t-28.809953703703705\n" +
                            "9\t-72.99143518518518\t-72.99143518518518\n" +
                            "10\t74.03819444444444\t74.03819444444444\n" +
                            "11\t-147.5388888888889\t-147.5388888888889\n" +
                            "12\t171.9039351851852\t171.9039351851852\n" +
                            "13\t17.845283018867924\t17.845283018867924\n" +
                            "14\t191.85555555555555\t191.85555555555555\n" +
                            "15\t-238.13611111111112\t-238.13611111111112\n" +
                            "16\t-237.17449494949494\t-237.17449494949494\n" +
                            "17\t-124.8510101010101\t-124.8510101010101\n" +
                            "18\t505.80151515151516\t505.80151515151516\n" +
                            "19\t-413.9492424242424\t-413.9492424242424\n" +
                            "20\t-427.335101010101\t-427.335101010101\n" +
                            "21\t677.4603535353535\t677.4603535353535\n" +
                            "22\t15.899747474747475\t15.899747474747475\n" +
                            "23\t34.224242424242426\t34.224242424242426\n",
                    "select hour(ts), avg(x) x, avg(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedMicroHourMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0, 10000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t32675\t32675\n" +
                            "1\t32767\t32767\n" +
                            "2\t32728\t32728\n" +
                            "3\t32722\t32722\n" +
                            "4\t32765\t32765\n" +
                            "5\t32761\t32761\n" +
                            "6\t32750\t32750\n" +
                            "7\t32765\t32765\n" +
                            "8\t32765\t32765\n" +
                            "9\t32750\t32750\n" +
                            "10\t32738\t32738\n" +
                            "11\t32757\t32757\n" +
                            "12\t32739\t32739\n" +
                            "13\t32753\t32753\n" +
                            "14\t32749\t32749\n" +
                            "15\t32746\t32746\n" +
                            "16\t32712\t32712\n" +
                            "17\t32760\t32760\n" +
                            "18\t32764\t32764\n" +
                            "19\t32761\t32761\n" +
                            "20\t32717\t32717\n" +
                            "21\t32751\t32751\n" +
                            "22\t32742\t32742\n" +
                            "23\t32716\t32716\n",
                    "select hour(ts), max(x) x, max(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedMicroHourMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0, 10000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t-32752\t-32752\n" +
                            "1\t-32752\t-32752\n" +
                            "2\t-32766\t-32766\n" +
                            "3\t-32768\t-32768\n" +
                            "4\t-32740\t-32740\n" +
                            "5\t-32768\t-32768\n" +
                            "6\t-32756\t-32756\n" +
                            "7\t-32738\t-32738\n" +
                            "8\t-32763\t-32763\n" +
                            "9\t-32751\t-32751\n" +
                            "10\t-32731\t-32731\n" +
                            "11\t-32763\t-32763\n" +
                            "12\t-32762\t-32762\n" +
                            "13\t-32752\t-32752\n" +
                            "14\t-32766\t-32766\n" +
                            "15\t-32758\t-32758\n" +
                            "16\t-32767\t-32767\n" +
                            "17\t-32765\t-32765\n" +
                            "18\t-32766\t-32766\n" +
                            "19\t-32767\t-32767\n" +
                            "20\t-32722\t-32722\n" +
                            "21\t-32735\t-32735\n" +
                            "22\t-32750\t-32750\n" +
                            "23\t-32767\t-32767\n",
                    "select hour(ts), min(x) x, min(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedMicroHourSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0, 10000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t1548222\t1548222\n" +
                            "1\t681530\t681530\n" +
                            "2\t789465\t789465\n" +
                            "3\t-1704091\t-1704091\n" +
                            "4\t174400\t174400\n" +
                            "5\t1052734\t1052734\n" +
                            "6\t-931176\t-931176\n" +
                            "7\t907029\t907029\n" +
                            "8\t-124459\t-124459\n" +
                            "9\t-315323\t-315323\n" +
                            "10\t319845\t319845\n" +
                            "11\t-637368\t-637368\n" +
                            "12\t742625\t742625\n" +
                            "13\t75664\t75664\n" +
                            "14\t759748\t759748\n" +
                            "15\t-943019\t-943019\n" +
                            "16\t-939211\t-939211\n" +
                            "17\t-494410\t-494410\n" +
                            "18\t2002974\t2002974\n" +
                            "19\t-1639239\t-1639239\n" +
                            "20\t-1692247\t-1692247\n" +
                            "21\t2682743\t2682743\n" +
                            "22\t62963\t62963\n" +
                            "23\t135528\t135528\n",
                    "select hour(ts), sum(x) x, sum(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0::timestamp_ns, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t358.3847222222222\t358.3847222222222\n" +
                            "1\t157.76157407407408\t157.76157407407408\n" +
                            "2\t182.74652777777777\t182.74652777777777\n" +
                            "3\t-394.46550925925925\t-394.46550925925925\n" +
                            "4\t40.370370370370374\t40.370370370370374\n" +
                            "5\t243.6884259259259\t243.6884259259259\n" +
                            "6\t-215.55\t-215.55\n" +
                            "7\t209.96041666666667\t209.96041666666667\n" +
                            "8\t-28.809953703703705\t-28.809953703703705\n" +
                            "9\t-72.99143518518518\t-72.99143518518518\n" +
                            "10\t74.03819444444444\t74.03819444444444\n" +
                            "11\t-147.5388888888889\t-147.5388888888889\n" +
                            "12\t171.9039351851852\t171.9039351851852\n" +
                            "13\t17.845283018867924\t17.845283018867924\n" +
                            "14\t191.85555555555555\t191.85555555555555\n" +
                            "15\t-238.13611111111112\t-238.13611111111112\n" +
                            "16\t-237.17449494949494\t-237.17449494949494\n" +
                            "17\t-124.8510101010101\t-124.8510101010101\n" +
                            "18\t505.80151515151516\t505.80151515151516\n" +
                            "19\t-413.9492424242424\t-413.9492424242424\n" +
                            "20\t-427.335101010101\t-427.335101010101\n" +
                            "21\t677.4603535353535\t677.4603535353535\n" +
                            "22\t15.899747474747475\t15.899747474747475\n" +
                            "23\t34.224242424242426\t34.224242424242426\n",
                    "select hour(ts), avg(x) x, avg(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0::timestamp_ns, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t32675\t32675\n" +
                            "1\t32767\t32767\n" +
                            "2\t32728\t32728\n" +
                            "3\t32722\t32722\n" +
                            "4\t32765\t32765\n" +
                            "5\t32761\t32761\n" +
                            "6\t32750\t32750\n" +
                            "7\t32765\t32765\n" +
                            "8\t32765\t32765\n" +
                            "9\t32750\t32750\n" +
                            "10\t32738\t32738\n" +
                            "11\t32757\t32757\n" +
                            "12\t32739\t32739\n" +
                            "13\t32753\t32753\n" +
                            "14\t32749\t32749\n" +
                            "15\t32746\t32746\n" +
                            "16\t32712\t32712\n" +
                            "17\t32760\t32760\n" +
                            "18\t32764\t32764\n" +
                            "19\t32761\t32761\n" +
                            "20\t32717\t32717\n" +
                            "21\t32751\t32751\n" +
                            "22\t32742\t32742\n" +
                            "23\t32716\t32716\n",
                    "select hour(ts), max(x) x, max(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0::timestamp_ns, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t-32752\t-32752\n" +
                            "1\t-32752\t-32752\n" +
                            "2\t-32766\t-32766\n" +
                            "3\t-32768\t-32768\n" +
                            "4\t-32740\t-32740\n" +
                            "5\t-32768\t-32768\n" +
                            "6\t-32756\t-32756\n" +
                            "7\t-32738\t-32738\n" +
                            "8\t-32763\t-32763\n" +
                            "9\t-32751\t-32751\n" +
                            "10\t-32731\t-32731\n" +
                            "11\t-32763\t-32763\n" +
                            "12\t-32762\t-32762\n" +
                            "13\t-32752\t-32752\n" +
                            "14\t-32766\t-32766\n" +
                            "15\t-32758\t-32758\n" +
                            "16\t-32767\t-32767\n" +
                            "17\t-32765\t-32765\n" +
                            "18\t-32766\t-32766\n" +
                            "19\t-32767\t-32767\n" +
                            "20\t-32722\t-32722\n" +
                            "21\t-32735\t-32735\n" +
                            "22\t-32750\t-32750\n" +
                            "23\t-32767\t-32767\n",
                    "select hour(ts), min(x) x, min(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence(0::timestamp_ns, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    "hour\tx\ty\n" +
                            "0\t1548222\t1548222\n" +
                            "1\t681530\t681530\n" +
                            "2\t789465\t789465\n" +
                            "3\t-1704091\t-1704091\n" +
                            "4\t174400\t174400\n" +
                            "5\t1052734\t1052734\n" +
                            "6\t-931176\t-931176\n" +
                            "7\t907029\t907029\n" +
                            "8\t-124459\t-124459\n" +
                            "9\t-315323\t-315323\n" +
                            "10\t319845\t319845\n" +
                            "11\t-637368\t-637368\n" +
                            "12\t742625\t742625\n" +
                            "13\t75664\t75664\n" +
                            "14\t759748\t759748\n" +
                            "15\t-943019\t-943019\n" +
                            "16\t-939211\t-939211\n" +
                            "17\t-494410\t-494410\n" +
                            "18\t2002974\t2002974\n" +
                            "19\t-1639239\t-1639239\n" +
                            "20\t-1692247\t-1692247\n" +
                            "21\t2682743\t2682743\n" +
                            "22\t62963\t62963\n" +
                            "23\t135528\t135528\n",
                    "select hour(ts), sum(x) x, sum(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testMaxIsNanWhenNoData() throws Exception {
        // empty table should produce null as sum
        assertMemoryLeak(() -> {
            execute("create table abc (x short)");

            assertSql(
                    "x\n" +
                            "null\n",
                    "select max(x) x from abc"
            );
        });
    }

    @Test
    public void testMaxVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x from long_sequence(100000));");
            execute("create table abc as (select x, x::short y from temp)");

            assertSql(
                    "x\ty\n" +
                            "32767\t32767\n",
                    "select max(x) x, max(y) y from abc"
            );
        });
    }

    @Test
    public void testMinIsNanWhenNoData() throws Exception {
        // empty table should produce null as sum
        assertMemoryLeak(() -> {
            execute("create table abc (x short)");

            assertSql(
                    "x\n" +
                            "null\n",
                    "select min(x) x from abc"
            );
        });
    }

    @Test
    public void testMinVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x from long_sequence(100000));");
            execute("create table abc as (select x, x::short y from temp)");

            assertSql(
                    "x\ty\n" +
                            "-32768\t-32768\n",
                    "select min(x) x, min(y) y from abc"
            );
        });
    }

    @Test
    public void testSumIsNanWhenNoData() throws Exception {
        // empty table should produce null as sum
        assertMemoryLeak(() -> {
            execute("create table abc (x short)");

            assertSql(
                    "x\n" +
                            "null\n",
                    "select sum(x) x from abc"
            );
        });
    }

    @Test
    public void testSumVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x from long_sequence(100000));");
            execute("create table abc as (select x, x::short y from temp)");

            assertSql(
                    "x\ty\n" +
                            "2514927\t2514927\n",
                    "select sum(x) x, sum(y) y from abc"
            );
        });
    }
}
