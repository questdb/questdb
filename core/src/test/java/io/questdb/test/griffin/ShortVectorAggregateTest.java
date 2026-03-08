/*******************************************************************************
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

public class ShortVectorAggregateTest extends AbstractCairoTest {

    @Test
    public void testKeyedIntAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb', null) s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    """
                            s\tx\ty
                            \t-42.416851774028935\t-42.416851774028935
                            aaa\t69.63866351306847\t69.63866351306847
                            bbb\t-81.60838215412348\t-81.60838215412348
                            """,
                    "select s, avg(x) x, avg(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedIntMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb', null) s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    """
                            s\tx\ty
                            \t32765\t32765
                            aaa\t32767\t32767
                            bbb\t32765\t32765
                            """,
                    "select s, max(x) x, max(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedIntMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb', null) s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    """
                            s\tx\ty
                            \t-32768\t-32768
                            aaa\t-32768\t-32768
                            bbb\t-32767\t-32767
                            """,
                    "select s, min(x) x, min(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedIntSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, rnd_symbol('aaa', 'bbb', null) s from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, s from temp)");

            assertSql(
                    """
                            s\tx\ty
                            \t-1413075\t-1413075
                            aaa\t2326001\t2326001
                            bbb\t-2716335\t-2716335
                            """,
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
                    """
                            hour\tx\ty
                            0\t358.3847222222222\t358.3847222222222
                            1\t157.76157407407408\t157.76157407407408
                            2\t182.74652777777777\t182.74652777777777
                            3\t-394.46550925925925\t-394.46550925925925
                            4\t40.370370370370374\t40.370370370370374
                            5\t243.6884259259259\t243.6884259259259
                            6\t-215.55\t-215.55
                            7\t209.96041666666667\t209.96041666666667
                            8\t-28.809953703703705\t-28.809953703703705
                            9\t-72.99143518518518\t-72.99143518518518
                            10\t74.03819444444444\t74.03819444444444
                            11\t-147.5388888888889\t-147.5388888888889
                            12\t171.9039351851852\t171.9039351851852
                            13\t17.845283018867924\t17.845283018867924
                            14\t191.85555555555555\t191.85555555555555
                            15\t-238.13611111111112\t-238.13611111111112
                            16\t-237.17449494949494\t-237.17449494949494
                            17\t-124.8510101010101\t-124.8510101010101
                            18\t505.80151515151516\t505.80151515151516
                            19\t-413.9492424242424\t-413.9492424242424
                            20\t-427.335101010101\t-427.335101010101
                            21\t677.4603535353535\t677.4603535353535
                            22\t15.899747474747475\t15.899747474747475
                            23\t34.224242424242426\t34.224242424242426
                            """,
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
                    """
                            hour\tx\ty
                            0\t32675\t32675
                            1\t32767\t32767
                            2\t32728\t32728
                            3\t32722\t32722
                            4\t32765\t32765
                            5\t32761\t32761
                            6\t32750\t32750
                            7\t32765\t32765
                            8\t32765\t32765
                            9\t32750\t32750
                            10\t32738\t32738
                            11\t32757\t32757
                            12\t32739\t32739
                            13\t32753\t32753
                            14\t32749\t32749
                            15\t32746\t32746
                            16\t32712\t32712
                            17\t32760\t32760
                            18\t32764\t32764
                            19\t32761\t32761
                            20\t32717\t32717
                            21\t32751\t32751
                            22\t32742\t32742
                            23\t32716\t32716
                            """,
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
                    """
                            hour\tx\ty
                            0\t-32752\t-32752
                            1\t-32752\t-32752
                            2\t-32766\t-32766
                            3\t-32768\t-32768
                            4\t-32740\t-32740
                            5\t-32768\t-32768
                            6\t-32756\t-32756
                            7\t-32738\t-32738
                            8\t-32763\t-32763
                            9\t-32751\t-32751
                            10\t-32731\t-32731
                            11\t-32763\t-32763
                            12\t-32762\t-32762
                            13\t-32752\t-32752
                            14\t-32766\t-32766
                            15\t-32758\t-32758
                            16\t-32767\t-32767
                            17\t-32765\t-32765
                            18\t-32766\t-32766
                            19\t-32767\t-32767
                            20\t-32722\t-32722
                            21\t-32735\t-32735
                            22\t-32750\t-32750
                            23\t-32767\t-32767
                            """,
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
                    """
                            hour\tx\ty
                            0\t1548222\t1548222
                            1\t681530\t681530
                            2\t789465\t789465
                            3\t-1704091\t-1704091
                            4\t174400\t174400
                            5\t1052734\t1052734
                            6\t-931176\t-931176
                            7\t907029\t907029
                            8\t-124459\t-124459
                            9\t-315323\t-315323
                            10\t319845\t319845
                            11\t-637368\t-637368
                            12\t742625\t742625
                            13\t75664\t75664
                            14\t759748\t759748
                            15\t-943019\t-943019
                            16\t-939211\t-939211
                            17\t-494410\t-494410
                            18\t2002974\t2002974
                            19\t-1639239\t-1639239
                            20\t-1692247\t-1692247
                            21\t2682743\t2682743
                            22\t62963\t62963
                            23\t135528\t135528
                            """,
                    "select hour(ts), sum(x) x, sum(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence_ns(0, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    """
                            hour\tx\ty
                            0\t358.3847222222222\t358.3847222222222
                            1\t157.76157407407408\t157.76157407407408
                            2\t182.74652777777777\t182.74652777777777
                            3\t-394.46550925925925\t-394.46550925925925
                            4\t40.370370370370374\t40.370370370370374
                            5\t243.6884259259259\t243.6884259259259
                            6\t-215.55\t-215.55
                            7\t209.96041666666667\t209.96041666666667
                            8\t-28.809953703703705\t-28.809953703703705
                            9\t-72.99143518518518\t-72.99143518518518
                            10\t74.03819444444444\t74.03819444444444
                            11\t-147.5388888888889\t-147.5388888888889
                            12\t171.9039351851852\t171.9039351851852
                            13\t17.845283018867924\t17.845283018867924
                            14\t191.85555555555555\t191.85555555555555
                            15\t-238.13611111111112\t-238.13611111111112
                            16\t-237.17449494949494\t-237.17449494949494
                            17\t-124.8510101010101\t-124.8510101010101
                            18\t505.80151515151516\t505.80151515151516
                            19\t-413.9492424242424\t-413.9492424242424
                            20\t-427.335101010101\t-427.335101010101
                            21\t677.4603535353535\t677.4603535353535
                            22\t15.899747474747475\t15.899747474747475
                            23\t34.224242424242426\t34.224242424242426
                            """,
                    "select hour(ts), avg(x) x, avg(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence_ns(0, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    """
                            hour\tx\ty
                            0\t32675\t32675
                            1\t32767\t32767
                            2\t32728\t32728
                            3\t32722\t32722
                            4\t32765\t32765
                            5\t32761\t32761
                            6\t32750\t32750
                            7\t32765\t32765
                            8\t32765\t32765
                            9\t32750\t32750
                            10\t32738\t32738
                            11\t32757\t32757
                            12\t32739\t32739
                            13\t32753\t32753
                            14\t32749\t32749
                            15\t32746\t32746
                            16\t32712\t32712
                            17\t32760\t32760
                            18\t32764\t32764
                            19\t32761\t32761
                            20\t32717\t32717
                            21\t32751\t32751
                            22\t32742\t32742
                            23\t32716\t32716
                            """,
                    "select hour(ts), max(x) x, max(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourMin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence_ns(0, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    """
                            hour\tx\ty
                            0\t-32752\t-32752
                            1\t-32752\t-32752
                            2\t-32766\t-32766
                            3\t-32768\t-32768
                            4\t-32740\t-32740
                            5\t-32768\t-32768
                            6\t-32756\t-32756
                            7\t-32738\t-32738
                            8\t-32763\t-32763
                            9\t-32751\t-32751
                            10\t-32731\t-32731
                            11\t-32763\t-32763
                            12\t-32762\t-32762
                            13\t-32752\t-32752
                            14\t-32766\t-32766
                            15\t-32758\t-32758
                            16\t-32767\t-32767
                            17\t-32765\t-32765
                            18\t-32766\t-32766
                            19\t-32767\t-32767
                            20\t-32722\t-32722
                            21\t-32735\t-32735
                            22\t-32750\t-32750
                            23\t-32767\t-32767
                            """,
                    "select hour(ts), min(x) x, min(y) y from abc order by 1"
            );
        });
    }

    @Test
    public void testKeyedNanoHourSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x, timestamp_sequence_ns(0, 10000000000) ts from long_sequence(100000));");
            execute("create table abc as (select x, x::short y, ts from temp)");

            assertSql(
                    """
                            hour\tx\ty
                            0\t1548222\t1548222
                            1\t681530\t681530
                            2\t789465\t789465
                            3\t-1704091\t-1704091
                            4\t174400\t174400
                            5\t1052734\t1052734
                            6\t-931176\t-931176
                            7\t907029\t907029
                            8\t-124459\t-124459
                            9\t-315323\t-315323
                            10\t319845\t319845
                            11\t-637368\t-637368
                            12\t742625\t742625
                            13\t75664\t75664
                            14\t759748\t759748
                            15\t-943019\t-943019
                            16\t-939211\t-939211
                            17\t-494410\t-494410
                            18\t2002974\t2002974
                            19\t-1639239\t-1639239
                            20\t-1692247\t-1692247
                            21\t2682743\t2682743
                            22\t62963\t62963
                            23\t135528\t135528
                            """,
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
                    """
                            x
                            null
                            """,
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
                    """
                            x\ty
                            32767\t32767
                            """,
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
                    """
                            x
                            null
                            """,
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
                    """
                            x\ty
                            -32768\t-32768
                            """,
                    "select min(x) x, min(y) y from abc"
            );
        });
    }

    @Test
    public void testNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table temp as (select rnd_short()::long x from long_sequence(100000));");
            execute("create table abc as (select x, x::short y from temp)");

            assertSql(
                    """
                            avg_x\tavg_y\tsum_x\tsum_y\tmin_x\tmin_y\tmax_x\tmax_y
                            25.14927\t25.14927\t2514927\t2514927\t-32768\t-32768\t32767\t32767
                            """,
                    "select avg(x) avg_x, avg(y) avg_y, " +
                            "sum(x) sum_x, sum(y) sum_y, " +
                            "min(x) min_x, min(y) min_y, " +
                            "max(x) max_x, max(y) max_y " +
                            "from abc"
            );
        });
    }

    @Test
    public void testNonKeyedEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x short, y long)");

            assertSql(
                    """
                            avg_x\tavg_y\tsum_x\tsum_y\tmin_x\tmin_y\tmax_x\tmax_y
                            null\tnull\tnull\tnull\tnull\tnull\tnull\tnull
                            """,
                    "select avg(x) avg_x, avg(y) avg_y, " +
                            "sum(x) sum_x, sum(y) sum_y, " +
                            "min(x) min_x, min(y) min_y, " +
                            "max(x) max_x, max(y) max_y " +
                            "from x"
            );
        });
    }

    @Test
    public void testSumIsNanWhenNoData() throws Exception {
        // empty table should produce null as sum
        assertMemoryLeak(() -> {
            execute("create table abc (x short)");

            assertSql(
                    """
                            x
                            null
                            """,
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
                    """
                            x\ty
                            2514927\t2514927
                            """,
                    "select sum(x) x, sum(y) y from abc"
            );
        });
    }
}
