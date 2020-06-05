/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class GroupByFunctionTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testKeyedAvgDoubleAllNaN() throws Exception {
        assertQuery("s\tsum\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, avg(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedKSumDoubleAllNaN() throws Exception {
        assertQuery("s\tksum\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, ksum(d) ksum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedKSumDoubleSomeNaN() throws Exception {
        assertQuery("s\tksum\n" +
                        "aa\t416262.4729439181\n" +
                        "bb\t416933.3416598129\n",
                "select s, ksum(d) ksum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedKSumKSumDoubleSomeNaN() throws Exception {
        assertQuery("s\tksum\tksum1\n" +
                        "aa\t416262.4729439181\t416262.4729439181\n" +
                        "bb\t416933.3416598129\t416933.3416598129\n",
                "select s, ksum(d), ksum(d) from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedKSumSumDoubleSomeNaN() throws Exception {
        assertQuery("s\tksum\tsum\n" +
                        "aa\t416262.4729439181\t416262.4729439233\n" +
                        "bb\t416933.3416598129\t416933.34165981587\n",
                "select s, ksum(d), sum(d) from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxDateAllNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\t\n" +
                        "bb\t\n",
                "select s, max(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as date) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxDateSomeNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\t1970-01-01T00:00:09.800Z\n" +
                        "bb\t1970-01-01T00:00:09.897Z\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(rnd_long(0, 10000, 1) as date) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxDoubleAllNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxIntAllNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as int) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxIntSomeNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\t9910\n" +
                        "bb\t9947\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxLongAllNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as long) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxLongSomeNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\t9800\n" +
                        "bb\t9897\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_long(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxTimestampAllNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\t\n" +
                        "bb\t\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as timestamp) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMaxTimestampSomeNaN() throws Exception {
        assertQuery("s\tmax\n" +
                        "aa\t1970-01-01T00:00:00.009800Z\n" +
                        "bb\t1970-01-01T00:00:00.009897Z\n",
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(rnd_long(0, 10000, 1) as timestamp) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinDateAllNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\t\n" +
                        "bb\t\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as date) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinDateSomeNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\t1970-01-01T00:00:00.320Z\n" +
                        "bb\t1970-01-01T00:00:00.085Z\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(rnd_long(0, 10000, 1) as date) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinDoubleAllNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinIntAllNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as int) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinIntSomeNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\t13\n" +
                        "bb\t324\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinLongAllNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as long) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinLongSomeNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\t320\n" +
                        "bb\t85\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_long(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedMinTimestampSomeNaN() throws Exception {
        assertQuery("s\tmin\n" +
                        "aa\t1970-01-01T00:00:00.000320Z\n" +
                        "bb\t1970-01-01T00:00:00.000085Z\n",
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(rnd_long(0, 10000, 1) as timestamp) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedNSumDoubleAllNaN() throws Exception {
        assertQuery("s\tnsum\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, nsum(d) nsum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedNSumDoubleSomeNaN() throws Exception {
        assertQuery("s\tnsum\n" +
                        "aa\t37.816973659638755\n" +
                        "bb\t50.90642211368272\n",
                "select s, nsum(d) nsum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedSumDoubleAllNaN() throws Exception {
        assertQuery("s\tsum\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, sum(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedSumDoubleSomeNaN() throws Exception {
        assertQuery("s\tksum\n" +
                        "aa\t37.81697365963876\n" +
                        "bb\t50.906422113682694\n",
                "select s, sum(d) ksum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedSumIntAllNaN() throws Exception {
        assertQuery("s\tsum\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, sum(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as int) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedSumIntSomeNaN() throws Exception {
        assertQuery("s\tsum\n" +
                        "aa\t371694\n" +
                        "bb\t336046\n",
                "select s, sum(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedAvgIntSomeNaN() throws Exception {
        assertQuery("s\tavg\tavg1\n" +
                        "aa\t4765.307692307692\t4765.307692307692\n" +
                        "bb\t4421.6578947368425\t4421.6578947368425\n",
                "select s, avg(d) avg, avg(d) from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedAvgIntSomeNaNRandomOrder() throws Exception {
        assertQuery("avg\ts\tavg1\n" +
                        "4765.307692307692\taa\t4765.307692307692\n" +
                        "4421.6578947368425\tbb\t4421.6578947368425\n",
                "select avg(d) avg, s, avg(d) from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedAvgIntSomeNaNKeyLast() throws Exception {
        assertQuery("avg\tavg1\ts\n" +
                        "4765.307692307692\t4765.307692307692\taa\n" +
                        "4421.6578947368425\t4421.6578947368425\tbb\n",
                "select avg(d) avg, avg(d), s from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedSumLongAllNaN() throws Exception {
        assertQuery("s\tsum\n" +
                        "aa\tNaN\n" +
                        "bb\tNaN\n",
                "select s, sum(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as long) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedSumLongSomeNaN() throws Exception {
        assertQuery("s\tsum\n" +
                        "aa\t396218\n" +
                        "bb\t483241\n",
                "select s, sum(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_long(0, 10000, 2) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testKeyedAvgLongSomeNaN() throws Exception {
        assertQuery("s\tavg\n" +
                        "aa\t4952.725\n" +
                        "bb\t5429.6741573033705\n",
                "select s, avg(d) avg from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_long(0, 10000, 2) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testVectorKSumDoubleAllNaN() throws Exception {
        assertQuery("sum\n" +
                        "NaN\n",
                "select ksum(d) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testVectorKSumOneDouble() throws Exception {
        assertQuery("sum\n" +
                        "416711.27751251\n",
                "select round(ksum(d),8) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(1000000)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testVectorNSumDoubleAllNaN() throws Exception {
        assertQuery("sum\n" +
                        "NaN\n",
                "select nsum(d) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " NaN d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testVectorNSumOneDouble() throws Exception {
        assertQuery("sum\n" +
                        "833539.8830410708\n",
                "select nsum(d) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testVectorSumDoubleAndIntWithNullsDanglingEdge() throws Exception {
        assertQuery("sum\tsum1\n" +
                        "1824\t20.7839974146286\n",
                "select sum(a),sum(b) from x",
                "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b from long_sequence(42))",
                null,
                false
        );
    }

    @Test
    public void testVectorSumOneDouble() throws Exception {
        assertQuery("sum\n" +
                        "9278.190426\n",
                "select round(sum(d),6) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testVectorSumOneDoubleInPos2() throws Exception {
        assertQuery("sum\n" +
                        "83462.04211\n",
                "select round(sum(d),6) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() i," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(200921)" +
                        ")",
                null,
                false
        );
    }

    @Test
    public void testVectorSumOneDoubleMultiplePartitions() throws Exception {
        assertQuery("sum\n" +
                        "9278.19042608885\n",
                "select sum(d) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 d," +
                        " timestamp_sequence(0, 10000000000) k" +
                        " from" +
                        " long_sequence(200)" +
                        ") timestamp(k) partition by DAY",
                null,
                false
        );
    }
}
