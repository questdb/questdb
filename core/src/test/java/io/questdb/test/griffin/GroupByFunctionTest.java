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

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class GroupByFunctionTest extends AbstractCairoTest {

    @Test
    public void testCaseInitsArgs() throws Exception {
        assertQuery(
                """
                        y_utc_15m\ty_sf_position_mw
                        1970-01-01T00:00:00.000000Z\t-0.2246301342497259
                        1970-01-01T00:30:00.000000Z\t-0.6508594025855301
                        1970-01-01T00:45:00.000000Z\t-0.9856290845874263
                        1970-01-01T01:00:00.000000Z\t-0.5093827001617407
                        1970-01-01T01:30:00.000000Z\t0.5599161804800813
                        1970-01-01T01:45:00.000000Z\t0.2390529010846525
                        1970-01-01T02:00:00.000000Z\t-0.6778564558839208
                        1970-01-01T02:15:00.000000Z\t0.38539947865244994
                        1970-01-01T02:30:00.000000Z\t-0.33608255572515877
                        1970-01-01T02:45:00.000000Z\t0.7675673070796104
                        1970-01-01T03:00:00.000000Z\t0.6217326707853098
                        1970-01-01T03:15:00.000000Z\t0.6381607531178513
                        1970-01-01T03:45:00.000000Z\t0.12026122412833129
                        1970-01-01T04:00:00.000000Z\t-0.8912587536603974
                        1970-01-01T04:15:00.000000Z\t-0.42281342727402726
                        1970-01-01T04:30:00.000000Z\t-0.7664256753596138
                        1970-01-01T05:15:00.000000Z\t-0.8847591603509142
                        1970-01-01T05:30:00.000000Z\t0.931192737286751
                        1970-01-01T05:45:00.000000Z\t0.8001121139739173
                        1970-01-01T06:00:00.000000Z\t0.92050039469858
                        1970-01-01T06:15:00.000000Z\t0.456344569609078
                        1970-01-01T06:30:00.000000Z\t0.40455469747939254
                        1970-01-01T06:45:00.000000Z\t0.5659429139861241
                        1970-01-01T07:00:00.000000Z\t-0.6821660861001273
                        1970-01-01T07:30:00.000000Z\t-0.11585982949541473
                        1970-01-01T07:45:00.000000Z\t0.8164182592467494
                        1970-01-01T08:00:00.000000Z\t0.5449155021518948
                        1970-01-01T08:30:00.000000Z\t0.49428905119584543
                        1970-01-01T08:45:00.000000Z\t-0.6551335839796312
                        1970-01-01T09:15:00.000000Z\t0.9540069089049732
                        1970-01-01T09:30:00.000000Z\t-0.03167026265669903
                        1970-01-01T09:45:00.000000Z\t-0.19751370382305056
                        1970-01-01T10:00:00.000000Z\t0.6806873134626418
                        1970-01-01T10:15:00.000000Z\t-0.24008362859107102
                        1970-01-01T10:30:00.000000Z\t-0.9455893004802433
                        1970-01-01T10:45:00.000000Z\t-0.6247427794126656
                        1970-01-01T11:00:00.000000Z\t-0.3901731258748704
                        1970-01-01T11:15:00.000000Z\t-0.10643046345788132
                        1970-01-01T11:30:00.000000Z\t0.07246172621937097
                        1970-01-01T11:45:00.000000Z\t-0.3679848625908545
                        1970-01-01T12:00:00.000000Z\t0.6697969295620055
                        1970-01-01T12:15:00.000000Z\t-0.26369335635512836
                        1970-01-01T12:45:00.000000Z\t-0.19846258365662472
                        1970-01-01T13:00:00.000000Z\t-0.8595900073631431
                        1970-01-01T13:15:00.000000Z\t0.7458169804091256
                        1970-01-01T13:30:00.000000Z\t0.4274704286353759
                        1970-01-01T14:00:00.000000Z\t-0.8291193369353376
                        1970-01-01T14:30:00.000000Z\t0.2711532808184136
                        1970-01-01T15:00:00.000000Z\t-0.8189713915910615
                        1970-01-01T15:15:00.000000Z\t0.7365115215570027
                        1970-01-01T15:30:00.000000Z\t-0.9418719455092096
                        1970-01-01T16:00:00.000000Z\t-0.05024615679069011
                        1970-01-01T16:15:00.000000Z\t-0.8952510116133903
                        1970-01-01T16:30:00.000000Z\t-0.029227696942726644
                        1970-01-01T16:45:00.000000Z\t-0.7668146556860689
                        1970-01-01T17:00:00.000000Z\t-0.05158459929273784
                        1970-01-01T17:15:00.000000Z\t-0.06846631555382798
                        1970-01-01T17:30:00.000000Z\t-0.5708643723875381
                        1970-01-01T17:45:00.000000Z\t0.7260468106076399
                        1970-01-01T18:15:00.000000Z\t-0.1010501916946902
                        1970-01-01T18:30:00.000000Z\t-0.05094182589333662
                        1970-01-01T18:45:00.000000Z\t-0.38402128906440336
                        1970-01-01T19:15:00.000000Z\t0.7694744648762927
                        1970-01-01T19:45:00.000000Z\t0.6901976778065181
                        1970-01-01T20:00:00.000000Z\t-0.5913874468544745
                        1970-01-01T20:30:00.000000Z\t-0.14261321308606745
                        1970-01-01T20:45:00.000000Z\t0.4440250924606578
                        1970-01-01T21:00:00.000000Z\t-0.09618589590900506
                        1970-01-01T21:15:00.000000Z\t-0.08675950660182763
                        1970-01-01T21:30:00.000000Z\t-0.741970173888595
                        1970-01-01T21:45:00.000000Z\t0.4167781163798937
                        1970-01-01T22:00:00.000000Z\t-0.05514933756198426
                        1970-01-01T22:30:00.000000Z\t-0.2093569947644236
                        1970-01-01T22:45:00.000000Z\t-0.8439276969435359
                        1970-01-01T23:00:00.000000Z\t-0.03973283003449557
                        1970-01-01T23:15:00.000000Z\t-0.8551850405049611
                        1970-01-01T23:45:00.000000Z\t0.6226001464598434
                        1970-01-02T00:00:00.000000Z\t-0.7195457109208119
                        1970-01-02T00:15:00.000000Z\t-0.23493793601747937
                        1970-01-02T00:30:00.000000Z\t-0.6334964081687151
                        """,
                """
                        SELECT
                            delivery_start_utc as y_utc_15m,
                            sum(case
                                    when seller='sf' then -1.0*volume_mw
                                    when buyer='sf' then 1.0*volume_mw
                                    else 0.0
                                end)
                            as y_sf_position_mw
                        FROM (
                            SELECT delivery_start_utc, seller, buyer, volume_mw FROM trades
                            WHERE
                                (seller = 'sf' OR buyer = 'sf')
                            )
                        group by y_utc_15m \
                        order by y_utc_15m""",
                "create table trades as (" +
                        "select" +
                        " timestamp_sequence(0, 15*60*1000000L) delivery_start_utc," +
                        " rnd_symbol('sf', null) seller," +
                        " rnd_symbol('sf', null) buyer," +
                        " rnd_double() volume_mw" +
                        " from long_sequence(100)" +
                        "), index(seller), index(buyer) timestamp(delivery_start_utc)",
                "y_utc_15m",
                true,
                true
        );
    }

    // https://github.com/questdb/questdb/issues/6549
    @Test
    public void testCaseWithAggregatesInGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table test as (" +
                            "  select 'a' as sym, 10.0 as val from long_sequence(1)" +
                            "  union all" +
                            "  select 'a' as sym, 20.0 as val from long_sequence(1)" +
                            "  union all" +
                            "  select 'a' as sym, 30.0 as val from long_sequence(1)" +
                            "  union all" +
                            "  select 'b' as sym, 40.0 as val from long_sequence(1)" +
                            "  union all" +
                            "  select 'b' as sym, 50.0 as val from long_sequence(1)" +
                            ")"
            );

            assertQueryNoLeakCheck(
                    """
                            sym\tresult
                            a\t60.0
                            b\t45.0
                            """,
                    "SELECT sym, " +
                            "CASE WHEN sym = 'a' THEN sum(val) ELSE avg(val) END as result " +
                            "FROM test " +
                            "GROUP BY sym " +
                            "ORDER BY sym",
                    null,
                    true,
                    true
            );

            // Same query, but with additionally SELECTed aggregates
            assertQueryNoLeakCheck(
                    """
                            sym\tsum\tavg\tresult
                            a\t60.0\t20.0\t60.0
                            b\t90.0\t45.0\t45.0
                            """,
                    "SELECT sym, sum(val), avg(val), " +
                            "CASE WHEN sym = 'a' THEN sum(val) ELSE avg(val) END as result " +
                            "FROM test " +
                            "GROUP BY sym " +
                            "ORDER BY sym",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testKeyedAvgDoubleAllNaN() throws Exception {
        assertQuery("""
                        s\tsum
                        aa\tnull
                        bb\tnull
                        """,
                "select s, avg(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedAvgIntSomeNaN() throws Exception {
        assertQuery("""
                        s\tavg\tavg1
                        aa\t4765.307692307692\t4765.307692307692
                        bb\t4421.6578947368425\t4421.6578947368425
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedAvgIntSomeNaNKeyLast() throws Exception {
        assertQuery("""
                        avg\tavg1\ts
                        4765.307692307692\t4765.307692307692\taa
                        4421.6578947368425\t4421.6578947368425\tbb
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedAvgIntSomeNaNRandomOrder() throws Exception {
        assertQueryExpectSize("""
                        avg\ts\tavg1
                        4765.307692307692\taa\t4765.307692307692
                        4421.6578947368425\tbb\t4421.6578947368425
                        """,
                "select avg(d) avg, s, avg(d) from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedAvgLongSomeNaN() throws Exception {
        assertQuery("""
                        s\tavg
                        aa\t4952.725
                        bb\t5429.6741573033705
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedKSumDoubleAllNaN() throws Exception {
        assertQuery("""
                        s\tksum
                        aa\tnull
                        bb\tnull
                        """,
                "select s, ksum(d) ksum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedKSumDoubleSomeNaN() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        assertQuery("""
                        s\tksum
                        aa\t416262.4729439181
                        bb\t416933.3416598129
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedKSumKSumDoubleSomeNaN() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        assertQueryExpectSize("""
                        s\tksum\tksum1
                        aa\t416262.47294392\t416262.47294392
                        bb\t416933.34165981\t416933.34165981
                        """,
                "select s, round(ksum(d), 8) ksum, round(ksum(d), 8) ksum1 from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")"
        );
    }

    @Test
    public void testKeyedKSumSumDoubleSomeNaN() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        assertQuery("""
                        s\tksum\tsum
                        aa\t416262.4729439\t416262.4729439
                        bb\t416933.3416598\t416933.3416598
                        """,
                "select s, round(ksum(d),7) as ksum, round(sum(d),7) as sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxDateAllNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\t
                        bb\t
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxDateSomeNaN() throws Exception {
        assertQuery("""
                        s\tmax
                        aa\t1970-01-01T00:00:09.800Z
                        bb\t1970-01-01T00:00:09.897Z
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxDoubleAllNaN() throws Exception {
        assertQuery("""
                        s\tmax
                        aa\tnull
                        bb\tnull
                        """,
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxIntAllNaN() throws Exception {
        assertQueryExpectSize("""
                        s\tmax
                        aa\tnull
                        bb\tnull
                        """,
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as int) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedMaxIntSomeNaN() throws Exception {
        assertQueryExpectSize("""
                        s\tmax
                        aa\t9910
                        bb\t9947
                        """,
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " rnd_int(0, 10000, 1) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedMaxLongAllNaN() throws Exception {
        assertQueryExpectSize("""
                        s\tmax
                        aa\tnull
                        bb\tnull
                        """,
                "select s, max(d) max from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as long) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedMaxLongSomeNaN() throws Exception {
        assertQuery("""
                        s\tmax
                        aa\t9800
                        bb\t9897
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxTimestampAllNaN() throws Exception {
        assertQuery("""
                        s\tmax
                        aa\t
                        bb\t
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMaxTimestampSomeNaN() throws Exception {
        assertQuery("""
                        s\tmax
                        aa\t1970-01-01T00:00:00.009800Z
                        bb\t1970-01-01T00:00:00.009897Z
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMinDateAllNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\t
                        bb\t
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMinDateSomeNaN() throws Exception {
        assertQueryExpectSize("""
                        s\tmin
                        aa\t1970-01-01T00:00:00.320Z
                        bb\t1970-01-01T00:00:00.085Z
                        """,
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(rnd_long(0, 10000, 1) as date) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedMinDoubleAllNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\tnull
                        bb\tnull
                        """,
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedMinIntAllNaN() throws Exception {
        assertQueryExpectSize("""
                        s\tmin
                        aa\tnull
                        bb\tnull
                        """,
                "select s, min(d) min from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " cast(NaN as int) d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedMinIntSomeNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\t13
                        bb\t324
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMinLongAllNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\tnull
                        bb\tnull
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMinLongSomeNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\t320
                        bb\t85
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedMinTimestampSomeNaN() throws Exception {
        assertQuery("""
                        s\tmin
                        aa\t1970-01-01T00:00:00.000320Z
                        bb\t1970-01-01T00:00:00.000085Z
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedNSumDoubleAllNaN() throws Exception {
        assertQuery("""
                        s\tnsum
                        aa\tnull
                        bb\tnull
                        """,
                "select s, nsum(d) nsum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedNSumDoubleSomeNaN() throws Exception {
        assertQuery("""
                        s\tnsum
                        aa\t37.816973659638755
                        bb\t50.90642211368272
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedSumDoubleAllNaN() throws Exception {
        assertQueryExpectSize("""
                        s\tsum
                        aa\tnull
                        bb\tnull
                        """,
                "select s, sum(d) sum from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('aa','bb') s," +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")"
        );
    }

    @Test
    public void testKeyedSumDoubleSomeNaN() throws Exception {
        assertQuery("""
                        s\tksum
                        aa\t37.81697365963876
                        bb\t50.906422113682694
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedSumIntAllNaN() throws Exception {
        assertQuery("""
                        s\tsum
                        aa\tnull
                        bb\tnull
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedSumIntSomeNaN() throws Exception {
        assertQuery("""
                        s\tsum
                        aa\t371694
                        bb\t336046
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedSumLongAllNaN() throws Exception {
        assertQuery("""
                        s\tsum
                        aa\tnull
                        bb\tnull
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testKeyedSumLongSomeNaN() throws Exception {
        assertQuery("""
                        s\tsum
                        aa\t396218
                        bb\t483241
                        """,
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
                true,
                true
        );
    }

    @Test
    public void testNestedGroupByFn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as(select x, rnd_symbol('a', 'b', 'c') sym from long_sequence(1));");
            try {
                assertExceptionNoLeakCheck("select sym, max(sum(x + min(x)) - avg(x)) from test");
            } catch (SqlException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "Aggregate function cannot be passed as an argument"));
            }
        });
    }

    @Test
    public void testNonNestedGroupByFn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test as(select x, rnd_symbol('a', 'b', 'c') sym from long_sequence(1));");
            try (RecordCursorFactory ignored = select("select sym, max(x) - (min(x) + 1) from test")) {
                Assert.assertTrue(true);
            }
        });
    }

    @Test
    public void testSumOverCrossJoinSubQuery() throws Exception {
        assertQuery("""
                        sum
                        -0.5260093253
                        """,
                "SELECT round(sum(lth*pcp), 10) as sum " +
                        "from ( " +
                        "  select (x.lth - avg_x.lth) as lth, (x.pcp - avg_x.pcp) as pcp " +
                        "  from x cross join (select avg(lth) as lth, avg(pcp) as pcp from x) avg_x " +
                        ")",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(42) lth," +
                        " rnd_double(42) pcp," +
                        " timestamp_sequence(0, 10000000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorCountFirstColumnIsVar() throws Exception {
        assertQuery("""
                        s\tc
                        101.99359297570571\t200
                        """,
                "select sum(d) s, count() c from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_str() s," +
                        " rnd_double() d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorKSumDoubleAllNaN() throws Exception {
        assertQuery("""
                        sum
                        null
                        """,
                "select ksum(d) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorKSumOneDouble() throws Exception {
        assertQuery("""
                        sum
                        416711.27751251
                        """,
                "select round(ksum(d),8) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(1000000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorKeySumResizeMap() throws Exception {
        assertQuery("""
                        s\tsum
                        0\t21996.396421529873
                        1\t18430.213351902767
                        2\t21582.20289545062
                        3\t18886.197550473087
                        4\t18626.875289842945
                        5\t22421.67012862614
                        6\t19274.50888184544
                        7\t21062.567363525584
                        8\t19300.87219062658
                        9\t21145.106798561585
                        10\t21078.253132584043
                        11\t21067.9786751317
                        12\t20468.346126715598
                        13\t20551.991498701467
                        14\t19748.47362124023
                        15\t17240.77286248588
                        16\t21721.823641766256
                        17\t21651.45751437765
                        18\t20081.058624395155
                        19\t19367.1480936651
                        20\t19185.270246154796
                        21\t22609.01497984462
                        22\t18527.36607099132
                        23\t20426.234075344797
                        24\t20864.094451035773
                        25\t20653.288709591638
                        26\t23377.0621986704
                        27\t21317.24774477195
                        28\t21634.76846016653
                        29\t18502.123578041294
                        30\t18599.835823062396
                        31\t20136.409923051087
                        32\t21637.801538769003
                        33\t20475.074412620415
                        34\t21191.904513785386
                        35\t19242.116531720712
                        36\t21114.4468768996
                        37\t18570.36151059884
                        38\t20881.299482715844
                        39\t21615.666995187752
                        40\t20307.475652642068
                        41\t21304.004506785495
                        42\t21907.871531687437
                        43\t21597.50074186244
                        44\t20305.76982683659
                        45\t18912.63548912271
                        46\t20499.125924588825
                        47\t21714.22706718378
                        48\t20904.75188042069
                        49\t20788.461323352192
                        50\t21003.394776289322
                        51\t21592.400805871926
                        52\t17682.20083780195
                        53\t19242.40501634759
                        54\t22141.499895801393
                        55\t21404.541903738234
                        56\t21511.80598746753
                        57\t20079.658593332915
                        58\t20168.53001890331
                        59\t19596.34130104231
                        60\t19447.313833010463
                        61\t18319.565412237593
                        62\t20685.892337847075
                        63\t20425.104437954833
                        64\t21033.02556052891
                        65\t20579.50560727755
                        66\t19511.927907368274
                        67\t21107.125473199736
                        68\t20735.596854793144
                        69\t21129.814589694503
                        70\t23092.303329979914
                        71\t19629.395943910284
                        72\t20794.631598006123
                        73\t23075.28237484086
                        74\t19601.273875555216
                        75\t17932.01171367682
                        76\t22951.822672732018
                        77\t21199.993812950826
                        78\t18785.16454161179
                        79\t19311.9848780359
                        80\t20956.377356863355
                        81\t19872.00532190974
                        82\t21600.331626488965
                        83\t21834.79981407108
                        84\t22115.927234351762
                        85\t21609.746186574088
                        86\t22139.548019370137
                        87\t19862.691446379336
                        88\t21588.234600174223
                        89\t19826.015324336295
                        90\t20199.946707692714
                        91\t18376.801465806846
                        92\t21610.209245564383
                        93\t20734.536750232142
                        94\t21217.755230691095
                        95\t22610.723353594007
                        96\t19074.17812631486
                        97\t21235.135560000825
                        98\t20354.994821321226
                        99\t20127.44757981346
                        100\t18350.327732504014
                        101\t22062.54926918999
                        102\t20579.114009140838
                        103\t19073.1589366246
                        104\t18318.22478021481
                        105\t19569.808591870904
                        106\t19324.205713914962
                        107\t20374.23564149026
                        108\t20317.059920491432
                        109\t20680.995281373267
                        110\t19528.67843954607
                        111\t19996.794772209847
                        112\t21266.340892432112
                        113\t18922.519435619073
                        114\t21003.340399127872
                        115\t21956.52855952085
                        116\t21457.588000943946
                        117\t20110.19060006405
                        118\t18055.100529476837
                        119\t20093.69067822885
                        120\t20352.821247991356
                        121\t19178.969049866046
                        122\t20486.26841842166
                        123\t20215.12454626706
                        124\t20147.14909634907
                        125\t22376.757562596085
                        126\t16031.254445965982
                        127\t20497.006174115664
                        128\t17400.993362389578
                        129\t20188.9585654348
                        130\t19634.107623646316
                        131\t19382.824777312257
                        132\t21078.288020635802
                        133\t21096.292899488275
                        134\t20563.957000816597
                        135\t18484.557444939066
                        136\t18364.52798717082
                        137\t19374.330448430188
                        138\t21596.748873865552
                        139\t18592.756330077995
                        140\t19577.579028991513
                        141\t20103.84269380019
                        142\t19594.74777809035
                        143\t18798.847733681385
                        144\t19896.776940924257
                        145\t20046.477143642824
                        146\t21553.878012992063
                        147\t19113.090602607896
                        148\t20635.182356857826
                        149\t21094.938844849028
                        150\t20790.29920581394
                        151\t19858.767748625563
                        152\t20345.487052655135
                        153\t19623.343677537112
                        154\t21021.143611040392
                        155\t22011.42472199991
                        156\t19827.84148635679
                        157\t19906.34475411807
                        158\t20216.69916378587
                        159\t19520.087346816446
                        160\t18363.623328108555
                        161\t19499.88593770522
                        162\t20073.003313014913
                        163\t21161.580953596273
                        164\t19308.690318112276
                        165\t23333.33094952223
                        166\t20332.087745591078
                        167\t19745.825048374383
                        168\t18253.733195063476
                        169\t19119.89101987033
                        170\t21947.623786626278
                        171\t20218.55445904121
                        172\t20402.25987332641
                        173\t19201.223824415647
                        174\t20421.968638924478
                        175\t23575.379788933453
                        176\t19444.446027005666
                        177\t20917.35331823729
                        178\t18490.432428633176
                        179\t20705.94703893491
                        180\t20695.102225687548
                        181\t20911.94165282523
                        182\t20851.791669186285
                        183\t19890.23678054491
                        184\t19239.540963194468
                        185\t20347.547640946137
                        186\t18916.532923771087
                        187\t22286.62889221096
                        188\t21785.06707438091
                        189\t19242.459444044463
                        190\t20347.545089339448
                        191\t18536.238087155467
                        192\t20131.158763611347
                        193\t19774.362637535232
                        194\t20511.94694881662
                        195\t19010.02942260133
                        196\t21618.976490048837
                        197\t21513.179702590103
                        198\t19392.134344603088
                        199\t20712.523454066584
                        200\t24808.583700373118
                        201\t20778.443030457838
                        202\t19948.573611561216
                        203\t20722.264170202212
                        204\t21128.546971977383
                        205\t20388.2032858762
                        206\t19711.436250263396
                        207\t20739.379332063403
                        208\t20220.618961184646
                        209\t20326.324254305677
                        210\t20533.03291192617
                        211\t20336.697439717245
                        212\t20676.882835390712
                        213\t20695.071231200116
                        214\t19513.769063927266
                        215\t17714.487034863443
                        216\t18312.240638616382
                        217\t20412.513406706872
                        218\t17931.433667799236
                        219\t20256.780423624798
                        220\t20215.16181760143
                        221\t19490.901806875612
                        222\t21090.58779224527
                        223\t20057.588351907812
                        224\t19873.323753421322
                        225\t20761.84835114679
                        226\t20419.388756195858
                        227\t21067.909232606416
                        228\t20300.820697609048
                        229\t19741.938707765952
                        230\t19685.274846963195
                        231\t17016.511175273423
                        232\t17965.011631845875
                        233\t19709.59769914074
                        234\t19329.694437520513
                        235\t20073.23505333253
                        236\t21014.09108597188
                        237\t20597.37538044743
                        238\t21400.450339585313
                        239\t22930.188094407444
                        240\t21508.57850006803
                        241\t19403.31661312281
                        242\t21778.324224584157
                        243\t18503.514352418253
                        244\t19293.264675185656
                        245\t21040.460558385406
                        246\t20126.92575343779
                        247\t20580.188670723317
                        248\t20004.83651766578
                        249\t21221.91485223463
                        250\t20020.619038727407
                        251\t20281.28953303891
                        252\t21429.759344230057
                        253\t22270.830015359177
                        254\t19893.183919746087
                        255\t20212.163158500967
                        256\t21967.424275986243
                        257\t20009.163965497108
                        258\t21913.207096150672
                        259\t21618.667740859353
                        260\t20066.268106876898
                        261\t20568.984243407267
                        262\t22463.511158490954
                        263\t20125.5586638387
                        264\t20730.293802180535
                        265\t20781.762497804717
                        266\t19558.0968008724
                        267\t21358.064354396014
                        268\t19612.86905295259
                        269\t21252.344761161232
                        270\t18434.251789966333
                        271\t20006.30147433436
                        272\t21230.376651984036
                        273\t20887.69850480357
                        274\t20633.80161874479
                        275\t19620.52515832962
                        276\t19614.918143874656
                        277\t19902.35500303006
                        278\t17552.234948387868
                        279\t19685.750473962984
                        280\t19234.594507172944
                        281\t21956.67517542751
                        282\t19477.896385964486
                        283\t18899.38364971125
                        284\t22186.113681728224
                        285\t21490.281028990114
                        286\t20047.90390365092
                        287\t19079.214534105125
                        288\t22756.918501198623
                        289\t21514.72739662589
                        290\t18361.119884525087
                        291\t19101.52469847238
                        292\t20491.65649096913
                        293\t20807.361326746144
                        294\t18676.20764095315
                        295\t19354.933064667654
                        296\t21457.45406382862
                        297\t21872.599557292622
                        298\t18661.956966557555
                        299\t21398.336709515785
                        300\t19265.699011847315
                        301\t21820.951514611574
                        302\t18732.800357059095
                        303\t19795.306546094784
                        304\t19925.457161363665
                        305\t22407.35179221074
                        306\t21438.61423249241
                        307\t19492.871235220107
                        308\t21508.806432136163
                        309\t20617.006653514007
                        310\t22413.66433085996
                        311\t20384.355911674098
                        312\t21057.459551066862
                        313\t21909.338541093675
                        314\t20004.098794442696
                        315\t19798.34997325148
                        316\t20629.213103693717
                        317\t21809.28746129522
                        318\t19837.778082124594
                        319\t21672.748871098876
                        320\t21685.654412941276
                        321\t19952.732009215022
                        322\t21385.50316196336
                        323\t20647.576181468503
                        324\t20121.78956465105
                        325\t21273.135061137902
                        326\t20464.692337339475
                        327\t21446.80043983786
                        328\t18522.06240805316
                        329\t20207.772404398136
                        330\t20536.607469983974
                        331\t17871.989521293992
                        332\t19428.405346421416
                        333\t21879.905697465725
                        334\t21322.374617034988
                        335\t21230.9877530124
                        336\t19051.298970274212
                        337\t20482.3823695181
                        338\t19889.30084420339
                        339\t20883.415923471683
                        340\t20347.272603985
                        341\t22192.634331915346
                        342\t20113.304377241806
                        343\t20451.01300730615
                        344\t21527.97863178663
                        345\t20202.46696727514
                        346\t19255.488576375894
                        347\t20371.350898393903
                        348\t20859.068541811663
                        349\t21216.82389673966
                        350\t21521.462953359824
                        351\t18722.125081610935
                        352\t21871.03438882927
                        353\t22059.31196051426
                        354\t20134.22347631108
                        355\t22876.078691588973
                        356\t19723.690445402393
                        357\t20857.397264758656
                        358\t20604.283713201756
                        359\t19638.615381871303
                        360\t19457.34970862684
                        361\t19740.759448667224
                        362\t20884.708835321326
                        363\t21728.34320253158
                        364\t21448.211555463375
                        365\t21919.370711781165
                        366\t19222.690306650897
                        367\t22827.132599583976
                        368\t21801.186161825743
                        369\t18951.536301629734
                        370\t19155.330040646666
                        371\t18561.24008449822
                        372\t20099.518330454688
                        373\t19921.513914866027
                        374\t20414.805333764252
                        375\t23071.361692873783
                        376\t21672.985212507578
                        377\t19726.124813212497
                        378\t19608.01909164477
                        379\t22300.800376085648
                        380\t19174.161586769227
                        381\t19331.34754692535
                        382\t21650.237905922695
                        383\t20685.74130647556
                        384\t22083.80286503049
                        385\t20778.94034130496
                        386\t21999.39033182578
                        387\t19722.484735806527
                        388\t19113.596818159695
                        389\t18778.770705394236
                        390\t20907.864235156158
                        391\t21064.39234450163
                        392\t20354.98921697214
                        393\t20497.78442505381
                        394\t21275.320304879806
                        395\t21223.894894211022
                        396\t19360.045850705832
                        397\t20465.140750739632
                        398\t19385.532339634
                        399\t19955.759629022235
                        400\t20199.215389085184
                        401\t20370.019923436474
                        402\t19484.414072839758
                        403\t19448.463181231153
                        404\t20184.5598118759
                        405\t20354.65371332485
                        406\t20536.732366642085
                        407\t21032.194961141606
                        408\t20840.201023130125
                        409\t21047.20128270261
                        410\t20490.31258324971
                        411\t21779.723758234748
                        412\t21415.903080691936
                        413\t18580.17881609032
                        414\t21464.858457866503
                        415\t21083.538127886528
                        416\t19593.19817473515
                        417\t18789.012843677334
                        418\t21111.69249593215
                        419\t21716.27237878259
                        420\t18324.432863132704
                        421\t21326.111048214432
                        422\t19645.81231065597
                        423\t20466.5979709147
                        424\t21119.175838591917
                        425\t22701.83547761915
                        426\t21496.55738046127
                        427\t20941.5867503509
                        428\t19573.340489525337
                        429\t20884.731484529788
                        430\t20365.389061598384
                        431\t21136.10594089836
                        432\t20800.274851592843
                        433\t20115.620291553023
                        434\t20604.002476913964
                        435\t22073.944657583994
                        436\t19506.423321123355
                        437\t17481.744604235766
                        438\t20101.789741754113
                        439\t20113.28500163228
                        440\t20737.90278543028
                        441\t19373.98614615818
                        442\t21094.578113211897
                        443\t18738.108229387275
                        444\t21474.353480908307
                        445\t18362.20183729841
                        446\t21477.296994259912
                        447\t19436.28074910509
                        448\t21581.721620446282
                        449\t18623.312360634347
                        450\t18726.781873865224
                        451\t18835.69715369462
                        452\t20221.73061537913
                        453\t19970.03935797742
                        454\t18927.4365373553
                        455\t21164.70455181253
                        456\t20970.13076557434
                        457\t19306.24853561359
                        458\t20469.641527067448
                        459\t20118.31607740643
                        460\t20792.134720673574
                        461\t20297.249654194824
                        462\t22333.31374512776
                        463\t21861.986514059507
                        464\t20376.38411109588
                        465\t21272.549360180023
                        466\t20617.000282553527
                        467\t19923.865569330206
                        468\t20897.13422902252
                        469\t21565.78988351037
                        470\t21886.80915607373
                        471\t21301.88851732523
                        472\t20543.15931034052
                        473\t20886.282709826322
                        474\t21011.935260988208
                        475\t19862.693880825023
                        476\t20250.13717577616
                        477\t21056.01118230471
                        478\t21621.535356781573
                        479\t20368.044394698347
                        480\t21479.32238598624
                        481\t19625.43445496937
                        482\t21670.2305835014
                        483\t21360.688596841086
                        484\t20330.280318965335
                        485\t19347.40837440343
                        486\t21290.03208341229
                        487\t20435.171737807854
                        488\t21121.33774587185
                        489\t19782.493073963076
                        490\t17891.73487405126
                        491\t19084.90993769181
                        492\t20985.932636932048
                        493\t20283.085896497654
                        494\t19444.254029709566
                        495\t20643.75799319503
                        496\t19602.004211503954
                        497\t19826.73296712897
                        498\t21974.490156906453
                        499\t20405.87138115992
                        500\t19400.554252324495
                        501\t21505.09312048224
                        502\t20353.89769531993
                        503\t20879.077198011157
                        504\t21641.191859608683
                        505\t18527.068567409384
                        506\t23088.92133338919
                        507\t21014.092605797952
                        508\t20384.96877424104
                        509\t22471.90081265033
                        510\t18846.312075679452
                        511\t21373.543777853058
                        512\t21282.439979355426
                        513\t19470.162735640923
                        514\t19672.571567979954
                        515\t18241.959150148563
                        516\t22234.9665964994
                        517\t18448.06741309487
                        518\t19819.59460613268
                        519\t20980.91531217039
                        520\t21193.991919418848
                        521\t20924.393260004912
                        522\t18629.286028202376
                        523\t21804.777823544784
                        524\t20343.303504417534
                        525\t20921.703709644702
                        526\t19960.857166277587
                        527\t20239.519639638413
                        528\t21939.435301212932
                        529\t19229.68936190394
                        530\t21864.34286373374
                        531\t21116.74151602794
                        532\t19981.10336351789
                        533\t21513.023375835794
                        534\t22520.98120947604
                        535\t20793.209813391237
                        536\t20605.754367814567
                        537\t18774.885187515774
                        538\t20121.99554121792
                        539\t21781.99332919553
                        540\t19550.137484913004
                        541\t20233.82249827499
                        542\t19950.05280792748
                        543\t21732.27969483177
                        544\t20383.01669941322
                        545\t19577.319201359114
                        546\t22234.365029878274
                        547\t21153.87060373522
                        548\t19338.96062332392
                        549\t22110.532360666817
                        550\t22240.247517665815
                        551\t19851.411761847416
                        552\t20564.5582600076
                        553\t20791.452500333824
                        554\t19849.73898964073
                        555\t21279.573199557748
                        556\t20132.083721081395
                        557\t21387.060045988943
                        558\t20470.352015497843
                        559\t19217.26439499147
                        560\t19237.862178300067
                        561\t20815.790895288876
                        562\t20887.6883600003
                        563\t20766.977910043446
                        564\t20717.518343061336
                        565\t23452.533419718773
                        566\t19354.431001615605
                        567\t19372.931790120056
                        568\t20266.303126937753
                        569\t20874.518480541712
                        570\t19410.46290076979
                        571\t20368.172667903167
                        572\t22858.774150248395
                        573\t20081.267275987942
                        574\t20526.956272261596
                        575\t18712.363402508938
                        576\t20785.817974029465
                        577\t18098.84630462674
                        578\t20081.06145100757
                        579\t18560.2370733912
                        580\t21027.376296088274
                        581\t20769.251095321666
                        582\t19100.49526996812
                        583\t20869.909618073438
                        584\t21044.630154812505
                        585\t21954.03491524224
                        586\t18151.532901011517
                        587\t21442.146728335178
                        588\t19926.54877302541
                        589\t20499.562267987156
                        590\t21026.978746332617
                        591\t21682.650806172205
                        592\t18017.389982314555
                        593\t21196.626345646946
                        594\t20133.07754129551
                        595\t17973.06539497298
                        596\t20745.652734401614
                        597\t19119.766919748043
                        598\t19005.52094528874
                        599\t21556.98197420946
                        600\t19565.1456791242
                        601\t23334.510395250578
                        602\t21005.05964085358
                        603\t20770.52444755059
                        604\t21580.43280802403
                        605\t20839.747386497642
                        606\t20546.533518907727
                        607\t20321.83345768913
                        608\t19970.1462229666
                        609\t19895.215720715536
                        610\t22055.118517822095
                        611\t20432.962468119185
                        612\t22661.197627741618
                        613\t20252.158186594777
                        614\t21440.087973906746
                        615\t19808.50805152451
                        616\t20076.551329102604
                        617\t20996.92783878634
                        618\t20189.397804041342
                        619\t19682.695425613445
                        620\t20344.267444596044
                        621\t19722.078331883375
                        622\t22054.97621093739
                        623\t19714.348535589223
                        624\t19788.505575631523
                        625\t21240.710170606882
                        626\t17965.52684492908
                        627\t17876.133180892284
                        628\t20167.270386982567
                        629\t20537.91115764532
                        630\t21591.574552446178
                        631\t20169.56791269436
                        632\t19973.950314089492
                        633\t21989.88132565973
                        634\t20139.942547587412
                        635\t21394.854855921003
                        636\t21504.898226047673
                        637\t18120.022385700322
                        638\t21393.577420711314
                        639\t19822.63407048646
                        640\t19478.215625640034
                        641\t21206.083626531796
                        642\t19269.293867607994
                        643\t19892.65987607263
                        644\t21290.377933626147
                        645\t20920.7977034404
                        646\t20717.096881670634
                        647\t18899.796326125244
                        648\t19686.64998941238
                        649\t20632.134199593685
                        650\t21730.16683945338
                        651\t19617.841244192452
                        652\t20894.59057143656
                        653\t21812.95365705345
                        654\t19667.97453294351
                        655\t19944.67352940235
                        656\t20748.8660255149
                        657\t19166.44761435281
                        658\t19360.2983108048
                        659\t20150.17213743987
                        660\t17760.848533409888
                        661\t20600.05798888988
                        662\t21979.180965974527
                        663\t20848.42615757374
                        664\t21157.412576238883
                        665\t20415.103035766075
                        666\t17819.52322754614
                        667\t21547.911515293687
                        668\t20959.315652554014
                        669\t21085.106614646265
                        670\t21632.869513327936
                        671\t18443.183369894206
                        672\t20479.267589192354
                        673\t20299.430410656667
                        674\t21640.697372077484
                        675\t21907.124000472188
                        676\t18519.068358125183
                        677\t20537.37454782867
                        678\t19200.371007221896
                        679\t19626.67982496681
                        680\t21086.14270267948
                        681\t19099.595897987092
                        682\t20124.049510605873
                        683\t21086.53067280067
                        684\t20350.108290997934
                        685\t20388.315780850124
                        686\t20960.27659624097
                        687\t19095.129037561353
                        688\t21327.2176524476
                        689\t19696.573516226454
                        690\t18566.463848627238
                        691\t20796.596725941905
                        692\t19930.880463504123
                        693\t21808.497253806443
                        694\t18420.229902690062
                        695\t22233.780500850826
                        696\t20462.72051768834
                        697\t21281.388013496093
                        698\t21393.63212217268
                        699\t20560.07616151501
                        700\t20379.543993927007
                        701\t20565.07535069424
                        702\t18630.223664045305
                        703\t21658.892595865113
                        704\t20289.357485725912
                        705\t21695.18597204934
                        706\t20841.48564834473
                        707\t20897.687841357532
                        708\t20609.2170531095
                        709\t20957.258782632867
                        710\t22055.484244880045
                        711\t20889.95164827431
                        712\t18618.092560720997
                        713\t21556.934809235587
                        714\t21423.493596257125
                        715\t19932.62485979401
                        716\t20641.69458342141
                        717\t20522.970912412657
                        718\t20575.138931474914
                        719\t21067.011435853943
                        720\t23521.1883821866
                        721\t20978.977857600406
                        722\t20337.264881856627
                        723\t21099.221790684467
                        724\t21055.82464200346
                        725\t19891.802571512057
                        726\t19704.959807154402
                        727\t20614.53840373194
                        728\t22187.38317867404
                        729\t21049.161425446026
                        730\t21500.793571866052
                        731\t19798.152394931953
                        732\t21150.706602463397
                        733\t21086.75369249237
                        734\t19798.55253864408
                        735\t20342.435042742865
                        736\t19513.995276455553
                        737\t20483.82300224263
                        738\t20646.607800699592
                        739\t21935.086670767665
                        740\t19735.54403727172
                        741\t20101.307807414552
                        742\t20882.641647531287
                        743\t19957.35899368776
                        744\t20679.650809502004
                        745\t19048.81601352114
                        746\t19838.822226533815
                        747\t20530.646875793787
                        748\t20439.35927282681
                        749\t19288.09110167857
                        750\t20872.271592797613
                        751\t17992.76350051117
                        752\t18337.380452454698
                        753\t22233.95227625255
                        754\t19130.853861893083
                        755\t20290.526712619838
                        756\t21659.26989480996
                        757\t20489.214178727154
                        758\t21806.031255094473
                        759\t21290.40977542604
                        760\t21576.03339949987
                        761\t23320.043222358847
                        762\t19955.15505639996
                        763\t21922.284139245938
                        764\t19578.90987826597
                        765\t20820.738904333804
                        766\t19922.364824021013
                        767\t18522.23529418135
                        768\t19571.095561294198
                        769\t19206.916590869707
                        770\t21016.825763177818
                        771\t18950.38550044364
                        772\t20378.263605611723
                        773\t18133.85816280555
                        774\t20286.494337460666
                        775\t20645.888659013337
                        776\t23563.98004195245
                        777\t21304.814461490605
                        778\t20188.7507142504
                        779\t19805.674447265897
                        780\t21476.038454559628
                        781\t21841.53332339384
                        782\t21710.25279443068
                        783\t19042.557153956994
                        784\t22065.407460029364
                        785\t20809.70988474769
                        786\t19691.6183892756
                        787\t21289.61435221814
                        788\t23190.207693455126
                        789\t21991.29765694929
                        790\t21880.968803918888
                        791\t20315.867703416196
                        792\t20103.408698819105
                        793\t17858.64365193047
                        794\t20094.96658519408
                        795\t21419.669568083387
                        796\t20689.06632942337
                        797\t20595.545343783622
                        798\t20059.268342742045
                        799\t21203.54331145269
                        800\t19636.92771736701
                        801\t19472.95065217337
                        802\t21246.811104436485
                        803\t20999.31883615907
                        804\t20017.910390641406
                        805\t21944.657853215067
                        806\t19219.1838705248
                        807\t20370.56897969859
                        808\t17977.45767708371
                        809\t20169.76508633492
                        810\t20180.72287272868
                        811\t19220.829593080376
                        812\t19951.4698782106
                        813\t17325.657795648895
                        814\t20892.871932956175
                        815\t19296.398130767226
                        816\t21750.473472133013
                        817\t22615.85917244452
                        818\t20738.003418898083
                        819\t21509.175949539516
                        820\t22280.152678147202
                        821\t19413.971208382532
                        822\t20174.71256451588
                        823\t19450.382757394593
                        824\t21276.141923775773
                        825\t22915.387927184853
                        826\t19781.879102659077
                        827\t19529.948338779206
                        828\t19646.858735539467
                        829\t19582.281759363577
                        830\t19174.178506770317
                        831\t21442.48044057653
                        832\t19392.724512120683
                        833\t20634.0360539889
                        834\t19500.772780633262
                        835\t21716.114320524524
                        836\t19332.358378642224
                        837\t18560.680111896036
                        838\t19296.812464307928
                        839\t19441.29939669176
                        840\t20580.47611481819
                        841\t22396.66316135552
                        842\t20166.455040697198
                        843\t19701.396703732717
                        844\t21296.223842902913
                        845\t21553.638644703628
                        846\t20569.20154246872
                        847\t20715.48656199934
                        848\t19750.41386866224
                        849\t21556.629368347607
                        850\t20985.02261636703
                        851\t21640.371087072108
                        852\t19564.239068101182
                        853\t19326.019610504856
                        854\t21602.217632570613
                        855\t20785.6875241488
                        856\t20977.107868878848
                        857\t21357.991937534378
                        858\t20428.105690338285
                        859\t23018.7832999893
                        860\t22683.38274367983
                        861\t19834.130216788482
                        862\t21931.62431667362
                        863\t18810.052727223057
                        864\t20737.389450934737
                        865\t18966.70418649418
                        866\t18920.653797487346
                        867\t20186.54858159011
                        868\t19766.590918830698
                        869\t21363.18652998055
                        870\t21221.876077755467
                        871\t21447.1004685863
                        872\t19094.70708491569
                        873\t18156.25675341627
                        874\t20700.298375845763
                        875\t21439.630831746872
                        876\t21109.118298193658
                        877\t19537.483635744138
                        878\t19901.84951034268
                        879\t19002.6841049428
                        880\t18487.877279997003
                        881\t18872.928985892417
                        882\t19950.858048740778
                        883\t19877.159646867156
                        884\t21940.389788063443
                        885\t20679.196267163956
                        886\t19927.582304193606
                        887\t19821.53827743212
                        888\t20611.463030023504
                        889\t19962.86565407423
                        890\t23094.01802134652
                        891\t19788.21337113462
                        892\t20003.116139313406
                        893\t21543.26845261543
                        894\t21113.786615247198
                        895\t21691.99621262249
                        896\t22510.026450927642
                        897\t21354.67637962369
                        898\t19780.94477525001
                        899\t18365.19656994662
                        900\t20050.361741925924
                        901\t21055.884376416358
                        902\t19762.060619724853
                        903\t19112.239552817122
                        904\t22763.395067992875
                        905\t20800.963223943247
                        906\t21183.34151945915
                        907\t21211.982903616397
                        908\t20082.825496865018
                        909\t21267.919627832478
                        910\t20011.85811965086
                        911\t20595.05751813289
                        912\t19562.643158571474
                        913\t20858.340839265064
                        914\t19677.61224696134
                        915\t21550.74479693868
                        916\t19485.843610065283
                        917\t20872.605416382517
                        918\t21059.245021727947
                        919\t21550.410306929632
                        920\t20989.185400189283
                        921\t19038.756782169545
                        922\t18950.020528135115
                        923\t20429.23686261661
                        924\t21707.43455705278
                        925\t19500.37402241013
                        926\t20167.07269298281
                        927\t22089.06281509671
                        928\t18425.573684977157
                        929\t20214.24195469606
                        930\t20364.344399220823
                        931\t18971.525802459862
                        932\t20454.57223089007
                        933\t20301.912260271583
                        934\t19627.441951734883
                        935\t19978.547520230273
                        936\t21455.44423771139
                        937\t19775.805609732037
                        938\t20035.917498034632
                        939\t21643.586846531685
                        940\t19808.181882636185
                        941\t18797.992025785574
                        942\t20738.582741849157
                        943\t20302.39725437418
                        944\t20471.71973352979
                        945\t19720.613932506305
                        946\t19090.29362007381
                        947\t19037.55214098704
                        948\t19871.88459481243
                        949\t20933.459413636545
                        950\t20848.458281888536
                        951\t20242.93746605866
                        952\t21370.65802178224
                        953\t22902.96579792867
                        954\t19110.23079770205
                        955\t22034.82908795499
                        956\t20214.562584950414
                        957\t19629.904134588025
                        958\t17732.7221893484
                        959\t20224.76544825604
                        960\t19221.121960312812
                        961\t21789.26859241213
                        962\t18229.083134914752
                        963\t20018.49166633012
                        964\t19438.657326326804
                        965\t19432.832016156055
                        966\t18458.55044052068
                        967\t18021.513388518055
                        968\t20625.345478144518
                        969\t20698.746754206742
                        970\t21040.811554291253
                        971\t20285.330166467982
                        972\t19147.28757545965
                        973\t20983.684198805906
                        974\t21277.05868649091
                        975\t19457.19459996923
                        976\t20925.43355968431
                        977\t19378.506266991793
                        978\t18565.26966948819
                        979\t20982.459585741817
                        980\t21534.349424696473
                        981\t20658.79029369611
                        982\t19161.149824730237
                        983\t22094.10610751024
                        984\t20357.819805438066
                        985\t19611.902394225788
                        986\t19236.048693016775
                        987\t22643.25376404075
                        988\t20421.893207179644
                        989\t21244.260789883818
                        990\t20249.73617276099
                        991\t21782.48807847377
                        992\t18878.91659822052
                        993\t18783.02255166903
                        994\t21434.44273443705
                        995\t21699.118126456116
                        996\t20100.353328456935
                        997\t17729.15458176397
                        998\t20370.61648380776
                        999\t19118.94238658126
                        1000\t21710.328011576985
                        1001\t19692.263085463994
                        1002\t20866.196060441707
                        1003\t19078.837341313847
                        1004\t19154.096283153536
                        1005\t19979.823297372426
                        1006\t20123.656164456504
                        1007\t19562.240913063964
                        1008\t18300.632745372906
                        1009\t20840.90494885689
                        1010\t19454.268107446514
                        1011\t20541.773892509013
                        1012\t20180.263087038853
                        1013\t22169.095949183
                        1014\t20937.665461685792
                        1015\t20933.069871513766
                        1016\t19851.274566480406
                        1017\t21216.062599793164
                        1018\t21153.726690372714
                        1019\t19404.976455280124
                        1020\t18512.660176355876
                        1021\t22697.49280255488
                        1022\t20219.84681331536
                        1023\t20960.23740308324
                        1024\t18373.04232734418
                        1025\t20464.945060735412
                        1026\t18678.216547120606
                        1027\t19160.525348714713
                        1028\t20279.846657842445
                        1029\t19372.667880878133
                        1030\t19656.8940578021
                        1031\t21195.185812675565
                        1032\t20429.882790266543
                        1033\t22528.855333547777
                        1034\t21945.361517977526
                        1035\t18419.028730194586
                        1036\t21856.434962786134
                        1037\t19562.44942576516
                        1038\t20979.356743865945
                        1039\t22461.39606124166
                        1040\t20320.980448576534
                        1041\t21573.238537570214
                        1042\t21325.653217233004
                        1043\t19249.297515298364
                        1044\t20076.786498689176
                        1045\t21758.134310484278
                        1046\t20935.25418283318
                        1047\t20313.806317324663
                        1048\t21297.649978954196
                        1049\t21619.585938408487
                        1050\t20457.851713884625
                        1051\t21811.527431570514
                        1052\t19264.966694842835
                        1053\t20452.20295876226
                        1054\t19564.03551661633
                        1055\t20384.183124116433
                        1056\t21561.23702335092
                        1057\t20777.06110407014
                        1058\t20530.11155766476
                        1059\t19629.790160188768
                        1060\t21551.653408392278
                        1061\t19826.292687376485
                        1062\t18943.81867318477
                        1063\t21464.052212139777
                        1064\t21016.889396118044
                        1065\t19958.913040298285
                        1066\t21422.200471577875
                        1067\t21377.704786251074
                        1068\t21801.854109851858
                        1069\t19463.484131572317
                        1070\t21529.10386418399
                        1071\t19145.677249764147
                        1072\t20837.30015629232
                        1073\t17835.183595377963
                        1074\t22564.148957569814
                        1075\t19466.89428907957
                        1076\t21795.73607642398
                        1077\t20594.125984277096
                        1078\t19126.35669445983
                        1079\t21081.751887219187
                        1080\t20688.632348361738
                        1081\t20375.406457628695
                        1082\t19685.921871711937
                        1083\t20382.50898292084
                        1084\t19603.59237273754
                        1085\t20416.628707018677
                        1086\t20719.10707532466
                        1087\t19038.89828665595
                        1088\t19188.202853730763
                        1089\t20491.162949949874
                        1090\t19653.42153526974
                        1091\t21548.878919770974
                        1092\t19666.76344032232
                        1093\t19483.70914454487
                        1094\t18613.717930057384
                        1095\t19349.49610737046
                        1096\t21546.059940177467
                        1097\t20780.185541202765
                        1098\t19552.067739156973
                        1099\t18078.871619713103
                        1100\t20377.774743462243
                        1101\t19981.91336864403
                        1102\t19414.500494771677
                        1103\t19240.418947819304
                        1104\t19222.51245433043
                        1105\t19101.098415518758
                        1106\t20418.379607780265
                        1107\t20768.88803749501
                        1108\t19724.246536525163
                        1109\t20650.81984199098
                        1110\t20657.908816456846
                        1111\t20774.835987187413
                        1112\t22970.648346522983
                        1113\t20129.91520383487
                        1114\t19057.30270336969
                        1115\t19601.569510423273
                        1116\t19210.215509195517
                        1117\t19358.767680042864
                        1118\t21328.266453547578
                        1119\t19538.34521458885
                        1120\t22144.192863882876
                        1121\t19944.690779177396
                        1122\t20117.86711799549
                        1123\t20919.881808029233
                        1124\t22158.581487128908
                        1125\t20596.301568009516
                        1126\t20477.3516912766
                        1127\t20726.986688933677
                        1128\t20018.536272481375
                        1129\t20544.36218036062
                        1130\t21700.220851759503
                        1131\t20648.583489929613
                        1132\t19367.53933516151
                        1133\t19352.918192442907
                        1134\t19268.309498937295
                        1135\t21367.85593520236
                        1136\t18069.412458558618
                        1137\t19681.396960789196
                        1138\t20144.035489816
                        1139\t19186.821802211467
                        1140\t20264.383518875093
                        1141\t21309.59967172171
                        1142\t21921.203809027946
                        1143\t20065.34396628513
                        1144\t20275.906063302937
                        1145\t22046.233761146374
                        1146\t20656.224384500616
                        1147\t19162.880726596813
                        1148\t19823.702021126122
                        1149\t18164.091741708315
                        1150\t19057.39050820989
                        1151\t20432.794740227757
                        1152\t18712.866057528587
                        1153\t19886.986374298358
                        1154\t20310.87563116679
                        1155\t22321.753031000368
                        1156\t18036.732997738738
                        1157\t20071.917820674284
                        1158\t19912.132148210014
                        1159\t19689.458473588777
                        1160\t18504.383206110477
                        1161\t19133.788481545806
                        1162\t19671.13749322384
                        1163\t21034.659348416928
                        1164\t20579.2941078454
                        1165\t19451.75463184456
                        1166\t19001.402626730003
                        1167\t20645.79566442614
                        1168\t23191.94679630778
                        1169\t21493.026676993213
                        1170\t20726.040313577647
                        1171\t18950.881647967628
                        1172\t20070.600542336728
                        1173\t19618.38144703038
                        1174\t19888.30526048463
                        1175\t19516.40269381467
                        1176\t20260.03307941531
                        1177\t20917.55783584156
                        1178\t20723.800005772806
                        1179\t20850.308837906905
                        1180\t20568.050275405134
                        1181\t19750.485798252204
                        1182\t21952.38440329025
                        1183\t20773.22741815481
                        1184\t17998.736747426614
                        1185\t19781.45835597602
                        1186\t22654.002584828417
                        1187\t20631.12408436602
                        1188\t21140.376372097897
                        1189\t19806.764679853557
                        1190\t21620.552306191697
                        1191\t19528.305017795297
                        1192\t19976.649068323546
                        1193\t20823.935173910668
                        1194\t20503.87406456851
                        1195\t18495.68178729958
                        1196\t18408.367707910314
                        1197\t19857.708162297262
                        1198\t21688.834442457835
                        1199\t19598.487705813488
                        1200\t21435.618088050454
                        1201\t22043.038057637914
                        1202\t20685.72781637254
                        1203\t20149.16163088453
                        1204\t20103.653986523797
                        1205\t20148.267658715413
                        1206\t19014.42321209237
                        1207\t19347.528887944853
                        1208\t20802.033426646572
                        1209\t20068.55949078244
                        1210\t20436.160921730414
                        1211\t21996.37428272774
                        1212\t19935.371496231764
                        1213\t20141.847451813734
                        1214\t19808.77764209393
                        1215\t20356.180916500936
                        1216\t19663.530532271212
                        1217\t18288.220644351157
                        1218\t21105.924728836995
                        1219\t19401.682815128643
                        1220\t21024.147813970936
                        1221\t22000.900442663424
                        1222\t18003.034111042973
                        1223\t21258.83404109589
                        1224\t19325.151230017942
                        1225\t17197.393546328425
                        1226\t21892.42412817628
                        1227\t20654.341097022178
                        1228\t18926.444456838824
                        1229\t20395.855709291212
                        1230\t19283.295627131705
                        1231\t19137.44961579078
                        1232\t20595.67160432812
                        1233\t21428.993769716788
                        1234\t18128.309032748195
                        1235\t18436.02808755988
                        1236\t21214.072846801213
                        1237\t19700.473834502336
                        1238\t20105.152149784648
                        1239\t19711.96722771421
                        1240\t23105.363705656233
                        1241\t20502.874966571442
                        1242\t19911.82693766804
                        1243\t20534.047312523016
                        1244\t20243.461754090873
                        1245\t19149.290420184145
                        1246\t18879.857934597374
                        1247\t21328.582862800955
                        1248\t19462.620885752352
                        1249\t19926.3171731048
                        1250\t19252.97940813008
                        1251\t21249.938137463654
                        1252\t19917.910417036404
                        1253\t19151.8478569273
                        1254\t21731.695527302363
                        1255\t19148.55495783986
                        1256\t20819.647329050287
                        1257\t19793.602101772653
                        1258\t19135.683058179566
                        1259\t22299.779398646057
                        1260\t20070.664549312947
                        1261\t20879.991347264666
                        1262\t18529.9846034872
                        1263\t18501.893443812023
                        1264\t20373.614165824394
                        1265\t22857.49114100163
                        1266\t21055.10644228213
                        1267\t21569.83083949811
                        1268\t19087.75635358942
                        1269\t20484.743749163412
                        1270\t19424.233199684873
                        1271\t21716.605434115816
                        1272\t19611.294396635793
                        1273\t19823.319543715326
                        1274\t20331.097093080607
                        1275\t20154.857720073436
                        1276\t19769.514445779027
                        1277\t20182.76023883704
                        1278\t19006.754555397358
                        1279\t19099.8105294473
                        1280\t20326.916059671
                        1281\t20518.301578123395
                        1282\t17737.522848934488
                        1283\t19679.192117766317
                        1284\t21277.971024792692
                        1285\t23315.68557664755
                        1286\t21392.331221552326
                        1287\t19846.703786102884
                        1288\t23003.704559735015
                        1289\t20185.792842618852
                        1290\t21832.802052837527
                        1291\t22650.627157306888
                        1292\t18977.539883785495
                        1293\t20263.722643664758
                        1294\t20321.802142488195
                        1295\t20105.783051929033
                        1296\t21060.374047175777
                        1297\t21005.17811984143
                        1298\t18730.72643138883
                        1299\t21667.737485788784
                        1300\t21386.2448064358
                        1301\t20870.81531159481
                        1302\t19927.57343675371
                        1303\t20338.160226027325
                        1304\t20276.561321694546
                        1305\t20615.940949704134
                        1306\t19950.279229783024
                        1307\t20479.021289700362
                        1308\t18962.078326542596
                        1309\t18671.849855108983
                        1310\t22685.35385770848
                        1311\t19862.90690236406
                        1312\t19369.92903199442
                        1313\t20995.26970174781
                        1314\t22070.86003488161
                        1315\t18576.297897681005
                        1316\t19816.96042370434
                        1317\t19413.675604447046
                        1318\t20681.762573823442
                        1319\t21965.996752362433
                        1320\t20886.764047530247
                        1321\t16698.146204988996
                        1322\t20614.073732227534
                        1323\t22718.149545495122
                        1324\t21515.02076534202
                        1325\t19413.253135944244
                        1326\t20627.18835862194
                        1327\t19897.50531484845
                        1328\t20126.86112920206
                        1329\t18978.095695686952
                        1330\t19050.968623807486
                        1331\t20954.4301528877
                        1332\t20760.729260258333
                        1333\t19399.268368682402
                        1334\t19374.982696284194
                        1335\t20794.14972567086
                        1336\t20707.27173619046
                        1337\t22247.924565102876
                        1338\t18807.012878282258
                        1339\t19985.2280918771
                        1340\t19514.792105529737
                        1341\t20158.62279510373
                        1342\t22588.251361287887
                        1343\t18945.61057297765
                        1344\t21204.186980258444
                        1345\t18636.3693112822
                        1346\t19546.26583480128
                        1347\t20974.550813111004
                        1348\t20433.295372585275
                        1349\t20740.874515151445
                        1350\t19105.784991701737
                        1351\t22630.294669695504
                        1352\t22011.657573199882
                        1353\t21852.7393241922
                        1354\t22746.23034160445
                        1355\t19964.733355800046
                        1356\t18812.156944779093
                        1357\t18427.363457664807
                        1358\t20591.849449410318
                        1359\t19994.52134818991
                        1360\t20078.53315323534
                        1361\t19008.53476298195
                        1362\t21447.477669072403
                        1363\t21526.703925807786
                        1364\t19862.286098164375
                        1365\t19906.180049315346
                        1366\t20113.84476608908
                        1367\t18881.26273542333
                        1368\t20177.84287059197
                        1369\t19708.785494965905
                        1370\t22132.486322563476
                        1371\t20404.76192216039
                        1372\t20001.001926586807
                        1373\t21186.60492873984
                        1374\t18479.94765881539
                        1375\t20812.849582694944
                        1376\t23286.842735664384
                        1377\t21386.555300088963
                        1378\t21545.301790198162
                        1379\t20100.238732732803
                        1380\t19649.296845212204
                        1381\t22101.180818583078
                        1382\t21952.003652451185
                        1383\t19418.83045898794
                        1384\t20680.879232715106
                        1385\t21232.77589641409
                        1386\t18129.139174583626
                        1387\t18942.042810918745
                        1388\t20439.2425499106
                        1389\t22123.619464464464
                        1390\t20896.214432030712
                        1391\t18605.183692252675
                        1392\t19171.013718367947
                        1393\t20304.99955396808
                        1394\t21726.99552366838
                        1395\t19637.80417638382
                        1396\t20777.20551086274
                        1397\t18550.63996402842
                        1398\t19868.148940722956
                        1399\t19085.30139624654
                        1400\t18913.495618809196
                        1401\t19094.057822570296
                        1402\t19679.447385904772
                        1403\t19674.629526690704
                        1404\t21571.866929318174
                        1405\t20571.512253888708
                        1406\t21580.72862315872
                        1407\t21321.75984665035
                        1408\t22716.311878185283
                        1409\t20294.926231970105
                        1410\t18547.56808566737
                        1411\t21209.447424521142
                        1412\t21666.241649192365
                        1413\t18707.29814314788
                        1414\t20495.080299076242
                        1415\t19156.223635401653
                        1416\t19983.829020531055
                        1417\t19881.185019950306
                        1418\t20451.498658044624
                        1419\t20570.94164772062
                        1420\t20050.241549467217
                        1421\t20360.954285570282
                        1422\t19039.663470678464
                        1423\t19332.92445315202
                        1424\t19389.308376194105
                        1425\t21121.356028427195
                        1426\t17729.480961956582
                        1427\t19090.820337087596
                        1428\t20753.431306049006
                        1429\t19138.11535161945
                        1430\t18281.226621567654
                        1431\t17217.081200879868
                        1432\t22408.31403247538
                        1433\t21580.91003134528
                        1434\t19220.41840947448
                        1435\t21255.141129327658
                        1436\t19632.8674646941
                        1437\t21767.731678235523
                        1438\t18987.217260137204
                        1439\t23306.77181443141
                        1440\t20190.99335993465
                        1441\t22275.04857031528
                        1442\t20434.63883436705
                        1443\t20125.77275495445
                        1444\t18833.321598206905
                        1445\t20139.943874139404
                        1446\t20775.592219297705
                        1447\t19517.31024430505
                        1448\t18249.934727201027
                        1449\t20198.857237274267
                        1450\t19740.356860166972
                        1451\t19687.354222947237
                        1452\t19429.88039446423
                        1453\t19541.057652450236
                        1454\t21431.17144792317
                        1455\t20689.809382585132
                        1456\t20752.52615360526
                        1457\t17936.05248735719
                        1458\t18844.744026821885
                        1459\t19043.358234765295
                        1460\t22521.54605085265
                        1461\t21772.521682203238
                        1462\t20860.55152456676
                        1463\t22619.152722469178
                        1464\t19031.63416316093
                        1465\t19900.285193268483
                        1466\t21225.4561565427
                        1467\t18601.911360094895
                        1468\t22062.38379913733
                        1469\t20058.973758726424
                        1470\t20129.574201239142
                        1471\t20632.815619712357
                        1472\t19391.68717124834
                        1473\t19902.538225325767
                        1474\t19669.021203385084
                        1475\t20008.26051965831
                        1476\t19034.573806662844
                        1477\t20190.172253962155
                        1478\t21940.875430329732
                        1479\t21010.435541952887
                        1480\t21499.636225149043
                        1481\t21920.310466600717
                        1482\t22127.92920419284
                        1483\t21701.27727652403
                        1484\t19449.386675231817
                        1485\t21363.199733041478
                        1486\t20845.647611961627
                        1487\t20066.52057943814
                        1488\t22205.2872476047
                        1489\t20942.939573985328
                        1490\t22630.48861923642
                        1491\t21460.803436533937
                        1492\t19263.60896280063
                        1493\t20449.735441860346
                        1494\t19385.806040024247
                        1495\t19845.755747765128
                        1496\t22282.99535110803
                        1497\t19137.78740494128
                        1498\t19351.212873004744
                        1499\t21411.733618301427
                        1500\t21503.21823765038
                        1501\t20681.746632205493
                        1502\t20596.556701491772
                        1503\t18838.41734569153
                        1504\t21584.38462275216
                        1505\t19681.081312546987
                        1506\t20125.84610590152
                        1507\t22168.554230557907
                        1508\t20337.961553805202
                        1509\t22472.47884063833
                        1510\t21877.912327096397
                        1511\t21029.40539596896
                        1512\t17855.293676036068
                        1513\t22456.94456086072
                        1514\t18226.893537292515
                        1515\t20404.042769400072
                        1516\t20130.220210909298
                        1517\t21375.6314944865
                        1518\t21464.193341309558
                        1519\t19989.988904079306
                        1520\t18855.97780630837
                        1521\t20366.79816324361
                        1522\t20433.466068604426
                        1523\t19792.61921367964
                        1524\t18646.237910401123
                        1525\t20354.505065336045
                        1526\t19705.502132844864
                        1527\t21714.554687974618
                        1528\t21022.718558923156
                        1529\t20709.78469841381
                        1530\t20000.372405397233
                        1531\t21581.436115170778
                        1532\t17791.170792569475
                        1533\t18767.550047833036
                        1534\t18670.573789140402
                        1535\t20432.111953740674
                        1536\t20213.628941997362
                        1537\t18664.577709641482
                        1538\t20637.616994101296
                        1539\t19896.18794827329
                        1540\t17808.53359582599
                        1541\t19659.09964201447
                        1542\t19933.76706493909
                        1543\t18510.850675241232
                        1544\t20478.344857684988
                        1545\t21730.55036400158
                        1546\t20824.166221913973
                        1547\t21341.569057061548
                        1548\t19303.63496629762
                        1549\t20453.648465253435
                        1550\t18080.052611936135
                        1551\t17915.58323325874
                        1552\t20837.432340992025
                        1553\t19583.436010094345
                        1554\t22404.77442315378
                        1555\t19470.170360011416
                        1556\t22259.028475452
                        1557\t19230.2984799285
                        1558\t18628.375561395253
                        1559\t20638.790869934088
                        1560\t18656.885800645487
                        1561\t21018.73712335104
                        1562\t22610.69154310878
                        1563\t20465.458440354276
                        1564\t19695.289373108462
                        1565\t19200.786829227232
                        1566\t19106.773268153578
                        1567\t20219.755861039233
                        1568\t21394.585013551638
                        1569\t22168.522552497285
                        1570\t20775.028298326295
                        1571\t20077.657917560024
                        1572\t20802.963154570967
                        1573\t21318.833118731105
                        1574\t20881.315918069413
                        1575\t18466.855989350075
                        1576\t21716.76755781387
                        1577\t22233.649688278052
                        1578\t18206.252091585586
                        1579\t20933.508065941383
                        1580\t20529.53279752357
                        1581\t20204.501842489637
                        1582\t20679.915722955324
                        1583\t20899.71368146802
                        1584\t19798.817735604927
                        1585\t19755.569053573774
                        1586\t21256.810051144847
                        1587\t19421.284913719177
                        1588\t19883.545202230718
                        1589\t18222.640732740907
                        1590\t18322.065082347035
                        1591\t20525.371424562574
                        1592\t18721.74742087013
                        1593\t21875.60164874604
                        1594\t21774.517902390096
                        1595\t20964.89237143346
                        1596\t19482.452198325096
                        1597\t21555.659136331727
                        1598\t20288.026047765285
                        1599\t20434.560907545863
                        1600\t19491.672887615427
                        1601\t20317.045028803313
                        1602\t21403.08834014176
                        1603\t22230.15813037821
                        1604\t21859.721482124347
                        1605\t19427.930056800615
                        1606\t17653.775598805554
                        1607\t18204.396311772856
                        1608\t20016.33686555377
                        1609\t20681.743394706595
                        1610\t19803.797385426755
                        1611\t23219.582296976994
                        1612\t21147.372745064706
                        1613\t18705.71576598752
                        1614\t19707.23457797013
                        1615\t20075.770317750466
                        1616\t20213.526449208686
                        1617\t20453.839946009102
                        1618\t21642.284123823163
                        1619\t20687.32021163325
                        1620\t21071.99711975442
                        1621\t19005.162605974565
                        1622\t20796.80555785114
                        1623\t19990.778022864062
                        1624\t19426.819763220697
                        1625\t20173.641946098065
                        1626\t20501.238010914483
                        1627\t19899.653756423835
                        1628\t17973.519249085
                        1629\t19241.981534582144
                        1630\t19621.727147915677
                        1631\t20496.217703371327
                        1632\t21907.705168824283
                        1633\t21004.79501264698
                        1634\t20729.9594634503
                        1635\t20552.109025115875
                        1636\t19327.991622110098
                        1637\t21533.58075613506
                        1638\t18944.421622048565
                        1639\t20158.061713555842
                        1640\t21759.06140059305
                        1641\t20931.538188347295
                        1642\t20892.352353584636
                        1643\t20510.92899061004
                        1644\t20055.165560600886
                        1645\t23241.17066413162
                        1646\t22158.25842871295
                        1647\t21175.23907707641
                        1648\t20975.708414879104
                        1649\t22666.288583580477
                        1650\t20639.983841744157
                        1651\t18672.18527071149
                        1652\t19861.42679016056
                        1653\t20974.17430378402
                        1654\t18917.318809115597
                        1655\t21191.092754777794
                        1656\t21312.24788864203
                        1657\t21448.214316536105
                        1658\t20905.008212863744
                        1659\t19323.413135554147
                        1660\t19769.749997306837
                        1661\t20843.818486270968
                        1662\t20777.938931473655
                        1663\t21375.75343948677
                        1664\t20992.809074137265
                        1665\t22423.148066086258
                        1666\t20733.64333214882
                        1667\t21138.401026481464
                        1668\t21346.251246220858
                        1669\t19981.12998484796
                        1670\t18885.294945381826
                        1671\t19251.136792132416
                        1672\t20015.878534790227
                        1673\t22067.53254426082
                        1674\t18273.34779170841
                        1675\t22904.586974624333
                        1676\t19602.56454354078
                        1677\t19685.89148548595
                        1678\t21214.536027069036
                        1679\t21204.04116426205
                        1680\t19726.26794806011
                        1681\t19056.272865010735
                        1682\t19010.577064778692
                        1683\t18616.59606489536
                        1684\t18553.37592892126
                        1685\t18467.815592577583
                        1686\t18531.848422274037
                        1687\t18710.246207166743
                        1688\t22465.727292237123
                        1689\t21431.063855134416
                        1690\t20816.364935977777
                        1691\t20508.077698931356
                        1692\t18664.802691858848
                        1693\t20766.548743300908
                        1694\t19983.538882680357
                        1695\t20118.891890646657
                        1696\t18326.581362008907
                        1697\t18882.89714425213
                        1698\t20152.24404357607
                        1699\t18169.072981584268
                        1700\t20360.755318387794
                        1701\t18827.437290522837
                        1702\t20515.782208150045
                        1703\t20444.431743097393
                        1704\t20026.85912632822
                        1705\t22825.498750344595
                        1706\t19193.04266764382
                        1707\t19529.29936746245
                        1708\t20433.486243422714
                        1709\t19298.554725802693
                        1710\t21753.664949598606
                        1711\t19104.236483009798
                        1712\t17912.712790709058
                        1713\t20534.256602411428
                        1714\t20353.17557234836
                        1715\t18895.134958105584
                        1716\t20569.634792380162
                        1717\t19994.75252243165
                        1718\t20560.841247980592
                        1719\t19540.39319902623
                        1720\t20489.19542463179
                        1721\t21368.38263073127
                        1722\t19465.43386349383
                        1723\t19496.657061254966
                        1724\t19581.158410315904
                        1725\t20213.264288461003
                        1726\t21278.83778183671
                        1727\t20540.187753986567
                        1728\t21164.68640299774
                        1729\t19453.276196173352
                        1730\t21591.157374221148
                        1731\t20274.104170520568
                        1732\t22486.654715938443
                        1733\t21343.929563329642
                        1734\t21298.24302611569
                        1735\t20989.268866424565
                        1736\t19119.46514363623
                        1737\t20055.370021354087
                        1738\t20652.117189740104
                        1739\t20177.317300658033
                        1740\t19828.28258754663
                        1741\t21701.414675519147
                        1742\t20101.50077999343
                        1743\t20177.17168212626
                        1744\t19600.653429317666
                        1745\t19965.049692870678
                        1746\t21554.186609042314
                        1747\t21738.44536252079
                        1748\t19465.402473610375
                        1749\t20270.252194294564
                        1750\t17828.89546404518
                        1751\t19389.856574255922
                        1752\t20141.080850476552
                        1753\t22856.850857712598
                        1754\t20238.264092329504
                        1755\t20055.81226835048
                        1756\t20483.580710739858
                        1757\t20349.578569748686
                        1758\t21106.019329242463
                        1759\t20314.62572678425
                        1760\t18138.21375086935
                        1761\t21622.197969754685
                        1762\t18565.774932438744
                        1763\t20447.074684304447
                        1764\t22005.775367472306
                        1765\t20539.53274443087
                        1766\t18479.843011483717
                        1767\t21597.770003649308
                        1768\t20826.55021814852
                        1769\t20142.632431192433
                        1770\t19315.9384550575
                        1771\t19979.817830039534
                        1772\t20199.039805941036
                        1773\t19422.95582674617
                        1774\t20475.22403327807
                        1775\t21327.958280226147
                        1776\t21564.025233157292
                        1777\t20844.874934312338
                        1778\t20339.710075853385
                        1779\t21155.932382793675
                        1780\t18289.91953712855
                        1781\t21081.132456976105
                        1782\t19661.045225916474
                        1783\t20657.85688620414
                        1784\t20323.10676241121
                        1785\t19610.03125918423
                        1786\t21933.1185456863
                        1787\t20475.639046752152
                        1788\t19547.396684935175
                        1789\t18322.613155507734
                        1790\t18962.625547599873
                        1791\t17579.275164869057
                        1792\t21161.23158999837
                        1793\t21028.534205126216
                        1794\t21867.380092136063
                        1795\t20969.921316950073
                        1796\t20273.24477812441
                        1797\t19790.932628574552
                        1798\t18306.32213856324
                        1799\t20864.75339452038
                        1800\t20431.09260458229
                        1801\t19143.666110918515
                        1802\t19734.71534703164
                        1803\t20714.899701261686
                        1804\t19911.784528854037
                        1805\t20316.57445982594
                        1806\t19824.21618751719
                        1807\t21087.369541957178
                        1808\t19369.630992133236
                        1809\t18958.307165340524
                        1810\t22316.654109619732
                        1811\t21710.235041254055
                        1812\t20439.24611132552
                        1813\t19977.355547090643
                        1814\t20153.76852611992
                        1815\t20653.14551396611
                        1816\t19279.426025837864
                        1817\t24228.081668789226
                        1818\t19708.991981896343
                        1819\t19355.191784659695
                        1820\t18250.635559590628
                        1821\t19306.075837873996
                        1822\t18841.932405120297
                        1823\t19224.52389194246
                        1824\t21311.40078926848
                        1825\t20287.966243162708
                        1826\t20370.873926288936
                        1827\t21350.167090746672
                        1828\t20387.255626404745
                        1829\t18359.983504390104
                        1830\t20304.023535475724
                        1831\t19691.960142078195
                        1832\t17628.09010849083
                        1833\t21155.226700140367
                        1834\t19229.463047370868
                        1835\t21107.737748882384
                        1836\t17724.898315161146
                        1837\t20257.155790257737
                        1838\t20029.633374206016
                        1839\t20783.94179585276
                        1840\t20461.010373404453
                        1841\t18701.650047842424
                        1842\t20248.427181704606
                        1843\t19552.901054269736
                        1844\t18910.317147741862
                        1845\t19689.754359752074
                        1846\t19919.084883360985
                        1847\t19650.685228940347
                        1848\t19475.574094198673
                        1849\t22125.603363470193
                        1850\t20048.960387379582
                        1851\t21198.1320076351
                        1852\t19426.399913355992
                        1853\t20134.830265005516
                        1854\t19926.468675522996
                        1855\t20015.887626169748
                        1856\t19658.756088644248
                        1857\t21562.638807435927
                        1858\t18290.710124884725
                        1859\t19498.56382933794
                        1860\t21016.71656103931
                        1861\t21458.065790425808
                        1862\t20496.194022639324
                        1863\t22057.79482247675
                        1864\t19401.7706235732
                        1865\t20392.070994739952
                        1866\t19232.003315123864
                        1867\t19040.87608922242
                        1868\t21234.8161366396
                        1869\t20175.668959381255
                        1870\t21558.53286128915
                        1871\t21545.900599170418
                        1872\t20314.21768140181
                        1873\t21258.604128094725
                        1874\t20900.885102664142
                        1875\t22966.834374858485
                        1876\t19711.561416647142
                        1877\t20870.722360110405
                        1878\t20087.72799400154
                        1879\t22585.20078639215
                        1880\t20773.8780251312
                        1881\t19668.799545128153
                        1882\t19916.117075672842
                        1883\t19740.062555290682
                        1884\t19050.353582265925
                        1885\t20057.737123858667
                        1886\t20103.441600572896
                        1887\t20266.320425265814
                        1888\t19819.627275238934
                        1889\t20018.65822378638
                        1890\t20480.938207734915
                        1891\t18803.714964422466
                        1892\t20106.966396258864
                        1893\t20674.001870592452
                        1894\t19488.784734621782
                        1895\t21900.552106021707
                        1896\t19197.930548275228
                        1897\t19243.512107633218
                        1898\t19056.855751040068
                        1899\t20808.534222379712
                        1900\t18915.188090219188
                        1901\t18855.33051228066
                        1902\t19378.77830231637
                        1903\t20593.27412123705
                        1904\t19687.274107497495
                        1905\t19756.18123916677
                        1906\t20248.851125367364
                        1907\t19814.162058437905
                        1908\t19663.92378251161
                        1909\t20257.9394801986
                        1910\t19766.870989501662
                        1911\t20281.010300028887
                        1912\t23312.54580127612
                        1913\t20612.31022435077
                        1914\t17518.880020115048
                        1915\t18430.618069238633
                        1916\t21811.102249849944
                        1917\t19354.326513399654
                        1918\t20720.17204815627
                        1919\t20395.49200294638
                        1920\t19090.48435612339
                        1921\t21820.23407214863
                        1922\t20446.63293997881
                        1923\t20167.673405237136
                        1924\t18388.89044993766
                        1925\t20443.37501336225
                        1926\t22375.681581508823
                        1927\t20530.30671340991
                        1928\t21355.016979385902
                        1929\t22104.301901054445
                        1930\t21212.648105401568
                        1931\t21762.08809373781
                        1932\t19712.056773215136
                        1933\t20076.41980666199
                        1934\t19382.620934906186
                        1935\t20683.74319500401
                        1936\t20320.75260394858
                        1937\t20067.35508608998
                        1938\t20875.009167520253
                        1939\t20197.97796872161
                        1940\t19662.59357778078
                        1941\t19536.647583282276
                        1942\t20037.878174827012
                        1943\t20720.37297767212
                        1944\t18707.05178820616
                        1945\t20398.61478516513
                        1946\t20907.07919578409
                        1947\t19765.200378340523
                        1948\t21815.955099960633
                        1949\t20465.936671444597
                        1950\t19561.00592761788
                        1951\t21748.577176749975
                        1952\t18055.992849424572
                        1953\t19033.406248587144
                        1954\t18585.159701641758
                        1955\t22559.378251766542
                        1956\t21299.784475988
                        1957\t21320.427590035935
                        1958\t20968.32869350647
                        1959\t19237.860423113063
                        1960\t20375.49081637289
                        1961\t20267.657651035905
                        1962\t21795.954829140763
                        1963\t20653.584506609142
                        1964\t20791.86207165299
                        1965\t20209.7496618192
                        1966\t20611.018065738906
                        1967\t20833.447978284712
                        1968\t19358.740141785176
                        1969\t21964.27700138339
                        1970\t22834.204341169585
                        1971\t21221.502048637758
                        1972\t21096.70606456808
                        1973\t22151.26008902353
                        1974\t19297.488247992227
                        1975\t19315.343700897094
                        1976\t21737.209315035972
                        1977\t22363.427108256386
                        1978\t20627.34330468378
                        1979\t20123.02111375733
                        1980\t19015.645282836354
                        1981\t19596.58465374503
                        1982\t20400.34612450737
                        1983\t19888.038822715294
                        1984\t19564.975204291182
                        1985\t20769.81983466583
                        1986\t18257.033019601095
                        1987\t20604.56045895006
                        1988\t19728.251269361193
                        1989\t20643.9085307783
                        1990\t21663.78890982736
                        1991\t19109.729245238956
                        1992\t20357.989755984898
                        1993\t20178.694401159944
                        1994\t21313.40557825874
                        1995\t20562.602480450514
                        1996\t19226.167395375374
                        1997\t19242.987220730858
                        1998\t22204.63594834593
                        1999\t21382.633938016697
                        2000\t21214.00033313938
                        2001\t20979.92103226214
                        2002\t20098.96277725745
                        2003\t21067.489576871285
                        2004\t19648.61623794023
                        2005\t22779.75800008844
                        2006\t19389.101402595523
                        2007\t20251.88705964821
                        2008\t20951.273917427177
                        2009\t23459.37200506331
                        2010\t19116.666726763327
                        2011\t18961.13969278356
                        2012\t19318.27824184976
                        2013\t18828.179274551614
                        2014\t20448.359467295373
                        2015\t21443.847863947492
                        2016\t19469.37119480719
                        2017\t21634.722666218302
                        2018\t21194.618140507013
                        2019\t20786.940140225073
                        2020\t20662.037621562955
                        2021\t21991.46973470971
                        2022\t18769.022274328454
                        2023\t21631.835053529987
                        2024\t18360.167177819432
                        2025\t20277.653183754242
                        2026\t21514.66473074804
                        2027\t20295.631539595815
                        2028\t21857.65805167593
                        2029\t18414.187041718014
                        2030\t20219.037270890647
                        2031\t21569.18663201295
                        2032\t20105.582982577424
                        2033\t20896.36367011628
                        2034\t20872.074098781075
                        2035\t20658.00629870781
                        2036\t20587.00446542346
                        2037\t20797.01909409651
                        2038\t19996.049535253504
                        2039\t17936.42354554084
                        2040\t20088.636140491515
                        2041\t19920.466305707683
                        2042\t21563.079825142788
                        2043\t20635.481794895135
                        2044\t22769.396176816364
                        2045\t19064.134057687435
                        2046\t19945.052057072502
                        2047\t19419.954464361075
                        """,
                "select s, sum(d) from x order by s",
                "create table x as " +
                        "(" +
                        "select" +
                        " abs(rnd_int())%2048 s," +
                        " rnd_double(2)*100 d" +
                        " from" +
                        " long_sequence(1000000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testVectorNSumDoubleAllNaN() throws Exception {
        assertQuery("""
                        sum
                        null
                        """,
                "select nsum(d) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " null::double d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorNSumOneDouble() throws Exception {
        assertQuery("""
                        sum
                        833539.8830410708
                        """,
                "select nsum(d) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2) d" +
                        " from" +
                        " long_sequence(2000000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorSumDoubleAndIntWithNullsDanglingEdge() throws Exception {
        assertQuery("""
                        sum\tsum1
                        1824\t20.7839974146286
                        """,
                "select sum(a),sum(b) from x",
                "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b from long_sequence(42))",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorSumOneDouble() throws Exception {
        assertQuery("""
                        sum
                        9278.190426
                        """,
                "select round(sum(d),6) sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 d" +
                        " from" +
                        " long_sequence(200)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorSumOneDoubleInPos2() throws Exception {
        assertQuery("""
                        sum
                        83462.04211
                        """,
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
                false,
                true
        );
    }

    @Test
    public void testVectorSumOneDoubleMultiplePartitions() throws Exception {
        assertQuery("""
                        sum
                        9278.190426089
                        """,
                "select round(sum(d), 9) as sum from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 d," +
                        " timestamp_sequence(0, 10000000000) k" +
                        " from" +
                        " long_sequence(200)" +
                        ") timestamp(k) partition by DAY",
                null,
                false,
                true
        );
    }
}
