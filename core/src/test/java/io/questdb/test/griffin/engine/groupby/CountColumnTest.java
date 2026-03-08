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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CountColumnTest extends AbstractCairoTest {

    @Test
    public void testCountNull() throws Exception {
        assertMemoryLeak(() -> {
            String[] types = {"int", "long", "float", "double", "date", "timestamp", "long256", "string",
                    "symbol", "geohash(5b)", "geohash(10b)", "geohash(20b)", "geohash(40b) "};

            for (String type : types) {
                assertSql(
                        "count\n" +
                                "0\n",
                        "select count(cast(null as " + type + "))"
                );
            }
        });
    }

    @Test
    public void testKeyedCountAllColumnTypesOnDataWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( tstmp timestamp ) timestamp (tstmp) partition by hour");
            execute("insert into x values  (0::timestamp), (1::timestamp), (3600L*1000000::timestamp) ");
            execute("alter table x add column k int");
            execute("insert into x values ((1+3600L*1000000)::timestamp, 3), (2*3600L*1000000::timestamp, 4), ((1+2*3600L*1000000)::timestamp, 5), (3*3600L*1000000::timestamp, 0) ");

            execute("alter table x add column i int, " +
                    " l long, " +
                    " f float, " +
                    " d double, " +
                    " dat date, " +
                    " ts timestamp, " +
                    " l256 long256, " +
                    " str string, " +
                    " sym symbol, " +
                    " gb geohash(5b), " +
                    " gs geohash(10b), " +
                    " gi geohash(20b), " +
                    " gl geohash(40b) ");

            execute("insert into x values ((1+3*3600L*1000000)::timestamp,1, null,null,null,null,null,null,null,null,null,null,null,null,null)");
            execute("insert into x values ((2+3*3600L*1000000)::timestamp,2, 8,8,8f,8d,cast(8 as date),8::timestamp,rnd_long256(),'8','8',rnd_geohash(5) ,rnd_geohash(10),rnd_geohash(20),rnd_geohash(40))");

            execute("insert into x values ((1+4*3600L*1000000)::timestamp,3, null,null,null,null,null,null,null,null,null,null,null,null,null)");
            execute("insert into x values ((2+4*3600L*1000000)::timestamp,4, 10,10,10f,10d,cast(10 as date),10::timestamp,rnd_long256(),'10','10',rnd_geohash(5) ,rnd_geohash(10),rnd_geohash(20),rnd_geohash(40))");
        });

        assertQuery("k\tc1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "null\t3\t3\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "0\t1\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "1\t1\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "2\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\n" +
                        "3\t2\t2\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "4\t2\t2\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\n" +
                        "5\t1\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x order by k",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedCountAllColumnTypesOnEmptyData() throws Exception {
        assertQuery("k\tc1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x " +
                        "( " +
                        " ignore int," +
                        " k int," +
                        " i int, " +
                        " l long, " +
                        " f float, " +
                        " d double, " +
                        " dat date, " +
                        " ts timestamp, " +
                        " l256 long256, " +
                        " str string, " +
                        " sym symbol, " +
                        " gb geohash(5b), " +
                        " gs geohash(10b), " +
                        " gi geohash(20b), " +
                        " gl geohash(40b) " +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedCountAllColumnTypesOnFixedData1() throws Exception {
        assertQuery("k\tc1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "0\t2\t2\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x as " +
                        "(" +
                        " select 0 k, 1 i, 2L l, 3f f, 4d d, cast(1 as date) dat, 1::timestamp ts, rnd_long256() l256, 's' str, 'sym'::symbol sym, rnd_geohash(5) gb, rnd_geohash(10) gs, rnd_geohash(20) gi, rnd_geohash(40) gl from long_sequence(1) " +
                        " union all " +
                        " select 0, null, null, null, null, null, null, null, null, null, null, null, null, null from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedCountAllColumnTypesOnFixedData2() throws Exception {
        assertQuery("k\tc1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "null\t1\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "0\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x " +
                        "order by k",
                "create table x as " +
                        "(" +
                        " select 0 k, 1 i, 2L l, 3f f, 4d d, cast(1 as date) dat, 1::timestamp ts, rnd_long256() l256, 's' str, 'sym'::symbol sym, rnd_geohash(5) gb, rnd_geohash(10) gs, rnd_geohash(20) gi, rnd_geohash(40) gl from long_sequence(1) " +
                        " union all " +
                        " select null, null, null, null, null, null, null, null, null, null, null, null, null, null from long_sequence(1)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedCountAllColumnTypesOnNullData() throws Exception {
        assertQuery("k\tc1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcvar\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "0\t1000\t1000\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(var) cvar, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x as " +
                        "(" +
                        " select 0 k, 1 i, 2L l, 3f f, 4d d, cast(1 as date) dat, 1::timestamp ts, rnd_long256() l256, 's' str, 'v'::varchar var, 'sym'::symbol sym, rnd_geohash(5) gb, rnd_geohash(10) gs, rnd_geohash(20) gi, rnd_geohash(40) gl from long_sequence(1) where x = 10 " +
                        " union all " +
                        " select 0, null, null , null, null, null, null, null, null, null, null, null, null, null, null from long_sequence(1000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testKeyedCountAllColumnTypesOnRandomData() throws Exception {
        assertQuery(
                "k\tc1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcvar\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "0\t10000\t10000\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "1\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8392\t10000\t10000\t10000\t10000\t10000\n" +
                        "2\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8341\t10000\t10000\t10000\t10000\t10000\n" +
                        "3\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8327\t10000\t10000\t10000\t10000\t10000\n" +
                        "4\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8298\t10000\t10000\t10000\t10000\t10000\n" +
                        "5\t10000\t10000\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
                        "6\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8372\t10000\t10000\t10000\t10000\t10000\n" +
                        "7\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8346\t10000\t10000\t10000\t10000\t10000\n" +
                        "8\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8273\t10000\t10000\t10000\t10000\t10000\n" +
                        "9\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t10000\t8378\t10000\t10000\t10000\t10000\t10000\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(varchar) cvar, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x " +
                        "order by k",
                "create table x as " +
                        "(" +
                        "select x%10 k," +
                        " case when x%5 != 0 then rnd_int(1,100,0) else null end i," +
                        " case when x%5 != 0 then rnd_long(1,100,0) else null end l," +
                        " case when x%5 != 0 then rnd_float(0) else null end f," +
                        " case when x%5 != 0 then rnd_double(0) else null end d," +
                        " case when x%5 != 0 then cast(rnd_long() as date) else null end dat," +
                        " case when x%5 != 0 then rnd_long()::timestamp else null end ts," +
                        " case when x%5 != 0 then rnd_long256()  else null end l256," +
                        " case when x%5 != 0 then rnd_str(100,1,10,0) else null end str," +
                        " case when x%5 != 0 then rnd_varchar(5,16,2) else null end varchar," +
                        " case when x%5 != 0 then rnd_symbol(100,1,10,0) else null end sym," +
                        " case when x%5 != 0 then rnd_geohash(5)  else null end gb," +
                        " case when x%5 != 0 then rnd_geohash(10) else null end gs," +
                        " case when x%5 != 0 then rnd_geohash(20) else null end gi," +
                        " case when x%5 != 0 then rnd_geohash(40) else null end gl" +
                        " from" +
                        " long_sequence(100000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testNotKeyedCountAllColumnTypesOnEmptyData() throws Exception {
        assertQuery("c1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcvar\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n",
                "select count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(var) cvar, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x " +
                        "(" +
                        " i int, " +
                        " l long, " +
                        " f float, " +
                        " d double, " +
                        " dat date, " +
                        " ts timestamp, " +
                        " l256 long256, " +
                        " str string, " +
                        " var varchar, " +
                        " sym symbol, " +
                        " gb geohash(5b), " +
                        " gs geohash(10b), " +
                        " gi geohash(20b), " +
                        " gl geohash(40b) " +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testNotKeyedCountAllColumnTypesOnFixedData() throws Exception {
        assertQuery("c1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "2\t2\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\t1\n",
                "select count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x as " +
                        "(" +
                        " select 1 i, 2L l, 3f f, 4d d, cast(1 as date) dat, 1::timestamp ts, rnd_long256() l256, 's' str, 'sym'::symbol sym, rnd_geohash(5) gb, rnd_geohash(10) gs, rnd_geohash(20) gi, rnd_geohash(40) gl from long_sequence(1) " +
                        " union all " +
                        " select null, null, null, null, null, null, null, null, null, null, null, null, null from long_sequence(1)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testNotKeyedCountAllColumnTypesOnNullData() throws Exception {
        assertQuery("c1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "1000\t1000\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n",
                "select count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x as " +
                        "(" +
                        " select 1 i, 2L l, 3f f, 4d d, cast(1 as date) dat, 1::timestamp ts, rnd_long256() l256, 's' str, 'sym'::symbol sym, rnd_geohash(5) gb, rnd_geohash(10) gs, rnd_geohash(20) gi, rnd_geohash(40) gl from long_sequence(1) where x = 10 " +
                        " union all " +
                        " select null, null , null, null, null, null, null, null, null, null, null, null, null from long_sequence(1000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testNotKeyedCountAllColumnTypesOnRandomData() throws Exception {
        assertQuery("c1\tcstar\tci\tcl\tcf\tcd\tcdat\tcts\tcl256\tcstr\tcsym\tcgb\tcgs\tcgi\tcgl\n" +
                        "100000\t100000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\t80000\n",
                "select count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(f) cf, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts, " +
                        "count(l256) cl256, " +
                        "count(str) cstr, " +
                        "count(sym) csym, " +
                        "count(gb) cgb, " +
                        "count(gs) cgs, " +
                        "count(gi) cgi, " +
                        "count(gl) cgl " +
                        "from x",
                "create table x as " +
                        "(" +
                        "select " +
                        " case when x%5 != 0 then rnd_int(1,100,0) else null end i," +
                        " case when x%5 != 0 then rnd_long(1,100,0) else null end l," +
                        " case when x%5 != 0 then rnd_float(0) else null end f," +
                        " case when x%5 != 0 then rnd_double(0) else null end d," +
                        " case when x%5 != 0 then cast(rnd_long() as date) else null end dat," +
                        " case when x%5 != 0 then rnd_long()::timestamp else null end ts," +
                        " case when x%5 != 0 then rnd_long256()  else null end l256," +
                        " case when x%5 != 0 then rnd_str(100,1,10,0) else null end str," +
                        " case when x%5 != 0 then rnd_symbol(100,1,10,0) else null end sym," +
                        " case when x%5 != 0 then rnd_geohash(5)  else null end gb," +
                        " case when x%5 != 0 then rnd_geohash(10) else null end gs," +
                        " case when x%5 != 0 then rnd_geohash(20) else null end gi," +
                        " case when x%5 != 0 then rnd_geohash(40) else null end gl" +
                        " from" +
                        " long_sequence(100000)" +
                        ")",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorizedKeyedCount() throws Exception {
        assertQuery("k\tc1\tcstar\tci\tcl\n" +
                        "null\t769230\t769230\t615384\t615384\n" +
                        "1\t769231\t769231\t615385\t615385\n" +
                        "2\t769231\t769231\t615385\t615385\n" +
                        "3\t769231\t769231\t615385\t615385\n" +
                        "4\t769231\t769231\t615385\t615385\n" +
                        "5\t769231\t769231\t615384\t615384\n" +
                        "6\t769231\t769231\t615385\t615385\n" +
                        "7\t769231\t769231\t615385\t615385\n" +
                        "8\t769231\t769231\t615385\t615385\n" +
                        "9\t769231\t769231\t615385\t615385\n" +
                        "10\t769231\t769231\t615384\t615384\n" +
                        "11\t769230\t769230\t615384\t615384\n" +
                        "12\t769230\t769230\t615384\t615384\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl " +
                        "from x order by k",
                "create table x as " +
                        "(" +
                        "select case when x%13 != 0 then (x%13)::int else null end k," +
                        " case when x%5 != 0 then rnd_int(1,100,0) else null end i," +
                        " case when x%5 != 0 then rnd_long(1,100,0) else null end l" +
                        " from" +
                        " long_sequence(10000000)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testVectorizedKeyedCountWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x ( tstmp timestamp ) timestamp (tstmp) partition by hour");
            execute("insert into x values  (0::timestamp), (1::timestamp), (3600L*1000000::timestamp) ");
            execute("alter table x add column k int");
            execute("insert into x values ((1+3600L*1000000)::timestamp, 3), (2*3600L*1000000::timestamp, 4), ((1+2*3600L*1000000)::timestamp, 5), (3*3600L*1000000::timestamp, 0) ");
            execute("alter table x add column i int, l long ");
            execute("insert into x values ((1+3*3600L*1000000)::timestamp,1, null,null)");
            execute("insert into x values ((2+3*3600L*1000000)::timestamp,2, 8,8)");
            execute("insert into x values ((1+4*3600L*1000000)::timestamp,3, null,null)");
            execute("insert into x values ((2+4*3600L*1000000)::timestamp,4, 10,10) ");
        });

        assertQuery("k\tc1\tcstar\tci\tcl\n" +
                        "null\t3\t3\t0\t0\n" +
                        "0\t1\t1\t0\t0\n" +
                        "1\t1\t1\t0\t0\n" +
                        "2\t1\t1\t1\t1\n" +
                        "3\t2\t2\t0\t0\n" +
                        "4\t2\t2\t1\t1\n" +
                        "5\t1\t1\t0\t0\n",
                "select k, " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl " +
                        "from x order by k",
                null, null, true, true);

        assertQuery("hour\tc1\tcstar\tci\tcl\n" +
                        "0\t2\t2\t0\t0\n" +
                        "1\t2\t2\t0\t0\n" +
                        "2\t2\t2\t0\t0\n" +
                        "3\t3\t3\t1\t1\n" +
                        "4\t2\t2\t1\t1\n",
                "select hour(tstmp), " +
                        "count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl " +
                        "from x " +
                        "order by 1",
                null, null, true, true);
    }

    @Test
    public void testVectorizedNotKeyedCount() throws Exception {
        assertQuery("c1\tcstar\tci\tcl\tdl\tddat\tdts\n" +
                        "100000\t100000\t80000\t80000\t80000\t80000\t80000\n",
                "select count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl," +
                        "count(d) dl, " +
                        "count(dat) ddat, " +
                        "count(ts) dts " +
                        "from x",
                "create table x as " +
                        "(" +
                        "select " +
                        " case when x%5 != 0 then rnd_int(1,100,0) else null end i," +
                        " case when x%5 != 0 then rnd_long(1,100,0) else null end l," +
                        " case when x%5 != 0 then rnd_double(0) else null end d," +
                        " case when x%5 != 0 then cast(rnd_long() as date) else null end dat, " +
                        " case when x%5 != 0 then rnd_long()::timestamp else null end ts " +
                        " from" +
                        " long_sequence(100000)" +
                        ")",
                null,
                false,
                true
        );
    }


}
