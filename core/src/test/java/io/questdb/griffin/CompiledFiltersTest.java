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

public class CompiledFiltersTest extends AbstractGriffinTest {

    // TODO: keep this test for advanced features such as:
    //  * col tops
    //  * random access cursor

    @Test
    public void testSelectAllTypesFromRecord() throws Exception {
        final String query = "select * from x where b = true and kk < 10";
        final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tcc\tl2\thash1b\thash2b\thash3b\thash1c\thash2c\thash4c\thash8c\n" +
                "2\t1637847416\ttrue\tV\t0.4900510449885239\t0.8258\t553\t2015-12-28T22:25:40.934Z\t\t-7611030538224290496\t1970-01-05T15:15:00.000000Z\t37\t00000000 3e e3 f1 f1 1e ca 9c 1d 06 ac\tKGHVUVSDOTSED\tY\t0x772c8b7f9505620ebbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea61\t0\t10\t110\te\tsj\tfhcq\t35jvygt2\n" +
                "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-05T15:23:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\tY\t0xabbcbeeddca3d4fe4f25a88863fc0f467f24de22c77acf93e983e65f5551d073\t0\t01\t000\tf\t33\teusj\tb5z6npxr\n" +
                "6\t-1501720177\ttrue\tP\t0.18158967304439033\t0.8197\t501\t2015-06-08T17:20:46.703Z\tPEHN\t-4229502740666959541\t1970-01-05T15:48:20.000000Z\t19\t\tTNLEGP\tU\t0x79423d4d320d2649767a4feda060d4fb6923c0c7d965969da1b1140a2be25241\t1\t01\t010\tr\tc0\twhjh\trcqfw2hw\n" +
                "8\t526232578\ttrue\tE\t0.6379992093447574\t0.8515\t850\t2015-08-19T05:52:05.329Z\tPEHN\t-5157086556591926155\t1970-01-05T16:05:00.000000Z\t42\t00000000 6d 8c d8 ac c8 46 3b 47 3c e1 72 3b 9d\tJSMKIXEYVTUPD\tH\t0x2337f7e6b82ebc2405c5c1b231cffa455a6e970fb8b80abcc4129ae493cc6076\t0\t11\t000\t5\ttp\tx578\ttdnxkw6d\n";
        final String ddl = "create table x as (select" +
                " cast(x as int) kk," +
                " rnd_int() a," +
                " rnd_boolean() b," +
                " rnd_str(1,1,2) c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                " rnd_symbol(4,4,4,2) i," +
                " rnd_long() j," +
                " timestamp_sequence(400000000000, 500000000) k," +
                " rnd_byte(2,50) l," +
                " rnd_bin(10, 20, 2) m," +
                " rnd_str(5,16,2) n," +
                " rnd_char() cc," +
                " rnd_long256() l2," +
                " rnd_geohash(1) hash1b," +
                " rnd_geohash(2) hash2b," +
                " rnd_geohash(3) hash3b," +
                " rnd_geohash(5) hash1c," +
                " rnd_geohash(10) hash2c," +
                " rnd_geohash(20) hash4c," +
                " rnd_geohash(40) hash8c" +
                " from long_sequence(100)) timestamp(k) partition by DAY";

        assertQueryRunWithJit(expected,
                query,
                ddl,
                "k",
                true);
    }

    @Test
    public void testFilterWithColTops() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table t1 as (select " +
                    " x," +
                    " timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(20)) timestamp(ts)", sqlExecutionContext);

            compiler.compile("alter table t1 add j long", sqlExecutionContext);

            compiler.compile("insert into t1 select x," +
                            "timestamp_sequence(100000000, 1000000) ts," +
                            " rnd_long() j " +
                            "from long_sequence(20)",
                    sqlExecutionContext);

            final String query = "select * from t1 where j < 0";
            final String expected = "x\tts\tj\n" +
                    "4\t1970-01-01T00:01:43.000000Z\t-6945921502384501475\n" +
                    "7\t1970-01-01T00:01:46.000000Z\t-7611843578141082998\n" +
                    "8\t1970-01-01T00:01:47.000000Z\t-5354193255228091881\n" +
                    "9\t1970-01-01T00:01:48.000000Z\t-2653407051020864006\n" +
                    "10\t1970-01-01T00:01:49.000000Z\t-1675638984090602536\n" +
                    "14\t1970-01-01T00:01:53.000000Z\t-7489826605295361807\n" +
                    "15\t1970-01-01T00:01:54.000000Z\t-4094902006239100839\n" +
                    "16\t1970-01-01T00:01:55.000000Z\t-4474835130332302712\n" +
                    "17\t1970-01-01T00:01:56.000000Z\t-6943924477733600060\n";
            assertQueryRunWithJit(expected,
                    query,
                    null,
                    "ts",
                    true);
        });
    }
}
