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

    @Test
    public void testEqConst() throws Exception {
        final String query = "select * from x where i8 = 1 and i16 = 1 and i32 = 1 and i64 = 1 and f32 = 1.0 and f64 = 1.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k) partition by DAY";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testNotEqConst() throws Exception {
        final String query = "select * from x where i8 <> 1 and i16 <> 1 and i32 <> 1 and i64 <> 1 and f32 <> 1.0000 and f64 <> 1.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n" +
                "1970-01-05T15:31:40.000000Z\t4\t4\t4\t4\t4.0000\t4.0\n" +
                "1970-01-05T15:40:00.000000Z\t5\t5\t5\t5\t5.0000\t5.0\n" +
                "1970-01-05T15:48:20.000000Z\t6\t6\t6\t6\t6.0000\t6.0\n" +
                "1970-01-05T15:56:40.000000Z\t7\t7\t7\t7\t7.0000\t7.0\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testLtConst() throws Exception {
        final String query = "select * from x where i8 < 2 and i16 < 2 and i32 < 2 and i64 < 2 and f32 < 2.0000 and f64 < 2.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testLeConst() throws Exception {
        final String query = "select * from x where i8 <= 2 and i16 <= 2 and i32 <= 2 and i64 <= 2 and f32 <= 2.0000 and f64 <= 2.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testGtConst() throws Exception {
        final String query = "select * from x where i8 > 9 and i16 > 9 and i32 > 9 and i64 > 9 and f32 > 9.0000 and f64 > 9.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testGeConst() throws Exception {
        final String query = "select * from x where i8 >= 9 and i16 >= 9 and i32 >= 9 and i64 >= 9 and f32 >= 9.0000 and f64 >= 9.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testAddConst() throws Exception {
        final String query = "select * from x where i8 + 1 = 10 and i16 + 1 = 10 and i32 + 1 = 10 and i64 + 1 = 10 " +
                "and f32 + 1.0 = 10.0 and f64 + 1.0 = 10.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testSubConst() throws Exception {
        final String query = "select * from x where i8 - 1 = 2 and i16 - 1 = 2 and i32 - 1 = 2 and i64 - 1 = 2 " +
                "and f32 - 1.0 = 2.0 and f64 - 1.0 = 2.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testMulConst() throws Exception {
        final String query = "select * from x where i8 * 2 = 4 and i16 * 2 = 4 and i32 * 2 = 4 and i64 * 2 = 4" +
                " and f32 * 2.0 = 4.0 and f64 * 2.0 = 4.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testDivConst() throws Exception {
        final String query = "select * from x where i8 / 2 = 4 and i16 / 2 = 4 and i32 / 2 = 4 and i64 / 2 = 4" +
                " and f32 / 2.0 = 4.0 and f64 / 2.0 = 4.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testIntFloatCast() throws Exception {
        final String query = "select * from x where " +
                "i8 = f32 and " +
                "i8 = f64 and " +
                "i16 = f32 and " +
                "i16 = f64 and " +
                "i32 = f32 and " +
                "i32 = f64 and " +
                "i64 = f32 and " +
                "i64 = f64";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n" +
                "1970-01-05T15:31:40.000000Z\t4\t4\t4\t4\t4.0000\t4.0\n" +
                "1970-01-05T15:40:00.000000Z\t5\t5\t5\t5\t5.0000\t5.0\n" +
                "1970-01-05T15:48:20.000000Z\t6\t6\t6\t6\t6.0000\t6.0\n" +
                "1970-01-05T15:56:40.000000Z\t7\t7\t7\t7\t7.0000\t7.0\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testNegConst() throws Exception {
        final String query = "select * from x where 20 + -i8 = 10 and -f32 * 2 = -20.0";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:21:40.000000Z\t10\t10\t10\t10\t10.0000\t10.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testOrNot() throws Exception {
        final String query = "select * from x where i8 / 2 = 4 or i8 > 6 and not f64 = 10 and not i64 = 7";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T16:05:00.000000Z\t8\t8\t8\t8\t8.0000\t8.0\n" +
                "1970-01-05T16:13:20.000000Z\t9\t9\t9\t9\t9.0000\t9.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(10)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testNull() throws Exception {
        final String query = "select * from x where i8 <> null and i16 <> null and i32 <> null and i64 <> null and f32 <> null and f64 <> null";
        final String expected = "k\ti8\ti16\ti32\ti64\tf32\tf64\n" +
                "1970-01-05T15:06:40.000000Z\t1\t1\t1\t1\t1.0000\t1.0\n" +
                "1970-01-05T15:15:00.000000Z\t2\t2\t2\t2\t2.0000\t2.0\n" +
                "1970-01-05T15:23:20.000000Z\t3\t3\t3\t3\t3.0000\t3.0\n" +
                "1970-01-05T15:31:40.000000Z\t4\t4\t4\t4\t4.0000\t4.0\n" +
                "1970-01-05T15:40:00.000000Z\t5\t5\t5\t5\t5.0000\t5.0\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " cast(x as byte) i8," +
                " cast(x as short) i16," +
                " cast(x as int) i32," +
                " cast(x as long) i64," +
                " cast(x as float) f32," +
                " cast(x as double) f64" +
                " from long_sequence(5)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testSelectAllTypesFromRecord() throws Exception {
        final String query = "select * from x where b = true and kk < 10";
        final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tcc\tl2\thash1b\thash2b\thash3b\thash1c\thash2c\thash4c\thash8c\n" +
                "2\t1637847416\ttrue\tV\t0.4900510449885239\t0.8258\t553\t2015-12-28T22:25:40.934Z\t\t-7611030538224290496\t1970-01-05T15:15:00.000000Z\t37\t00000000 3e e3 f1 f1 1e ca 9c 1d 06 ac\tKGHVUVSDOTSED\tY\t0xbccb30ed7795ebc85f20a35e80e154f458dfd08eeb9cc39ecec82869edec121b\t0\t10\t110\te\tsj\tfhcq\t35jvygt2\n" +
                "3\t844704299\ttrue\t\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-05T15:23:20.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\tY\t0x772c8b7f9505620ebbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea61\t0\t01\t000\tf\t33\teusj\tb5z6npxr\n" +
                "6\t-1501720177\ttrue\tP\t0.18158967304439033\t0.8197\t501\t2015-06-08T17:20:46.703Z\tPEHN\t-4229502740666959541\t1970-01-05T15:48:20.000000Z\t19\t\tTNLEGP\tU\t0x0b4735986b97a80520051a2ed05467f71d3abd90d55b0a125db8f13ef95ce839\t1\t01\t010\tr\tc0\twhjh\trcqfw2hw\n" +
                "8\t526232578\ttrue\tE\t0.6379992093447574\t0.8515\t850\t2015-08-19T05:52:05.329Z\tPEHN\t-5157086556591926155\t1970-01-05T16:05:00.000000Z\t42\t00000000 6d 8c d8 ac c8 46 3b 47 3c e1 72 3b 9d\tJSMKIXEYVTUPD\tH\t0x5ec6d73428fb1c01b680be3ee552450eef8b1c47f7e7f9ecae395228bc24ce17\t0\t11\t000\t5\ttp\tx578\ttdnxkw6d\n";
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

        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32Nulls() throws Exception {
        final String query = "select * from x where i32a = null or i32b = null";
        final String expected = "k\ti32a\ti32b\n" +
                "1970-01-05T15:06:40.000000Z\t-1\tNaN\n" +
                "1970-01-05T15:23:20.000000Z\tNaN\t-3\n" +
                "1970-01-05T15:40:00.000000Z\tNaN\t2\n" +
                "1970-01-05T15:48:20.000000Z\tNaN\t-3\n" +
                "1970-01-05T15:56:40.000000Z\tNaN\tNaN\n" +
                "1970-01-05T16:05:00.000000Z\t1\tNaN\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32NotNulls() throws Exception {
        final String query = "select * from x where i32a <> null and i32b <> null";
        final String expected = "k\ti32a\ti32b\n" +
                "1970-01-05T15:15:00.000000Z\t7\t-2\n" +
                "1970-01-05T15:31:40.000000Z\t10\t0\n" +
                "1970-01-05T16:13:20.000000Z\t4\t9\n" +
                "1970-01-05T16:21:40.000000Z\t-2\t-5\n" +
                "1970-01-05T16:30:00.000000Z\t2\t-5\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32NegNulls() throws Exception {
        final String query = "select * from x where -i32a = null or -i32b = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32AddNulls() throws Exception {
        final String query = "select * from x where i32a + 1 = null";
//        final String query = "select * from x where i32a = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32SubNulls() throws Exception {
        final String query = "select * from x where i32a - i32b = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32MulNulls() throws Exception {
        final String query = "select * from x where i32a * i32b = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32DivNulls() throws Exception {
        final String query = "select * from x where i32a / i32b = null";
        final String expected = "k\ti32a\ti32b\n" +
                "1970-01-05T15:06:40.000000Z\t-1\tNaN\n" +
                "1970-01-05T15:23:20.000000Z\tNaN\t-3\n" +
                "1970-01-05T15:40:00.000000Z\tNaN\t2\n" +
                "1970-01-05T15:48:20.000000Z\tNaN\t-3\n" +
                "1970-01-05T15:56:40.000000Z\tNaN\tNaN\n" +
                "1970-01-05T16:05:00.000000Z\t1\tNaN\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI32CmpIgnoreNulls() throws Exception {
        final String query = "select * from x where i32a > i32b or i32a >= i32b or i32a < i32b or i32a <= i32b";
        final String expected = "k\ti32a\ti32b\n" +
                "1970-01-05T15:15:00.000000Z\t7\t-2\n" +
                "1970-01-05T15:31:40.000000Z\t10\t0\n" +
                "1970-01-05T16:13:20.000000Z\t4\t9\n" +
                "1970-01-05T16:21:40.000000Z\t-2\t-5\n" +
                "1970-01-05T16:30:00.000000Z\t2\t-5\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_int(-10, 10, 1) i32a," +
                " rnd_int(-10, 10, 1) i32b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64Nulls() throws Exception {
        final String query = "select * from x where i64a = null or i64b = null";
        final String expected = "k\ti64a\ti64b\n" +
                "1970-01-05T15:06:40.000000Z\t2\tNaN\n" +
                "1970-01-05T15:23:20.000000Z\tNaN\t9\n" +
                "1970-01-05T15:40:00.000000Z\tNaN\t1\n" +
                "1970-01-05T15:48:20.000000Z\tNaN\t-5\n" +
                "1970-01-05T15:56:40.000000Z\tNaN\tNaN\n" +
                "1970-01-05T16:05:00.000000Z\t-2\tNaN\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64NotNulls() throws Exception {
        final String query = "select * from x where i64a <> null and i64b <> null";
        final String expected = "k\ti64a\ti64b\n" +
                "1970-01-05T15:15:00.000000Z\t4\t1\n" +
                "1970-01-05T15:31:40.000000Z\t8\t8\n" +
                "1970-01-05T16:13:20.000000Z\t-9\t-10\n" +
                "1970-01-05T16:21:40.000000Z\t-6\t7\n" +
                "1970-01-05T16:30:00.000000Z\t0\t7\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64NegNulls() throws Exception {
        final String query = "select * from x where -i64a = null or -i64b = null";
        final String expected = "k\ti64a\ti64b\n" +
                "1970-01-05T15:06:40.000000Z\t2\tNaN\n" +
                "1970-01-05T15:23:20.000000Z\tNaN\t9\n" +
                "1970-01-05T15:40:00.000000Z\tNaN\t1\n" +
                "1970-01-05T15:48:20.000000Z\tNaN\t-5\n" +
                "1970-01-05T15:56:40.000000Z\tNaN\tNaN\n" +
                "1970-01-05T16:05:00.000000Z\t-2\tNaN\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64AddNulls() throws Exception {
        final String query = "select * from x where i64a + i64b = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64SubNulls() throws Exception {
        final String query = "select * from x where i64a - i64b = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64MulNulls() throws Exception {
        final String query = "select * from x where i64a * i64b = null";
        final String expected = "";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64DivNulls() throws Exception {
        final String query = "select * from x where i64a / i64b = null";
        final String expected = "k\ti64a\ti64b\n" +
                "1970-01-05T15:06:40.000000Z\t2\tNaN\n" +
                "1970-01-05T15:23:20.000000Z\tNaN\t9\n" +
                "1970-01-05T15:40:00.000000Z\tNaN\t1\n" +
                "1970-01-05T15:48:20.000000Z\tNaN\t-5\n" +
                "1970-01-05T15:56:40.000000Z\tNaN\tNaN\n" +
                "1970-01-05T16:05:00.000000Z\t-2\tNaN\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    @Test
    public void testI64CmpIgnoreNulls() throws Exception {
        final String query = "select * from x where i64a > i64b or i64a >= i64b or i64a < i64b or i64a <= i64b";
//        final String query = "select * from x where i64a <> null and i64b <> null";
        final String expected = "k\ti64a\ti64b\n" +
                "1970-01-05T15:15:00.000000Z\t4\t1\n" +
                "1970-01-05T15:31:40.000000Z\t8\t8\n" +
                "1970-01-05T16:13:20.000000Z\t-9\t-10\n" +
                "1970-01-05T16:21:40.000000Z\t-6\t7\n" +
                "1970-01-05T16:30:00.000000Z\t0\t7\n";
        final String ddl = "create table x as " +
                "(select timestamp_sequence(400000000000, 500000000) as k," +
                " rnd_long(-10, 10, 1) i64a," +
                " rnd_long(-10, 10, 1) i64b" +
                " from long_sequence(11)) timestamp(k)";
        assertQuery(expected,
                query,
                ddl,
                "k",
                false);
    }

    // test const expr
    // test null
    // test filter on subquery
    // test interval and filter
    // test wrong type expression a+b
    // test join
    // test latestby
}
