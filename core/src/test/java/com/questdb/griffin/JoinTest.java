/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class JoinTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testJoinConstantFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "c\ta\tb\tcolumn\n";
                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(10))", bindVariableService);
                compiler.compile("create table y as (select x, to_int(2*((x-1)/2))+2 m, abs(rnd_int() % 100) b from long_sequence(10))", bindVariableService);

                // master records should be filtered out because slave records missing
                assertJoinQuery(expected, "select x.c, x.a, b, a+b from x join y on y.m = x.c and 1 > 10");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinConstantTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "c\ta\tb\n" +
                        "2\t568\t72\n" +
                        "2\t568\t16\n" +
                        "4\t371\t3\n" +
                        "4\t371\t14\n" +
                        "6\t439\t12\n" +
                        "6\t439\t81\n" +
                        "8\t521\t97\n" +
                        "8\t521\t16\n" +
                        "10\t598\t74\n" +
                        "10\t598\t5\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(10))", bindVariableService);
                compiler.compile("create table y as (select x, to_int(2*((x-1)/2))+2 m, abs(rnd_int() % 100) b from long_sequence(10))", bindVariableService);

                // master records should be filtered out because slave records missing
                assertJoinQuery(expected, "select x.c, x.a, b from x join y on y.m = x.c and 1 < 10");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {

                final String expected = "c\ta\tb\td\tcolumn\n" +
                        "1\t120\t6\t50\t44\n" +
                        "1\t120\t6\t0\t-6\n" +
                        "1\t120\t71\t50\t-21\n" +
                        "1\t120\t71\t0\t-71\n" +
                        "1\t120\t42\t50\t8\n" +
                        "1\t120\t42\t0\t-42\n" +
                        "1\t120\t39\t50\t11\n" +
                        "1\t120\t39\t0\t-39\n" +
                        "2\t568\t14\t55\t41\n" +
                        "2\t568\t14\t968\t954\n" +
                        "2\t568\t72\t55\t-17\n" +
                        "2\t568\t72\t968\t896\n" +
                        "2\t568\t16\t55\t39\n" +
                        "2\t568\t16\t968\t952\n" +
                        "2\t568\t48\t55\t7\n" +
                        "2\t568\t48\t968\t920\n" +
                        "3\t333\t16\t305\t289\n" +
                        "3\t333\t16\t964\t948\n" +
                        "3\t333\t12\t305\t293\n" +
                        "3\t333\t12\t964\t952\n" +
                        "3\t333\t81\t305\t224\n" +
                        "3\t333\t81\t964\t883\n" +
                        "3\t333\t3\t305\t302\n" +
                        "3\t333\t3\t964\t961\n" +
                        "4\t371\t67\t104\t37\n" +
                        "4\t371\t67\t171\t104\n" +
                        "4\t371\t74\t104\t30\n" +
                        "4\t371\t74\t171\t97\n" +
                        "4\t371\t5\t104\t99\n" +
                        "4\t371\t5\t171\t166\n" +
                        "4\t371\t97\t104\t7\n" +
                        "4\t371\t97\t171\t74\n" +
                        "5\t251\t7\t198\t191\n" +
                        "5\t251\t7\t279\t272\n" +
                        "5\t251\t97\t198\t101\n" +
                        "5\t251\t97\t279\t182\n" +
                        "5\t251\t44\t198\t154\n" +
                        "5\t251\t44\t279\t235\n" +
                        "5\t251\t47\t198\t151\n" +
                        "5\t251\t47\t279\t232\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select to_int((x-1)/4 + 1) c, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select to_int((x-1)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(40))", bindVariableService);

                assertJoinQuery(expected, "select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerAllTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "kk\ta\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tkk1\ta1\tb1\tc1\td1\te1\tf1\tg1\ti1\tj1\tk1\tl1\tm1\tn1\n" +
                        "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t2067844108\tfalse\tF\t0.089094427039\t0.8439\t111\t2015-11-01T18:55:38.528Z\t\t8798087869168938593\t1970-01-01T00:50:00.000000Z\t15\t00000000 93 e5 57 a5 db a1 76 1c 1c 26 fb 2e 42 fa f5 6e\n" +
                        "00000010 8f 80 e3 54\tLPBNHG\n" +
                        "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1180113884\tfalse\tZ\t0.041732636309\t0.5677\t16\t2015-11-23T00:35:00.838Z\t\t5953039264407551685\t1970-01-01T00:33:20.000000Z\t24\t00000000 ce 5f b2 8b 5c 54 90 25 c2 20 ff 70 3a c7 8a b3\n" +
                        "00000010 14 cd 47\t\n" +
                        "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t-210935524\tfalse\tL\tNaN\t0.0516\t285\t\tPHRI\t3527911398466283309\t1970-01-01T00:16:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\tHTWNWIFFLRBROMNX\n" +
                        "1\t1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t1\t1389971928\tfalse\tH\t0.599254849305\t0.6456\t632\t2015-01-23T07:09:43.557Z\tPHRI\t-5103414617212558357\t1970-01-01T00:00:00.000000Z\t25\t00000000 6a 71 34 e0 b0 e9 98 f7 67 62 28 60 b0 ec\tLUOHNZH\n" +
                        "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t-1763054372\tfalse\tX\tNaN\t0.9998\t184\t2015-05-16T03:27:28.517Z\tPHRI\t-8441475391834338900\t1970-01-01T01:56:40.000000Z\t13\t00000000 47 3c e1 72 3b 9d ef c4 4a c9 cf fb 9d 63 ca 94\n" +
                        "00000010 00 6b dd 18\tHGGIWH\n" +
                        "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t495047580\ttrue\tD\t0.140225804223\t0.1105\t433\t2015-09-01T17:07:49.293Z\tPZIM\t-8768558643112932333\t1970-01-01T01:40:00.000000Z\t31\t00000000 4b af 8f 89 df 35 8f da fe 33 98 80 85 20 53 3b\n" +
                        "00000010 51 9d 5d\tENNEBQQEM\n" +
                        "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t-779364310\tfalse\t\t0.291509800820\tNaN\t277\t2015-02-20T01:54:36.644Z\tPZIM\t-4036499202601723677\t1970-01-01T01:23:20.000000Z\t23\t00000000 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e 44\tNFKPEVMCGFNW\n" +
                        "2\t-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\t2\t-950108024\tfalse\tC\t0.472902235737\t0.7665\t179\t2015-02-08T12:28:36.066Z\t\t7036584259400395476\t1970-01-01T01:06:40.000000Z\t38\t00000000 49 40 44 49 96 cf 2b b3 71 a7 d5\tIGQZVKHT\n" +
                        "3\t161592763\ttrue\tZ\t0.187697081573\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                        "00000010 8e e5 61 2f\tQOLYXWC\t3\t450540087\tfalse\t\tNaN\t0.1354\t932\t\t\t-6426355179359373684\t1970-01-01T03:03:20.000000Z\t30\t\tKVSBEGM\n" +
                        "3\t161592763\ttrue\tZ\t0.187697081573\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                        "00000010 8e e5 61 2f\tQOLYXWC\t3\t882350590\ttrue\tZ\tNaN\t0.0331\t575\t2015-08-28T02:22:07.682Z\tPZIM\t-6342128731155487317\t1970-01-01T02:46:40.000000Z\t26\t00000000 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a 0c\tVFDBZW\n" +
                        "3\t161592763\ttrue\tZ\t0.187697081573\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                        "00000010 8e e5 61 2f\tQOLYXWC\t3\t-1751905058\tfalse\tV\t0.897795794206\t0.1897\t262\t2015-06-14T03:59:52.156Z\tPZIM\t8231256356538221412\t1970-01-01T02:30:00.000000Z\t13\t\tXFSUWPNXH\n" +
                        "3\t161592763\ttrue\tZ\t0.187697081573\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                        "00000010 8e e5 61 2f\tQOLYXWC\t3\t1159512064\ttrue\tH\t0.812430684497\t0.0033\t432\t2015-09-12T17:45:31.519Z\tPZIM\t7964539812331152681\t1970-01-01T02:13:20.000000Z\t8\t\tWLEVMLKC\n" +
                        "4\t-1172180184\tfalse\tS\t0.589121648388\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t-267213623\ttrue\tG\t0.522178146784\t0.6246\t263\t2015-07-07T21:30:05.180Z\tNZZR\t6868735889622839219\t1970-01-01T04:10:00.000000Z\t31\t00000000 78 09 1c 5d 88 f5 52 fd 36 02 50\t\n" +
                        "4\t-1172180184\tfalse\tS\t0.589121648388\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t-2099411412\ttrue\t\tNaN\tNaN\t119\t2015-09-08T05:51:33.432Z\tMFMB\t8196152051414471878\t1970-01-01T03:53:20.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n" +
                        "4\t-1172180184\tfalse\tS\t0.589121648388\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t-682294338\ttrue\tG\t0.915304483996\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T03:36:40.000000Z\t38\t\tBOSEPGIUQZHEISQH\n" +
                        "4\t-1172180184\tfalse\tS\t0.589121648388\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\t4\t815018557\tfalse\t\t0.073834641749\t0.8791\t187\t\tMFMB\t8725895078168602870\t1970-01-01T03:20:00.000000Z\t36\t\tVLOMPBETTTKRIV\n" +
                        "5\t-2088317486\tfalse\tU\t0.744600037109\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t77821642\tfalse\tG\t0.221227479480\t0.4873\t322\t2015-10-22T18:19:01.452Z\tNZZR\t-4117907293110263427\t1970-01-01T05:16:40.000000Z\t28\t00000000 25 42 67 78 47 b3 80 69 b9 14 d6 fc ee 03 22 81\n" +
                        "00000010 b8 06\tQSPZPBHLNEJ\n" +
                        "5\t-2088317486\tfalse\tU\t0.744600037109\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t-958065826\ttrue\tU\t0.344821709198\t0.5708\t1001\t2015-11-04T17:03:03.434Z\tPZIM\t-2022828060719876991\t1970-01-01T05:00:00.000000Z\t42\t00000000 22 35 3b 1c 9c 1d 5c c1 5d 2d 44 ea 00 81 c4 19\n" +
                        "00000010 a1 ec 74 f8\tIFDYPDKOEZBRQ\n" +
                        "5\t-2088317486\tfalse\tU\t0.744600037109\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t1911638855\tfalse\tK\tNaN\t0.3505\t384\t2015-05-09T06:21:47.768Z\t\t-6966377555709737822\t1970-01-01T04:43:20.000000Z\t3\t\t\n" +
                        "5\t-2088317486\tfalse\tU\t0.744600037109\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\t5\t350233248\tfalse\tT\tNaN\tNaN\t542\t2015-10-10T12:23:35.567Z\tMFMB\t7638330131199319038\t1970-01-01T04:26:40.000000Z\t27\t00000000 fd a9 d7 0e 39 5a 28 ed 97 99\tVMKPYV\n";

                compiler.compile("create table x as (select" +
                        " to_int(x) kk, " +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str(1,1,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(5))", bindVariableService);

                compiler.compile("create table y as (select" +
                        " to_int((x-1)/4 + 1) kk," +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str(1,1,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long() j," +
                        " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(20))", bindVariableService);

                // filter is applied to final join result
                assertJoinQuery(expected, "select * from x join y on (kk)");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerDifferentColumnNames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {

                final String expected = "c\ta\tb\td\tcolumn\n" +
                        "1\t120\t6\t50\t44\n" +
                        "1\t120\t6\t0\t-6\n" +
                        "1\t120\t71\t50\t-21\n" +
                        "1\t120\t71\t0\t-71\n" +
                        "1\t120\t42\t50\t8\n" +
                        "1\t120\t42\t0\t-42\n" +
                        "1\t120\t39\t50\t11\n" +
                        "1\t120\t39\t0\t-39\n" +
                        "2\t568\t14\t55\t41\n" +
                        "2\t568\t14\t968\t954\n" +
                        "2\t568\t72\t55\t-17\n" +
                        "2\t568\t72\t968\t896\n" +
                        "2\t568\t16\t55\t39\n" +
                        "2\t568\t16\t968\t952\n" +
                        "2\t568\t48\t55\t7\n" +
                        "2\t568\t48\t968\t920\n" +
                        "3\t333\t16\t305\t289\n" +
                        "3\t333\t16\t964\t948\n" +
                        "3\t333\t12\t305\t293\n" +
                        "3\t333\t12\t964\t952\n" +
                        "3\t333\t81\t305\t224\n" +
                        "3\t333\t81\t964\t883\n" +
                        "3\t333\t3\t305\t302\n" +
                        "3\t333\t3\t964\t961\n" +
                        "4\t371\t67\t104\t37\n" +
                        "4\t371\t67\t171\t104\n" +
                        "4\t371\t74\t104\t30\n" +
                        "4\t371\t74\t171\t97\n" +
                        "4\t371\t5\t104\t99\n" +
                        "4\t371\t5\t171\t166\n" +
                        "4\t371\t97\t104\t7\n" +
                        "4\t371\t97\t171\t74\n" +
                        "5\t251\t7\t198\t191\n" +
                        "5\t251\t7\t279\t272\n" +
                        "5\t251\t97\t198\t101\n" +
                        "5\t251\t97\t279\t182\n" +
                        "5\t251\t44\t198\t154\n" +
                        "5\t251\t44\t279\t235\n" +
                        "5\t251\t47\t198\t151\n" +
                        "5\t251\t47\t279\t232\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select to_int((x-1)/4 + 1) m, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select to_int((x-1)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(40))", bindVariableService);

                assertJoinQuery(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerInnerFilter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "c\ta\tb\td\tcolumn\n" +
                        "1\t120\t6\t50\t44\n" +
                        "1\t120\t6\t0\t-6\n" +
                        "2\t568\t14\t55\t41\n" +
                        "2\t568\t14\t968\t954\n" +
                        "2\t568\t16\t55\t39\n" +
                        "2\t568\t16\t968\t952\n" +
                        "3\t333\t16\t305\t289\n" +
                        "3\t333\t16\t964\t948\n" +
                        "3\t333\t12\t305\t293\n" +
                        "3\t333\t12\t964\t952\n" +
                        "3\t333\t3\t305\t302\n" +
                        "3\t333\t3\t964\t961\n" +
                        "4\t371\t5\t104\t99\n" +
                        "4\t371\t5\t171\t166\n" +
                        "5\t251\t7\t198\t191\n" +
                        "5\t251\t7\t279\t272\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select to_int((x-1)/4 + 1) m, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select to_int((x-1)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(16))", bindVariableService);

                // filter is applied to intermediate join result
                assertJoinQuery(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20");

                compiler.compile("insert into x select to_int(x+6) c, abs(rnd_int() % 650) a from long_sequence(3)", bindVariableService);
                compiler.compile("insert into y select to_int((x+19)/4 + 1) m, abs(rnd_int() % 100) b from long_sequence(16)", bindVariableService);
                compiler.compile("insert into z select to_int((x+15)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(2)", bindVariableService);

                assertJoinQuery(expected +
                                "7\t253\t14\t723\t709\n" +
                                "7\t253\t14\t228\t214\n" +
                                "8\t431\t0\t790\t790\n" +
                                "8\t431\t0\t348\t348\n" +
                                "9\t100\t8\t456\t448\n" +
                                "9\t100\t8\t667\t659\n" +
                                "9\t100\t19\t456\t437\n" +
                                "9\t100\t19\t667\t648\n",
                        "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where y.b < 20");

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerLastFilter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "c\ta\tb\td\tcolumn\n" +
                        "2\t568\t14\t968\t954\n" +
                        "2\t568\t72\t968\t896\n" +
                        "2\t568\t16\t968\t952\n" +
                        "2\t568\t48\t968\t920\n" +
                        "3\t333\t16\t305\t289\n" +
                        "3\t333\t16\t964\t948\n" +
                        "3\t333\t12\t305\t293\n" +
                        "3\t333\t12\t964\t952\n" +
                        "3\t333\t81\t305\t224\n" +
                        "3\t333\t81\t964\t883\n" +
                        "3\t333\t3\t305\t302\n" +
                        "3\t333\t3\t964\t961\n" +
                        "4\t371\t67\t171\t104\n" +
                        "4\t371\t5\t171\t166\n" +
                        "5\t251\t7\t198\t191\n" +
                        "5\t251\t7\t279\t272\n" +
                        "5\t251\t97\t198\t101\n" +
                        "5\t251\t97\t279\t182\n" +
                        "5\t251\t44\t198\t154\n" +
                        "5\t251\t44\t279\t235\n" +
                        "5\t251\t47\t198\t151\n" +
                        "5\t251\t47\t279\t232\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select to_int((x-1)/4 + 1) m, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select to_int((x-1)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(40))", bindVariableService);

                // filter is applied to final join result
                assertJoinQuery(expected, "select z.c, x.a, b, d, d-b from x join y on y.m = x.c join z on (c) where d-b > 100");
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerNoSlaveRecords() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "c\ta\tb\n" +
                        "2\t568\t72\n" +
                        "2\t568\t16\n" +
                        "4\t371\t3\n" +
                        "4\t371\t14\n" +
                        "6\t439\t12\n" +
                        "6\t439\t81\n" +
                        "8\t521\t97\n" +
                        "8\t521\t16\n" +
                        "10\t598\t74\n" +
                        "10\t598\t5\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(10))", bindVariableService);
                compiler.compile("create table y as (select x, to_int(2*((x-1)/2))+2 m, abs(rnd_int() % 100) b from long_sequence(10))", bindVariableService);

                // master records should be filtered out because slave records missing
                assertJoinQuery(expected, "select x.c, x.a, b from x join y on y.m = x.c");

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerOnSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "xc\tzc\tyc\ta\tb\td\tcolumn\n" +
                        "A\tA\tA\t568\t54\t263\t209\n" +
                        "A\tA\tA\t568\t54\t456\t402\n" +
                        "A\tA\tA\t568\t54\t319\t265\n" +
                        "A\tA\tA\t568\t71\t263\t192\n" +
                        "A\tA\tA\t568\t71\t456\t385\n" +
                        "A\tA\tA\t568\t71\t319\t248\n" +
                        "A\tA\tA\t568\t74\t263\t189\n" +
                        "A\tA\tA\t568\t74\t456\t382\n" +
                        "A\tA\tA\t568\t74\t319\t245\n" +
                        "A\tA\tA\t568\t12\t263\t251\n" +
                        "A\tA\tA\t568\t12\t456\t444\n" +
                        "A\tA\tA\t568\t12\t319\t307\n" +
                        "B\tB\tB\t371\t79\t467\t388\n" +
                        "B\tB\tB\t371\t79\t667\t588\n" +
                        "B\tB\tB\t371\t79\t933\t854\n" +
                        "B\tB\tB\t371\t79\t703\t624\n" +
                        "B\tB\tB\t371\t79\t842\t763\n" +
                        "B\tB\tB\t371\t97\t467\t370\n" +
                        "B\tB\tB\t371\t97\t667\t570\n" +
                        "B\tB\tB\t371\t97\t933\t836\n" +
                        "B\tB\tB\t371\t97\t703\t606\n" +
                        "B\tB\tB\t371\t97\t842\t745\n" +
                        "B\tB\tB\t371\t97\t467\t370\n" +
                        "B\tB\tB\t371\t97\t667\t570\n" +
                        "B\tB\tB\t371\t97\t933\t836\n" +
                        "B\tB\tB\t371\t97\t703\t606\n" +
                        "B\tB\tB\t371\t97\t842\t745\n" +
                        "B\tB\tB\t371\t72\t467\t395\n" +
                        "B\tB\tB\t371\t72\t667\t595\n" +
                        "B\tB\tB\t371\t72\t933\t861\n" +
                        "B\tB\tB\t371\t72\t703\t631\n" +
                        "B\tB\tB\t371\t72\t842\t770\n" +
                        "B\tB\tB\t439\t79\t467\t388\n" +
                        "B\tB\tB\t439\t79\t667\t588\n" +
                        "B\tB\tB\t439\t79\t933\t854\n" +
                        "B\tB\tB\t439\t79\t703\t624\n" +
                        "B\tB\tB\t439\t79\t842\t763\n" +
                        "B\tB\tB\t439\t97\t467\t370\n" +
                        "B\tB\tB\t439\t97\t667\t570\n" +
                        "B\tB\tB\t439\t97\t933\t836\n" +
                        "B\tB\tB\t439\t97\t703\t606\n" +
                        "B\tB\tB\t439\t97\t842\t745\n" +
                        "B\tB\tB\t439\t97\t467\t370\n" +
                        "B\tB\tB\t439\t97\t667\t570\n" +
                        "B\tB\tB\t439\t97\t933\t836\n" +
                        "B\tB\tB\t439\t97\t703\t606\n" +
                        "B\tB\tB\t439\t97\t842\t745\n" +
                        "B\tB\tB\t439\t72\t467\t395\n" +
                        "B\tB\tB\t439\t72\t667\t595\n" +
                        "B\tB\tB\t439\t72\t933\t861\n" +
                        "B\tB\tB\t439\t72\t703\t631\n" +
                        "B\tB\tB\t439\t72\t842\t770\n" +
                        "\t\t\t521\t53\t908\t855\n" +
                        "\t\t\t521\t53\t540\t487\n" +
                        "\t\t\t521\t53\t2\t-51\n" +
                        "\t\t\t521\t53\t8\t-45\n" +
                        "\t\t\t521\t69\t908\t839\n" +
                        "\t\t\t521\t69\t540\t471\n" +
                        "\t\t\t521\t69\t2\t-67\n" +
                        "\t\t\t521\t69\t8\t-61\n" +
                        "\t\t\t521\t68\t908\t840\n" +
                        "\t\t\t521\t68\t540\t472\n" +
                        "\t\t\t521\t68\t2\t-66\n" +
                        "\t\t\t521\t68\t8\t-60\n" +
                        "\t\t\t521\t3\t908\t905\n" +
                        "\t\t\t521\t3\t540\t537\n" +
                        "\t\t\t521\t3\t2\t-1\n" +
                        "\t\t\t521\t3\t8\t5\n" +
                        "\t\t\t598\t53\t908\t855\n" +
                        "\t\t\t598\t53\t540\t487\n" +
                        "\t\t\t598\t53\t2\t-51\n" +
                        "\t\t\t598\t53\t8\t-45\n" +
                        "\t\t\t598\t69\t908\t839\n" +
                        "\t\t\t598\t69\t540\t471\n" +
                        "\t\t\t598\t69\t2\t-67\n" +
                        "\t\t\t598\t69\t8\t-61\n" +
                        "\t\t\t598\t68\t908\t840\n" +
                        "\t\t\t598\t68\t540\t472\n" +
                        "\t\t\t598\t68\t2\t-66\n" +
                        "\t\t\t598\t68\t8\t-60\n" +
                        "\t\t\t598\t3\t908\t905\n" +
                        "\t\t\t598\t3\t540\t537\n" +
                        "\t\t\t598\t3\t2\t-1\n" +
                        "\t\t\t598\t3\t8\t5\n";

                compiler.compile("create table x as (select rnd_symbol('A','B',null,'D') c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select rnd_symbol('B','A',null,'D') m, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select rnd_symbol('D','B',null,'A') c, abs(rnd_int() % 1000) d from long_sequence(16))", bindVariableService);

                // filter is applied to intermediate join result
                assertJoinQuery(expected, "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)");

                compiler.compile("insert into x select rnd_symbol('L','K','P') c, abs(rnd_int() % 650) a from long_sequence(3)", bindVariableService);
                compiler.compile("insert into y select rnd_symbol('P','L','K') m, abs(rnd_int() % 100) b from long_sequence(6)", bindVariableService);
                compiler.compile("insert into z select rnd_symbol('K','P','L') c, abs(rnd_int() % 1000) d from long_sequence(6)", bindVariableService);

                assertJoinQuery(expected +
                                "L\tL\tL\t148\t52\t121\t69\n" +
                                "L\tL\tL\t148\t38\t121\t83\n",
                        "select x.c xc, z.c zc, y.m yc, x.a, b, d, d-b from x join y on y.m = x.c join z on (c)");

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testJoinInnerPostJoinFilter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                final String expected = "c\ta\tb\td\tcolumn\n" +
                        "1\t120\t6\t50\t126\n" +
                        "1\t120\t6\t0\t126\n" +
                        "1\t120\t71\t50\t191\n" +
                        "1\t120\t71\t0\t191\n" +
                        "1\t120\t42\t50\t162\n" +
                        "1\t120\t42\t0\t162\n" +
                        "1\t120\t39\t50\t159\n" +
                        "1\t120\t39\t0\t159\n" +
                        "5\t251\t7\t198\t258\n" +
                        "5\t251\t7\t279\t258\n" +
                        "5\t251\t44\t198\t295\n" +
                        "5\t251\t44\t279\t295\n" +
                        "5\t251\t47\t198\t298\n" +
                        "5\t251\t47\t279\t298\n";

                compiler.compile("create table x as (select to_int(x) c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select to_int((x-1)/4 + 1) m, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select to_int((x-1)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(16))", bindVariableService);

                // filter is applied to intermediate join result
                assertJoinQuery(expected, "select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300");

                compiler.compile("insert into x select to_int(x+6) c, abs(rnd_int() % 650) a from long_sequence(3)", bindVariableService);
                compiler.compile("insert into y select to_int((x+19)/4 + 1) m, abs(rnd_int() % 100) b from long_sequence(16)", bindVariableService);
                compiler.compile("insert into z select to_int((x+15)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(2)", bindVariableService);

                assertJoinQuery(expected +
                                "7\t253\t14\t723\t267\n" +
                                "7\t253\t14\t228\t267\n" +
                                "7\t253\t35\t723\t288\n" +
                                "7\t253\t35\t228\t288\n" +
                                "9\t100\t8\t456\t108\n" +
                                "9\t100\t8\t667\t108\n" +
                                "9\t100\t38\t456\t138\n" +
                                "9\t100\t38\t667\t138\n" +
                                "9\t100\t19\t456\t119\n" +
                                "9\t100\t19\t667\t119\n" +
                                "9\t100\t63\t456\t163\n" +
                                "9\t100\t63\t667\t163\n",
                        "select z.c, x.a, b, d, a+b from x join y on y.m = x.c join z on (c) where a+b < 300");

                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    @Test
    public void testTypeMismatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile("create table x as (select x c, abs(rnd_int() % 650) a from long_sequence(5))", bindVariableService);
                compiler.compile("create table y as (select to_int((x-1)/4 + 1) c, abs(rnd_int() % 100) b from long_sequence(20))", bindVariableService);
                compiler.compile("create table z as (select to_int((x-1)/2 + 1) c, abs(rnd_int() % 1000) d from long_sequence(40))", bindVariableService);

                try {
                    compiler.compile("select z.c, x.a, b, d, d-b from x join y on(c) join z on (c)", bindVariableService);
                    Assert.fail();
                } catch (SqlException e) {
                    Assert.assertEquals(44, e.getPosition());
                }
                Assert.assertEquals(0, engine.getBusyReaderCount());
                Assert.assertEquals(0, engine.getBusyWriterCount());
            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }

    private void assertJoinQuery(String expected, String query) throws IOException, SqlException {
        try (final RecordCursorFactory factory = compiler.compile(query, bindVariableService)) {
            Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
            assertCursor(expected, factory, false);
            // make sure we get the same outcome when we get factory to create new cursor
            assertCursor(expected, factory, false);
            // make sure strings, binary fields and symbols are compliant with expected record behaviour
            assertVariableColumns(factory);
        }
    }
}
