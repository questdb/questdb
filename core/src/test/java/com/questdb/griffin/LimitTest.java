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

import com.questdb.griffin.engine.functions.rnd.SharedRandom;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class LimitTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBottomRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "25\tmsft\t0.918000000000\t2018-01-01T00:50:00.000000Z\tfalse\t\t0.326136520120\t0.3394\t176\t2015-02-06T18:42:24.631Z\t\t-8352023907145286323\t1970-01-01T06:40:00.000000Z\t14\t00000000 6a 9b cd bb 2e 74 cd 44 54 13 3f ff\tIGENFEL\n" +
                "26\tibm\t0.330000000000\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.198236477005\tNaN\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                "00000010 42 71 a3 7a\tDNZNLCNGZTOY\n" +
                "27\tmsft\t0.673000000000\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712011\t0.0237\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t\n" +
                "28\tgoogl\t0.173000000000\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "55\tibm\t0.213000000000\t2018-01-01T01:50:00.000000Z\tfalse\tKZZ\tNaN\t0.0379\t503\t2015-08-25T16:59:46.151Z\tKKUS\t8510474930626176160\t1970-01-01T06:40:00.000000Z\t13\t\t\n" +
                "56\tmsft\t0.061000000000\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.779251143760\t0.3966\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX\n" +
                "57\tgoogl\t0.756000000000\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.892572303318\t0.9925\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f\n" +
                "00000010 61 2d\tEYDNMIOCCVV\n" +
                "58\tibm\t0.445000000000\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.761311594585\tNaN\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB\n";

        String query = "select * from y limit -6,-2";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testLastN() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "26\tibm\t0.330000000000\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.198236477005\tNaN\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                "00000010 42 71 a3 7a\tDNZNLCNGZTOY\n" +
                "27\tmsft\t0.673000000000\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712011\t0.0237\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t\n" +
                "28\tgoogl\t0.173000000000\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n" +
                "29\tibm\t0.240000000000\t2018-01-01T00:58:00.000000Z\tfalse\t\tNaN\t0.5505\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e\n" +
                "00000010 23 03 dd\t\n" +
                "30\tibm\t0.607000000000\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.478083025671\t0.0109\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "56\tmsft\t0.061000000000\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.779251143760\t0.3966\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX\n" +
                "57\tgoogl\t0.756000000000\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.892572303318\t0.9925\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f\n" +
                "00000010 61 2d\tEYDNMIOCCVV\n" +
                "58\tibm\t0.445000000000\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.761311594585\tNaN\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB\n" +
                "59\tgoogl\t0.778000000000\t2018-01-01T01:58:00.000000Z\tfalse\tKZZ\t0.774180142253\t0.1870\t586\t2015-05-27T15:12:16.295Z\t\t-7715437488835448247\t1970-01-01T07:46:40.000000Z\t10\t\tEPLWDUWIWJTLCP\n" +
                "60\tgoogl\t0.852000000000\t2018-01-01T02:00:00.000000Z\ttrue\tKZZ\tNaN\tNaN\t834\t2015-07-15T04:34:51.645Z\tLMSR\t-4834150290387342806\t1970-01-01T08:03:20.000000Z\t23\t00000000 dd 02 98 ad a8 82 73 a6 7f db d6 20\tFDRPHNGTNJJPT\n";

        String query = "select * from y limit -5";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testTopN() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t0.509000000000\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.524372285929\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t0.423000000000\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.529840594176\tNaN\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t0.174000000000\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.882822836670\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                "4\tibm\t0.148000000000\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.345689799154\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                "5\tgoogl\t0.868000000000\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.427470428635\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n";

        String query = "select * from y limit 5";

        testLimit(expected, expected, query);
    }

    @Test
    public void testTopRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "4\tibm\t0.148000000000\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.345689799154\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                "5\tgoogl\t0.868000000000\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.427470428635\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                "6\tmsft\t0.297000000000\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.267212048922\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n";

        String query = "select * from y limit 4,6";
        testLimit(expected, expected, query);
    }

    private void testLimit(String expected1, String expected2, String query) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "create table y as (" +
                                "select" +
                                " to_int(x) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym2," +
                                " round(rnd_double(0), 3) price," +
                                " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp," +
                                " rnd_boolean() b," +
                                " rnd_str(1,1,2) c," +
                                " rnd_double(2) d," +
                                " rnd_float(2) e," +
                                " rnd_short(10,1024) f," +
                                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                " rnd_symbol(4,4,4,2) ik," +
                                " rnd_long() j," +
                                " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                                " rnd_byte(2,50) l," +
                                " rnd_bin(10, 20, 2) m," +
                                " rnd_str(5,16,2) n" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)"
                        , bindVariableService
                );
                assertQueryAndCache(expected1, query, "timestamp", true);

                compiler.compile(
                        "insert into y select * from " +
                                "(select" +
                                " to_int(x + 30) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym2," +
                                " round(rnd_double(0), 3) price," +
                                " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp," +
                                " rnd_boolean() b," +
                                " rnd_str('ABC', 'CDE', null, 'KZZ') c," +
                                " rnd_double(2) d," +
                                " rnd_float(2) e," +
                                " rnd_short(10,1024) f," +
                                " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                                " rnd_symbol(4,4,4,2) ik," +
                                " rnd_long() j," +
                                " timestamp_sequence(to_timestamp(0), 1000000000) k," +
                                " rnd_byte(2,50) l," +
                                " rnd_bin(10, 20, 2) m," +
                                " rnd_str(5,16,2) n" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)"
                        , bindVariableService
                );

                assertQuery(expected2, query, "timestamp", true);

            } finally {
                engine.releaseAllWriters();
                engine.releaseAllReaders();
            }
        });
    }
}
