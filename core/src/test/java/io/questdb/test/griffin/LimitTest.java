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

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LimitTest extends AbstractCairoTest {

    @Test
    public void testBottomRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "25\tmsft\t0.918\t2018-01-01T00:50:00.000000Z\tfalse\t\t0.32613652012030125\t0.3394\t176\t2015-02-06T18:42:24.631Z\t\t-8352023907145286323\t1970-01-01T06:40:00.000000Z\t14\t00000000 6a 9b cd bb 2e 74 cd 44 54 13 3f ff\tIGENFEL\n" +
                "26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                "00000010 42 71 a3 7a\tDNZNLCNGZTOY\n" +
                "27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.0237\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t\n" +
                "28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "55\tibm\t0.213\t2018-01-01T01:50:00.000000Z\tfalse\tKZZ\tnull\t0.0379\t503\t2015-08-25T16:59:46.151Z\tKKUS\t8510474930626176160\t1970-01-01T06:40:00.000000Z\t13\t\t\n" +
                "56\tmsft\t0.061\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.7792511437604662\t0.3966\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX\n" +
                "57\tgoogl\t0.756\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.8925723033175609\t0.9925\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f\n" +
                "00000010 61 2d\tEYDNMIOCCVV\n" +
                "58\tibm\t0.445\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.7613115945849444\tnull\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB\n";

        String query = "select * from y limit -6,-2";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testBottomRangeNotEnoughRows() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n" +
                "29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.5505\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e\n" +
                "00000010 23 03 dd\t\n";

        String query = "select * from y limit -33,-31";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testBottomRangePartialRows() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                "00000010 42 71 a3 7a\tDNZNLCNGZTOY\n" +
                "27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.0237\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t\n" +
                "28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n" +
                "29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.5505\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e\n" +
                "00000010 23 03 dd\t\n" +
                "30\tibm\t0.607\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.47808302567081573\t0.0109\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD\n" +
                "31\tibm\t0.181\t2018-01-01T01:02:00.000000Z\tfalse\tCDE\t0.5455175324785665\t0.8249\t905\t2015-03-02T13:31:56.918Z\tLVSY\t-4086246124104754347\t1970-01-01T00:00:00.000000Z\t26\t00000000 4b c0 d9 1c 71 cf 5a 8f 21 06 b2 3f\t\n" +
                "32\tibm\t0.47300000000000003\t2018-01-01T01:04:00.000000Z\tfalse\tABC\t0.0396096812427591\t0.0783\t42\t2015-05-21T22:28:44.644Z\tKKUS\t2296041148680180183\t1970-01-01T00:16:40.000000Z\t35\t\tSVCLLERSMKRZ\n";

        String query = "select * from y limit -35,-28";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testDefaultHi() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n";

        String query = "select * from y limit 5,";
        testLimit(expected, expected, query);
    }

    @Test
    public void testDefaultLo() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n";

        String query = "select * from y limit ,5";
        testLimit(expected, expected, query);
    }

    @Test
    public void testInvalidBottomRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n";

        String query = "select * from y limit -3,-10";
        testLimit(expected, expected, query);
    }

    @Test
    public void testInvalidHiType() throws Exception {
        assertException(
                "select * from y limit 5,'ab'",
                "create table y as (" +
                        "select" +
                        " cast(x as int) i," +
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
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(30)" +
                        ") timestamp(timestamp)",
                24,
                "invalid type: STRING"
        );
    }

    @Test
    public void testInvalidLoType() throws Exception {
        assertException(
                "select * from y limit 5 + 0.3",
                "create table y as (" +
                        "select" +
                        " cast(x as int) i," +
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
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(30)" +
                        ") timestamp(timestamp)",
                24,
                "invalid type: DOUBLE"
        );
    }

    @Test
    public void testInvalidNegativeLimitJitDisabled() throws Exception {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        testInvalidNegativeLimit();
    }

    @Test
    public void testInvalidNegativeLimitJitEnabled() throws Exception {
        testInvalidNegativeLimit();
    }

    @Test
    public void testInvalidTopRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n";

        String query = "select * from y limit 6,5";
        testLimit(expected, expected, query);
    }

    @Test
    public void testLastN() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                "00000010 42 71 a3 7a\tDNZNLCNGZTOY\n" +
                "27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.0237\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t\n" +
                "28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n" +
                "29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.5505\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e\n" +
                "00000010 23 03 dd\t\n" +
                "30\tibm\t0.607\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.47808302567081573\t0.0109\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "56\tmsft\t0.061\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.7792511437604662\t0.3966\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX\n" +
                "57\tgoogl\t0.756\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.8925723033175609\t0.9925\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f\n" +
                "00000010 61 2d\tEYDNMIOCCVV\n" +
                "58\tibm\t0.445\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.7613115945849444\tnull\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB\n" +
                "59\tgoogl\t0.778\t2018-01-01T01:58:00.000000Z\tfalse\tKZZ\t0.7741801422529707\t0.1870\t586\t2015-05-27T15:12:16.295Z\t\t-7715437488835448247\t1970-01-01T07:46:40.000000Z\t10\t\tEPLWDUWIWJTLCP\n" +
                "60\tgoogl\t0.852\t2018-01-01T02:00:00.000000Z\ttrue\tKZZ\tnull\tnull\t834\t2015-07-15T04:34:51.645Z\tLMSR\t-4834150290387342806\t1970-01-01T08:03:20.000000Z\t23\t00000000 dd 02 98 ad a8 82 73 a6 7f db d6 20\tFDRPHNGTNJJPT\n";

        String query = "select * from y limit -5";
        testLimit(expected, expected2, query, true);
    }

    @Test
    public void testLimitMinusOneAndPredicateAndColumnAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, id symbol)");
            execute("insert into t1 values (0, 'abc'), (2, 'a1'), (3, 'abc'), (4, 'abc'), (5, 'a2')");
            assertQueryAndCache(
                    "ts\tid\n" +
                            "1970-01-01T00:00:00.000004Z\tabc\n",
                    "select ts, id as id from t1 where id = 'abc' limit -1",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLimitMinusOneJitDisabled() throws Exception {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        testLimitMinusOne();
    }

    @Test
    public void testLimitMinusOneJitEnabled() throws Exception {
        testLimitMinusOne();
    }

    @Test
    public void testNegativeLimitEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table y (sym symbol, ts timestamp) timestamp(ts) partition by day");

            assertQueryNoLeakCheck(
                    "sym\tts\n",
                    "y where sym = 'googl' limit -3",
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testNegativeLimitMultiplePageFramesNonPartitioned() throws Exception {
        // Here we verify that the implicit timestamp descending order is preserved
        // by negative limit clause even if it spans multiple page frames.

        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        final int N = 64 * 5;

        assertMemoryLeak(() -> {
            execute("create table y as (" +
                    "select" +
                    " cast(x as int) i," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                    " from long_sequence(" + N + ")" +
                    ") timestamp(timestamp)");

            String query = "select * from y where i % 64 = 1 limit -1";
            String expected = "i\ttimestamp\n" +
                    "257\t2018-01-01T08:34:00.000000Z\n";
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 = 1 limit -2";
            expected = "i\ttimestamp\n" +
                    "193\t2018-01-01T06:26:00.000000Z\n" +
                    "257\t2018-01-01T08:34:00.000000Z\n";
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 < 3 limit -5";
            expected = "i\ttimestamp\n" +
                    "194\t2018-01-01T06:28:00.000000Z\n" +
                    "256\t2018-01-01T08:32:00.000000Z\n" +
                    "257\t2018-01-01T08:34:00.000000Z\n" +
                    "258\t2018-01-01T08:36:00.000000Z\n" +
                    "320\t2018-01-01T10:40:00.000000Z\n";
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testNegativeLimitMultiplePageFramesPartitioned() throws Exception {
        // Here we verify that the implicit timestamp descending order is preserved
        // by negative limit clause even if it spans multiple page frames.

        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 64);
        final int N = 64 * 5;

        assertMemoryLeak(() -> {
            execute("create table y as (" +
                    "select" +
                    " cast(x as int) i," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                    " from long_sequence(" + N + ")" +
                    ") timestamp(timestamp) partition by hour");

            String query = "select * from y where i % 64 = 1 limit -1";
            String expected = "i\ttimestamp\n" +
                    "257\t2018-01-01T08:34:00.000000Z\n";
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 = 1 limit -2";
            expected = "i\ttimestamp\n" +
                    "193\t2018-01-01T06:26:00.000000Z\n" +
                    "257\t2018-01-01T08:34:00.000000Z\n";
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 < 3 limit -5";
            expected = "i\ttimestamp\n" +
                    "194\t2018-01-01T06:28:00.000000Z\n" +
                    "256\t2018-01-01T08:32:00.000000Z\n" +
                    "257\t2018-01-01T08:34:00.000000Z\n" +
                    "258\t2018-01-01T08:36:00.000000Z\n" +
                    "320\t2018-01-01T10:40:00.000000Z\n";
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testNegativeLimitOnIndexedSymbolFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
                            " rnd_symbol('msft','ibm', 'googl') sym," +
                            " round(rnd_double(0), 3) price," +
                            " cast(x * 120000000 as timestamp) timestamp" +
                            " from long_sequence(30)" +
                            "), index(sym) timestamp(timestamp) partition by month"
            );

            execute("insert into y values (-3, 'googl', 1, to_timestamp('2001-01-01', 'yyyy-MM-dd'))");
            execute("insert into y values (-2, 'googl', 2, to_timestamp('2002-01-01', 'yyyy-MM-dd'))");
            execute("insert into y values (-1, 'googl', 3, to_timestamp('2003-01-01', 'yyyy-MM-dd'))");

            assertQueryNoLeakCheck(
                    "i\tsym\tprice\ttimestamp\n" +
                            "-3\tgoogl\t1.0\t2001-01-01T00:00:00.000000Z\n" +
                            "-2\tgoogl\t2.0\t2002-01-01T00:00:00.000000Z\n" +
                            "-1\tgoogl\t3.0\t2003-01-01T00:00:00.000000Z\n",
                    "y where sym = 'googl' limit -3",
                    "timestamp",
                    true,
                    false
            );
        });
    }

    @Test
    public void testRangeVariable() throws Exception {
        final String query = "select * from y limit :lo,:hi";
        assertMemoryLeak(() -> {
            try {
                String expected1 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                        "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                        "6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                        "7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.0658\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ\n" +
                        "8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8688\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ\n";

                String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                        "7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.0658\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ\n" +
                        "8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8688\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ\n" +
                        "9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                        "10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t\n" +
                        "11\tmsft\t0.578\t2018-01-01T00:22:00.000000Z\ttrue\tP\t0.7467013668130107\t0.5795\t122\t2015-11-25T07:36:56.937Z\t\t2004830221820243556\t1970-01-01T02:46:40.000000Z\t45\t00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13\n" +
                        "00000010 5d b2 15 e7\tWGRMDGGIJYDVRV\n" +
                        "12\tmsft\t0.661\t2018-01-01T00:24:00.000000Z\ttrue\tO\t0.01396079545983997\t0.8136\t345\t2015-08-18T10:31:42.373Z\tVTJW\t5045825384817367685\t1970-01-01T03:03:20.000000Z\t23\t00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8\n" +
                        "00000010 43\tJCTIZKYFLUHZ\n";

                execute(
                        "create table y as (" +
                                "select" +
                                " cast(x as int) i," +
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
                                " timestamp_sequence(0, 1000000000) k," +
                                " rnd_byte(2,50) l," +
                                " rnd_bin(10, 20, 2) m," +
                                " rnd_str(5,16,2) n" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)"
                );

                bindVariableService.setLong("lo", 4);
                bindVariableService.setInt("hi", 8);

                assertQueryAndCache(expected1, query, "timestamp", true, false);
                bindVariableService.setLong("lo", 6);
                bindVariableService.setInt("hi", 12);

                assertQueryAndCache(expected2, query, "timestamp", true, false);
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testSelectLast10RecordsInReverseTsOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE intervaltest (\n" +
                    "  id long,\n" +
                    "  ts TIMESTAMP\n" +
                    ") timestamp (ts) PARTITION BY DAY");

            execute("insert into intervaltest \n" +
                    "select x, ('2023-04-06T00:00:00.000000Z'::timestamp::long + (x*1000))::timestamp\n" +
                    "from long_sequence(600000)");

            assertQueryNoLeakCheck(
                    "count\n1000\n",
                    "select count(*)\n" +
                            "from intervaltest\n" +
                            "WHERE ts > '2023-04-06T00:09:59.000000Z'",
                    null,
                    false,
                    true
            );

            String query = "select *\n" +
                    "from intervaltest\n" +
                    "WHERE ts > '2023-04-06T00:09:59.000000Z' \n" +
                    "ORDER BY ts DESC " +
                    "LIMIT 10";

            assertPlanNoLeakCheck(
                    query,
                    "Limit lo: 10\n" +
                            "    PageFrame\n" +
                            "        Row backward scan\n" +
                            "        Interval backward scan on: intervaltest\n" +
                            "          intervals: [(\"2023-04-06T00:09:59.000001Z\",\"MAX\")]\n"
            );

            assertQuery(
                    "id\tts\n" +
                            "600000\t2023-04-06T00:10:00.000000Z\n" +
                            "599999\t2023-04-06T00:09:59.999000Z\n" +
                            "599998\t2023-04-06T00:09:59.998000Z\n" +
                            "599997\t2023-04-06T00:09:59.997000Z\n" +
                            "599996\t2023-04-06T00:09:59.996000Z\n" +
                            "599995\t2023-04-06T00:09:59.995000Z\n" +
                            "599994\t2023-04-06T00:09:59.994000Z\n" +
                            "599993\t2023-04-06T00:09:59.993000Z\n" +
                            "599992\t2023-04-06T00:09:59.992000Z\n" +
                            "599991\t2023-04-06T00:09:59.991000Z\n",
                    query,
                    "ts###DESC",
                    true,
                    false
            );
        });
    }

    @Test
    public void testTopBottomRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                "6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                "7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.0658\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ\n" +
                "8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8688\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ\n" +
                "9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                "10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t\n" +
                "11\tmsft\t0.578\t2018-01-01T00:22:00.000000Z\ttrue\tP\t0.7467013668130107\t0.5795\t122\t2015-11-25T07:36:56.937Z\t\t2004830221820243556\t1970-01-01T02:46:40.000000Z\t45\t00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13\n" +
                "00000010 5d b2 15 e7\tWGRMDGGIJYDVRV\n" +
                "12\tmsft\t0.661\t2018-01-01T00:24:00.000000Z\ttrue\tO\t0.01396079545983997\t0.8136\t345\t2015-08-18T10:31:42.373Z\tVTJW\t5045825384817367685\t1970-01-01T03:03:20.000000Z\t23\t00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8\n" +
                "00000010 43\tJCTIZKYFLUHZ\n" +
                "13\tibm\t0.704\t2018-01-01T00:26:00.000000Z\ttrue\tK\t0.036735155240002815\t0.8406\t742\t2015-05-03T18:49:03.996Z\tPEHN\t2568830294369411037\t1970-01-01T03:20:00.000000Z\t24\t00000000 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2 bf 84 5a\n" +
                "00000010 6f 38 35\t\n" +
                "14\tgoogl\t0.651\t2018-01-01T00:28:00.000000Z\ttrue\tL\tnull\t0.1389\t984\t2015-04-30T08:35:52.508Z\tHYRX\t-6929866925584807039\t1970-01-01T03:36:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\t\n" +
                "15\tmsft\t0.40900000000000003\t2018-01-01T00:30:00.000000Z\tfalse\tW\t0.8034049105590781\t0.0440\t854\t2015-04-10T01:03:15.469Z\t\t-9050844408181442061\t1970-01-01T03:53:20.000000Z\t24\t\tPFYXPV\n" +
                "16\tgoogl\t0.839\t2018-01-01T00:32:00.000000Z\tfalse\tN\t0.11048000399634927\t0.7096\t379\t2015-04-14T08:49:20.021Z\t\t-5749151825415257775\t1970-01-01T04:10:00.000000Z\t16\t\t\n" +
                "17\tibm\t0.28600000000000003\t2018-01-01T00:34:00.000000Z\tfalse\t\t0.1353529674614602\tnull\t595\t\tVTJW\t-6237826420165615015\t1970-01-01T04:26:40.000000Z\t33\t\tBEGMITI\n" +
                "18\tibm\t0.932\t2018-01-01T00:36:00.000000Z\ttrue\tH\t0.88982264111644\t0.7074\t130\t2015-04-09T21:18:23.066Z\tCPSW\t-5708280760166173503\t1970-01-01T04:43:20.000000Z\t27\t00000000 69 94 3f 7d ef 3b b8 be f8 a1 46 87 28 92 a3 9b\n" +
                "00000010 e3 cb\tNIZOSBOSE\n" +
                "19\tibm\t0.151\t2018-01-01T00:38:00.000000Z\ttrue\tH\t0.39201296350741366\t0.5700\t748\t\t\t-8060696376111078264\t1970-01-01T05:00:00.000000Z\t11\t00000000 10 20 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d\n" +
                "00000010 6c a9\tTDCEBYWXBBZV\n" +
                "20\tgoogl\t0.624\t2018-01-01T00:40:00.000000Z\tfalse\tY\t0.5863937813368164\t0.2103\t834\t2015-10-15T00:48:00.413Z\tHYRX\t7913225810447636126\t1970-01-01T05:16:40.000000Z\t12\t00000000 f6 78 09 1c 5d 88 f5 52 fd 36 02 50 d9 a0 b5 90\n" +
                "00000010 6c 9c 23\tILLEYMIWTCWLFORG\n" +
                "21\tmsft\t0.597\t2018-01-01T00:42:00.000000Z\ttrue\tP\t0.6690790546123128\t0.6591\t974\t\tVTJW\t8843532011989881581\t1970-01-01T05:33:20.000000Z\t17\t00000000 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de 46\n" +
                "00000010 04 d3\tZZBBUKOJSOLDYR\n" +
                "22\tgoogl\t0.7020000000000001\t2018-01-01T00:44:00.000000Z\ttrue\tR\t0.7134568516750471\t0.0921\t879\t2015-03-07T18:51:10.265Z\tHYRX\t5447530387277886439\t1970-01-01T05:50:00.000000Z\t2\t\tQSQJGDIHHNSS\n" +
                "23\tibm\t0.5730000000000001\t2018-01-01T00:46:00.000000Z\ttrue\tV\t0.32282028174282695\tnull\t791\t2015-10-07T21:38:49.138Z\tPEHN\t4430387718690044436\t1970-01-01T06:06:40.000000Z\t34\t\tZVQQHSQSPZPB\n" +
                "24\tmsft\t0.23800000000000002\t2018-01-01T00:48:00.000000Z\ttrue\t\t0.47930730718677406\t0.6937\t635\t2015-10-11T00:49:46.817Z\tCPSW\t3860877990849202595\t1970-01-01T06:23:20.000000Z\t14\t\tPZNYVLTPKBBQ\n";

        String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                "6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                "7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.0658\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ\n" +
                "8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8688\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ\n" +
                "9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                "10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t\n" +
                "11\tmsft\t0.578\t2018-01-01T00:22:00.000000Z\ttrue\tP\t0.7467013668130107\t0.5795\t122\t2015-11-25T07:36:56.937Z\t\t2004830221820243556\t1970-01-01T02:46:40.000000Z\t45\t00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13\n" +
                "00000010 5d b2 15 e7\tWGRMDGGIJYDVRV\n" +
                "12\tmsft\t0.661\t2018-01-01T00:24:00.000000Z\ttrue\tO\t0.01396079545983997\t0.8136\t345\t2015-08-18T10:31:42.373Z\tVTJW\t5045825384817367685\t1970-01-01T03:03:20.000000Z\t23\t00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8\n" +
                "00000010 43\tJCTIZKYFLUHZ\n" +
                "13\tibm\t0.704\t2018-01-01T00:26:00.000000Z\ttrue\tK\t0.036735155240002815\t0.8406\t742\t2015-05-03T18:49:03.996Z\tPEHN\t2568830294369411037\t1970-01-01T03:20:00.000000Z\t24\t00000000 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2 bf 84 5a\n" +
                "00000010 6f 38 35\t\n" +
                "14\tgoogl\t0.651\t2018-01-01T00:28:00.000000Z\ttrue\tL\tnull\t0.1389\t984\t2015-04-30T08:35:52.508Z\tHYRX\t-6929866925584807039\t1970-01-01T03:36:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\t\n" +
                "15\tmsft\t0.40900000000000003\t2018-01-01T00:30:00.000000Z\tfalse\tW\t0.8034049105590781\t0.0440\t854\t2015-04-10T01:03:15.469Z\t\t-9050844408181442061\t1970-01-01T03:53:20.000000Z\t24\t\tPFYXPV\n" +
                "16\tgoogl\t0.839\t2018-01-01T00:32:00.000000Z\tfalse\tN\t0.11048000399634927\t0.7096\t379\t2015-04-14T08:49:20.021Z\t\t-5749151825415257775\t1970-01-01T04:10:00.000000Z\t16\t\t\n" +
                "17\tibm\t0.28600000000000003\t2018-01-01T00:34:00.000000Z\tfalse\t\t0.1353529674614602\tnull\t595\t\tVTJW\t-6237826420165615015\t1970-01-01T04:26:40.000000Z\t33\t\tBEGMITI\n" +
                "18\tibm\t0.932\t2018-01-01T00:36:00.000000Z\ttrue\tH\t0.88982264111644\t0.7074\t130\t2015-04-09T21:18:23.066Z\tCPSW\t-5708280760166173503\t1970-01-01T04:43:20.000000Z\t27\t00000000 69 94 3f 7d ef 3b b8 be f8 a1 46 87 28 92 a3 9b\n" +
                "00000010 e3 cb\tNIZOSBOSE\n" +
                "19\tibm\t0.151\t2018-01-01T00:38:00.000000Z\ttrue\tH\t0.39201296350741366\t0.5700\t748\t\t\t-8060696376111078264\t1970-01-01T05:00:00.000000Z\t11\t00000000 10 20 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d\n" +
                "00000010 6c a9\tTDCEBYWXBBZV\n" +
                "20\tgoogl\t0.624\t2018-01-01T00:40:00.000000Z\tfalse\tY\t0.5863937813368164\t0.2103\t834\t2015-10-15T00:48:00.413Z\tHYRX\t7913225810447636126\t1970-01-01T05:16:40.000000Z\t12\t00000000 f6 78 09 1c 5d 88 f5 52 fd 36 02 50 d9 a0 b5 90\n" +
                "00000010 6c 9c 23\tILLEYMIWTCWLFORG\n" +
                "21\tmsft\t0.597\t2018-01-01T00:42:00.000000Z\ttrue\tP\t0.6690790546123128\t0.6591\t974\t\tVTJW\t8843532011989881581\t1970-01-01T05:33:20.000000Z\t17\t00000000 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de 46\n" +
                "00000010 04 d3\tZZBBUKOJSOLDYR\n" +
                "22\tgoogl\t0.7020000000000001\t2018-01-01T00:44:00.000000Z\ttrue\tR\t0.7134568516750471\t0.0921\t879\t2015-03-07T18:51:10.265Z\tHYRX\t5447530387277886439\t1970-01-01T05:50:00.000000Z\t2\t\tQSQJGDIHHNSS\n" +
                "23\tibm\t0.5730000000000001\t2018-01-01T00:46:00.000000Z\ttrue\tV\t0.32282028174282695\tnull\t791\t2015-10-07T21:38:49.138Z\tPEHN\t4430387718690044436\t1970-01-01T06:06:40.000000Z\t34\t\tZVQQHSQSPZPB\n" +
                "24\tmsft\t0.23800000000000002\t2018-01-01T00:48:00.000000Z\ttrue\t\t0.47930730718677406\t0.6937\t635\t2015-10-11T00:49:46.817Z\tCPSW\t3860877990849202595\t1970-01-01T06:23:20.000000Z\t14\t\tPZNYVLTPKBBQ\n" +
                "25\tmsft\t0.918\t2018-01-01T00:50:00.000000Z\tfalse\t\t0.32613652012030125\t0.3394\t176\t2015-02-06T18:42:24.631Z\t\t-8352023907145286323\t1970-01-01T06:40:00.000000Z\t14\t00000000 6a 9b cd bb 2e 74 cd 44 54 13 3f ff\tIGENFEL\n" +
                "26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a\n" +
                "00000010 42 71 a3 7a\tDNZNLCNGZTOY\n" +
                "27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.0237\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t\n" +
                "28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX\n" +
                "29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.5505\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e\n" +
                "00000010 23 03 dd\t\n" +
                "30\tibm\t0.607\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.47808302567081573\t0.0109\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD\n" +
                "31\tibm\t0.181\t2018-01-01T01:02:00.000000Z\tfalse\tCDE\t0.5455175324785665\t0.8249\t905\t2015-03-02T13:31:56.918Z\tLVSY\t-4086246124104754347\t1970-01-01T00:00:00.000000Z\t26\t00000000 4b c0 d9 1c 71 cf 5a 8f 21 06 b2 3f\t\n" +
                "32\tibm\t0.47300000000000003\t2018-01-01T01:04:00.000000Z\tfalse\tABC\t0.0396096812427591\t0.0783\t42\t2015-05-21T22:28:44.644Z\tKKUS\t2296041148680180183\t1970-01-01T00:16:40.000000Z\t35\t\tSVCLLERSMKRZ\n" +
                "33\tibm\t0.6\t2018-01-01T01:06:00.000000Z\tfalse\tCDE\t0.11423618345717534\t0.9925\t288\t2015-11-17T19:46:21.874Z\tLVSY\t-5669452046744991419\t1970-01-01T00:33:20.000000Z\t43\t00000000 ef 2d 99 66 3d db c1 cc b8 82 3d ec f3 66 5e 70\n" +
                "00000010 38 5e\tBWWXFQDCQSCMO\n" +
                "34\tmsft\t0.867\t2018-01-01T01:08:00.000000Z\tfalse\tKZZ\t0.7593868458005614\tnull\t177\t2015-01-12T08:29:36.861Z\t\t7660755785254391246\t1970-01-01T00:50:00.000000Z\t22\t00000000 a0 ba a5 d1 63 ca 32 e5 0d 68 52 c6 94 c3 18 c9\n" +
                "00000010 7c\t\n" +
                "35\tgoogl\t0.11\t2018-01-01T01:10:00.000000Z\tfalse\t\t0.3619006645431899\t0.1061\t510\t2015-09-11T19:36:06.058Z\t\t-8165131599894925271\t1970-01-01T01:06:40.000000Z\t4\t\tGZGKCGBZDMGYDEQ\n" +
                "36\tibm\t0.07200000000000001\t2018-01-01T01:12:00.000000Z\ttrue\tABC\t0.12049392901868938\t0.7400\t65\t2015-06-24T11:48:21.316Z\t\t7391407846465678094\t1970-01-01T01:23:20.000000Z\t39\t00000000 bb c3 ec 4b 97 27 df cd 7a 14 07 92 01 f5 6a\t\n" +
                "37\tmsft\t0.73\t2018-01-01T01:14:00.000000Z\ttrue\tABC\t0.2717670505640706\t0.2094\t941\t2015-02-13T23:00:15.193Z\tLVSY\t-4554200170857361590\t1970-01-01T01:40:00.000000Z\t11\t00000000 3f 4e 27 42 f2 f8 5e 29 d3 b9 67 75 95 fa 1f 92\n" +
                "00000010 24 b1\tWYUHNBCCPM\n" +
                "38\tgoogl\t0.59\t2018-01-01T01:16:00.000000Z\ttrue\tABC\t0.1296510741209903\t0.4552\t350\t2015-05-27T12:47:41.516Z\tHGKR\t7992851382615210277\t1970-01-01T01:56:40.000000Z\t26\t\tKUNRDCWNPQYTEW\n" +
                "39\tibm\t0.47700000000000004\t2018-01-01T01:18:00.000000Z\ttrue\t\t0.110401374979613\t0.4204\t701\t2015-02-06T16:19:37.448Z\tHGKR\t7258963507745426614\t1970-01-01T02:13:20.000000Z\t13\t\tFCSRED\n" +
                "40\tmsft\t0.009000000000000001\t2018-01-01T01:20:00.000000Z\ttrue\tCDE\t0.10288432263881209\t0.9336\t626\t2015-07-16T10:03:42.421Z\tKKUS\t5578476878779881545\t1970-01-01T02:30:00.000000Z\t14\t00000000 3f d6 88 3a 93 ef 24 a5 e2 bc 86 f9 92 a3 f1 92\n" +
                "00000010 08 f1 96 7f\tWUVMBPSVEZDY\n" +
                "41\tibm\t0.255\t2018-01-01T01:22:00.000000Z\tfalse\tCDE\t0.007868356216637062\t0.0238\t610\t2015-08-16T21:13:37.471Z\t\t-1995707592048806500\t1970-01-01T02:46:40.000000Z\t49\t\tXGYTEQCHND\n" +
                "42\tmsft\t0.9490000000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.09594113652452096\tnull\t521\t2015-12-25T22:48:17.395Z\tHGKR\t-7773905708214258099\t1970-01-01T03:03:20.000000Z\t31\t00000000 b8 17 f7 41 ff c1 a7 5c c3 31 17 dd 8d\tSXQSTVSTYSWHLSWP\n" +
                "43\tgoogl\t0.056\t2018-01-01T01:26:00.000000Z\tfalse\tABC\t0.922772279885169\t0.6153\t497\t2015-04-20T22:03:28.690Z\tHGKR\t-3371567840873310066\t1970-01-01T03:20:00.000000Z\t28\t00000000 b2 31 9c 69 be 74 9a ad cc cf b8 e4 d1 7a 4f\tEODDBHEVGXY\n" +
                "44\tgoogl\t0.552\t2018-01-01T01:28:00.000000Z\tfalse\tABC\t0.1760335737117784\t0.1548\t707\t\tLVSY\t3865339280024677042\t1970-01-01T03:36:40.000000Z\t15\t\tBIDSTDTFB\n" +
                "45\tgoogl\t0.032\t2018-01-01T01:30:00.000000Z\ttrue\tKZZ\tnull\t0.4632\t309\t2015-09-22T19:28:39.051Z\tLVSY\t8710436463015317840\t1970-01-01T03:53:20.000000Z\t9\t00000000 66 ca 85 56 e2 44 db b8 e9 93 fc d9 cb 99\tCIYIXGHRQQTKO\n" +
                "46\tgoogl\t0.064\t2018-01-01T01:32:00.000000Z\tfalse\t\tnull\t0.3006\t272\t2015-11-11T13:18:22.490Z\tLMSR\t8918536674918169108\t1970-01-01T04:10:00.000000Z\t34\t00000000 27 c7 97 9b 8b f8 04 6f d6 af 3f 2f 84 d5 12 fb\n" +
                "00000010 71 99 34\tQUJJFGQIZKM\n" +
                "47\tgoogl\t0.183\t2018-01-01T01:34:00.000000Z\ttrue\tCDE\t0.5155110628992104\t0.8308\t1002\t2015-06-19T13:11:21.018Z\tHGKR\t-4579557415183386099\t1970-01-01T04:26:40.000000Z\t34\t00000000 92 ff 37 63 be 5f b7 70 a0 07 8f 33\tIHCTIVYIVCHUC\n" +
                "48\tgoogl\t0.164\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.18100042286604445\t0.5756\t415\t2015-04-28T21:13:18.568Z\t\t7970442953226983551\t1970-01-01T04:43:20.000000Z\t19\t\t\n" +
                "49\tibm\t0.048\t2018-01-01T01:38:00.000000Z\ttrue\t\t0.3744661371925302\t0.1264\t28\t2015-09-06T14:09:17.223Z\tHGKR\t-7172806426401245043\t1970-01-01T05:00:00.000000Z\t29\t00000000 42 9e 8a 86 17 89 6b c0 cd a4 21 12 b7 e3\tRPYKHPMBMDR\n" +
                "50\tibm\t0.706\t2018-01-01T01:40:00.000000Z\tfalse\tKZZ\t0.4743479290495217\t0.5190\t864\t2015-04-26T09:59:33.624Z\tKKUS\t2808899229016932370\t1970-01-01T05:16:40.000000Z\t5\t00000000 39 dc 8c 6c 6b ac 60 aa bc f4 27 61 78\tPGHPS\n" +
                "51\tgoogl\t0.761\t2018-01-01T01:42:00.000000Z\ttrue\tABC\t0.25251288918411996\tnull\t719\t2015-05-22T06:14:06.815Z\tHGKR\t7822359916932392178\t1970-01-01T05:33:20.000000Z\t2\t00000000 26 4f e4 51 37 85 e1 e4 6e 75 fc f4 57 0e 7b 09\n" +
                "00000010 de 09 5e d7\tWCDVPWCYCTDDNJ\n" +
                "52\tgoogl\t0.512\t2018-01-01T01:44:00.000000Z\tfalse\tABC\t0.4112208369860437\t0.2756\t740\t2015-02-23T09:03:19.389Z\tHGKR\t1930705357282501293\t1970-01-01T05:50:00.000000Z\t19\t\t\n" +
                "53\tgoogl\t0.106\t2018-01-01T01:46:00.000000Z\ttrue\tABC\t0.5869842992348637\t0.4215\t762\t2015-03-07T03:16:06.453Z\tHGKR\t-7872707389967331757\t1970-01-01T06:06:40.000000Z\t3\t00000000 8d ca 1d d0 b2 eb 54 3f 32 82\tQQDOZFIDQTYO\n" +
                "54\tmsft\t0.266\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.26652004252953776\t0.0911\t937\t\t\t-7761587678997431446\t1970-01-01T06:23:20.000000Z\t39\t00000000 53 28 c0 93 b2 7b c7 55 0c dd fd c1\t\n";

        String query = "select * from y limit 4,-6";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testTopN() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n";

        testLimit(expected, expected, "select * from y limit 5");
    }

    @Test
    public void testTopN2() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n";

        testLimit(expected, expected, "select * from y limit 0,5");
    }

    @Test
    public void testTopNIndexVariable() throws Exception {
        final String query = "select * from y limit $1";
        assertMemoryLeak(() -> {
            try {
                String expected1 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                        "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                        "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                        "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                        "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                        "00000010 e7 0c 89\tLJUMLGLHMLLEO\n";

                String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                        "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                        "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                        "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                        "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                        "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                        "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                        "6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n";

                execute(
                        "create table y as (" +
                                "select" +
                                " cast(x as int) i," +
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
                                " timestamp_sequence(0, 1000000000) k," +
                                " rnd_byte(2,50) l," +
                                " rnd_bin(10, 20, 2) m," +
                                " rnd_str(5,16,2) n" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)"
                );

                bindVariableService.setLong(0, 4);
                assertQueryAndCache(expected1, query, "timestamp", true, false);
                bindVariableService.setLong(0, 6);
                assertQueryAndCache(expected2, query, "timestamp", true, false);
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testTopNVariable() throws Exception {
        final String query = "select * from y limit :lim";
        assertMemoryLeak(() -> {
            try {
                String expected1 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                        "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                        "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                        "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                        "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                        "00000010 e7 0c 89\tLJUMLGLHMLLEO\n";

                String expected2 = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                        "1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ\n" +
                        "2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t\n" +
                        "3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.7230\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV\n" +
                        "4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.2401\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a\n" +
                        "00000010 e7 0c 89\tLJUMLGLHMLLEO\n" +
                        "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                        "6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n";

                execute(
                        "create table y as (" +
                                "select" +
                                " cast(x as int) i," +
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
                                " timestamp_sequence(0, 1000000000) k," +
                                " rnd_byte(2,50) l," +
                                " rnd_bin(10, 20, 2) m," +
                                " rnd_str(5,16,2) n" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)"
                );

                bindVariableService.setLong("lim", 4);
                assertQueryAndCache(expected1, query, "timestamp", true, false);
                bindVariableService.setLong("lim", 6);
                assertQueryAndCache(expected2, query, "timestamp", true, false);
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testTopRange() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n" +
                "5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.0212\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH\n" +
                "6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.1326\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n";

        String query = "select * from y limit 4,6";
        testLimit(expected, expected, query);
    }

    private void testInvalidNegativeLimit() throws Exception {
        final int maxLimit = configuration.getSqlMaxNegativeLimit();

        assertMemoryLeak(() -> {
            execute("create table y as (" +
                    "select" +
                    " cast(x as int) i," +
                    " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                    " from long_sequence(100)" +
                    ") timestamp(timestamp)");

            String expectedMessage = "absolute LIMIT value is too large, maximum allowed value: " + maxLimit;
            int expectedPosition = 34;

            String query = "select * from y where i > 0 limit -" + (maxLimit + 1);
            try (final RecordCursorFactory factory = select(query)) {
                try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail();
                }
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), expectedMessage);
                Assert.assertEquals(Chars.toString(query), expectedPosition, e.getPosition());
            }
        });
    }

    private void testLimit(String expected1, String expected2, String query) throws Exception {
        testLimit(expected1, expected2, query, false);
    }

    private void testLimit(String expected1, String expected2, String query, boolean expectSize) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table y as (" +
                            "select" +
                            " cast(x as int) i," +
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
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQueryAndCache(expected1, query, "timestamp", true, expectSize);

            execute(
                    "insert into y select * from " +
                            "(select" +
                            " cast(x + 30 as int) i," +
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
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n" +
                            " from long_sequence(30)" +
                            ") timestamp(timestamp)"
            );

            assertQuery(expected2, query, "timestamp", true, expectSize);
        });
    }

    private void testLimitMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, id symbol)");

            String inserts = "insert into t1 values (0L, 'abc')\n" +
                    "insert into t1 values (2L, 'a1')\n" +
                    "insert into t1 values (3L, 'abc')\n" +
                    "insert into t1 values (4L, 'abc')\n" +
                    "insert into t1 values (5L, 'a2')";

            for (String sql : inserts.split("\\r?\\n")) {
                execute(sql);
            }

            assertQueryAndCache(
                    "ts\tid\n" +
                            "1970-01-01T00:00:00.000004Z\tabc\n",
                    "select * from t1 where id = 'abc' limit -1",
                    null,
                    true,
                    true
            );

            // now with a virtual column
            assertQueryAndCache(
                    "the_answer\tts\tid\n" +
                            "1764\t1970-01-01T00:00:00.000004Z\tabc\n",
                    "select 42*42 as the_answer, ts, id from t1 where id = 'abc' limit -1",
                    null,
                    true,
                    true
            );
        });
    }
}
