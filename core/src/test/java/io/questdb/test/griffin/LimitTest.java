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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LimitTest extends AbstractCairoTest {

    private static final String createTableDdl = "create table y as (" +
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
            ") timestamp(timestamp)";

    private static final String createTableDml = "insert into y select * from " +
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
            " from long_sequence(30))";

    @Test
    public void testBottomRange() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                25\tmsft\t0.918\t2018-01-01T00:50:00.000000Z\tfalse\t\t0.32613652012030125\t0.33935094\t176\t2015-02-06T18:42:24.631Z\t\t-8352023907145286323\t1970-01-01T06:40:00.000000Z\t14\t00000000 6a 9b cd bb 2e 74 cd 44 54 13 3f ff\tIGENFEL
                26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                00000010 42 71 a3 7a\tDNZNLCNGZTOY
                27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.023669481\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t
                28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699251\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX
                """;

        String expected2 = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                55\tibm\t0.213\t2018-01-01T01:50:00.000000Z\tfalse\tKZZ\tnull\t0.037858427\t503\t2015-08-25T16:59:46.151Z\tKKUS\t8510474930626176160\t1970-01-01T06:40:00.000000Z\t13\t\t
                56\tmsft\t0.061\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.7792511437604662\t0.39658612\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX
                57\tgoogl\t0.756\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.8925723033175609\t0.9924997\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f
                00000010 61 2d\tEYDNMIOCCVV
                58\tibm\t0.445\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.7613115945849444\tnull\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB
                """;

        String query = "select * from y limit -6,-2";
        testLimit(expected, expected2, query);
        assertPlanNoLeakCheck(query, """
                Limit left: -6 right: -2 skip-rows: 54 take-rows: 4
                    PageFrame
                        Row forward scan
                        Frame forward scan on: y
                """);
    }

    @Test
    public void testBottomRangeNotEnoughRows() throws Exception {
        String expected = "i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn\n";

        String expected2 = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699251\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX
                29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.55051345\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e
                00000010 23 03 dd\t
                """;

        String query = "select * from y limit -33,-31";
        testLimit(expected, expected2, query);
        assertPlanNoLeakCheck(query, """
                Limit left: -33 right: -31 skip-rows: 27 take-rows: 2
                    PageFrame
                        Row forward scan
                        Frame forward scan on: y
                """);
    }

    @Test
    public void testBottomRangePartialRows() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                """;

        String expected2 = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                00000010 42 71 a3 7a\tDNZNLCNGZTOY
                27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.023669481\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t
                28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699251\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX
                29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.55051345\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e
                00000010 23 03 dd\t
                30\tibm\t0.607\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.47808302567081573\t0.010880172\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD
                31\tibm\t0.181\t2018-01-01T01:02:00.000000Z\tfalse\tCDE\t0.5455175324785665\t0.82485497\t905\t2015-03-02T13:31:56.918Z\tLVSY\t-4086246124104754347\t1970-01-01T00:00:00.000000Z\t26\t00000000 4b c0 d9 1c 71 cf 5a 8f 21 06 b2 3f\t
                32\tibm\t0.47300000000000003\t2018-01-01T01:04:00.000000Z\tfalse\tABC\t0.0396096812427591\t0.078288496\t42\t2015-05-21T22:28:44.644Z\tKKUS\t2296041148680180183\t1970-01-01T00:16:40.000000Z\t35\t\tSVCLLERSMKRZ
                """;

        String query = "select * from y limit -35,-28";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testDefaultHi() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                00000010 e7 0c 89\tLJUMLGLHMLLEO
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                """;

        String query = "select * from y limit 5,";
        testLimit(expected, expected, query);
    }

    @Test
    public void testDefaultLo() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                00000010 e7 0c 89\tLJUMLGLHMLLEO
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                """;

        String query = "select * from y limit ,5";
        testLimit(expected, expected, query);
    }

    @Test
    public void testInvalidBottomRange() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                21\tmsft\t0.597\t2018-01-01T00:42:00.000000Z\ttrue\tP\t0.6690790546123128\t0.65911466\t974\t\tVTJW\t8843532011989881581\t1970-01-01T05:33:20.000000Z\t17\t00000000 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de 46
                00000010 04 d3\tZZBBUKOJSOLDYR
                22\tgoogl\t0.7020000000000001\t2018-01-01T00:44:00.000000Z\ttrue\tR\t0.7134568516750471\t0.09213334\t879\t2015-03-07T18:51:10.265Z\tHYRX\t5447530387277886439\t1970-01-01T05:50:00.000000Z\t2\t\tQSQJGDIHHNSS
                23\tibm\t0.5730000000000001\t2018-01-01T00:46:00.000000Z\ttrue\tV\t0.32282028174282695\tnull\t791\t2015-10-07T21:38:49.138Z\tPEHN\t4430387718690044436\t1970-01-01T06:06:40.000000Z\t34\t\tZVQQHSQSPZPB
                24\tmsft\t0.23800000000000002\t2018-01-01T00:48:00.000000Z\ttrue\t\t0.47930730718677406\t0.69366693\t635\t2015-10-11T00:49:46.817Z\tCPSW\t3860877990849202595\t1970-01-01T06:23:20.000000Z\t14\t\tPZNYVLTPKBBQ
                25\tmsft\t0.918\t2018-01-01T00:50:00.000000Z\tfalse\t\t0.32613652012030125\t0.33935094\t176\t2015-02-06T18:42:24.631Z\t\t-8352023907145286323\t1970-01-01T06:40:00.000000Z\t14\t00000000 6a 9b cd bb 2e 74 cd 44 54 13 3f ff\tIGENFEL
                26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                00000010 42 71 a3 7a\tDNZNLCNGZTOY
                27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.023669481\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t
                """;

        String expected2 = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                51\tgoogl\t0.761\t2018-01-01T01:42:00.000000Z\ttrue\tABC\t0.25251288918411996\tnull\t719\t2015-05-22T06:14:06.815Z\tHGKR\t7822359916932392178\t1970-01-01T05:33:20.000000Z\t2\t00000000 26 4f e4 51 37 85 e1 e4 6e 75 fc f4 57 0e 7b 09
                00000010 de 09 5e d7\tWCDVPWCYCTDDNJ
                52\tgoogl\t0.512\t2018-01-01T01:44:00.000000Z\tfalse\tABC\t0.4112208369860437\t0.27559507\t740\t2015-02-23T09:03:19.389Z\tHGKR\t1930705357282501293\t1970-01-01T05:50:00.000000Z\t19\t\t
                53\tgoogl\t0.106\t2018-01-01T01:46:00.000000Z\ttrue\tABC\t0.5869842992348637\t0.4214564\t762\t2015-03-07T03:16:06.453Z\tHGKR\t-7872707389967331757\t1970-01-01T06:06:40.000000Z\t3\t00000000 8d ca 1d d0 b2 eb 54 3f 32 82\tQQDOZFIDQTYO
                54\tmsft\t0.266\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.26652004252953776\t0.091148734\t937\t\t\t-7761587678997431446\t1970-01-01T06:23:20.000000Z\t39\t00000000 53 28 c0 93 b2 7b c7 55 0c dd fd c1\t
                55\tibm\t0.213\t2018-01-01T01:50:00.000000Z\tfalse\tKZZ\tnull\t0.037858427\t503\t2015-08-25T16:59:46.151Z\tKKUS\t8510474930626176160\t1970-01-01T06:40:00.000000Z\t13\t\t
                56\tmsft\t0.061\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.7792511437604662\t0.39658612\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX
                57\tgoogl\t0.756\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.8925723033175609\t0.9924997\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f
                00000010 61 2d\tEYDNMIOCCVV
                """;

        testLimit(expected, expected2, "select * from y limit -3,-10");
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
    public void testInvalidNegativePositiveArgs() throws Exception {
        execute("CREATE TABLE tango (name VARCHAR)");
        assertException("tango LIMIT -3, 2", 12, "LIMIT <negative>, <positive> is not allowed");
    }

    @Test
    public void testInvertedTopRange() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                """;
        testLimit(expected, expected, "select * from y limit 6,5");
    }

    @Test
    public void testLastN() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                00000010 42 71 a3 7a\tDNZNLCNGZTOY
                27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.023669481\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t
                28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699251\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX
                29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.55051345\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e
                00000010 23 03 dd\t
                30\tibm\t0.607\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.47808302567081573\t0.010880172\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD
                """;

        String expected2 = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                56\tmsft\t0.061\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.7792511437604662\t0.39658612\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX
                57\tgoogl\t0.756\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.8925723033175609\t0.9924997\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f
                00000010 61 2d\tEYDNMIOCCVV
                58\tibm\t0.445\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.7613115945849444\tnull\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB
                59\tgoogl\t0.778\t2018-01-01T01:58:00.000000Z\tfalse\tKZZ\t0.7741801422529707\t0.18701869\t586\t2015-05-27T15:12:16.295Z\t\t-7715437488835448247\t1970-01-01T07:46:40.000000Z\t10\t\tEPLWDUWIWJTLCP
                60\tgoogl\t0.852\t2018-01-01T02:00:00.000000Z\ttrue\tKZZ\tnull\tnull\t834\t2015-07-15T04:34:51.645Z\tLMSR\t-4834150290387342806\t1970-01-01T08:03:20.000000Z\t23\t00000000 dd 02 98 ad a8 82 73 a6 7f db d6 20\tFDRPHNGTNJJPT
                """;

        String query = "select * from y limit -5";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testLimitDefinesBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            execute(createTableDdl);
            execute(createTableDml);
            testLimitDefinesBindVariables0("select * from y limit $1", 1);
            testLimitDefinesBindVariables0("select * from y limit $1, $2", 2);
            testLimitDefinesBindVariables0("select * from y order by timestamp limit $1, $2", 2);
            testLimitDefinesBindVariables0("select * from y order by timestamp desc limit $1, $2", 2);
        });
    }

    @Test
    public void testLimitForSubQueryWithAnotherLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table if not exists table1" +
                            " (ts timestamp, k symbol capacity 2048, k2 symbol capacity 512, v long)" +
                            " timestamp(ts) partition by day wal"
            );
            for (int i = 0; i < 9; i++) {
                execute("insert into table1 values (" + (i * 10000000) + ", 'k" + i + "', " + "'k2_" + i + "', " + i + ")");
            }
            drainWalQueue();

            String query = """
                    SELECT ts, v_max FROM (
                        SELECT ts, k, max(v) AS v_max FROM table1 LIMIT -2
                    ) LIMIT 1""";
            assertPlanNoLeakCheck(query, """
                    Limit value: 1 skip-rows-max: 0 take-rows-max: 1
                        SelectedRecord
                            Limit value: -2 skip-rows: baseRows-2 take-rows-max: 2
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                    """);
            assertQueryAndCache(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            """,
                    query,
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testLimitForSubQueryWithFilterAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE eq_equities_market_data (" +
                    "timestamp TIMESTAMP, " +
                    "symbol SYMBOL, " +
                    "venue SYMBOL, " +
                    "asks DOUBLE[][], bids DOUBLE[][]" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY");
            execute("INSERT INTO eq_equities_market_data VALUES " +
                    "(0, 'AAPL', 'NYSE', ARRAY[ [11.4, 12], [10.3, 15] ], ARRAY[ [21.1, 31], [20.1, 21] ]), " +
                    "(1, 'AAPL', 'NYSE', ARRAY[ [11.5, 13], [10.4, 14] ], ARRAY[ [21.2, 32], [20.2, 22] ]), " +
                    "(2, 'AAPL', 'NYSE', ARRAY[ [11.6, 17], [10.5, 15] ], ARRAY[ [21.3, 33], [20.3, 23] ]), " +
                    "(3, 'AAPL', 'NYSE', ARRAY[ [11.2, 30], [10.2, 16] ], ARRAY[ [21.4, 34], [20.4, 24] ]), " +
                    "(4, 'AAPL', 'NYSE', ARRAY[ [11.4, 20], [10.4,  7] ], ARRAY[ [21.5, 35], [20.5, 25] ]), " +
                    "(5, 'AAPL', 'NYSE', ARRAY[ [16.0,  3], [15.0,  2] ], ARRAY[ [15.6, 36], [14.6, 26] ])"
            );
            drainWalQueue();

            assertQueryAndCache(
                    """
                            timestamp\tsymbol\tvenue\tbid_price\tbid_size\task_price\task_size
                            1970-01-01T00:00:00.000002Z\tAAPL\tNYSE\t21.3\t20.3\t11.6\t10.5
                            1970-01-01T00:00:00.000003Z\tAAPL\tNYSE\t21.4\t20.4\t11.2\t10.2
                            """,
                    """
                            select * from (
                                select timestamp, symbol, venue,
                                    bids[1][1] as bid_price,
                                    bids[2][1] as bid_size,
                                    asks[1][1] as ask_price,
                                    asks[2][1] as ask_size
                                from eq_equities_market_data
                                where symbol='AAPL' and timestamp in '1970'
                                limit -4
                            ) limit 2""",
                    "timestamp",
                    true,
                    false
            );
        });
    }

    @Test
    public void testLimitMinusOneAndPredicateAndColumnAlias() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, id symbol)");
            execute("insert into t1 values (0, 'abc'), (2, 'a1'), (3, 'abc'), (4, 'abc'), (5, 'a2')");
            assertQueryAndCache(
                    """
                            ts\tid
                            1970-01-01T00:00:00.000004Z\tabc
                            """,
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
    public void testLimitPlanChangesWhenLimitSignChanges() throws Exception {
        assertMemoryLeak(() -> {
            execute(createTableDdl);
            execute(createTableDml);

            String query = "select * from y limit $1, $2;";

            bindVariableService.setLong(0, 0L);
            bindVariableService.setLong(1, 2L);

            assertQueryAndCache(
                    """
                            i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                            1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                            2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                            """,
                    query,
                    "timestamp",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit left: $0::long[0] right: $1::long[2] skip-rows: 0 take-rows: 2
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            assertPlanNoLeakCheck(
                    "select * from y limit -2",
                    """
                            Limit value: -2 skip-rows: 58 take-rows: 2
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            bindVariableService.setLong(0, -2L);
            bindVariableService.setLong(1, 0L);

            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit left: $0::long[-2] right: $1::long[0] skip-rows: 58 take-rows: 2
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            assertQuery(
                    """
                            i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                            59\tgoogl\t0.778\t2018-01-01T01:58:00.000000Z\tfalse\tKZZ\t0.7741801422529707\t0.18701869\t586\t2015-05-27T15:12:16.295Z\t\t-7715437488835448247\t1970-01-01T07:46:40.000000Z\t10\t\tEPLWDUWIWJTLCP
                            60\tgoogl\t0.852\t2018-01-01T02:00:00.000000Z\ttrue\tKZZ\tnull\tnull\t834\t2015-07-15T04:34:51.645Z\tLMSR\t-4834150290387342806\t1970-01-01T08:03:20.000000Z\t23\t00000000 dd 02 98 ad a8 82 73 a6 7f db d6 20\tFDRPHNGTNJJPT
                            """,
                    query,
                    "timestamp",
                    true,
                    true
            );
        });
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
            String expected = """
                    i\ttimestamp
                    257\t2018-01-01T08:34:00.000000Z
                    """;
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 = 1 limit -2";
            expected = """
                    i\ttimestamp
                    193\t2018-01-01T06:26:00.000000Z
                    257\t2018-01-01T08:34:00.000000Z
                    """;
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 < 3 limit -5";
            expected = """
                    i\ttimestamp
                    194\t2018-01-01T06:28:00.000000Z
                    256\t2018-01-01T08:32:00.000000Z
                    257\t2018-01-01T08:34:00.000000Z
                    258\t2018-01-01T08:36:00.000000Z
                    320\t2018-01-01T10:40:00.000000Z
                    """;
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
            String expected = """
                    i\ttimestamp
                    257\t2018-01-01T08:34:00.000000Z
                    """;
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 = 1 limit -2";
            expected = """
                    i\ttimestamp
                    193\t2018-01-01T06:26:00.000000Z
                    257\t2018-01-01T08:34:00.000000Z
                    """;
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);

            query = "select * from y where i % 64 < 3 limit -5";
            expected = """
                    i\ttimestamp
                    194\t2018-01-01T06:28:00.000000Z
                    256\t2018-01-01T08:32:00.000000Z
                    257\t2018-01-01T08:34:00.000000Z
                    258\t2018-01-01T08:36:00.000000Z
                    320\t2018-01-01T10:40:00.000000Z
                    """;
            assertQueryNoLeakCheck(expected, query, "timestamp", true, true);
        });
    }

    @Test
    public void testNegativeLimitMultipleTOrderTermsRewriteInJoins() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("create table x as (select rnd_int() a, timestamp_sequence(0, 100) ts from long_sequence(100)) timestamp(ts)");
                    execute("create table y as (select rnd_int() a, timestamp_sequence(10, 100) ts from long_sequence(100)) timestamp(ts)");

                    assertQuery(
                            """
                                    a\tts
                                    1100812407\t1970-01-01T00:00:00.009010Z
                                    -889224806\t1970-01-01T00:00:00.009110Z
                                    1362833895\t1970-01-01T00:00:00.009210Z
                                    576104460\t1970-01-01T00:00:00.009310Z
                                    -805434743\t1970-01-01T00:00:00.009410Z
                                    1503763988\t1970-01-01T00:00:00.009510Z
                                    -640305320\t1970-01-01T00:00:00.009610Z
                                    372462435\t1970-01-01T00:00:00.009710Z
                                    1751526583\t1970-01-01T00:00:00.009810Z
                                    -101516094\t1970-01-01T00:00:00.009910Z
                                    """,
                            "y order by ts, a limit -10",
                            "ts",
                            true,
                            true
                    );

                    // here the last order by (after join) confused the optimiser into removing ordering of
                    // the left part of as-of join
                    assertSql(
                            """
                                    QUERY PLAN
                                    Sort
                                      keys: [ts, a]
                                        SelectedRecord
                                            AsOf Join
                                                Sort light
                                                  keys: [ts, a]
                                                    Sort light lo: 10 partiallySorted: true
                                                      keys: [ts desc, a desc]
                                                        PageFrame
                                                            Row backward scan
                                                            Frame backward scan on: y
                                                Sort light
                                                  keys: [ts, a desc]
                                                    Sort light lo: 4 partiallySorted: true
                                                      keys: [ts desc, a]
                                                        PageFrame
                                                            Row backward scan
                                                            Frame backward scan on: x
                                    """,
                            "explain with cte as (select * from x order by ts, a desc limit -4) select * from (y order by ts, a limit -10) y asof join cte order by y.ts, y.a "
                    );

                    // here the last order by (after join) confused the optimiser into removing ordering of
                    // the left part of lt-join. There could be an optimisation opportunity here to remove
                    assertSql(
                            """
                                    QUERY PLAN
                                    Sort
                                      keys: [ts, a]
                                        SelectedRecord
                                            Lt Join
                                                Sort light
                                                  keys: [ts, a]
                                                    Sort light lo: 10 partiallySorted: true
                                                      keys: [ts desc, a desc]
                                                        PageFrame
                                                            Row backward scan
                                                            Frame backward scan on: y
                                                Sort light
                                                  keys: [ts, a desc]
                                                    Sort light lo: 4 partiallySorted: true
                                                      keys: [ts desc, a]
                                                        PageFrame
                                                            Row backward scan
                                                            Frame backward scan on: x
                                    """,
                            "explain with cte as (select * from x order by ts, a desc limit -4) select * from (y order by ts, a limit -10) y lt join cte order by y.ts, y.a "
                    );

                    // here the result of as-of join is not specifically ordered, but
                    // the plan for as-of join incorrect. The left side of the join is
                    // required to be presented in ascending timestamp order.
                    assertSql(
                            """
                                    QUERY PLAN
                                    SelectedRecord
                                        AsOf Join
                                            Sort light
                                              keys: [ts, a]
                                                Sort light lo: 10 partiallySorted: true
                                                  keys: [ts desc, a desc]
                                                    PageFrame
                                                        Row backward scan
                                                        Frame backward scan on: y
                                            Sort light
                                              keys: [ts, a desc]
                                                Sort light lo: 4 partiallySorted: true
                                                  keys: [ts desc, a]
                                                    PageFrame
                                                        Row backward scan
                                                        Frame backward scan on: x
                                    """,
                            "explain with cte as (select * from x order by ts, a desc limit -4) select * from (y order by ts, a limit -10) y asof join cte "
                    );

                    // as-of data, timestamp order is asc
                    assertQuery(
                            """
                                    a\tts\ta1\tts1
                                    1100812407\t1970-01-01T00:00:00.009010Z\tnull\t
                                    -889224806\t1970-01-01T00:00:00.009110Z\tnull\t
                                    1362833895\t1970-01-01T00:00:00.009210Z\tnull\t
                                    576104460\t1970-01-01T00:00:00.009310Z\tnull\t
                                    -805434743\t1970-01-01T00:00:00.009410Z\tnull\t
                                    1503763988\t1970-01-01T00:00:00.009510Z\tnull\t
                                    -640305320\t1970-01-01T00:00:00.009610Z\t-1538602195\t1970-01-01T00:00:00.009600Z
                                    372462435\t1970-01-01T00:00:00.009710Z\t-360860352\t1970-01-01T00:00:00.009700Z
                                    1751526583\t1970-01-01T00:00:00.009810Z\t-372268574\t1970-01-01T00:00:00.009800Z
                                    -101516094\t1970-01-01T00:00:00.009910Z\t-235358133\t1970-01-01T00:00:00.009900Z
                                    """,
                            "with cte as (select * from x order by ts, a desc limit -4) select * from (y order by ts, a limit -10) y lt join cte order by y.ts, y.a ",
                            "ts",
                            true,
                            true
                    );

                    assertQuery(
                            """
                                    a\tts\ta1\tts1
                                    -101516094\t1970-01-01T00:00:00.009910Z\t-235358133\t1970-01-01T00:00:00.009900Z
                                    1751526583\t1970-01-01T00:00:00.009810Z\t-372268574\t1970-01-01T00:00:00.009800Z
                                    372462435\t1970-01-01T00:00:00.009710Z\t-360860352\t1970-01-01T00:00:00.009700Z
                                    -640305320\t1970-01-01T00:00:00.009610Z\t-1538602195\t1970-01-01T00:00:00.009600Z
                                    1503763988\t1970-01-01T00:00:00.009510Z\tnull\t
                                    -805434743\t1970-01-01T00:00:00.009410Z\tnull\t
                                    576104460\t1970-01-01T00:00:00.009310Z\tnull\t
                                    1362833895\t1970-01-01T00:00:00.009210Z\tnull\t
                                    -889224806\t1970-01-01T00:00:00.009110Z\tnull\t
                                    1100812407\t1970-01-01T00:00:00.009010Z\tnull\t
                                    """,
                            "with cte as (select * from x order by ts, a desc limit -4) select * from (y order by ts, a limit -10) y lt join cte order by y.ts desc, y.a ",
                            "ts###desc",
                            true,
                            true
                    );

                    assertSql(
                            """
                                    QUERY PLAN
                                    Union All
                                        Sort light
                                          keys: [ts, a desc]
                                            Sort light lo: 4 partiallySorted: true
                                              keys: [ts desc, a]
                                                PageFrame
                                                    Row backward scan
                                                    Frame backward scan on: x
                                        Sort light
                                          keys: [ts, a]
                                            Sort light lo: 10 partiallySorted: true
                                              keys: [ts desc, a desc]
                                                PageFrame
                                                    Row backward scan
                                                    Frame backward scan on: y
                                    """,
                            "explain " +
                                    "(select * from x order by ts, a desc limit -4)" +
                                    " union all " +
                                    "(select * from y order by ts, a limit -10)"
                    );

                    // last 4 + last 10 rows
                    assertQuery(
                            """
                                    a\tts
                                    -1538602195\t1970-01-01T00:00:00.009600Z
                                    -360860352\t1970-01-01T00:00:00.009700Z
                                    -372268574\t1970-01-01T00:00:00.009800Z
                                    -235358133\t1970-01-01T00:00:00.009900Z
                                    1100812407\t1970-01-01T00:00:00.009010Z
                                    -889224806\t1970-01-01T00:00:00.009110Z
                                    1362833895\t1970-01-01T00:00:00.009210Z
                                    576104460\t1970-01-01T00:00:00.009310Z
                                    -805434743\t1970-01-01T00:00:00.009410Z
                                    1503763988\t1970-01-01T00:00:00.009510Z
                                    -640305320\t1970-01-01T00:00:00.009610Z
                                    372462435\t1970-01-01T00:00:00.009710Z
                                    1751526583\t1970-01-01T00:00:00.009810Z
                                    -101516094\t1970-01-01T00:00:00.009910Z
                                    """,
                            "(select * from x order by ts, a desc limit -4)" +
                                    " union all " +
                                    "(select * from y order by ts, a limit -10)",
                            null,
                            false,
                            true
                    );
                }
        );
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
                    """
                            i\tsym\tprice\ttimestamp
                            -3\tgoogl\t1.0\t2001-01-01T00:00:00.000000Z
                            -2\tgoogl\t2.0\t2002-01-01T00:00:00.000000Z
                            -1\tgoogl\t3.0\t2003-01-01T00:00:00.000000Z
                            """,
                    "y where sym = 'googl' limit -3",
                    "timestamp",
                    true,
                    true
            );
        });
    }

    @Test
    public void testOrderByTimestampDescWithWhereAndLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (id LONG, price DOUBLE, timestamp TIMESTAMP) timestamp(timestamp);");
            execute("INSERT INTO trades " +
                    "SELECT x, x * 1.5, " +
                    "  timestamp_sequence(to_timestamp('2024-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 3600000000) " +
                    "FROM long_sequence(100);");

            assertQuery(
                    """
                            id\tprice\ttimestamp
                            100\t150.0\t2024-01-05T03:00:00.000000Z
                            99\t148.5\t2024-01-05T02:00:00.000000Z
                            98\t147.0\t2024-01-05T01:00:00.000000Z
                            97\t145.5\t2024-01-05T00:00:00.000000Z
                            96\t144.0\t2024-01-04T23:00:00.000000Z
                            95\t142.5\t2024-01-04T22:00:00.000000Z
                            94\t141.0\t2024-01-04T21:00:00.000000Z
                            93\t139.5\t2024-01-04T20:00:00.000000Z
                            92\t138.0\t2024-01-04T19:00:00.000000Z
                            91\t136.5\t2024-01-04T18:00:00.000000Z
                            """,
                    "SELECT * FROM trades WHERE timestamp < '2025-01-01' ORDER BY timestamp DESC LIMIT 10",
                    "timestamp###desc",
                    true,
                    false
            );

            // Both queries should return the same results - the last 10 rows in descending order
            // Query 1: Direct ORDER BY DESC LIMIT (this was broken - returned 0 rows)
            // Query 2: Workaround with negative LIMIT then ORDER BY DESC
            assertSqlCursors(
                    "SELECT * FROM trades WHERE timestamp < '2025-01-01' ORDER BY timestamp DESC LIMIT 10",
                    "(SELECT * FROM trades WHERE timestamp < '2025-01-01' LIMIT -10) ORDER BY timestamp DESC"
            );
        });
    }

    @Test
    public void testPlanUnresolvedBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (name VARCHAR)");
            assertPlanNoLeakCheck("tango WHERE name <> 'a' LIMIT 2, -3", """
                    Limit left: 2 right: -3 skip-rows-max: 2 take-rows: baseRows-5
                        Async Filter workers: 1
                          filter: name!='a'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tango
                    """);
            assertPlanNoLeakCheck("(tango WHERE name <> 'a' LIMIT 2, -3) LIMIT 2, -3", """
                    Limit left: 2 right: -3 skip-rows-max: 2 take-rows: baseRows-5
                        Limit left: 2 right: -3 skip-rows-max: 2 take-rows: baseRows-5
                            Async Filter workers: 1
                              filter: name!='a'
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tango
                    """);
            assertQueryNoLeakCheck("""
                            name
                            """, "(tango WHERE name <> 'a' LIMIT 2, 5) LIMIT 2, 5",
                    null, true, false);
            assertQueryNoLeakCheck("""
                            name
                            """, "tango WHERE name <> 'a' LIMIT 2, 10",
                    null, true, false);
        });
    }

    @Test
    public void testPlanZeroLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (name VARCHAR)");
            String query = "tango WHERE name <> 'a' LIMIT 2, 2";
            assertPlanNoLeakCheck(query, """
                    Limit left: 2 right: 2 skip-rows-max: 0 take-rows-max: 0
                        Async Filter workers: 1
                          filter: name!='a'
                            PageFrame
                                Row forward scan
                                Frame forward scan on: tango
                    """);
            assertQueryNoLeakCheck("""
                            name
                            """, query,
                    null, true, false);
        });
    }

    @Test
    public void testRangeVariable() throws Exception {
        final String query = "select * from y limit :lo,:hi";
        assertMemoryLeak(() -> {
            try {
                String expected1 = """
                        i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                        5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                        6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                        7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.065787554\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ
                        8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8687886\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ
                        """;

                String expected2 = """
                        i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                        7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.065787554\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ
                        8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8687886\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ
                        9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t
                        10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024457574\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t
                        11\tmsft\t0.578\t2018-01-01T00:22:00.000000Z\ttrue\tP\t0.7467013668130107\t0.5794665\t122\t2015-11-25T07:36:56.937Z\t\t2004830221820243556\t1970-01-01T02:46:40.000000Z\t45\t00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13
                        00000010 5d b2 15 e7\tWGRMDGGIJYDVRV
                        12\tmsft\t0.661\t2018-01-01T00:24:00.000000Z\ttrue\tO\t0.01396079545983997\t0.81360143\t345\t2015-08-18T10:31:42.373Z\tVTJW\t5045825384817367685\t1970-01-01T03:03:20.000000Z\t23\t00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8
                        00000010 43\tJCTIZKYFLUHZ
                        """;

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

                assertQueryAndCache(expected1, query, "timestamp", true, true);
                bindVariableService.setLong("lo", 6);
                bindVariableService.setInt("hi", 12);

                assertQueryAndCache(expected2, query, "timestamp", true, true);
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testSelectLast10RecordsInReverseTsOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE intervaltest (
                      id long,
                      ts TIMESTAMP
                    ) timestamp (ts) PARTITION BY DAY""");

            execute("""
                    insert into intervaltest
                    select x, ('2023-04-06T00:00:00.000000Z'::timestamp::long + (x*1000))::timestamp
                    from long_sequence(600000)""");

            assertQueryNoLeakCheck(
                    "count\n1000\n",
                    """
                            select count(*)
                            from intervaltest
                            WHERE ts > '2023-04-06T00:09:59.000000Z'""",
                    null,
                    false,
                    true
            );

            String query = """
                    select *
                    from intervaltest
                    WHERE ts > '2023-04-06T00:09:59.000000Z'
                    ORDER BY ts DESC
                    LIMIT 10""";

            assertPlanNoLeakCheck(
                    query,
                    """
                            Limit value: 10 skip-rows-max: 0 take-rows-max: 10
                                PageFrame
                                    Row backward scan
                                    Interval backward scan on: intervaltest
                                      intervals: [("2023-04-06T00:09:59.000001Z","MAX")]
                            """
            );

            assertQuery(
                    """
                            id\tts
                            600000\t2023-04-06T00:10:00.000000Z
                            599999\t2023-04-06T00:09:59.999000Z
                            599998\t2023-04-06T00:09:59.998000Z
                            599997\t2023-04-06T00:09:59.997000Z
                            599996\t2023-04-06T00:09:59.996000Z
                            599995\t2023-04-06T00:09:59.995000Z
                            599994\t2023-04-06T00:09:59.994000Z
                            599993\t2023-04-06T00:09:59.993000Z
                            599992\t2023-04-06T00:09:59.992000Z
                            599991\t2023-04-06T00:09:59.991000Z
                            """,
                    query,
                    "ts###DESC",
                    true,
                    false
            );
        });
    }

    @Test
    public void testSortedAndLimitedFactoriesCorrectlyUpdateWhenLimitsChange() throws Exception {
        assertMemoryLeak(() -> {
            execute(createTableDdl);
            execute(createTableDml);

            String query = "select * from y order by timestamp desc, c limit $1, $2";

            bindVariableService.setLong(0, 2L);
            bindVariableService.setLong(1, 5L);

            assertQueryAndCache(
                    """
                            i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                            58\tibm\t0.445\t2018-01-01T01:56:00.000000Z\ttrue\tCDE\t0.7613115945849444\tnull\t118\t\tHGKR\t-5065534156372441821\t1970-01-01T07:30:00.000000Z\t4\t00000000 cd 98 7d ba 9d 68 2a 79 76 fc\tBGCKOSB
                            57\tgoogl\t0.756\t2018-01-01T01:54:00.000000Z\tfalse\tKZZ\t0.8925723033175609\t0.9924997\t416\t2015-11-08T09:45:16.753Z\tLVSY\t7173713836788833462\t1970-01-01T07:13:20.000000Z\t29\t00000000 4d 0d d7 44 2d f1 57 ea aa 41 c5 55 ef 19 d9 0f
                            00000010 61 2d\tEYDNMIOCCVV
                            56\tmsft\t0.061\t2018-01-01T01:52:00.000000Z\ttrue\tCDE\t0.7792511437604662\t0.39658612\t341\t2015-03-04T08:18:06.265Z\tLVSY\t5320837171213814710\t1970-01-01T06:56:40.000000Z\t16\t\tJCUBBMQSRHLWSX
                            """,
                    query,
                    "timestamp###DESC",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort light lo: $0::long hi: $1::long
                              keys: [timestamp desc, c]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            bindVariableService.setLong(0, 8L);
            bindVariableService.setLong(1, 13L);

            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort light lo: $0::long hi: $1::long
                              keys: [timestamp desc, c]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            assertQuery(
                    """
                            i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                            52\tgoogl\t0.512\t2018-01-01T01:44:00.000000Z\tfalse\tABC\t0.4112208369860437\t0.27559507\t740\t2015-02-23T09:03:19.389Z\tHGKR\t1930705357282501293\t1970-01-01T05:50:00.000000Z\t19\t\t
                            51\tgoogl\t0.761\t2018-01-01T01:42:00.000000Z\ttrue\tABC\t0.25251288918411996\tnull\t719\t2015-05-22T06:14:06.815Z\tHGKR\t7822359916932392178\t1970-01-01T05:33:20.000000Z\t2\t00000000 26 4f e4 51 37 85 e1 e4 6e 75 fc f4 57 0e 7b 09
                            00000010 de 09 5e d7\tWCDVPWCYCTDDNJ
                            50\tibm\t0.706\t2018-01-01T01:40:00.000000Z\tfalse\tKZZ\t0.4743479290495217\t0.5189641\t864\t2015-04-26T09:59:33.624Z\tKKUS\t2808899229016932370\t1970-01-01T05:16:40.000000Z\t5\t00000000 39 dc 8c 6c 6b ac 60 aa bc f4 27 61 78\tPGHPS
                            49\tibm\t0.048\t2018-01-01T01:38:00.000000Z\ttrue\t\t0.3744661371925302\t0.12639552\t28\t2015-09-06T14:09:17.223Z\tHGKR\t-7172806426401245043\t1970-01-01T05:00:00.000000Z\t29\t00000000 42 9e 8a 86 17 89 6b c0 cd a4 21 12 b7 e3\tRPYKHPMBMDR
                            48\tgoogl\t0.164\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.18100042286604445\t0.5755603\t415\t2015-04-28T21:13:18.568Z\t\t7970442953226983551\t1970-01-01T04:43:20.000000Z\t19\t\t
                            """,
                    query,
                    "timestamp###DESC",
                    true,
                    true
            );

            // non-parameterized query
            assertQuery(
                    """
                            i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                            10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024457574\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t
                            9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t
                            8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8687886\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ
                            7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.065787554\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ
                            6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                            5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                            4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                            00000010 e7 0c 89\tLJUMLGLHMLLEO
                            """,
                    "select * from y order by timestamp desc, c limit -10, -3",
                    "timestamp###DESC",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    "select * from y order by timestamp desc, c limit -10, -3",
                    """
                            Sort light lo: -10 hi: -3
                              keys: [timestamp desc, c]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            bindVariableService.setLong(0, -10);
            bindVariableService.setLong(1, -3);

            assertPlanNoLeakCheck(
                    query,
                    """
                            Sort light lo: $0::long hi: $1::long
                              keys: [timestamp desc, c]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: y
                            """
            );

            assertQuery(
                    """
                            i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                            10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024457574\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t
                            9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t
                            8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8687886\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ
                            7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.065787554\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ
                            6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                            5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                            4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                            00000010 e7 0c 89\tLJUMLGLHMLLEO
                            """,
                    query,
                    "timestamp###DESC",
                    true,
                    true
            );
        });
    }

    @Test
    public void testSortedAndLimitedFactoriesCorrectlyUpdateWhenLimitsChangePartiallySorted() throws Exception {
        assertMemoryLeak(() -> {
            execute(createTableDdl);
            execute(createTableDml);

            String query = "select * from y order by timestamp, c limit :lo, :hi";

            bindVariableService.setLong("lo", 2L);
            bindVariableService.setLong("hi", 5L);

            assertQueryAndCache(
                    """
                            i	sym2	price	timestamp	b	c	d	e	f	g	ik	j	k	l	m	n
                            3	googl	0.17400000000000002	2018-01-01T00:06:00.000000Z	false	W	0.8828228366697741	0.72300154	845	2015-08-26T10:57:26.275Z	VTJW	9029468389542245059	1970-01-01T00:33:20.000000Z	46	00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47	DSWUGSHOLNV
                            4	ibm	0.148	2018-01-01T00:08:00.000000Z	true	I	0.3456897991538844	0.24008358	775	2015-08-03T15:58:03.335Z	VTJW	-8910603140262731534	1970-01-01T00:50:00.000000Z	24	00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                            00000010 e7 0c 89	LJUMLGLHMLLEO
                            5	googl	0.868	2018-01-01T00:10:00.000000Z	true	Z	0.4274704286353759	0.021189213	179			5746626297238459939	1970-01-01T01:06:40.000000Z	35	00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb	RGIIH
                            """,
                    query,
                    "timestamp",
                    true,
                    true
            );

            assertPlanNoLeakCheck(query, """
                    Sort light lo: :lo::long hi: :hi::long partiallySorted: true
                      keys: [timestamp, c]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: y
                    """);

            bindVariableService.setLong("lo", 8L);
            bindVariableService.setLong("hi", 13L);

            assertPlanNoLeakCheck(query, """
                    Sort light lo: :lo::long hi: :hi::long partiallySorted: true
                      keys: [timestamp, c]
                        PageFrame
                            Row forward scan
                            Frame forward scan on: y
                    """);

            assertQuery(
                    """
                            i	sym2	price	timestamp	b	c	d	e	f	g	ik	j	k	l	m	n
                            9	msft	0.623	2018-01-01T00:18:00.000000Z	false	I	0.8786111112537701	0.9966377	403	2015-08-19T00:36:24.375Z	CPSW	-8506266080452644687	1970-01-01T02:13:20.000000Z	6	00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t
                            10	msft	0.509	2018-01-01T00:20:00.000000Z	true	I	0.49153268154777974	0.0024457574	195	2015-10-15T17:45:21.025Z		3987576220753016999	1970-01-01T02:30:00.000000Z	20	00000000 96 37 08 dd 98 ef 54 88 2a a2\t
                            11	msft	0.578	2018-01-01T00:22:00.000000Z	true	P	0.7467013668130107	0.5794665	122	2015-11-25T07:36:56.937Z		2004830221820243556	1970-01-01T02:46:40.000000Z	45	00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13
                            00000010 5d b2 15 e7	WGRMDGGIJYDVRV
                            12	msft	0.661	2018-01-01T00:24:00.000000Z	true	O	0.01396079545983997	0.81360143	345	2015-08-18T10:31:42.373Z	VTJW	5045825384817367685	1970-01-01T03:03:20.000000Z	23	00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8
                            00000010 43	JCTIZKYFLUHZ
                            13	ibm	0.704	2018-01-01T00:26:00.000000Z	true	K	0.036735155240002815	0.84058154	742	2015-05-03T18:49:03.996Z	PEHN	2568830294369411037	1970-01-01T03:20:00.000000Z	24	00000000 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2 bf 84 5a
                            00000010 6f 38 35\t
                            """,
                    query,
                    "timestamp",
                    true,
                    true
            );
        });
    }

    @Test
    public void testTopBottomRange() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.065787554\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ
                8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8687886\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ
                9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t
                10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024457574\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t
                11\tmsft\t0.578\t2018-01-01T00:22:00.000000Z\ttrue\tP\t0.7467013668130107\t0.5794665\t122\t2015-11-25T07:36:56.937Z\t\t2004830221820243556\t1970-01-01T02:46:40.000000Z\t45\t00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13
                00000010 5d b2 15 e7\tWGRMDGGIJYDVRV
                12\tmsft\t0.661\t2018-01-01T00:24:00.000000Z\ttrue\tO\t0.01396079545983997\t0.81360143\t345\t2015-08-18T10:31:42.373Z\tVTJW\t5045825384817367685\t1970-01-01T03:03:20.000000Z\t23\t00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8
                00000010 43\tJCTIZKYFLUHZ
                13\tibm\t0.704\t2018-01-01T00:26:00.000000Z\ttrue\tK\t0.036735155240002815\t0.84058154\t742\t2015-05-03T18:49:03.996Z\tPEHN\t2568830294369411037\t1970-01-01T03:20:00.000000Z\t24\t00000000 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2 bf 84 5a
                00000010 6f 38 35\t
                14\tgoogl\t0.651\t2018-01-01T00:28:00.000000Z\ttrue\tL\tnull\t0.13890666\t984\t2015-04-30T08:35:52.508Z\tHYRX\t-6929866925584807039\t1970-01-01T03:36:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\t
                15\tmsft\t0.40900000000000003\t2018-01-01T00:30:00.000000Z\tfalse\tW\t0.8034049105590781\t0.044039965\t854\t2015-04-10T01:03:15.469Z\t\t-9050844408181442061\t1970-01-01T03:53:20.000000Z\t24\t\tPFYXPV
                16\tgoogl\t0.839\t2018-01-01T00:32:00.000000Z\tfalse\tN\t0.11048000399634927\t0.70958537\t379\t2015-04-14T08:49:20.021Z\t\t-5749151825415257775\t1970-01-01T04:10:00.000000Z\t16\t\t
                17\tibm\t0.28600000000000003\t2018-01-01T00:34:00.000000Z\tfalse\t\t0.1353529674614602\tnull\t595\t\tVTJW\t-6237826420165615015\t1970-01-01T04:26:40.000000Z\t33\t\tBEGMITI
                18\tibm\t0.932\t2018-01-01T00:36:00.000000Z\ttrue\tH\t0.88982264111644\t0.7073941\t130\t2015-04-09T21:18:23.066Z\tCPSW\t-5708280760166173503\t1970-01-01T04:43:20.000000Z\t27\t00000000 69 94 3f 7d ef 3b b8 be f8 a1 46 87 28 92 a3 9b
                00000010 e3 cb\tNIZOSBOSE
                19\tibm\t0.151\t2018-01-01T00:38:00.000000Z\ttrue\tH\t0.39201296350741366\t0.5700419\t748\t\t\t-8060696376111078264\t1970-01-01T05:00:00.000000Z\t11\t00000000 10 20 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d
                00000010 6c a9\tTDCEBYWXBBZV
                20\tgoogl\t0.624\t2018-01-01T00:40:00.000000Z\tfalse\tY\t0.5863937813368164\t0.21032876\t834\t2015-10-15T00:48:00.413Z\tHYRX\t7913225810447636126\t1970-01-01T05:16:40.000000Z\t12\t00000000 f6 78 09 1c 5d 88 f5 52 fd 36 02 50 d9 a0 b5 90
                00000010 6c 9c 23\tILLEYMIWTCWLFORG
                21\tmsft\t0.597\t2018-01-01T00:42:00.000000Z\ttrue\tP\t0.6690790546123128\t0.65911466\t974\t\tVTJW\t8843532011989881581\t1970-01-01T05:33:20.000000Z\t17\t00000000 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de 46
                00000010 04 d3\tZZBBUKOJSOLDYR
                22\tgoogl\t0.7020000000000001\t2018-01-01T00:44:00.000000Z\ttrue\tR\t0.7134568516750471\t0.09213334\t879\t2015-03-07T18:51:10.265Z\tHYRX\t5447530387277886439\t1970-01-01T05:50:00.000000Z\t2\t\tQSQJGDIHHNSS
                23\tibm\t0.5730000000000001\t2018-01-01T00:46:00.000000Z\ttrue\tV\t0.32282028174282695\tnull\t791\t2015-10-07T21:38:49.138Z\tPEHN\t4430387718690044436\t1970-01-01T06:06:40.000000Z\t34\t\tZVQQHSQSPZPB
                24\tmsft\t0.23800000000000002\t2018-01-01T00:48:00.000000Z\ttrue\t\t0.47930730718677406\t0.69366693\t635\t2015-10-11T00:49:46.817Z\tCPSW\t3860877990849202595\t1970-01-01T06:23:20.000000Z\t14\t\tPZNYVLTPKBBQ
                """;

        String expected2 = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                7\tgoogl\t0.076\t2018-01-01T00:14:00.000000Z\ttrue\tE\t0.7606252634124595\t0.065787554\t1018\t2015-02-23T07:09:35.550Z\tPEHN\t7797019568426198829\t1970-01-01T01:40:00.000000Z\t10\t00000000 80 c9 eb a3 67 7a 1a 79 e4 35 e4 3a dc 5c 65 ff\tIGYVFZ
                8\tibm\t0.543\t2018-01-01T00:16:00.000000Z\ttrue\tO\t0.4835256202036067\t0.8687886\t355\t2015-09-06T20:21:06.672Z\t\t-9219078548506735248\t1970-01-01T01:56:40.000000Z\t33\t00000000 b3 14 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1\tXUKLGMXSLUQ
                9\tmsft\t0.623\t2018-01-01T00:18:00.000000Z\tfalse\tI\t0.8786111112537701\t0.9966377\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:13:20.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t
                10\tmsft\t0.509\t2018-01-01T00:20:00.000000Z\ttrue\tI\t0.49153268154777974\t0.0024457574\t195\t2015-10-15T17:45:21.025Z\t\t3987576220753016999\t1970-01-01T02:30:00.000000Z\t20\t00000000 96 37 08 dd 98 ef 54 88 2a a2\t
                11\tmsft\t0.578\t2018-01-01T00:22:00.000000Z\ttrue\tP\t0.7467013668130107\t0.5794665\t122\t2015-11-25T07:36:56.937Z\t\t2004830221820243556\t1970-01-01T02:46:40.000000Z\t45\t00000000 a0 dd 44 11 e2 a3 24 4e 44 a8 0d fe 27 ec 53 13
                00000010 5d b2 15 e7\tWGRMDGGIJYDVRV
                12\tmsft\t0.661\t2018-01-01T00:24:00.000000Z\ttrue\tO\t0.01396079545983997\t0.81360143\t345\t2015-08-18T10:31:42.373Z\tVTJW\t5045825384817367685\t1970-01-01T03:03:20.000000Z\t23\t00000000 51 9d 5d 28 ac 02 2e fe 05 3b 94 5f ec d3 dc f8
                00000010 43\tJCTIZKYFLUHZ
                13\tibm\t0.704\t2018-01-01T00:26:00.000000Z\ttrue\tK\t0.036735155240002815\t0.84058154\t742\t2015-05-03T18:49:03.996Z\tPEHN\t2568830294369411037\t1970-01-01T03:20:00.000000Z\t24\t00000000 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2 bf 84 5a
                00000010 6f 38 35\t
                14\tgoogl\t0.651\t2018-01-01T00:28:00.000000Z\ttrue\tL\tnull\t0.13890666\t984\t2015-04-30T08:35:52.508Z\tHYRX\t-6929866925584807039\t1970-01-01T03:36:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\t
                15\tmsft\t0.40900000000000003\t2018-01-01T00:30:00.000000Z\tfalse\tW\t0.8034049105590781\t0.044039965\t854\t2015-04-10T01:03:15.469Z\t\t-9050844408181442061\t1970-01-01T03:53:20.000000Z\t24\t\tPFYXPV
                16\tgoogl\t0.839\t2018-01-01T00:32:00.000000Z\tfalse\tN\t0.11048000399634927\t0.70958537\t379\t2015-04-14T08:49:20.021Z\t\t-5749151825415257775\t1970-01-01T04:10:00.000000Z\t16\t\t
                17\tibm\t0.28600000000000003\t2018-01-01T00:34:00.000000Z\tfalse\t\t0.1353529674614602\tnull\t595\t\tVTJW\t-6237826420165615015\t1970-01-01T04:26:40.000000Z\t33\t\tBEGMITI
                18\tibm\t0.932\t2018-01-01T00:36:00.000000Z\ttrue\tH\t0.88982264111644\t0.7073941\t130\t2015-04-09T21:18:23.066Z\tCPSW\t-5708280760166173503\t1970-01-01T04:43:20.000000Z\t27\t00000000 69 94 3f 7d ef 3b b8 be f8 a1 46 87 28 92 a3 9b
                00000010 e3 cb\tNIZOSBOSE
                19\tibm\t0.151\t2018-01-01T00:38:00.000000Z\ttrue\tH\t0.39201296350741366\t0.5700419\t748\t\t\t-8060696376111078264\t1970-01-01T05:00:00.000000Z\t11\t00000000 10 20 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d
                00000010 6c a9\tTDCEBYWXBBZV
                20\tgoogl\t0.624\t2018-01-01T00:40:00.000000Z\tfalse\tY\t0.5863937813368164\t0.21032876\t834\t2015-10-15T00:48:00.413Z\tHYRX\t7913225810447636126\t1970-01-01T05:16:40.000000Z\t12\t00000000 f6 78 09 1c 5d 88 f5 52 fd 36 02 50 d9 a0 b5 90
                00000010 6c 9c 23\tILLEYMIWTCWLFORG
                21\tmsft\t0.597\t2018-01-01T00:42:00.000000Z\ttrue\tP\t0.6690790546123128\t0.65911466\t974\t\tVTJW\t8843532011989881581\t1970-01-01T05:33:20.000000Z\t17\t00000000 a2 3c d0 65 5e b7 95 2e 4a af c6 d0 19 6a de 46
                00000010 04 d3\tZZBBUKOJSOLDYR
                22\tgoogl\t0.7020000000000001\t2018-01-01T00:44:00.000000Z\ttrue\tR\t0.7134568516750471\t0.09213334\t879\t2015-03-07T18:51:10.265Z\tHYRX\t5447530387277886439\t1970-01-01T05:50:00.000000Z\t2\t\tQSQJGDIHHNSS
                23\tibm\t0.5730000000000001\t2018-01-01T00:46:00.000000Z\ttrue\tV\t0.32282028174282695\tnull\t791\t2015-10-07T21:38:49.138Z\tPEHN\t4430387718690044436\t1970-01-01T06:06:40.000000Z\t34\t\tZVQQHSQSPZPB
                24\tmsft\t0.23800000000000002\t2018-01-01T00:48:00.000000Z\ttrue\t\t0.47930730718677406\t0.69366693\t635\t2015-10-11T00:49:46.817Z\tCPSW\t3860877990849202595\t1970-01-01T06:23:20.000000Z\t14\t\tPZNYVLTPKBBQ
                25\tmsft\t0.918\t2018-01-01T00:50:00.000000Z\tfalse\t\t0.32613652012030125\t0.33935094\t176\t2015-02-06T18:42:24.631Z\t\t-8352023907145286323\t1970-01-01T06:40:00.000000Z\t14\t00000000 6a 9b cd bb 2e 74 cd 44 54 13 3f ff\tIGENFEL
                26\tibm\t0.33\t2018-01-01T00:52:00.000000Z\tfalse\tM\t0.19823647700531244\tnull\t557\t2015-01-30T03:27:34.392Z\t\t5324839128380055812\t1970-01-01T06:56:40.000000Z\t25\t00000000 25 07 db 62 44 33 6e 00 8e 93 bd 27 42 f8 25 2a
                00000010 42 71 a3 7a\tDNZNLCNGZTOY
                27\tmsft\t0.673\t2018-01-01T00:54:00.000000Z\tfalse\tP\t0.527776712010911\t0.023669481\t517\t2015-05-20T07:51:29.675Z\tPEHN\t-7667438647671231061\t1970-01-01T07:13:20.000000Z\t22\t00000000 61 99 be 2d f5 30 78 6d 5a 3b\t
                28\tgoogl\t0.17300000000000001\t2018-01-01T00:56:00.000000Z\tfalse\tY\t0.991107083990332\t0.6699251\t324\t2015-03-27T18:25:29.970Z\t\t-4241819446186560308\t1970-01-01T07:30:00.000000Z\t35\t00000000 34 cd 15 35 bb a4 a3 c8 66 0c 40 71 ea 20\tTHMHZNVZHCNX
                29\tibm\t0.24\t2018-01-01T00:58:00.000000Z\tfalse\t\tnull\t0.55051345\t667\t2015-12-17T04:08:24.080Z\tPEHN\t-1549327479854558367\t1970-01-01T07:46:40.000000Z\t10\t00000000 03 5b 11 44 83 06 63 2b 58 3b 4b b7 e2 7f ab 6e
                00000010 23 03 dd\t
                30\tibm\t0.607\t2018-01-01T01:00:00.000000Z\ttrue\tF\t0.47808302567081573\t0.010880172\t998\t2015-04-30T21:40:32.732Z\tCPSW\t4654788096008024367\t1970-01-01T08:03:20.000000Z\t32\t\tJLKTRD
                31\tibm\t0.181\t2018-01-01T01:02:00.000000Z\tfalse\tCDE\t0.5455175324785665\t0.82485497\t905\t2015-03-02T13:31:56.918Z\tLVSY\t-4086246124104754347\t1970-01-01T00:00:00.000000Z\t26\t00000000 4b c0 d9 1c 71 cf 5a 8f 21 06 b2 3f\t
                32\tibm\t0.47300000000000003\t2018-01-01T01:04:00.000000Z\tfalse\tABC\t0.0396096812427591\t0.078288496\t42\t2015-05-21T22:28:44.644Z\tKKUS\t2296041148680180183\t1970-01-01T00:16:40.000000Z\t35\t\tSVCLLERSMKRZ
                33\tibm\t0.6\t2018-01-01T01:06:00.000000Z\tfalse\tCDE\t0.11423618345717534\t0.9925168\t288\t2015-11-17T19:46:21.874Z\tLVSY\t-5669452046744991419\t1970-01-01T00:33:20.000000Z\t43\t00000000 ef 2d 99 66 3d db c1 cc b8 82 3d ec f3 66 5e 70
                00000010 38 5e\tBWWXFQDCQSCMO
                34\tmsft\t0.867\t2018-01-01T01:08:00.000000Z\tfalse\tKZZ\t0.7593868458005614\tnull\t177\t2015-01-12T08:29:36.861Z\t\t7660755785254391246\t1970-01-01T00:50:00.000000Z\t22\t00000000 a0 ba a5 d1 63 ca 32 e5 0d 68 52 c6 94 c3 18 c9
                00000010 7c\t
                35\tgoogl\t0.11\t2018-01-01T01:10:00.000000Z\tfalse\t\t0.3619006645431899\t0.1061278\t510\t2015-09-11T19:36:06.058Z\t\t-8165131599894925271\t1970-01-01T01:06:40.000000Z\t4\t\tGZGKCGBZDMGYDEQ
                36\tibm\t0.07200000000000001\t2018-01-01T01:12:00.000000Z\ttrue\tABC\t0.12049392901868938\t0.7399948\t65\t2015-06-24T11:48:21.316Z\t\t7391407846465678094\t1970-01-01T01:23:20.000000Z\t39\t00000000 bb c3 ec 4b 97 27 df cd 7a 14 07 92 01 f5 6a\t
                37\tmsft\t0.73\t2018-01-01T01:14:00.000000Z\ttrue\tABC\t0.2717670505640706\t0.20943618\t941\t2015-02-13T23:00:15.193Z\tLVSY\t-4554200170857361590\t1970-01-01T01:40:00.000000Z\t11\t00000000 3f 4e 27 42 f2 f8 5e 29 d3 b9 67 75 95 fa 1f 92
                00000010 24 b1\tWYUHNBCCPM
                38\tgoogl\t0.59\t2018-01-01T01:16:00.000000Z\ttrue\tABC\t0.1296510741209903\t0.45522648\t350\t2015-05-27T12:47:41.516Z\tHGKR\t7992851382615210277\t1970-01-01T01:56:40.000000Z\t26\t\tKUNRDCWNPQYTEW
                39\tibm\t0.47700000000000004\t2018-01-01T01:18:00.000000Z\ttrue\t\t0.110401374979613\t0.42038977\t701\t2015-02-06T16:19:37.448Z\tHGKR\t7258963507745426614\t1970-01-01T02:13:20.000000Z\t13\t\tFCSRED
                40\tmsft\t0.009000000000000001\t2018-01-01T01:20:00.000000Z\ttrue\tCDE\t0.10288432263881209\t0.93362087\t626\t2015-07-16T10:03:42.421Z\tKKUS\t5578476878779881545\t1970-01-01T02:30:00.000000Z\t14\t00000000 3f d6 88 3a 93 ef 24 a5 e2 bc 86 f9 92 a3 f1 92
                00000010 08 f1 96 7f\tWUVMBPSVEZDY
                41\tibm\t0.255\t2018-01-01T01:22:00.000000Z\tfalse\tCDE\t0.007868356216637062\t0.0238384\t610\t2015-08-16T21:13:37.471Z\t\t-1995707592048806500\t1970-01-01T02:46:40.000000Z\t49\t\tXGYTEQCHND
                42\tmsft\t0.9490000000000001\t2018-01-01T01:24:00.000000Z\ttrue\tCDE\t0.09594113652452096\tnull\t521\t2015-12-25T22:48:17.395Z\tHGKR\t-7773905708214258099\t1970-01-01T03:03:20.000000Z\t31\t00000000 b8 17 f7 41 ff c1 a7 5c c3 31 17 dd 8d\tSXQSTVSTYSWHLSWP
                43\tgoogl\t0.056\t2018-01-01T01:26:00.000000Z\tfalse\tABC\t0.922772279885169\t0.6153355\t497\t2015-04-20T22:03:28.690Z\tHGKR\t-3371567840873310066\t1970-01-01T03:20:00.000000Z\t28\t00000000 b2 31 9c 69 be 74 9a ad cc cf b8 e4 d1 7a 4f\tEODDBHEVGXY
                44\tgoogl\t0.552\t2018-01-01T01:28:00.000000Z\tfalse\tABC\t0.1760335737117784\t0.15482622\t707\t\tLVSY\t3865339280024677042\t1970-01-01T03:36:40.000000Z\t15\t\tBIDSTDTFB
                45\tgoogl\t0.032\t2018-01-01T01:30:00.000000Z\ttrue\tKZZ\tnull\t0.46317148\t309\t2015-09-22T19:28:39.051Z\tLVSY\t8710436463015317840\t1970-01-01T03:53:20.000000Z\t9\t00000000 66 ca 85 56 e2 44 db b8 e9 93 fc d9 cb 99\tCIYIXGHRQQTKO
                46\tgoogl\t0.064\t2018-01-01T01:32:00.000000Z\tfalse\t\tnull\t0.30057436\t272\t2015-11-11T13:18:22.490Z\tLMSR\t8918536674918169108\t1970-01-01T04:10:00.000000Z\t34\t00000000 27 c7 97 9b 8b f8 04 6f d6 af 3f 2f 84 d5 12 fb
                00000010 71 99 34\tQUJJFGQIZKM
                47\tgoogl\t0.183\t2018-01-01T01:34:00.000000Z\ttrue\tCDE\t0.5155110628992104\t0.8307896\t1002\t2015-06-19T13:11:21.018Z\tHGKR\t-4579557415183386099\t1970-01-01T04:26:40.000000Z\t34\t00000000 92 ff 37 63 be 5f b7 70 a0 07 8f 33\tIHCTIVYIVCHUC
                48\tgoogl\t0.164\t2018-01-01T01:36:00.000000Z\ttrue\tABC\t0.18100042286604445\t0.5755603\t415\t2015-04-28T21:13:18.568Z\t\t7970442953226983551\t1970-01-01T04:43:20.000000Z\t19\t\t
                49\tibm\t0.048\t2018-01-01T01:38:00.000000Z\ttrue\t\t0.3744661371925302\t0.12639552\t28\t2015-09-06T14:09:17.223Z\tHGKR\t-7172806426401245043\t1970-01-01T05:00:00.000000Z\t29\t00000000 42 9e 8a 86 17 89 6b c0 cd a4 21 12 b7 e3\tRPYKHPMBMDR
                50\tibm\t0.706\t2018-01-01T01:40:00.000000Z\tfalse\tKZZ\t0.4743479290495217\t0.5189641\t864\t2015-04-26T09:59:33.624Z\tKKUS\t2808899229016932370\t1970-01-01T05:16:40.000000Z\t5\t00000000 39 dc 8c 6c 6b ac 60 aa bc f4 27 61 78\tPGHPS
                51\tgoogl\t0.761\t2018-01-01T01:42:00.000000Z\ttrue\tABC\t0.25251288918411996\tnull\t719\t2015-05-22T06:14:06.815Z\tHGKR\t7822359916932392178\t1970-01-01T05:33:20.000000Z\t2\t00000000 26 4f e4 51 37 85 e1 e4 6e 75 fc f4 57 0e 7b 09
                00000010 de 09 5e d7\tWCDVPWCYCTDDNJ
                52\tgoogl\t0.512\t2018-01-01T01:44:00.000000Z\tfalse\tABC\t0.4112208369860437\t0.27559507\t740\t2015-02-23T09:03:19.389Z\tHGKR\t1930705357282501293\t1970-01-01T05:50:00.000000Z\t19\t\t
                53\tgoogl\t0.106\t2018-01-01T01:46:00.000000Z\ttrue\tABC\t0.5869842992348637\t0.4214564\t762\t2015-03-07T03:16:06.453Z\tHGKR\t-7872707389967331757\t1970-01-01T06:06:40.000000Z\t3\t00000000 8d ca 1d d0 b2 eb 54 3f 32 82\tQQDOZFIDQTYO
                54\tmsft\t0.266\t2018-01-01T01:48:00.000000Z\tfalse\t\t0.26652004252953776\t0.091148734\t937\t\t\t-7761587678997431446\t1970-01-01T06:23:20.000000Z\t39\t00000000 53 28 c0 93 b2 7b c7 55 0c dd fd c1\t
                """;

        String query = "select * from y limit 4,-6";
        testLimit(expected, expected2, query);
    }

    @Test
    public void testTopN() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                00000010 e7 0c 89\tLJUMLGLHMLLEO
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                """;

        testLimit(expected, expected, "select * from y limit 5");
    }

    @Test
    public void testTopN2() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                00000010 e7 0c 89\tLJUMLGLHMLLEO
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                """;

        testLimit(expected, expected, "select * from y limit 0,5");
    }

    @Test
    public void testTopNIndexVariable() throws Exception {
        final String query = "select * from y limit $1";
        assertMemoryLeak(() -> {
            try {
                String expected1 = """
                        i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                        1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                        2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                        3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                        4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                        00000010 e7 0c 89\tLJUMLGLHMLLEO
                        """;

                String expected2 = """
                        i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                        1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                        2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                        3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                        4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                        00000010 e7 0c 89\tLJUMLGLHMLLEO
                        5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                        6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                        """;

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
                assertQueryAndCache(expected1, query, "timestamp", true, true);
                bindVariableService.setLong(0, 6);
                assertQueryAndCache(expected2, query, "timestamp", true, true);
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
                String expected1 = """
                        i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                        1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                        2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                        3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                        4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                        00000010 e7 0c 89\tLJUMLGLHMLLEO
                        """;

                String expected2 = """
                        i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                        1\tmsft\t0.509\t2018-01-01T00:02:00.000000Z\tfalse\tU\t0.5243722859289777\t0.8072372\t365\t2015-05-02T19:30:57.935Z\t\t-4485747798769957016\t1970-01-01T00:00:00.000000Z\t19\t00000000 19 c4 95 94 36 53 49 b4 59 7e 3b 08 a1 1e\tYSBEOUOJSHRUEDRQ
                        2\tgoogl\t0.423\t2018-01-01T00:04:00.000000Z\tfalse\tG\t0.5298405941762054\tnull\t493\t2015-04-09T11:42:28.332Z\tHYRX\t-8811278461560712840\t1970-01-01T00:16:40.000000Z\t29\t00000000 53 d0 fb 64 bb 1a d4 f0 2d 40 e2 4b b1 3e e3 f1\t
                        3\tgoogl\t0.17400000000000002\t2018-01-01T00:06:00.000000Z\tfalse\tW\t0.8828228366697741\t0.72300154\t845\t2015-08-26T10:57:26.275Z\tVTJW\t9029468389542245059\t1970-01-01T00:33:20.000000Z\t46\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\tDSWUGSHOLNV
                        4\tibm\t0.148\t2018-01-01T00:08:00.000000Z\ttrue\tI\t0.3456897991538844\t0.24008358\t775\t2015-08-03T15:58:03.335Z\tVTJW\t-8910603140262731534\t1970-01-01T00:50:00.000000Z\t24\t00000000 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69 38 e1 77 9a
                        00000010 e7 0c 89\tLJUMLGLHMLLEO
                        5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                        6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                        """;

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
                assertQueryAndCache(expected1, query, "timestamp", true, true);
                bindVariableService.setLong("lim", 6);
                assertQueryAndCache(expected2, query, "timestamp", true, true);
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testTopRange() throws Exception {
        String expected = """
                i\tsym2\tprice\ttimestamp\tb\tc\td\te\tf\tg\tik\tj\tk\tl\tm\tn
                5\tgoogl\t0.868\t2018-01-01T00:10:00.000000Z\ttrue\tZ\t0.4274704286353759\t0.021189213\t179\t\t\t5746626297238459939\t1970-01-01T01:06:40.000000Z\t35\t00000000 91 88 28 a5 18 93 bd 0b 61 f5 5d d0 eb\tRGIIH
                6\tmsft\t0.297\t2018-01-01T00:12:00.000000Z\tfalse\tY\t0.2672120489216767\t0.13264287\t215\t\t\t-8534688874718947140\t1970-01-01T01:23:20.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE
                """;

        String query = "select * from y limit 4,6";
        testLimit(expected, expected, query);
    }

    private static void testLimitDefinesBindVariables0(String sqlText, int bindVarCount) throws SqlException {
        // ensure clear test environment
        sqlExecutionContext.getBindVariableService().clear();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            try (RecordCursorFactory ignored = compiler.compile(sqlText, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < bindVarCount; i++) {
                    Function fun = sqlExecutionContext.getBindVariableService().getFunction(i);
                    Assert.assertNotNull(fun);
                    Assert.assertEquals(ColumnType.LONG, fun.getType());
                    Assert.assertEquals(Numbers.LONG_NULL, fun.getLong(null));
                }
            }
        }
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
        assertMemoryLeak(() -> {
            execute(createTableDdl);

            assertQueryAndCache(expected1, query, "timestamp", true, true);

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

            assertQuery(expected2, query, "timestamp", true, true);
        });
    }

    private void testLimitMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 (ts timestamp, id symbol)");

            String inserts = """
                    insert into t1 values (0L, 'abc')
                    insert into t1 values (2L, 'a1')
                    insert into t1 values (3L, 'abc')
                    insert into t1 values (4L, 'abc')
                    insert into t1 values (5L, 'a2')""";

            for (String sql : inserts.split("\\r?\\n")) {
                execute(sql);
            }

            assertQueryAndCache(
                    """
                            ts\tid
                            1970-01-01T00:00:00.000004Z\tabc
                            """,
                    "select * from t1 where id = 'abc' limit -1",
                    null,
                    true,
                    true
            );

            // now with a virtual column
            assertQueryAndCache(
                    """
                            the_answer\tts\tid
                            1764\t1970-01-01T00:00:00.000004Z\tabc
                            """,
                    "select 42*42 as the_answer, ts, id from t1 where id = 'abc' limit -1",
                    null,
                    true,
                    true
            );
        });
    }
}
