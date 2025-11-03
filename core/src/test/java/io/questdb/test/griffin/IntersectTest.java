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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class IntersectTest extends AbstractCairoTest {

    @Test
    public void testIntersectAllCastDuplicateRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (SELECT rnd_int(1,5,0) i, rnd_symbol('A', 'B', 'C') s FROM long_sequence(10))");
            execute("create table y as (SELECT rnd_int(1,5,0) i, rnd_str('A', 'B', 'C') s FROM long_sequence(3))");

            final String expected = "i\ts\n" +
                    "4\tB\n" +
                    "2\tC\n" +
                    "2\tC\n" + // <--- duplicate here
                    "4\tB\n";  // <--- duplicate here

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("(select i,s from x) intersect all (select i,s from y)")) {
                assertCursor(expected, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectAllDuplicateRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (SELECT rnd_int(1,5,0) i, rnd_symbol('A', 'B', 'C') s FROM long_sequence(10))");
            execute("create table y as (SELECT rnd_int(1,5,0) i, rnd_symbol('A', 'B', 'C') s FROM long_sequence(20))");

            final String expected = "i\ts\n" +
                    "4\tB\n" +
                    "2\tC\n" +
                    "2\tC\n" + // <--- duplicate here
                    "4\tB\n" + // <--- duplicate here
                    "5\tB\n" +
                    "1\tA\n";

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("(select i,s from x) intersect all (select i,first(s) from y)")) {
                assertCursor(expected, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectAllOfAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6761935\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.18769705\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.25533193\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.32424557\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.06381655\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tnull\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tnull\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tnull\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7942522\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tnull\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.034652293\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.37303334\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.011099219\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tnull\t0.23567414\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tnull\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.86413944\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9216729\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.46343064\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n";

            final String expected2 = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6761935\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.18769705\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.25533193\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.32424557\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.06381655\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tnull\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tnull\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tnull\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n";

            execute(
                    "create table x as " +
                            "(" +
                            "select" +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() l256," +
                            " rnd_char() chr" +
                            " from" +
                            " long_sequence(20))"
            );

            snapshotMemoryUsage();
            try (RecordCursorFactory rcf = select("x")) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            execute(
                    "create table y as " +
                            "(" +
                            "select" +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() l256," +
                            " rnd_char() chr" +
                            " from" +
                            " long_sequence(10))"
            );
            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select * from x intersect all y")) {
                assertCursor(expected2, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectAllOfSymbolFor3Tables() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "t\n" +
                    "CAR\n" +
                    "CAR\n" +
                    "VAN\n" +
                    "MOTORBIKE\n" +
                    "MOTORBIKE\n" +
                    "MOTORBIKE\n" +
                    "MOTORBIKE\n";

            final String expected2 = "t\n" +
                    "MOTORBIKE\n";

            execute(
                    "CREATE TABLE x as " +
                            "(SELECT " +
                            " rnd_symbol('CAR', 'VAN', 'MOTORBIKE') t " +
                            " FROM long_sequence(7) x)"
            );
            snapshotMemoryUsage();
            try (RecordCursorFactory rcf = select("x")) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            execute(
                    "CREATE TABLE y as " +
                            "(SELECT " +
                            " rnd_symbol('PLANE', 'MOTORBIKE', 'SCOOTER') t " +
                            " FROM long_sequence(7) x)"
            );

            execute(
                    "CREATE TABLE z as " +
                            "(SELECT " +
                            " rnd_symbol('MOTORBIKE', 'HELICOPTER', 'VAN') t " +
                            " FROM long_sequence(13) x)"
            ); //produces HELICOPTER MOTORBIKE HELICOPTER HELICOPTER VAN HELICOPTER HELICOPTER HELICOPTER MOTORBIKE MOTORBIKE HELICOPTER MOTORBIKE HELICOPTER

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select distinct t from x intersect all y intersect all z")) {
                assertCursor(expected2, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectAllWithTimestampNs() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trades1 (trade_id int, ts_nano timestamp_ns, price double)");
            execute("create table trades2 (trade_id int, ts_nano timestamp_ns, price double)");

            long baseNanos = 1_609_459_200_000_000_000L; // 2021-01-01T00:00:00.000000000

            // trades1 - with duplicates
            execute("insert into trades1 values (1, " + baseNanos + ", 100.0)");
            execute("insert into trades1 values (2, " + (baseNanos + 999_999_999L) + ", 200.0)"); // +999.999999ms
            execute("insert into trades1 values (2, " + (baseNanos + 999_999_999L) + ", 200.0)"); // duplicate
            execute("insert into trades1 values (3, " + (baseNanos + 2_000_000_789L) + ", 300.0)"); // +2.000000789s
            execute("insert into trades1 values (4, " + (baseNanos + 3_000_000_000L) + ", 400.0)"); // +3s

            // trades2 - intersection set (with one duplicate matching)
            execute("insert into trades2 values (2, " + (baseNanos + 999_999_999L) + ", 200.0)");
            execute("insert into trades2 values (3, " + (baseNanos + 2_000_000_789L) + ", 300.0)");
            execute("insert into trades2 values (5, " + (baseNanos + 4_000_000_000L) + ", 500.0)"); // not in trades1

            final String expected = "trade_id\tts_nano\tprice\n" +
                    "2\t2021-01-01T00:00:00.999999999Z\t200.0\n" +
                    "2\t2021-01-01T00:00:00.999999999Z\t200.0\n" +
                    "3\t2021-01-01T00:00:02.000000789Z\t300.0\n";


            assertQueryNoLeakCheck(expected, "select * from trades1 intersect all select * from trades2", null, true, false);
        });
    }

    @Test
    public void testIntersectCastNoDuplicateRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (SELECT rnd_int(1,5,0) i, rnd_symbol('A', 'B', 'C') s FROM long_sequence(10))");
            execute("create table y as (SELECT rnd_int(1,5,0) i, rnd_str('A', 'B', 'C') s FROM long_sequence(3))");

            final String expected = "i\ts\n" +
                    "4\tB\n" +
                    "2\tC\n";

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("(select i,s from x) intersect (select i,s from y)")) {
                assertCursor(expected, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectMixedTimestampPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table micro_events (id int, ts_micro timestamp, type symbol)");
            execute("create table nano_events (id int, ts_nano timestamp_ns, type symbol)");

            long baseMicros = 1_577_836_800_123_456L; // 2020-01-01T00:00:00.123456
            long baseNanos = baseMicros * 1000;

            execute("insert into micro_events values (1, " + baseMicros + ", 'A')");
            execute("insert into micro_events values (2, " + (baseMicros + 1000) + ", 'B')"); // +1ms
            execute("insert into micro_events values (3, " + (baseMicros + 2000) + ", 'C')"); // +2ms

            execute("insert into nano_events values (1, " + baseNanos + ", 'A')"); // exact match
            execute("insert into nano_events values (2, " + ((baseMicros + 1000) * 1000 + 123) + ", 'B')"); // +1ms+123ns
            execute("insert into nano_events values (4, " + (baseNanos + 5_000_000L) + ", 'D')"); // not in micro_events

            final String expected = "id\tts_micro\ttype\n" +
                    "1\t2020-01-01T00:00:00.123456Z\tA\n" +
                    "2\t2020-01-01T00:00:00.124456Z\tB\n";

            // explicit cast to micros truncates nanos: 2 matching rows
            assertQueryNoLeakCheck(expected,
                    "select id, ts_micro, type from micro_events " +
                            "intersect " +
                            "select id, ts_nano::timestamp as ts_micro, type from nano_events",
                    null, true, false);

            // implicit cast promotes micros to nanos -> only the first row matches
            assertQueryNoLeakCheck("id\tts\ttype\n" +
                            "1\t2020-01-01T00:00:00.123456000Z\tA\n",
                    "select id, ts_micro as ts, type from micro_events " +
                            "intersect " +
                            "select id, ts_nano as ts, type from nano_events",
                    null, true, false);
        });
    }

    @Test
    public void testIntersectNoDuplicateRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (SELECT rnd_int(1,5,0) i, rnd_symbol('A', 'B', 'C') s FROM long_sequence(10))");
            execute("create table y as (SELECT rnd_int(1,5,0) i, rnd_symbol('A', 'B', 'C') s FROM long_sequence(20))");

            final String expected = "i\ts\n" +
                    "4\tB\n" +
                    "2\tC\n" +
                    "5\tB\n" +
                    "1\tA\n";

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("(select i,s from x) intersect (select i,first(s) from y)")) {
                assertCursor(expected, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectOfAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6761935\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.18769705\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.25533193\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.32424557\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.06381655\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tnull\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tnull\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tnull\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7942522\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tnull\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.034652293\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.37303334\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.011099219\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tnull\t0.23567414\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tnull\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.86413944\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9216729\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.46343064\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n";

            final String expected2 = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tnull\t0.7611029\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6761935\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.18769705\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.25533193\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.32424557\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.26721203\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.06381655\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tnull\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tnull\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tnull\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n";

            execute(
                    "create table x as " +
                            "(" +
                            "select" +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() l256," +
                            " rnd_char() chr" +
                            " from" +
                            " long_sequence(20))"
            );

            snapshotMemoryUsage();
            try (RecordCursorFactory rcf = select("x")) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            execute(
                    "create table y as " +
                            "(" +
                            "select" +
                            " rnd_int() a," +
                            " rnd_boolean() b," +
                            " rnd_str(1,1,2) c," +
                            " rnd_double(2) d," +
                            " rnd_float(2) e," +
                            " rnd_short(10,1024) f," +
                            " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                            " rnd_symbol(4,4,4,2) i," +
                            " rnd_long() j," +
                            " timestamp_sequence(0, 1000000000) k," +
                            " rnd_byte(2,50) l," +
                            " rnd_bin(10, 20, 2) m," +
                            " rnd_str(5,16,2) n," +
                            " rnd_long256() l256," +
                            " rnd_char() chr" +
                            " from" +
                            " long_sequence(10))"
            );
            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select * from x intersect y")) {
                assertCursor(expected2, factory, true, false);
            }
        });
    }

    @Test
    public void testIntersectWithTimestampNs() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table events1 (event_id int, ts_nano timestamp_ns, type symbol)");
            execute("create table events2 (event_id int, ts_nano timestamp_ns, type symbol)");

            long baseNanos = 1_577_836_800_000_000_000L; // 2020-01-01T00:00:00.000000000

            execute("insert into events1 values (1, " + baseNanos + ", 'click')");
            execute("insert into events1 values (2, " + (baseNanos + 123_456_789L) + ", 'view')"); // +123.456789ms
            execute("insert into events1 values (3, " + (baseNanos + 500_000_000L) + ", 'scroll')"); // +500ms
            execute("insert into events1 values (4, " + (baseNanos + 1_000_000_000L) + ", 'click')"); // +1s
            execute("insert into events1 values (5, " + (baseNanos + 1_500_000_123L) + ", 'close')"); // +1.500000123s

            execute("insert into events2 values (2, " + (baseNanos + 123_456_789L) + ", 'view')");
            execute("insert into events2 values (4, " + (baseNanos + 1_000_000_000L) + ", 'click')");
            execute("insert into events2 values (6, " + (baseNanos + 2_000_000_000L) + ", 'exit')"); // not in events1

            final String expected = "event_id\tts_nano\ttype\n" +
                    "2\t2020-01-01T00:00:00.123456789Z\tview\n" +
                    "4\t2020-01-01T00:00:01.000000000Z\tclick\n";

            assertQueryNoLeakCheck(expected, "select * from events1 intersect select * from events2", null, true, false);
        });
    }

    @Test
    public void testIntersectWithTimestampNsAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table sensors1 (sensor_id int, ts_nano timestamp_ns, reading double)");
            execute("create table sensors2 (sensor_id int, ts_nano timestamp_ns, reading double)");

            long baseNanos = 1_640_995_200_000_000_000L; // 2022-01-01T00:00:00.000000000

            // sensors1 - with nulls
            execute("insert into sensors1 values (1, " + baseNanos + ", 25.5)");
            execute("insert into sensors1 values (2, null, 30.0)");
            execute("insert into sensors1 values (3, " + (baseNanos + 1_111_111_111L) + ", null)"); // +1.111111111s
            execute("insert into sensors1 values (4, " + (baseNanos + 2_222_222_222L) + ", 35.5)"); // +2.222222222s

            execute("insert into sensors2 values (1, " + baseNanos + ", 25.5)");
            execute("insert into sensors2 values (2, null, 30.0)");
            execute("insert into sensors2 values (5, " + (baseNanos + 5_000_000_000L) + ", 40.0)"); // not in sensors1

            final String expected = "sensor_id\tts_nano\treading\n" +
                    "1\t2022-01-01T00:00:00.000000000Z\t25.5\n" +
                    "2\t\t30.0\n";

            assertQueryNoLeakCheck(expected, "select * from sensors1 intersect select * from sensors2", null, true, false);
        });
    }
}
