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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class UnionTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testUnionAllOfAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tNaN\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.0347\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.3730\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.0111\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tNaN\t0.2357\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tNaN\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.8641\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9217\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.4634\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n";

            final String expected2 = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tNaN\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.0347\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.3730\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.0111\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tNaN\t0.2357\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tNaN\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.8641\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9217\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.4634\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n";

            compiler.compile("create table x as " +
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
                            " long_sequence(20)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );


            try (RecordCursorFactory rcf = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            compiler.compile("create table y as " +
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
                            " long_sequence(10)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile("select * from x union all y", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected2, factory, false, true);
            }
        });
    }

    //select distinct sym from a union all b
    @Test
    public void testUnionAllOfSymboFor3Tablesl() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "t\n" +
                    "CAR\n" +
                    "CAR\n" +
                    "VAN\n" +
                    "PLANE\n" +
                    "PLANE\n" +
                    "PLANE\n" +
                    "PLANE\n";

            final String expected2 = "t\n" +
                    "CAR\n" +
                    "VAN\n" +
                    "PLANE\n" +
                    "BICYCLE\n" +
                    "SCOOTER\n" +
                    "HELICOPTER\n" +
                    "MOTORBIKE\n";

            compiler.compile(
                    "CREATE TABLE x as " +
                            "(SELECT " +
                            " rnd_symbol('CAR', 'VAN', 'PLANE') t " +
                            " FROM long_sequence(7) x)" +
                            " partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory rcf = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            compiler.compile(
                    "CREATE TABLE y as " +
                            "(SELECT " +
                            " rnd_symbol('PLANE', 'BICYCLE', 'SCOOTER') t " +
                            " FROM long_sequence(7) x)" +
                            " partition by NONE",
                    sqlExecutionContext
            );

            compiler.compile(
                    "CREATE TABLE z as " +
                            "(SELECT " +
                            " rnd_symbol('MOTORBIKE', 'HELICOPTER', 'VAN') t " +
                            " FROM long_sequence(13) x)" +
                            " partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile("select distinct t from x union all y union all z", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected2, factory, false, true);
            }
        });
    }

    //select distinct sym from a union all b
    @Test
    public void testUnionAllOfSymbol() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "t\n" +
                    "CAR\n" +
                    "CAR\n" +
                    "VAN\n" +
                    "PLANE\n" +
                    "PLANE\n" +
                    "PLANE\n" +
                    "PLANE\n";

            final String expected2 = "t\n" +
                    "CAR\n" +
                    "VAN\n" +
                    "PLANE\n" +
                    "BICYCLE\n" +
                    "SCOOTER\n";

            compiler.compile(
                    "CREATE TABLE x as " +
                            "(SELECT " +
                            " rnd_symbol('CAR', 'VAN', 'PLANE') t " +
                            " FROM long_sequence(7) x)" +
                            " partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory rcf = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            compiler.compile(
                    "CREATE TABLE y as " +
                            "(SELECT " +
                            " rnd_symbol('PLANE', 'BICYCLE', 'SCOOTER') t " +
                            " FROM long_sequence(7) x)" +
                            " partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile("select distinct t from x union all y", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected2, factory, false, true);
            }
        });
    }

    @Test
    public void testExcept() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table events2 (contact symbol, groupid symbol, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('amy', 'grp1', 'flash')");
            executeInsert("insert into events2 values ('joey', 'grp2', 'sit')");
            executeInsert("insert into events2 values ('stewy', 'grp1', 'stand')");
            executeInsert("insert into events2 values ('bobby', 'grp1', 'flash')");
            executeInsert("insert into events2 values ('stewy', 'grp1', 'flash')");

            assertQuery(
                    "groupid\tcontact\n" +
                            "grp1\tamy\n" +
                            "grp1\tbobby\n",
                    "select groupid, contact from events2 where groupid = 'grp1' and eventid = 'flash'\n" +
                            "except\n" +
                            "select groupid, contact from events2 where groupid = 'grp1' and eventid = 'stand'",
                    null,
                    true
            );
        });

    }

    @Test
    public void testIntersect() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table events2 (contact symbol, groupid symbol, eventid string)", sqlExecutionContext);
            executeInsert("insert into events2 values ('amy', 'grp1', 'flash')");
            executeInsert("insert into events2 values ('joey', 'grp2', 'sit')");
            executeInsert("insert into events2 values ('stewy', 'grp1', 'stand')");
            executeInsert("insert into events2 values ('bobby', 'grp1', 'flash')");
            executeInsert("insert into events2 values ('stewy', 'grp1', 'flash')");

            assertQuery(
                    "groupid\tcontact\n" +
                            "grp1\tstewy\n",
                    "select groupid, contact from events2 where groupid = 'grp1' and eventid = 'flash'\n" +
                            "intersect\n" +
                            "select groupid, contact from events2 where groupid = 'grp1' and eventid = 'stand'",
                    null,
                    true
            );
        });

    }

    @Test
    public void testUnionAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tNaN\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.0347\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.3730\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.0111\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tNaN\t0.2357\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tNaN\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.8641\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9217\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.4634\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n";

            final String expected2 = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tNaN\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.0347\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.3730\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.0111\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tNaN\t0.2357\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tNaN\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.8641\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9217\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.4634\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n" +
                    "374615833\ttrue\tK\t0.023600615130049185\t0.1353\t553\t2015-09-02T18:25:47.825Z\tVTJW\t-7637525141395615332\t1970-01-01T05:33:20.000000Z\t7\t00000000 5d 2d 44 ea 00 81 c4 19 a1 ec 74\tMIFDYPDKOEZBR\t0x4a3ad201ebc732d67b16b1feb1be805bd3da193a04a376caa08679cf663ea669\tG\n" +
                    "126163977\tfalse\tH\t0.9321703394650436\t0.4847\t991\t2015-06-29T17:59:41.483Z\tCPSW\t-6872420962128019931\t1970-01-01T05:50:00.000000Z\t19\t00000000 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8 06 c4 06\n" +
                    "00000010 af 38 71\tBHLNEJRM\t0xd93db19428fc489eb01e38b8cbaf881782ffe46771f9b5f956897ab3e2068b4e\tI\n" +
                    "-1536345939\ttrue\tB\t0.5884931033499815\t0.7588\t805\t\tYQPZ\t5227940066601195428\t1970-01-01T00:00:00.000000Z\t15\t00000000 50 b1 8c 4d 66 e8 32 6a 9b cd bb 2e 74 cd 44 54\n" +
                    "00000010 13 3f\t\t0x34fa8218520f9a046282dda91ca20ccda519bc9c0850a07eaa0106bdfb6d9cb6\tE\n" +
                    "-1896190362\tfalse\tL\tNaN\t0.3300\t442\t2015-02-06T14:17:40.815Z\tSGQF\t8892131787872519172\t1970-01-01T00:16:40.000000Z\t41\t\t\t0x49e5a0f99d31a104a75dd7280fc9b66b84debde2e574e061463243e5bb8a743d\tK\n" +
                    "1996219179\ttrue\tZ\t0.7022152611814457\t0.3258\t410\t2015-10-24T06:25:39.828Z\t\t-8698821645604291033\t1970-01-01T00:33:20.000000Z\t25\t00000000 2a 42 71 a3 7a 58 e5 78 b8 1c d6 fc\tGZTOY\t0x6eb1dd50a390ca7e2c60ac400987268c77962e845080f34354377431fb8f0a1d\tS\n" +
                    "1403475204\tfalse\tR\t0.981074259037815\t0.9892\t483\t2015-05-09T04:42:23.511Z\t\t-4912776313422450773\t1970-01-01T00:50:00.000000Z\t48\t\tFCYQWPKLHTIIGQ\t0xb58ffdb93190ab917fb4298ae30f186b48c87eff38ac95a63fbc031330b2396e\tD\n";

            compiler.compile("create table x as " +
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
                            " long_sequence(20)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );


            try (RecordCursorFactory rcf = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            compiler.compile("create table y as " +
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
                            " long_sequence(22)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );


            compiler.compile("create table z as " +
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
                            " long_sequence(4)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile("select * from x union y union z", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected2, factory, false, true);
            }
        });
    }

    @Test
    public void testUnionAndUnionAllOfAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tNaN\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.0347\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.3730\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.0111\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tNaN\t0.2357\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tNaN\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.8641\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9217\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.4634\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n";

            final String expected2 = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\tl256\tchr\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "494704403\ttrue\tC\t0.4834201611292943\t0.7943\t28\t2015-06-16T21:00:55.459Z\tHYRX\t6785355388782691241\t1970-01-01T02:46:40.000000Z\t39\t\tRVNGSTEQOD\t0xbabcd0482f05618f926cdd99e63abb35650d1fb462d014df59070392ef6aa389\tW\n" +
                    "-173290754\ttrue\tK\t0.7198854503668188\tNaN\t114\t2015-06-15T20:39:39.538Z\tVTJW\t9064962137287142402\t1970-01-01T03:03:20.000000Z\t20\t00000000 3b 94 5f ec d3 dc f8 43 b2 e3\tTIZKYFLUHZQSNPX\t0x74c509677990f1c962588b84eddb7b4a64a4822086748dc4b096d89b65baebef\tM\n" +
                    "-2041781509\ttrue\tE\t0.44638626240707313\t0.0347\t605\t\tVTJW\t415951511685691973\t1970-01-01T03:20:00.000000Z\t28\t00000000 00 7c fb 01 19 ca f2 bf 84 5a 6f 38 35\t\t0x543554ee7efea2c341b1a691af3ce51f91a63337ac2e96836e87bac2e9746529\tW\n" +
                    "813111021\ttrue\t\t0.1389067130304884\t0.3730\t259\t\tCPSW\t4422067104162111415\t1970-01-01T03:36:40.000000Z\t19\t00000000 2d 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9\tPNXHQUTZODWKOC\t0x50902704a317faeea7fc3b8563ada5ab985499c7f07368a33b8ad671f6730aab\tP\n" +
                    "980916820\tfalse\tC\t0.8353079103853974\t0.0111\t670\t2015-10-06T01:12:57.175Z\t\t7536661420632276058\t1970-01-01T03:53:20.000000Z\t37\t\tFDBZWNIJEE\t0xb265942d3a1f96a1cff85f9258847e03a6f2e2a772cd2f3751d822a67dff3d23\tP\n" +
                    "-2016176825\ttrue\tT\tNaN\t0.2357\t813\t2015-12-27T00:19:42.415Z\tCPSW\t3464609208866088600\t1970-01-01T04:10:00.000000Z\t49\t\tFNUHNR\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\tW\n" +
                    "1173790070\tfalse\t\t0.5380626833618448\tNaN\t440\t2015-07-18T08:33:51.750Z\tCPSW\t-5206126114193456581\t1970-01-01T04:26:40.000000Z\t31\t00000000 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8 ab\n" +
                    "00000010 3f a1 f5\t\t0x8e230f5d5b94e76393ce26b5c735a6c94d2c5190fa645a018c7dd745bfadc2ea\tQ\n" +
                    "2146422524\ttrue\tI\t0.11025007918539531\t0.8641\t539\t\t\t5637967617527425113\t1970-01-01T04:43:20.000000Z\t12\t00000000 81 c6 3d bc b5 05 2b 73 51 cf c3 7e c0 1d 6c\t\t0x70c4f4015e9b707986026ba259cee6ad9c3c470dfd351e81960599005766a265\tC\n" +
                    "-1433178628\tfalse\tW\t0.9246085617322545\t0.9217\t578\t2015-06-28T14:27:36.686Z\t\t7861908914614478025\t1970-01-01T05:00:00.000000Z\t32\t00000000 0e 98 0a 8a 0b 1e c4 fd a2 9e b3 77 f8 f6 78\tRPXZSFXUNYQXT\t0x98065c0bf90d68899f5ac37d91bde62260af712d2a1cbb23579b0bab5109229c\tI\n" +
                    "94087085\ttrue\tY\t0.5675831821917149\t0.4634\t872\t2015-11-16T04:04:55.664Z\tPEHN\t8951464047863942551\t1970-01-01T05:16:40.000000Z\t26\t00000000 33 3f b2 67 da 98 47 47 bf 4f ea 5f 48 ed\tDDCIHCNP\t0x680d2fd4ebf181c6960658463c4b85af603e1494ba44804a7fa40f7e4efac82e\tP\n" +
                    "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\t0x5f20a35e80e154f458dfd08eeb9cc39ecec82869edec121bc2593f82b430328d\tE\n" +
                    "-461611463\tfalse\tJ\t0.9687423276940171\t0.6762\t279\t2015-11-21T14:32:13.134Z\tHYRX\t-6794405451419334859\t1970-01-01T00:16:40.000000Z\t6\t\tETJRSZSRYR\t0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\n" +
                    "-1515787781\tfalse\t\t0.8001121139739173\t0.1877\t759\t2015-06-17T02:40:55.328Z\tCPSW\t-4091897709796604687\t1970-01-01T00:33:20.000000Z\t6\t00000000 9c 1d 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3\tDYYCTGQOLYXWCKYL\t0x78c594c496995885aa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b047\tS\n" +
                    "1235206821\ttrue\t\t0.9540069089049732\t0.2553\t310\t\tVTJW\t6623443272143014835\t1970-01-01T00:50:00.000000Z\t17\t00000000 cc 76 48 a3 bb 64 d2 ad 49 1c f2 3c ed 39 ac\tVSJOJIPHZEPIHVLT\t0xb942438168662cb7aa21f9d816335363d27e6df7d9d5b758ea7a0db859a19e14\tU\n" +
                    "454820511\tfalse\tL\t0.9918093114862231\t0.3242\t727\t2015-02-10T08:56:03.707Z\t\t5703149806881083206\t1970-01-01T01:06:40.000000Z\t36\t00000000 68 79 8b 43 1d 57 34 04 23 8d d8 57\tWVDKFLOPJOXPK\t0x550988dbaca497348692bc8c04e4bb71d24b84c08ea7606a70061ac6a4115ca7\tH\n" +
                    "1728220848\tfalse\tO\t0.24642266252221556\t0.2672\t174\t2015-02-20T01:11:53.748Z\t\t2151565237758036093\t1970-01-01T01:23:20.000000Z\t31\t\tHZSQLDGLOGIFO\t0x8531876c963316d961f392242addf45287dd0b29ca2c4c8455b68bf3e1f27620\tZ\n" +
                    "-120660220\tfalse\tB\t0.07594017197103131\t0.0638\t542\t2015-01-16T16:01:53.328Z\tVTJW\t5048272224871876586\t1970-01-01T01:40:00.000000Z\t23\t00000000 f5 0f 2d b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35\n" +
                    "00000010 e4 3a dc 5c\tULIGYVFZ\t0x99193c2e0a9e76da695f8ae33a2cc2aa529d71aba0f6fec5172a489c48c26926\tL\n" +
                    "-1548274994\ttrue\tX\t0.9292491654871197\tNaN\t523\t2015-01-05T19:01:46.416Z\tHYRX\t9044897286885345735\t1970-01-01T01:56:40.000000Z\t16\t00000000 cd 47 0b 0c 39 12 f7 05 10 f4 6d f1 e3 ee 58 35\n" +
                    "00000010 61\tMXSLUQDYOPHNIMYF\t0x483c83d88ac674e3894499a1a1680580cfedff23a67d918fb49b3c24e456ad6e\tP\n" +
                    "1430716856\tfalse\tP\t0.7707249647497968\tNaN\t162\t2015-02-05T10:14:02.889Z\t\t7046578844650327247\t1970-01-01T02:13:20.000000Z\t47\t\tLEGPUHHIUGGLNYR\t0x36be4fe79117ebd53756b77218c738a7737b1dacd6be597192384aabd888ecb3\tD\n" +
                    "-772867311\tfalse\tQ\t0.7653255982993546\tNaN\t681\t2015-05-07T02:45:07.603Z\t\t4794469881975683047\t1970-01-01T02:30:00.000000Z\t31\t00000000 4e d6 b2 57 5b e3 71 3d 20 e2 37 f2 64 43 84 55\n" +
                    "00000010 a0 dd\tVTNPIW\t0x2d1c6f57bbfd47ec39bd4dd9ad497a2721dc4adc870c62fe19b2faa4e8255a0d\tP\n" +
                    "374615833\ttrue\tK\t0.023600615130049185\t0.1353\t553\t2015-09-02T18:25:47.825Z\tVTJW\t-7637525141395615332\t1970-01-01T05:33:20.000000Z\t7\t00000000 5d 2d 44 ea 00 81 c4 19 a1 ec 74\tMIFDYPDKOEZBR\t0x4a3ad201ebc732d67b16b1feb1be805bd3da193a04a376caa08679cf663ea669\tG\n" +
                    "126163977\tfalse\tH\t0.9321703394650436\t0.4847\t991\t2015-06-29T17:59:41.483Z\tCPSW\t-6872420962128019931\t1970-01-01T05:50:00.000000Z\t19\t00000000 47 b3 80 69 b9 14 d6 fc ee 03 22 81 b8 06 c4 06\n" +
                    "00000010 af 38 71\tBHLNEJRM\t0xd93db19428fc489eb01e38b8cbaf881782ffe46771f9b5f956897ab3e2068b4e\tI\n" +
                    "-1604872342\ttrue\tF\t0.9674352881185491\t0.3088\t643\t2015-12-07T03:56:58.742Z\tHYRX\t-6544713176186588811\t1970-01-01T06:06:40.000000Z\t35\t00000000 87 88 45 b9 9d 20 13 51 c0 e0 b7\tSNSXH\t0x5ae63bdf09a84e32bac4484bdeec40e887ec84d0151017668c17a681e308fd4d\tE\n" +
                    "172654235\tfalse\tM\tNaN\t0.8643\t184\t2015-10-10T03:50:18.267Z\t\t-6196664199248241482\t1970-01-01T06:23:20.000000Z\t50\t00000000 27 66 94 89 db 3c 1a 23 f3 88 83 73\tGJBFQ\t0xcfd0f01a76fbe32b8e7fd4a84ba9813349e5a0f99d31a104a75dd7280fc9b66b\tI\n";

            compiler.compile("create table x as " +
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
                            " long_sequence(20)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );


            try (RecordCursorFactory rcf = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected, rcf, true, true);
            }

            SharedRandom.RANDOM.get().reset();

            compiler.compile("create table y as " +
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
                            " long_sequence(10)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );


            SharedRandom.RANDOM.get().reset();

            compiler.compile("create table z as " +
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
                            " long_sequence(24)" +
                            ") partition by NONE",
                    sqlExecutionContext
            );

            try (RecordCursorFactory factory = compiler.compile("select * from x union all y union z", sqlExecutionContext).getRecordCursorFactory()) {
                assertCursor(expected2, factory, false, true);
            }
        });
    }
}
