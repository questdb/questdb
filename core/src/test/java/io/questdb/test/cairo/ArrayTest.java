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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayBuffers;
import io.questdb.cairo.arr.ArrayMeta;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.BorrowedDirectArrayView;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cutlass.line.tcp.ArrayParser;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArrayTest extends AbstractCairoTest {

    @Test
    public void testArrayToJsonDouble() {
        BorrowedDirectArrayView array = new BorrowedDirectArrayView();
        try (ArrayBuffers bufs = new ArrayBuffers();
             DirectUtf8Sink sink = new DirectUtf8Sink(20)
        ) {
            bufs.shape.add(2);
            bufs.shape.add(2);
            bufs.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, (int) bufs.shape.size());
            ArrayMeta.determineDefaultStrides(bufs.shape.asSlice(), bufs.strides);
            bufs.values.putDouble(1.0);
            bufs.values.putDouble(2.0);
            bufs.values.putDouble(3.0);
            bufs.values.putDouble(4.0);
            bufs.updateView(array);
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink);
            assertEquals("[[1.0,2.0],[3.0,4.0]]", sink.toString());

            // transpose the array
            bufs.strides.reverse();
            bufs.updateView(array);
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink);
            assertEquals("[[1.0,3.0],[2.0,4.0]]", sink.toString());
        }
    }

    @Test
    public void testArrayToJsonLong() {
        BorrowedDirectArrayView array = new BorrowedDirectArrayView();
        try (ArrayBuffers bufs = new ArrayBuffers();
             DirectUtf8Sink sink = new DirectUtf8Sink(20)
        ) {
            bufs.shape.add(2);
            bufs.shape.add(2);
            bufs.type = ColumnType.encodeArrayType(ColumnType.LONG, (int) bufs.shape.size());
            ArrayMeta.determineDefaultStrides(bufs.shape.asSlice(), bufs.strides);
            bufs.values.putLong(1);
            bufs.values.putLong(2);
            bufs.values.putLong(3);
            bufs.values.putLong(4);
            bufs.updateView(array);
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink);
            assertEquals("[[1,2],[3,4]]", sink.toString());

            // transpose the array
            bufs.strides.reverse();
            bufs.updateView(array);
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink);
            assertEquals("[[1,3],[2,4]]", sink.toString());
        }
    }

    @Test
    public void testArrayToJsonUsingParser() {
        DirectUtf8String str = new DirectUtf8String();
        try (ArrayParser parser = new ArrayParser();
             DirectUtf8Sink sink = new DirectUtf8Sink(20)
        ) {
            String arrayExpr = "[[1,2],[3,4]]";
            sink.clear();
            sink.put("[6i").put(arrayExpr.substring(1));
            parser.parse(str.of(sink.lo(), sink.hi()));
            sink.clear();
            ArrayTypeDriver.arrayToJson(parser.getView(), sink);
            assertEquals(arrayExpr, sink.toString());
        } catch (ArrayParser.ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCreateAsSelectDoubleNoWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table blah as (\n" +
                            "select rnd_varchar() a, rnd_double_array(1, 0) arr, rnd_int() b from long_sequence(10)\n" +
                            ");"
            );

            assertQuery(
                    "a\tarr\tb\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t[0.4224356661645131,0.7094360487171202,0.38539947865244994,0.0035983672154330515,0.3288176907679504,0.021651819007252326]\t-938514914\n" +
                            "Y9}#jF\t[0.9687423276940171,0.6761934857077543,0.4882051101858693,0.42281342727402726,0.810161274171258,0.5298405941762054,0.022965637512889825,0.7763904674818695,0.975019885372507]\t-1533414895\n" +
                            "K䰭\u008B\t[0.456344569609078,0.2282233596526786,0.4149517697653501,0.04645849844580874,0.6821660861001273,0.45659895188239796,0.9566236549439661]\t-45567293\n" +
                            "#F0-k\\<*i^\t[0.49428905119584543,0.6359144993891355,0.5811247005631662,0.6752509547112409,0.9540069089049732,0.2553319339703062,0.8940917126581895,0.2879973939681931,0.6806873134626418,0.625966045857722,0.8733293804420821,0.7657837745299522]\t462277692\n" +
                            "\uDB59\uDF3B룒jᷚ\t[0.6697969295620055]\t-640305320\n" +
                            "?49Mqqpk-Z\t[0.8677181848634951,0.30716667810043663,0.4274704286353759,0.021189232728939578,0.7777024823107295,0.8221637568563206,0.22631523434159562,0.18336217509438513,0.9862476361578772]\t-2053564939\n" +
                            "V~!\t[0.5346019596733254]\t1728220848\n" +
                            "%l-\t[0.05024615679069011,0.9946372046359034,0.6940904779678791,0.5391626621794673,0.7668146556860689,0.2065823085842221,0.750281471677565,0.6590829275055244,0.5708643723875381,0.3568111021227658,0.05758228485190853,0.6729405590773638,0.1010501916946902]\t1448081412\n" +
                            "ꋵ\uD96E\uDCE7uKJܜߧ\t[0.1264215196329228,0.7215959171612961,0.4440250924606578,0.6810852005509421,0.24001459007748394,0.9292491654871197,0.741970173888595,0.25353478516307626,0.2739985338660311,0.39413730502371824,0.9359814814085834]\t-715453934\n" +
                            "ͱ:աf@ץ;윦\t[0.2559680920632348,0.23493793601747937,0.5150229280217947]\t1430716856\n",
                    "select * from blah",
                    true
            );
        });
    }

    @Test
    public void testCreateTableAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "d double[][][]" +
                    ", b boolean[][][][]" +
                    ", bt byte[][][][][][][][]" +
                    ", f float[]" +
                    ", i int[][]" +
                    ", l long[][]" +
                    ", s short[][][][][]" +
                    ", dt date[][][][]" +
                    ", ts timestamp[][]" +
                    ", l2 long256[][][]" +
                    ", u uuid[][][][]" +
                    ", ip ipv4[][]" +
                    ", c double)");

            String[] expectedColumnNames = {
                    "d",
                    "b",
                    "bt",
                    "f",
                    "i",
                    "l",
                    "s",
                    "dt",
                    "ts",
                    "l2",
                    "u",
                    "ip",
                    "c"
            };

            String[] expectedColumnTypes = {
                    "DOUBLE[][][]",
                    "BOOLEAN[][][][]",
                    "BYTE[][][][][][][][]",
                    "FLOAT[]",
                    "INT[][]",
                    "LONG[][]",
                    "SHORT[][][][][]",
                    "DATE[][][][]",
                    "TIMESTAMP[][]",
                    "LONG256[][][]",
                    "UUID[][][][]",
                    "IPv4[][]",
                    "DOUBLE"
            };

            Assert.assertEquals(expectedColumnNames.length, expectedColumnTypes.length);
            // check the metadata
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                Assert.assertEquals(expectedColumnNames.length, m.getColumnCount());

                for (int i = 0, n = expectedColumnNames.length; i < n; i++) {
                    Assert.assertEquals(expectedColumnNames[i], m.getColumnName(i));
                    Assert.assertEquals(expectedColumnTypes[i], ColumnType.nameOf(m.getColumnType(i)));
                }
            }
        });
    }

    @Test
    public void testInsertAsSelectDoubleNoWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table blah (a double[][])");
            execute("insert into blah select rnd_double_array(2, 1) from long_sequence(10)");

            assertQuery(
                    "a\n" +
                            "[[null,null,null,null]]\n" +
                            "[[0.9344604857394011,0.8423410920883345],[null,0.22452340856088226],[null,0.6254021542412018]]\n" +
                            "[[null,0.5599161804800813,null],[0.2390529010846525,0.6693837147631712,null],[null,0.38539947865244994,0.0035983672154330515],[0.3288176907679504,null,0.9771103146051203],[0.24808812376657652,0.6381607531178513,null],[null,0.38179758047769774,0.12026122412833129],[0.6700476391801052,0.3435685332942956,null],[null,0.810161274171258,null],[null,null,null],[null,null,null]]\n" +
                            "[[0.975019885372507,null,null,0.931192737286751,null,0.8001121139739173,null,0.92050039469858,null,0.456344569609078,null,0.40455469747939254,null,0.5659429139861241],[0.8828228366697741,null,null,null,null,0.9566236549439661,null,null,null,0.8164182592467494,null,0.5449155021518948,null,null],[null,0.9640289041849747,0.7133910271555843,null,0.5891216483879789,null,null,0.48558682958070665,null,0.44804689668613573,null,null,0.14830552335848957,null],[null,0.6806873134626418,0.625966045857722,0.8733293804420821,null,0.17833722747266334,null,null,0.026836863013701473,0.03993124821273464,null,null,null,0.07246172621937097],[0.4892743433711657,0.8231249461985348,null,0.4295631643526773,null,0.7632615004324503,0.8816905018995145,null,0.5357010561860446,null,null,null,null,0.7458169804091256],[null,0.4274704286353759,null,null,null,0.7777024823107295,null,0.7445998836567925,0.2825582712777682,0.2711532808184136,null,null,null,null],[0.029080850168636263,0.10459352312331183,null,0.20585069039325443,null,0.9884011094887449,0.9457212646911386,0.05024615679069011,0.9946372046359034,0.6940904779678791,0.5391626621794673,null,0.4416432347777828,null]]\n" +
                            "[[0.2445295612285482,null,0.043606408996349044,null]]\n" +
                            "[[null,null,null,null,null,null,0.06381657870188628,null,0.35731092171284307,0.9583687530177664,null],[null,null,null,null,0.6069927532469744,null,null,null,null,0.062027497477155635,0.6901976778065181],[0.7586254118589676,null,null,null,null,null,0.5677191487344088,0.2677326840703891,null,0.23507754029460548,0.20727557301543031],[null,0.9292491654871197,null,0.49154607371672154,0.4167781163798937,0.3454148777596554,null,null,null,0.9423671624137644,null],[null,0.7873229912811514,null,null,0.5859332388599638,null,0.3460851141092931,null,null,null,null],[null,null,0.6226001464598434,0.4346135812930124,0.8786111112537701,0.996637725831904,null,0.30028279396280155,null,0.8196554745841765,0.9130151105125102],[null,null,null,null,null,null,null,0.007985454958725269,0.5090837921075583,null,null],[null,null,0.8775452659546193,0.8379891991223047,null,null,null,null,null,null,0.5626370294064983],[null,0.49199001716312474,0.6292086569587337,null,null,0.5779007672652298,0.5692090442741059,null,null,null,null],[null,0.7732229848518976,0.587752738240427,0.4667778758533798,null,0.7202789791127316,null,0.7407842990690816,0.9790787740413469,null,0.7527907209539796]]\n" +
                            "[[null,null,0.848083900630095,0.4698648140712085,0.8911615631017953,null,null,0.7431472218131966,0.5889504900909748,null,null,null]]\n" +
                            "[[null],[null],[null],[null]]\n" +
                            "[[0.8574212636138532,0.8280460741052847,0.7842455970681089,null,null,null,0.3889200123396954,0.933609514582851],[null,0.17202485647400034,null,0.4610963091405301,0.5673376522667354,0.48782086416459025,null,0.13312214396754163],[0.9435138098640453,null,0.17094358360735395,null,null,null,0.5449970817079417,null],[null,null,null,0.3058008320091107,null,null,0.6479617440673516,0.5900836401674938],[0.12217702189166091,0.7717552767944976,null,null,0.3153349572730255,0.4627885105398635,0.4028291715584078,null],[null,null,null,null,null,0.5501133139397699,0.7134500775259477,null],[0.734728770956117,null,null,0.8531407145325477,null,0.009302399817494589,null,null],[0.32824342042623134,null,0.4086323159337839,null,null,null,null,null],[null,0.04404000858917945,0.14295673988709012,null,null,0.36078878996232167,null,null],[null,0.7397816490927717,null,0.8386104714017393,0.9561778292078881,0.11048000399634927,null,null],[0.11947100943679911,null,null,null,null,null,0.5335953576307257,null],[0.8504099903010793,null,null,null,null,0.15274858078119136,null,0.7468602267994937],[0.55200903114214,null,0.12483505553793961,0.024056391028085766,null,null,0.3663509090570607,null],[null,0.6940917925148332,null,null,0.4564667537900823,null,0.4412051102084278,null]]\n" +
                            "[[null,null,null,0.97613283653158,null,0.7707892345682454,0.8782062052833822,null,null],[0.8376372223926546,0.365427022047211,null,null,null,null,null,0.31861843394057765,null],[0.9370193388878216,0.39201296350741366,null,0.28813952005117305,0.65372393289891,null,0.9375691350784857,null,0.4913342104187668],[null,null,null,null,null,null,null,0.38881940598288367,0.4444125234732249],[null,null,null,null,0.5261234649527643,0.030750139424332357,0.20921704056371593,0.681606585145203,0.11134244333117826],[null,0.5863937813368164,0.2103287968720018,0.3242526975448907,0.42558021324800144,null,0.6068565916347403,null,0.004918542726028763],[null,0.008134052047644613,0.1339704489137793,0.4950615235019964,0.04558283749364911,0.3595576962747611,null,null,0.0966240354078981],[null,null,null,null,0.9694731343686098,0.24584615213823513,null,0.5965069739835686,null],[0.16979644136429572,0.28122627418701307,null,null,0.8545896910200949,null,0.40609845936584743,0.041230021906704994,null],[0.6852762111021103,0.08039440728458325,0.7600550885615773,0.05890936334115593,0.023600615130049185,null,0.7630648900646654,null,null],[0.05995797344646303,0.7600677648426976,null,0.6198590038961462,null,0.7280036952357564,0.6404197786416339,0.828928908465152,null],[null,0.38000152873098747,0.5157225592346661,null,0.16320835762949149,0.6952925744703682,null,null,null],[null,null,null,0.535993442770838,0.5725722946886976,null,null,null,null],[0.38392106356809774,null,null,0.6172681090021809,0.5320636725174561,0.13226561658653546,null,0.7553832117277283,0.12715627282156716]]\n",
                    "select * from blah",
                    true
            );
        });
    }

    @Test
    public void testTypeCast() {
        for (int i = 1; i < ColumnType.ARRAY_NDIMS_LIMIT; i++) {
            for (short j = ColumnType.BOOLEAN; j <= ColumnType.IPv4; j++) {
                if (!ColumnType.isSupportedArrayElementType(j)) {
                    continue;
                }
                Assert.assertTrue(ColumnType.isAssignableFrom(
                        ColumnType.encodeArrayType(j, i),
                        ColumnType.encodeArrayType(j, i)
                ));
                Assert.assertTrue(ColumnType.isAssignableFrom(
                        ColumnType.NULL,
                        ColumnType.encodeArrayType(j, i)
                ));
            }
        }

        for (int i = 1; i < ColumnType.ARRAY_NDIMS_LIMIT; i++) {
            for (short j = ColumnType.BOOLEAN; j <= ColumnType.IPv4; j++) {
                if (!ColumnType.isSupportedArrayElementType(j)) {
                    continue;
                }
                // not assignable from scalar to any array
                Assert.assertFalse(ColumnType.isAssignableFrom(j, ColumnType.encodeArrayType(j, i)));
                // ... nor the other way around
                Assert.assertFalse(ColumnType.isAssignableFrom(ColumnType.encodeArrayType(j, i), j));
            }
        }
    }

    @Test
    public void testUnsupportedArrayDimension() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a int[][][][][][][][][][][][][][][][])");
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                Assert.assertEquals(1, m.getColumnCount());
                Assert.assertEquals("a", m.getColumnName(0));
                Assert.assertEquals("INT[][][][][][][][][][][][][][][][]", ColumnType.nameOf(m.getColumnType(0)));
            }
            assertExceptionNoLeakCheck(
                    "create table y (a int[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][])", // 33 dimensions
                    18,
                    "array dimension limit is 32"
            );
        });
    }

    @Test
    public void testUnsupportedArrayType() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "create table x (a symbol[][][])",
                    18,
                    "SYMBOL array type is not supported"
            );
            assertExceptionNoLeakCheck(
                    "create table x (abc varchar[][][])",
                    20,
                    "VARCHAR array type is not supported"
            );
        });
    }

}
