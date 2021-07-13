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

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.groupby.vect.GroupByJob;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.questdb.griffin.CompiledQuery.CREATE_TABLE;

public class SqlCodeGeneratorTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testAmbiguousFunction() throws Exception {
        assertQuery("column\n" +
                        "234990000000000\n",
                "select 23499000000000*10 from long_sequence(1)",
                null, null, true, true, true
        );
    }

    @Test
    public void testAvgDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(50.03730496259993, r.avgDouble(0), 0.00001);
        }
    }

    @Test
    public void testAvgDoubleColumnPartitionByNone() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k)",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(50.03730496259993, r.avgDouble(0), 0.00001);
        }
    }

    @Test
    public void testAvgDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(49.99614105606191, r.avgDouble(0), 0.00001);
        }
    }

    @Test
    public void testAvgDoubleEmptyColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k)",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(0, r.avgDouble(0), 0.00001);
        }
    }

    @Test
    public void testBindVariableInIndexLookup() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE 'alcatel_traffic_tmp' (deviceName SYMBOL capacity 1000 index, time TIMESTAMP, slot SYMBOL, port SYMBOL, downStream DOUBLE, upStream DOUBLE) timestamp(time) partition by DAY", sqlExecutionContext);
            compiler.compile("create table src as (select rnd_symbol(15000, 4,4,0) sym, timestamp_sequence(0, 100000) ts, rnd_double() val from long_sequence(5000000))", sqlExecutionContext);
            compiler.compile("insert into alcatel_traffic_tmp select sym, ts, sym, null, val, val from src", sqlExecutionContext);
            try (
                    RecordCursorFactory factory = compiler.compile("select distinct deviceName from alcatel_traffic_tmp", sqlExecutionContext).getRecordCursorFactory();
                    RecordCursorFactory lookupFactory = compiler.compile("select * from alcatel_traffic_tmp where deviceName = $1", sqlExecutionContext).getRecordCursorFactory()
            ) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final Record deviceNameRecord = cursor.getRecord();
                    while (cursor.hasNext()) {
                        CharSequence device = deviceNameRecord.getStr(0);

                        bindVariableService.clear();
                        bindVariableService.setStr(0, device);
                        try (RecordCursor lookupCursor = lookupFactory.getCursor(sqlExecutionContext)) {
                            Record lookupRecord = lookupCursor.getRecord();
                            final boolean hasNext = lookupCursor.hasNext();
                            Assert.assertTrue(hasNext);
                            do {
                                TestUtils.assertEquals(device, lookupRecord.getSym(0));
                                TestUtils.assertEquals(device, lookupRecord.getSym(2));
                            } while (lookupCursor.hasNext());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBindVariableInIndexLookupList() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE 'alcatel_traffic_tmp' (deviceName SYMBOL capacity 1000 index, time TIMESTAMP, slot SYMBOL, port SYMBOL, downStream DOUBLE, upStream DOUBLE) timestamp(time) partition by DAY", sqlExecutionContext);
            compiler.compile("create table src as (select rnd_symbol(15000, 4,4,0) sym, timestamp_sequence(0, 100000) ts, rnd_double() val from long_sequence(500))", sqlExecutionContext);
            compiler.compile("insert into alcatel_traffic_tmp select sym, ts, sym, null, val, val from src", sqlExecutionContext);
            try (
                    RecordCursorFactory lookupFactory = compiler.compile("select * from alcatel_traffic_tmp where deviceName in ($1,$2)", sqlExecutionContext).getRecordCursorFactory()
            ) {
                bindVariableService.setStr(0, "FKBW");
                bindVariableService.setStr(1, "SHRI");


                sink.clear();
                try (RecordCursor cursor = lookupFactory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, lookupFactory.getMetadata(), true, sink);
                    TestUtils.assertEquals(
                            "deviceName\ttime\tslot\tport\tdownStream\tupStream\n" +
                                    "FKBW\t1970-01-01T00:00:02.300000Z\tFKBW\t\t0.04998168904446332\t0.04998168904446332\n" +
                                    "SHRI\t1970-01-01T00:00:02.900000Z\tSHRI\t\t0.007781200348629724\t0.007781200348629724\n",
                            sink
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInSelect() throws Exception {
        assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 10);
                try (RecordCursorFactory factory = compiler.compile("select x, $1 from long_sequence(2)", sqlExecutionContext).getRecordCursorFactory()) {
                    assertCursor("x\t$1\n" +
                                    "1\t10\n" +
                                    "2\t10\n",
                            factory,
                            true,
                            true,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInSelect2() throws Exception {
        assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong("y", 10);
                try (RecordCursorFactory factory = compiler.compile("select x, :y from long_sequence(2)", sqlExecutionContext).getRecordCursorFactory()) {
                    assertCursor("x\t:y\n" +
                                    "1\t10\n" +
                                    "2\t10\n",
                            factory,
                            true,
                            true,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInSelect3() throws Exception {
        assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 10);
                try (RecordCursorFactory factory = compiler.compile("select x, $1 from long_sequence(2)", sqlExecutionContext).getRecordCursorFactory()) {
                    assertCursor("x\t$1\n" +
                                    "1\t10\n" +
                                    "2\t10\n",
                            factory,
                            true,
                            true,
                            true
                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInWhere() throws Exception {
        assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong(0, 10);
                try (RecordCursorFactory factory = compiler.compile("select x from long_sequence(100) where x = $1", sqlExecutionContext).getRecordCursorFactory()) {
                    assertCursor("x\n" +
                                    "10\n",
                            factory,
                            true,
                            true,
                            false

                    );
                }
            }
        });
    }

    @Test
    public void testBindVariableInvalid() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE 'alcatel_traffic_tmp' (deviceName SYMBOL capacity 1000 index, time TIMESTAMP, slot SYMBOL, port SYMBOL, downStream DOUBLE, upStream DOUBLE) timestamp(time) partition by DAY", sqlExecutionContext);
            try {
                compiler.compile("select * from alcatel_traffic_tmp where deviceName in ($n1)", sqlExecutionContext).getRecordCursorFactory();
            } catch (SqlException e) {
                Assert.assertEquals(51, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "invalid bind variable index [value=$n1]");
            }
        });
    }

    @Test
    public void testBindVariableWithILike() throws Exception {
        testBindVariableWithLike0("ilike");
    }

    @Test
    public void testBindVariableWithLike() throws Exception {
        testBindVariableWithLike0("like");
    }

    @Test
    public void testBug484() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery("sym\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n" +
                        "cc\n",
                "select * from x2 where sym in (select distinct sym from x2 where sym  in (select distinct sym from x2 where sym = 'cc')) and test_match()",
                "create table x2 as (select rnd_symbol('aa','bb','cc') sym from long_sequence(50))",
                null
        );

        // also good numbers, extra top calls are due to symbol column API check
        // tables without symbol columns will skip this check
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testCreateTableAsSelectUsesQueryTimestamp() throws Exception {
        assertQuery(
                "a\tb\tt\n" +
                        "-1148479920\t0.8043224099968393\t1970-01-01T00:00:00.000000Z\n" +
                        "-727724771\t0.08486964232560668\t1970-01-01T02:46:40.000000Z\n" +
                        "1326447242\t0.0843832076262595\t1970-01-01T05:33:20.000000Z\n" +
                        "-847531048\t0.6508594025855301\t1970-01-01T08:20:00.000000Z\n" +
                        "-1436881714\t0.7905675319675964\t1970-01-01T11:06:40.000000Z\n" +
                        "1545253512\t0.22452340856088226\t1970-01-01T13:53:20.000000Z\n" +
                        "-409854405\t0.3491070363730514\t1970-01-01T16:40:00.000000Z\n" +
                        "1904508147\t0.7611029514995744\t1970-01-01T19:26:40.000000Z\n" +
                        "1125579207\t0.4217768841969397\t1970-01-01T22:13:20.000000Z\n" +
                        "426455968\t0.0367581207471136\t1970-01-02T01:00:00.000000Z\n" +
                        "-1844391305\t0.6276954028373309\t1970-01-02T03:46:40.000000Z\n" +
                        "-1153445279\t0.6778564558839208\t1970-01-02T06:33:20.000000Z\n" +
                        "-1125169127\t0.8756771741121929\t1970-01-02T09:20:00.000000Z\n" +
                        "-1252906348\t0.8799634725391621\t1970-01-02T12:06:40.000000Z\n" +
                        "-2119387831\t0.5249321062686694\t1970-01-02T14:53:20.000000Z\n" +
                        "1110979454\t0.7675673070796104\t1970-01-02T17:40:00.000000Z\n" +
                        "-422941535\t0.21583224269349388\t1970-01-02T20:26:40.000000Z\n" +
                        "-1271909747\t0.15786635599554755\t1970-01-02T23:13:20.000000Z\n" +
                        "-2132716300\t0.1911234617573182\t1970-01-03T02:00:00.000000Z\n" +
                        "-27395319\t0.5793466326862211\t1970-01-03T04:46:40.000000Z\n" +
                        "-483853667\t0.9687423276940171\t1970-01-03T07:33:20.000000Z\n" +
                        "-1272693194\t0.6761934857077543\t1970-01-03T10:20:00.000000Z\n" +
                        "-2002373666\t0.4882051101858693\t1970-01-03T13:06:40.000000Z\n" +
                        "410717394\t0.42281342727402726\t1970-01-03T15:53:20.000000Z\n" +
                        "-1418341054\t0.810161274171258\t1970-01-03T18:40:00.000000Z\n" +
                        "-530317703\t0.5298405941762054\t1970-01-03T21:26:40.000000Z\n" +
                        "936627841\t0.022965637512889825\t1970-01-04T00:13:20.000000Z\n" +
                        "-1870444467\t0.7763904674818695\t1970-01-04T03:00:00.000000Z\n" +
                        "1637847416\t0.975019885372507\t1970-01-04T05:46:40.000000Z\n" +
                        "-1533414895\t0.0011075361080621349\t1970-01-04T08:33:20.000000Z\n" +
                        "-1515787781\t0.7643643144642823\t1970-01-04T11:20:00.000000Z\n" +
                        "1920890138\t0.8001121139739173\t1970-01-04T14:06:40.000000Z\n" +
                        "-1538602195\t0.18769708157331322\t1970-01-04T16:53:20.000000Z\n" +
                        "-235358133\t0.16381374773748514\t1970-01-04T19:40:00.000000Z\n" +
                        "-10505757\t0.6590341607692226\t1970-01-04T22:26:40.000000Z\n" +
                        "-661194722\t0.40455469747939254\t1970-01-05T01:13:20.000000Z\n" +
                        "1196016669\t0.8837421918800907\t1970-01-05T04:00:00.000000Z\n" +
                        "-1269042121\t0.05384400312338511\t1970-01-05T06:46:40.000000Z\n" +
                        "1876812930\t0.09750574414434399\t1970-01-05T09:33:20.000000Z\n" +
                        "-1424048819\t0.9644183832564398\t1970-01-05T12:20:00.000000Z\n" +
                        "1234796102\t0.7588175403454873\t1970-01-05T15:06:40.000000Z\n" +
                        "1775935667\t0.5778947915182423\t1970-01-05T17:53:20.000000Z\n" +
                        "-916132123\t0.9269068519549879\t1970-01-05T20:40:00.000000Z\n" +
                        "215354468\t0.5449155021518948\t1970-01-05T23:26:40.000000Z\n" +
                        "-731466113\t0.1202416087573498\t1970-01-06T02:13:20.000000Z\n" +
                        "852921272\t0.9640289041849747\t1970-01-06T05:00:00.000000Z\n" +
                        "-1172180184\t0.7133910271555843\t1970-01-06T07:46:40.000000Z\n" +
                        "1254404167\t0.6551335839796312\t1970-01-06T10:33:20.000000Z\n" +
                        "-1768335227\t0.4971342426836798\t1970-01-06T13:20:00.000000Z\n" +
                        "1060917944\t0.48558682958070665\t1970-01-06T16:06:40.000000Z\n" +
                        "-876466531\t0.9047642416961028\t1970-01-06T18:53:20.000000Z\n" +
                        "1864113037\t0.03167026265669903\t1970-01-06T21:40:00.000000Z\n" +
                        "838743782\t0.14830552335848957\t1970-01-07T00:26:40.000000Z\n" +
                        "844704299\t0.9441658975532605\t1970-01-07T03:13:20.000000Z\n" +
                        "-2043803188\t0.3456897991538844\t1970-01-07T06:00:00.000000Z\n" +
                        "1335037859\t0.24008362859107102\t1970-01-07T08:46:40.000000Z\n" +
                        "-2088317486\t0.619291960382302\t1970-01-07T11:33:20.000000Z\n" +
                        "1743740444\t0.17833722747266334\t1970-01-07T14:20:00.000000Z\n" +
                        "614536941\t0.2185865835029681\t1970-01-07T17:06:40.000000Z\n" +
                        "-942999384\t0.3901731258748704\t1970-01-07T19:53:20.000000Z\n" +
                        "-283321892\t0.7056586460237274\t1970-01-07T22:40:00.000000Z\n" +
                        "502711083\t0.8438459563914771\t1970-01-08T01:26:40.000000Z\n" +
                        "-636975106\t0.13006100084163252\t1970-01-08T04:13:20.000000Z\n" +
                        "359345889\t0.3679848625908545\t1970-01-08T07:00:00.000000Z\n" +
                        "1362833895\t0.06944480046327317\t1970-01-08T09:46:40.000000Z\n" +
                        "1503763988\t0.4295631643526773\t1970-01-08T12:33:20.000000Z\n" +
                        "1751526583\t0.5893398488053903\t1970-01-08T15:20:00.000000Z\n" +
                        "-246923735\t0.5699444693578853\t1970-01-08T18:06:40.000000Z\n" +
                        "-1311366306\t0.9918093114862231\t1970-01-08T20:53:20.000000Z\n" +
                        "-1270731285\t0.32424562653969957\t1970-01-08T23:40:00.000000Z\n" +
                        "387510473\t0.8998921791869131\t1970-01-09T02:26:40.000000Z\n" +
                        "1677463366\t0.7458169804091256\t1970-01-09T05:13:20.000000Z\n" +
                        "133913299\t0.33746104579374825\t1970-01-09T08:00:00.000000Z\n" +
                        "-230430837\t0.18740488620384377\t1970-01-09T10:46:40.000000Z\n" +
                        "2076507991\t0.10527282622013212\t1970-01-09T13:33:20.000000Z\n" +
                        "-1613687261\t0.8291193369353376\t1970-01-09T16:20:00.000000Z\n" +
                        "422714199\t0.32673950830571696\t1970-01-09T19:06:40.000000Z\n" +
                        "-1121895896\t0.5992548493051852\t1970-01-09T21:53:20.000000Z\n" +
                        "1295866259\t0.6455967424250787\t1970-01-10T00:40:00.000000Z\n" +
                        "-1204245663\t0.6202777455654276\t1970-01-10T03:26:40.000000Z\n" +
                        "-2108151088\t0.029080850168636263\t1970-01-10T06:13:20.000000Z\n" +
                        "239305284\t0.10459352312331183\t1970-01-10T09:00:00.000000Z\n" +
                        "82099057\t0.5346019596733254\t1970-01-10T11:46:40.000000Z\n" +
                        "1728220848\t0.9418719455092096\t1970-01-10T14:33:20.000000Z\n" +
                        "-623471113\t0.6341292894843615\t1970-01-10T17:20:00.000000Z\n" +
                        "-907794648\t0.7340656260730631\t1970-01-10T20:06:40.000000Z\n" +
                        "-292438036\t0.5025890936351257\t1970-01-10T22:53:20.000000Z\n" +
                        "-1960168360\t0.8952510116133903\t1970-01-11T01:40:00.000000Z\n" +
                        "-370796356\t0.48964139862697853\t1970-01-11T04:26:40.000000Z\n" +
                        "-1871994006\t0.7700798090070919\t1970-01-11T07:13:20.000000Z\n" +
                        "-147343840\t0.4416432347777828\t1970-01-11T10:00:00.000000Z\n" +
                        "-1810676855\t0.05158459929273784\t1970-01-11T12:46:40.000000Z\n" +
                        "1920398380\t0.2445295612285482\t1970-01-11T15:33:20.000000Z\n" +
                        "-1465751763\t0.5466900921405317\t1970-01-11T18:20:00.000000Z\n" +
                        "719189074\t0.5290006415737116\t1970-01-11T21:06:40.000000Z\n" +
                        "-120660220\t0.7260468106076399\t1970-01-11T23:53:20.000000Z\n" +
                        "-1234141625\t0.7229359906306887\t1970-01-12T02:40:00.000000Z\n" +
                        "-720881601\t0.4592067757817594\t1970-01-12T05:26:40.000000Z\n" +
                        "1826239903\t0.5716129058692643\t1970-01-12T08:13:20.000000Z\n" +
                        "-1165635863\t0.05094182589333662\t1970-01-12T11:00:00.000000Z\n",
                "tst",
                "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 10000000000l) t from long_sequence(100)) timestamp(t)) partition by DAY",
                "t",
                true,
                true,
                true
        );
    }

    @Test
    public void testCreateTableIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 10; i++) {
                compiler.compile("create table if not exists y as (select rnd_int() a from long_sequence(21))", sqlExecutionContext);
            }
        });

        assertQuery(
                "a\n" +
                        "-1148479920\n" +
                        "315515118\n" +
                        "1548800833\n" +
                        "-727724771\n" +
                        "73575701\n" +
                        "-948263339\n" +
                        "1326447242\n" +
                        "592859671\n" +
                        "1868723706\n" +
                        "-847531048\n" +
                        "-1191262516\n" +
                        "-2041844972\n" +
                        "-1436881714\n" +
                        "-1575378703\n" +
                        "806715481\n" +
                        "1545253512\n" +
                        "1569490116\n" +
                        "1573662097\n" +
                        "-409854405\n" +
                        "339631474\n" +
                        "1530831067\n",
                "y",
                null,
                true,
                true
        );
    }

    @Test
    public void testCreateTableSymbolColumnViaCastCached() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CREATE_TABLE, compiler.compile("create table x (col string)", sqlExecutionContext).getType());

            engine.clear();

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public boolean getDefaultSymbolCacheFlag() {
                    return false;
                }
            };

            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                compiler.compile("create table y as (x), cast(col as symbol cache)", sqlExecutionContext);

                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "y", TableUtils.ANY_TABLE_VERSION)) {
                    Assert.assertTrue(reader.getSymbolMapReader(0).isCached());
                }
            }
        });
    }

    @Test
    public void testCreateTableSymbolColumnViaCastCachedSymbolCapacityHigh() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CREATE_TABLE, compiler.compile("create table x (col string)", sqlExecutionContext).getType());
            try {
                compiler.compile("create table y as (x), cast(col as symbol capacity 100000000)", sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(51, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "max cached symbol capacity");
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testCreateTableSymbolColumnViaCastNocache() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CREATE_TABLE, compiler.compile("create table x (col string)", sqlExecutionContext).getType());

            compiler.compile("create table y as (x), cast(col as symbol nocache)", sqlExecutionContext);

            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "y", TableUtils.ANY_TABLE_VERSION)) {
                Assert.assertFalse(reader.getSymbolMapReader(0).isCached());
            }
        });
    }

    @Test
    public void testDistinctFunctionColumn() throws Exception {
        final String expected = "v\n" +
                "8.0\n" +
                "1.0\n" +
                "7.0\n" +
                "2.0\n" +
                "3.0\n" +
                "4.0\n" +
                "0.0\n" +
                "6.0\n" +
                "9.0\n" +
                "5.0\n" +
                "10.0\n";

        assertQuery(expected,
                "select distinct round(val*10, 0) v from prices",
                "create table prices as " +
                        "(" +
                        " SELECT \n" +
                        " rnd_double(0) val\n" +
                        " from" +
                        " long_sequence(1200000)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testDistinctOperatorColumn() throws Exception {
        final String expected = "v\n" +
                "10.0\n" +
                "3.0\n" +
                "9.0\n" +
                "4.0\n" +
                "5.0\n" +
                "6.0\n" +
                "2.0\n" +
                "8.0\n" +
                "11.0\n" +
                "7.0\n" +
                "12.0\n";

        assertQuery(expected,
                "select distinct 2+round(val*10,0) v from prices",
                "create table prices as " +
                        "(" +
                        " SELECT \n" +
                        " rnd_double(0) val\n" +
                        " from" +
                        " long_sequence(1200000)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testDistinctSymbolColumn() throws Exception {
        final String expected = "pair\n" +
                "A\n" +
                "B\n" +
                "C\n";

        assertQuery(expected,
                "select distinct pair from prices",
                "create table prices as " +
                        "(" +
                        " SELECT \n" +
                        " x ID, --increasing integer\n" +
                        " rnd_symbol('A', 'B', 'C') pair, \n" +
                        " rnd_double(0) length,\n" +
                        " rnd_double(0) height" +
                        " from" +
                        " long_sequence(1200000)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testDistinctSymbolColumnWithFilter() throws Exception {
        final String expected = "pair\n" +
                "A\n" +
                "B\n";

        assertQuery(expected,
                "select distinct pair from prices where pair in ('A','B')",
                "create table prices as " +
                        "(" +
                        " SELECT \n" +
                        " x ID, --increasing integer\n" +
                        " rnd_symbol('A', 'B', 'C') pair, \n" +
                        " rnd_double(0) length,\n" +
                        " rnd_double(0) height" +
                        " from" +
                        " long_sequence(1200000)" +
                        ")",
                null,
                true
        );
    }

    @Test
    public void testDynamicTimestamp() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        final String expected = "a\tb\tk\n" +
                "21.583224269349387\tYSBE\t1970-01-07T22:40:00.000000Z\n";

        assertQuery(expected,
                "select * from (x timestamp(k)) where k IN '1970-01-07'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                "k",
                true,
                true,
                true
        );
    }

    @Test
    public void testFilterAPI() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        final String expected = "a\tb\tk\n" +
                "80.43224099968394\tCPSW\t1970-01-01T00:00:00.000000Z\n" +
                "93.4460485739401\tPEHN\t1970-01-02T03:46:40.000000Z\n" +
                "88.99286912289664\tSXUX\t1970-01-03T07:33:20.000000Z\n" +
                "42.17768841969397\tGPGW\t1970-01-04T11:20:00.000000Z\n" +
                "66.93837147631712\tDEYY\t1970-01-05T15:06:40.000000Z\n" +
                "0.35983672154330515\tHFOW\t1970-01-06T18:53:20.000000Z\n" +
                "21.583224269349387\tYSBE\t1970-01-07T22:40:00.000000Z\n" +
                "12.503042190293423\tSHRU\t1970-01-09T02:26:40.000000Z\n" +
                "67.00476391801053\tQULO\t1970-01-10T06:13:20.000000Z\n" +
                "81.0161274171258\tTJRS\t1970-01-11T10:00:00.000000Z\n" +
                "24.59345277606021\tRFBV\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tOOZZ\t1970-01-13T17:33:20.000000Z\n" +
                "18.769708157331323\tMYIC\t1970-01-14T21:20:00.000000Z\n" +
                "22.82233596526786\tUICW\t1970-01-16T01:06:40.000000Z\n" +
                "88.2822836669774\t\t1970-01-17T04:53:20.000000Z\n" +
                "45.659895188239794\tDOTS\t1970-01-18T08:40:00.000000Z\n" +
                "97.03060808244088\tCTGQ\t1970-01-19T12:26:40.000000Z\n" +
                "12.02416087573498\tWCKY\t1970-01-20T16:13:20.000000Z\n" +
                "63.59144993891355\tDSWU\t1970-01-21T20:00:00.000000Z\n" +
                "50.65228336156442\tLNVT\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k");

        // these values are assured to be correct for the scenario
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterConstantTrue() throws Exception {
        Record[] expected = new Record[]{
                new Record() {
                    @Override
                    public double getDouble(int col) {
                        return 551.3822454600646;
                    }
                }
        };
        assertQuery(expected,
                "(select sum(a) from x) where 1=1",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(12)" +
                        ") timestamp(k)",
                null,
                true,
                true
        );
    }

    @Test
    public void testFilterFunctionOnSubQuery() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        final String expected = "a\tb\tk\n" +
                "93.4460485739401\tPEHN\t1970-01-02T03:46:40.000000Z\n" +
                "88.2822836669774\t\t1970-01-17T04:53:20.000000Z\n";

        assertQuery(expected,
                "select * from x where cast(b as symbol) in (select rnd_str('PEHN', 'HYRX', null) a from long_sequence(10)) and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "33.74610457937482\tHYRX\t1971-01-01T00:00:00.000000Z\n");
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterOnConstantFalse() throws Exception {
        assertQuery(null,
                "select * from x o where 10 < 8",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k)",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)," +
                        " timestamp_sequence(to_timestamp('2019', 'yyyy'), 1000000000) timestamp" +
                        " from long_sequence(50)" +
                        ") timestamp(timestamp)",
                null,
                false);
    }

    @Test
    public void testFilterOnIndexAndExpression() throws Exception {

        TestMatchFunctionFactory.clear();

        assertQuery("contactId\n" +
                        "KOJSOLDYRO\n" +
                        "SKEDJ\n",
                "SELECT\n" +
                        "    DISTINCT E.contactId AS contactId\n" +
                        "FROM\n" +
                        "    contact_events E\n" +
                        "WHERE\n" +
                        "    E.groupId = 'ZIMN'\n" +
                        "    AND E.eventId = 'IPHZ'\n" +
                        "EXCEPT\n" +
                        "SELECT\n" +
                        "    DISTINCT E.contactId AS contactId\n" +
                        "FROM\n" +
                        "    contact_events  E\n" +
                        "WHERE\n" +
                        "    E.groupId = 'MLGL'\n" +
                        "    AND E.site__clean = 'EPIH'",
                "create table contact_events as (" +
                        "select" +
                        " rnd_str(5,10,0) id," +
                        " rnd_str(5,10,0) contactId," +
                        " rnd_symbol(5,4,4,1) site__query__utm_source," +
                        " rnd_symbol(5,4,4,1) site__query__utm_medium," +
                        " rnd_symbol(5,4,4,1) site__query__utm_campaign," +
                        " rnd_symbol(5,4,4,1) site__query__campaignId," +
                        " rnd_symbol(5,4,4,1) site__query__campaignGroupId," +
                        " rnd_symbol(5,4,4,1) site__query__adsetId," +
                        " rnd_symbol(5,4,4,1) site__query__adId," +
                        " rnd_symbol(5,4,4,1) site__main," +
                        " rnd_str(5,10,0) site__queryString," +
                        " rnd_str(5,10,0) site__clean," +
                        " rnd_symbol(5,4,4,1) site__hash," +
                        " rnd_symbol(5,4,4,1) eventId," +
                        " rnd_symbol(5,4,4,1) groupId" +
                        " from long_sequence(100)" +
                        ")," +
                        " index(groupId)",
                null
        );
    }

    @Test
    public void testFilterOnInterval() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "84.45258177211063\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.5019885372507\t\t1970-01-01T03:53:20.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-01T04:10:00.000000Z\n",
                "select * from x o where k IN '1970-01-01T03:36:40;45m' and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k)",
                "k");
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterOnIntervalAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery("a\tb\tk\n" +
                        "84.45258177211063\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.5019885372507\t\t1970-01-01T03:53:20.000000Z\n",
                "select * from x o where k IN '1970-01-01T03:36:40;45m' and a > 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        "  timestamp_sequence(0, 1000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k)",
                "k");

        // also good numbers, extra top calls are due to symbol column API check
        // tables without symbol columns will skip this check
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterOnIntrinsicFalse() throws Exception {
        assertQuery(null,
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < a",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, timestamp_sequence(0, 1000000000) k from long_sequence(20)), index(b)",
                null,
                false);
    }

    @Test
    public void testFilterOnNull() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756228\t\n" +
                "87.99634725391621\t\n" +
                "32.881769076795045\t\n" +
                "57.93466326862211\t\n" +
                "26.922103479744898\t\n" +
                "52.98405941762054\t\n" +
                "97.5019885372507\t\n" +
                "80.01121139739173\t\n" +
                "92.050039469858\t\n" +
                "45.6344569609078\t\n" +
                "40.455469747939254\t\n";
        assertQuery(expected,
                "select * from x where b = null",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b from long_sequence(20)), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100, " +
                        " rnd_symbol(5,4,4,1)" +
                        " from long_sequence(15)",
                expected +
                        "54.49155021518948\t\n" +
                        "76.9238189433781\t\n" +
                        "58.912164838797885\t\n" +
                        "44.80468966861358\t\n" +
                        "89.40917126581896\t\n" +
                        "3.993124821273464\t\n");
    }

    @Test
    public void testFilterOnSubQueryIndexed() throws Exception {
        TestMatchFunctionFactory.clear();
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null, 'ABC') a from long_sequence(10)) and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n");
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterOnSubQueryIndexedAndFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        final String expected = "b\tk\ta\n" +
                "HYRX\t1970-01-07T22:40:00.000000Z\t97.71103146051203\n" +
                "HYRX\t1970-01-11T10:00:00.000000Z\t12.026122412833129\n";

        assertQuery(expected,
                "select b, k, a from x where b in (select list('RXGZ', 'HYRX', null, 'ABC') a from long_sequence(10)) and b = 'HYRX'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "HYRX\t1971-01-01T00:00:00.000000Z\t56.594291398612405\n");
    }

    @Test
    public void testFilterOnSubQueryIndexedDeferred() throws Exception {

        assertQuery(null,
                "select * from x where b in (select rnd_symbol('ABC') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(5)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "95.40069089049732\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "25.53319339703062\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "89.40917126581896\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "28.799739396819312\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "68.06873134626417\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterOnSubQueryIndexedFiltered() throws Exception {

        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null, 'ABC') a from long_sequence(10)) and test_match()" +
                        "and a < 80",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(5)" +
                        ") timestamp(t)",
                expected + "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "72.30015763133606\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "12.105630273556178\tABC\t1971-01-01T00:00:00.000000Z\n" +
                        "11.585982949541474\tABC\t1971-01-01T00:00:00.000000Z\n");

        // these value are also ok because ddl2 is present, there is another round of check for that
        // this ensures that "init" on filter is invoked
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterOnSubQueryIndexedFilteredEmpty() throws Exception {

        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\tk\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null, 'ABC') a from long_sequence(10)) and test_match() and 1 = 2",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        // these value are also ok because ddl2 is present, there is another round of check for that
        // this ensures that "init" on filter is invoked
        Assert.assertTrue(TestMatchFunctionFactory.isClosed());
    }

    @Test
    public void testFilterOnSubQueryIndexedStrColumn() throws Exception {

        assertQuery(null,
                "select * from x where b in (select 'ABC' a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterOnSymInList() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in ('RXGZ', 'HYRX', null, 'QZT')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'QZT'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                        "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                        "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                        "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                        "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                        "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                        "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tQZT\t1971-01-01T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testFilterOnValues() throws Exception {
        final String expected1 = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "70.94360487171201\tPEHN\t1970-01-01T00:50:00.000000Z\n" +
                "87.99634725391621\t\t1970-01-01T01:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-01T01:23:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-01T01:40:00.000000Z\n" +
                "81.46807944500559\tPEHN\t1970-01-01T01:56:40.000000Z\n" +
                "57.93466326862211\t\t1970-01-01T02:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-01T03:03:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-01T03:20:00.000000Z\n" +
                "84.45258177211063\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                "97.5019885372507\t\t1970-01-01T03:53:20.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                "80.01121139739173\t\t1970-01-01T04:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-01T04:43:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-01T05:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-01T05:16:40.000000Z\n";

        assertQuery(expected1,
                "select * from x o where o.b in ('HYRX','PEHN', null, 'ABCD')",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, timestamp_sequence(0, 1000000000) k from long_sequence(20)), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol('HYRX','PEHN', null, 'ABCD')" +
                        " from" +
                        " long_sequence(10)",
                expected1 +
                        "56.594291398612405\tHYRX\t\n" +
                        "68.21660861001273\tPEHN\t\n" +
                        "96.44183832564399\t\t\n" +
                        "11.585982949541474\tABCD\t\n" +
                        "81.64182592467493\tABCD\t\n" +
                        "54.49155021518948\tPEHN\t\n" +
                        "76.9238189433781\tABCD\t\n" +
                        "49.42890511958454\tHYRX\t\n" +
                        "65.51335839796312\tABCD\t\n" +
                        "28.20020716674768\tABCD\t\n");
    }

    @Test
    public void testFilterOnValuesAndFilter() throws Exception {

        TestMatchFunctionFactory.clear();

        assertQuery("a\tb\tk\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "32.881769076795045\t\t1970-01-01T01:23:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                        "26.922103479744898\t\t1970-01-01T03:03:20.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                        "45.6344569609078\t\t1970-01-01T05:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-01T05:16:40.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < 50 and test_match()",
                "create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from long_sequence(20)" +
                        ")," +
                        " index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol(5,4,4,1)" +
                        " from" +
                        " long_sequence(10)",
                "a\tb\tk\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "32.881769076795045\t\t1970-01-01T01:23:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-01T02:30:00.000000Z\n" +
                        "26.922103479744898\t\t1970-01-01T03:03:20.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-01T04:10:00.000000Z\n" +
                        "45.6344569609078\t\t1970-01-01T05:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-01T05:16:40.000000Z\n" +
                        "44.80468966861358\t\t\n");

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterOnValuesAndInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.45258177211063\tPEHN\t1970-01-01T03:36:40.000000Z\n" +
                        "97.5019885372507\t\t1970-01-01T03:53:20.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-01T04:10:00.000000Z\n",
                "select * from x o where o.b in ('HYRX','PEHN', null) and k IN '1970-01-01T03:36:40;45m'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k)",
                "k");
    }

    @Test
    public void testFilterOnValuesDeferred() throws Exception {
        assertQuery(null,
                "select * from x o where o.b in ('ABCD', 'XYZ')",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, timestamp_sequence(0, 1000000000) k from long_sequence(20)), index(b)",
                null,
                "insert into x (a,b)" +
                        " select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol('HYRX','PEHN', null, 'ABCD')" +
                        " from" +
                        " long_sequence(10)",
                "a\tb\tk\n" +
                        "11.585982949541474\tABCD\t\n" +
                        "81.64182592467493\tABCD\t\n" +
                        "76.9238189433781\tABCD\t\n" +
                        "65.51335839796312\tABCD\t\n" +
                        "28.20020716674768\tABCD\t\n");
    }

    @Test
    public void testFilterSingleKeyValue() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756228\tHYRX\n" +
                "52.98405941762054\tHYRX\n" +
                "40.455469747939254\tHYRX\n" +
                "72.30015763133606\tHYRX\n";
        assertQuery(expected,
                "select * from x where b = 'HYRX'",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.88175403454873\tHYRX\n" +
                        "57.78947915182423\tHYRX\n");
    }

    @Test
    public void testFilterSingleKeyValueAndField() throws Exception {
        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\n" +
                "52.98405941762054\tHYRX\n" +
                "72.30015763133606\tHYRX\n";
        assertQuery(expected,
//                "x",
                "select * from x where b = 'HYRX' and a > 41 and test_match()",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.88175403454873\tHYRX\n" +
                        "57.78947915182423\tHYRX\n");

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterSingleNonExistingSymbol() throws Exception {
        assertQuery(null,
                "select * from x where b = 'ABC'",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.88175403454873\tABC\n" +
                        "57.78947915182423\tABC\n");
    }

    @Test
    public void testFilterSingleNonExistingSymbolAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(null,
                "select * from x where b = 'ABC' and a > 30 and test_match()",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.88175403454873\tABC\n" +
                        "57.78947915182423\tABC\n");
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testFilterSubQuery() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterSubQueryAddSymbol() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', 'ABC', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterSubQueryStrColumn() throws Exception {
        // no index
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testFilterSubQueryUnsupportedColType() throws Exception {
        assertFailure("select * from x where b in (select 12, rnd_str('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                24,
                "supported column types are STRING and SYMBOL, found: INT");
    }

    @Test
    public void testFilterWrongType() throws Exception {
        assertFailure("select * from x where b - a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0) a," +
                        " rnd_double(0) b," +
                        " rnd_symbol(5,4,4,1) c" +
                        " from long_sequence(10)" +
                        "), index(c)",
                24,
                "boolean expression expected"
        );
    }

    @Test
    public void testGroupByConstantMatchingColumnName() throws Exception {
        assertQuery(
                "nts\tmin\nnts\t\n",
                "select 'nts', min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'",
                "create table tt (dts timestamp, nts timestamp) timestamp(dts)",
                null,
                "insert into tt " +
                        "select timestamp_sequence(1577836800000000L, 10L), timestamp_sequence(1577836800000000L, 10L) " +
                        "from long_sequence(2L)",
                "nts\tmin\n" +
                        "nts\t2020-01-01T00:00:00.000010Z\n",
                false,
                false,
                true);
    }

    @Test
    public void testInsertMissingQuery() throws Exception {
        assertFailure(
                "insert into x (a,b)",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b from long_sequence(20)), index(b)",
                19,
                "'select' or 'values' expected"
        );
    }

    @Test
    public void testJoinOnExecutionOrder() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table l as( select x from long_sequence(100) )", sqlExecutionContext);
            compiler.compile("create table rr as( select x + 50 as y from long_sequence(100) )", sqlExecutionContext);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "select x, y from l left join rr on l.x = rr.y and (y > 0 or y > 10)",
                    sink,
                    "x\ty\n" +
                            "1\tNaN\n" +
                            "2\tNaN\n" +
                            "3\tNaN\n" +
                            "4\tNaN\n" +
                            "5\tNaN\n" +
                            "6\tNaN\n" +
                            "7\tNaN\n" +
                            "8\tNaN\n" +
                            "9\tNaN\n" +
                            "10\tNaN\n" +
                            "11\tNaN\n" +
                            "12\tNaN\n" +
                            "13\tNaN\n" +
                            "14\tNaN\n" +
                            "15\tNaN\n" +
                            "16\tNaN\n" +
                            "17\tNaN\n" +
                            "18\tNaN\n" +
                            "19\tNaN\n" +
                            "20\tNaN\n" +
                            "21\tNaN\n" +
                            "22\tNaN\n" +
                            "23\tNaN\n" +
                            "24\tNaN\n" +
                            "25\tNaN\n" +
                            "26\tNaN\n" +
                            "27\tNaN\n" +
                            "28\tNaN\n" +
                            "29\tNaN\n" +
                            "30\tNaN\n" +
                            "31\tNaN\n" +
                            "32\tNaN\n" +
                            "33\tNaN\n" +
                            "34\tNaN\n" +
                            "35\tNaN\n" +
                            "36\tNaN\n" +
                            "37\tNaN\n" +
                            "38\tNaN\n" +
                            "39\tNaN\n" +
                            "40\tNaN\n" +
                            "41\tNaN\n" +
                            "42\tNaN\n" +
                            "43\tNaN\n" +
                            "44\tNaN\n" +
                            "45\tNaN\n" +
                            "46\tNaN\n" +
                            "47\tNaN\n" +
                            "48\tNaN\n" +
                            "49\tNaN\n" +
                            "50\tNaN\n" +
                            "51\t51\n" +
                            "52\t52\n" +
                            "53\t53\n" +
                            "54\t54\n" +
                            "55\t55\n" +
                            "56\t56\n" +
                            "57\t57\n" +
                            "58\t58\n" +
                            "59\t59\n" +
                            "60\t60\n" +
                            "61\t61\n" +
                            "62\t62\n" +
                            "63\t63\n" +
                            "64\t64\n" +
                            "65\t65\n" +
                            "66\t66\n" +
                            "67\t67\n" +
                            "68\t68\n" +
                            "69\t69\n" +
                            "70\t70\n" +
                            "71\t71\n" +
                            "72\t72\n" +
                            "73\t73\n" +
                            "74\t74\n" +
                            "75\t75\n" +
                            "76\t76\n" +
                            "77\t77\n" +
                            "78\t78\n" +
                            "79\t79\n" +
                            "80\t80\n" +
                            "81\t81\n" +
                            "82\t82\n" +
                            "83\t83\n" +
                            "84\t84\n" +
                            "85\t85\n" +
                            "86\t86\n" +
                            "87\t87\n" +
                            "88\t88\n" +
                            "89\t89\n" +
                            "90\t90\n" +
                            "91\t91\n" +
                            "92\t92\n" +
                            "93\t93\n" +
                            "94\t94\n" +
                            "95\t95\n" +
                            "96\t96\n" +
                            "97\t97\n" +
                            "98\t98\n" +
                            "99\t99\n" +
                            "100\t100\n"
            );
        });
    }

    @Test
    public void testJoinWhereExecutionOrder() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table l as( select x from long_sequence(100) )", sqlExecutionContext);
            compiler.compile("create table rr as( select x + 50 as y from long_sequence(100) )", sqlExecutionContext);

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "select x, y\n" +
                            "from l left join rr on l.x = rr.y\n" +
                            "where y > 0 or y > 10",
                    sink,
                    "x\ty\n" +
                            "51\t51\n" +
                            "52\t52\n" +
                            "53\t53\n" +
                            "54\t54\n" +
                            "55\t55\n" +
                            "56\t56\n" +
                            "57\t57\n" +
                            "58\t58\n" +
                            "59\t59\n" +
                            "60\t60\n" +
                            "61\t61\n" +
                            "62\t62\n" +
                            "63\t63\n" +
                            "64\t64\n" +
                            "65\t65\n" +
                            "66\t66\n" +
                            "67\t67\n" +
                            "68\t68\n" +
                            "69\t69\n" +
                            "70\t70\n" +
                            "71\t71\n" +
                            "72\t72\n" +
                            "73\t73\n" +
                            "74\t74\n" +
                            "75\t75\n" +
                            "76\t76\n" +
                            "77\t77\n" +
                            "78\t78\n" +
                            "79\t79\n" +
                            "80\t80\n" +
                            "81\t81\n" +
                            "82\t82\n" +
                            "83\t83\n" +
                            "84\t84\n" +
                            "85\t85\n" +
                            "86\t86\n" +
                            "87\t87\n" +
                            "88\t88\n" +
                            "89\t89\n" +
                            "90\t90\n" +
                            "91\t91\n" +
                            "92\t92\n" +
                            "93\t93\n" +
                            "94\t94\n" +
                            "95\t95\n" +
                            "96\t96\n" +
                            "97\t97\n" +
                            "98\t98\n" +
                            "99\t99\n" +
                            "100\t100\n"
            );
        });
    }

    @Test
    public void testLatestByAll() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllBool() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "97.55263540567968\ttrue\t1970-01-20T16:13:20.000000Z\n" +
                        "37.62501709498378\tfalse\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select * from" +
                        "(" +
                        " select" +
                        " rnd_double(0)*100 a," +
                        " rnd_boolean() b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20) " +
                        ")" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " false," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "97.55263540567968\ttrue\t1970-01-20T16:13:20.000000Z\n" +
                        "24.59345277606021\tfalse\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllConstantFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where 6 < 10",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where a > 40",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedGeohash() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeohashTable();
                    assertQuery("time\tuuid\thash8s\thash8i\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf91t48s7\t490759922439\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz31wzd5w\t1068437057724\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tvepe7h62\t942390100162\n",
                            "select * from pos latest by uuid where hash8i within('z31','f91','vepe7h')",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeohashTimeRange() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeohashTable();
                    assertQuery(
                            "time\tuuid\thash8s\thash8i\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz31wzd5w\t1068437057724\n",
                            "select * from pos latest by uuid where time in '2021-05-11' and hash8i within ('z31','bbx')",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    private void createGeohashTable() throws SqlException {
        compiler.compile("create table pos(time timestamp, uuid symbol, hash8s symbol, hash8i long)" +
                ", index(uuid) timestamp(time) partition by DAY", sqlExecutionContext);
        executeInsert("insert into pos values('2021-05-11T00:00:00.083000Z','YYY','z31wzd5w',1068437057724)");
        executeInsert("insert into pos values('2021-05-10T23:59:59.150000Z','XXX','f91t48s7',490759922439)");
        executeInsert("insert into pos values('2021-05-10T23:59:59.322000Z','ddd','bbqyzfp6',355105487526)");
        executeInsert("insert into pos values('2021-05-10T23:59:59.351000Z','bbb','9egcyrxq',323712147382)");
        executeInsert("insert into pos values('2021-05-10T23:59:59.439000Z','bbb','ewef1vk8',477192318536)");
        executeInsert("insert into pos values('2021-05-10T00:00:00.016000Z','aaa','vb2wg49h',938547319088)");
        executeInsert("insert into pos values('2021-05-10T00:00:00.042000Z','ccc','bft3gn89',359472288009)");
        executeInsert("insert into pos values('2021-05-10T00:00:00.055000Z','aaa','z6cf5j85',1071978300677)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.066000Z','ddd','vcunv6j7',940418374183)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.072000Z','ccc','edez0n5y',460030234814)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.074000Z','aaa','fds32zgc',494729788907)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.092000Z','ddd','v9nwc4ny',938077426334)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.107000Z','ccc','f6yb1yx9',488495971241)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.111000Z','ddd','bcnktpnw',356099348124)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.123000Z','aaa','z3t2we5z',1069215003839)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.127000Z','aaa','bgn1yt4y',360376657054)");
        executeInsert("insert into pos values('2021-05-11T00:00:00.144000Z','aaa','fuetk3k6',509416640070)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.167000Z','ccc','bchx5x14',355976016932)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.167000Z','ZZZ','bbxwb5jj',355337573937)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.186000Z','ZZZ','vepe7h62',942390100162)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.241000Z','bbb','bchxpmmg',355976531567)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.245000Z','ddd','f90z3bs5',490732628741)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.247000Z','bbb','bftqreuh',359492466512)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.295000Z','ddd','u2rqgy9s',896296024376)");
        executeInsert("insert into pos values('2021-05-12T00:00:00.304000Z','aaa','w23bhjd2',964331849090)");
    }

    @Test
    public void testLatestByAllIndexed() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedConstantFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where 5 > 2",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedFilter() throws Exception {
        final String expected = "a\tk\tb\n" +
                "78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n";
        assertQuery(expected,
                "select a,k,b from x latest by b where a > 40",
                "create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2," +
                        " rnd_double(0)*100 a3," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " to_timestamp('2019', 'yyyy') t," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " 46.578761277152225," +
                        " 'VTJW'" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tk\tb\n" +
                        "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                        "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                        "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n" +
                        "46.578761277152225\t2019-01-01T00:00:00.000000Z\tVTJW\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedFilterBySymbol() throws Exception {
        final String expected = "a\tb\tc\tk\n" +
                "67.52509547112409\tCPSW\tSXUX\t1970-01-21T20:00:00.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where c = 'SXUX'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_symbol(5,4,4,1) c," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " 'SXUX'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tb\tc\tk\n" +
                        "94.41658975532606\tVTJW\tSXUX\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedFilteredMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile("create table trips(id int, vendor symbol index, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
                    // insert three partitions
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "trips latest by vendor where id > 0 order by ts",
                            sink,
                            "id\tvendor\tts\n" +
                                    "1878619626\tKK\t1970-01-01T00:01:39.200000Z\n" +
                                    "371958898\tDD\t1970-01-02T00:01:39.900000Z\n" +
                                    "1699760758\tPP\t1970-01-03T00:01:39.100000Z\n"
                    );
                }
        );
    }

    @Test
    public void testLatestByAllIndexedListMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile("create table trips(id int, vendor symbol index, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
                    // insert three partitions
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "trips latest by vendor where vendor in ('KK', 'ZZ', 'TT', 'DD', 'PP', 'QQ', 'CC') order by ts",
                            sink,
                            "id\tvendor\tts\n" +
                                    "-1243990650\tZZ\t1970-01-01T00:01:39.900000Z\n" +
                                    "1878619626\tKK\t1970-01-01T00:01:39.200000Z\n" +
                                    "371958898\tDD\t1970-01-02T00:01:39.900000Z\n" +
                                    "-774731115\tTT\t1970-01-02T00:01:39.800000Z\n" +
                                    "-1808277542\tCC\t1970-01-03T00:01:39.900000Z\n" +
                                    "-610460127\tQQ\t1970-01-03T00:01:39.500000Z\n" +
                                    "1699760758\tPP\t1970-01-03T00:01:39.100000Z\n"
                    );
                }
        );
    }

    @Test
    public void testLatestByAllIndexedMixed() throws Exception {
        final String expected = "a\tk\tb\n" +
                "78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW\n" +
                "2.6836863013701473\t1970-01-13T17:33:20.000000Z\tHYRX\n" +
                "9.76683471072458\t1970-01-14T21:20:00.000000Z\tPEHN\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n";
        assertQuery(expected,
                "select a,k,b from x latest by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2," +
                        " rnd_double(0)*100 a3," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " to_timestamp('2019', 'yyyy') t," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " 'VTJW'" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "a\tk\tb\n" +
                        "2.6836863013701473\t1970-01-13T17:33:20.000000Z\tHYRX\n" +
                        "9.76683471072458\t1970-01-14T21:20:00.000000Z\tPEHN\n" +
                        "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                        "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                        "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n" +
                        "6.578761277152223\t2019-01-01T00:00:00.000000Z\tVTJW\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedMixedColumns() throws Exception {
        final String expected = "k\ta\n" +
                "1970-01-03T07:33:20.000000Z\t23.90529010846525\n" +
                "1970-01-11T10:00:00.000000Z\t12.026122412833129\n" +
                "1970-01-12T13:46:40.000000Z\t48.820511018586934\n" +
                "1970-01-18T08:40:00.000000Z\t49.00510449885239\n" +
                "1970-01-22T23:46:40.000000Z\t40.455469747939254\n";
        assertQuery(expected,
                "select k,a from x latest by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "k\ta\n" +
                        "1970-01-03T07:33:20.000000Z\t23.90529010846525\n" +
                        "1970-01-11T10:00:00.000000Z\t12.026122412833129\n" +
                        "1970-01-18T08:40:00.000000Z\t49.00510449885239\n" +
                        "1970-01-22T23:46:40.000000Z\t40.455469747939254\n" +
                        "2019-01-01T00:00:00.000000Z\t56.594291398612405\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllIndexedMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile("create table trips(id int, vendor symbol index, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
                    // insert three partitions
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "trips latest by vendor order by ts",
                            sink,
                            "id\tvendor\tts\n" +
                                    "1878619626\tKK\t1970-01-01T00:01:39.200000Z\n" +
                                    "-1243990650\tZZ\t1970-01-01T00:01:39.900000Z\n" +
                                    "-774731115\tTT\t1970-01-02T00:01:39.800000Z\n" +
                                    "371958898\tDD\t1970-01-02T00:01:39.900000Z\n" +
                                    "1699760758\tPP\t1970-01-03T00:01:39.100000Z\n" +
                                    "-610460127\tQQ\t1970-01-03T00:01:39.500000Z\n" +
                                    "-1808277542\tCC\t1970-01-03T00:01:39.900000Z\n"
                    );
                }
        );
    }

    @Test
    public void testLatestByAllMixed() throws Exception {
        assertQuery("b\tk\ta\n" +
                        "VTJW\t1970-01-04T11:20:00.000000Z\t78.83065830055033\n" +
                        "HYRX\t1970-01-13T17:33:20.000000Z\t2.6836863013701473\n" +
                        "PEHN\t1970-01-14T21:20:00.000000Z\t9.76683471072458\n" +
                        "CPSW\t1970-01-19T12:26:40.000000Z\t51.85631921367574\n" +
                        "RXGZ\t1970-01-20T16:13:20.000000Z\t50.25890936351257\n" +
                        "\t1970-01-22T23:46:40.000000Z\t72.604681060764\n",
                "select b,k,a from x latest by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2," +
                        " rnd_double(0)*100 a3," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " to_timestamp('2019', 'yyyy') t," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " rnd_double(0)*100," +
                        " 'VTJW'" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                "b\tk\ta\n" +
                        "HYRX\t1970-01-13T17:33:20.000000Z\t2.6836863013701473\n" +
                        "PEHN\t1970-01-14T21:20:00.000000Z\t9.76683471072458\n" +
                        "CPSW\t1970-01-19T12:26:40.000000Z\t51.85631921367574\n" +
                        "RXGZ\t1970-01-20T16:13:20.000000Z\t50.25890936351257\n" +
                        "\t1970-01-22T23:46:40.000000Z\t72.604681060764\n" +
                        "VTJW\t2019-01-01T00:00:00.000000Z\t6.578761277152223\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByAllMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    compiler.compile("create table trips(id int, vendor symbol, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
                    // insert three partitions
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    compiler.compile(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)",
                            sqlExecutionContext
                    );

                    TestUtils.assertSql(
                            compiler,
                            sqlExecutionContext,
                            "trips latest by vendor order by ts",
                            sink,
                            "id\tvendor\tts\n" +
                                    "1878619626\tKK\t1970-01-01T00:01:39.200000Z\n" +
                                    "-1243990650\tZZ\t1970-01-01T00:01:39.900000Z\n" +
                                    "-774731115\tTT\t1970-01-02T00:01:39.800000Z\n" +
                                    "371958898\tDD\t1970-01-02T00:01:39.900000Z\n" +
                                    "1699760758\tPP\t1970-01-03T00:01:39.100000Z\n" +
                                    "-610460127\tQQ\t1970-01-03T00:01:39.500000Z\n" +
                                    "-1808277542\tCC\t1970-01-03T00:01:39.900000Z\n"
                    );
                }
        );
    }

    @Test
    public void testLatestByAllNewSymFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected,
                "select * from x latest by b where a > 40",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        " select" +
                        " rnd_double(0)*100," +
                        " 'CCKS'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp (t)",
                expected +
                        "56.594291398612405\tCCKS\t2019-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByIOFailure() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = new FilesFacadeImpl() {

                @Override
                public long openRO(LPSZ name) {
                    if (Chars.endsWith(name, "b.d")) {
                        return -1;
                    }
                    return super.openRO(name);
                }
            };
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration);
                 SqlCompiler compiler = new SqlCompiler(engine)) {
                try {
                    compiler.compile(("create table x as " +
                                    "(" +
                                    "select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, timestamp_sequence(0, 100000000000) k from" +
                                    " long_sequence(200)" +
                                    ") timestamp(k) partition by DAY"),
                            sqlExecutionContext);

                    try (final RecordCursorFactory factory = compiler.compile(
                            "select * from x latest by b where b = 'PEHN' and a < 22",
                            sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        try {
                            assertCursor(
                                    "a\tb\tk\n" +
                                            "5.942010834028\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                                    factory,
                                    true,
                                    true,
                                    false
                            );
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                        }
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                } finally {
                    engine.clear();
                }
            }
        });
    }

    @Test
    public void testLatestByKeyValue() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b = 'RXGZ'",
                "create table x as " +
                        "(" +
                        "select " +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValueFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "5.942010834028011\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and a < 22 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(200)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 11.3," +
                        " 'PEHN'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "11.3\tPEHN\t1971-01-01T00:00:00.000000Z\n");

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByKeyValueFilteredEmpty() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n",
                "select * from x latest by b where b = 'PEHN' and a < 22 and 1 = 2 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(200)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.isClosed());
    }

    @Test
    public void testLatestByKeyValueIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b = 'RXGZ'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByKeyValueIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "5.942010834028011\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and a < 22 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(200)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 11.3," +
                        " 'PEHN'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "11.3\tPEHN\t1971-01-01T00:00:00.000000Z\n");

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByKeyValueInterval() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n",
                "select * from x latest by b where b = 'PEHN' and k IN '1970-01-06T18:53:20;11d'",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k");
    }

    @Test
    public void testLatestByKeyValues() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByKeyValuesFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX', null) and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(5)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "12.105630273556178\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByKeyValuesIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByKeyValuesIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n",
                "select * from x latest by b where b in ('RXGZ','HYRX') and a > 20 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByMissingKeyValue() throws Exception {
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "72.30015763133606\tXYZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testLatestByMissingKeyValueFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ') and a < 60 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n");

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByMissingKeyValueIndexed() throws Exception {
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " rnd_symbol('XYZ', 'PEHN', 'ZZNK')," +
                        " timestamp_sequence(to_timestamp('1971', 'yyyy'), 100000000000) t" +
                        " from long_sequence(10)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "81.64182592467493\tXYZ\t1971-01-05T15:06:40.000000Z\n");
    }

    @Test
    public void testLatestByMissingKeyValueIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(null,
                "select * from x latest by b where b in ('XYZ') and a < 60 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n");
        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByMissingKeyValues() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ','HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByMissingKeyValuesFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ', 'HYRX') and a > 30 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByMissingKeyValuesIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ', 'HYRX')",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByMissingKeyValuesIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "54.55175324785665\tHYRX\t1970-02-02T07:00:00.000000Z\n",
                "select * from x latest by b where b in ('XYZ', 'HYRX') and a > 30 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 10000000000) k" +
                        " from" +
                        " long_sequence(300)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 88.1," +
                        " 'XYZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "54.55175324785665\tHYRX\t1970-02-02T07:00:00.000000Z\n" +
                        "88.10000000000001\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestByMultipleColumns() throws Exception {
        assertQuery("cust_id\tbalance_ccy\tbalance\tstatus\ttimestamp\n",
                "select * from balances latest by cust_id, balance_ccy",
                "create table balances (\n" +
                        "\tcust_id int, \n" +
                        "\tbalance_ccy symbol, \n" +
                        "\tbalance double, \n" +
                        "\tstatus byte, \n" +
                        "\ttimestamp timestamp\n" +
                        ") timestamp(timestamp)",
                "timestamp",
                "insert into balances select * from (" +
                        " select" +
                        " abs(rnd_int()) % 4," +
                        " rnd_str('USD', 'GBP', 'EUR')," +
                        " rnd_double()," +
                        " rnd_byte(0,1)," +
                        " cast(0 as timestamp) timestamp" +
                        " from long_sequence(150)" +
                        ") timestamp (timestamp)",
                "cust_id\tbalance_ccy\tbalance\tstatus\ttimestamp\n" +
                        "3\tUSD\t0.8796413468565342\t0\t1970-01-01T00:00:00.000000Z\n" +
                        "3\tEUR\t0.011099265671968506\t0\t1970-01-01T00:00:00.000000Z\n" +
                        "1\tEUR\t0.10747511833573742\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "1\tGBP\t0.15274858078119136\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "0\tGBP\t0.07383464174908916\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "2\tEUR\t0.30062011052460846\t0\t1970-01-01T00:00:00.000000Z\n" +
                        "1\tUSD\t0.12454054765285283\t0\t1970-01-01T00:00:00.000000Z\n" +
                        "0\tUSD\t0.3124458010612313\t0\t1970-01-01T00:00:00.000000Z\n" +
                        "2\tUSD\t0.7943185767500432\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "2\tGBP\t0.4388864091771264\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "0\tEUR\t0.5921457770297527\t1\t1970-01-01T00:00:00.000000Z\n" +
                        "3\tGBP\t0.31861843394057765\t1\t1970-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByNonExistingColumn() throws Exception {
        assertFailure(
                "select * from x latest by y",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                26,
                "Invalid column");
    }

    @Test
    public void testLatestBySubQuery() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestBySubQueryDeferred() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tUCLA\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestBySubQueryDeferredFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.46\tUCLA\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestBySubQueryDeferredIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tUCLA\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestBySubQueryDeferredIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'UCLA'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.46\tUCLA\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestBySubQueryFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.46\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestBySubQueryIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestBySubQueryIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                "select * from x latest by b where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " and a > 12 and a < 50 and test_match()",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " 33.46," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "33.46\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI());
    }

    @Test
    public void testLatestBySubQueryIndexedIntColumn() throws Exception {
        assertFailure(
                "select * from x latest by b where b in (select 1 a from long_sequence(4))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                47,
                "unsupported column type");
    }

    @Test
    public void testLatestBySubQueryIndexedStrColumn() throws Exception {
        assertQuery("a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x latest by b where b in (select 'RXGZ' from long_sequence(4))",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testLatestByTimestampInclusion() throws Exception {
        assertQuery("ts\tmarket_type\tavg\n" +
                        "1970-01-01T00:00:00.000000Z\taaa\t0.49992728629932576\n" +
                        "1970-01-01T00:00:00.000000Z\tbbb\t0.500285563758478\n" +
                        "1970-01-01T00:00:01.000000Z\taaa\t0.500040169925671\n" +
                        "1970-01-01T00:00:01.000000Z\tbbb\t0.5008686113849173\n" +
                        "1970-01-01T00:00:02.000000Z\taaa\t0.49977074601999855\n" +
                        "1970-01-01T00:00:02.000000Z\tbbb\t0.4999258418217269\n" +
                        "1970-01-01T00:00:03.000000Z\taaa\t0.5003595019568708\n" +
                        "1970-01-01T00:00:03.000000Z\tbbb\t0.5002857992170555\n" +
                        "1970-01-01T00:00:04.000000Z\tbbb\t0.4997116251279621\n" +
                        "1970-01-01T00:00:04.000000Z\taaa\t0.5006208473770267\n" +
                        "1970-01-01T00:00:05.000000Z\tbbb\t0.49988619432529985\n" +
                        "1970-01-01T00:00:05.000000Z\taaa\t0.5002852550150528\n" +
                        "1970-01-01T00:00:06.000000Z\taaa\t0.4998229395659802\n" +
                        "1970-01-01T00:00:06.000000Z\tbbb\t0.4997012831335711\n" +
                        "1970-01-01T00:00:07.000000Z\tbbb\t0.49945806525231845\n" +
                        "1970-01-01T00:00:07.000000Z\taaa\t0.4995901449794158\n" +
                        "1970-01-01T00:00:08.000000Z\taaa\t0.5002616949495469\n" +
                        "1970-01-01T00:00:08.000000Z\tbbb\t0.5005399447758458\n" +
                        "1970-01-01T00:00:09.000000Z\taaa\t0.5003054203632804\n" +
                        "1970-01-01T00:00:09.000000Z\tbbb\t0.500094369884023\n",
                "select ts, market_type, avg(bid_price) FROM market_updates LATEST BY ts, market_type SAMPLE BY 1s",
                "create table market_updates as (select rnd_symbol('aaa','bbb') market_type, rnd_double() bid_price, timestamp_sequence(0,1) ts from long_sequence(10000000)" +
                        ") timestamp(ts)",
                "ts",
                false,
                true,
                false
        );
    }

    @Test
    public void testLatestByValue() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                        "65.08594025855301\tHNR\t1970-01-02T03:46:40.000000Z\n",
                "select * from x latest by b where b = 'HNR'",
                "create table x as " +
                        "(" +
                        "select " +
                        " rnd_double(0)*100 a," +
                        " rnd_str(2,4,4) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'HNR'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "34.56897991538844\tHNR\t1971-01-01T00:00:00.000000Z\n",
                true,
                true,
                false,
                true);
    }

    @Test
    public void testLeftJoinDoesNotRequireTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("CREATE TABLE sensors (ID LONG, make STRING, city STRING);", sqlExecutionContext);
            compiler.compile(
                    "INSERT INTO sensors\n" +
                            "SELECT\n" +
                            "    x ID, --increasing integer\n" +
                            "    rnd_str('Eberle', 'Honeywell', 'Omron', 'United Automation', 'RS Pro') make,\n" +
                            "    rnd_str('New York', 'Miami', 'Boston', 'Chicago', 'San Francisco') city\n" +
                            "FROM long_sequence(10000) x;",
                    sqlExecutionContext
            );

            compiler.compile(
                    "CREATE TABLE readings\n" +
                            "AS(\n" +
                            "    SELECT\n" +
                            "        x ID,\n" +
                            "        timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), rnd_long(1,10,2) * 100000L) ts,\n" +
                            "        rnd_double(0)*8 + 15 temp,\n" +
                            "        rnd_long(0, 10000, 0) sensorId\n" +
                            "    FROM long_sequence(10000000) x)\n" +
                            "TIMESTAMP(ts)\n" +
                            "PARTITION BY MONTH;",
                    sqlExecutionContext
            );

            TestUtils.assertSql(
                    compiler,
                    sqlExecutionContext,
                    "SELECT ts, a.city, a.make, avg(temp)\n" +
                            "FROM readings timestamp(ts)\n" +
                            "JOIN\n" +
                            "    (SELECT ID sensId, city, make\n" +
                            "    FROM sensors\n" +
                            "    WHERE city='Miami' AND make='Omron') a\n" +
                            "ON readings.sensorId = a.sensId\n" +
                            "WHERE ts in '2019-10-21;1d'\n" +
                            "SAMPLE BY 1h;",
                    sink,
                    "ts\tcity\tmake\tavg\n" +
                            "2019-10-21T00:00:15.500000Z\tMiami\tOmron\t18.932522082097226\n" +
                            "2019-10-21T01:00:15.500000Z\tMiami\tOmron\t19.15925478107482\n" +
                            "2019-10-21T02:00:15.500000Z\tMiami\tOmron\t19.159665531591223\n" +
                            "2019-10-21T03:00:15.500000Z\tMiami\tOmron\t19.010622605362947\n" +
                            "2019-10-21T04:00:15.500000Z\tMiami\tOmron\t18.97741743469738\n" +
                            "2019-10-21T05:00:15.500000Z\tMiami\tOmron\t19.06602720501639\n" +
                            "2019-10-21T06:00:15.500000Z\tMiami\tOmron\t19.0154539187458\n" +
                            "2019-10-21T07:00:15.500000Z\tMiami\tOmron\t19.090575502276064\n" +
                            "2019-10-21T08:00:15.500000Z\tMiami\tOmron\t19.058070616124247\n" +
                            "2019-10-21T09:00:15.500000Z\tMiami\tOmron\t18.867127969081405\n" +
                            "2019-10-21T10:00:15.500000Z\tMiami\tOmron\t19.06682985165929\n" +
                            "2019-10-21T11:00:15.500000Z\tMiami\tOmron\t19.22028310819655\n" +
                            "2019-10-21T12:00:15.500000Z\tMiami\tOmron\t18.80882810933519\n" +
                            "2019-10-21T13:00:15.500000Z\tMiami\tOmron\t19.14103324474202\n" +
                            "2019-10-21T14:00:15.500000Z\tMiami\tOmron\t18.95574759642734\n" +
                            "2019-10-21T15:00:15.500000Z\tMiami\tOmron\t19.048820770397864\n" +
                            "2019-10-21T16:00:15.500000Z\tMiami\tOmron\t18.870082747356754\n" +
                            "2019-10-21T17:00:15.500000Z\tMiami\tOmron\t19.070063390729352\n" +
                            "2019-10-21T18:00:15.500000Z\tMiami\tOmron\t18.800281301245974\n" +
                            "2019-10-21T19:00:15.500000Z\tMiami\tOmron\t19.06787535086026\n" +
                            "2019-10-21T20:00:15.500000Z\tMiami\tOmron\t18.991759766316864\n" +
                            "2019-10-21T21:00:15.500000Z\tMiami\tOmron\t19.037181603168655\n" +
                            "2019-10-21T22:00:15.500000Z\tMiami\tOmron\t18.872801496558417\n" +
                            "2019-10-21T23:00:15.500000Z\tMiami\tOmron\t18.83742694955379\n" +
                            "2019-10-22T00:00:15.500000Z\tMiami\tOmron\t18.86576729294054\n" +
                            "2019-10-22T01:00:15.500000Z\tMiami\tOmron\t19.147747156078424\n" +
                            "2019-10-22T02:00:15.500000Z\tMiami\tOmron\t19.285711244931413\n" +
                            "2019-10-22T03:00:15.500000Z\tMiami\tOmron\t19.098624194171673\n" +
                            "2019-10-22T04:00:15.500000Z\tMiami\tOmron\t18.773860641442706\n" +
                            "2019-10-22T05:00:15.500000Z\tMiami\tOmron\t19.123521509981906\n" +
                            "2019-10-22T06:00:15.500000Z\tMiami\tOmron\t18.84440182119623\n" +
                            "2019-10-22T07:00:15.500000Z\tMiami\tOmron\t18.759557276148946\n" +
                            "2019-10-22T08:00:15.500000Z\tMiami\tOmron\t19.211618604307823\n" +
                            "2019-10-22T09:00:15.500000Z\tMiami\tOmron\t18.93353049132073\n" +
                            "2019-10-22T10:00:15.500000Z\tMiami\tOmron\t18.87472683854936\n" +
                            "2019-10-22T11:00:15.500000Z\tMiami\tOmron\t19.243116585499656\n" +
                            "2019-10-22T12:00:15.500000Z\tMiami\tOmron\t18.95200734422105\n" +
                            "2019-10-22T13:00:15.500000Z\tMiami\tOmron\t18.936687869662595\n" +
                            "2019-10-22T14:00:15.500000Z\tMiami\tOmron\t19.017821082620944\n" +
                            "2019-10-22T15:00:15.500000Z\tMiami\tOmron\t18.94411857118302\n" +
                            "2019-10-22T16:00:15.500000Z\tMiami\tOmron\t19.02323124842833\n" +
                            "2019-10-22T17:00:15.500000Z\tMiami\tOmron\t19.22329319385733\n" +
                            "2019-10-22T18:00:15.500000Z\tMiami\tOmron\t19.04591977492699\n" +
                            "2019-10-22T19:00:15.500000Z\tMiami\tOmron\t19.02326158364971\n" +
                            "2019-10-22T20:00:15.500000Z\tMiami\tOmron\t19.084012685666192\n" +
                            "2019-10-22T21:00:15.500000Z\tMiami\tOmron\t19.11105909280177\n" +
                            "2019-10-22T22:00:15.500000Z\tMiami\tOmron\t18.937124396725192\n" +
                            "2019-10-22T23:00:15.500000Z\tMiami\tOmron\t19.0127371108151\n"
            );
        });
    }

    @Test
    public void testLongCursor() throws Exception {
        assertQuery("x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n" +
                        "10\n",
                "select * from long_sequence(10)",
                null,
                null,
                true,
                true,
                true
        );


        // test another record count

        assertQuery("x\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n" +
                        "6\n" +
                        "7\n" +
                        "8\n" +
                        "9\n" +
                        "10\n" +
                        "11\n" +
                        "12\n" +
                        "13\n" +
                        "14\n" +
                        "15\n" +
                        "16\n" +
                        "17\n" +
                        "18\n" +
                        "19\n" +
                        "20\n",
                "select * from long_sequence(20)",
                null,
                null,
                true,
                true,
                true
        );

        // test 0 record count

        assertQuery("x\n",
                "select * from long_sequence(0)",
                null,
                null,
                true,
                true,
                true
        );

        assertQuery("x\n",
                "select * from long_sequence(-2)",
                null,
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testMaxDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(99.99975504094375, r.maxDouble(0), 0.00001);
        }
    }

    @Test
    public void testMaxDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(120)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(98.8401109488745, r.maxDouble(0), 0.00001);
        }
    }

    @Test
    public void testMinDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(1.743072089888109E-4, r.minDouble(0), 0.00001);
        }
    }

    @Test
    public void testMinDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(120)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(0.11075361080621349, r.minDouble(0), 0.00001);
        }
    }

    @Test
    public void testNamedBindVariableInWhere() throws Exception {
        assertMemoryLeak(() -> {

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                bindVariableService.clear();
                bindVariableService.setLong("var", 10);
                try (RecordCursorFactory factory = compiler.compile("select x from long_sequence(100) where x = :var", sqlExecutionContext).getRecordCursorFactory()) {
                    assertCursor(
                            "x\n" +
                                    "10\n",
                            factory,
                            true,
                            true,
                            false
                    );
                }
            }
        });
    }

    @Test
    public void testNonAggFunctionWithAggFunctionSampleBy() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "day\tisin\tlast\n" +
                        "1\tcc\t0.7544827361952741\n",
                "select day(ts), isin, last(start_price) from xetra where isin='cc' sample by 1d",
                "create table xetra as (" +
                        "select" +
                        " rnd_symbol('aa', 'bb', 'cc') isin," +
                        " rnd_double() start_price," +
                        " timestamp_sequence(0, 1000000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts)",
                null,
                false
        ));
    }

    @Test
    public void testNonAggFunctionWithAggFunctionSampleBySubQuery() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "day\tisin\tlast\n" +
                        "1\tcc\t0.7544827361952741\n",
//                "select day(ts), isin, last(start_price) from xetra where isin='cc' sample by 1d",
                "select day(ts), isin, last from (select ts, isin, last(start_price) from xetra where isin='cc' sample by 1d)",
                "create table xetra as (" +
                        "select" +
                        " rnd_symbol('aa', 'bb', 'cc') isin," +
                        " rnd_double() start_price," +
                        " timestamp_sequence(0, 1000000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts)",
                null,
                false
        ));
    }

    @Test
    public void testOrderBy() throws Exception {
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " order by b,a,x.a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n");
    }

    @Test
    public void testOrderByAllSupported() throws Exception {
        final String expected = "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\n" +
                "-2099411412\ttrue\t\tNaN\tNaN\t119\t2015-09-08T05:51:33.432Z\tPEHN\t8196152051414471878\t1970-01-01T05:16:40.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n" +
                "-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\n" +
                "-2077041000\ttrue\tM\t0.7340656260730631\t0.5026\t345\t2015-02-16T05:23:30.407Z\t\t-8534688874718947140\t1970-01-01T01:40:00.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                "-1915752164\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                "-1508370878\tfalse\t\tNaN\tNaN\t400\t2015-07-23T20:17:04.236Z\tHYRX\t-7146439436217962540\t1970-01-01T04:43:20.000000Z\t27\t00000000 fa 8d ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6\n" +
                "00000010 2c 23\tVLOMPBETTTKRIV\n" +
                "-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\n" +
                "-1234141625\tfalse\tC\t0.06381657870188628\t0.7606\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\n" +
                "-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\n" +
                "-857795778\ttrue\t\t0.07828020681514525\t0.2395\t519\t2015-06-12T11:35:40.668Z\tPEHN\t5360746485515325739\t1970-01-01T02:46:40.000000Z\t43\t\tDMIGQZVK\n" +
                "-682294338\ttrue\tG\t0.9153044839960652\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T05:00:00.000000Z\t38\t\tBOSEPGIUQZHEISQH\n" +
                "-42049305\tfalse\tW\t0.4698648140712085\t0.8912\t264\t2015-04-25T07:53:52.476Z\t\t-5296023984443079410\t1970-01-01T03:20:00.000000Z\t17\t00000000 9f 13 8f bb 2a 4b af 8f 89 df 35 8f\tOQKYHQQ\n" +
                "33027131\tfalse\tS\t0.15369837085455984\t0.5083\t107\t2015-08-04T00:55:25.323Z\t\t-8966711730402783587\t1970-01-01T03:53:20.000000Z\t48\t00000000 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb 01\tGZJYYFLSVIHDWWL\n" +
                "131103569\ttrue\tO\tNaN\tNaN\t658\t2015-12-24T01:28:12.922Z\tVTJW\t-7745861463408011425\t1970-01-01T03:36:40.000000Z\t43\t\tKXEJCTIZKYFLU\n" +
                "161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                "00000010 8e e5 61 2f\tQOLYXWC\n" +
                "971963578\ttrue\t\t0.22347827811588927\t0.7347\t925\t2015-01-03T11:24:48.587Z\tPEHN\t-8851773155849999621\t1970-01-01T04:10:00.000000Z\t40\t00000000 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47\tXHQUTZOD\n" +
                "976011946\ttrue\tU\t0.24001459007748394\t0.9292\t379\t\tVTJW\t3820631780839257855\t1970-01-01T02:13:20.000000Z\t12\t00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4\tGMXUKLGMXSLUQDYO\n" +
                "1150448121\ttrue\tC\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t1970-01-01T04:26:40.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                "00000010 00\t\n" +
                "1194691156\tfalse\tQ\tNaN\t0.2915\t348\t\tHYRX\t9026435187365103026\t1970-01-01T03:03:20.000000Z\t13\t00000000 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3\tIWZNFKPEVMC\n" +
                "1431425139\tfalse\t\t0.30716667810043663\t0.4275\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\n" +
                "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\n";

        assertQuery(expected,
                "x order by a,b,c,d,e,f,g,i,j,k,l,n",
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
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x(a,d,c,k) select * from (" +
                        "select" +
                        " 1194691157," +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\n" +
                        "-2099411412\ttrue\t\tNaN\tNaN\t119\t2015-09-08T05:51:33.432Z\tPEHN\t8196152051414471878\t1970-01-01T05:16:40.000000Z\t17\t00000000 05 2b 73 51 cf c3 7e c0 1d 6c a9 65 81 ad 79 87\tYWXBBZVRLPT\n" +
                        "-2088317486\tfalse\tU\t0.7446000371089992\tNaN\t651\t2015-07-18T10:50:24.009Z\tVTJW\t3446015290144635451\t1970-01-01T01:06:40.000000Z\t8\t00000000 92 fe 69 38 e1 77 9a e7 0c 89 14 58\tUMLGLHMLLEOY\n" +
                        "-2077041000\ttrue\tM\t0.7340656260730631\t0.5026\t345\t2015-02-16T05:23:30.407Z\t\t-8534688874718947140\t1970-01-01T01:40:00.000000Z\t34\t00000000 1c 0b 20 a2 86 89 37 11 2c 14\tUSZMZVQE\n" +
                        "-1915752164\tfalse\tI\t0.8786111112537701\t0.9966\t403\t2015-08-19T00:36:24.375Z\tCPSW\t-8506266080452644687\t1970-01-01T02:30:00.000000Z\t6\t00000000 9a ef 88 cb 4b a1 cf cf 41 7d a6\t\n" +
                        "-1508370878\tfalse\t\tNaN\tNaN\t400\t2015-07-23T20:17:04.236Z\tHYRX\t-7146439436217962540\t1970-01-01T04:43:20.000000Z\t27\t00000000 fa 8d ac 3d 98 a0 ad 9a 5d df dc 72 d7 97 cb f6\n" +
                        "00000010 2c 23\tVLOMPBETTTKRIV\n" +
                        "-1271909747\ttrue\tB\tNaN\t0.1250\t524\t2015-02-23T11:11:04.998Z\t\t-8955092533521658248\t1970-01-01T00:16:40.000000Z\t3\t00000000 de e4 7c d2 35 07 42 fc 31 79\tRSZSRYRFBVTMHG\n" +
                        "-1234141625\tfalse\tC\t0.06381657870188628\t0.7606\t397\t2015-02-14T21:43:16.924Z\tHYRX\t-8888027247206813045\t1970-01-01T01:56:40.000000Z\t10\t00000000 b3 14 33 80 c9 eb a3 67 7a 1a 79 e4 35 e4\tUIZULIGYVFZFK\n" +
                        "-1172180184\tfalse\tS\t0.5891216483879789\t0.2820\t886\t\tPEHN\t1761725072747471430\t1970-01-01T00:50:00.000000Z\t27\t\tIQBZXIOVIKJS\n" +
                        "-857795778\ttrue\t\t0.07828020681514525\t0.2395\t519\t2015-06-12T11:35:40.668Z\tPEHN\t5360746485515325739\t1970-01-01T02:46:40.000000Z\t43\t\tDMIGQZVK\n" +
                        "-682294338\ttrue\tG\t0.9153044839960652\t0.7943\t646\t2015-11-20T14:44:35.439Z\t\t8432832362817764490\t1970-01-01T05:00:00.000000Z\t38\t\tBOSEPGIUQZHEISQH\n" +
                        "-42049305\tfalse\tW\t0.4698648140712085\t0.8912\t264\t2015-04-25T07:53:52.476Z\t\t-5296023984443079410\t1970-01-01T03:20:00.000000Z\t17\t00000000 9f 13 8f bb 2a 4b af 8f 89 df 35 8f\tOQKYHQQ\n" +
                        "33027131\tfalse\tS\t0.15369837085455984\t0.5083\t107\t2015-08-04T00:55:25.323Z\t\t-8966711730402783587\t1970-01-01T03:53:20.000000Z\t48\t00000000 00 6b dd 18 fe 71 76 bc 45 24 cd 13 00 7c fb 01\tGZJYYFLSVIHDWWL\n" +
                        "131103569\ttrue\tO\tNaN\tNaN\t658\t2015-12-24T01:28:12.922Z\tVTJW\t-7745861463408011425\t1970-01-01T03:36:40.000000Z\t43\t\tKXEJCTIZKYFLU\n" +
                        "161592763\ttrue\tZ\t0.18769708157331322\t0.1638\t137\t2015-03-12T05:14:11.462Z\t\t7522482991756933150\t1970-01-01T00:33:20.000000Z\t43\t00000000 06 ac 37 c8 cd 82 89 2b 4d 5f f6 46 90 c3 b3 59\n" +
                        "00000010 8e e5 61 2f\tQOLYXWC\n" +
                        "971963578\ttrue\t\t0.22347827811588927\t0.7347\t925\t2015-01-03T11:24:48.587Z\tPEHN\t-8851773155849999621\t1970-01-01T04:10:00.000000Z\t40\t00000000 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47\tXHQUTZOD\n" +
                        "976011946\ttrue\tU\t0.24001459007748394\t0.9292\t379\t\tVTJW\t3820631780839257855\t1970-01-01T02:13:20.000000Z\t12\t00000000 8a b3 14 cd 47 0b 0c 39 12 f7 05 10 f4\tGMXUKLGMXSLUQDYO\n" +
                        "1150448121\ttrue\tC\t0.600707072503926\t0.7398\t663\t2015-08-17T00:23:29.874Z\tVTJW\t8873452284097498126\t1970-01-01T04:26:40.000000Z\t48\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                        "00000010 00\t\n" +
                        "1194691156\tfalse\tQ\tNaN\t0.2915\t348\t\tHYRX\t9026435187365103026\t1970-01-01T03:03:20.000000Z\t13\t00000000 71 3d 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3\tIWZNFKPEVMC\n" +
                        "1194691157\tfalse\tRXGZ\t88.69397617459538\tNaN\t0\t\t\tNaN\t1971-01-01T00:00:00.000000Z\t0\t\t\n" +
                        "1431425139\tfalse\t\t0.30716667810043663\t0.4275\t181\t2015-07-26T11:59:20.003Z\t\t-8546113611224784332\t1970-01-01T01:23:20.000000Z\t11\t00000000 d8 57 91 88 28 a5 18 93 bd 0b\tJOXPKRGIIHYH\n" +
                        "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByFull() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t19.202208853547866\t1970-01-03T00:00:00.000000Z\n" +
                        "\t32.5403220015421\t1970-01-03T18:00:00.000000Z\n" +
                        "BHF\t87.99634725391621\t1970-01-03T06:00:00.000000Z\n" +
                        "CPS\t80.43224099968394\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.93837147631712\t1970-01-03T03:00:00.000000Z\n" +
                        "DOT\t45.659895188239794\t1970-01-03T15:00:00.000000Z\n" +
                        "DXY\t2.1651819007252326\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.87423276940171\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.63904674818694\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707855875\t1970-01-03T12:00:00.000000Z\n" +
                        "KGH\t56.594291398612405\t1970-01-03T15:00:00.000000Z\n" +
                        "OFJ\t34.35685332942956\t1970-01-03T09:00:00.000000Z\n" +
                        "OOZ\t49.00510449885239\t1970-01-03T12:00:00.000000Z\n" +
                        "PGW\t55.99161804800813\t1970-01-03T03:00:00.000000Z\n" +
                        "RSZ\t41.38164748227684\t1970-01-03T09:00:00.000000Z\n" +
                        "UOJ\t63.81607531178513\t1970-01-03T06:00:00.000000Z\n" +
                        "UXI\t34.91070363730514\t1970-01-03T03:00:00.000000Z\n" +
                        "XPE\t20.447441837877754\t1970-01-03T00:00:00.000000Z\n" +
                        "YCT\t57.78947915182423\t1970-01-03T18:00:00.000000Z\n" +
                        "ZOU\t65.90341607692226\t1970-01-03T15:00:00.000000Z\n",
                // we have 'sample by fill(none)' because it doesn't support
                // random record access, which is what we intend on testing
                "select b, sum(a), k from x sample by 3h fill(none) order by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t19.202208853547866\t1970-01-03T00:00:00.000000Z\n" +
                        "\t32.5403220015421\t1970-01-03T18:00:00.000000Z\n" +
                        "\t3.831785863680992\t1970-01-04T03:00:00.000000Z\n" +
                        "\t71.33910271555844\t1970-01-04T06:00:00.000000Z\n" +
                        "BHF\t87.99634725391621\t1970-01-03T06:00:00.000000Z\n" +
                        "CPS\t80.43224099968394\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.93837147631712\t1970-01-03T03:00:00.000000Z\n" +
                        "DOT\t45.659895188239794\t1970-01-03T15:00:00.000000Z\n" +
                        "DXY\t2.1651819007252326\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.87423276940171\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.63904674818694\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707855875\t1970-01-03T12:00:00.000000Z\n" +
                        "KGH\t56.594291398612405\t1970-01-03T15:00:00.000000Z\n" +
                        "NVT\t95.40069089049732\t1970-01-04T06:00:00.000000Z\n" +
                        "OFJ\t34.35685332942956\t1970-01-03T09:00:00.000000Z\n" +
                        "OOZ\t49.00510449885239\t1970-01-03T12:00:00.000000Z\n" +
                        "PGW\t55.99161804800813\t1970-01-03T03:00:00.000000Z\n" +
                        "RSZ\t41.38164748227684\t1970-01-03T09:00:00.000000Z\n" +
                        "UOJ\t63.81607531178513\t1970-01-03T06:00:00.000000Z\n" +
                        "UXI\t34.91070363730514\t1970-01-03T03:00:00.000000Z\n" +
                        "WUG\t58.912164838797885\t1970-01-04T06:00:00.000000Z\n" +
                        "XIO\t14.830552335848957\t1970-01-04T09:00:00.000000Z\n" +
                        "XPE\t20.447441837877754\t1970-01-03T00:00:00.000000Z\n" +
                        "YCT\t57.78947915182423\t1970-01-03T18:00:00.000000Z\n" +
                        "ZOU\t65.90341607692226\t1970-01-03T15:00:00.000000Z\n",
                true);
    }

    @Test
    public void testOrderByFullSymbol() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t144.98448717090477\t1970-01-03T00:00:00.000000Z\n" +
                        "\t87.99634725391621\t1970-01-03T03:00:00.000000Z\n" +
                        "\t146.37943613686184\t1970-01-03T06:00:00.000000Z\n" +
                        "\t52.98405941762054\t1970-01-03T12:00:00.000000Z\n" +
                        "\t177.51319993464244\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t78.83065830055033\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t186.00010813544145\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t157.95345554678028\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t94.84889498017726\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t40.22810626779558\t1970-01-03T09:00:00.000000Z\n",
                // we have 'sample by fill(none)' because it doesn't support
                // random record access, which is what we intend on testing
                "select b, sum(a), k from x sample by 3h fill(none) order by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(4,4,4,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(4,4,4,2) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t144.98448717090477\t1970-01-03T00:00:00.000000Z\n" +
                        "\t87.99634725391621\t1970-01-03T03:00:00.000000Z\n" +
                        "\t146.37943613686184\t1970-01-03T06:00:00.000000Z\n" +
                        "\t52.98405941762054\t1970-01-03T12:00:00.000000Z\n" +
                        "\t177.51319993464244\t1970-01-03T15:00:00.000000Z\n" +
                        "\t57.78947915182423\t1970-01-04T03:00:00.000000Z\n" +
                        "\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t78.83065830055033\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t186.00010813544145\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t157.95345554678028\t1970-01-03T18:00:00.000000Z\n" +
                        "OUIC\t86.85154305419587\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t94.84889498017726\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "SDOT\t12.02416087573498\t1970-01-04T06:00:00.000000Z\n" +
                        "SDOT\t65.51335839796312\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t40.22810626779558\t1970-01-03T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testOrderByFullTimestampLead() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t19.202208853547866\t1970-01-03T00:00:00.000000Z\n" +
                        "CPS\t80.43224099968394\t1970-01-03T00:00:00.000000Z\n" +
                        "XPE\t20.447441837877754\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.93837147631712\t1970-01-03T03:00:00.000000Z\n" +
                        "PGW\t55.99161804800813\t1970-01-03T03:00:00.000000Z\n" +
                        "UXI\t34.91070363730514\t1970-01-03T03:00:00.000000Z\n" +
                        "BHF\t87.99634725391621\t1970-01-03T06:00:00.000000Z\n" +
                        "DXY\t2.1651819007252326\t1970-01-03T06:00:00.000000Z\n" +
                        "UOJ\t63.81607531178513\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.87423276940171\t1970-01-03T09:00:00.000000Z\n" +
                        "OFJ\t34.35685332942956\t1970-01-03T09:00:00.000000Z\n" +
                        "RSZ\t41.38164748227684\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.63904674818694\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707855875\t1970-01-03T12:00:00.000000Z\n" +
                        "OOZ\t49.00510449885239\t1970-01-03T12:00:00.000000Z\n" +
                        "DOT\t45.659895188239794\t1970-01-03T15:00:00.000000Z\n" +
                        "KGH\t56.594291398612405\t1970-01-03T15:00:00.000000Z\n" +
                        "ZOU\t65.90341607692226\t1970-01-03T15:00:00.000000Z\n" +
                        "\t32.5403220015421\t1970-01-03T18:00:00.000000Z\n" +
                        "YCT\t57.78947915182423\t1970-01-03T18:00:00.000000Z\n",
                // we have 'sample by fill(none)' because it doesn't support
                // random record access, which is what we intend on testing
                "select b, sum(a), k from x sample by 3h fill(none) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t19.202208853547866\t1970-01-03T00:00:00.000000Z\n" +
                        "CPS\t80.43224099968394\t1970-01-03T00:00:00.000000Z\n" +
                        "XPE\t20.447441837877754\t1970-01-03T00:00:00.000000Z\n" +
                        "DEY\t66.93837147631712\t1970-01-03T03:00:00.000000Z\n" +
                        "PGW\t55.99161804800813\t1970-01-03T03:00:00.000000Z\n" +
                        "UXI\t34.91070363730514\t1970-01-03T03:00:00.000000Z\n" +
                        "BHF\t87.99634725391621\t1970-01-03T06:00:00.000000Z\n" +
                        "DXY\t2.1651819007252326\t1970-01-03T06:00:00.000000Z\n" +
                        "UOJ\t63.81607531178513\t1970-01-03T06:00:00.000000Z\n" +
                        "EDR\t96.87423276940171\t1970-01-03T09:00:00.000000Z\n" +
                        "OFJ\t34.35685332942956\t1970-01-03T09:00:00.000000Z\n" +
                        "RSZ\t41.38164748227684\t1970-01-03T09:00:00.000000Z\n" +
                        "FBV\t77.63904674818694\t1970-01-03T12:00:00.000000Z\n" +
                        "JMY\t38.642336707855875\t1970-01-03T12:00:00.000000Z\n" +
                        "OOZ\t49.00510449885239\t1970-01-03T12:00:00.000000Z\n" +
                        "DOT\t45.659895188239794\t1970-01-03T15:00:00.000000Z\n" +
                        "KGH\t56.594291398612405\t1970-01-03T15:00:00.000000Z\n" +
                        "ZOU\t65.90341607692226\t1970-01-03T15:00:00.000000Z\n" +
                        "\t32.5403220015421\t1970-01-03T18:00:00.000000Z\n" +
                        "YCT\t57.78947915182423\t1970-01-03T18:00:00.000000Z\n" +
                        "\t3.831785863680992\t1970-01-04T03:00:00.000000Z\n" +
                        "\t71.33910271555844\t1970-01-04T06:00:00.000000Z\n" +
                        "NVT\t95.40069089049732\t1970-01-04T06:00:00.000000Z\n" +
                        "WUG\t58.912164838797885\t1970-01-04T06:00:00.000000Z\n" +
                        "XIO\t14.830552335848957\t1970-01-04T09:00:00.000000Z\n",
                true);
    }

    @Test
    public void testOrderByLong256AndChar() throws Exception {
        final String expected = "a\tb\tk\n" +
                "0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB\t1970-01-12T13:46:40.000000Z\n" +
                "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1970-01-01T00:00:00.000000Z\n" +
                "0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE\t1970-01-14T21:20:00.000000Z\n" +
                "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t1970-01-17T04:53:20.000000Z\n" +
                "0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\t1970-01-10T06:13:20.000000Z\n" +
                "0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t1970-01-03T07:33:20.000000Z\n" +
                "0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t1970-01-05T15:06:40.000000Z\n" +
                "0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ\t1970-01-22T23:46:40.000000Z\n" +
                "0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ\t1970-01-13T17:33:20.000000Z\n" +
                "0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\t1970-01-20T16:13:20.000000Z\n" +
                "0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t1970-01-06T18:53:20.000000Z\n" +
                "0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t1970-01-11T10:00:00.000000Z\n" +
                "0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR\t1970-01-19T12:26:40.000000Z\n" +
                "0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS\t1970-01-18T08:40:00.000000Z\n" +
                "0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU\t1970-01-16T01:06:40.000000Z\n" +
                "0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\t1970-01-07T22:40:00.000000Z\n" +
                "0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t1970-01-02T03:46:40.000000Z\n" +
                "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t1970-01-09T02:26:40.000000Z\n" +
                "0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ\t1970-01-21T20:00:00.000000Z\n" +
                "0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t1970-01-04T11:20:00.000000Z\n";

        final String expected2 = "a\tb\tk\n" +
                "0x58dfd08eeb9cc39ecec82869edec121bc2593f82b430328d84a09f29df637e38\tB\t1970-01-12T13:46:40.000000Z\n" +
                "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t1970-01-01T00:00:00.000000Z\n" +
                "0x10bb226eb4243e3683b91ec970b04e788a50f7ff7f6ed3305705e75fe328fa9d\tE\t1970-01-14T21:20:00.000000Z\n" +
                "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t1970-01-17T04:53:20.000000Z\n" +
                "0x8a538661f350d0b46f06560981acb5496adc00ebd29fdd5373dee145497c5436\tH\t1970-01-10T06:13:20.000000Z\n" +
                "0x980eca62a219a0f16846d7a3aa5aecce322a2198864beb14797fa69eb8fec6cc\tH\t1970-01-03T07:33:20.000000Z\n" +
                "0x73b27651a916ab1b568bc2d7a4aa860483881d4171847cf36e60a01a5b3ea0db\tI\t1970-01-05T15:06:40.000000Z\n" +
                "0x9b27eba5e9cfa1e29660300cea7db540954a62eca44acb2d71660a9b0890a2f0\tJ\t1970-01-22T23:46:40.000000Z\n" +
                "0x4c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39bccb30ed7795ebc8\tJ\t1970-01-13T17:33:20.000000Z\n" +
                "0x69440048957ae05360802a2ca499f211b771e27f939096b9c356f99ae70523b5\tM\t1970-01-20T16:13:20.000000Z\n" +
                "0x87aa0968faec6879a0d8cea7196b33a07e828f56aaa12bde8d076bf991c0ee88\tP\t1970-01-06T18:53:20.000000Z\n" +
                "0x9c8afa23e6ca6ca17c1b058af93c08086bafc47f4abcd93b7f98b0c74238337e\tP\t1970-01-11T10:00:00.000000Z\n" +
                "0x4cd64b0b0a344f8e6698c6c186b7571a9cba3ef59083484d98c2d832d83de993\tR\t1970-01-19T12:26:40.000000Z\n" +
                "0x3ad08d6037d3ce8155c06051ee52138b655f87a3a21d575f610f69efe063fe79\tS\t1970-01-18T08:40:00.000000Z\n" +
                "0xbacd57f41b59057caa237cfb02a208e494cfe42988a633de738bab883dc7e332\tU\t1970-01-16T01:06:40.000000Z\n" +
                "0xc736a8b67656c4f159d574d2ff5fb1e3687a84abb7bfac3ebedf29efb28cdcb1\tW\t1971-01-01T00:00:00.000000Z\n" +
                "0xc718ab5cbb3fd261c1bf6c24be53876861b1a0b0a559551538b73d329210d277\tY\t1970-01-07T22:40:00.000000Z\n" +
                "0xdb2d34586f6275fab5b2159a23565217965d4c984f0ffa8a7bcd48d8c77aa655\tY\t1970-01-02T03:46:40.000000Z\n" +
                "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t1970-01-09T02:26:40.000000Z\n" +
                "0x9a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d645277ee62a5a6e9fb\tZ\t1970-01-21T20:00:00.000000Z\n" +
                "0x2f1a8266e7921e3b716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288\tZ\t1970-01-04T11:20:00.000000Z\n";

        assertQuery(expected,
                "select * from x " +
                        " order by b, a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_long256() a," +
                        " rnd_char() b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_long256()," +
                        " 'W'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                expected2,
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByNonUnique() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n";

        assertQuery(expected,
                "x order by c",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_int()," +
                        " 'J'," +
                        " to_timestamp('1971', 'yyyy') t," +
                        " 'APPC'" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tc\tk\tn\n" +
                        "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                        "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                        "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                        "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                        "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                        "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                        "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                        "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                        "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                        "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                        "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                        "1570930196\tJ\t1971-01-01T00:00:00.000000Z\tAPPC\n" +
                        "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                        "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                        "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                        "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                        "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByTimestamp() throws Exception {
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "select * from x where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                        " order by k",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'RXGZ'," +
                        " to_timestamp('1971', 'yyyy') t" +
                        " from long_sequence(3)" +
                        ") timestamp(t)",
                expected +
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n" +
                        "88.2822836669774\tRXGZ\t1971-01-01T00:00:00.000000Z\n" +
                        "72.30015763133606\tRXGZ\t1971-01-01T00:00:00.000000Z\n"
        );
    }

    @Test
    public void testOrderByTimestampLead() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-2105201404\tB\t1970-01-01T00:00:01.000000Z\tGHWVDKFL\n" +
                "-1966408995\t\t1970-01-01T00:00:01.000000Z\tBZXIOVIKJSMSS\n" +
                "-1715058769\tE\t1970-01-01T00:00:01.000000Z\tQEHBHFOWL\n" +
                "-1470806499\t\t1970-01-01T00:00:01.000000Z\t\n" +
                "-1204245663\tJ\t1970-01-01T00:00:01.000000Z\tPKRGIIHYHBOQMY\n" +
                "-1182156192\t\t1970-01-01T00:00:01.000000Z\tGLUOHNZHZS\n" +
                "-1148479920\tJ\t1970-01-01T00:00:01.000000Z\tPSWHYRXPEH\n" +
                "-938514914\tX\t1970-01-01T00:00:01.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "-514934130\tH\t1970-01-01T00:00:01.000000Z\t\n" +
                "-235358133\tY\t1970-01-01T00:00:01.000000Z\tCXZOUICWEK\n" +
                "-147343840\tD\t1970-01-01T00:00:01.000000Z\tOGIFOUSZMZVQEB\n" +
                "116799613\tI\t1970-01-01T00:00:01.000000Z\tZEPIHVLTOVLJUML\n" +
                "326010667\tS\t1970-01-01T00:00:01.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "410717394\tO\t1970-01-01T00:00:01.000000Z\tGETJR\n" +
                "852921272\tC\t1970-01-01T00:00:01.000000Z\tLSUWDSWUGSHOLN\n" +
                "1431775887\tC\t1970-01-01T00:00:01.000000Z\tEHNOMVELLKKHT\n" +
                "1545253512\tX\t1970-01-01T00:00:01.000000Z\tSXUXIBBTGPGWFFY\n" +
                "1743740444\tS\t1970-01-01T00:00:01.000000Z\tTKVVSJ\n" +
                "1876812930\tV\t1970-01-01T00:00:01.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "1907911110\tE\t1970-01-01T00:00:01.000000Z\tPHRIPZIMNZ\n";

        assertQuery(expected,
                "x order by k,a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " cast(1000000 as timestamp) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " 852921272," +
                        " 'J'," +
                        " cast(1000000 as timestamp) t," +
                        " 'APPC'" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tc\tk\tn\n" +
                        "-2105201404\tB\t1970-01-01T00:00:01.000000Z\tGHWVDKFL\n" +
                        "-1966408995\t\t1970-01-01T00:00:01.000000Z\tBZXIOVIKJSMSS\n" +
                        "-1715058769\tE\t1970-01-01T00:00:01.000000Z\tQEHBHFOWL\n" +
                        "-1470806499\t\t1970-01-01T00:00:01.000000Z\t\n" +
                        "-1204245663\tJ\t1970-01-01T00:00:01.000000Z\tPKRGIIHYHBOQMY\n" +
                        "-1182156192\t\t1970-01-01T00:00:01.000000Z\tGLUOHNZHZS\n" +
                        "-1148479920\tJ\t1970-01-01T00:00:01.000000Z\tPSWHYRXPEH\n" +
                        "-938514914\tX\t1970-01-01T00:00:01.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "-514934130\tH\t1970-01-01T00:00:01.000000Z\t\n" +
                        "-235358133\tY\t1970-01-01T00:00:01.000000Z\tCXZOUICWEK\n" +
                        "-147343840\tD\t1970-01-01T00:00:01.000000Z\tOGIFOUSZMZVQEB\n" +
                        "116799613\tI\t1970-01-01T00:00:01.000000Z\tZEPIHVLTOVLJUML\n" +
                        "326010667\tS\t1970-01-01T00:00:01.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "410717394\tO\t1970-01-01T00:00:01.000000Z\tGETJR\n" +
                        "852921272\tJ\t1970-01-01T00:00:01.000000Z\tAPPC\n" +
                        "852921272\tC\t1970-01-01T00:00:01.000000Z\tLSUWDSWUGSHOLN\n" +
                        "1431775887\tC\t1970-01-01T00:00:01.000000Z\tEHNOMVELLKKHT\n" +
                        "1545253512\tX\t1970-01-01T00:00:01.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "1743740444\tS\t1970-01-01T00:00:01.000000Z\tTKVVSJ\n" +
                        "1876812930\tV\t1970-01-01T00:00:01.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "1907911110\tE\t1970-01-01T00:00:01.000000Z\tPHRIPZIMNZ\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByTwoStrings() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n";

        assertQuery(expected,
                "x order by c,n desc",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_int()," +
                        " 'J'," +
                        " to_timestamp('1971', 'yyyy') t," +
                        " 'ZZCC'" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tc\tk\tn\n" +
                        "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
                        "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                        "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                        "-2105201404\tB\t1970-01-01T04:10:00.000000Z\tGHWVDKFL\n" +
                        "852921272\tC\t1970-01-01T02:13:20.000000Z\tLSUWDSWUGSHOLN\n" +
                        "1431775887\tC\t1970-01-01T05:16:40.000000Z\tEHNOMVELLKKHT\n" +
                        "-147343840\tD\t1970-01-01T05:00:00.000000Z\tOGIFOUSZMZVQEB\n" +
                        "-1715058769\tE\t1970-01-01T00:33:20.000000Z\tQEHBHFOWL\n" +
                        "1907911110\tE\t1970-01-01T03:36:40.000000Z\tPHRIPZIMNZ\n" +
                        "-514934130\tH\t1970-01-01T03:20:00.000000Z\t\n" +
                        "116799613\tI\t1970-01-01T03:03:20.000000Z\tZEPIHVLTOVLJUML\n" +
                        "1570930196\tJ\t1971-01-01T00:00:00.000000Z\tZZCC\n" +
                        "-1148479920\tJ\t1970-01-01T00:00:00.000000Z\tPSWHYRXPEH\n" +
                        "-1204245663\tJ\t1970-01-01T04:26:40.000000Z\tPKRGIIHYHBOQMY\n" +
                        "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                        "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                        "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderByUnsupportedType() throws Exception {
        assertFailure(
                "x order by a,m,n",
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
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                13, "unsupported column type: BINARY"
        );

    }

    @Test
    public void testOrderChar() throws Exception {
        assertQuery("a\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "T\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                "select * from x order by a",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_char() a" +
                        " from" +
                        " long_sequence(20)" +
                        ") partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_char()" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "a\n" +
                        "C\n" +
                        "E\n" +
                        "G\n" +
                        "H\n" +
                        "H\n" +
                        "I\n" +
                        "J\n" +
                        "N\n" +
                        "P\n" +
                        "P\n" +
                        "R\n" +
                        "R\n" +
                        "S\n" +
                        "S\n" +
                        "T\n" +
                        "U\n" +
                        "V\n" +
                        "W\n" +
                        "W\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "X\n" +
                        "Y\n" +
                        "Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderUsingIndexAndInterval() throws Exception {
        final String expected = "a\tb\tk\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n";

        assertQuery(expected,
                "x where k IN '1970-01' order by b asc",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by MONTH",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                        "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                        "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                        "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                        "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                        "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                        "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                        "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                        "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "56.594291398612405\tABC\t1970-01-24T00:00:00.000000Z\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                        "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                        "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                        "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n",
                true,
                true,
                true

        );
    }

    @Test
    public void testOrderUsingIndexAndIntervalDesc() throws Exception {
        final String expected = "a\tb\tk\n" +
                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "x where k IN '1970-01' order by b desc",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by MONTH",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                        "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                        "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                        "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612405\tABC\t1970-01-24T00:00:00.000000Z\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                        "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                        "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                        "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                        "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                        "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                        "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                        "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                        "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                true,
                true,
                true

        );
    }

    @Test
    public void testOrderUsingIndexAndIntervalTimestampDesc() throws Exception {
        final String expected = "a\tb\tk\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n";

        assertQuery(expected,
                "x where k IN '1970-01' order by b, k desc",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by MONTH",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                        "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                        "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                        "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                        "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                        "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                        "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                        "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                        "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "56.594291398612405\tABC\t1970-01-24T00:00:00.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                        "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                        "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                        "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testOrderUsingIndexIntervalTooWide() throws Exception {
        final String expected = "a\tb\tk\n" +
                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";

        assertQuery(expected,
                "x where k IN '1970-01' order by b desc, k",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "),index(b) timestamp(k) partition by DAY",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100," +
                        " 'ABC'," +
                        " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                        " from long_sequence(1)" +
                        ") timestamp(t)",
                "a\tb\tk\n" +
                        "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                        "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                        "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                        "81.46807944500559\tPEHN\t1970-01-09T02:26:40.000000Z\n" +
                        "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n" +
                        "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                        "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                        "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                        "56.594291398612405\tABC\t1970-01-24T00:00:00.000000Z\n" +
                        "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                        "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n" +
                        "32.881769076795045\t\t1970-01-06T18:53:20.000000Z\n" +
                        "57.93466326862211\t\t1970-01-10T06:13:20.000000Z\n" +
                        "26.922103479744898\t\t1970-01-13T17:33:20.000000Z\n" +
                        "52.98405941762054\t\t1970-01-14T21:20:00.000000Z\n" +
                        "97.5019885372507\t\t1970-01-17T04:53:20.000000Z\n" +
                        "80.01121139739173\t\t1970-01-19T12:26:40.000000Z\n" +
                        "92.050039469858\t\t1970-01-20T16:13:20.000000Z\n" +
                        "45.6344569609078\t\t1970-01-21T20:00:00.000000Z\n" +
                        "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleByFillLinearEmptyCursor() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x where b = 'ZZZ' sample by 3h fill(linear) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleByFillNoneEmptyCursor() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x where b = 'ZZZ' sample by 3h fill(none) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                true
        );
    }

    @Test
    public void testSampleByFillNullEmptyCursor() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x where b = 'ZZZ' sample by 3h fill(null) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                true
        );
    }

    @Test
    public void testSampleByFillPrevEmptyCursor() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x where b = 'ZZZ' sample by 3h fill(prev) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                true
        );
    }

    @Test
    public void testSampleByFillValueEmptyCursor() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x where b = 'ZZZ' sample by 3h fill(10.0) order by k,b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(3,3,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                true
        );
    }

    @Test
    public void testSelectColumns() throws Exception {
        assertQuery("a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t0.7611\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                        "10\t1253890363\tfalse\tXYS\t0.1911234617573182\t0.5793\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.04142812470232493\t0.9205\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                        "18\t-1201923128\ttrue\tUVS\t0.7588175403454873\t0.5779\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                        "NaN\t865832060\ttrue\t\t0.14830552335848957\t0.9442\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                        "00000010 38 e1\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t0.7633\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.33747075654972813\t0.1179\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                        "00000010 28 60\n" +
                        "4\t39497392\tfalse\tUOH\t0.029227696942726644\t0.1718\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\n" +
                        "10\t1545963509\tfalse\tNWI\t0.11371841836123953\t0.0620\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                        "4\t53462821\tfalse\tGOO\t0.05514933756198426\t0.1195\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                        "30\t-2139296159\tfalse\t\t0.18586435581637295\t0.5638\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                        "17\t415709351\tfalse\tGQZ\t0.49199001716312474\t0.6292\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                        "00000010 44 a8 0d fe\n" +
                        "19\t-1387693529\ttrue\tMCG\t0.848083900630095\t0.4699\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                        "00000010 20 53 3b 51\n" +
                        "21\t346891421\tfalse\t\t0.933609514582851\t0.6380\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                        "27\t263487884\ttrue\tHZQ\t0.7039785408034679\t0.8461\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                        "9\t-1034870849\tfalse\tLSV\t0.6506604601705693\t0.7020\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                        "26\t1848218326\ttrue\tSUW\t0.8034049105590781\t0.0440\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "5\t-1496904948\ttrue\tDBZ\t0.2862717364877081\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "20\t856634079\ttrue\tRJU\t0.10820602386069589\t0.4565\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                        "00000010 ab 3f a1 f5\n",
                "select a,a1,b,c,d,e,f1,f,g,h,i,j,j1,k,l,m from x",
                "create table x as (" +
                        "select" +
                        "  rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m" +
                        " from long_sequence(20)" +
                        ")  timestamp(k) partition by DAY",
                "k",
                true,
                true,
                true
        );
    }

    @Test
    public void testSelectColumnsSansTimestamp() throws Exception {
        assertQuery("a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t0.7611\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\n" +
                        "10\t1253890363\tfalse\tXYS\t0.1911234617573182\t0.5793\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.04142812470232493\t0.9205\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\n" +
                        "18\t-1201923128\ttrue\tUVS\t0.7588175403454873\t0.5779\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\n" +
                        "NaN\t865832060\ttrue\t\t0.14830552335848957\t0.9442\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t0.7633\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.33747075654972813\t0.1179\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\n" +
                        "4\t39497392\tfalse\tUOH\t0.029227696942726644\t0.1718\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\n" +
                        "10\t1545963509\tfalse\tNWI\t0.11371841836123953\t0.0620\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\n" +
                        "4\t53462821\tfalse\tGOO\t0.05514933756198426\t0.1195\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\n" +
                        "30\t-2139296159\tfalse\t\t0.18586435581637295\t0.5638\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\n" +
                        "17\t415709351\tfalse\tGQZ\t0.49199001716312474\t0.6292\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\n" +
                        "19\t-1387693529\ttrue\tMCG\t0.848083900630095\t0.4699\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\n" +
                        "21\t346891421\tfalse\t\t0.933609514582851\t0.6380\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\n" +
                        "27\t263487884\ttrue\tHZQ\t0.7039785408034679\t0.8461\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\n" +
                        "9\t-1034870849\tfalse\tLSV\t0.6506604601705693\t0.7020\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\n" +
                        "26\t1848218326\ttrue\tSUW\t0.8034049105590781\t0.0440\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\n" +
                        "5\t-1496904948\ttrue\tDBZ\t0.2862717364877081\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\n" +
                        "20\t856634079\ttrue\tRJU\t0.10820602386069589\t0.4565\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\n",
                "select a,a1,b,c,d,e,f1,f,g,h,i,j,j1 from x",
                "create table x as (" +
                        "select" +
                        " rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m" +
                        " from long_sequence(20)" +
                        ")  timestamp(k) partition by DAY",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testSelectDistinct() throws Exception {
        final String expected = "a\n" +
                "0\n" +
                "8\n" +
                "3\n" +
                "1\n" +
                "9\n" +
                "2\n" +
                "6\n" +
                "4\n" +
                "7\n" +
                "5\n";

        assertQuery(expected,
                "select distinct a from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " abs(rnd_int())%10 a" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " abs(rnd_int())%10 a" +
                        " from long_sequence(1000000)" +
                        ") ",
                expected, true);
    }

    @Test
    public void testSelectDistinctSymbol() throws Exception {
        final String expected = "a\n" +
                "EHNRX\n" +
                "BHFOW\n" +
                "QULOF\n" +
                "RUEDR\n" +
                "SZSRY\n" +
                "YYQE\n" +
                "IBBTGP\n" +
                "TJWC\n" +
                "ZSXU\n" +
                "CCXZ\n" +
                "KGHVUV\n" +
                "SWHYRX\n" +
                "OUOJS\n" +
                "PDXYSB\n" +
                "OOZZ\n" +
                "WFFYUD\n" +
                "DZJMY\n" +
                "GETJ\n" +
                "FBVTMH\n" +
                "UICW\n" +
                "\n";

        final String expected2 = "a\n" +
                "EHNRX\n" +
                "BHFOW\n" +
                "QULOF\n" +
                "RUEDR\n" +
                "SZSRY\n" +
                "YYQE\n" +
                "IBBTGP\n" +
                "TJWC\n" +
                "ZSXU\n" +
                "CCXZ\n" +
                "KGHVUV\n" +
                "SWHYRX\n" +
                "OUOJS\n" +
                "PDXYSB\n" +
                "OOZZ\n" +
                "WFFYUD\n" +
                "DZJMY\n" +
                "GETJ\n" +
                "FBVTMH\n" +
                "UICW\n" +
                "SSCL\n" +
                "HLDN\n" +
                "IIB\n" +
                "ROGHY\n" +
                "CJFT\n" +
                "WNX\n" +
                "VZKE\n" +
                "NDMRS\n" +
                "SVNVD\n" +
                "ILQP\n" +
                "\n";

        assertQuery(expected,
                "select distinct a from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol(20,4,6,2) a" +
                        " from" +
                        " long_sequence(10000)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_symbol(10,3,5,0) a" +
                        " from long_sequence(1000000)" +
                        ") ",
                expected2,
                true,
                true,
                true
        );
    }

    @Test
    public void testSelectFromAliasedTable() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(CREATE_TABLE, compiler.compile("create table my_table (sym int, id long)", sqlExecutionContext).getType());
            try (RecordCursorFactory factory = compiler.compile("select sum(a.sym) yo, a.id from my_table a", sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertNotNull(factory);
            }
        });
    }

    @Test
    public void testSelectUndefinedBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            try (RecordCursorFactory factory = compiler.compile("select $1+x, $2 from long_sequence(10)", sqlExecutionContext).getRecordCursorFactory()) {
                sink.clear();
                factory.getMetadata().toJson(sink);
                TestUtils.assertEquals("{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"column\",\"type\":\"LONG\"},{\"index\":1,\"name\":\"$2\",\"type\":\"STRING\"}],\"timestampIndex\":-1}", sink);
            }
        });
    }

    @Test
    public void testSumDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(6.004476595511992E7, r.sumDouble(0), 0.00001);
        }
    }

    @Test
    public void testSumDoubleColumnPartitionByNone() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k)",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(6.004476595511992E7, r.sumDouble(0), 0.00001);
        }
    }

    @Test
    public void testSumDoubleColumnWithKahanMethodVectorised1() throws Exception {
        String ddl = "create table x (ds double) partition by NONE";
        compiler.compile(ddl, sqlExecutionContext);

        executeInsertStatement(1.0);
        executeInsertStatement(2.0);

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(3, r.sumDouble(0), 0.00001);
        }
    }

    @Test
    public void testSumDoubleColumnWithKahanMethodVectorised2() throws Exception {
        String ddl = "create table x (ds double) partition by NONE";
        compiler.compile(ddl, sqlExecutionContext);

        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(0.0);
        executeInsertStatement(0.0);


        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(8, r.sumDouble(0), 0.0000001);
        }
    }

    @Test
    public void testSumDoubleColumnWithKahanMethodVectorised3() throws Exception {
        String ddl = "create table x (ds double) partition by NONE";
        compiler.compile(ddl, sqlExecutionContext);

        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);

        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(1.0);
        executeInsertStatement(0.0);


        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(12, r.sumDouble(0), 0.0000001);
        }
    }

    @Test
    public void testSumDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(2)*100 a," +
                        " timestamp_sequence(0, 10000) k" +
                        " from" +
                        " long_sequence(1200000)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(5.001433965140632E7, r.sumDouble(0), 0.00001);
        }
    }

    @Test
    public void testSymbolStrB() throws Exception {
        assertQuery("a\nC\nC\nB\nA\nA\n",
                "select cast(a as string) a from x order by 1 desc",
                "create table x as (select rnd_symbol('A','B','C') a, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)",
                null,
                null,
                null,
                true,
                false,
                true
        );
    }

    @Test
    public void testTimestampCrossReference() throws Exception {
        compiler.compile("create table x (val double, t timestamp)", sqlExecutionContext);
        compiler.compile("create table y (timestamp timestamp, d double)", sqlExecutionContext);
        compiler.compile("insert into y select timestamp_sequence(cast('2018-01-31T23:00:00.000000Z' as timestamp), 100), rnd_double() from long_sequence(1000)", sqlExecutionContext);

        // to shut up memory leak check
        engine.clear();
        assertQuery(
                "time\tvisMiles\n" +
                        "2018-01-31T23:00:00.000000Z\t0.26625499503275796\n" +
                        "2018-01-31T23:00:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:00:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:01:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:01:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:01:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:02:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:02:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:02:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:03:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:03:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:03:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:04:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:04:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:04:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:05:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:05:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:05:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:06:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:06:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:06:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:07:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:07:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:07:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:08:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:08:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:08:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:09:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:09:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:09:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:10:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:10:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:10:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:11:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:11:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:11:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:12:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:12:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:12:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:13:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:13:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:13:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:14:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:14:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:14:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:15:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:15:20.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:15:40.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:16:00.000000Z\t0.2647050470565634\n" +
                        "2018-01-31T23:16:20.000000Z\t0.2647050470565634\n",
                "SELECT\n" +
                        "    t as \"time\",\n" +
                        "    avg(d) as visMiles\n" +
                        "FROM ((x timestamp(t)) WHERE t BETWEEN '2018-01-31T23:00:00Z' AND '2018-02-28T22:59:59Z')\n" +
                        "ASOF JOIN (y timestamp(timestamp))\n" +
                        "SAMPLE BY 20s",
                "insert into x select rnd_double(), timestamp_sequence(cast('2018-01-31T23:00:00.000000Z' as timestamp), 10000) from long_sequence(100000)",
                "time",
                false
        );
    }

    @Test
    public void testTimestampPropagation() throws Exception {
        compiler.compile("create table readings (sensorId int)", sqlExecutionContext);
        compiler.compile("create table sensors (ID int, make symbol, city symbol)", sqlExecutionContext);
        assertQuery(
                "sensorId\tsensId\tmake\tcity\n",
                "SELECT * FROM readings JOIN(SELECT ID sensId, make, city FROM sensors) ON readings.sensorId = sensId",
                null,
                false
        );
    }

    @Test
    public void testVectorAggregateOnSparsePartitions() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(expected,
                "x where 1 = 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " timestamp_sequence(0, (x/100) * 1000000*60*60*24*2 + (x%100)) k" +
                        " from" +
                        " long_sequence(120)" +
                        ") timestamp(k) partition by DAY",
                "k",
                false,
                true,
                true
        );

        try (TableReader r = new TableReader(configuration, "x")) {
            Assert.assertEquals(6158.373651379578, r.sumDouble(0), 0.00001);
            Assert.assertEquals(0.11075361080621349, r.minDouble(0), 0.00001);
            Assert.assertEquals(99.1809311486223, r.maxDouble(0), 0.00001);
            Assert.assertEquals(53.20159680986086, r.avgDouble(0), 0.00001);
        }
    }

    @Test
    public void testVectorSumAvgDoubleRndColumnWithNulls() throws Exception {
        assertQuery("avg\tsum\n" +
                        "0.49811606109211604\t17.932178199316176\n",
                "select avg(c),sum(c) from x",
                "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b, rnd_double(2) c, rnd_int() d from long_sequence(42))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testVectorSumAvgDoubleRndColumnWithNullsParallel() throws Exception {

        Sequence seq = engine.getMessageBus().getVectorAggregateSubSeq();
        // consume sequence fully and do nothing
        // this might be needed to make sure we don't consume things other tests publish here
        while (true) {
            long cursor = seq.next();
            if (cursor == -1) {
                break;
            } else if (cursor > -1) {
                seq.done(cursor);
            }
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
        final GroupByJob job = new GroupByJob(engine.getMessageBus());
        new Thread(() -> {
            while (running.get()) {
                job.run(0);
            }
            haltLatch.countDown();
        }).start();

        try {
            assertQuery("avg\tsum\n" +
                            "0.50035043\t834470.437288\n",
                    "select round(avg(c), 9) avg, round(sum(c), 6) sum from x",
                    "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b, rnd_double(2) c, rnd_int() d from long_sequence(2000000))",
                    null,
                    false,
                    true,
                    true
            );
        } finally {
            running.set(false);
            haltLatch.await();
        }
    }

    @Test
    public void testVectorSumDoubleAndIntWithNulls() throws Exception {
        assertQuery("sum\tsum1\n" +
                        "41676799\t416969.81549\n",
                "select sum(a),round(sum(b),5) sum1 from x",
                "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b from long_sequence(1000035L))",
                null,
                false,
                true,
                true
        );
    }

    @Test
    public void testVirtualColumns() throws Exception {
        assertQuery("a\ta1\tb\tc\tcolumn\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\t-1593\t428\t2015-04-04T16:34:47.226Z\t\t\t185\t7039584373105579285\t1970-01-01T00:00:00.000000Z\t4\t00000000 af 19 c4 95 94 36 53 49 b4 59 7e\n" +
                        "10\t1253890363\tfalse\tXYS\t0.7704700589519898\t-1379\t881\t\t2015-03-04T23:08:35.722465Z\tHYRX\t188\t-4986232506486815364\t1970-01-01T00:16:40.000000Z\t50\t00000000 42 fc 31 79 5f 8b 81 2b 93 4d 1a 8e 78 b5\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.9619284627798701\t-9039\t97\t2015-08-25T03:15:07.653Z\t2015-12-06T09:41:30.297134Z\tHYRX\t109\t571924429013198086\t1970-01-01T00:33:20.000000Z\t21\t\n" +
                        "18\t-1201923128\ttrue\tUVS\t1.33671228760272\t-4379\t480\t2015-12-16T09:15:02.086Z\t2015-05-31T18:12:45.686366Z\tCPSW\tNaN\t-6161552193869048721\t1970-01-01T00:50:00.000000Z\t27\t00000000 28 c7 84 47 dc d2 85 7f a5 b8 7b 4a 9d 46\n" +
                        "NaN\t865832060\ttrue\t\t1.0924714088069454\t2508\t95\t\t2015-10-20T09:33:20.502524Z\t\tNaN\t-3289070757475856942\t1970-01-01T01:06:40.000000Z\t40\t00000000 f2 3c ed 39 ac a8 3b a6 dc 3b 7d 2b e3 92 fe 69\n" +
                        "00000010 38 e1\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\t-17778\t698\t2015-09-13T09:55:17.815Z\t\tCPSW\t182\t-8757007522346766135\t1970-01-01T01:23:20.000000Z\t23\t\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.4553238616179349\t18904\t533\t2015-05-13T23:13:05.262Z\t2015-05-10T00:20:17.926993Z\t\t175\t6351664568801157821\t1970-01-01T01:40:00.000000Z\t29\t00000000 5d d0 eb 67 44 a7 6a 71 34 e0 b0 e9 98 f7 67 62\n" +
                        "00000010 28 60\n" +
                        "4\t39497392\tfalse\tUOH\t0.20103057532254842\t14242\t652\t\t2015-05-24T22:09:55.175991Z\tVTJW\t141\t3527911398466283309\t1970-01-01T01:56:40.000000Z\t9\t00000000 d9 6f 04 ab 27 47 8f 23 3f ae 7c 9f 77 04 e9 0c\n" +
                        "00000010 ea 4e ea 8b\n" +
                        "10\t1545963509\tfalse\tNWI\t0.17574587273746023\t-29980\t356\t2015-09-12T14:33:11.105Z\t2015-08-06T04:51:01.526782Z\t\t168\t6380499796471875623\t1970-01-01T02:13:20.000000Z\t13\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\n" +
                        "4\t53462821\tfalse\tGOO\t0.17466147831286927\t-6087\t115\t2015-08-09T19:28:14.249Z\t2015-09-20T01:50:37.694867Z\tCPSW\t145\t-7212878484370155026\t1970-01-01T02:30:00.000000Z\t46\t\n" +
                        "30\t-2139296159\tfalse\t\t0.7496385839123813\t21020\t299\t2015-12-30T22:10:50.759Z\t2015-01-19T15:54:44.696040Z\tHYRX\t105\t-3463832009795858033\t1970-01-01T02:46:40.000000Z\t38\t00000000 b8 07 b1 32 57 ff 9a ef 88 cb 4b\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\t21057\t968\t2015-10-17T07:20:26.881Z\t2015-06-02T13:00:45.180827Z\tPEHN\t102\t5360746485515325739\t1970-01-01T03:03:20.000000Z\t43\t\n" +
                        "17\t415709351\tfalse\tGQZ\t1.1211986415260702\t18605\t581\t2015-03-04T06:48:42.194Z\t2015-08-14T15:51:23.307152Z\tHYRX\t185\t-5611837907908424613\t1970-01-01T03:20:00.000000Z\t19\t00000000 20 e2 37 f2 64 43 84 55 a0 dd 44 11 e2 a3 24 4e\n" +
                        "00000010 44 a8 0d fe\n" +
                        "19\t-1387693529\ttrue\tMCG\t1.317948686301329\t24206\t119\t2015-03-01T23:54:10.204Z\t2015-10-01T12:02:08.698373Z\t\t175\t3669882909701240516\t1970-01-01T03:36:40.000000Z\t12\t00000000 8f bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85\n" +
                        "00000010 20 53 3b 51\n" +
                        "21\t346891421\tfalse\t\t1.571608691561916\t15084\t405\t2015-10-12T05:36:54.066Z\t2015-11-16T05:48:57.958190Z\tPEHN\t196\t-9200716729349404576\t1970-01-01T03:53:20.000000Z\t43\t\n" +
                        "27\t263487884\ttrue\tHZQ\t1.5500996731772778\t31562\t834\t2015-08-04T00:55:25.323Z\t2015-07-25T18:26:42.499255Z\tHYRX\t128\t8196544381931602027\t1970-01-01T04:10:00.000000Z\t15\t00000000 71 76 bc 45 24 cd 13 00 7c fb 01 19 ca f2\n" +
                        "9\t-1034870849\tfalse\tLSV\t1.3527049471700812\t-838\t110\t2015-08-17T23:50:39.534Z\t2015-03-17T03:23:26.126568Z\tHYRX\tNaN\t-6929866925584807039\t1970-01-01T04:26:40.000000Z\t4\t00000000 4b fb 2d 16 f3 89 a3 83 64 de\n" +
                        "26\t1848218326\ttrue\tSUW\t0.8474448752349815\t-3502\t854\t2015-04-04T20:55:02.116Z\t2015-11-23T07:46:10.570856Z\t\t145\t4290477379978201771\t1970-01-01T04:43:20.000000Z\t35\t00000000 6d 54 75 10 b3 4c 0e 8f f1 0c c5 60 b7 d1 5a\n" +
                        "5\t-1496904948\ttrue\tDBZ\tNaN\t5698\t764\t2015-02-06T02:49:54.147Z\t\t\tNaN\t-3058745577013275321\t1970-01-01T05:00:00.000000Z\t19\t00000000 d4 ab be 30 fa 8d ac 3d 98 a0 ad 9a 5d\n" +
                        "20\t856634079\ttrue\tRJU\t0.5646727582700282\t13505\t669\t2015-11-14T15:19:19.390Z\t\tVTJW\t134\t-3700177025310488849\t1970-01-01T05:16:40.000000Z\t3\t00000000 f8 a1 46 87 28 92 a3 9b e3 cb c2 64 8a b0 35 d8\n" +
                        "00000010 ab 3f a1 f5\n",
                "select a,a1,b,c,d+e,f1,f,g,h,i,j,j1,k,l,m from x",
                "create table x as (" +
                        "select" +
                        "  rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m" +
                        " from long_sequence(20)" +
                        ")  timestamp(k) partition by DAY",
                "k",
                true,
                true,
                true
        );
    }

    @Test
    public void testVirtualColumnsSansTimestamp() throws Exception {
        assertQuery("a\ta1\tb\tc\tcolumn\n" +
                        "NaN\t1569490116\tfalse\t\tNaN\n" +
                        "10\t1253890363\tfalse\tXYS\t0.7704700589519898\n" +
                        "27\t-1819240775\ttrue\tGOO\t0.9619284627798701\n" +
                        "18\t-1201923128\ttrue\tUVS\t1.33671228760272\n" +
                        "NaN\t865832060\ttrue\t\t1.0924714088069454\n" +
                        "22\t1100812407\tfalse\tOVL\tNaN\n" +
                        "18\t1677463366\tfalse\tMNZ\t0.4553238616179349\n" +
                        "4\t39497392\tfalse\tUOH\t0.20103057532254842\n" +
                        "10\t1545963509\tfalse\tNWI\t0.17574587273746023\n" +
                        "4\t53462821\tfalse\tGOO\t0.17466147831286927\n" +
                        "30\t-2139296159\tfalse\t\t0.7496385839123813\n" +
                        "21\t-406528351\tfalse\tNLE\tNaN\n" +
                        "17\t415709351\tfalse\tGQZ\t1.1211986415260702\n" +
                        "19\t-1387693529\ttrue\tMCG\t1.317948686301329\n" +
                        "21\t346891421\tfalse\t\t1.571608691561916\n" +
                        "27\t263487884\ttrue\tHZQ\t1.5500996731772778\n" +
                        "9\t-1034870849\tfalse\tLSV\t1.3527049471700812\n" +
                        "26\t1848218326\ttrue\tSUW\t0.8474448752349815\n" +
                        "5\t-1496904948\ttrue\tDBZ\tNaN\n" +
                        "20\t856634079\ttrue\tRJU\t0.5646727582700282\n",
                "select a,a1,b,c,d+e from x",
                "create table x as (" +
                        "select" +
                        " rnd_int() a1," +
                        " rnd_int(0, 30, 2) a," +
                        " rnd_boolean() b," +
                        " rnd_str(3,3,2) c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_short() f1," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_timestamp(to_timestamp('2015', 'yyyy'), to_timestamp('2016', 'yyyy'), 2) h," +
                        " rnd_symbol(4,4,4,2) i," +
                        " rnd_long(100,200,2) j," +
                        " rnd_long() j1," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m" +
                        " from long_sequence(20)" +
                        ")  timestamp(k) partition by DAY",
                null,
                true,
                true,
                true
        );
    }

    private void executeInsertStatement(double d) throws SqlException {
        String ddl = "insert into x (ds) values (" + d + ")";
        executeInsert(ddl);
    }

    private void testBindVariableWithLike0(String keyword) throws Exception {
        assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine)
            ) {
                compiler.compile("create table xy as (select rnd_str() v from long_sequence(100))", sqlExecutionContext);
                bindVariableService.clear();
                try (RecordCursorFactory factory = compiler.compile("xy where v " + keyword + " $1", sqlExecutionContext).getRecordCursorFactory()) {

                    bindVariableService.setStr(0, "MBE%");
                    assertCursor("v\n" +
                                    "MBEZGHW\n",
                            factory,
                            true,
                            true,
                            false
                    );

                    bindVariableService.setStr(0, "Z%");
                    assertCursor("v\n" +
                                    "ZSQLDGLOG\n" +
                                    "ZLUOG\n" +
                                    "ZLCBDMIG\n" +
                                    "ZJYYFLSVI\n" +
                                    "ZWEVQTQO\n" +
                                    "ZSFXUNYQ\n",
                            factory,
                            true,
                            true,
                            false
                    );

                    assertCursor("v\n" +
                                    "ZSQLDGLOG\n" +
                                    "ZLUOG\n" +
                                    "ZLCBDMIG\n" +
                                    "ZJYYFLSVI\n" +
                                    "ZWEVQTQO\n" +
                                    "ZSFXUNYQ\n",
                            factory,
                            true,
                            true,
                            false
                    );


                    bindVariableService.setStr(0, null);
                    assertCursor("v\n",
                            factory,
                            true,
                            true,
                            false
                    );
                }
            }
        });
    }
}
