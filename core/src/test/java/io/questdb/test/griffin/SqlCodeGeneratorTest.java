/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.groupby.vect.GroupByVectorAggregateJob;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class SqlCodeGeneratorTest extends AbstractCairoTest {

    @Test
    public void testAliasedColumnFollowedByWildcard() throws Exception {
        assertQuery(
                "k1\ta\tk\n" +
                        "1970-01-01T00:00:00.000000Z\t80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.010000Z\t8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                        "1970-01-01T00:00:00.020000Z\t8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                "select k as k1, * from x",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_double(0)*100 a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                true,
                true
        );
    }

    @Test
    public void testAliasedColumnFollowedByWildcardInJoinQuery() throws Exception {
        assertQuery(
                "col_k\ta\tk\tcol_k1\n" +
                        "1970-01-01T00:00:00.000000Z\t4689592037643856\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.010000Z\t4729996258992366\t1970-01-01T00:00:00.010000Z\t1970-01-01T00:00:00.010000Z\n" +
                        "1970-01-01T00:00:00.020000Z\t7746536061816329025\t1970-01-01T00:00:00.020000Z\t1970-01-01T00:00:00.020000Z\n",
                "select x1.k col_k, x1.*, x2.k col_k1 from x x1 join x x2 on x1.a = x2.a",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_long() a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                false,
                true
        );
    }

    @Test
    public void testAliasedColumnFollowedByWildcardInJoinQuery2() throws Exception {
        assertQuery(
                "col_k\ta\tk\ta1\tk1\n" +
                        "1970-01-01T00:00:00.000000Z\t4689592037643856\t1970-01-01T00:00:00.000000Z\t4689592037643856\t1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.010000Z\t4729996258992366\t1970-01-01T00:00:00.010000Z\t4729996258992366\t1970-01-01T00:00:00.010000Z\n" +
                        "1970-01-01T00:00:00.020000Z\t7746536061816329025\t1970-01-01T00:00:00.020000Z\t7746536061816329025\t1970-01-01T00:00:00.020000Z\n",
                "select x1.k col_k, * from x x1 join x x2 on x1.a = x2.a",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_long() a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                false,
                true
        );
    }

    @Test
    public void testAmbiguousFunction() throws Exception {
        assertQuery("column\n" +
                        "234990000000000\n",
                "select 23499000000000*10 from long_sequence(1)",
                null, null, true, true
        );
    }

    @Test
    public void testAvgDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testAvgDoubleColumnPartitionByNone() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testAvgDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testAvgDoubleEmptyColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testBindVariableInIndexedLookup() throws Exception {
        testBindVariableInIndexedLookup(true);
    }

    @Test
    public void testBindVariableInListIndexedLookup() throws Exception {
        testBindVariableInLookupList(true);
    }

    @Test
    public void testBindVariableInListNonIndexedLookup() throws Exception {
        testBindVariableInLookupList(false);
    }

    @Test
    public void testBindVariableInNonIndexedLookup() throws Exception {
        testBindVariableInIndexedLookup(false);
    }

    @Test
    public void testBindVariableInSelect() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 10);

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select x, $1 from long_sequence(2)")) {
                assertCursor(
                        "x\t$1\n" +
                                "1\t10\n" +
                                "2\t10\n",
                        factory,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testBindVariableInSelect2() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong("y", 10);

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select x, :y from long_sequence(2)")) {
                assertCursor(
                        "x\t:y\n" +
                                "1\t10\n" +
                                "2\t10\n",
                        factory,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testBindVariableInSelect3() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 10);

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select x, $1 from long_sequence(2)")) {
                assertCursor(
                        "x\t$1\n" +
                                "1\t10\n" +
                                "2\t10\n",
                        factory,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testBindVariableInWhere() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong(0, 10);

            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("select x from long_sequence(100) where x = $1")) {
                assertCursor(
                        "x\n" +
                                "10\n",
                        factory,
                        true,
                        false
                );
            }
        });
    }

    @Test
    public void testBindVariableInvalid() throws Exception {
        assertMemoryLeak(() -> {
            ddl("CREATE TABLE 'alcatel_traffic_tmp' (deviceName SYMBOL capacity 1000 index, time TIMESTAMP, slot SYMBOL, port SYMBOL, downStream DOUBLE, upStream DOUBLE) timestamp(time) partition by DAY");
            try {
                assertException("select * from alcatel_traffic_tmp where deviceName in ($n1)");
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

        assertQuery(
                "sym\n" +
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
                "select * from x2 where sym in (select distinct sym from x2 where sym in (select distinct sym from x2 where sym = 'cc')) and test_match()",
                "create table x2 as (select rnd_symbol('aa','bb','cc') sym from long_sequence(50))",
                null
        );

        // also good numbers, extra top calls are due to symbol column API check
        // tables without symbol columns will skip this check
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testCastAndAliasedColumnAfterWildcard() throws Exception {
        assertQuery(
                "a\tk\tklong\n" +
                        "80.43224099968394\t1970-01-01T00:00:00.000000Z\t0\n" +
                        "8.486964232560668\t1970-01-01T00:00:00.010000Z\t10000\n" +
                        "8.43832076262595\t1970-01-01T00:00:00.020000Z\t20000\n",
                "select *, cast(k as long) klong from x",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_double(0)*100 a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                true,
                true
        );
    }

    @Test
    public void testCastAndAliasedColumnFollowedByWildcard() throws Exception {
        assertQuery(
                "klong\ta\tk\n" +
                        "0\t80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                        "10000\t8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                        "20000\t8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                "select cast(k as long) klong, * from x",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_double(0)*100 a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                true,
                true
        );
    }

    @Test
    public void testCastAndAliasedColumnFollowedByWildcardInJoinQuery() throws Exception {
        assertQuery(
                "klong1\tklong2\ta\tk\ta1\tk1\n" +
                        "0\t0\t4689592037643856\t1970-01-01T00:00:00.000000Z\t4689592037643856\t1970-01-01T00:00:00.000000Z\n" +
                        "10000\t10000\t4729996258992366\t1970-01-01T00:00:00.010000Z\t4729996258992366\t1970-01-01T00:00:00.010000Z\n" +
                        "20000\t20000\t7746536061816329025\t1970-01-01T00:00:00.020000Z\t7746536061816329025\t1970-01-01T00:00:00.020000Z\n",
                "select cast(x1.k as long) klong1, cast(x2.k as long) klong2, * from x x1 join x x2 on x1.a = x2.a",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_long() a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                false,
                true
        );
    }

    @Test
    public void testCastAndAliasedColumnRepeatedTwiceFollowedByWildcard() throws Exception {
        assertQuery(
                "klong\tkdate\ta\tk\n" +
                        "0\t1970-01-01T00:00:00.000Z\t80.43224099968394\t1970-01-01T00:00:00.000000Z\n" +
                        "10000\t1970-01-01T00:00:00.010Z\t8.486964232560668\t1970-01-01T00:00:00.010000Z\n" +
                        "20000\t1970-01-01T00:00:00.020Z\t8.43832076262595\t1970-01-01T00:00:00.020000Z\n",
                "select cast(k as long) klong, cast(k as date) kdate, * from x",
                "create table x as " +
                        "(" +
                        "  select" +
                        "    rnd_double(0)*100 a," +
                        "    timestamp_sequence(0, 10000) k" +
                        "  from long_sequence(3)" +
                        ") timestamp(k)",
                "k",
                true,
                true
        );
    }

    @Test
    public void testCg() throws Exception {
        assertQuery(
                "title\tcurrent\told\tdifference\tdifference_percentage\tcolors\n" +
                        "Revenue\tNaN\tNaN\tNaN\tNaN\trag\n",
                "SELECT\n" +
                        "  'Revenue' as title,\n" +
                        "  current_orders as current,\n" +
                        "  old_orders as old,\n" +
                        "  current_orders - old_orders as difference,\n" +
                        "  ((current_orders - old_orders) / current_orders) * 100.0 difference_percentage,\n" +
                        "  'rag' as colors\n" +
                        "from\n" +
                        "(\n" +
                        "    SELECT\n" +
                        "      cast(max('orders_1'.revenue) as float) as current_orders,\n" +
                        "      cast(max('orders_2'.revenue) as float) as old_orders\n" +
                        "    from\n" +
                        "      (\n" +
                        "        SELECT\n" +
                        "          timestamp as time,\n" +
                        "          sum(total_revenue) as revenue\n" +
                        "        from\n" +
                        "          'mdc_data'\n" +
                        "        WHERE\n" +
                        "          timestamp > timestamp_floor('d', now()) SAMPLE BY 10s\n" +
                        "      ) as orders_1,\n" +
                        "      (\n" +
                        "        SELECT\n" +
                        "          timestamp as time,\n" +
                        "          sum(total_revenue) as revenue\n" +
                        "        from\n" +
                        "          'mdc_data'\n" +
                        "        WHERE\n" +
                        "          timestamp < dateadd('d', -1, now())\n" +
                        "          and timestamp > timestamp_floor('d', dateadd('d', -1, now())) SAMPLE BY 10s\n" +
                        "      ) as orders_2\n" +
                        "  );\n",
                "create table mdc_data as (select rnd_double() total_revenue, timestamp_sequence(dateadd('d', -1, now()),2) timestamp from long_sequence(10000)) timestamp(timestamp) partition by day",
                null,
                false,
                true
        );
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
                "create table tst as (select * from (select rnd_int() a, rnd_double() b, timestamp_sequence(0, 10000000000l) t from long_sequence(100)) timestamp(t)) timestamp(t) partition by DAY",
                "t",
                true,
                true
        );
    }

    @Test
    public void testCreateTableIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 10; i++) {
                ddl("create table if not exists y as (select rnd_int() a from long_sequence(21))");
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
            ddl("create table x (col string)");

            engine.clear();

            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public boolean getDefaultSymbolCacheFlag() {
                    return false;
                }
            };

            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                ddl(compiler, "create table y as (x), cast(col as symbol cache)", sqlExecutionContext);

                try (TableReader reader = engine.getReader("y")) {
                    Assert.assertTrue(reader.getSymbolMapReader(0).isCached());
                }
            }
        });
    }

    @Test
    public void testCreateTableSymbolColumnViaCastCachedSymbolCapacityHigh() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (col string)");

            try {
                assertException("create table y as (x), cast(col as symbol capacity 100000000)");
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
            ddl("create table x (col string)");
            ddl("create table y as (x), cast(col as symbol nocache)");

            try (TableReader reader = getReader("y")) {
                Assert.assertFalse(reader.getSymbolMapReader(0).isCached());
            }
        });
    }

    @Test
    public void testCursorForLatestByOnSubQueryWithRandomAccessSupport() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "81.0161274171258\tCC\t1970-01-21T20:00:00.000000Z\n" +
                        "37.62501709498378\tBB\t1970-01-22T23:46:40.000000Z\n",
                "(x where b in ('BB','CC')) where a > 0 latest on k partition by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol('AA','BB','CC') b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                true
        );
    }

    @Test
    public void testCursorForLatestByOnSubQueryWithoutRandomAccessSupport() throws Exception {
        assertQuery(
                "b\tk\n" +
                        "CC\t1970-01-21T20:00:00.000000Z\n" +
                        "BB\t1970-01-22T23:46:40.000000Z\n",
                "(select b, max(k) k from x where b in ('BB','CC') sample by 1T) latest on k partition by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol('AA','BB','CC') b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                null,
                false,
                true
        );
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

        assertQuery(
                expected,
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

        assertQuery(
                expected,
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

        assertQuery(
                expected,
                "select distinct pair from prices order by pair",
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
                false
        );
    }

    @Test
    public void testDistinctSymbolColumnWithFilter() throws Exception {
        final String expected = "pair\n" +
                "A\n" +
                "B\n";

        assertQuery(
                expected,
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
    public void testDoesntFailForOrderByPositionWithColumnAliasesAndInvalidAggregateFuncCall() throws Exception {
        // This query was leading to an NPE in the past.
        assertQuery("col_1\tcol_cnt\n" +
                        "\t11\n" +
                        "PEHN\t4\n" +
                        "HYRX\t2\n" +
                        "VTJW\t2\n" +
                        "RXGZ\t1\n",
                "select b col_1, count(a) col_cnt from x order by 2 desc, 1 asc",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by DAY", null, true, true
        );
    }

    @Test
    public void testDynamicTimestamp() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        final String expected = "a\tb\tk\n" +
                "21.583224269349387\tYSBE\t1970-01-07T22:40:00.000000Z\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testEqGeoHashWhenOtherIsStr1() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(4);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.439000Z\tbbb\tewef\n",
                            "select * from pos where hash = 'ewef'",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testEqGeoHashWhenOtherIsStr2() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(4);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.439000Z\tbbb\tewef\n",
                            "select * from pos where 'ewef' = hash",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testFailsForLatestByOnSubQueryWithNoTimestampSpecified() throws Exception {
        assertException(
                "with tab as (x where b in ('BB')) select * from tab latest by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_symbol('AA','BB','CC') b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ")",
                34,
                "latest by query does not provide dedicated TIMESTAMP column"
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

        assertQuery(
                expected,
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
                "k"
        );

        // these values are assured to be correct for the scenario
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
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
        assertQuery(
                expected,
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

        assertQuery(
                expected,
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
                        "48.52404686849972\tHYRX\t1971-01-01T00:00:00.000000Z\n",
                true
        );
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterNotEqualsWithIndexedBindVariableSingleIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testFilterWithSymbolBindVariableNotEquals("select * from x where b != $1", true);
    }

    @Test
    public void testFilterNotEqualsWithIndexedBindVariableSingleNonIndexedSymbolNotEquals() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testFilterWithSymbolBindVariableNotEquals("select * from x where b != $1", false);
    }

    @Test
    public void testFilterNotEqualsWithNamedBindVariableSingleIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testFilterWithSymbolBindVariableNotEquals("select * from x where b != :b", true);
    }

    @Test
    public void testFilterNotEqualsWithNamedBindVariableSingleNonIndexedSymbolNotEquals() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testFilterWithSymbolBindVariableNotEquals("select * from x where b != :b", false);
    }

    @Test
    public void testFilterOnConstantFalse() throws Exception {
        assertQuery(
                (CharSequence) null,
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
                false
        );
    }

    @Test
    public void testFilterOnIndexAndExpression() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery(
                "contactId\n" +
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
        assertQuery(
                "a\tb\tk\n" +
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
                "k"
        );
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterOnIntervalAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery(
                "a\tb\tk\n" +
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
                "k"
        );

        // also good numbers, extra top calls are due to symbol column API check
        // tables without symbol columns will skip this check
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterOnIntrinsicFalse() throws Exception {
        assertQuery(
                (CharSequence) null,
                "select * from x o where o.b in ('HYRX','PEHN', null) and a < a",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, timestamp_sequence(0, 1000000000) k from long_sequence(20)), index(b)",
                null,
                false
        );
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
        assertQuery(
                expected,
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
                        "3.993124821273464\t\n",
                true
        );
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

        assertQuery(
                expected,
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
                        "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n",
                true
        );
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterOnSubQueryIndexedAndFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        final String expected = "b\tk\ta\n" +
                "HYRX\t1970-01-07T22:40:00.000000Z\t97.71103146051203\n" +
                "HYRX\t1970-01-11T10:00:00.000000Z\t12.026122412833129\n";

        assertQuery(
                expected,
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
                        "HYRX\t1971-01-01T00:00:00.000000Z\t56.594291398612405\n",
                true
        );
    }

    @Test
    public void testFilterOnSubQueryIndexedDeferred() throws Exception {
        assertQuery(
                null,
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
                        "68.06873134626417\tABC\t1971-01-01T00:00:00.000000Z\n",
                true
        );
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

        assertQuery(
                expected,
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
                        "11.585982949541474\tABC\t1971-01-01T00:00:00.000000Z\n",
                true
        );

        // these value are also ok because ddl2 is present, there is another round of check for that
        // this ensures that "init" on filter is invoked
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterOnSubQueryIndexedFilteredEmpty() throws Exception {
        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\tk\n";

        assertQuery(
                expected,
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
                true
        );

        // these value are also ok because ddl2 is present, there is another round of check for that
        // this ensures that "init" on filter is invoked
        Assert.assertTrue(TestMatchFunctionFactory.isClosed());
    }

    @Test
    public void testFilterOnSubQueryIndexedStrColumn() throws Exception {
        assertQuery(
                null,
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
                        "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n",
                true
        );
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

        assertQuery(
                expected,
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
                        "56.594291398612405\tQZT\t1971-01-01T00:00:00.000000Z\n",
                true
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

        assertQuery(
                expected1,
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
                        "28.20020716674768\tABCD\t\n",
                true
        );
    }

    @Test
    public void testFilterOnValuesAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery(
                "a\tb\tk\n" +
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
                        "44.80468966861358\t\t\n",
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterOnValuesAndInterval() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
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
                "k"
        );
    }

    @Test
    public void testFilterOnValuesDeferred() throws Exception {
        assertQuery(
                null,
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
                        "28.20020716674768\tABCD\t\n",
                true
        );
    }

    @Test
    public void testFilterSingleKeyValue() throws Exception {
        final String expected = "a\tb\n" +
                "11.427984775756228\tHYRX\n" +
                "52.98405941762054\tHYRX\n" +
                "40.455469747939254\tHYRX\n" +
                "72.30015763133606\tHYRX\n";
        assertQuery(
                expected,
                "select * from x where b = 'HYRX'",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.88175403454873\tHYRX\n" +
                        "57.78947915182423\tHYRX\n",
                true
        );
    }

    @Test
    public void testFilterSingleKeyValueAndField() throws Exception {
        TestMatchFunctionFactory.clear();

        final String expected = "a\tb\n" +
                "52.98405941762054\tHYRX\n" +
                "72.30015763133606\tHYRX\n";
        assertQuery(
                expected,
                "select * from x where b = 'HYRX' and a > 41 and test_match()",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'HYRX'" +
                        " from long_sequence(2)",
                expected +
                        "75.88175403454873\tHYRX\n" +
                        "57.78947915182423\tHYRX\n",
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testFilterSingleNonExistingSymbol() throws Exception {
        assertQuery(
                null,
                "select * from x where b = 'ABC'",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.88175403454873\tABC\n" +
                        "57.78947915182423\tABC\n",
                true
        );
    }

    @Test
    public void testFilterSingleNonExistingSymbolAndFilter() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                null,
                "select * from x where b = 'ABC' and a > 30 and test_match()",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,0) b from long_sequence(20)), index(b)",
                null,
                "insert into x select" +
                        " rnd_double(0)*100," +
                        " 'ABC'" +
                        " from long_sequence(2)",
                "a\tb\n" +
                        "75.88175403454873\tABC\n" +
                        "57.78947915182423\tABC\n",
                true
        );
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
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

        assertQuery(
                expected,
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
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
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

        assertQuery(
                expected,
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
                        "56.594291398612405\tABC\t1971-01-01T00:00:00.000000Z\n",
                true
        );
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

        assertQuery(
                expected,
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
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
    }

    @Test
    public void testFilterTimestamps() throws Exception {
        // ts
        // 2022-03-22 10:00:00.0
        // 2022-03-23 10:00:00.0
        // 2022-03-24 10:00:00.0
        // 2022-03-25 10:00:00.0
        // 2022-03-26 10:00:00.0
        // 2022-03-27 10:00:00.0
        // 2022-03-28 10:00:00.0
        // 2022-03-29 10:00:00.0
        // 2022-03-30 10:00:00.0
        // 2022-03-31 10:00:00.0
        currentMicros = 1649186452792000L; // '2022-04-05T19:20:52.792Z'
        assertQuery("min\tmax\n" +
                "\t\n", "SELECT min(ts), max(ts)\n" +
                "FROM tab\n" +
                "WHERE ts >= '2022-03-23T08:00:00.000000Z' AND ts < '2022-03-25T10:00:00.000000Z' AND ts > dateadd('d', -10, systimestamp())", "CREATE TABLE tab AS (\n" +
                "    SELECT dateadd('d', CAST(-(10-x) AS INT) , '2022-03-31T10:00:00.000000Z') AS ts \n" +
                "    FROM long_sequence(10)\n" +
                ") TIMESTAMP(ts) PARTITION BY DAY", null, null, null, false, true, false);

        drop("drop table tab");

        assertQuery("min\tmax\n" +
                "\t\n", "SELECT min(ts), max(ts)\n" +
                " FROM tab\n" +
                " WHERE ts >= '2022-03-23T08:00:00.000000Z' AND ts < '2022-03-25T10:00:00.000000Z' AND ts > dateadd('d', -10, now())", "CREATE TABLE tab AS (\n" +
                "    SELECT dateadd('d', CAST(-(10-x) AS INT) , '2022-03-31T10:00:00.000000Z') AS ts \n" +
                "    FROM long_sequence(10)\n" +
                ") TIMESTAMP(ts) PARTITION BY DAY", null, null, null, false, true, false);
    }

    @Test
    public void testFilterWithIndexedBindVariableIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = $1 and a != 0", true);
    }

    @Test
    public void testFilterWithIndexedBindVariableNonIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = $1 and a != 0", false);
    }

    @Test
    public void testFilterWithIndexedBindVariableSingleIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = $1", true);
    }

    @Test
    public void testFilterWithIndexedBindVariableSingleNonIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = $1", false);
    }

    @Test
    public void testFilterWithNamedBindVariableIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = :b and a != 0", true);
    }

    @Test
    public void testFilterWithNamedBindVariableNonIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = :b and a != 0", false);
    }

    @Test
    public void testFilterWithNamedBindVariableSingleIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = :b", true);
    }

    @Test
    public void testFilterWithNamedBindVariableSingleNonIndexedSymbol() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testFilterWithSymbolBindVariable("select * from x where b = :b", false);
    }

    @Test
    public void testFilterWrongType() throws Exception {
        assertException(
                "select * from x where b - a",
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
    public void testGreaterNoOpFilter() throws Exception {
        assertQuery(
                "c0\n",
                "select t7.c0 from t7 where t7.c0 > t7.c0",
                "create table t7 as (select 42 as c0 from long_sequence(1))",
                null,
                false,
                false
        );
    }

    @Test
    public void testGreaterOrEqualsNoOpFilter() throws Exception {
        assertQuery(
                "c0\n" +
                        "42\n",
                "select t7.c0 from t7 where t7.c0 >= t7.c0",
                "create table t7 as (select 42 as c0 from long_sequence(1))",
                null,
                true,
                false
        );
    }

    @Test
    public void testGroupByConstantMatchingColumnName() throws Exception {
        assertQuery("nts\tmin\nnts\t\n", "select 'nts', min(nts) from tt where nts > '2020-01-01T00:00:00.000000Z'", "create table tt (dts timestamp, nts timestamp) timestamp(dts)", null, "insert into tt " +
                "select timestamp_sequence(1577836800000000L, 10L), timestamp_sequence(1577836800000000L, 10L) " +
                "from long_sequence(2L)", "nts\tmin\n" +
                "nts\t2020-01-01T00:00:00.000010Z\n", false, true, false);
    }

    @Test
    public void testGroupByDoesNotDependOnAliases() throws Exception {
        // Here we verify that the same data set is returned no matter what column aliases are in a GROUP BY query.
        assertMemoryLeak(() -> {
            final String expectedData = "a\t60\n" +
                    "b\t65\n" +
                    "c\t75\n";

            ddl("create table x as " +
                    "(" +
                    "select" +
                    " rnd_symbol('a','b','c') s," +
                    " timestamp_sequence(0, 1000000000) k" +
                    " from long_sequence(200)" +
                    ") timestamp(k) partition by DAY");

            String query = "select s, count() from x order by s";
            try (RecordCursorFactory factory = select(query)) {
                assertCursor("s\tcount\n" + expectedData, factory, true, true);
            }

            query = "select s as symbol, count() from x order by symbol";
            try (RecordCursorFactory factory = select(query)) {
                assertCursor("symbol\tcount\n" + expectedData, factory, true, true);
            }

            query = "select s as symbol, count() as cnt from x group by symbol order by symbol";
            try (RecordCursorFactory factory = select(query)) {
                assertCursor("symbol\tcnt\n" + expectedData, factory, true, true);
            }
        });
    }

    @Test
    public void testImplicitCastStrToDouble() throws Exception {
        assertQuery(
                "column\tprice\n" +
                        "80.43224099968394\t0.8043224099968393\n" +
                        "28.45577791213847\t0.2845577791213847\n" +
                        "93.4460485739401\t0.9344604857394011\n" +
                        "79.05675319675964\t0.7905675319675964\n" +
                        "88.99286912289664\t0.8899286912289663\n" +
                        "11.427984775756228\t0.11427984775756228\n" +
                        "42.17768841969397\t0.4217768841969397\n" +
                        "72.61136209823621\t0.7261136209823622\n" +
                        "66.93837147631712\t0.6693837147631712\n" +
                        "87.56771741121929\t0.8756771741121929\n",
                "select '100'*price, price from trades",
                "create table trades as " +
                        "(" +
                        "select" +
                        " rnd_double(42) price," +
                        " rnd_symbol('AA','BB','CC') symbol," +
                        " timestamp_sequence(0, 100000000000) ts" +
                        " from long_sequence(10)" +
                        ") timestamp(ts) partition by day",
                null,
                true,
                true
        );
    }

    @Test
    public void testInsertMissingQuery() throws Exception {
        assertException(
                "insert into x (a,b)",
                "create table x as (select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b from long_sequence(20)), index(b)",
                19,
                "'select' or 'values' expected"
        );
    }

    @Test
    public void testJoinOnExecutionOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table l as( select x from long_sequence(100) )");
            ddl("create table rr as( select x + 50 as y from long_sequence(100) )");

            TestUtils.assertSql(
                    engine,
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
            ddl("create table l as( select x from long_sequence(100) )");
            ddl("create table rr as( select x + 50 as y from long_sequence(100) )");

            TestUtils.assertSql(
                    engine,
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
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllBool() throws Exception {
        assertQuery("a\tb\tk\n" +
                "97.55263540567968\ttrue\t1970-01-20T16:13:20.000000Z\n" +
                "37.62501709498378\tfalse\t1970-01-22T23:46:40.000000Z\n", "select * from x latest on k partition by b", "create table x as " +
                "(" +
                "select * from" +
                "(" +
                " select" +
                " rnd_double(0)*100 a," +
                " rnd_boolean() b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from long_sequence(20) " +
                ")" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " false," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tk\n" +
                "97.55263540567968\ttrue\t1970-01-20T16:13:20.000000Z\n" +
                "24.59345277606021\tfalse\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllConstantFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected, "select * from x where 6 < 10 latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected, "select * from x where a > 40 latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexed() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected, "select * from x latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexedConstantFilter() throws Exception {
        final String expected = "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n";
        assertQuery(expected, "select * from x where 5 > 2 latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "49.00510449885239\tPEHN\t1970-01-18T08:40:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tVTJW\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexedExternalFilter() throws Exception {
        final String expected = "a\tk\tb\n" +
                "78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n";
        assertQuery(expected, "select * from (select a,k,b from x latest on k partition by b) where a > 40", "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100000000000) k," +
                " rnd_double(0)*100 a1," +
                " rnd_double(0)*100 a2," +
                " rnd_double(0)*100 a3," +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " to_timestamp('2019', 'yyyy') t," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " 46.578761277152225," +
                " 'VTJW'" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tk\tb\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n" +
                "46.578761277152225\t2019-01-01T00:00:00.000000Z\tVTJW\n", true, false, false);
    }

    @Test
    public void testLatestByAllIndexedFilter() throws Exception {
        final String expected = "a\tk\tb\n" +
                "78.83065830055033\t1970-01-04T11:20:00.000000Z\tVTJW\n" +
                "95.40069089049732\t1970-01-11T10:00:00.000000Z\tHYRX\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n";
        assertQuery(expected, "select a,k,b from x where a > 40 latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100000000000) k," +
                " rnd_double(0)*100 a1," +
                " rnd_double(0)*100 a2," +
                " rnd_double(0)*100 a3," +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " to_timestamp('2019', 'yyyy') t," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " 46.578761277152225," +
                " 'VTJW'" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tk\tb\n" +
                "95.40069089049732\t1970-01-11T10:00:00.000000Z\tHYRX\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n" +
                "46.578761277152225\t2019-01-01T00:00:00.000000Z\tVTJW\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexedFilterBySymbol() throws Exception {
        final String expected = "a\tb\tc\tk\n" +
                "67.52509547112409\tCPSW\tSXUX\t1970-01-21T20:00:00.000000Z\n";
        assertQuery(expected, "select * from x where c = 'SXUX' latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " rnd_symbol(5,4,4,1) c," +
                " timestamp_sequence(0, 100000000000) k" +
                " from long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " 'SXUX'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tb\tc\tk\n" +
                "67.52509547112409\tCPSW\tSXUX\t1970-01-21T20:00:00.000000Z\n" +
                "94.41658975532606\tVTJW\tSXUX\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexedFilterColumnDereference() throws Exception {
        final String expected = "b\tk\n" +
                "RXGZ\t1970-01-12T13:46:40.000000Z\n";
        assertQuery(
                expected,
                "select b,k from x where b = 'RXGZ' and k < '1970-01-22' latest on k partition by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " timestamp_sequence(0, 100000000000) k," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 a1," +
                        " rnd_double(0)*100 a2" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                "k",
                true,
                false
        );
    }

    @Test
    public void testLatestByAllIndexedFilteredMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table trips(id int, vendor symbol index, ts timestamp) timestamp(ts) partition by DAY");
                    // insert three partitions
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "trips where id > 0 latest on ts partition by vendor order by ts",
                            sink,
                            "id\tvendor\tts\n" +
                                    "1878619626\tKK\t1970-01-01T00:01:39.200000Z\n" +
                                    "666152026\tZZ\t1970-01-01T00:01:39.500000Z\n" +
                                    "1093218218\tTT\t1970-01-02T00:01:37.700000Z\n" +
                                    "371958898\tDD\t1970-01-02T00:01:39.900000Z\n" +
                                    "1699760758\tPP\t1970-01-03T00:01:39.100000Z\n" +
                                    "415357759\tCC\t1970-01-03T00:01:39.200000Z\n" +
                                    "1176091947\tQQ\t1970-01-03T00:01:39.400000Z\n"
                    );
                }
        );
    }

    @Test
    public void testLatestByAllIndexedGeoHash1c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(1);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tv\n",
                            "select * from pos where hash within(#f, #z, #v) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHash2c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf9\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz3\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tve\n",
                            "select * from pos where hash within(#f9, #z3, #ve) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHash2cFn() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf9\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz3\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tve\n",
                            "select * from pos where hash within(make_geohash(-62, 53.4, 10), #z3, #ve) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHash4c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(4);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf91t\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz31w\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tvepe\n",
                            "select * from pos where hash within(#f91, #z31w, #vepe) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHash8c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(8);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf91t48s7\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz31wzd5w\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tvepe7h62\n",
                            "select * from pos where hash within(#f91, #z31w, #vepe7h) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashExcludeLongPrefix() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where hash within(#f9, #z3, #vepe7h) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "prefix precision mismatch");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashFnNonConst() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl(
                            "create table x as (" +
                                    "select" +
                                    " rnd_symbol(113, 4, 4, 2) s," +
                                    " timestamp_sequence(500000000000L,100000000L) ts," +
                                    " (rnd_double()*360.0 - 180.0) lon, " +
                                    " (rnd_double()*180.0 - 90.0) lat, " +
                                    " rnd_geohash(40) geo8" +
                                    " from long_sequence(1000)" +
                                    "), index(s) timestamp (ts) partition by DAY"
                    );
                    try {
                        assertQuery(
                                "time\tuuid\thash\n",
                                "select * from x where geo8 within(make_geohash(lon, lat, 40), #z3, #vegg) latest on ts partition by s",
                                "ts",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "GeoHash const function expected");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashLiteralExpected() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "time\tuuid\thash\n" +
                                        "2021-05-10T23:59:59.150000Z\tXXX\tf9\n" +
                                        "2021-05-11T00:00:00.083000Z\tYYY\tz3\n" +
                                        "2021-05-12T00:00:00.186000Z\tZZZ\tve\n",
                                "select * from pos where hash within('z3', #z3, #ve) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "GeoHash literal expected");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashOutOfRangeFn() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "time\tuuid\thash\n" +
                                        "2021-05-10T23:59:59.150000Z\tXXX\tf9\n" +
                                        "2021-05-11T00:00:00.083000Z\tYYY\tz3\n" +
                                        "2021-05-12T00:00:00.186000Z\tZZZ\tve\n",
                                "select * from pos where hash within(make_geohash(-620.0, 53.4, 10), #z3, #ve) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "longitude must be in [-180.0..180.0] range");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRnd1c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashTable();
                    assertQuery(
                            "geo1\tts\n" +
                                    "x\t1970-01-17T21:43:20.000000Z\n" +
                                    "x\t1970-01-18T02:38:20.000000Z\n" +
                                    "y\t1970-01-18T03:03:20.000000Z\n" +
                                    "x\t1970-01-18T03:06:40.000000Z\n" +
                                    "y\t1970-01-18T05:53:20.000000Z\n" +
                                    "y\t1970-01-18T07:41:40.000000Z\n" +
                                    "y\t1970-01-18T08:18:20.000000Z\n" +
                                    "z\t1970-01-18T08:35:00.000000Z\n",
                            "select geo1, ts from x where geo1 within(#x, #y, #z) latest on ts partition by s",
                            "ts",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRnd2c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashTable();
                    assertQuery(
                            "geo2\tts\n" +
                                    "z7g\t1970-01-17T18:45:00.000000Z\n" +
                                    "xzu\t1970-01-17T21:06:40.000000Z\n" +
                                    "yyg\t1970-01-18T01:36:40.000000Z\n" +
                                    "yds\t1970-01-18T01:56:40.000000Z\n" +
                                    "yjx\t1970-01-18T05:03:20.000000Z\n" +
                                    "ymx\t1970-01-18T05:53:20.000000Z\n" +
                                    "y8x\t1970-01-18T06:45:00.000000Z\n" +
                                    "y25\t1970-01-18T06:48:20.000000Z\n" +
                                    "yvh\t1970-01-18T06:55:00.000000Z\n" +
                                    "y1n\t1970-01-18T07:28:20.000000Z\n" +
                                    "zs4\t1970-01-18T08:03:20.000000Z\n",
                            "select geo2, ts from x where geo2 within(#x, #y, #z) latest on ts partition by s",
                            "ts",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRnd4c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashTable();
                    assertQuery(
                            "geo4\tts\n" +
                                    "zd4gu\t1970-01-17T20:06:40.000000Z\n" +
                                    "xwnjg\t1970-01-18T01:36:40.000000Z\n" +
                                    "yv6gp\t1970-01-18T02:48:20.000000Z\n" +
                                    "z4wbx\t1970-01-18T05:51:40.000000Z\n" +
                                    "zejr0\t1970-01-18T06:43:20.000000Z\n" +
                                    "ybsge\t1970-01-18T06:45:00.000000Z\n" +
                                    "zdhfv\t1970-01-18T06:53:20.000000Z\n" +
                                    "z4t7w\t1970-01-18T07:45:00.000000Z\n" +
                                    "xxusm\t1970-01-18T07:55:00.000000Z\n" +
                                    "x1dse\t1970-01-18T08:18:20.000000Z\n" +
                                    "zmt6j\t1970-01-18T08:38:20.000000Z\n",
                            "select geo4, ts from x where geo4 within(#x, #y, #z) latest on ts partition by s",
                            "ts",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRnd6Bits() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashBitsTable();
                    assertQuery(
                            "bits7\tts\n" +
                                    "1111111\t1970-01-16T21:43:20.000000Z\n" +
                                    "1111111\t1970-01-18T00:50:00.000000Z\n" +
                                    "1111111\t1970-01-18T00:55:00.000000Z\n" +
                                    "1111110\t1970-01-18T05:11:40.000000Z\n" +
                                    "1111110\t1970-01-18T07:10:00.000000Z\n" +
                                    "1111110\t1970-01-18T08:20:00.000000Z\n" +
                                    "1111111\t1970-01-18T08:28:20.000000Z\n",
                            "select bits7, ts from x where bits7 within(##111111) latest on ts partition by s",
                            "ts",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRnd8c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashTable();
                    assertQuery(
                            "geo4\tts\n" +
                                    "yv6gp\t1970-01-18T02:48:20.000000Z\n" +
                                    "z4wbx\t1970-01-18T05:51:40.000000Z\n" +
                                    "ybsge\t1970-01-18T06:45:00.000000Z\n" +
                                    "z4t7w\t1970-01-18T07:45:00.000000Z\n" +
                                    "xxusm\t1970-01-18T07:55:00.000000Z\n",
                            "select geo4, ts from x where geo4 within(#xx, #y, #z4) latest on ts partition by s",
                            "ts",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRndLongBitsMask() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashBitsTable();
                    assertQuery(
                            "i\ts\tts\tbits3\tbits7\tbits9\n" +
                                    "9384\tYFFD\t1970-01-17T15:31:40.000000Z\t101\t1110000\t101111011\n" +
                                    "9397\tMXUK\t1970-01-17T15:53:20.000000Z\t100\t1110001\t110001111\n",
                            "select * from x where bits7 within(#wt/5) latest on ts partition by s",//(##11100)",
                            "ts",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashRndLongBitsPrefix() throws Exception {
        assertMemoryLeak(
                () -> {
                    createRndGeoHashBitsTable();
                    try {
                        assertQuery(
                                "",
                                "select * from x where bits3 within(##111111) latest on ts partition by s",
                                "ts",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "prefix precision mismatch");
                    }

                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashStrCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-10T23:59:59.150000Z\tXXX\tf9\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz3\n" +
                                    "2021-05-12T00:00:00.186000Z\tZZZ\tve\n",
                            "select * from pos where hash within(cast('f9' as geohash(2c)), #z3, #ve) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashTimeRange1c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(1);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz\n" +
                                    "2021-05-11T00:00:00.111000Z\tddd\tb\n",
                            "select * from pos where time in '2021-05-11' and hash within (#z, #b) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashTimeRange2c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz3\n" +
                                    "2021-05-11T00:00:00.111000Z\tddd\tbc\n",
                            "select * from pos where time in '2021-05-11' and hash within (#z, #b) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashTimeRange4c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(4);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz31w\n" +
                                    "2021-05-11T00:00:00.111000Z\tddd\tbcnk\n",
                            "select * from pos where time in '2021-05-11' and hash within (#z, #b) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashTimeRange8c() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(8);
                    assertQuery(
                            "time\tuuid\thash\n" +
                                    "2021-05-11T00:00:00.083000Z\tYYY\tz31wzd5w\n",
                            "select * from pos where time in '2021-05-11' and hash within (#z31, #bbx) latest on time partition by uuid",
                            "time",
                            true,
                            true,
                            true
                    );
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashWithinColumnNotLiteral() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where 'hash' within(#f9) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "unexpected token [");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashWithinColumnWrongType() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where uuid within(#f9) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "GeoHash column type expected");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashWithinEmpty() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where hash within() latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "too few arguments for 'within'");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashWithinNullArg() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where hash within(#f9, #z3, null) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "GeoHash value expected");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashWithinOr() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where hash within(#f9) or hash within(#z3) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "Multiple 'within' expressions not supported");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedGeoHashWithinWrongCast() throws Exception {
        assertMemoryLeak(
                () -> {
                    createGeoHashTable(2);
                    try {
                        assertQuery(
                                "",
                                "select * from pos where hash within(cast('f91t' as geohash(4c)), #z3, null) latest on time partition by uuid",
                                "time",
                                true,
                                true,
                                true
                        );
                    } catch (SqlException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "prefix precision mismatch");
                    }
                });
    }

    @Test
    public void testLatestByAllIndexedListMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table trips(id int, vendor symbol index, ts timestamp) timestamp(ts) partition by DAY");
                    // insert three partitions
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "trips where vendor in ('KK', 'ZZ', 'TT', 'DD', 'PP', 'QQ', 'CC') latest on ts partition by vendor order by ts",
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
        assertQuery(expected, "select a,k,b from x latest on k partition by b", "create table x as " +
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
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " to_timestamp('2019', 'yyyy') t," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " 'VTJW'" +
                " from long_sequence(1)" +
                ") timestamp (t)", "a\tk\tb\n" +
                "2.6836863013701473\t1970-01-13T17:33:20.000000Z\tHYRX\n" +
                "9.76683471072458\t1970-01-14T21:20:00.000000Z\tPEHN\n" +
                "51.85631921367574\t1970-01-19T12:26:40.000000Z\tCPSW\n" +
                "50.25890936351257\t1970-01-20T16:13:20.000000Z\tRXGZ\n" +
                "72.604681060764\t1970-01-22T23:46:40.000000Z\t\n" +
                "6.578761277152223\t2019-01-01T00:00:00.000000Z\tVTJW\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexedMixedColumns() throws Exception {
        final String expected = "k\ta\n" +
                "1970-01-03T07:33:20.000000Z\t23.90529010846525\n" +
                "1970-01-11T10:00:00.000000Z\t12.026122412833129\n" +
                "1970-01-12T13:46:40.000000Z\t48.820511018586934\n" +
                "1970-01-18T08:40:00.000000Z\t49.00510449885239\n" +
                "1970-01-22T23:46:40.000000Z\t40.455469747939254\n";
        assertQuery(expected, "select k,a from x latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'VTJW'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", "k\ta\n" +
                "1970-01-03T07:33:20.000000Z\t23.90529010846525\n" +
                "1970-01-11T10:00:00.000000Z\t12.026122412833129\n" +
                "1970-01-18T08:40:00.000000Z\t49.00510449885239\n" +
                "1970-01-22T23:46:40.000000Z\t40.455469747939254\n" +
                "2019-01-01T00:00:00.000000Z\t56.594291398612405\n", true, true, false);
    }

    @Test
    public void testLatestByAllIndexedMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table trips(id int, vendor symbol index, ts timestamp) timestamp(ts) partition by DAY");
                    // insert three partitions
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "trips latest on ts partition by vendor order by ts",
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
                "\t1970-01-22T23:46:40.000000Z\t72.604681060764\n", "select b,k,a from x latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " timestamp_sequence(0, 100000000000) k," +
                " rnd_double(0)*100 a1," +
                " rnd_double(0)*100 a2," +
                " rnd_double(0)*100 a3," +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b" +
                " from long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " to_timestamp('2019', 'yyyy') t," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " rnd_double(0)*100," +
                " 'VTJW'" +
                " from long_sequence(1)" +
                ") timestamp (t)", "b\tk\ta\n" +
                "HYRX\t1970-01-13T17:33:20.000000Z\t2.6836863013701473\n" +
                "PEHN\t1970-01-14T21:20:00.000000Z\t9.76683471072458\n" +
                "CPSW\t1970-01-19T12:26:40.000000Z\t51.85631921367574\n" +
                "RXGZ\t1970-01-20T16:13:20.000000Z\t50.25890936351257\n" +
                "\t1970-01-22T23:46:40.000000Z\t72.604681060764\n" +
                "VTJW\t2019-01-01T00:00:00.000000Z\t6.578761277152223\n", true, true, false);
    }

    @Test
    public void testLatestByAllMultiplePartitions() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create table trips(id int, vendor symbol, ts timestamp) timestamp(ts) partition by DAY");
                    // insert three partitions
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('KK','ZZ', 'TT'), " +
                                    "timestamp_sequence(0, 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    // cast('1970-01-02' as timestamp) produces incorrect timestamp
                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('DD','QQ', 'TT'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-02', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    insert(
                            "insert into trips select " +
                                    "rnd_int(), " +
                                    "rnd_symbol('PP','QQ', 'CC'), " +
                                    "timestamp_sequence(to_timestamp('1970-01-03', 'yyyy-MM-dd'), 100000L) " +
                                    "from long_sequence(1000)"
                    );

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "trips latest on ts partition by vendor order by ts",
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
        assertQuery(expected, "select * from x where a > 40 latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                " select" +
                " rnd_double(0)*100," +
                " 'CCKS'," +
                " to_timestamp('2019', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp (t)", expected +
                "56.594291398612405\tCCKS\t2019-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByAllValueIndexedColumn() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table balances(\n" +
                    "cust_id SYMBOL index,\n" +
                    "balance_ccy SYMBOL,\n" +
                    "balance DOUBLE,\n" +
                    "timestamp TIMESTAMP\n" +
                    ")\n" +
                    "timestamp(timestamp)");

            insert("insert into balances values ('c1', 'USD', 1500, '2021-09-14T17:35:01.000000Z')");
            insert("insert into balances values ('c1', 'USD', 900.75, '2021-09-14T17:35:02.000000Z')");
            insert("insert into balances values ('c1', 'EUR', 880.2, '2021-09-14T17:35:03.000000Z')");
            insert("insert into balances values ('c1', 'EUR', 782, '2021-09-14T17:35:04.000000Z')");
            insert("insert into balances values ('c2', 'USD', 900, '2021-09-14T17:35:05.000000Z')");
            insert("insert into balances values ('c2', 'USD', 190.75, '2021-09-14T17:35:06.000000Z')");
            insert("insert into balances values ('c2', 'EUR', 890.2, '2021-09-14T17:35:07.000000Z')");
            insert("insert into balances values ('c2', 'EUR', 1000, '2021-09-14T17:35:08.000000Z')");

            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "SELECT * FROM balances \n" +
                            "WHERE cust_id = 'c1' and balance_ccy='EUR' \n" +
                            "LATEST ON timestamp PARTITION BY cust_id, balance_ccy",
                    sink,
                    "cust_id\tbalance_ccy\tbalance\ttimestamp\n" +
                            "c1\tEUR\t782.0\t2021-09-14T17:35:04.000000Z\n"
            );
        });
    }

    @Test
    public void testLatestByDeprecated() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "28.45577791213847\tHNR\t1970-01-02T03:46:40.000000Z\n" +
                        "88.99286912289664\tABC\t1970-01-05T15:06:40.000000Z\n",
                "select * from x latest by b",
                "create table x as " +
                        "(" +
                        "select " +
                        " rnd_double(0)*100 a," +
                        " rnd_str('ABC','HNR') b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(5)" +
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
                        "88.99286912289664\tABC\t1970-01-05T15:06:40.000000Z\n" +
                        "11.427984775756228\tHNR\t1971-01-01T00:00:00.000000Z\n",
                true,
                false,
                true
        );
    }

    @Test
    public void testLatestByDeprecatedFiltered() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
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
                false,
                true
        );
    }

    @Test
    public void testLatestByFailsOnNonDesignatedTimestamp() throws Exception {
        assertException(
                "tab latest on ts partition by id",
                "create table tab(" +
                        "    id symbol, " +
                        "    name symbol, " +
                        "    value double, " +
                        "    ts timestamp" +
                        ")",
                14,
                "latest by over a table requires designated TIMESTAMP"
        );
    }

    @Test
    public void testLatestByFilteredBySymbolInAllIndexed() throws Exception {
        testLatestByFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol index,\n" +
                "  metric symbol index,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestByFilteredBySymbolInNoIndexes() throws Exception {
        testLatestByFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol,\n" +
                "  metric symbol,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestByFilteredSymbolInPartiallyIndexed1() throws Exception {
        testLatestByFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol index,\n" +
                "  metric symbol,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestByFilteredSymbolInPartiallyIndexed2() throws Exception {
        testLatestByFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol,\n" +
                "  metric symbol index,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestByIOFailure() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public int openRO(LPSZ name) {
                    if (Utf8s.endsWithAscii(name, "b.d")) {
                        return -1;
                    }
                    return super.openRO(name);
                }
            };
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                try {
                    compiler.compile(
                            "create table x as " +
                                    "(" +
                                    "select rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, timestamp_sequence(0, 100000000000) k from" +
                                    " long_sequence(200)" +
                                    ") timestamp(k) partition by DAY",
                            sqlExecutionContext
                    );

                    refreshTablesInBaseEngine();
                    try (RecordCursorFactory factory = compiler.compile("select * from x where b = 'PEHN' and a < 22 latest on k partition by b", sqlExecutionContext).getRecordCursorFactory()) {
                        try {
                            assertCursor(
                                    "a\tb\tk\n" +
                                            "5.942010834028011\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                                    factory,
                                    true,
                                    false,
                                    false,
                                    // we need to pass the engine here, so the global test context won't do
                                    new SqlExecutionContextStub(engine)
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
    public void testLatestByIndexedKeyValueWithFilterAndIndexedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = $1 and a != 0 latest on k partition by b", true);
    }

    @Test
    public void testLatestByIndexedKeyValueWithFilterAndNamedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = :b latest on k partition by b", true);
    }

    @Test
    public void testLatestByIndexedKeyValueWithIndexedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = $1 latest on k partition by b", true);
    }

    @Test
    public void testLatestByIndexedKeyValueWithNamedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = :b latest on k partition by b", true);
    }

    @Test
    public void testLatestByIndexedKeyValuesWithIndexedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        bindVariableService.setStr(1, "VTJW");
        testLatestByKeyValuesWithBindVariable("select * from x where b in ($1,$2) latest on k partition by b", true);
    }

    @Test
    public void testLatestByIndexedKeyValuesWithNamedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b1", "HYRX");
        bindVariableService.setStr("b2", "VTJW");
        testLatestByKeyValuesWithBindVariable("select * from x where b in (:b1,:b2) latest on k partition by b", true);
    }

    @Test
    public void testLatestByKeyValue() throws Exception {
        // no index
        assertQuery(
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x where b = 'RXGZ' latest on k partition by b",
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
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
    }

    @Test
    public void testLatestByKeyValueColumnDereference() throws Exception {
        // no index
        assertQuery(
                "k\ta\tb\n" +
                        "1970-01-03T07:33:20.000000Z\t23.90529010846525\tRXGZ\n",
                "select k,a,b from x where b = 'RXGZ' latest on k partition by b",
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
                "k\ta\tb\n" +
                        "1971-01-01T00:00:00.000000Z\t56.594291398612405\tRXGZ\n",
                true
        );
    }

    @Test
    public void testLatestByKeyValueFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                "a\tb\tk\n" +
                        "5.942010834028011\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                "select * from x where b = 'PEHN' and a < 22 and test_match() latest on k partition by b",
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
                        "11.3\tPEHN\t1971-01-01T00:00:00.000000Z\n",
                true
        );

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByKeyValueFilteredEmpty() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                "a\tb\tk\n",
                "select * from x where b = 'PEHN' and a < 22 and 1 = 2 and test_match() latest on k partition by b",
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
                true
        );

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.isClosed());
    }

    @Test
    public void testLatestByKeyValueIndexed() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n",
                "select * from x where b = 'RXGZ' latest on k partition by b",
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
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
    }

    @Test
    public void testLatestByKeyValueIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                "a\tb\tk\n" +
                        "5.942010834028011\tPEHN\t1970-08-03T02:53:20.000000Z\n",
                "select * from x where b = 'PEHN' and a < 22 and test_match() latest on k partition by b",
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
                        "11.3\tPEHN\t1971-01-01T00:00:00.000000Z\n",
                true
        );

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByKeyValueInterval() throws Exception {
        assertQuery(
                "a\tb\tk\n" +
                        "84.45258177211063\tPEHN\t1970-01-16T01:06:40.000000Z\n",
                "select * from x where b = 'PEHN' and k IN '1970-01-06T18:53:20;11d' latest on k partition by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                "k"
        );
    }

    @Test
    public void testLatestByKeyValues() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n", "select * from x where b in ('RXGZ','HYRX') latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByKeyValuesFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in ('RXGZ','HYRX', null) and a > 12 and a < 50 and test_match() latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(5)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "12.105630273556178\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByKeyValuesIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n", "select * from x where b in ('RXGZ','HYRX') latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByKeyValuesIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n", "select * from x where b in ('RXGZ','HYRX') and a > 20 and test_match() latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByMissingKeyValue() throws Exception {
        assertQuery(
                null,
                "select * from x where b in ('XYZ') latest on k partition by b",
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
                        "72.30015763133606\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
    }

    @Test
    public void testLatestByMissingKeyValueFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                null,
                "select * from x where b in ('XYZ') and a < 60 and test_match() latest on k partition by b",
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
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );

        // this is good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByMissingKeyValueIndexed() throws Exception {
        assertQuery(
                null,
                "select * from x where b in ('XYZ') latest on k partition by b",
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
                        "81.64182592467493\tXYZ\t1971-01-05T15:06:40.000000Z\n",
                true
        );
    }

    @Test
    public void testLatestByMissingKeyValueIndexedColumnDereference() throws Exception {
        assertQuery(
                null,
                "select b,k,a from x where b in ('XYZ') latest on k partition by b",
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
                "b\tk\ta\n" +
                        "XYZ\t1971-01-05T15:06:40.000000Z\t81.64182592467493\n",
                true
        );
    }

    @Test
    public void testLatestByMissingKeyValueIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                null,
                "select * from x where b in ('XYZ') and a < 60 and test_match() latest on k partition by b",
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
                        "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByMissingKeyValues() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n", "select * from x where b in ('XYZ','HYRX') latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'XYZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByMissingKeyValuesFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n", "select * from x where b in ('XYZ', 'HYRX') and a > 30 and test_match() latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'XYZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByMissingKeyValuesIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n", "select * from x where b in ('XYZ', 'HYRX') latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'XYZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "56.594291398612405\tXYZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByMissingKeyValuesIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                "54.55175324785665\tHYRX\t1970-02-02T07:00:00.000000Z\n", "select * from x where b in ('XYZ', 'HYRX') and a > 30 and test_match() latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 10000000000) k" +
                " from" +
                " long_sequence(300)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " 88.1," +
                " 'XYZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "54.55175324785665\tHYRX\t1970-02-02T07:00:00.000000Z\n" +
                "88.1\tXYZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestByMultiColumnPlusFilter0() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol, " +
                "    name symbol, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts)");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter1() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol, " +
                "    name symbol, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts) partition by DAY");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter2() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol index, " +
                "    name symbol, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts) partition by DAY");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter3() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol, " +
                "    name symbol index, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts) partition by DAY");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter4() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol index, " +
                "    name symbol index, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts) partition by DAY");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter5() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol index, " +
                "    name symbol index, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts)");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter6() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol, " +
                "    name symbol index, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts)");
    }

    @Test
    public void testLatestByMultiColumnPlusFilter7() throws Exception {
        testLatestByMultiColumnPlusFilter("create table tab(" +
                "    id symbol index, " +
                "    name symbol, " +
                "    value double, " +
                "    ts timestamp" +
                ") timestamp(ts)");
    }

    @Test
    public void testLatestByMultipleColumns() throws Exception {
        assertQuery("cust_id\tbalance_ccy\tbalance\tstatus\ttimestamp\n", "select * from balances latest on timestamp partition by cust_id, balance_ccy", "create table balances (\n" +
                "\tcust_id int, \n" +
                "\tbalance_ccy symbol, \n" +
                "\tbalance double, \n" +
                "\tstatus byte, \n" +
                "\ttimestamp timestamp\n" +
                ") timestamp(timestamp)", "timestamp", "insert into balances select * from (" +
                " select" +
                " abs(rnd_int()) % 4," +
                " rnd_str('USD', 'GBP', 'EUR')," +
                " rnd_double()," +
                " rnd_byte(0,1)," +
                " cast(0 as timestamp) timestamp" +
                " from long_sequence(150)" +
                ") timestamp (timestamp)", "cust_id\tbalance_ccy\tbalance\tstatus\ttimestamp\n" +
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
                "3\tGBP\t0.31861843394057765\t1\t1970-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestByNonExistingColumn() throws Exception {
        assertException(
                "select * from x latest on k partition by y",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                41,
                "Invalid column"
        );
    }

    @Test
    public void testLatestByNonIndexedKeyValueWithFilterAndIndexedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = $1 and a != 0 latest on k partition by b", false);
    }

    @Test
    public void testLatestByNonIndexedKeyValueWithFilterAndNamedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = :b and a != 0 latest on k partition by b", false);
    }

    @Test
    public void testLatestByNonIndexedKeyValueWithIndexedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = $1 latest on k partition by b", false);
    }

    @Test
    public void testLatestByNonIndexedKeyValueWithNamedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b", "HYRX");
        testLatestByKeyValueWithBindVariable("select * from x where b = :b latest on k partition by b", false);
    }

    @Test
    public void testLatestByNonIndexedKeyValuesWithIndexedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr(0, "HYRX");
        bindVariableService.setStr(1, "VTJW");
        testLatestByKeyValuesWithBindVariable("select * from x where b in ($1,$2) latest on k partition by b", false);
    }

    @Test
    public void testLatestByNonIndexedKeyValuesWithNamedBindVariable() throws Exception {
        bindVariableService.clear();
        bindVariableService.setStr("b1", "HYRX");
        bindVariableService.setStr("b2", "VTJW");
        testLatestByKeyValuesWithBindVariable("select * from x where b in (:b1,:b2) latest on k partition by b", false);
    }

    @Test
    public void testLatestByOnSubQueryWithRandomAccessSupport() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab(" +
                    "    id symbol, " +
                    "    name symbol, " +
                    "    value long, " +
                    "    ts timestamp, " +
                    "    other_ts timestamp" +
                    ") timestamp(ts) partition by day");

            insert("insert into tab values ('d1', 'c1', 111, 1, 3)");
            insert("insert into tab values ('d1', 'c1', 112, 2, 2)");
            insert("insert into tab values ('d1', 'c1', 113, 3, 1)");
            insert("insert into tab values ('d1', 'c2', 121, 2, 1)");
            insert("insert into tab values ('d1', 'c2', 122, 3, 2)");
            insert("insert into tab values ('d1', 'c2', 123, 4, 3)");
            insert("insert into tab values ('d2', 'c1', 211, 3, 3)");
            insert("insert into tab values ('d2', 'c1', 212, 4, 3)");
            insert("insert into tab values ('d2', 'c2', 221, 5, 4)");
            insert("insert into tab values ('d2', 'c2', 222, 6, 5)");

            // latest by designated timestamp, no order by, select all columns
            assertSql(
                    "id\tname\tvalue\tts\tother_ts\n" +
                            "d1\tc1\t113\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\n" +
                            "d2\tc1\t212\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000003Z\n", "(tab where name in ('c1')) latest on ts partition by id"
            );

            // latest by designated timestamp, ordered by another timestamp, select all columns
            assertSql(
                    "id\tname\tvalue\tts\tother_ts\n" +
                            "d1\tc1\t113\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\n" +
                            "d2\tc1\t212\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000003Z\n", "(tab where name in ('c1') order by other_ts) latest on ts partition by id"
            );

            // latest by designated timestamp, select subset of columns
            assertSql(
                    "value\tts\n" +
                            "113\t1970-01-01T00:00:00.000003Z\n" +
                            "212\t1970-01-01T00:00:00.000004Z\n", "select value, ts from (tab where name in ('c1')) latest on ts partition by id"
            );

            // latest by designated timestamp, partition by multiple columns
            assertSql(
                    "id\tname\tvalue\tts\tother_ts\n" +
                            "d1\tc1\t113\t1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000001Z\n" +
                            "d1\tc2\t123\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000003Z\n" +
                            "d2\tc1\t212\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000003Z\n" +
                            "d2\tc2\t222\t1970-01-01T00:00:00.000006Z\t1970-01-01T00:00:00.000005Z\n", "(tab where name in ('c1','c2')) latest on ts partition by id, name"
            );

            // latest by non-designated timestamp, ordered
            assertSql(
                    "id\tname\tvalue\tts\tother_ts\n" +
                            "d1\tc1\t111\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000003Z\n" +
                            "d2\tc1\t212\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000003Z\n",
                    "(tab where name in ('c1') order by other_ts) latest on other_ts partition by id"
            );

            // latest by non-designated timestamp, no order
            // note: other_ts is equal for both 211 and 212 records, so it's fine that
            // the second returned row is different from the ordered by query
            assertSql(
                    "id\tname\tvalue\tts\tother_ts\n" +
                            "d1\tc1\t111\t1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000003Z\n" +
                            "d2\tc1\t212\t1970-01-01T00:00:00.000004Z\t1970-01-01T00:00:00.000003Z\n", "(tab where name in ('c1')) latest on other_ts partition by id"
            );

            // empty sub-query
            assertSql(
                    "id\tname\tvalue\tts\tother_ts\n", "(tab where name in ('c3')) latest on ts partition by id"
            );
        });
    }

    @Test
    public void testLatestByOnSubQueryWithoutRandomAccessSupport() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab(" +
                    "    id symbol, " +
                    "    name symbol, " +
                    "    value long, " +
                    "    ts timestamp, " +
                    "    other_ts timestamp" +
                    ") timestamp(ts) partition by day");

            insert("insert into tab values ('d1', 'c1', 111, 1, 3)");
            insert("insert into tab values ('d1', 'c1', 112, 2, 2)");
            insert("insert into tab values ('d1', 'c1', 113, 3, 1)");
            insert("insert into tab values ('d1', 'c2', 121, 2, 1)");
            insert("insert into tab values ('d1', 'c2', 122, 3, 2)");
            insert("insert into tab values ('d1', 'c2', 123, 4, 3)");
            insert("insert into tab values ('d2', 'c1', 211, 3, 3)");
            insert("insert into tab values ('d2', 'c1', 212, 4, 3)");
            insert("insert into tab values ('d2', 'c2', 221, 5, 4)");
            insert("insert into tab values ('d2', 'c2', 222, 6, 5)");

            // select all columns
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc2\t123\t1970-01-01T00:00:00.000004Z\n" +
                            "d2\tc2\t222\t1970-01-01T00:00:00.000006Z\n", "(select id, name, max(value) value, max(ts) ts from tab sample by 1T) latest on ts partition by id"
            );

            // partition by multiple columns
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t113\t1970-01-01T00:00:00.000003Z\n" +
                            "d1\tc2\t123\t1970-01-01T00:00:00.000004Z\n" +
                            "d2\tc1\t212\t1970-01-01T00:00:00.000004Z\n" +
                            "d2\tc2\t222\t1970-01-01T00:00:00.000006Z\n", "(select id, name, max(value) value, max(ts) ts from tab sample by 1T) latest on ts partition by id, name"
            );

            // select subset of columns
            assertSql(
                    "value\tts\n" +
                            "123\t1970-01-01T00:00:00.000004Z\n" +
                            "222\t1970-01-01T00:00:00.000006Z\n", "select value, ts from (select id, name, max(value) value, max(ts) ts from tab sample by 1T) latest on ts partition by id"
            );

            // empty sub-query
            assertSql(
                    "id\tname\tvalue\tts\n", "(select id, name, max(value) value, max(ts) ts from tab where id in('c3') sample by 1T) latest on ts partition by id"
            );
        });
    }

    @Test
    public void testLatestBySelectAllFilteredBySymbolInAllIndexed() throws Exception {
        testLatestBySelectAllFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol index,\n" +
                "  metric symbol index,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestBySelectAllFilteredBySymbolInNoIndexes() throws Exception {
        testLatestBySelectAllFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol,\n" +
                "  metric symbol,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestBySelectAllFilteredBySymbolInPartiallyIndexed1() throws Exception {
        testLatestBySelectAllFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol index,\n" +
                "  metric symbol,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestBySelectAllFilteredBySymbolInPartiallyIndexed2() throws Exception {
        testLatestBySelectAllFilteredBySymbolIn("create table x (\n" +
                "  ts timestamp,\n" +
                "  node symbol,\n" +
                "  metric symbol index,\n" +
                "  value long) \n" +
                "  timestamp(ts) partition by day");
    }

    @Test
    public void testLatestBySubQuery() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10)) latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestBySubQueryDeferred() throws Exception {
        // no index
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'UCLA'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tUCLA\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestBySubQueryDeferredFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))" +
                " and a > 12 and a < 50 and test_match()" +
                " latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " 33.46," +
                " 'UCLA'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "33.46\tUCLA\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        // good
        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestBySubQueryDeferredIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select list('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10)) latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'UCLA'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tUCLA\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestBySubQueryDeferredIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null, 'UCLA') a from long_sequence(10))" +
                " and a > 12 and a < 50 and test_match()" +
                " latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " 33.46," +
                " 'UCLA'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "33.46\tUCLA\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestBySubQueryFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        // no index
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                " and a > 12 and a < 50 and test_match()" +
                " latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " 33.46," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "33.46\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestBySubQueryIndexed() throws Exception {
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select list('RXGZ', 'HYRX', null) a from long_sequence(10)) latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestBySubQueryIndexedFiltered() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", "select * from x where b in (select rnd_symbol('RXGZ', 'HYRX', null) a from long_sequence(10))" +
                " and a > 12 and a < 50 and test_match()" +
                " latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "),index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " 33.46," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n" +
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n" +
                "33.46\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);

        Assert.assertTrue(TestMatchFunctionFactory.assertAPI(sqlExecutionContext));
    }

    @Test
    public void testLatestBySubQueryIndexedIntColumn() throws Exception {
        assertException(
                "select * from x where b in (select 1 a from long_sequence(4)) latest on k partition by b",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                35,
                "unsupported column type"
        );
    }

    @Test
    public void testLatestBySubQueryIndexedStrColumn() throws Exception {
        assertQuery("a\tb\tk\n" +
                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n", "select * from x where b in (select 'RXGZ' from long_sequence(4)) latest on k partition by b", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "), index(b) timestamp(k) partition by DAY", "k", "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
                "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n", true, true, false);
    }

    @Test
    public void testLatestBySupportedColumnTypes0() throws Exception {
        testLatestBySupportedColumnTypes(
                "create table tab (" +
                        "    boolean boolean, " +
                        "    short short, " +
                        "    int int, " +
                        "    long long, " +
                        "    long256 long256, " +
                        "    char char, " +
                        "    string string, " +
                        "    symbol symbol, " +
                        "    ts timestamp" +
                        ") timestamp(ts)"
        );
    }

    @Test
    public void testLatestBySupportedColumnTypes1() throws Exception {
        testLatestBySupportedColumnTypes(
                "create table tab (" +
                        "    boolean boolean, " +
                        "    short short, " +
                        "    int int, " +
                        "    long long, " +
                        "    long256 long256, " +
                        "    char char, " +
                        "    string string, " +
                        "    symbol symbol, " +
                        "    ts timestamp" +
                        ") timestamp(ts) partition by DAY"
        );
    }

    @Test
    public void testLatestBySupportedColumnTypes2() throws Exception {
        testLatestBySupportedColumnTypes(
                "create table tab (" +
                        "    boolean boolean, " +
                        "    short short, " +
                        "    int int, " +
                        "    long long, " +
                        "    long256 long256, " +
                        "    char char, " +
                        "    string string, " +
                        "    symbol symbol index, " +
                        "    ts timestamp" +
                        ") timestamp(ts)"
        );
    }

    @Test
    public void testLatestBySupportedColumnTypes3() throws Exception {
        testLatestBySupportedColumnTypes(
                "create table tab (" +
                        "    boolean boolean, " +
                        "    short short, " +
                        "    int int, " +
                        "    long long, " +
                        "    long256 long256, " +
                        "    char char, " +
                        "    string string, " +
                        "    symbol symbol index, " +
                        "    ts timestamp" +
                        ") timestamp(ts) partition by DAY"
        );
    }

    @Test
    public void testLatestByTimestampInclusion() throws Exception {
        assertQuery(
                "ts\tmarket_type\tavg\n" +
                        "1970-01-01T00:00:09.999996Z\taaa\t0.02110922811597793\n" +
                        "1970-01-01T00:00:09.999996Z\tbbb\t0.344021345830156\n",
                "select ts, market_type, avg(bid_price) FROM market_updates LATEST ON ts PARTITION BY market_type SAMPLE BY 1s",
                "create table market_updates as (" +
                        "select " +
                        "rnd_symbol('aaa','bbb') market_type, " +
                        "rnd_double() bid_price, " +
                        "timestamp_sequence(0,1) ts " +
                        "from long_sequence(10000000)" +
                        ") timestamp(ts)",
                "ts",
                false,
                false
        );
    }

    @Test
    public void testLatestByUnsupportedColumnTypes() throws Exception {
        // unsupported: [BINARY]
        CharSequence createTableDDL = "create table comprehensive as (" +
                "    select" +
                "        rnd_bin(10, 20, 2) binary, " +
                "        timestamp_sequence(0, 1000000000) ts" +
                "    from long_sequence(10)" +
                ") timestamp(ts) partition by DAY";
        CharSequence expectedTail = "invalid type, only [BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, FLOAT, DOUBLE, LONG128, LONG256, CHAR, STRING, SYMBOL, UUID, GEOHASH, IPv4] are supported in LATEST ON";
        ddl(createTableDDL);
        for (String[] nameType : new String[][]{
                {"binary", "BINARY"}}) {
            assertException(
                    "comprehensive latest on ts partition by " + nameType[0],
                    40,
                    String.format("%s (%s): %s", nameType[0], nameType[1], expectedTail)
            );
        }
    }

    @Test
    public void testLatestByValue() throws Exception {
        // no index
        assertQuery(
                "a\tb\tk\n" +
                        "65.08594025855301\tHNR\t1970-01-02T03:46:40.000000Z\n",
                "select * from x where b = 'HNR' latest on k partition by b",
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
                false,
                true
        );
    }

    @Test
    public void testLeftJoinDoesNotRequireTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create TABLE sensors (ID LONG, make STRING, city STRING);");
            insert(
                    "INSERT INTO sensors\n" +
                            "SELECT\n" +
                            "    x ID, --increasing integer\n" +
                            "    rnd_str('Eberle', 'Honeywell', 'Omron', 'United Automation', 'RS Pro') make,\n" +
                            "    rnd_str('New York', 'Miami', 'Boston', 'Chicago', 'San Francisco') city\n" +
                            "FROM long_sequence(10000) x;"
            );

            ddl(
                    "CREATE TABLE readings\n" +
                            "AS(\n" +
                            "    SELECT\n" +
                            "        x ID,\n" +
                            "        timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), rnd_long(1,10,0) * 100000L) ts,\n" +
                            "        rnd_double(0)*8 + 15 temp,\n" +
                            "        rnd_long(0, 10000, 0) sensorId\n" +
                            "    FROM long_sequence(10000000) x)\n" +
                            "TIMESTAMP(ts)\n" +
                            "PARTITION BY MONTH;"
            );

            TestUtils.assertSql(
                    engine,
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
                            "2019-10-21T00:00:33.400000Z\tMiami\tOmron\t18.932566749129236\n" +
                            "2019-10-21T01:00:33.400000Z\tMiami\tOmron\t18.802647154761893\n" +
                            "2019-10-21T02:00:33.400000Z\tMiami\tOmron\t19.03035744821532\n" +
                            "2019-10-21T03:00:33.400000Z\tMiami\tOmron\t18.804900907023185\n" +
                            "2019-10-21T04:00:33.400000Z\tMiami\tOmron\t19.123091392621898\n" +
                            "2019-10-21T05:00:33.400000Z\tMiami\tOmron\t18.78896500447384\n" +
                            "2019-10-21T06:00:33.400000Z\tMiami\tOmron\t19.035384774448648\n" +
                            "2019-10-21T07:00:33.400000Z\tMiami\tOmron\t18.962130361474422\n" +
                            "2019-10-21T08:00:33.400000Z\tMiami\tOmron\t19.053135989612862\n" +
                            "2019-10-21T09:00:33.400000Z\tMiami\tOmron\t19.06031649712395\n" +
                            "2019-10-21T10:00:33.400000Z\tMiami\tOmron\t18.905183164733987\n" +
                            "2019-10-21T11:00:33.400000Z\tMiami\tOmron\t19.25110121789431\n" +
                            "2019-10-21T12:00:33.400000Z\tMiami\tOmron\t18.949748689865256\n" +
                            "2019-10-21T13:00:33.400000Z\tMiami\tOmron\t18.879919778920833\n" +
                            "2019-10-21T14:00:33.400000Z\tMiami\tOmron\t18.860652952600383\n" +
                            "2019-10-21T15:00:33.400000Z\tMiami\tOmron\t18.887336270163946\n" +
                            "2019-10-21T16:00:33.400000Z\tMiami\tOmron\t19.024634888199024\n" +
                            "2019-10-21T17:00:33.400000Z\tMiami\tOmron\t18.790227608308893\n" +
                            "2019-10-21T18:00:33.400000Z\tMiami\tOmron\t19.19110337386028\n" +
                            "2019-10-21T19:00:33.400000Z\tMiami\tOmron\t19.264680712259782\n" +
                            "2019-10-21T20:00:33.400000Z\tMiami\tOmron\t19.040759013405363\n" +
                            "2019-10-21T21:00:33.400000Z\tMiami\tOmron\t18.880447260735636\n" +
                            "2019-10-21T22:00:33.400000Z\tMiami\tOmron\t18.931106673084784\n" +
                            "2019-10-21T23:00:33.400000Z\tMiami\tOmron\t18.902751956422147\n" +
                            "2019-10-22T00:00:33.400000Z\tMiami\tOmron\t19.17787378419351\n" +
                            "2019-10-22T01:00:33.400000Z\tMiami\tOmron\t19.152877099534674\n" +
                            "2019-10-22T02:00:33.400000Z\tMiami\tOmron\t18.758696537494444\n" +
                            "2019-10-22T03:00:33.400000Z\tMiami\tOmron\t18.976188057903155\n" +
                            "2019-10-22T04:00:33.400000Z\tMiami\tOmron\t19.012327751968147\n" +
                            "2019-10-22T05:00:33.400000Z\tMiami\tOmron\t19.10191226261768\n" +
                            "2019-10-22T06:00:33.400000Z\tMiami\tOmron\t18.901817634883447\n" +
                            "2019-10-22T07:00:33.400000Z\tMiami\tOmron\t18.978597612188544\n" +
                            "2019-10-22T08:00:33.400000Z\tMiami\tOmron\t18.81682404517824\n" +
                            "2019-10-22T09:00:33.400000Z\tMiami\tOmron\t18.990314654856732\n" +
                            "2019-10-22T10:00:33.400000Z\tMiami\tOmron\t19.18799015324707\n" +
                            "2019-10-22T11:00:33.400000Z\tMiami\tOmron\t19.364656961385503\n" +
                            "2019-10-22T12:00:33.400000Z\tMiami\tOmron\t18.995868339279518\n" +
                            "2019-10-22T13:00:33.400000Z\tMiami\tOmron\t18.96954916895684\n" +
                            "2019-10-22T14:00:33.400000Z\tMiami\tOmron\t19.011398221478398\n" +
                            "2019-10-22T15:00:33.400000Z\tMiami\tOmron\t18.865504622696875\n" +
                            "2019-10-22T16:00:33.400000Z\tMiami\tOmron\t18.837200881627382\n" +
                            "2019-10-22T17:00:33.400000Z\tMiami\tOmron\t19.09786870870688\n" +
                            "2019-10-22T18:00:33.400000Z\tMiami\tOmron\t19.06560806760199\n" +
                            "2019-10-22T19:00:33.400000Z\tMiami\tOmron\t18.843835675155812\n" +
                            "2019-10-22T20:00:33.400000Z\tMiami\tOmron\t18.939776640738202\n" +
                            "2019-10-22T21:00:33.400000Z\tMiami\tOmron\t18.87168249042935\n" +
                            "2019-10-22T22:00:33.400000Z\tMiami\tOmron\t18.835373939360398\n" +
                            "2019-10-22T23:00:33.400000Z\tMiami\tOmron\t18.90154105169603\n"
            );
        });
    }

    @Test
    public void testLessNoOpFilter() throws Exception {
        assertQuery(
                "c0\n",
                "select t7.c0 from t7 where t7.c0 < t7.c0",
                "create table t7 as (select 42 as c0 from long_sequence(1))",
                null,
                false,
                false
        );
    }

    @Test
    public void testLessOrEqualsNoOpFilter() throws Exception {
        assertQuery(
                "c0\n" +
                        "42\n",
                "select t7.c0 from t7 where t7.c0 <= t7.c0",
                "create table t7 as (select 42 as c0 from long_sequence(1))",
                null,
                true,
                false
        );
    }

    @Test
    public void testLimitOverflow() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select x from long_sequence(10))");
            snapshotMemoryUsage();
            try (RecordCursorFactory factory = select("x limit -9223372036854775807-1, -1")) {
                assertCursor(
                        "x\n" +
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
                        factory,
                        true,
                        false
                );
            }
        });
    }

    @Test
    public void testLongCursor() throws Exception {
        assertQuery(
                "x\n" +
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
                true
        );


        // test another record count

        assertQuery(
                "x\n" +
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
                true
        );

        // test 0 record count

        assertQuery(
                "x\n",
                "select * from long_sequence(0)",
                null,
                null,
                true,
                true
        );

        assertQuery(
                "x\n",
                "select * from long_sequence(-2)",
                null,
                null,
                true,
                true
        );
    }

    @Test
    public void testMaxDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testMaxDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testMinDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testMinDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testNamedBindVariableInWhere() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            bindVariableService.setLong("var", 10);

            try (RecordCursorFactory factory = select("select x from long_sequence(100) where x = :var")) {
                assertCursor(
                        "x\n" +
                                "10\n",
                        factory,
                        true,
                        false
                );
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

        assertQuery(
                expected,
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
                        "56.594291398612405\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true
        );
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

        assertQuery(expected, "x order by a,b,c,d,e,f,g,i,j,k,l,n", "create table x as " +
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
                ") timestamp(k) partition by NONE", null, "insert into x(a,d,c,k) select * from (" +
                "select" +
                " 1194691157," +
                " rnd_double(0)*100," +
                " 'RXGZ'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tc\td\te\tf\tg\ti\tj\tk\tl\tm\tn\n" +
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
                "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t1970-01-01T00:00:00.000000Z\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\tFOWLPD\n", true, true, false);
    }

    @Test
    public void testOrderByFull() throws Exception {
        assertQuery(
                "b\tsum\tk\n" +
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
                true
        );
    }

    @Test
    public void testOrderByFullSymbol() throws Exception {
        assertQuery(
                "b\tsum\tk\n" +
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
                true
        );
    }

    @Test
    public void testOrderByFullTimestampLead() throws Exception {
        assertQuery(
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
                        "YCT\t57.78947915182423\t1970-01-03T18:00:00.000000Z\n",
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
                "k",
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
                true
        );
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

        assertQuery(expected, "select * from x " +
                " order by b, a", "create table x as " +
                "(" +
                "select" +
                " rnd_long256() a," +
                " rnd_char() b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by DAY", null, "insert into x select * from (" +
                "select" +
                " rnd_long256()," +
                " 'W'," +
                " to_timestamp('1971', 'yyyy') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", expected2, true, true, false);
    }

    @Test
    public void testOrderByNonUnique() throws Exception {
        final String expected = "a\tc\tk\tn\n" +
                "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
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
                "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n";

        assertQuery(expected, "x order by c",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_str(1,1,2) c," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_str(5,16,2) n" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
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
                        "-1966408995\t\t1970-01-01T02:30:00.000000Z\tBZXIOVIKJSMSS\n" +
                        "-1470806499\t\t1970-01-01T03:53:20.000000Z\t\n" +
                        "-1182156192\t\t1970-01-01T04:43:20.000000Z\tGLUOHNZHZS\n" +
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
                        "1570930196\tJ\t1971-01-01T00:00:00.000000Z\tAPPC\n" +
                        "410717394\tO\t1970-01-01T01:06:40.000000Z\tGETJR\n" +
                        "326010667\tS\t1970-01-01T01:23:20.000000Z\tRFBVTMHGOOZZVDZ\n" +
                        "1743740444\tS\t1970-01-01T02:46:40.000000Z\tTKVVSJ\n" +
                        "1876812930\tV\t1970-01-01T01:56:40.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "1545253512\tX\t1970-01-01T00:16:40.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "-938514914\tX\t1970-01-01T00:50:00.000000Z\tBEOUOJSHRUEDRQQ\n" +
                        "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n", true, true, false);
    }

    @Test
    public void testOrderByPositionalOnAggregateColumn() throws Exception {
        final String expected = "a\tcount\n" +
                "0\t3\n" +
                "1\t3\n" +
                "4\t3\n" +
                "5\t1\n";

        assertQuery(
                expected,
                "select a, count(*) from x order by 2 desc, 1 asc",
                "create table x as (" +
                        "select" +
                        " rnd_int(0,5,0) a," +
                        " rnd_int() b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPositionalOnAliasedAggregateColumn() throws Exception {
        final String expected = "col_1\tcol_cnt\n" +
                "0\t3\n" +
                "1\t3\n" +
                "4\t3\n" +
                "5\t1\n";

        assertQuery(
                expected,
                "select a col_1, count(*) col_cnt from x order by 2 desc, 1 asc",
                "create table x as (" +
                        "select" +
                        " rnd_int(0,5,0) a," +
                        " rnd_int() b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPositionalOnAliasedVirtualColumn() throws Exception {
        final String expected = "col_1\tcol_2\tcol_sum\n" +
                "1326447242\t592859671\t1919306913\n" +
                "-1436881714\t-1575378703\t1282706879\n" +
                "-1191262516\t-2041844972\t1061859808\n" +
                "1868723706\t-847531048\t1021192658\n" +
                "1548800833\t-727724771\t821076062\n" +
                "-409854405\t339631474\t-70222931\n" +
                "-1148479920\t315515118\t-832964802\n" +
                "73575701\t-948263339\t-874687638\n" +
                "1569490116\t1573662097\t-1151815083\n" +
                "806715481\t1545253512\t-1942998303\n";

        assertQuery(
                expected,
                "select a col_1, b col_2, a+b col_sum from x order by 3 desc, 2, 1 desc",
                "create table x as (" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPositionalOnVirtualColumn() throws Exception {
        final String expected = "a\tb\tcolumn\n" +
                "1326447242\t592859671\t1919306913\n" +
                "-1436881714\t-1575378703\t1282706879\n" +
                "-1191262516\t-2041844972\t1061859808\n" +
                "1868723706\t-847531048\t1021192658\n" +
                "1548800833\t-727724771\t821076062\n" +
                "-409854405\t339631474\t-70222931\n" +
                "-1148479920\t315515118\t-832964802\n" +
                "73575701\t-948263339\t-874687638\n" +
                "1569490116\t1573662097\t-1151815083\n" +
                "806715481\t1545253512\t-1942998303\n";

        assertQuery(
                expected,
                "select a, b, a+b from x order by 3 desc, 2, 1 desc",
                "create table x as (" +
                        "select" +
                        " rnd_int() a," +
                        " rnd_int() b," +
                        " timestamp_sequence(0, 1000000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                null,
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

        assertQuery(
                expected,
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
                        "72.30015763133606\tRXGZ\t1971-01-01T00:00:00.000000Z\n",
                true
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
                "k",
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
                        "852921272\tC\t1970-01-01T00:00:01.000000Z\tLSUWDSWUGSHOLN\n" +
                        "852921272\tJ\t1970-01-01T00:00:01.000000Z\tAPPC\n" +
                        "1431775887\tC\t1970-01-01T00:00:01.000000Z\tEHNOMVELLKKHT\n" +
                        "1545253512\tX\t1970-01-01T00:00:01.000000Z\tSXUXIBBTGPGWFFY\n" +
                        "1743740444\tS\t1970-01-01T00:00:01.000000Z\tTKVVSJ\n" +
                        "1876812930\tV\t1970-01-01T00:00:01.000000Z\tSDOTSEDYYCTGQOLY\n" +
                        "1907911110\tE\t1970-01-01T00:00:01.000000Z\tPHRIPZIMNZ\n",
                true, true, false);
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

        assertQuery(expected, "x order by c,n desc", "create table x as " +
                "(" +
                "select" +
                " rnd_int() a," +
                " rnd_str(1,1,2) c," +
                " timestamp_sequence(0, 1000000000) k," +
                " rnd_str(5,16,2) n" +
                " from" +
                " long_sequence(20)" +
                ") timestamp(k) partition by NONE", null, "insert into x select * from (" +
                "select" +
                " rnd_int()," +
                " 'J'," +
                " to_timestamp('1971', 'yyyy') t," +
                " 'ZZCC'" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tc\tk\tn\n" +
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
                "-235358133\tY\t1970-01-01T01:40:00.000000Z\tCXZOUICWEK\n", true, true, false);
    }

    @Test
    public void testOrderByUnsupportedType() throws Exception {
        assertException(
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
                        ") timestamp(k) partition by NONE",
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
                "Z\n", "select * from x order by a", "create table x as " +
                "(" +
                "select" +
                " rnd_char() a" +
                " from" +
                " long_sequence(20)" +
                ")", null, "insert into x select * from (" +
                "select" +
                " rnd_char()" +
                " from" +
                " long_sequence(5)" +
                ")", "a\n" +
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
                "Z\n", true, true, false);
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

        assertQuery(expected, "x where k IN '1970-01' order by b asc", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "),index(b) timestamp(k) partition by MONTH", null, "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'ABC'," +
                " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
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
                "48.820511018586934\tVTJW\t1970-01-12T13:46:40.000000Z\n", true, true, false);
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

        assertQuery(expected, "x where k IN '1970-01' order by b desc", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "),index(b) timestamp(k) partition by MONTH", null, "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'ABC'," +
                " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
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
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", true, true, false);
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

        assertQuery(expected, "x where k IN '1970-01' order by b, k desc", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from long_sequence(20)" +
                "),index(b) timestamp(k) partition by MONTH", null, "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'ABC'," +
                " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
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
                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n", true, true, false);
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

        assertQuery(expected, "x where k IN '1970-01' order by b desc, k", "create table x as " +
                "(" +
                "select" +
                " rnd_double(0)*100 a," +
                " rnd_symbol(5,4,4,1) b," +
                " timestamp_sequence(0, 100000000000) k" +
                " from" +
                " long_sequence(20)" +
                "),index(b) timestamp(k) partition by DAY", null, "insert into x select * from (" +
                "select" +
                " rnd_double(0)*100," +
                " 'ABC'," +
                " to_timestamp('1970-01-24', 'yyyy-MM-dd') t" +
                " from long_sequence(1)" +
                ") timestamp(t)", "a\tb\tk\n" +
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
                "40.455469747939254\t\t1970-01-22T23:46:40.000000Z\n", true, true, false);
    }

    @Test
    public void testRecordJoinExpansion() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x(a int)");
            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "select pg_catalog.pg_class() x, (pg_catalog.pg_class()).relnamespace from long_sequence(2)",
                    sink,
                    "x1\tcolumn\n" +
                            "\t11\n" +
                            "\t2200\n" +
                            "\t11\n" +
                            "\t2200\n"
            );
        });
    }

    @Test
    public void testSampleByFillLinearEmptyCursor() throws Exception {
        assertQuery(
                "b\tsum\tk\n",
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
                "k",
                true,
                true
        );
    }

    @Test
    public void testSampleByFillNoneEmptyCursor() throws Exception {
        assertQuery(
                "b\tsum\tk\n",
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
                "k",
                true
        );
    }

    @Test
    public void testSampleByFillNullEmptyCursor() throws Exception {
        assertQuery(
                "b\tsum\tk\n",
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
                "k",
                true
        );
    }

    @Test
    public void testSampleByFillPrevEmptyCursor() throws Exception {
        assertQuery(
                "b\tsum\tk\n",
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
                "k",
                true
        );
    }

    @Test
    public void testSampleByFillValueEmptyCursor() throws Exception {
        assertQuery(
                "b\tsum\tk\n",
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
                "k",
                true
        );
    }

    @Test
    public void testSampleByOnTimestampOverriddenByOtherColumnAlias() throws Exception {
        assertQuery(
                "min\ttimestamp\n" +
                        "1\tA\n" +
                        "3\tB\n" +
                        "16\tA\n" +
                        "18\tB\n",
                "select min(x), sym timestamp from test1 sample by 15s",
                "create table test1 as (" +
                        "select rnd_symbol('A', 'B') sym, x, timestamp_sequence('2023-07-20', 1000000) timestamp " +
                        "from long_sequence(20)) " +
                        "timestamp(timestamp)",
                null,
                false,
                false
        );

        assertPlan("select min(x), sym timestamp  from test1 sample by 15s",
                "SampleBy\n" +
                        "  keys: [timestamp]\n" +
                        "  values: [min(x)]\n" +
                        "    SelectedRecord\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: test1\n");

        assertQuery(
                "min\ttimestamp\ttimestamp0\n" +
                        "1\tB\tB\n" +
                        "2\tA\tB\n" +
                        "16\tA\tB\n" +
                        "17\tB\tB\n",
                "select min(x), sym1 timestamp, sym2 timestamp0 from test2 sample by 15s",
                "create table test2 as (" +
                        "select rnd_symbol('A', 'B') sym1, " +
                        "       rnd_symbol('B') sym2, " +
                        "       x, " +
                        "       timestamp_sequence('2023-07-20', 1000000) timestamp " +
                        "from long_sequence(20)) " +
                        "timestamp(timestamp)",
                null,
                false,
                false
        );
    }

    @Test
    public void testSelectColumns() throws Exception {
        assertQuery(
                "a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
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
                true
        );
    }

    @Test
    public void testSelectColumnsSansTimestamp() throws Exception {
        assertQuery(
                "a\ta1\tb\tc\td\te\tf1\tf\tg\th\ti\tj\tj1\n" +
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
                true
        );
    }

    @Test
    public void testSelectDistinct() throws Exception {
        final String expected = "a\n" +
                "0\n" +
                "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n" +
                "6\n" +
                "7\n" +
                "8\n" +
                "9\n";

        assertQuery(expected, "select distinct a from x order by a", "create table x as " +
                "(" +
                "select" +
                " abs(rnd_int())%10 a" +
                " from" +
                " long_sequence(20)" +
                ")", null, "insert into x select * from (" +
                "select" +
                " abs(rnd_int())%10 a" +
                " from long_sequence(1000000)" +
                ") ", expected, true, true, false);
    }

    @Test
    public void testSelectDistinctSymbol() throws Exception {
        final String expected = "a\n" +
                "\n" +
                "BHFOW\n" +
                "CCXZ\n" +
                "DZJMY\n" +
                "EHNRX\n" +
                "FBVTMH\n" +
                "GETJ\n" +
                "IBBTGP\n" +
                "KGHVUV\n" +
                "OOZZ\n" +
                "OUOJS\n" +
                "PDXYSB\n" +
                "QULOF\n" +
                "RUEDR\n" +
                "SWHYRX\n" +
                "SZSRY\n" +
                "TJWC\n" +
                "UICW\n" +
                "WFFYUD\n" +
                "YYQE\n" +
                "ZSXU\n";

        final String expected2 = "a\n" +
                "\n" +
                "BHFOW\n" +
                "CCXZ\n" +
                "CJFT\n" +
                "DZJMY\n" +
                "EHNRX\n" +
                "FBVTMH\n" +
                "GETJ\n" +
                "HLDN\n" +
                "IBBTGP\n" +
                "IIB\n" +
                "ILQP\n" +
                "KGHVUV\n" +
                "NDMRS\n" +
                "OOZZ\n" +
                "OUOJS\n" +
                "PDXYSB\n" +
                "QULOF\n" +
                "ROGHY\n" +
                "RUEDR\n" +
                "SSCL\n" +
                "SVNVD\n" +
                "SWHYRX\n" +
                "SZSRY\n" +
                "TJWC\n" +
                "UICW\n" +
                "VZKE\n" +
                "WFFYUD\n" +
                "WNX\n" +
                "YYQE\n" +
                "ZSXU\n";

        assertQuery(
                expected,
                "select distinct a from x order by a",
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
                false,
                false
        );
    }

    @Test
    public void testSelectDistinctWithColumnAlias() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table my_table as (select x as id from long_sequence(1))");
            try (RecordCursorFactory factory = select("select distinct id as foo from my_table")) {
                RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(ColumnType.LONG, metadata.getColumnType(0));
                assertCursor("foo\n" +
                        "1\n", factory, true, false);
            }
        });
    }

    @Test
    public void testSelectDistinctWithColumnAliasAndTableFunction() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table my_table (id long)");
            try (RecordCursorFactory factory = select("select distinct x as foo from long_sequence(1)")) {
                RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(ColumnType.LONG, metadata.getColumnType(0));

                assertCursor("foo\n" +
                        "1\n", factory, true, false);
            }
        });
    }

    @Test
    public void testSelectDistinctWithColumnAliasOnExplicitJoin() throws Exception {
        assertQuery("id\n" +
                        "1\n" +
                        "2\n",
                "select distinct t1.id " +
                        "from  tab t1 " +
                        "join (select x as id from long_sequence(2)) t2 on (t1.id=t2.id)",
                "create table tab as (select x as id from long_sequence(3))",
                null, false, false
        );
    }

    @Test
    public void testSelectDistinctWithColumnAliasOnImplicitJoin() throws Exception {
        assertQuery("id\n" +
                        "1\n" +
                        "2\n",
                "select distinct t1.id " +
                        "from  tab t1, tab t2",
                "create table tab as (select x as id from long_sequence(2))",
                null, false, false
        );
    }

    @Ignore("result order is currently dependent on stability of sorting method")
    // TODO: this is broken, the expected result order for "select * from tab" in the presence
    //  of repeated timestamps needs to be predefined and consistent, one of two alternatives:
    //  1.- most recent insert for a given timestamp first:
    //    "d2\tc1\t201.10000000000002\t2021-10-05T11:31:35.878000Z\n" +
    //    "d1\tc2\t102.10000000000001\t2021-10-05T11:31:35.878000Z\n" +
    //    "d1\tc1\t101.10000000000001\t2021-10-05T11:31:35.878000Z\n" +
    //    "d2\tc1\t201.2\t2021-10-05T12:31:35.878000Z\n" +
    //    "d1\tc2\t102.2\t2021-10-05T12:31:35.878000Z\n" +
    //    "d1\tc1\t101.2\t2021-10-05T12:31:35.878000Z\n" +
    //    "d2\tc1\t201.3\t2021-10-05T13:31:35.878000Z\n" +
    //    "d1\tc2\t102.30000000000001\t2021-10-05T13:31:35.878000Z\n" +
    //    "d1\tc1\t101.3\t2021-10-05T13:31:35.878000Z\n" +
    //    "d2\tc1\t201.4\t2021-10-05T14:31:35.878000Z\n" +
    //    "d1\tc2\t102.4\t2021-10-05T14:31:35.878000Z\n" +
    //    "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
    //    "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n" +
    //    "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n" +
    //    "d2\tc1\t401.20000000000005\t2021-10-06T12:31:35.878000Z\n" +
    //    "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n"
    //  2.- least recent insert for a given timestamp first:
    //    "d1\tc1\t101.10000000000001\t2021-10-05T11:31:35.878000Z\n" +
    //    "d1\tc2\t102.10000000000001\t2021-10-05T11:31:35.878000Z\n" +
    //    "d2\tc1\t201.10000000000002\t2021-10-05T11:31:35.878000Z\n" +
    //    "d1\tc1\t101.2\t2021-10-05T12:31:35.878000Z\n" +
    //    "d1\tc2\t102.2\t2021-10-05T12:31:35.878000Z\n" +
    //    "d2\tc1\t201.2\t2021-10-05T12:31:35.878000Z\n" +
    //    "d1\tc1\t101.3\t2021-10-05T13:31:35.878000Z\n" +
    //    "d1\tc2\t102.30000000000001\t2021-10-05T13:31:35.878000Z\n" +
    //    "d2\tc1\t201.3\t2021-10-05T13:31:35.878000Z\n" +
    //    "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
    //    "d1\tc2\t102.4\t2021-10-05T14:31:35.878000Z\n" +
    //    "d2\tc1\t201.4\t2021-10-05T14:31:35.878000Z\n" +
    //    "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n" +
    //    "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n" +
    //    "d2\tc1\t401.20000000000005\t2021-10-06T12:31:35.878000Z\n" +
    //    "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n"
    //  in the assertSql that follows, option #2 has been taken.
    @Test
    public void testSelectExpectedOrder() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab(" +
                    "    id symbol index, " +
                    "    name symbol index, " +
                    "    value double, " +
                    "    ts timestamp" +
                    ") timestamp(ts) partition by DAY");
            insert("insert into tab values ('d1', 'c1', 101.1, '2021-10-05T11:31:35.878Z')");
            insert("insert into tab values ('d1', 'c1', 101.2, '2021-10-05T12:31:35.878Z')");
            insert("insert into tab values ('d1', 'c1', 101.3, '2021-10-05T13:31:35.878Z')");
            insert("insert into tab values ('d1', 'c1', 101.4, '2021-10-05T14:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.1, '2021-10-05T11:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.2, '2021-10-05T12:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.3, '2021-10-05T13:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.4, '2021-10-05T14:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.5, '2021-10-05T15:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.1, '2021-10-05T11:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.2, '2021-10-05T12:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.3, '2021-10-05T13:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.4, '2021-10-05T14:31:35.878Z')");
            insert("insert into tab values ('d2', 'c2', 401.1, '2021-10-06T11:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 401.2, '2021-10-06T12:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 111.7, '2021-10-06T15:31:35.878Z')");
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t101.10000000000001\t2021-10-05T11:31:35.878000Z\n" +
                            "d1\tc2\t102.10000000000001\t2021-10-05T11:31:35.878000Z\n" +
                            "d2\tc1\t201.10000000000002\t2021-10-05T11:31:35.878000Z\n" +
                            "d1\tc1\t101.2\t2021-10-05T12:31:35.878000Z\n" +
                            "d1\tc2\t102.2\t2021-10-05T12:31:35.878000Z\n" +
                            "d2\tc1\t201.2\t2021-10-05T12:31:35.878000Z\n" +
                            "d1\tc1\t101.3\t2021-10-05T13:31:35.878000Z\n" +
                            "d1\tc2\t102.30000000000001\t2021-10-05T13:31:35.878000Z\n" +
                            "d2\tc1\t201.3\t2021-10-05T13:31:35.878000Z\n" +
                            "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d1\tc2\t102.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d2\tc1\t201.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n" +
                            "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n" +
                            "d2\tc1\t401.20000000000005\t2021-10-06T12:31:35.878000Z\n" +
                            "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n", "tab"
            );
        });
    }

    @Test
    public void testSelectFromAliasedTable() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table my_table (sym int, id long)");
            try (RecordCursorFactory factory = select("select sum(a.sym) yo, a.id from my_table a")) {
                Assert.assertNotNull(factory);
            }
        });
    }

    @Test
    public void testSelectUndefinedBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            bindVariableService.clear();
            try (RecordCursorFactory factory = select("select $1+x, $2 from long_sequence(10)")) {
                sink.clear();
                factory.getMetadata().toJson(sink);
                TestUtils.assertEquals("{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"column\",\"type\":\"LONG\"},{\"index\":1,\"name\":\"$2\",\"type\":\"STRING\"}],\"timestampIndex\":-1}", sink);
            }
        });
    }

    @Test
    public void testStrAndNullComparisonInFilter() throws Exception {
        assertQuery(
                "res\n",
                "SELECT 1 as res FROM x WHERE x.b > NULL LIMIT 1;",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                null,
                false,
                false
        );
    }

    @Test
    public void testStrAndNullComparisonInSelect() throws Exception {
        assertQuery(
                "column\n" +
                        "false\n",
                "SELECT x.b > NULL FROM x LIMIT 1;",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY",
                null,
                true,
                false
        );
    }

    @Test
    public void testStrComparisonInSelect() throws Exception {
        assertQuery(
                "a\tb\tcolumn\tcolumn1\tcolumn2\tcolumn3\tcolumn4\tcolumn5\n" +
                        "TJ\tCP\tfalse\ttrue\tfalse\ttrue\ttrue\tfalse\n" +
                        "WH\tRX\tfalse\ttrue\tfalse\ttrue\tfalse\ttrue\n" +
                        "EH\tRX\ttrue\tfalse\ttrue\tfalse\tfalse\ttrue\n" +
                        "ZS\tUX\tfalse\ttrue\tfalse\ttrue\tfalse\ttrue\n" +
                        "BB\tGP\ttrue\tfalse\ttrue\tfalse\ttrue\tfalse\n" +
                        "WF\tYU\ttrue\tfalse\tfalse\ttrue\tfalse\ttrue\n" +
                        "EY\tQE\ttrue\tfalse\ttrue\tfalse\ttrue\tfalse\n" +
                        "BH\tOW\ttrue\tfalse\ttrue\tfalse\ttrue\tfalse\n" +
                        "PD\tYS\ttrue\tfalse\ttrue\tfalse\tfalse\ttrue\n" +
                        "EO\tOJ\ttrue\tfalse\ttrue\tfalse\ttrue\tfalse\n" +
                        "HR\tED\tfalse\ttrue\ttrue\tfalse\ttrue\tfalse\n" +
                        "QQ\tLO\tfalse\ttrue\tfalse\ttrue\ttrue\tfalse\n" +
                        "JG\tTJ\ttrue\tfalse\ttrue\tfalse\tfalse\ttrue\n" +
                        "\t\tfalse\tfalse\tfalse\tfalse\tfalse\tfalse\n" +
                        "SR\tRF\tfalse\ttrue\tfalse\ttrue\tfalse\ttrue\n" +
                        "VT\tHG\tfalse\ttrue\tfalse\ttrue\ttrue\tfalse\n" +
                        "\tZZ\tfalse\tfalse\tfalse\tfalse\tfalse\ttrue\n" +
                        "DZ\tMY\ttrue\tfalse\ttrue\tfalse\ttrue\tfalse\n" +
                        "CC\tZO\ttrue\tfalse\ttrue\tfalse\tfalse\ttrue\n" +
                        "IC\tEK\tfalse\ttrue\ttrue\tfalse\ttrue\tfalse\n",
                "SELECT a, b, a < b, a > b, a < 'QQ', a >= 'QQ', b < 'QQ', b >= 'QQ' FROM x;",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_str(2,2,5) a," +
                        " rnd_str(2,2,5) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY",
                null,
                true,
                true
        );
    }

    @Test
    public void testStrippingRowId() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (a int)");
            RecordCursorFactory factory = select("select * from '*!*x'");
            Assert.assertNotNull(factory);
            try {
                Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
            } finally {
                Misc.free(factory);
            }
        });
    }

    @Test
    public void testSumDoubleColumn() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testSumDoubleColumnPartitionByNone() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testSumDoubleColumnWithKahanMethodVectorised1() throws Exception {
        ddl("create table x (ds double)");

        executeInsertStatement(1.0);
        executeInsertStatement(2.0);
    }

    @Test
    public void testSumDoubleColumnWithKahanMethodVectorised2() throws Exception {
        ddl("create table x (ds double)");

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
    }

    @Test
    public void testSumDoubleColumnWithKahanMethodVectorised3() throws Exception {
        ddl("create table x (ds double)");

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
    }

    @Test
    public void testSumDoubleColumnWithNaNs() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testSymbolStrB() throws Exception {
        assertQuery("a\nC\nC\nB\nA\nA\n", "select cast(a as string) a from x order by 1 desc", "create table x as (select rnd_symbol('A','B','C') a, timestamp_sequence(0, 10000) k from long_sequence(5)) timestamp(k)", null, null, null, true, true, false);
    }

    @Test
    public void testTableReaderRemainsUsableAfterClosingAllButNLatestOpenPartitions() throws Exception {
        maxOpenPartitions = 2;

        assertMemoryLeak(() -> {
            ddl("create table x as (" +
                    "select" +
                    " rnd_symbol('foo','bar') s," +
                    " timestamp_sequence(0, 10000000000) ts" +
                    " from long_sequence(50)" +
                    ") timestamp(ts) partition by DAY");

            // we need have more partitions than maxOpenPartitions for this test
            assertSql(
                    "count_distinct\n" +
                            "6\n", "select count_distinct(timestamp_floor('d', ts)) from x"
            );

            for (int i = 0; i < 10; i++) {
                printSqlResult(
                        "ts\ts\tcount\n" +
                                "1970-01-01T00:00:00.000000Z\tfoo\t4\n" +
                                "1970-01-01T00:00:00.000000Z\tbar\t5\n" +
                                "1970-01-02T00:00:00.000000Z\tfoo\t6\n" +
                                "1970-01-02T00:00:00.000000Z\tbar\t3\n" +
                                "1970-01-03T00:00:00.000000Z\tbar\t5\n" +
                                "1970-01-03T00:00:00.000000Z\tfoo\t3\n" +
                                "1970-01-04T00:00:00.000000Z\tfoo\t5\n" +
                                "1970-01-04T00:00:00.000000Z\tbar\t4\n" +
                                "1970-01-05T00:00:00.000000Z\tbar\t5\n" +
                                "1970-01-05T00:00:00.000000Z\tfoo\t4\n" +
                                "1970-01-06T00:00:00.000000Z\tbar\t3\n" +
                                "1970-01-06T00:00:00.000000Z\tfoo\t3\n",
                        "select ts, s, count() from x sample by 1d",
                        "ts",
                        false,
                        false
                );
                // verify that the reader doesn't keep all partitions open once it's returned back to the pool
                try (TableReader reader = engine.getReader("x")) {
                    Assert.assertEquals(6, reader.getPartitionCount());
                    Assert.assertEquals(maxOpenPartitions, reader.getOpenPartitionCount());
                }
            }
        });
    }

    @Test
    public void testTimestampCrossReference() throws Exception {
        ddl("create table x (val double, t timestamp)");
        ddl("create table y (timestamp timestamp, d double)");
        insert("insert into y select timestamp_sequence(cast('2018-01-31T23:00:00.000000Z' as timestamp), 100), rnd_double() from long_sequence(1000)");

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
        ddl("create table readings (sensorId int)");
        ddl("create table sensors (ID int, make symbol, city symbol)");
        assertQuery(
                "sensorId\tsensId\tmake\tcity\n",
                "SELECT * FROM readings JOIN(SELECT ID sensId, make, city FROM sensors) ON readings.sensorId = sensId",
                null,
                false
        );
    }

    @Test
    public void testUtf8TableName() throws Exception {
        assertMemoryLeak(
                () -> {
                    ddl("create TABLE 'привет от штиблет' (f0 STRING, штиблет STRING, f2 STRING);");
                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "select id,table_name,designatedTimestamp,partitionBy,maxUncommittedRows,o3MaxLag from tables()",
                            sink,
                            "id\ttable_name\tdesignatedTimestamp\tpartitionBy\tmaxUncommittedRows\to3MaxLag\n" +
                                    "1\tпривет от штиблет\t\tNONE\t1000\t300000000\n"
                    );

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "show columns from 'привет от штиблет'",
                            sink,
                            "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tdesignated\tupsertKey\n" +
                                    "f0\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                                    "штиблет\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n" +
                                    "f2\tSTRING\tfalse\t0\tfalse\t0\tfalse\tfalse\n"
                    );
                }
        );
    }

    @Test
    public void testVectorAggregateOnSparsePartitions() throws Exception {
        final String expected = "a\tk\n";

        assertQuery(
                expected,
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
                true
        );
    }

    @Test
    public void testVectorSumAvgDoubleRndColumnWithNulls() throws Exception {
        assertQuery(
                "avg\tsum\n" +
                        "0.49811606109211604\t17.932178199316176\n",
                "select avg(c),sum(c) from x",
                "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b, rnd_double(2) c, rnd_int() d from long_sequence(42))",
                null,
                false,
                true
        );
    }

    @Test
    public void testVectorSumAvgDoubleRndColumnWithNullsParallel() throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
        final GroupByVectorAggregateJob job = new GroupByVectorAggregateJob(engine.getMessageBus());
        new Thread(() -> {
            while (running.get()) {
                job.run(0);
            }
            haltLatch.countDown();
        }).start();

        try {
            assertQuery(
                    "avg\tsum\n" +
                            "0.5003504\t834470.437288\n",
                    "select round(avg(c), 7) avg, round(sum(c), 6) sum from x",
                    "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b, rnd_double(2) c, rnd_int() d from long_sequence(2000000))",
                    null,
                    false,
                    true
            );
        } finally {
            running.set(false);
            haltLatch.await();
        }
    }

    @Test
    public void testVectorSumDoubleAndIntWithNulls() throws Exception {
        assertQuery(
                "sum\tsum1\n" +
                        "41676799\t416969.81549\n",
                "select sum(a),round(sum(b),5) sum1 from x",
                "create table x as (select rnd_int(0,100,2) a, rnd_double(2) b from long_sequence(1000035L))",
                null,
                false,
                true
        );
    }

    @Test
    public void testVirtualColumns() throws Exception {
        assertQuery(
                "a\ta1\tb\tc\tcolumn\tf1\tf\tg\th\ti\tj\tj1\tk\tl\tm\n" +
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
                true
        );
    }

    @Test
    public void testVirtualColumnsSansTimestamp() throws Exception {
        assertQuery(
                "a\ta1\tb\tc\tcolumn\n" +
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
                true
        );
    }

    @Test
    public void testWithinClauseWithFilterFails() throws Exception {
        assertException(
                "select * from tab where geo within(#zz) and x > 0 latest on ts partition by sym",
                "create table tab as " +
                        "(" +
                        " select  x, rnd_symbol('a', 'b') sym, rnd_geohash(10) geo, x::timestamp ts " +
                        " from long_sequence(20) " +
                        "), index(sym) timestamp(ts)",
                28,
                "WITHIN clause doesn't work with filters"
        );
    }

    @Test
    public void testWithinClauseWithLatestByNonSymbolOrNonIndexedSymbolColumnFails() throws Exception {
        assertException(
                "select * from tab where geo within(#zz) latest on ts partition by x",
                "create table tab as " +
                        "(" +
                        " select  x, " +
                        "         rnd_symbol('a', 'b') sym_idx, " +
                        "         rnd_symbol('c') sym_noidx, " +
                        "         rnd_geohash(10) geo, " +
                        "         x::timestamp ts " +
                        " from long_sequence(20) " +
                        "), index(sym_idx) timestamp(ts)",
                28,
                "WITHIN clause requires LATEST BY using only indexed symbol columns"
        );

        assertException(
                "select * from tab where geo within(#zz) latest on ts partition by sym_idx, x",
                28,
                "WITHIN clause requires LATEST BY using only indexed symbol columns"
        );

        assertException(
                "select * from tab where geo within(#zz) latest on ts partition by sym_noidx",
                28,
                "WITHIN clause requires LATEST BY using only indexed symbol columns"
        );

        assertException(
                "select * from tab where geo within(#zz) latest on ts partition by sym_idx, sym_noidx",
                28,
                "WITHIN clause requires LATEST BY using only indexed symbol columns"
        );
    }

    @Test
    public void testWithinClauseWithTsFilter() throws Exception {
        assertQuery("x\tsym\tgeo\tts\n" +
                        "17\ta\tzz\t1970-01-01T00:00:00.000017Z\n" +
                        "18\tb\tzz\t1970-01-01T00:00:00.000018Z\n",
                "select * " +
                        "from t1 " +
                        "where geo within(#zz) " +
                        "and ts < '1970-01-01T00:00:00.000019Z' " +
                        "latest on ts " +
                        "partition by sym",
                "create table t1 as " +
                        "(" +
                        " select  x, rnd_symbol('a', 'b') sym, #zz as geo, x::timestamp ts " +
                        " from long_sequence(20) " +
                        "), index(sym) timestamp(ts) ", "ts", true, true
        );
    }

    @Test
    public void testWithinClauseWithoutLatestByFails() throws Exception {
        assertException(
                "select * from tab where geo within(#zz)",
                "create table tab as " +
                        "(" +
                        " select  x, rnd_symbol('a', 'b') sym, rnd_geohash(10) geo, x::timestamp ts " +
                        " from long_sequence(20) " +
                        "), index(sym) timestamp(ts)",
                28,
                "WITHIN clause requires LATEST BY clause"
        );
    }

    private void createGeoHashTable(int chars) throws SqlException {
        ddl(String.format("create table pos(time timestamp, uuid symbol, hash geohash(%dc))", chars) + ", index(uuid) timestamp(time) partition by DAY");

        insert("insert into pos values('2021-05-10T23:59:59.150000Z','XXX','f91t48s7')");
        insert("insert into pos values('2021-05-10T23:59:59.322000Z','ddd','bbqyzfp6')");
        insert("insert into pos values('2021-05-10T23:59:59.351000Z','bbb','9egcyrxq')");
        insert("insert into pos values('2021-05-10T23:59:59.439000Z','bbb','ewef1vk8')");
        insert("insert into pos values('2021-05-10T00:00:00.016000Z','aaa','vb2wg49h')");
        insert("insert into pos values('2021-05-10T00:00:00.042000Z','ccc','bft3gn89')");
        insert("insert into pos values('2021-05-10T00:00:00.055000Z','aaa','z6cf5j85')");
        insert("insert into pos values('2021-05-11T00:00:00.066000Z','ddd','vcunv6j7')");
        insert("insert into pos values('2021-05-11T00:00:00.072000Z','ccc','edez0n5y')");
        insert("insert into pos values('2021-05-11T00:00:00.074000Z','aaa','fds32zgc')");
        insert("insert into pos values('2021-05-11T00:00:00.083000Z','YYY','z31wzd5w')");
        insert("insert into pos values('2021-05-11T00:00:00.092000Z','ddd','v9nwc4ny')");
        insert("insert into pos values('2021-05-11T00:00:00.107000Z','ccc','f6yb1yx9')");
        insert("insert into pos values('2021-05-11T00:00:00.111000Z','ddd','bcnktpnw')");
        insert("insert into pos values('2021-05-11T00:00:00.123000Z','aaa','z3t2we5z')");
        insert("insert into pos values('2021-05-11T00:00:00.127000Z','aaa','bgn1yt4y')");
        insert("insert into pos values('2021-05-11T00:00:00.144000Z','aaa','fuetk3k6')");
        insert("insert into pos values('2021-05-12T00:00:00.167000Z','ccc','bchx5x14')");
        insert("insert into pos values('2021-05-12T00:00:00.167000Z','ZZZ','bbxwb5jj')");
        insert("insert into pos values('2021-05-12T00:00:00.186000Z','ZZZ','vepe7h62')");
        insert("insert into pos values('2021-05-12T00:00:00.241000Z','bbb','bchxpmmg')");
        insert("insert into pos values('2021-05-12T00:00:00.245000Z','ddd','f90z3bs5')");
        insert("insert into pos values('2021-05-12T00:00:00.247000Z','bbb','bftqreuh')");
        insert("insert into pos values('2021-05-12T00:00:00.295000Z','ddd','u2rqgy9s')");
        insert("insert into pos values('2021-05-12T00:00:00.304000Z','aaa','w23bhjd2')");
    }

    private void createRndGeoHashBitsTable() throws SqlException {
        ddl(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol(113, 4, 4, 2) s," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_geohash(3) bits3," +
                        " rnd_geohash(7) bits7," +
                        " rnd_geohash(9) bits9" +
                        " from long_sequence(10000)" +
                        "), index(s) timestamp (ts) partition by DAY"
        );
    }

    private void createRndGeoHashTable() throws SqlException {
        ddl(
                "create table x as (" +
                        "select" +
                        " cast(x as int) i," +
                        " rnd_symbol(113, 4, 4, 2) s," +
                        " timestamp_sequence(500000000000L,100000000L) ts," +
                        " rnd_geohash(5) geo1," +
                        " rnd_geohash(15) geo2," +
                        " rnd_geohash(25) geo4," +
                        " rnd_geohash(40) geo8" +
                        " from long_sequence(10000)" +
                        "), index(s) timestamp (ts) partition by DAY"
        );
    }

    private void executeInsertStatement(double d) throws SqlException {
        String ddl = "insert into x (ds) values (" + d + ")";
        insert(ddl);
    }

    private void expectSqlResult(CharSequence expected, CharSequence query) throws SqlException {
        printSqlResult(expected, query, "ts", true, true);
    }

    private void testBindVariableInIndexedLookup(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create TABLE 'alcatel_traffic_tmp' (" +
                    "deviceName SYMBOL capacity 1000" + (indexed ? " index, " : " , ") +
                    "time TIMESTAMP, " +
                    "slot SYMBOL, " +
                    "port SYMBOL, " +
                    "downStream DOUBLE, " +
                    "upStream DOUBLE" +
                    ") timestamp(time) partition by DAY");
            ddl("create table src as (" +
                    "    select rnd_symbol(15000, 4,4,0) sym, " +
                    "           timestamp_sequence(0, 100000) ts, " +
                    "           rnd_double() val " +
                    "    from long_sequence(500)" +
                    ")");
            insert("insert into alcatel_traffic_tmp select sym, ts, sym, null, val, val from src");
            // =
            try (RecordCursorFactory lookupFactory = select("select * from alcatel_traffic_tmp where deviceName in $1")) {
                bindVariableService.clear();
                bindVariableService.setStr(0, "FKBW");
                sink.clear();
                try (RecordCursor cursor = lookupFactory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, lookupFactory.getMetadata(), true, sink);
                    TestUtils.assertEquals(
                            "deviceName\ttime\tslot\tport\tdownStream\tupStream\n" +
                                    "FKBW\t1970-01-01T00:00:02.300000Z\tFKBW\t\t0.04998168904446332\t0.04998168904446332\n",
                            sink
                    );
                }
            }
            // !=
            try (RecordCursorFactory lookupFactory = select("select * from alcatel_traffic_tmp where deviceName != $1 and time < '1970-01-01T00:00:00.300000Z'")) {
                bindVariableService.clear();
                bindVariableService.setStr(0, "FKBW");
                sink.clear();
                try (RecordCursor cursor = lookupFactory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, lookupFactory.getMetadata(), true, sink);
                    TestUtils.assertEquals(
                            "deviceName\ttime\tslot\tport\tdownStream\tupStream\n" +
                                    "OQCN\t1970-01-01T00:00:00.000000Z\tOQCN\t\t0.004671510951376301\t0.004671510951376301\n" +
                                    "MIWT\t1970-01-01T00:00:00.100000Z\tMIWT\t\t0.8736195736185558\t0.8736195736185558\n" +
                                    "QUUG\t1970-01-01T00:00:00.200000Z\tQUUG\t\t0.6124307350390543\t0.6124307350390543\n",
                            sink
                    );
                }
            }
        });
    }

    private void testBindVariableInLookupList(boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create TABLE 'alcatel_traffic_tmp' (" +
                    "deviceName SYMBOL capacity 1000" + (indexed ? " index, " : " , ") +
                    "time TIMESTAMP, " +
                    "slot SYMBOL, " +
                    "port SYMBOL, " +
                    "downStream DOUBLE, " +
                    "upStream DOUBLE" +
                    ") timestamp(time) partition by DAY");
            ddl("create table src as (" +
                    "    select rnd_symbol(15000, 4,4,0) sym, " +
                    "           timestamp_sequence(0, 100000) ts, " +
                    "           rnd_double() val " +
                    "    from long_sequence(500)" +
                    ")");
            insert("insert into alcatel_traffic_tmp select sym, ts, sym, null, val, val from src");
            // in
            try (RecordCursorFactory lookupFactory = select("select * from alcatel_traffic_tmp where deviceName in ($1,$2)")) {
                bindVariableService.clear();
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
            // not in
            try (RecordCursorFactory lookupFactory = select("select * from alcatel_traffic_tmp where deviceName not in ($1,$2) and time < '1970-01-01T00:00:00.300000Z'")) {
                bindVariableService.clear();
                bindVariableService.setStr(0, "FKBW");
                bindVariableService.setStr(1, "SHRI");
                sink.clear();
                try (RecordCursor cursor = lookupFactory.getCursor(sqlExecutionContext)) {
                    printer.print(cursor, lookupFactory.getMetadata(), true, sink);
                    TestUtils.assertEquals(
                            "deviceName\ttime\tslot\tport\tdownStream\tupStream\n" +
                                    "OQCN\t1970-01-01T00:00:00.000000Z\tOQCN\t\t0.004671510951376301\t0.004671510951376301\n" +
                                    "MIWT\t1970-01-01T00:00:00.100000Z\tMIWT\t\t0.8736195736185558\t0.8736195736185558\n" +
                                    "QUUG\t1970-01-01T00:00:00.200000Z\tQUUG\t\t0.6124307350390543\t0.6124307350390543\n",
                            sink
                    );
                }
            }
        });
    }

    private void testBindVariableWithLike0(String keyword) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table xy as (select rnd_str() v from long_sequence(100))");
            refreshTablesInBaseEngine();
            bindVariableService.clear();
            try (RecordCursorFactory factory = select("xy where v " + keyword + " $1")) {
                bindVariableService.setStr(0, "MBE%");
                assertCursor(
                        "v\n" +
                                "MBEZGHW\n",
                        factory,
                        true,
                        false
                );

                bindVariableService.setStr(0, "Z%");
                assertCursor(
                        "v\n" +
                                "ZSQLDGLOG\n" +
                                "ZLUOG\n" +
                                "ZLCBDMIG\n" +
                                "ZJYYFLSVI\n" +
                                "ZWEVQTQO\n" +
                                "ZSFXUNYQ\n",
                        factory,
                        true,
                        false
                );

                assertCursor(
                        "v\n" +
                                "ZSQLDGLOG\n" +
                                "ZLUOG\n" +
                                "ZLCBDMIG\n" +
                                "ZJYYFLSVI\n" +
                                "ZWEVQTQO\n" +
                                "ZSFXUNYQ\n",
                        factory,
                        true,
                        false
                );


                bindVariableService.setStr(0, null);
                assertCursor(
                        "v\n",
                        factory,
                        true,
                        false
                );
            }
        });
    }

    private void testFilterWithSymbolBindVariable(String query, boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as " +
                    "(" +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,1) b," +
                    " timestamp_sequence(0, 100000000000) k" +
                    " from" +
                    " long_sequence(20)" +
                    ")" + (indexed ? ", index(b) " : " ") + "timestamp(k) partition by DAY");

            try (RecordCursorFactory factory = select(query)) {
                assertCursor(
                        "a\tb\tk\n" +
                                "97.71103146051203\tHYRX\t1970-01-07T22:40:00.000000Z\n" +
                                "12.026122412833129\tHYRX\t1970-01-11T10:00:00.000000Z\n",
                        factory,
                        true,
                        false
                );
            }
        });
    }

    private void testFilterWithSymbolBindVariableNotEquals(String query, boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as " +
                    "(" +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,1) b," +
                    " timestamp_sequence(0, 100000000000) k" +
                    " from" +
                    " long_sequence(5)" +
                    ")" + (indexed ? ", index(b) " : " ") + "timestamp(k) partition by DAY");

            try (RecordCursorFactory factory = select(query)) {
                assertCursor(
                        "a\tb\tk\n" +
                                "11.427984775756228\t\t1970-01-01T00:00:00.000000Z\n" +
                                "42.17768841969397\tVTJW\t1970-01-02T03:46:40.000000Z\n" +
                                "23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z\n" +
                                "70.94360487171201\tPEHN\t1970-01-04T11:20:00.000000Z\n" +
                                "87.99634725391621\t\t1970-01-05T15:06:40.000000Z\n",
                        factory,
                        true,
                        false
                );
            }
        });
    }

    private void testLatestByFilteredBySymbolIn(String ddl) throws Exception {
        assertMemoryLeak(() -> {
            ddl(ddl);

            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node1', 'cpu', 1)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node1', 'cpu', 10)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node1', 'cpu', 100)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node2', 'cpu', 7)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node2', 'cpu', 15)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node2', 'cpu', 75)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node3', 'cpu', 5)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node3', 'cpu', 20)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node3', 'cpu', 25)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node1', 'memory', 20)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node1', 'memory', 200)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node1', 'memory', 2000)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node2', 'memory', 30)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node2', 'memory', 300)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node2', 'memory', 3000)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node3', 'memory', 40)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node3', 'memory', 400)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node3', 'memory', 4000)");

            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "select metric, sum(value) from x \n" +
                            "where node in ('node1', 'node2') and metric in ('cpu') \n" +
                            "latest on ts partition by node",
                    sink,
                    "metric\tsum\n" +
                            "cpu\t175\n"
            );
        });
    }

    private void testLatestByKeyValueWithBindVariable(String query, boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as " +
                    "(" +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,1) b," +
                    " timestamp_sequence(0, 100000000000) k" +
                    " from" +
                    " long_sequence(50)" +
                    ")" + (indexed ? ", index(b) " : " ") + "timestamp(k) partition by DAY");

            try (RecordCursorFactory factory = select(query)) {
                assertCursor(
                        "a\tb\tk\n" +
                                "89.98921791869131\tHYRX\t1970-02-18T14:40:00.000000Z\n",
                        factory,
                        true,
                        false
                );
            }
        });
    }

    private void testLatestByKeyValuesWithBindVariable(String query, boolean indexed) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as " +
                    "(" +
                    "select" +
                    " rnd_double(0)*100 a," +
                    " rnd_symbol(5,4,4,1) b," +
                    " timestamp_sequence(0, 100000000000) k" +
                    " from" +
                    " long_sequence(50)" +
                    ")" + (indexed ? ", index(b) " : " ") + "timestamp(k) partition by DAY");

            try (RecordCursorFactory factory = select(query)) {
                assertCursor(
                        "a\tb\tk\n" +
                                "66.97969295620055\tVTJW\t1970-02-13T23:33:20.000000Z\n" +
                                "89.98921791869131\tHYRX\t1970-02-18T14:40:00.000000Z\n",
                        factory,
                        true,
                        true
                );
            }
        });
    }

    private void testLatestByMultiColumnPlusFilter(CharSequence ddl) throws Exception {
        assertMemoryLeak(() -> {
            ddl(ddl);

            insert("insert into tab values ('d1', 'c1', 101.1, '2021-10-05T11:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.1, '2021-10-05T11:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.1, '2021-10-05T11:31:35.878Z')");
            insert("insert into tab values ('d1', 'c1', 101.2, '2021-10-05T12:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.2, '2021-10-05T12:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.2, '2021-10-05T12:31:35.878Z')");
            insert("insert into tab values ('d1', 'c1', 101.3, '2021-10-05T13:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.3, '2021-10-05T13:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.3, '2021-10-05T13:31:35.878Z')");
            insert("insert into tab values ('d1', 'c1', 101.4, '2021-10-05T14:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.4, '2021-10-05T14:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 201.4, '2021-10-05T14:31:35.878Z')");
            insert("insert into tab values ('d1', 'c2', 102.5, '2021-10-05T15:31:35.878Z')");
            insert("insert into tab values ('d2', 'c2', 401.1, '2021-10-06T11:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 401.2, '2021-10-06T12:31:35.878Z')");
            insert("insert into tab values ('d2', 'c1', 111.7, '2021-10-06T15:31:35.878Z')");

            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n", "tab where id = 'd1' latest on ts partition by id, name"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d1\tc2\t102.4\t2021-10-05T14:31:35.878000Z\n", "tab where id != 'd2' and value < 102.5 latest on ts partition by id, name"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n", "tab where name = 'c1' latest on ts partition by id, name"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n", "tab where name != 'c2' and value <= 111.7 latest on ts partition by id, name"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n" +
                            "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n", "tab where name = 'c2' latest on ts partition by id"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d1\tc1\t101.4\t2021-10-05T14:31:35.878000Z\n" +
                            "d1\tc2\t102.5\t2021-10-05T15:31:35.878000Z\n", "tab where id = 'd1' latest on ts partition by name"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n" +
                            "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n", "tab where id = 'd2' latest on ts partition by name"
            );
            assertSql(
                    "id\tname\tvalue\tts\n" +
                            "d2\tc2\t401.1\t2021-10-06T11:31:35.878000Z\n" +
                            "d2\tc1\t111.7\t2021-10-06T15:31:35.878000Z\n", "tab where id != 'd1' latest on ts partition by name"
            );
        });
    }

    private void testLatestBySelectAllFilteredBySymbolIn(String ddl) throws Exception {
        assertMemoryLeak(() -> {
            ddl(ddl);

            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node1', 'cpu', 1)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node1', 'cpu', 10)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node1', 'cpu', 100)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node2', 'cpu', 7)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node2', 'cpu', 15)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node2', 'cpu', 75)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node3', 'cpu', 5)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node3', 'cpu', 20)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node3', 'cpu', 25)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node1', 'memory', 20)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node1', 'memory', 200)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node1', 'memory', 2000)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node2', 'memory', 30)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node2', 'memory', 300)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node2', 'memory', 3000)");
            insert("insert into x values ('2021-11-17T17:35:01.000000Z', 'node3', 'memory', 40)");
            insert("insert into x values ('2021-11-17T17:35:02.000000Z', 'node3', 'memory', 400)");
            insert("insert into x values ('2021-11-17T17:35:03.000000Z', 'node3', 'memory', 4000)");

            TestUtils.assertSql(
                    engine,
                    sqlExecutionContext,
                    "select * from x \n" +
                            "where node in ('node2') and metric in ('cpu', 'memory') \n" +
                            "latest on ts partition by node, metric",
                    sink,
                    "ts\tnode\tmetric\tvalue\n" +
                            "2021-11-17T17:35:03.000000Z\tnode2\tcpu\t75\n" +
                            "2021-11-17T17:35:03.000000Z\tnode2\tmemory\t3000\n"
            );
        });
    }

    private void testLatestBySupportedColumnTypes(CharSequence ddl) throws Exception {
        assertMemoryLeak(() -> {
            // supported: [BOOLEAN, CHAR, INT, LONG, LONG256, STRING, SYMBOL]
            ddl(ddl);

            insert("insert into tab values (false, cast(24814 as short), 24814, 8260188555232587029, 0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7, 'J', 'ORANGE', '123', '1970-01-01T00:00:01.000000Z')");
            insert("insert into tab values (true, cast(14817 as short), 14817, 8260188555232587029, 0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9, 'A', 'COCO', 'XoXoX', '1970-01-01T00:00:02.000000Z')");
            insert("insert into tab values (true, cast(14817 as short), 14817, 3614738589890112276, 0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e, 'Q', null, 'XoXoX', '1970-01-01T00:00:03.000000Z')");
            insert("insert into tab values (true, cast(24814 as short), 24814, 3614738589890112276, 0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7, 'J', null, 'XoXoX', '1970-01-01T00:00:04.000000Z')");
            insert("insert into tab values (true, cast(24814 as short), 24814, 8260188555232587029, 0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9, 'Q', 'BANANA', '_(*y*)_', '1970-01-01T00:00:05.000000Z')");
            insert("insert into tab values (false, cast(14817 as short), 14817, 6404066507400987550, 0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e, 'M', null, '123', '1970-01-01T00:00:06.000000Z')");
            insert("insert into tab values (false, cast(14333 as short), 14333, 8260188555232587029, 0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7, 'J', 'COCO', '123', '1970-01-01T00:00:07.000000Z')");
            insert("insert into tab values (false, cast(14817 as short), 14817, 8260188555232587029, 0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9, 'Z', 'BANANA', '_(*y*)_', '1970-01-01T00:00:08.000000Z')");
            insert("insert into tab values (true, cast(24814 as short), 24814, 7759636733976435003, 0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e, 'J', 'ORANGE', '123', '1970-01-01T00:00:09.000000Z')");
            insert("insert into tab values (false, cast(24814 as short), 24814, 6404066507400987550, 0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086, 'W', 'BANANA', '123', '1970-01-01T00:00:10.000000Z')");
            insert("insert into tab values (false, cast(24814 as short), 24814, 6404066507400987550, 0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086, 'W', 'BANANA', '123', '1970-01-02T00:00:01.000000Z')");
            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "true\t24814\t24814\t7759636733976435003\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tJ\tORANGE\t123\t1970-01-01T00:00:09.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by boolean"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "false\t14333\t14333\t8260188555232587029\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\tJ\tCOCO\t123\t1970-01-01T00:00:07.000000Z\n" +
                            "false\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tZ\tBANANA\t_(*y*)_\t1970-01-01T00:00:08.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by short"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "false\t14333\t14333\t8260188555232587029\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\tJ\tCOCO\t123\t1970-01-01T00:00:07.000000Z\n" +
                            "false\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tZ\tBANANA\t_(*y*)_\t1970-01-01T00:00:08.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by int"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "true\t24814\t24814\t3614738589890112276\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\tJ\t\tXoXoX\t1970-01-01T00:00:04.000000Z\n" +
                            "false\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tZ\tBANANA\t_(*y*)_\t1970-01-01T00:00:08.000000Z\n" +
                            "true\t24814\t24814\t7759636733976435003\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tJ\tORANGE\t123\t1970-01-01T00:00:09.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by long"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "false\t14333\t14333\t8260188555232587029\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\tJ\tCOCO\t123\t1970-01-01T00:00:07.000000Z\n" +
                            "false\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tZ\tBANANA\t_(*y*)_\t1970-01-01T00:00:08.000000Z\n" +
                            "true\t24814\t24814\t7759636733976435003\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tJ\tORANGE\t123\t1970-01-01T00:00:09.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by long256"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "true\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tA\tCOCO\tXoXoX\t1970-01-01T00:00:02.000000Z\n" +
                            "true\t24814\t24814\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tQ\tBANANA\t_(*y*)_\t1970-01-01T00:00:05.000000Z\n" +
                            "false\t14817\t14817\t6404066507400987550\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tM\t\t123\t1970-01-01T00:00:06.000000Z\n" +
                            "false\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tZ\tBANANA\t_(*y*)_\t1970-01-01T00:00:08.000000Z\n" +
                            "true\t24814\t24814\t7759636733976435003\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tJ\tORANGE\t123\t1970-01-01T00:00:09.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by char"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "false\t14817\t14817\t6404066507400987550\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tM\t\t123\t1970-01-01T00:00:06.000000Z\n" +
                            "false\t14333\t14333\t8260188555232587029\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\tJ\tCOCO\t123\t1970-01-01T00:00:07.000000Z\n" +
                            "true\t24814\t24814\t7759636733976435003\t0x386129f34be87b5e3990fb6012dac1d3495a30aaa8bf53224e89d27e7ee5104e\tJ\tORANGE\t123\t1970-01-01T00:00:09.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by string"
            );

            expectSqlResult(
                    "boolean\tshort\tint\tlong\tlong256\tchar\tstring\tsymbol\tts\n" +
                            "true\t24814\t24814\t3614738589890112276\t0x7ee65ec7b6e3bc3a422a8855e9d7bfd29199af5c2aa91ba39c022fa261bdede7\tJ\t\tXoXoX\t1970-01-01T00:00:04.000000Z\n" +
                            "false\t14817\t14817\t8260188555232587029\t0x4e1c798ce76392e690c6042566c5a1cda5b9a155686af43ac109ac68336ea0c9\tZ\tBANANA\t_(*y*)_\t1970-01-01T00:00:08.000000Z\n" +
                            "false\t24814\t24814\t6404066507400987550\t0x8b04de5aad1f110fdda84f010e21add4b83e6733ca158dd091627fc790e28086\tW\tBANANA\t123\t1970-01-02T00:00:01.000000Z\n",
                    "tab latest on ts partition by symbol"
            );
        });
    }
}
