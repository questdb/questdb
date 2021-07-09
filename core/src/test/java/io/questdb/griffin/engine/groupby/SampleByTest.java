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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.EmptyRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SampleByTest extends AbstractGriffinTest {
    private final static Log LOG = LogFactory.getLog(SampleByTest.class);

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBadFunction() throws Exception {
        assertFailure("select b, sum(a), sum(c), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                22,
                "Invalid column: c");
    }

    @Test
    public void testGroupByAllTypes() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\n" +
                        "HYRX\t108.41975808143616\t129.3991122184773\t2127224767\t95\t57207\t1696566079386694074\n" +
                        "\t680.7650983333588\t771.0922622028395\t15020424080\t333\t197423\t-5259855777509188759\n" +
                        "CPSW\t101.22759938240051\t111.11358403739061\t2567523370\t33\t43254\t7594916031131877487\n" +
                        "PEHN\t104.29040789604187\t100.8772613783025\t3354324129\t18\t17565\t-4882690809235649274\n" +
                        "RXGZ\t96.40288949012756\t42.02044253932608\t712702244\t46\t22661\t2762535352290012031\n",
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\n" +
                        "HYRX\t108.41975808143616\t129.3991122184773\t2127224767\t95\t57207\t1696566079386694074\n" +
                        "\t779.355800151825\t869.932373151714\t16932485166\t363\t215247\t3597805051091659961\n" +
                        "CPSW\t101.22759938240051\t111.11358403739061\t2567523370\t33\t43254\t7594916031131877487\n" +
                        "PEHN\t104.29040789604187\t100.8772613783025\t3354324129\t18\t17565\t-4882690809235649274\n" +
                        "RXGZ\t96.40288949012756\t42.02044253932608\t712702244\t46\t22661\t2762535352290012031\n" +
                        "ZGHW\t50.258904695510864\t38.42254384471547\t597366062\t21\t23702\t7037372650941669660\n" +
                        "LOPJ\t76.6814649105072\t5.158459929273784\t1920398380\t38\t16628\t3527911398466283309\n" +
                        "VDKF\t4.360640048980713\t35.68111021227658\t503883303\t38\t10895\t7202923278768687325\n" +
                        "OXPK\t45.9206759929657\t76.06252634124596\t2043541236\t21\t19278\t1832315370633201942\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByAllTypesAndInvalidTimestampColumn() throws Exception {
        assertFailure("select \n" +
                        "    LastUpdate, \n" +
                        "    CountryRegion, \n" +
                        "    last(Confirmed) Confirmed, \n" +
                        "    last(Recovered) Recovered, \n" +
                        "    last(Deaths) Deaths \n" +
                        "    from (\n" +
                        "        select \n" +
                        "            LastUpdate, \n" +
                        "            CountryRegion, \n" +
                        "            sum(Confirmed) Confirmed, \n" +
                        "            sum(Recovered) Recovered, \n" +
                        "            sum(Deaths) Deaths\n" +
                        "        from (\n" +
                        "            select \n" +
                        "                LastUpdate, \n" +
                        "                ProvinceState, \n" +
                        "                CountryRegion, \n" +
                        "                last(Confirmed) Confirmed, \n" +
                        "                last(Recovered) Recovered, \n" +
                        "                last(Deaths) Deaths\n" +
                        "            from (covid where CountryRegion in ('China', 'Mainland China'))\n" +
                        "            sample by 1d fill(prev)\n" +
                        "        ) timestamp(xy)\n" +
                        "    ) sample by 1M\n" +
                        ";\n",
                "create table covid as " +
                        "(" +
                        "select" +
                        " rnd_symbol(5,4,4,1) ProvinceState," +
                        " rnd_symbol(5,4,4,1) CountryRegion," +
                        " abs(rnd_int()) Confirmed," +
                        " abs(rnd_int()) Recovered," +
                        " abs(rnd_int()) Deaths," +
                        " timestamp_sequence(172800000000, 3600000000) LastUpdate" +
                        " from" +
                        " long_sequence(1000)" +
                        ") timestamp(LastUpdate) partition by NONE",
                707,
                "Invalid column: xy"
        );
    }

    @Test
    public void testGroupByAllTypesAndInvalidTimestampType() throws Exception {
        assertFailure("select \n" +
                        "    LastUpdate, \n" +
                        "    CountryRegion, \n" +
                        "    last(Confirmed) Confirmed, \n" +
                        "    last(Recovered) Recovered, \n" +
                        "    last(Deaths) Deaths \n" +
                        "    from (\n" +
                        "        select \n" +
                        "            LastUpdate, \n" +
                        "            CountryRegion, \n" +
                        "            sum(Confirmed) Confirmed, \n" +
                        "            sum(Recovered) Recovered, \n" +
                        "            sum(Deaths) Deaths\n" +
                        "        from (\n" +
                        "            select \n" +
                        "                LastUpdate, \n" +
                        "                ProvinceState, \n" +
                        "                CountryRegion, \n" +
                        "                last(Confirmed) Confirmed, \n" +
                        "                last(Recovered) Recovered, \n" +
                        "                last(Deaths) Deaths\n" +
                        "            from (covid where CountryRegion in ('China', 'Mainland China'))\n" +
                        "            sample by 1d fill(prev)\n" +
                        "        ) timestamp(ProvinceState)\n" +
                        "    ) sample by 1M\n" +
                        ";\n",
                "create table covid as " +
                        "(" +
                        "select" +
                        " rnd_symbol(5,4,4,1) ProvinceState," +
                        " rnd_symbol(5,4,4,1) CountryRegion," +
                        " abs(rnd_int()) Confirmed," +
                        " abs(rnd_int()) Recovered," +
                        " abs(rnd_int()) Deaths," +
                        " timestamp_sequence(172800000000, 3600000000) LastUpdate" +
                        " from" +
                        " long_sequence(1000)" +
                        ") timestamp(LastUpdate) partition by NONE",
                707,
                "not a TIMESTAMP"
        );
    }

    @Test
    public void testGroupByAllTypesAndTimestampSameLevel() throws Exception {
        assertQuery("k\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\n" +
                        "1970-01-03T00:00:00.000000Z\t11.427980661392212\t42.17768841969397\t426455968\t42\t4924\t4086802474270249591\n" +
                        "1970-01-03T01:00:00.000000Z\t42.24356412887573\t70.94360487171201\t1631244228\t50\t10900\t8349358446893356086\n" +
                        "1970-01-03T02:00:00.000000Z\t33.60825181007385\t76.75673070796104\t422941535\t27\t32312\t4442449726822927731\n" +
                        "1970-01-03T03:00:00.000000Z\t81.46807551383972\t12.503042190293423\t2085282008\t9\t11472\t8955092533521658248\n" +
                        "1970-01-03T04:00:00.000000Z\t67.61934757232666\t34.35685332942956\t2144581835\t6\t10942\t3152466304308949756\n" +
                        "1970-01-03T05:00:00.000000Z\t41.38164520263672\t55.22494170511608\t667031149\t38\t22298\t5536695302686527374\n" +
                        "1970-01-03T06:00:00.000000Z\t97.50198721885681\t0.11075361080621349\t1515787781\t49\t19013\t7316123607359392486\n" +
                        "1970-01-03T07:00:00.000000Z\t4.14280891418457\t92.050039469858\t1299391311\t31\t19997\t4091897709796604687\n" +
                        "1970-01-03T08:00:00.000000Z\t22.82233238220215\t88.37421918800908\t1269042121\t9\t6093\t4608960730952244094\n" +
                        "1970-01-03T09:00:00.000000Z\t72.30015397071838\t12.105630273556178\t572338288\t28\t24397\t8081265393416742311\n" +
                        "1970-01-03T10:00:00.000000Z\t81.64182305335999\t91.0141759290032\t1609750740\t3\t14377\t6161552193869048721\n" +
                        "1970-01-03T11:00:00.000000Z\t96.40288949012756\t42.02044253932608\t712702244\t46\t22661\t2762535352290012031\n" +
                        "1970-01-03T12:00:00.000000Z\t67.52509474754333\t95.40069089049732\t865832060\t48\t1315\t9063592617902736531\n" +
                        "1970-01-03T13:00:00.000000Z\t14.830547571182251\t94.41658975532606\t2043803188\t6\t1464\t9144172287200792483\n" +
                        "1970-01-03T14:00:00.000000Z\t57.97446966171265\t76.57837745299521\t462277692\t40\t21561\t9143800334706665900\n" +
                        "1970-01-03T15:00:00.000000Z\t39.0173077583313\t10.643046345788132\t1238491107\t13\t30722\t6912707344119330199\n" +
                        "1970-01-03T16:00:00.000000Z\t48.92743229866028\t82.31249461985348\t805434743\t31\t18600\t6187389706549636253\n" +
                        "1970-01-03T17:00:00.000000Z\t58.93397927284241\t56.99444693578853\t1311366306\t9\t27078\t8755128364143858197\n" +
                        "1970-01-03T18:00:00.000000Z\t65.40474891662598\t86.7718184863495\t593242882\t6\t23251\t5292387498953709416\n" +
                        "1970-01-03T19:00:00.000000Z\t85.93131303787231\t33.74707565497281\t2105201404\t34\t14733\t8994301462266164776\n",
                "(select k, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g) from x) timestamp(k)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "k\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\n" +
                        "1970-01-03T00:00:00.000000Z\t11.427980661392212\t42.17768841969397\t426455968\t42\t4924\t4086802474270249591\n" +
                        "1970-01-03T01:00:00.000000Z\t42.24356412887573\t70.94360487171201\t1631244228\t50\t10900\t8349358446893356086\n" +
                        "1970-01-03T02:00:00.000000Z\t33.60825181007385\t76.75673070796104\t422941535\t27\t32312\t4442449726822927731\n" +
                        "1970-01-03T03:00:00.000000Z\t81.46807551383972\t12.503042190293423\t2085282008\t9\t11472\t8955092533521658248\n" +
                        "1970-01-03T04:00:00.000000Z\t67.61934757232666\t34.35685332942956\t2144581835\t6\t10942\t3152466304308949756\n" +
                        "1970-01-03T05:00:00.000000Z\t41.38164520263672\t55.22494170511608\t667031149\t38\t22298\t5536695302686527374\n" +
                        "1970-01-03T06:00:00.000000Z\t97.50198721885681\t0.11075361080621349\t1515787781\t49\t19013\t7316123607359392486\n" +
                        "1970-01-03T07:00:00.000000Z\t4.14280891418457\t92.050039469858\t1299391311\t31\t19997\t4091897709796604687\n" +
                        "1970-01-03T08:00:00.000000Z\t22.82233238220215\t88.37421918800908\t1269042121\t9\t6093\t4608960730952244094\n" +
                        "1970-01-03T09:00:00.000000Z\t72.30015397071838\t12.105630273556178\t572338288\t28\t24397\t8081265393416742311\n" +
                        "1970-01-03T10:00:00.000000Z\t81.64182305335999\t91.0141759290032\t1609750740\t3\t14377\t6161552193869048721\n" +
                        "1970-01-03T11:00:00.000000Z\t96.40288949012756\t42.02044253932608\t712702244\t46\t22661\t2762535352290012031\n" +
                        "1970-01-03T12:00:00.000000Z\t67.52509474754333\t95.40069089049732\t865832060\t48\t1315\t9063592617902736531\n" +
                        "1970-01-03T13:00:00.000000Z\t14.830547571182251\t94.41658975532606\t2043803188\t6\t1464\t9144172287200792483\n" +
                        "1970-01-03T14:00:00.000000Z\t57.97446966171265\t76.57837745299521\t462277692\t40\t21561\t9143800334706665900\n" +
                        "1970-01-03T15:00:00.000000Z\t39.0173077583313\t10.643046345788132\t1238491107\t13\t30722\t6912707344119330199\n" +
                        "1970-01-03T16:00:00.000000Z\t48.92743229866028\t82.31249461985348\t805434743\t31\t18600\t6187389706549636253\n" +
                        "1970-01-03T17:00:00.000000Z\t58.93397927284241\t56.99444693578853\t1311366306\t9\t27078\t8755128364143858197\n" +
                        "1970-01-03T18:00:00.000000Z\t65.40474891662598\t86.7718184863495\t593242882\t6\t23251\t5292387498953709416\n" +
                        "1970-01-03T19:00:00.000000Z\t85.93131303787231\t33.74707565497281\t2105201404\t34\t14733\t8994301462266164776\n" +
                        "1970-01-04T05:00:00.000000Z\t98.59070181846619\t98.8401109488745\t1912061086\t30\t17824\t8857660828600848720\n" +
                        "1970-01-04T06:00:00.000000Z\t50.258904695510864\t38.42254384471547\t597366062\t21\t23702\t7037372650941669660\n" +
                        "1970-01-04T07:00:00.000000Z\t76.6814649105072\t5.158459929273784\t1920398380\t38\t16628\t3527911398466283309\n" +
                        "1970-01-04T08:00:00.000000Z\t4.360640048980713\t35.68111021227658\t503883303\t38\t10895\t7202923278768687325\n" +
                        "1970-01-04T09:00:00.000000Z\t45.9206759929657\t76.06252634124596\t2043541236\t21\t19278\t1832315370633201942\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByCount() throws Exception {
        assertQuery("c\tcount\n" +
                        "\t5\n" +
                        "UU\t4\n" +
                        "XY\t6\n" +
                        "ZP\t5\n",
                "select c, count() from x order by c",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tcount\n" +
                        "\t5\n" +
                        "KK\t1\n" +
                        "PL\t4\n" +
                        "UU\t4\n" +
                        "XY\t6\n" +
                        "ZP\t5\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByCountFromSubQuery() throws Exception {
        assertQuery("c\tcount\n" +
                        "UU\t1\n" +
                        "XY\t1\n" +
                        "ZP\t1\n" +
                        "\t1\n",
                "select c, count() from (x latest by c)",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tcount\n" +
                        "UU\t1\n" +
                        "XY\t1\n" +
                        "ZP\t1\n" +
                        "\t1\n" +
                        "KK\t1\n" +
                        "PL\t1\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByEmpty() throws Exception {
        assertQuery("c\tsum_t\n",
                "select c, sum_t(d) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(0)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tsum_t\n" +
                        "PL\t1.088880189118224\n" +
                        "KK\t2.614956708935964\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByFail() throws Exception {
        assertMemoryLeak(() -> {

            compiler.compile("create table x as " +
                            "(" +
                            "select" +
                            " x," +
                            " rnd_double(0) d," +
                            " rnd_symbol('XY','ZP', null, 'UU') c" +
                            " from" +
                            " long_sequence(1000000)" +
                            ")",
                    sqlExecutionContext
            );

            engine.clear();

            final FilesFacade ff = new FilesFacadeImpl() {
                int count = 10;

                @Override
                public long mmap(long fd, long len, long offset, int flags) {
                    if (count-- > 0) {
                        return super.mmap(fd, len, offset, flags);
                    }
                    return -1;
                }
            };

            final CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (SqlCompiler compiler = new SqlCompiler(engine)) {
                    try {
                        try (RecordCursorFactory factory = compiler.compile("select c, sum_t(d) from x", sqlExecutionContext).getRecordCursorFactory()) {
                            factory.getCursor(sqlExecutionContext);
                        }
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Could not mmap");
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
                engine.clear();
            }
        });
    }

    @Test
    public void testGroupByFreesFunctions() throws Exception {
        assertQuery("c\tsum_t\n" +
                        "UU\t4.192763851971972\n" +
                        "XY\t5.326379743132296\n" +
                        "\t1.8586710189229834\n" +
                        "ZP\t0.7836635625207334\n",
                "select c, sum_t(d) from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                "insert into x select * from (" +
                        "select" +
                        " x," +
                        " rnd_double(0) d," +
                        " rnd_symbol('KK', 'PL') c" +
                        " from" +
                        " long_sequence(5)" +
                        ")",
                "c\tsum_t\n" +
                        "UU\t4.192763851971972\n" +
                        "XY\t5.326379743132296\n" +
                        "\t1.8586710189229834\n" +
                        "ZP\t0.7836635625207334\n" +
                        "KK\t1.6435699091508287\n" +
                        "PL\t1.1627169669458202\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testGroupByRandomAccessConsistency() throws Exception {
        assertQuery("c\tcount\n" +
                        "XY\t6\n" +
                        "ZP\t5\n",
                "select c, count() count from (x where c = 'ZP' union all x where c = 'XY') order by 1, 2",
                "create table x as " +
                        "(" +
                        "select" +
                        " x," +
                        " rnd_symbol('XY','ZP', null, 'UU') c" +
                        " from" +
                        " long_sequence(20)" +
                        ")",
                null,
                true,
                true,
                true
        );
    }

    @Test
    public void testIndexSampleBy() throws Exception {
        assertQuery("k\ts\tlat\tlon\n" +
                        "1970-01-04T00:26:40.000000Z\ta\t70.00560222114518\t168.04971262491318\n" +
                        "1970-01-04T01:26:40.000000Z\ta\t6.612327943200507\t151.3046788842135\n" +
                        "1970-01-04T02:26:40.000000Z\ta\t117.11888283070247\tNaN\n" +
                        "1970-01-04T03:26:40.000000Z\ta\t99.02039650915859\t128.42101395467057\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from x " +
                        "where k > '1970-01-04' and s in ('a') " +
                        "sample by 1h",
                "create table x as " +
                        "(" +
                        "select" +
                        "   rnd_double(1)*180 lat," +
                        "   rnd_double(1)*180 lon," +
                        "   rnd_symbol('a','b',null) s," +
                        "   timestamp_sequence(172800000000, 1000000000) k" +
                        "   from" +
                        "   long_sequence(100)" +
                        "), index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false);
    }

    @Test
    public void testIndexSampleByWithArithmetics() throws Exception {
        assertQuery("k\ts\tlat\tlon\tconst\n" +
                        "1970-01-04T00:26:40.000000Z\ta\t71.00560222114518\t336.09942524982637\t1\n" +
                        "1970-01-04T01:26:40.000000Z\ta\t7.612327943200507\t302.609357768427\t1\n" +
                        "1970-01-04T02:26:40.000000Z\ta\t118.11888283070247\tNaN\t1\n" +
                        "1970-01-04T03:26:40.000000Z\ta\t100.02039650915859\t256.84202790934114\t1\n",
                "select k, s, first(lat) + 1 lat, last(lon) * 2 lon, 1 as const " +
                        "from x " +
                        "where k > '1970-01-04' and s in ('a') " +
                        "sample by 1h",
                "create table x as " +
                        "(" +
                        "select" +
                        "   rnd_double(1)*180 lat," +
                        "   rnd_double(1)*180 lon," +
                        "   rnd_symbol('a','b',null) s," +
                        "   timestamp_sequence(172800000000, 1000000000) k" +
                        "   from" +
                        "   long_sequence(100)" +
                        "), index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false);
    }

    @Test
    public void testIndexSampleByWithInvalidFunctionArgs() throws Exception {
        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-04T00:26:40.000000Z\ta\t71.00560222114518\t336.09942524982637\n" +
                        "1970-01-04T01:26:40.000000Z\ta\t7.612327943200507\t302.609357768427\n" +
                        "1970-01-04T02:26:40.000000Z\ta\t118.11888283070247\tNaN\n" +
                        "1970-01-04T03:26:40.000000Z\ta\t100.02039650915859\t256.84202790934114\n",
                "select k, s, first(lat + 1) lat, last(lon * 2) lon " +
                        "from x " +
                        "where k > '1970-01-04' and s in ('a') " +
                        "sample by 1h",
                "create table x as " +
                        "(" +
                        "select" +
                        "   rnd_double(1)*180 lat," +
                        "   rnd_double(1)*180 lon," +
                        "   rnd_symbol('a','b',null) s," +
                        "   timestamp_sequence(172800000000, 1000000000) k" +
                        "   from" +
                        "   long_sequence(100)" +
                        "), index(s capacity 10) timestamp(k) partition by DAY",
                false);
    }

    @Test
    public void testIndexSampleByWithInvalidFunctionArgs2() throws Exception {
        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-04T00:26:40.000000Z\ta\t70.00560222114518\t1\n" +
                        "1970-01-04T01:26:40.000000Z\ta\t6.612327943200507\t1\n" +
                        "1970-01-04T02:26:40.000000Z\ta\t117.11888283070247\t1\n" +
                        "1970-01-04T03:26:40.000000Z\ta\t99.02039650915859\t1\n",
                "select k, s, first(lat) lat, last(1) lon " +
                        "from x " +
                        "where k > '1970-01-04' and s in ('a') " +
                        "sample by 1h",
                "create table x as " +
                        "(" +
                        "select" +
                        "   rnd_double(1)*180 lat," +
                        "   rnd_double(1)*180 lon," +
                        "   rnd_symbol('a','b',null) s," +
                        "   timestamp_sequence(172800000000, 1000000000) k" +
                        "   from" +
                        "   long_sequence(100)" +
                        "), index(s capacity 10) timestamp(k) partition by DAY",
                false);
    }

    @Test
    public void testIndexSampleBySameTimePoints() throws Exception {
        assertQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:00:00.000000Z\ta\t1\t58\n" +
                        "1970-01-01T01:00:00.000000Z\ta\t63\t116\n" +
                        "1970-01-01T02:00:00.000000Z\ta\t126\t178\n" +
                        "1970-01-01T03:00:00.000000Z\ta\t184\t238\n" +
                        "1970-01-01T04:00:00.000000Z\ta\t240\t299\n",
                "select k, s, first(lat) lat, last(lat) lon " +
                        "from x " +
                        "where k between '1970-01-01' and '1970-01-01T04:00' and s in ('a') " +
                        "sample by 1h",
                "create table x as " +
                        "(" +
                        "select" +
                        "   x lat," +
                        "   x lon," +
                        "   rnd_symbol('a','b',null) s," +
                        "   cast(((x / 60L) * 1000000L * 60L * 60L) as timestamp) k" +
                        "   from" +
                        "   long_sequence(25*60)" +
                        "), index(s) timestamp(k) partition by DAY",
                "k",
                false);
    }

    @Test
    public void testIndexSampleBy2() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s in ('a')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 256) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:20:00.000000Z\tb\t-3.0\t7.0\n" +
                        "1970-01-01T01:20:00.000000Z\tb\t-9.0\t13.0\n" +
                        "1970-01-01T02:20:00.000000Z\tb\t-15.0\t19.0\n" +
                        "1970-01-01T03:20:00.000000Z\tb\t-21.0\t25.0\n" +
                        "1970-01-01T04:20:00.000000Z\tb\t-27.0\t31.0\n" +
                        "1970-01-01T05:20:00.000000Z\tb\t-33.0\t37.0\n" +
                        "1970-01-01T06:20:00.000000Z\tb\t-39.0\t43.0\n" +
                        "1970-01-01T07:20:00.000000Z\tb\t-45.0\t49.0\n" +
                        "1970-01-01T08:20:00.000000Z\tb\t-51.0\t55.0\n" +
                        "1970-01-01T09:20:00.000000Z\tb\t-57.0\t61.0\n" +
                        "1970-01-01T10:20:00.000000Z\tb\t-63.0\t67.0\n" +
                        "1970-01-01T11:20:00.000000Z\tb\t-69.0\t73.0\n" +
                        "1970-01-01T12:20:00.000000Z\tb\t-75.0\t79.0\n" +
                        "1970-01-01T13:20:00.000000Z\tb\t-81.0\t85.0\n" +
                        "1970-01-01T14:20:00.000000Z\tb\t-87.0\t91.0\n" +
                        "1970-01-01T15:20:00.000000Z\tb\t-93.0\t97.0\n" +
                        "1970-01-01T16:20:00.000000Z\tb\t-99.0\t103.0\n" +
                        "1970-01-01T17:20:00.000000Z\tb\t-105.0\t109.0\n" +
                        "1970-01-01T18:20:00.000000Z\tb\t-111.0\t115.0\n" +
                        "1970-01-01T19:20:00.000000Z\tb\t-117.0\t121.0\n" +
                        "1970-01-01T20:20:00.000000Z\tb\t-123.0\t127.0\n" +
                        "1970-01-01T21:20:00.000000Z\tb\t-129.0\t133.0\n" +
                        "1970-01-01T22:20:00.000000Z\tb\t-135.0\t139.0\n" +
                        "1970-01-01T23:20:00.000000Z\tb\t-141.0\t145.0\n" +
                        "1970-01-02T00:20:00.000000Z\tb\t-147.0\t149.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k > '1970-01-01' and s in ('b')" +
                        "sample by 1h",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 10 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(150)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:10:00.000000Z\t\t-2.0\t13.0\n" +
                        "1970-01-01T02:10:00.000000Z\t\t-14.0\t25.0\n" +
                        "1970-01-01T04:10:00.000000Z\t\t-26.0\t37.0\n" +
                        "1970-01-01T06:10:00.000000Z\t\t-38.0\t49.0\n" +
                        "1970-01-01T08:10:00.000000Z\t\t-50.0\t61.0\n" +
                        "1970-01-01T10:10:00.000000Z\t\t-62.0\t73.0\n" +
                        "1970-01-01T12:10:00.000000Z\t\t-74.0\t85.0\n" +
                        "1970-01-01T14:10:00.000000Z\t\t-86.0\t97.0\n" +
                        "1970-01-01T16:10:00.000000Z\t\t-98.0\t109.0\n" +
                        "1970-01-01T18:10:00.000000Z\t\t-110.0\t121.0\n" +
                        "1970-01-01T20:10:00.000000Z\t\t-122.0\t133.0\n" +
                        "1970-01-01T22:10:00.000000Z\t\t-134.0\t145.0\n" +
                        "1970-01-02T00:10:00.000000Z\t\t-146.0\t150.0\n",
                "select k, s, first(lat) lat, last(lon) lon \n" +
                        "from xx \n" +
                        "where k > '1970-01-01' and s = null \n" +
                        "sample by 2h");
    }

    @Test
    public void testIndexSampleBy3() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s in ('a')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 256) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T21:10:00.000000Z\ta\t-128.0\t128.0\n" +
                        "1970-01-01T23:10:00.000000Z\ta\t-140.0\t140.0\n" +
                        "1970-01-02T01:10:00.000000Z\ta\t-152.0\t152.0\n" +
                        "1970-01-02T03:10:00.000000Z\ta\t-164.0\t164.0\n" +
                        "1970-01-02T05:10:00.000000Z\ta\t-176.0\t176.0\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k > '1970-01-01T21:00' and s in ('a')" +
                        "sample by 2h",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 10 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(180)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-01-01T21:10:00.000000Z\t\t-128.0\t128.0\n" +
                        "1970-01-01T23:10:00.000000Z\t\t-140.0\t140.0\n" +
                        "1970-01-02T01:10:00.000000Z\t\t-152.0\t152.0\n" +
                        "1970-01-02T03:10:00.000000Z\t\t-164.0\t164.0\n" +
                        "1970-01-02T05:10:00.000000Z\t\t-176.0\t176.0\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k > '1970-01-01T21:00' and s = null " +
                        "sample by 2h");
    }

    @Test
    public void testIndexSampleByBufferExceeded() throws Exception {
        sampleByIndexSearchPageSize = 16;

        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where s in ('a')" +
                        "sample by 60s",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 4096) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:01:00.000000Z\ta\t-2.0\t2.0\n" +
                        "1970-01-01T00:03:00.000000Z\ta\t-4.0\t4.0\n" +
                        "1970-01-01T00:05:00.000000Z\ta\t-6.0\t6.0\n" +
                        "1970-01-01T00:07:00.000000Z\ta\t-8.0\t8.0\n" +
                        "1970-01-01T00:09:00.000000Z\ta\t-10.0\t10.0\n" +
                        "1970-01-01T00:11:00.000000Z\ta\t-12.0\t12.0\n" +
                        "1970-01-01T00:13:00.000000Z\ta\t-14.0\t14.0\n" +
                        "1970-01-01T00:15:00.000000Z\ta\t-16.0\t16.0\n" +
                        "1970-01-01T00:17:00.000000Z\ta\t-18.0\t18.0\n" +
                        "1970-01-01T00:19:00.000000Z\ta\t-20.0\t20.0\n" +
                        "1970-01-01T00:21:00.000000Z\ta\t-22.0\t22.0\n" +
                        "1970-01-01T00:23:00.000000Z\ta\t-24.0\t24.0\n" +
                        "1970-01-01T00:25:00.000000Z\ta\t-26.0\t26.0\n" +
                        "1970-01-01T00:27:00.000000Z\ta\t-28.0\t28.0\n" +
                        "1970-01-01T00:29:00.000000Z\ta\t-30.0\t30.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where s in ('a')" +
                        "sample by 2m",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 60 * 1000L * 1000L) k\n" +
                        "from\n" +
                        "long_sequence(30)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:00:00.000000Z\t\t-1.0\t10.0\n" +
                        "1970-01-01T00:10:00.000000Z\t\t-11.0\t20.0\n" +
                        "1970-01-01T00:20:00.000000Z\t\t-21.0\t30.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where s = null " +
                        "sample by 10m");
    }

    @Test
    public void testIndexSampleByEmpty() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k > '2000-01-04' and s in ('a') " +
                        "sample by 1h",
                "create table xx as " +
                        "(" +
                        "select" +
                        "   rnd_double(1)*180 lat," +
                        "   rnd_double(1)*180 lon," +
                        "   rnd_symbol('a','b',null) s," +
                        "   timestamp_sequence(172800000000, 1000000000) k" +
                        "   from" +
                        "   long_sequence(100)" +
                        "), index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                false);

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k > '2000-01-04' and s = null " +
                        "sample by 1h");
    }

    @Test
    public void testIndexSampleByFirstAndLast() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and s in ('b')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-02-01T00:00:00.000000Z\tb\t-745.0\t767.0\n" +
                        "1970-02-02T00:00:00.000000Z\tb\t-769.0\t791.0\n" +
                        "1970-02-03T00:00:00.000000Z\tb\t-793.0\t815.0\n" +
                        "1970-02-04T00:00:00.000000Z\tb\t-817.0\t839.0\n" +
                        "1970-02-05T00:00:00.000000Z\tb\t-841.0\t863.0\n" +
                        "1970-02-06T00:00:00.000000Z\tb\t-865.0\t887.0\n" +
                        "1970-02-07T00:00:00.000000Z\tb\t-889.0\t911.0\n" +
                        "1970-02-08T00:00:00.000000Z\tb\t-913.0\t935.0\n" +
                        "1970-02-09T00:00:00.000000Z\tb\t-937.0\t959.0\n" +
                        "1970-02-10T00:00:00.000000Z\tb\t-961.0\t983.0\n" +
                        "1970-02-11T00:00:00.000000Z\tb\t-985.0\t1007.0\n" +
                        "1970-02-12T00:00:00.000000Z\tb\t-1009.0\t1031.0\n" +
                        "1970-02-13T00:00:00.000000Z\tb\t-1033.0\t1055.0\n" +
                        "1970-02-14T00:00:00.000000Z\tb\t-1057.0\t1079.0\n" +
                        "1970-02-15T00:00:00.000000Z\tb\t-1081.0\t1103.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and k < '1970-02-16' and s in ('b')" +
                        "sample by 1d",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 60 * 60 * 1000000L) k\n" + // 60 mins
                        "from\n" +
                        "long_sequence(365 * 24)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-02-01T00:00:00.000000Z\t\t-745.0\t816.0\n" +
                        "1970-02-04T00:00:00.000000Z\t\t-817.0\t888.0\n" +
                        "1970-02-07T00:00:00.000000Z\t\t-889.0\t960.0\n" +
                        "1970-02-10T00:00:00.000000Z\t\t-961.0\t1032.0\n" +
                        "1970-02-13T00:00:00.000000Z\t\t-1033.0\t1104.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and k < '1970-02-16' and s = null " +
                        "sample by 3d");
    }

    @Test
    public void testIndexSampleByIndexFrameExceedsDataFrame() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s in ('a')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 256) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:10:00.000000Z\ta\t-2.0\t2.0\n" +
                        "1970-01-01T04:10:00.000000Z\ta\t-32.0\t32.0\n" +
                        "1970-01-01T10:10:00.000000Z\ta\t-62.0\t62.0\n" +
                        "1970-01-01T14:10:00.000000Z\ta\t-92.0\t92.0\n" +
                        "1970-01-01T20:10:00.000000Z\ta\t-122.0\t122.0\n" +
                        "1970-01-02T00:10:00.000000Z\ta\t-152.0\t152.0\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s in ('a', 'none')" +
                        "sample by 2h",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 10 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(180)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:00:00.000000Z\t\t-1.0\t4.0\n" +
                        "1970-01-01T04:00:00.000000Z\t\t-31.0\t34.0\n" +
                        "1970-01-01T10:00:00.000000Z\t\t-61.0\t64.0\n" +
                        "1970-01-01T14:00:00.000000Z\t\t-91.0\t94.0\n" +
                        "1970-01-01T20:00:00.000000Z\t\t-121.0\t124.0\n" +
                        "1970-01-02T00:00:00.000000Z\t\t-151.0\t154.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s = null " +
                        "sample by 2h");
    }

    @Test
    public void testIndexSampleByIndexNoTimestampColSelected() throws Exception {
        assertMemoryLeak(() -> compiler.compile("create table xx (lat double, lon double, s symbol, k timestamp)" +
                ", index(s capacity 256) timestamp(k) partition by DAY", sqlExecutionContext));

        assertQuery("s\tlat\tlon\n" +
                        "a\t-2.0\t2.0\n" +
                        "a\t-32.0\t32.0\n" +
                        "a\t-62.0\t62.0\n" +
                        "a\t-92.0\t92.0\n" +
                        "a\t-122.0\t122.0\n" +
                        "a\t-152.0\t152.0\n",
                "select s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s in ('a')" +
                        "sample by 2h",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 10 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(180)\n",
                null,
                false,
                false,
                false);

        assertMemoryLeak(() -> {
            compiler.compile("alter table xx drop column s", sqlExecutionContext);
            compiler.compile("alter table xx add s SYMBOL INDEX", sqlExecutionContext);
        });

        TestUtils.assertSqlCursors(compiler,
                sqlExecutionContext,
                "select s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10'" +
                        "sample by 2h",
                "select s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s = null " +
                        "sample by 2h",
                LOG);
    }

    @Test
    public void testIndexSampleByIndexWithIrregularEmptyPeriods() throws Exception {
        assertMemoryLeak(() -> compiler.compile("create table xx (s symbol, k timestamp)" +
                ", index(s capacity 256) timestamp(k) partition by DAY", sqlExecutionContext));

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T20:50:00.000000Z\ta\t1970-01-01T20:50:00.000000Z\t1970-01-01T21:30:00.000000Z\n" +
                        "1970-01-01T21:50:00.000000Z\ta\t1970-01-01T21:50:00.000000Z\t1970-01-01T21:50:00.000000Z\n" +
                        "1970-01-01T23:50:00.000000Z\ta\t1970-01-02T00:30:00.000000Z\t1970-01-02T00:30:00.000000Z\n" +
                        "1970-01-02T00:50:00.000000Z\ta\t1970-01-02T00:50:00.000000Z\t1970-01-02T01:10:00.000000Z\n" +
                        "1970-01-02T03:50:00.000000Z\ta\t1970-01-02T03:50:00.000000Z\t1970-01-02T03:50:00.000000Z\n",
                "select k, s, first(k) lat, last(k) lon " +
                        "from xx " +
                        "where k between '1970-01-01T20:00' and '1970-01-02T04:00' and s in ('a')" +
                        "sample by 1h",
                "insert into xx " +
                        "select " +
                        "(case when (x / 7) % 3 = 0 and x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 10 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(360)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-01-01T20:00:00.000000Z\t\t1970-01-01T20:00:00.000000Z\t1970-01-01T21:50:00.000000Z\n" +
                        "1970-01-01T22:00:00.000000Z\t\t1970-01-01T22:00:00.000000Z\t1970-01-01T23:50:00.000000Z\n" +
                        "1970-01-02T00:00:00.000000Z\t\t1970-01-02T00:00:00.000000Z\t1970-01-02T01:50:00.000000Z\n" +
                        "1970-01-02T02:00:00.000000Z\t\t1970-01-02T02:00:00.000000Z\t1970-01-02T03:50:00.000000Z\n" +
                        "1970-01-02T04:00:00.000000Z\t\t1970-01-02T04:00:00.000000Z\t1970-01-02T04:00:00.000000Z\n",
                "select k, s, first(k) lat, last(k) lon " +
                        "from xx " +
                        "where k between '1970-01-01T20:00' and '1970-01-02T04:00' and s = null " +
                        "sample by 2h");
    }

    @Test
    public void testIndexSampleByLastAndFirstOnDifferentIndexPages() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01T00:00:00.000000Z;30m;5h;10' and s in ('a')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T21:10:00.000000Z\ta\t-128.0\t138.0\n" +
                        "1970-01-01T23:10:00.000000Z\ta\t-140.0\t150.0\n" +
                        "1970-01-02T01:10:00.000000Z\ta\t-152.0\t162.0\n" +
                        "1970-01-02T03:10:00.000000Z\ta\t-164.0\t174.0\n" +
                        "1970-01-02T05:10:00.000000Z\ta\t-176.0\t180.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k > '1970-01-01T21:00' and s in ('a')" +
                        "sample by 2h",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 10 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(180)\n");

        assertWithSymbolColumnTop("k\ts\tlat\tlon\n" +
                        "1970-01-01T21:10:00.000000Z\t\t-128.0\t139.0\n" +
                        "1970-01-01T23:10:00.000000Z\t\t-140.0\t151.0\n" +
                        "1970-01-02T01:10:00.000000Z\t\t-152.0\t163.0\n" +
                        "1970-01-02T03:10:00.000000Z\t\t-164.0\t175.0\n" +
                        "1970-01-02T05:10:00.000000Z\t\t-176.0\t180.0\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k > '1970-01-01T21:00' and s = null " +
                        "sample by 2h");
    }

    @Test
    public void testIndexSampleByManyPartitions() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and s in ('b')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-02-01T00:00:00.000000Z\tb\t-745.0\t745.0\n" +
                        "1970-02-02T00:00:00.000000Z\tb\t-769.0\t769.0\n" +
                        "1970-02-03T00:00:00.000000Z\tb\t-793.0\t793.0\n" +
                        "1970-02-04T00:00:00.000000Z\tb\t-817.0\t817.0\n" +
                        "1970-02-05T00:00:00.000000Z\tb\t-841.0\t841.0\n" +
                        "1970-02-06T00:00:00.000000Z\tb\t-865.0\t865.0\n" +
                        "1970-02-07T00:00:00.000000Z\tb\t-889.0\t889.0\n" +
                        "1970-02-08T00:00:00.000000Z\tb\t-913.0\t913.0\n" +
                        "1970-02-09T00:00:00.000000Z\tb\t-937.0\t937.0\n" +
                        "1970-02-10T00:00:00.000000Z\tb\t-961.0\t961.0\n" +
                        "1970-02-11T00:00:00.000000Z\tb\t-985.0\t985.0\n" +
                        "1970-02-12T00:00:00.000000Z\tb\t-1009.0\t1009.0\n" +
                        "1970-02-13T00:00:00.000000Z\tb\t-1033.0\t1033.0\n" +
                        "1970-02-14T00:00:00.000000Z\tb\t-1057.0\t1057.0\n" +
                        "1970-02-15T00:00:00.000000Z\tb\t-1081.0\t1081.0\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and k < '1970-02-16' and s in ('b')" +
                        "sample by 1d",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 60 * 60 * 1000000L) k\n" + // 60 mins
                        "from\n" +
                        "long_sequence(365 * 24)\n");
    }

    @Test
    public void testIndexSampleByMicro() throws Exception {
        sampleByIndexSearchPageSize = 256;
        assertSampleByIndexQuery(
                "k\tfirst\n" +
                        "2021-01-01T00:07:39.760000Z\t15318\n" +
                        "2021-01-01T00:07:40.560000Z\t15341\n" +
                        "2021-01-01T00:07:40.970000Z\t15355\n" +
                        "2021-01-01T00:07:41.090000Z\t15359\n" +
                        "2021-01-01T00:07:42.600000Z\t15410\n" +
                        "2021-01-01T00:07:42.890000Z\t15420\n" +
                        "2021-01-01T00:07:43.080000Z\t15425\n" +
                        "2021-01-01T00:07:43.400000Z\t15436\n" +
                        "2021-01-01T00:07:43.440000Z\t15437\n" +
                        "2021-01-01T00:07:43.520000Z\t15439\n" +
                        "2021-01-01T00:07:43.550000Z\t15440\n" +
                        "2021-01-01T00:07:43.980000Z\t15458\n" +
                        "2021-01-01T00:07:45.170000Z\t15497\n" +
                        "2021-01-01T00:07:45.250000Z\t15500\n" +
                        "2021-01-01T00:07:45.660000Z\t15513\n" +
                        "2021-01-01T00:07:46.440000Z\t15539\n" +
                        "2021-01-01T00:07:46.640000Z\t15544\n" +
                        "2021-01-01T00:07:48.000000Z\t15587\n" +
                        "2021-01-01T00:07:49.010000Z\t15620\n" +
                        "2021-01-01T00:07:49.240000Z\t15627\n" +
                        "2021-01-01T00:07:49.520000Z\t15636\n" +
                        "2021-01-01T00:07:49.620000Z\t15639\n" +
                        "2021-01-01T00:07:49.800000Z\t15647\n" +
                        "2021-01-01T00:07:50.290000Z\t15665\n" +
                        "2021-01-01T00:07:51.360000Z\t15699\n" +
                        "2021-01-01T00:07:51.470000Z\t15703\n" +
                        "2021-01-01T00:07:51.880000Z\t15716\n" +
                        "2021-01-01T00:07:51.930000Z\t15717\n" +
                        "2021-01-01T00:07:52.140000Z\t15724\n" +
                        "2021-01-01T00:07:52.390000Z\t15732\n" +
                        "2021-01-01T00:07:52.470000Z\t15734\n" +
                        "2021-01-01T00:07:52.910000Z\t15748\n" +
                        "2021-01-01T00:07:53.070000Z\t15754\n" +
                        "2021-01-01T00:07:53.110000Z\t15756\n" +
                        "2021-01-01T00:07:53.960000Z\t15789\n" +
                        "2021-01-01T00:07:54.540000Z\t15810\n" +
                        "2021-01-01T00:07:55.270000Z\t15838\n" +
                        "2021-01-01T00:07:55.340000Z\t15841\n" +
                        "2021-01-01T00:07:55.630000Z\t15852\n" +
                        "2021-01-01T00:07:55.680000Z\t15854\n" +
                        "2021-01-01T00:07:56.640000Z\t15883\n" +
                        "2021-01-01T00:07:57.150000Z\t15895\n" +
                        "2021-01-01T00:07:58.440000Z\t15939\n" +
                        "2021-01-01T00:07:58.600000Z\t15944\n" +
                        "2021-01-01T00:07:58.760000Z\t15949\n" +
                        "2021-01-01T00:07:58.980000Z\t15958\n" +
                        "2021-01-01T00:07:59.270000Z\t15966\n" +
                        "2021-01-01T00:08:00.370000Z\t15999\n" +
                        "2021-01-01T00:08:00.630000Z\t16008\n" +
                        "2021-01-01T00:08:00.670000Z\t16009\n" +
                        "2021-01-01T00:08:00.710000Z\t16011\n" +
                        "2021-01-01T00:08:01.270000Z\t16033\n" +
                        "2021-01-01T00:08:02.180000Z\t16072\n" +
                        "2021-01-01T00:08:02.280000Z\t16076\n" +
                        "2021-01-01T00:08:02.860000Z\t16099\n" +
                        "2021-01-01T00:08:02.880000Z\t16100\n" +
                        "2021-01-01T00:08:06.660000Z\t16227\n" +
                        "2021-01-01T00:08:06.720000Z\t16229\n" +
                        "2021-01-01T00:08:07.720000Z\t16257\n" +
                        "2021-01-01T00:08:08.830000Z\t16296\n" +
                        "2021-01-01T00:08:10.800000Z\t16367\n" +
                        "2021-01-01T00:08:10.830000Z\t16368\n" +
                        "2021-01-01T00:08:11.170000Z\t16382\n" +
                        "2021-01-01T00:08:11.590000Z\t16403\n" +
                        "2021-01-01T00:08:12.040000Z\t16418\n" +
                        "2021-01-01T00:08:12.090000Z\t16419\n" +
                        "2021-01-01T00:08:13.840000Z\t16480\n" +
                        "2021-01-01T00:08:13.890000Z\t16482\n" +
                        "2021-01-01T00:08:14.000000Z\t16485\n" +
                        "2021-01-01T00:08:14.420000Z\t16499\n" +
                        "2021-01-01T00:08:16.410000Z\t16567\n" +
                        "2021-01-01T00:08:16.890000Z\t16583\n" +
                        "2021-01-01T00:08:16.990000Z\t16586\n" +
                        "2021-01-01T00:08:18.180000Z\t16628\n" +
                        "2021-01-01T00:08:18.710000Z\t16645\n" +
                        "2021-01-01T00:08:19.190000Z\t16661\n" +
                        "2021-01-01T00:08:20.110000Z\t16698\n" +
                        "2021-01-01T00:08:20.240000Z\t16702\n" +
                        "2021-01-01T00:08:20.710000Z\t16718\n" +
                        "2021-01-01T00:08:21.040000Z\t16729\n" +
                        "2021-01-01T00:08:21.510000Z\t16744\n" +
                        "2021-01-01T00:08:22.940000Z\t16788\n" +
                        "2021-01-01T00:08:22.990000Z\t16789\n" +
                        "2021-01-01T00:08:23.370000Z\t16801\n" +
                        "2021-01-01T00:08:23.420000Z\t16802\n" +
                        "2021-01-01T00:08:23.560000Z\t16807\n" +
                        "2021-01-01T00:08:23.720000Z\t16811\n" +
                        "2021-01-01T00:08:23.990000Z\t16818\n" +
                        "2021-01-01T00:08:24.010000Z\t16820\n" +
                        "2021-01-01T00:08:24.650000Z\t16840\n" +
                        "2021-01-01T00:08:25.070000Z\t16853\n" +
                        "2021-01-01T00:08:25.870000Z\t16879\n" +
                        "2021-01-01T00:08:26.070000Z\t16887\n" +
                        "2021-01-01T00:08:27.110000Z\t16920\n" +
                        "2021-01-01T00:08:27.310000Z\t16926\n" +
                        "2021-01-01T00:08:27.350000Z\t16927\n" +
                        "2021-01-01T00:08:27.850000Z\t16940\n" +
                        "2021-01-01T00:08:28.020000Z\t16945\n" +
                        "2021-01-01T00:08:28.250000Z\t16951\n" +
                        "2021-01-01T00:08:29.420000Z\t16994\n",
                //"select count() from (\n" +
                "select k, first(lat)\n" +
                        "from sam_by_tst\n" +
                        "where s in ('a')\n" +
                        "sample by 10T limit -100\n"
                // + ")"
                ,
                "create table sam_by_tst as (\n" +
                        "select rnd_symbol('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g') as s,\n" +
                        "timestamp_sequence('2021-01-01', rnd_short(1,5) * 10000L) as k,\n" +
                        "x as lat,\n" +
                        "-x as lon\n" +
                        "from long_sequence(17 * 1000L)\n" +
                        "), index(s) timestamp(k) partition by DAY",
                true);
    }

    @Test
    public void testIndexSampleByVeryFewRowsPerInterval() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, last(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and s in ('a')" +
                        "sample by 2h",
                "create table xx (lat long, lon long, s symbol, k timestamp)" +
                        ", index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T00:54:00.000000Z\ta\t-2\t2\n" +
                        "1970-01-01T02:39:00.000000Z\ta\t-4\t4\n" +
                        "1970-01-01T04:29:00.000000Z\ta\t-6\t6\n" +
                        "1970-01-01T06:14:00.000000Z\ta\t-8\t8\n" +
                        "1970-01-01T08:04:00.000000Z\ta\t-10\t10\n" +
                        "1970-01-01T09:54:00.000000Z\ta\t-12\t12\n" +
                        "1970-01-01T11:39:00.000000Z\ta\t-14\t14\n" +
                        "1970-01-01T13:29:00.000000Z\ta\t-16\t16\n" +
                        "1970-01-01T15:14:00.000000Z\ta\t-18\t18\n" +
                        "1970-01-01T17:04:00.000000Z\ta\t-20\t20\n" +
                        "1970-01-01T18:54:00.000000Z\ta\t-22\t22\n" +
                        "1970-01-01T20:39:00.000000Z\ta\t-24\t24\n" +
                        "1970-01-01T22:29:00.000000Z\ta\t-26\t26\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-01-01' and s in ('a')" +
                        "sample by 5m",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 54 * 60 * 1000000L) k\n" + // 54 mins
                        "from\n" +
                        "long_sequence(48)\n");
    }

    @Test
    public void testIndexSampleByWithEmptyIndexPage() throws Exception {
        assertQuery("k\ts\tlat\tlon\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and s in ('b')" +
                        "sample by 2h",
                "create table xx (lat double, lon double, s symbol, k timestamp)" +
                        ", index(s capacity 10) timestamp(k) partition by DAY",
                "k",
                false,
                false,
                true);

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-02-02T00:00:00.000000Z\tb\t-33.0\t33.0\n" +
                        "1970-02-04T00:00:00.000000Z\tb\t-35.0\t35.0\n" +
                        "1970-02-06T00:00:00.000000Z\tb\t-37.0\t37.0\n" +
                        "1970-02-08T00:00:00.000000Z\tb\t-39.0\t39.0\n" +
                        "1970-02-10T00:00:00.000000Z\tb\t-41.0\t41.0\n" +
                        "1970-02-12T00:00:00.000000Z\tb\t-43.0\t43.0\n" +
                        "1970-02-14T00:00:00.000000Z\tb\t-45.0\t45.0\n",
                "select k, s, first(lat) lat, first(lon) lon " +
                        "from xx " +
                        "where k in '1970-02' and k < '1970-02-16' and s in ('b')" +
                        "sample by 1d",
                "insert into xx " +
                        "select -x lat,\n" +
                        "x lon,\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 24 * 60 * 60 * 1000000L) k\n" + // 60 mins
                        "from\n" +
                        "long_sequence(365)\n");
    }

    @Test
    public void testIndexSampleIndexNoRowsInIndex() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xx (k timestamp)\n" +
                    " timestamp(k) partition by DAY", sqlExecutionContext);
            compiler.compile(
                    "insert into xx " +
                            "select " +
                            "timestamp_sequence(0, 1 * 60 * 1000000L) k\n" +
                            "from\n" +
                            "long_sequence(100)\n", sqlExecutionContext);
            compiler.compile("alter table xx add s SYMBOL INDEX", sqlExecutionContext);
        });

        String expected = "fk\tlk\tk\ts\n" +
                "1970-01-01T00:00:00.000000Z\t1970-01-01T00:59:00.000000Z\t1970-01-01T00:00:00.000000Z\t\n" +
                "1970-01-01T01:00:00.000000Z\t1970-01-01T01:39:00.000000Z\t1970-01-01T01:00:00.000000Z\t\n";

        // Forced no index execution
        assertSql("select first(k) fk, last(k) lk, k, s\n" +
                        "from xx\n" +
                        "where s = null or s = 'none'\n" +
                        "sample by 1h",
                expected);

        // Indexed execution
        assertSql("select first(k) fk, last(k) lk, k, s\n" +
                        "from xx\n" +
                        "where s = null\n" +
                        "sample by 1h",
                expected);
    }

    @Test
    public void testIndexSampleLatestRestrictedByWhere() throws Exception {
        assertMemoryLeak(() -> compiler.compile("create table xx (s symbol, k timestamp)" +
                ", index(s capacity 256) timestamp(k) partition by DAY", sqlExecutionContext));

        assertSampleByIndexQuery("k\ts\tlat\tlon\n" +
                        "1970-01-01T05:01:00.000000Z\ta\t1970-01-01T05:01:00.000000Z\t1970-01-01T05:29:00.000000Z\n",
                "select k, s, first(k) lat, last(k) lon " +
                        "from xx " +
                        "where k between '1970-01-01T05:00' and '1970-01-01T05:30' and s in ('a')" +
                        "sample by 1h",
                "insert into xx " +
                        "select " +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(0, 1 * 60 * 1000000L) k\n" +
                        "from\n" +
                        "long_sequence(360)\n");
    }

    @Test
    public void testIndexSampleMainIndexHasColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xx (k timestamp)\n" +
                    " timestamp(k) partition by DAY", sqlExecutionContext);
            compiler.compile(
                    "insert into xx " +
                            "select " +
                            "timestamp_sequence(0, 1 * 60 * 1000000L) k\n" +
                            "from\n" +
                            "long_sequence(100)\n", sqlExecutionContext);
            compiler.compile("alter table xx add s SYMBOL INDEX", sqlExecutionContext);
            compiler.compile("insert into xx " +
                    "select " +
                    "timestamp_sequence(24 * 60 * 60 * 1000000L, 1 * 60 * 1000000L),\n" +
                    "(case when x % 2 = 0 then 'a' else 'b' end) sk\n" +
                    "from\n" +
                    "long_sequence(60)\n", sqlExecutionContext);
        });

        // 1970-01-01 data does not have s column
        // first hour of 1970-01-02 does not have s column
        assertSampleByIndexQuery("fk\tlk\tk\ts\n" +
                        "1970-01-02T00:00:00.000000Z\t1970-01-02T00:58:00.000000Z\t1970-01-02T00:00:00.000000Z\tb\n" +
                        "1970-01-02T01:00:00.000000Z\t1970-01-02T01:58:00.000000Z\t1970-01-02T01:00:00.000000Z\tb\n" +
                        "1970-01-02T02:00:00.000000Z\t1970-01-02T02:58:00.000000Z\t1970-01-02T02:00:00.000000Z\tb\n" +
                        "1970-01-02T03:00:00.000000Z\t1970-01-02T03:58:00.000000Z\t1970-01-02T03:00:00.000000Z\tb\n" +
                        "1970-01-02T04:00:00.000000Z\t1970-01-02T04:58:00.000000Z\t1970-01-02T04:00:00.000000Z\tb\n" +
                        "1970-01-02T05:00:00.000000Z\t1970-01-02T05:58:00.000000Z\t1970-01-02T05:00:00.000000Z\tb\n",
                "select first(k) fk, last(k) lk, k, s\n" +
                        "from xx " +
                        "where s in ('b')" +
                        "sample by 1h",
                "insert into xx " +
                        "select " +
                        "timestamp_sequence(25 * 60 * 60 * 1000000L, 1 * 60 * 1000000L),\n" +
                        "(case when x % 2 = 0 then 'a' else 'b' end) sk\n" +
                        "from\n" +
                        "long_sequence(300)\n");
    }

    @Test
    public void testIndexSampleWithColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xx (s symbol, k timestamp)" +
                    ", index(s capacity 256) timestamp(k) partition by DAY", sqlExecutionContext);

            compiler.compile(
                    "insert into xx " +
                            "select " +
                            "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                            "timestamp_sequence(0, 1 * 60 * 1000000L) k\n" +
                            "from\n" +
                            "long_sequence(100)\n", sqlExecutionContext);

            compiler.compile("alter table xx add i1 int", sqlExecutionContext);
            compiler.compile("alter table xx add c1 char", sqlExecutionContext);
            compiler.compile("alter table xx add l1 long", sqlExecutionContext);

            compiler.compile(
                    "insert into xx " +
                            "select " +
                            "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                            "timestamp_sequence(100 * 60 * 1000000L, 1 * 60 * 1000000L) k,\n" +
                            "cast(x + 100 as int) i1, \n" +
                            "rnd_char() c1,\n" +
                            "x as l1\n" +
                            "from\n" +
                            "long_sequence(100)", sqlExecutionContext);

            compiler.compile("alter table xx add f1 float", sqlExecutionContext);
            compiler.compile("alter table xx add d1 double", sqlExecutionContext);
            compiler.compile("alter table xx add s1 symbol", sqlExecutionContext);
            compiler.compile("alter table xx add ss1 short", sqlExecutionContext);
            compiler.compile("alter table xx add b1 byte", sqlExecutionContext);
            compiler.compile("alter table xx add t1 timestamp", sqlExecutionContext);
            compiler.compile("alter table xx add dt date", sqlExecutionContext);
        });

        assertSampleByIndexQuery("fi1\tli1\tfc1\tlc1\tfl1\tlf1\tff1\tlf11\tfd1\tld1\tfs1\tls1\tfss1\tlss1\tfb1\tlb1\tfk\tlk\tft1\tlt1\tfdt\tldt\tk\ts\n" +
                        "NaN\tNaN\t\t\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t\t\t0\t0\t0\t0\t1970-01-01T00:00:00.000000Z\t1970-01-01T00:28:00.000000Z\t\t\t\t\t1970-01-01T00:00:00.000000Z\tb\n" +
                        "NaN\tNaN\t\t\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t\t\t0\t0\t0\t0\t1970-01-01T00:30:00.000000Z\t1970-01-01T00:58:00.000000Z\t\t\t\t\t1970-01-01T00:30:00.000000Z\tb\n" +
                        "NaN\tNaN\t\t\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t\t\t0\t0\t0\t0\t1970-01-01T01:00:00.000000Z\t1970-01-01T01:28:00.000000Z\t\t\t\t\t1970-01-01T01:00:00.000000Z\tb\n" +
                        "NaN\t119\t\tG\tNaN\t19\tNaN\tNaN\tNaN\tNaN\t\t\t0\t0\t0\t0\t1970-01-01T01:30:00.000000Z\t1970-01-01T01:58:00.000000Z\t\t\t\t\t1970-01-01T01:30:00.000000Z\tb\n" +
                        "121\t149\tS\tL\t21\t49\tNaN\tNaN\tNaN\tNaN\t\t\t0\t0\t0\t0\t1970-01-01T02:00:00.000000Z\t1970-01-01T02:28:00.000000Z\t\t\t\t\t1970-01-01T02:00:00.000000Z\tb\n" +
                        "151\t179\tD\tR\t51\t79\tNaN\tNaN\tNaN\tNaN\t\t\t0\t0\t0\t0\t1970-01-01T02:30:00.000000Z\t1970-01-01T02:58:00.000000Z\t\t\t\t\t1970-01-01T02:30:00.000000Z\tb\n" +
                        "181\t209\tZ\tV\t81\t109\tNaN\t204.5000\tNaN\t222.5\t\tc3\t0\t9\t0\t9\t1970-01-01T03:00:00.000000Z\t1970-01-01T03:28:00.000000Z\t\t1970-01-01T00:00:00.000009Z\t\t1970-01-01T00:00:00.009Z\t1970-01-01T03:00:00.000000Z\tb\n" +
                        "211\t239\tD\tT\t111\t139\t205.5000\t219.5000\t227.5\t297.5\t\t\t11\t39\t11\t39\t1970-01-01T03:30:00.000000Z\t1970-01-01T03:58:00.000000Z\t1970-01-01T00:00:00.000011Z\t1970-01-01T00:00:00.000039Z\t1970-01-01T00:00:00.011Z\t1970-01-01T00:00:00.039Z\t1970-01-01T03:30:00.000000Z\tb\n" +
                        "241\t269\tS\tL\t141\t169\t220.5000\t234.5000\t302.5\t372.5\tc3\tc3\t41\t69\t41\t69\t1970-01-01T04:00:00.000000Z\t1970-01-01T04:28:00.000000Z\t1970-01-01T00:00:00.000041Z\t1970-01-01T00:00:00.000069Z\t1970-01-01T00:00:00.041Z\t1970-01-01T00:00:00.069Z\t1970-01-01T04:00:00.000000Z\tb\n" +
                        "271\t299\tO\tN\t171\t199\t235.5000\t249.5000\t377.5\t447.5\ta1\tc3\t71\t99\t71\t99\t1970-01-01T04:30:00.000000Z\t1970-01-01T04:58:00.000000Z\t1970-01-01T00:00:00.000071Z\t1970-01-01T00:00:00.000099Z\t1970-01-01T00:00:00.071Z\t1970-01-01T00:00:00.099Z\t1970-01-01T04:30:00.000000Z\tb\n",
                "select first(i1) fi1, last(i1) li1, first(c1) fc1, " +
                        "last(c1) lc1, first(l1) fl1, last(l1) lf1, first(f1) ff1, last(f1) lf1, " +
                        "first(d1) fd1, last(d1) ld1, first(s1) fs1, last(s1) ls1, first(ss1) fss1, " +
                        "last(ss1) lss1, first(b1) fb1, last(b1) lb1, first(k) fk, last(k) lk, first(t1) ft1, " +
                        "last(t1) lt1, first(dt) fdt, last(dt) ldt, k, s\n" +
                        "from xx " +
                        "where s in ('b')" +
                        "sample by 30m",
                "insert into xx " +
                        "select " +
                        "(case when x % 2 = 0 then 'a' else 'b' end) s,\n" +
                        "timestamp_sequence(200 * 60 * 1000000L, 1 * 60 * 1000000L) k,\n" +
                        "cast(x + 200 as int) i1, \n" +
                        "rnd_char() c1, \n" +
                        "x+100 as l1,\n" +
                        "cast(x * 0.5 + 200 as float) f1, \n" +
                        "x*2.5 + 200 d1,\n" +
                        "rnd_symbol(null, 'a1', 'b2', 'c3') s1, \n" +
                        "cast(x as SHORT) ss1,\n" +
                        "cast(x % 256 as byte) b1,\n" +
                        "cast(x as timestamp) t1,\n" +
                        "cast(x as date) dt\n" +
                        "from\n" +
                        "long_sequence(100)");
    }

    @Test
    public void testSampleByFirstLastRecordCursorFactoryInvalidColumns() {
        try {
            GenericRecordMetadata groupByMeta = new GenericRecordMetadata();
            groupByMeta.add(new TableColumnMetadata("col1", ColumnType.STRING, false, 0, false, EmptyRecordMetadata.INSTANCE));

            GenericRecordMetadata meta = new GenericRecordMetadata();
            meta.add(new TableColumnMetadata("col1", ColumnType.LONG, false, 0, false, EmptyRecordMetadata.INSTANCE));

            ObjList<QueryColumn> columns = new ObjList<>();
            ExpressionNode first = ExpressionNode.FACTORY.newInstance().of(ColumnType.LONG, "first", 0, 0);
            first.rhs = ExpressionNode.FACTORY.newInstance().of(ColumnType.LONG, "col1", 0, 0);
            QueryColumn col = QueryColumn.FACTORY.newInstance().of("col1", first);
            columns.add(col);

            new SampleByFirstLastRecordCursorFactory(null, new MicroTimestampSampler(100L), groupByMeta, columns, meta, 0, getSymbolFilter(), -1);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "first(), last() is not supported on data type");
        }
    }

    @Test
    public void testSampleByFirstLastRecordCursorFactoryInvalidNotFirstLast() {
        try {
            GenericRecordMetadata groupByMeta = new GenericRecordMetadata();
            TableColumnMetadata column = new TableColumnMetadata("col1", ColumnType.LONG, false, 0, false, EmptyRecordMetadata.INSTANCE);
            groupByMeta.add(column);

            GenericRecordMetadata meta = new GenericRecordMetadata();
            meta.add(column);

            ObjList<QueryColumn> columns = new ObjList<>();
            ExpressionNode first = ExpressionNode.FACTORY.newInstance().of(ColumnType.LONG, "min", 0, 0);
            first.rhs = ExpressionNode.FACTORY.newInstance().of(ColumnType.LONG, "col1", 0, 0);
            QueryColumn col = QueryColumn.FACTORY.newInstance().of("col1", first);
            columns.add(col);

            new SampleByFirstLastRecordCursorFactory(null, new MicroTimestampSampler(100L), groupByMeta, columns, meta, 0, getSymbolFilter(), -1);
            Assert.fail();
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "expected first() or last() functions but got min");
        }
    }

    @NotNull
    private SingleSymbolFilter getSymbolFilter() {
        return new SingleSymbolFilter() {
            @Override
            public int getColumnIndex() {
                return 0;
            }

            @Override
            public int getSymbolFilterKey() {
                return 0;
            }
        };
    }

    @Test
    public void testNoSampleByWithDeferredSingleSymbolFilterDataFrameRecordCursorFactory() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table xx (k timestamp, d DOUBLE, s SYMBOL)" +
                    ", index(s capacity 345) timestamp(k) partition by DAY \n", sqlExecutionContext);

            compiler.compile("insert into xx " +
                    "select " +
                    "timestamp_sequence(25 * 60 * 60 * 1000000L, 1 * 60 * 1000000L),\n" +
                    "rnd_double() d,\n" +
                    "(case when x % 2 = 0 then 'a' else 'b' end) sk\n" +
                    "from\n" +
                    "long_sequence(300)\n", sqlExecutionContext);

            assertSql("select sum(d)\n" +
                            "from xx " +
                            "where s in ('a')",
                    "sum\n" +
                            "75.42541658721542\n");
        });
    }

    @Test
    public void testSampleBadFunction() throws Exception {
        assertFailure(
                "select b, sumx(a, 'ab') k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "exception in function factory"
        );
    }

    @Test
    public void testSampleBadFunctionInterpolated() throws Exception {
        assertFailure(
                "select b, sumx(a, 'ac') k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "exception in function factory"
        );
    }

    @Test
    public void testSampleByAllTypesAndInvalidTimestampColumn() throws Exception {
        assertFailure("select \n" +
                        "    LastUpdate, \n" +
                        "    CountryRegion, \n" +
                        "    last(Confirmed) Confirmed, \n" +
                        "    last(Recovered) Recovered, \n" +
                        "    last(Deaths) Deaths \n" +
                        "    from (\n" +
                        "        select \n" +
                        "            LastUpdate, \n" +
                        "            CountryRegion, \n" +
                        "            sum(Confirmed) Confirmed, \n" +
                        "            sum(Recovered) Recovered, \n" +
                        "            sum(Deaths) Deaths\n" +
                        "        from (\n" +
                        "            select \n" +
                        "                LastUpdate, \n" +
                        "                ProvinceState, \n" +
                        "                CountryRegion, \n" +
                        "                last(Confirmed) Confirmed, \n" +
                        "                last(Recovered) Recovered, \n" +
                        "                last(Deaths) Deaths\n" +
                        "            from (covid where CountryRegion in ('China', 'Mainland China'))\n" +
                        "            sample by 1d fill(prev)\n" +
                        "        )\n" +
                        "    ) timestamp(xy) sample by 1M\n" +
                        ";\n",
                "create table covid as " +
                        "(" +
                        "select" +
                        " rnd_symbol(5,4,4,1) ProvinceState," +
                        " rnd_symbol(5,4,4,1) CountryRegion," +
                        " abs(rnd_int()) Confirmed," +
                        " abs(rnd_int()) Recovered," +
                        " abs(rnd_int()) Deaths," +
                        " timestamp_sequence(172800000000, 3600000000) LastUpdate" +
                        " from" +
                        " long_sequence(1000)" +
                        ") timestamp(LastUpdate) partition by NONE",
                713,
                "Invalid column: xy"
        );
    }

    @Test
    public void testSampleByAllTypesAndInvalidTimestampType() throws Exception {
        assertFailure("select \n" +
                        "    LastUpdate, \n" +
                        "    CountryRegion, \n" +
                        "    last(Confirmed) Confirmed, \n" +
                        "    last(Recovered) Recovered, \n" +
                        "    last(Deaths) Deaths \n" +
                        "    from (\n" +
                        "        select \n" +
                        "            LastUpdate, \n" +
                        "            CountryRegion, \n" +
                        "            sum(Confirmed) Confirmed, \n" +
                        "            sum(Recovered) Recovered, \n" +
                        "            sum(Deaths) Deaths\n" +
                        "        from (\n" +
                        "            select \n" +
                        "                LastUpdate, \n" +
                        "                ProvinceState, \n" +
                        "                CountryRegion, \n" +
                        "                last(Confirmed) Confirmed, \n" +
                        "                last(Recovered) Recovered, \n" +
                        "                last(Deaths) Deaths\n" +
                        "            from (covid where CountryRegion in ('China', 'Mainland China'))\n" +
                        "            sample by 1d fill(prev)\n" +
                        "        )\n" +
                        "    ) timestamp(CountryRegion) sample by 1M\n" +
                        ";\n",
                "create table covid as " +
                        "(" +
                        "select" +
                        " rnd_symbol(5,4,4,1) ProvinceState," +
                        " rnd_symbol(5,4,4,1) CountryRegion," +
                        " abs(rnd_int()) Confirmed," +
                        " abs(rnd_int()) Recovered," +
                        " abs(rnd_int()) Deaths," +
                        " timestamp_sequence(172800000000, 3600000000) LastUpdate" +
                        " from" +
                        " long_sequence(1000)" +
                        ") timestamp(LastUpdate) partition by NONE",
                713,
                "not a TIMESTAMP"
        );
    }

    @Test
    public void testSampleByMillisFillNoneNotKeyedEmpty() throws Exception {
        assertQuery("sum\tk\n",
                "select sum(a), k from x sample by 100T fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 100) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 100000) k" +
                        " from" +
                        " long_sequence(30)" +
                        ") timestamp(k)",
                "sum\tk\n" +
                        "0.35983672154330515\t1970-01-04T05:00:00.000000Z\n" +
                        "76.75673070796104\t1970-01-04T05:00:00.100000Z\n" +
                        "62.173267078530984\t1970-01-04T05:00:00.200000Z\n" +
                        "63.81607531178513\t1970-01-04T05:00:00.300000Z\n" +
                        "57.93466326862211\t1970-01-04T05:00:00.400000Z\n" +
                        "12.026122412833129\t1970-01-04T05:00:00.500000Z\n" +
                        "48.820511018586934\t1970-01-04T05:00:00.600000Z\n" +
                        "26.922103479744898\t1970-01-04T05:00:00.700000Z\n" +
                        "52.98405941762054\t1970-01-04T05:00:00.800000Z\n" +
                        "84.45258177211063\t1970-01-04T05:00:00.900000Z\n" +
                        "97.5019885372507\t1970-01-04T05:00:01.000000Z\n" +
                        "49.00510449885239\t1970-01-04T05:00:01.100000Z\n" +
                        "80.01121139739173\t1970-01-04T05:00:01.200000Z\n" +
                        "92.050039469858\t1970-01-04T05:00:01.300000Z\n" +
                        "45.6344569609078\t1970-01-04T05:00:01.400000Z\n" +
                        "40.455469747939254\t1970-01-04T05:00:01.500000Z\n" +
                        "56.594291398612405\t1970-01-04T05:00:01.600000Z\n" +
                        "9.750574414434398\t1970-01-04T05:00:01.700000Z\n" +
                        "12.105630273556178\t1970-01-04T05:00:01.800000Z\n" +
                        "57.78947915182423\t1970-01-04T05:00:01.900000Z\n" +
                        "86.85154305419587\t1970-01-04T05:00:02.000000Z\n" +
                        "12.02416087573498\t1970-01-04T05:00:02.100000Z\n" +
                        "49.42890511958454\t1970-01-04T05:00:02.200000Z\n" +
                        "58.912164838797885\t1970-01-04T05:00:02.300000Z\n" +
                        "67.52509547112409\t1970-01-04T05:00:02.400000Z\n" +
                        "44.80468966861358\t1970-01-04T05:00:02.500000Z\n" +
                        "89.40917126581896\t1970-01-04T05:00:02.600000Z\n" +
                        "94.41658975532606\t1970-01-04T05:00:02.700000Z\n" +
                        "62.5966045857722\t1970-01-04T05:00:02.800000Z\n" +
                        "94.55893004802432\t1970-01-04T05:00:02.900000Z\n",
                false);
    }

    @Test
    public void testSampleCountFillLinear() throws Exception {
        assertQuery("b\tcount\tk\n" +
                        "\t15\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t5\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "\t14\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t5\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "\t17\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t2\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8\t1970-01-03T06:00:00.000000Z\n" +
                        "\t4\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t11\t1970-01-03T09:00:00.000000Z\n",

                "select b, count(), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tcount\tk\n" +
                        "\t15\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t5\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t2\t1970-01-03T00:00:00.000000Z\n" +
                        "CGFN\t-8\t1970-01-03T00:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T00:00:00.000000Z\n" +
                        "PEVM\t-8\t1970-01-03T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t14\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t5\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t4\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\t-7\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\t-7\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t17\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t3\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t4\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t2\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t8\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\t-6\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\t-6\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t4\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t3\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t2\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t11\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\t-5\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\t-5\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t4\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t2\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t2\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t4\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t1\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t14\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\t-4\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\t-4\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t5\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t1\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t1\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t5\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t17\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\t-3\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\t-3\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t5\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t6\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-1\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\t-2\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\t-2\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t6\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t7\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-2\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t23\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\t-1\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t6\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t-2\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t-2\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t8\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-3\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t26\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t7\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\t1\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\t1\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t-3\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t-3\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t9\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-4\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t29\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t3\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t2\t1970-01-04T06:00:00.000000Z\n" +
                        "\t14\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t2\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t3\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t-4\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t-4\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t10\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-5\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t32\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleCountFillLinearFromSubQuery() throws Exception {
        assertQuery("b\tcount\tk\n" +
                        "CPSW\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T09:00:00.000000Z\n",

                "select b, count(), k from (x latest by b) sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 360000000) k" +
                        " from" +
                        " long_sequence(100)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 360000000) k" +
                        " from" +
                        " long_sequence(35)" +
                        ") timestamp(k)",
                "b\tcount\tk\n" +
                        "CPSW\t1\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t1\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t1\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "NPIW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "CGFN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZNFK\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEVM\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WGRM\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "NPIW\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "CGFN\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "ZNFK\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "PEVM\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "\t1\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillAllTypesLinear() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "HYRX\t11.427980661392212\t42.17768841969397\t426455968\t42\t4924\t4086802474270249591\t1970-01-03T00:00:00.000000Z\n" +
                        "\t42.24356412887573\t70.94360487171201\t1631244228\t50\t10900\t8349358446893356086\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t33.60825181007385\t76.75673070796104\t422941535\t27\t32312\t4442449726822927731\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t140.1138186454773\t-63.36813480742224\t2901521895\t9\t16851\t9223372036854775807\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t81.46807551383972\t12.503042190293423\t2085282008\t9\t11472\t8955092533521658248\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t67.61934757232666\t34.35685332942956\t2144581835\t6\t10942\t3152466304308949756\t1970-01-03T03:00:00.000000Z\n" +
                        "\t41.38164520263672\t55.22494170511608\t667031149\t38\t22298\t5536695302686527374\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t23.06460291147232\t50.77786067801929\t435411399\t41\t9083\t5351051939379353600\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t101.64479613304138\t92.16079308066422\t2815179092\t80\t39010\t-7038722756553554443\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t22.82233238220215\t88.37421918800908\t1269042121\t9\t6093\t4608960730952244094\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t34.70122516155243\t59.378032936344596\t444366830\t41\t13242\t6615301404488457216\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t101.63044333457947\t-8.043024049101913\t3866222134\t-15\t-10428\t1862482881794971392\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t153.94197702407837\t103.11980620255937\t2182089028\t31\t38774\t-4203926486423760584\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t96.40288949012756\t42.02044253932608\t712702244\t46\t22661\t2762535352290012031\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t46.33784741163254\t67.9782051946699\t453322261\t40\t17401\t7879550869597561856\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t135.64153909683228\t-50.442901427633394\t5587862435\t-36\t-31798\t572499459280992448\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-35.823410749435425\t164.24539618572473\t452802234\t9\t714\t262828928382831328\t1970-01-03T09:00:00.000000Z\n" +
                        "\t82.35564231872559\t189.81728064582336\t2909635248\t54\t2779\t-238979168606022602\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t57.97446966171265\t76.57837745299521\t462277692\t40\t21561\t9143800334706665900\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t169.65263485908508\t-92.84277880616484\t7309502735\t-57\t-53168\t-717483963232985728\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-94.469153881073\t240.11657318344038\t-363437653\t9\t-4665\t-4083302874186582528\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t39.0173077583313\t10.643046345788132\t1238491107\t13\t30722\t6912707344119330199\t1970-01-03T15:00:00.000000Z\n" +
                        "\t107.86141157150269\t139.30694155564203\t2116801049\t40\t45678\t-3504226003016057166\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t203.6637306213379\t-135.24265618469636\t9031143035\t-78\t-74538\t-2007467385746963968\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-153.1148970127106\t315.98775018115606\t-1179677539\t9\t-10044\t-8429434676755996672\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t151.3360619544983\t120.5188941413223\t2698444286\t40\t37984\t-4160055112489677424\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.06014585494995\t-55.29228476141894\t2014704521\t-14\t39883\t4681614353531994112\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t237.67482638359067\t-177.6425335632278\t10752783335\t-99\t-95908\t-3297450808260941824\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-211.76064014434814\t391.8589271788717\t-1995917427\t9\t-15423\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n",

                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "HYRX\t11.427980661392212\t42.17768841969397\t426455968\t42\t4924\t4086802474270249591\t1970-01-03T00:00:00.000000Z\n" +
                        "\t42.24356412887573\t70.94360487171201\t1631244228\t50\t10900\t8349358446893356086\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t33.60825181007385\t76.75673070796104\t422941535\t27\t32312\t4442449726822927731\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t140.1138186454773\t-63.36813480742224\t2901521895\t9\t16851\t9223372036854775807\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t81.46807551383972\t12.503042190293423\t2085282008\t9\t11472\t8955092533521658248\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t67.61934757232666\t34.35685332942956\t2144581835\t6\t10942\t3152466304308949756\t1970-01-03T03:00:00.000000Z\n" +
                        "\t41.38164520263672\t55.22494170511608\t667031149\t38\t22298\t5536695302686527374\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t23.06460291147232\t50.77786067801929\t435411399\t41\t9083\t5351051939379353600\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t101.64479613304138\t92.16079308066422\t2815179092\t80\t39010\t-7038722756553554443\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t22.82233238220215\t88.37421918800908\t1269042121\t9\t6093\t4608960730952244094\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t34.70122516155243\t59.378032936344596\t444366830\t41\t13242\t6615301404488457216\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t101.63044333457947\t-8.043024049101913\t3866222134\t-15\t-10428\t1862482881794971392\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t153.94197702407837\t103.11980620255937\t2182089028\t31\t38774\t-4203926486423760584\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t96.40288949012756\t42.02044253932608\t712702244\t46\t22661\t2762535352290012031\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t46.33784741163254\t67.9782051946699\t453322261\t40\t17401\t7879550869597561856\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t135.64153909683228\t-50.442901427633394\t5587862435\t-36\t-31798\t572499459280992448\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t-35.823410749435425\t164.24539618572473\t452802234\t9\t714\t262828928382831328\t1970-01-03T09:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t82.35564231872559\t189.81728064582336\t2909635248\t54\t2779\t-238979168606022602\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t57.97446966171265\t76.57837745299521\t462277692\t40\t21561\t9143800334706665900\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t169.65263485908508\t-92.84277880616484\t7309502735\t-57\t-53168\t-717483963232985728\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t-94.469153881073\t240.11657318344038\t-363437653\t9\t-4665\t-4083302874186582528\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t39.0173077583313\t10.643046345788132\t1238491107\t13\t30722\t6912707344119330199\t1970-01-03T15:00:00.000000Z\n" +
                        "\t107.86141157150269\t139.30694155564203\t2116801049\t40\t45678\t-3504226003016057166\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t203.6637306213379\t-135.24265618469636\t9031143035\t-78\t-74538\t-2007467385746963968\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t-153.1148970127106\t315.98775018115606\t-1179677539\t9\t-10044\t-8429434676755996672\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t151.3360619544983\t120.5188941413223\t2698444286\t40\t37984\t-4160055112489677424\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.06014585494995\t-55.29228476141894\t2014704521\t-14\t39883\t4681614353531994112\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t237.67482638359067\t-177.6425335632278\t10752783335\t-99\t-95908\t-3297450808260941824\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t-211.76064014434814\t391.8589271788717\t-1995917427\t9\t-15423\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t1.1029839515686035\t-121.22761586862603\t2790917936\t-41\t49044\t2450521362944657408\t1970-01-03T21:00:00.000000Z\n" +
                        "\t133.75427524248758\t113.29263307717305\t2436316552\t36\t31264\t179183534540497952\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t271.6859221458435\t-220.04241094175927\t12474423635\t-120\t-117278\t-4587434230774920192\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-270.4063832759857\t467.73010417658736\t-2812157313\t9\t-20802\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-17.854177951812744\t-187.1629469758331\t3567131351\t-68\t58205\t219428372357321856\t1970-01-04T00:00:00.000000Z\n" +
                        "\t116.17248853047688\t106.06637201302377\t2174188819\t33\t24544\t4518422181570673664\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t305.6970179080963\t-262.44228832029074\t14196063935\t-141\t-138648\t-5877417653288898560\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-329.05212640762335\t543.601281174303\t-3628397201\t9\t-26181\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t98.59070181846619\t98.8401109488745\t1912061086\t30\t17824\t8857660828600848720\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-36.81133985519409\t-253.09827808304019\t4343344767\t-95\t67366\t-2011664618230010368\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t339.7081136703491\t-304.84216569882227\t15917704235\t-162\t-160018\t-7167401075802876928\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-387.69786953926086\t619.4724581720187\t-4444637088\t9\t-31560\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "ZGHW\t50.258904695510864\t38.42254384471547\t597366062\t21\t23702\t7037372650941669660\t1970-01-04T06:00:00.000000Z\n" +
                        "LOPJ\t76.6814649105072\t5.158459929273784\t1920398380\t38\t16628\t3527911398466283309\t1970-01-04T06:00:00.000000Z\n" +
                        "VDKF\t4.360640048980713\t35.68111021227658\t503883303\t38\t10895\t7202923278768687325\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-55.76850175857544\t-319.0336091902473\t5119558182\t-122\t76527\t-4242757608817349120\t1970-01-04T06:00:00.000000Z\n" +
                        "\t81.00891510645549\t91.61384988472523\t1649933352\t27\t11104\t9223372036854775807\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t373.71920943260193\t-347.2420430773538\t17639344535\t-183\t-181388\t-8457384498316854272\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-446.34361267089844\t695.3436351697343\t-5260876975\t9\t-36939\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "OXPK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "OXPK\t45.9206759929657\t76.06252634124596\t2043541236\t21\t19278\t1832315370633201942\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t-74.72566366195679\t-384.9689402974543\t5895771596\t-149\t85688\t-6473850599404687360\t1970-01-04T09:00:00.000000Z\n" +
                        "\t63.42712839444481\t84.38758882057594\t1387805620\t24\t4384\t9223372036854775807\t1970-01-04T09:00:00.000000Z\n" +
                        "CPSW\t407.7303051948548\t-389.6419204558852\t19360984835\t-204\t-202758\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t-504.989355802536\t771.21481216745\t-6077116861\t9\t-42318\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "ZGHW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "LOPJ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "VDKF\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillAllTypesLinearNoData() throws Exception {
        // sum_t tests memory leak
        assertQuery("b\tsum_t\tsum\tsum1\tsum2\tsum3\tsum4\tk\n",
                "select b, sum_t(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum_t\tsum\tsum1\tsum2\tsum3\tsum4\tk\n" +
                        "\t0.3598332405090332\t32.881769076795045\t1253890363\t49\t27809\t7199909180655756830\t1970-01-04T03:00:00.000000Z\n" +
                        "DEYY\t164.43474292755127\t117.53515866608913\t22049944\t64\t3136\t4552387273114894848\t1970-01-04T03:00:00.000000Z\n" +
                        "SXUX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t24.808812141418457\t63.81607531178513\t461611463\t48\t514\t7709707078566863064\t1970-01-04T06:00:00.000000Z\n" +
                        "DEYY\t96.8742311000824\t67.00476391801053\t44173540\t34\t3282\t6794405451419334859\t1970-01-04T06:00:00.000000Z\n" +
                        "SXUX\t26.92210078239441\t52.98405941762054\t936627841\t16\t5741\t7153335833712179123\t1970-01-04T06:00:00.000000Z\n" +
                        "DEYY\t29.313719272613525\t16.47436916993191\t66297136\t4\t3428\t9036423629723776443\t1970-01-04T09:00:00.000000Z\n" +
                        "\t49.25779104232788\t94.75038154677522\t-330667436\t47\t-26781\t8219504976477969408\t1970-01-04T09:00:00.000000Z\n" +
                        "SXUX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillLinear() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t60.419130298418445\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t269.0808495558698\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t44.39196261932496\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t183.3959405081909\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t46.60623681895594\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t82.9603306085581\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t51.034785218217934\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-73.65878663484577\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t53.249059417848926\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-159.3436956825247\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t55.463333617479904\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t13.557627225594155\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-245.0286047302036\t1970-01-03T18:00:00.000000Z\n",

                "select b, sum(a), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t60.419130298418445\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t269.0808495558698\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t44.39196261932496\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t183.3959405081909\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t46.60623681895594\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t82.9603306085581\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t51.034785218217934\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t-73.65878663484577\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t53.249059417848926\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t-159.3436956825247\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t55.463333617479904\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t13.557627225594155\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t-245.0286047302036\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t75.55713454429453\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t57.67760781711089\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t-21.889850047664094\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t-330.7135137778825\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t65.02434237974201\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t59.891882016741896\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t-57.337327320922356\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t-416.39842282556134\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t62.10615621637288\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t-92.78480459418059\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t-502.0833318732403\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t64.32043041600387\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t-128.23228186743887\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t-587.7682409209192\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n" +
                        "\t217.1804173491625\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t66.53470461563491\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t-163.67975914069712\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t-673.453149968598\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T09:00:00.000000Z\n",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillLinearBadType() throws Exception {
        assertFailure(
                "select b, sum_t(b), k from x sample by 3h fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(1,1,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "Unsupported type"
        );
    }

    @Test
    public void testSampleFillLinearByMonth() throws Exception {
        assertQuery("b\tsum_t\tk\n" +
                        "\t54112.40405938657\t1970-01-01T00:00:00.000000Z\n" +
                        "VTJW\t11209.880434660998\t1970-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t9939.438287132381\t1970-01-01T00:00:00.000000Z\n" +
                        "PEHN\t11042.882403279875\t1970-01-01T00:00:00.000000Z\n" +
                        "HYRX\t11080.174817969955\t1970-01-01T00:00:00.000000Z\n" +
                        "CPSW\t9310.397369439\t1970-01-01T00:00:00.000000Z\n" +
                        "\t53936.039113863764\t1970-04-01T00:00:00.000000Z\n" +
                        "HYRX\t10382.092656987053\t1970-04-01T00:00:00.000000Z\n" +
                        "CPSW\t11677.451781387846\t1970-04-01T00:00:00.000000Z\n" +
                        "RXGZ\t12082.97398092452\t1970-04-01T00:00:00.000000Z\n" +
                        "VTJW\t11574.354700279142\t1970-04-01T00:00:00.000000Z\n" +
                        "PEHN\t11225.427167029598\t1970-04-01T00:00:00.000000Z\n" +
                        "\t53719.38559836983\t1970-07-01T00:00:00.000000Z\n" +
                        "VTJW\t10645.216313875992\t1970-07-01T00:00:00.000000Z\n" +
                        "RXGZ\t12441.881371617534\t1970-07-01T00:00:00.000000Z\n" +
                        "HYRX\t10478.918039106036\t1970-07-01T00:00:00.000000Z\n" +
                        "CPSW\t11215.534064219255\t1970-07-01T00:00:00.000000Z\n" +
                        "PEHN\t12053.625707887684\t1970-07-01T00:00:00.000000Z\n" +
                        "\t54106.362147164444\t1970-10-01T00:00:00.000000Z\n" +
                        "HYRX\t11883.354138407445\t1970-10-01T00:00:00.000000Z\n" +
                        "RXGZ\t11608.715762809448\t1970-10-01T00:00:00.000000Z\n" +
                        "CPSW\t11623.362686708584\t1970-10-01T00:00:00.000000Z\n" +
                        "PEHN\t11258.550294609915\t1970-10-01T00:00:00.000000Z\n" +
                        "VTJW\t10865.136275604094\t1970-10-01T00:00:00.000000Z\n" +
                        "\t33152.56289929654\t1971-01-01T00:00:00.000000Z\n" +
                        "PEHN\t7219.25966062438\t1971-01-01T00:00:00.000000Z\n" +
                        "CPSW\t6038.83487182006\t1971-01-01T00:00:00.000000Z\n" +
                        "RXGZ\t5862.505042201944\t1971-01-01T00:00:00.000000Z\n" +
                        "VTJW\t6677.581919995402\t1971-01-01T00:00:00.000000Z\n" +
                        "HYRX\t5998.730211949621\t1971-01-01T00:00:00.000000Z\n",
                "select b, sum_t(a), k from x sample by 3M fill(linear)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(10000)" +
                        ") timestamp(k) partition by NONE",
                "k",
                true,
                true,
                true
        );
    }

    @Test
    public void testSampleFillLinearConstructorFail() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as " +
                            "(" +
                            "select" +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " timestamp_sequence(172800000000, 3600000000) k" +
                            " from" +
                            " long_sequence(20000000)" +
                            ") timestamp(k) partition by NONE",
                    sqlExecutionContext
            );

            FilesFacade ff = new FilesFacadeImpl() {
                int count = 4;

                @Override
                public long mmap(long fd, long len, long offset, int flags) {
                    if (count-- > 0) {
                        return super.mmap(fd, len, offset, flags);
                    }
                    return -1;
                }
            };

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (SqlCompiler compiler = new SqlCompiler(engine)) {
                    try {
                        compiler.compile("select b, sum(a), k from x sample by 3h fill(linear)", sqlExecutionContext);
                        Assert.fail();
                    } catch (SqlException e) {
                        Assert.assertTrue(Chars.contains(e.getMessage(), "Could not mmap"));
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
            }
        });
    }

    @Test
    public void testSampleFillLinearFail() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table x as " +
                            "(" +
                            "select" +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " timestamp_sequence(172800000000, 3600000000) k" +
                            " from" +
                            " long_sequence(20000000)" +
                            ") timestamp(k) partition by NONE",
                    sqlExecutionContext
            );

            FilesFacade ff = new FilesFacadeImpl() {
                int count = 10;

                @Override
                public long mmap(long fd, long len, long offset, int flags) {
                    if (count-- > 0) {
                        return super.mmap(fd, len, offset, flags);
                    }
                    return -1;
                }
            };

            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (SqlCompiler compiler = new SqlCompiler(engine)) {
                    try {
                        try (RecordCursorFactory factory = compiler.compile("select b, sum(a), k from x sample by 3h fill(linear)", sqlExecutionContext).getRecordCursorFactory()) {
                            // with mmap count = 5 we should get failure in cursor
                            factory.getCursor(sqlExecutionContext);
                        }
                        Assert.fail();
                    } catch (CairoException e) {
                        Assert.assertTrue(Chars.contains(e.getMessage(), "Could not mmap"));
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                    Assert.assertEquals(0, engine.getBusyWriterCount());
                }
            }
        });
    }

    @Test
    public void testSampleFillNone() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneAllTypes() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.19752505948932\t113.1212830543518\t2557447177\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.35983672154330515\t76.75672769546509\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.64256753596138\t55.224937200546265\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900255\t34.356850385665894\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599554755\t12.503039836883545\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.05940141744613\t92.16078519821167\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.64158914718531\t88.37421536445618\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.78118249687527\t103.11979651451111\t3029605432\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863680992\t42.020440101623535\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.60937843256664\t189.81727361679077\t3717804370\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107102\t76.57837271690369\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621126815\t139.30693864822388\t2587989045\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.6836863013701473\t10.643041133880615\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.17160551750754\t120.51888704299927\t2362241402\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.19752505948932\t113.1212830543518\t2557447177\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.35983672154330515\t76.75672769546509\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.64256753596138\t55.224937200546265\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900255\t34.356850385665894\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599554755\t12.503039836883545\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.05940141744613\t92.16078519821167\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.64158914718531\t88.37421536445618\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.78118249687527\t103.11979651451111\t3029605432\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863680992\t42.020440101623535\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.60937843256664\t189.81727361679077\t3717804370\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107102\t76.57837271690369\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621126815\t139.30693864822388\t2587989045\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.6836863013701473\t10.643041133880615\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.17160551750754\t120.51888704299927\t2362241402\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t20.585069039325443\t98.84010553359985\t1278547815\t17250\t3\t-6703401424236463520\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "EZGH\t5.0246156790690115\t38.42254281044006\t370796356\t5422\t3\t4959459375462458218\t1970-01-04T06:00:00.000000Z\n" +
                        "FLOP\t17.180291960857296\t5.158454179763794\t532016913\t-3028\t7\t2282781332678491916\t1970-01-04T06:00:00.000000Z\n" +
                        "WVDK\t54.66900921405317\t35.68110466003418\t874367915\t-23001\t10\t9089874911309539983\t1970-01-04T06:00:00.000000Z\n" +
                        "JOXP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "CPSW\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "EZGH\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "FLOP\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "WVDK\tNaN\tNaN\tNaN\tNaN\tNaN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "JOXP\t67.29405590773638\t76.06252431869507\t1165635863\t2316\t9\t-4547802916868961458\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneDataGaps() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T01:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T02:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "\t87.99634725391621\t1970-01-03T04:00:00.000000Z\n" +
                        "\t32.881769076795045\t1970-01-03T05:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T07:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T08:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T10:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T11:00:00.000000Z\n" +
                        "\t52.98405941762054\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T13:00:00.000000Z\n" +
                        "\t97.5019885372507\t1970-01-03T14:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "\t80.01121139739173\t1970-01-03T16:00:00.000000Z\n" +
                        "\t92.050039469858\t1970-01-03T17:00:00.000000Z\n" +
                        "\t45.6344569609078\t1970-01-03T18:00:00.000000Z\n" +
                        "\t40.455469747939254\t1970-01-03T19:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 30m fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T01:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T02:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "\t87.99634725391621\t1970-01-03T04:00:00.000000Z\n" +
                        "\t32.881769076795045\t1970-01-03T05:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T07:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T08:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T10:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T11:00:00.000000Z\n" +
                        "\t52.98405941762054\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T13:00:00.000000Z\n" +
                        "\t97.5019885372507\t1970-01-03T14:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "\t80.01121139739173\t1970-01-03T16:00:00.000000Z\n" +
                        "\t92.050039469858\t1970-01-03T17:00:00.000000Z\n" +
                        "\t45.6344569609078\t1970-01-03T18:00:00.000000Z\n" +
                        "\t40.455469747939254\t1970-01-03T19:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T05:00:00.000000Z\n" +
                        "\t76.9238189433781\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T07:00:00.000000Z\n" +
                        "\t58.912164838797885\t1970-01-04T08:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneEmpty() throws Exception {
        assertQuery("b\tsum_t\tk\n",
                "select b, sum_t(a), k from x sample by 2h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum_t\tk\n" +
                        "IBBT\t0.35983672154330515\t1970-01-04T05:00:00.000000Z\n" +
                        "\t76.75673070796104\t1970-01-04T05:00:00.000000Z\n" +
                        "\t125.98934239031611\t1970-01-04T07:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneNotKeyed() throws Exception {
        assertQuery("sum\tk\n" +
                        "77.51096330391545\t1970-01-03T00:00:00.000000Z\n" +
                        "191.82172120242328\t1970-01-03T03:00:00.000000Z\n" +
                        "237.11377417413973\t1970-01-03T06:00:00.000000Z\n" +
                        "87.76873691116495\t1970-01-03T09:00:00.000000Z\n" +
                        "234.93862972698187\t1970-01-03T12:00:00.000000Z\n" +
                        "221.06635536610213\t1970-01-03T15:00:00.000000Z\n" +
                        "86.08992670884706\t1970-01-03T18:00:00.000000Z\n",
                "select sum(a), k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "sum\tk\n" +
                        "77.51096330391545\t1970-01-03T00:00:00.000000Z\n" +
                        "191.82172120242328\t1970-01-03T03:00:00.000000Z\n" +
                        "237.11377417413973\t1970-01-03T06:00:00.000000Z\n" +
                        "87.76873691116495\t1970-01-03T09:00:00.000000Z\n" +
                        "234.93862972698187\t1970-01-03T12:00:00.000000Z\n" +
                        "221.06635536610213\t1970-01-03T15:00:00.000000Z\n" +
                        "86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "185.26488890176051\t1970-01-04T06:00:00.000000Z\n" +
                        "67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNoneNotKeyedEmpty() throws Exception {
        assertQuery("sum\tk\n",
                "select sum(a), k from x sample by 3h fill(none)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "sum\tk\n" +
                        "139.2898345080353\t1970-01-04T05:00:00.000000Z\n" +
                        "121.75073858040724\t1970-01-04T08:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNull() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNullBadType() throws Exception {
        assertFailure(
                "select b, sum_t(b), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(1,1,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "Unsupported type"
        );
    }

    @Test
    public void testSampleFillNullDay() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t7275.778376911272\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t1883.352722741196\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t1778.991207981299\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t1320.0312922751193\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t1331.6811166028579\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t1028.7651538511032\t1970-01-03T00:00:00.000000Z\n" +
                        "\t3197.327071423042\t1970-01-15T00:00:00.000000Z\n" +
                        "VTJW\t620.7711228918114\t1970-01-15T00:00:00.000000Z\n" +
                        "RXGZ\t352.08258484411346\t1970-01-15T00:00:00.000000Z\n" +
                        "PEHN\t535.1155923549986\t1970-01-15T00:00:00.000000Z\n" +
                        "HYRX\t646.1950909401153\t1970-01-15T00:00:00.000000Z\n" +
                        "CPSW\t751.4428172676351\t1970-01-15T00:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 12d fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(400)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillNullDayNotKeyed() throws Exception {
        assertQuery("sum\tk\n" +
                        "14618.599870362843\t1970-01-03T00:00:00.000000Z\n" +
                        "6102.934279721718\t1970-01-15T00:00:00.000000Z\n",
                "select sum(a), k from x sample by 12d fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(400)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillNullDayNotKeyedGaps() throws Exception {
        assertQuery("sum\tk\n" +
                        "11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "NaN\t1970-01-04T00:00:00.000000Z\n" +
                        "42.17768841969397\t1970-01-05T00:00:00.000000Z\n" +
                        "NaN\t1970-01-06T00:00:00.000000Z\n" +
                        "23.90529010846525\t1970-01-07T00:00:00.000000Z\n" +
                        "NaN\t1970-01-08T00:00:00.000000Z\n" +
                        "70.94360487171201\t1970-01-09T00:00:00.000000Z\n" +
                        "NaN\t1970-01-10T00:00:00.000000Z\n" +
                        "87.99634725391621\t1970-01-11T00:00:00.000000Z\n" +
                        "NaN\t1970-01-12T00:00:00.000000Z\n" +
                        "32.881769076795045\t1970-01-13T00:00:00.000000Z\n" +
                        "NaN\t1970-01-14T00:00:00.000000Z\n" +
                        "97.71103146051203\t1970-01-15T00:00:00.000000Z\n" +
                        "NaN\t1970-01-16T00:00:00.000000Z\n" +
                        "81.46807944500559\t1970-01-17T00:00:00.000000Z\n" +
                        "NaN\t1970-01-18T00:00:00.000000Z\n" +
                        "57.93466326862211\t1970-01-19T00:00:00.000000Z\n" +
                        "NaN\t1970-01-20T00:00:00.000000Z\n" +
                        "12.026122412833129\t1970-01-21T00:00:00.000000Z\n" +
                        "NaN\t1970-01-22T00:00:00.000000Z\n" +
                        "48.820511018586934\t1970-01-23T00:00:00.000000Z\n" +
                        "NaN\t1970-01-24T00:00:00.000000Z\n" +
                        "26.922103479744898\t1970-01-25T00:00:00.000000Z\n" +
                        "NaN\t1970-01-26T00:00:00.000000Z\n" +
                        "52.98405941762054\t1970-01-27T00:00:00.000000Z\n" +
                        "NaN\t1970-01-28T00:00:00.000000Z\n" +
                        "84.45258177211063\t1970-01-29T00:00:00.000000Z\n" +
                        "NaN\t1970-01-30T00:00:00.000000Z\n" +
                        "97.5019885372507\t1970-01-31T00:00:00.000000Z\n" +
                        "NaN\t1970-02-01T00:00:00.000000Z\n" +
                        "49.00510449885239\t1970-02-02T00:00:00.000000Z\n" +
                        "NaN\t1970-02-03T00:00:00.000000Z\n" +
                        "80.01121139739173\t1970-02-04T00:00:00.000000Z\n" +
                        "NaN\t1970-02-05T00:00:00.000000Z\n" +
                        "92.050039469858\t1970-02-06T00:00:00.000000Z\n" +
                        "NaN\t1970-02-07T00:00:00.000000Z\n" +
                        "45.6344569609078\t1970-02-08T00:00:00.000000Z\n" +
                        "NaN\t1970-02-09T00:00:00.000000Z\n" +
                        "40.455469747939254\t1970-02-10T00:00:00.000000Z\n",
                "select sum(a), k from x sample by 1d fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 2*24*3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillNullMonth() throws Exception {
        assertQuery(
                "b\tsum\tk\n" +
                        "\t55342.35808049189\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t11567.037431069954\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t10201.524212064816\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t11202.70624166601\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t11272.942252172881\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t9549.728356483236\t1970-01-03T00:00:00.000000Z\n" +
                        "\t800.2014170461989\t1970-04-03T00:00:00.000000Z\n" +
                        "VTJW\t323.2593477270021\t1970-04-03T00:00:00.000000Z\n" +
                        "RXGZ\t272.6474603488786\t1970-04-03T00:00:00.000000Z\n" +
                        "PEHN\t200.0681408130791\t1970-04-03T00:00:00.000000Z\n" +
                        "HYRX\t294.5937385165431\t1970-04-03T00:00:00.000000Z\n" +
                        "CPSW\t205.63949754449527\t1970-04-03T00:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3M fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(2200)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillNullNotKeyedEmpty() throws Exception {
        assertQuery("sum\tk\n",
                "select sum(a), k from x sample by 3h fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "sum\tk\n" +
                        "139.2898345080353\t1970-01-04T05:00:00.000000Z\n" +
                        "121.75073858040724\t1970-01-04T08:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillNullYear() throws Exception {
        assertQuery(
                "b\tsum\tk\n" +
                        "\t434760.12538229994\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t87709.77375105419\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t88905.80881686346\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t91380.17439538923\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t87322.92024549567\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t87815.15120829944\t1970-01-03T00:00:00.000000Z\n" +
                        "\t313710.2720747207\t1972-01-03T00:00:00.000000Z\n" +
                        "VTJW\t60726.582461611106\t1972-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t63630.846278341654\t1972-01-03T00:00:00.000000Z\n" +
                        "PEHN\t64225.437480744265\t1972-01-03T00:00:00.000000Z\n" +
                        "HYRX\t64384.17131854287\t1972-01-03T00:00:00.000000Z\n" +
                        "CPSW\t60762.09297190822\t1972-01-03T00:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 2y fill(null)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(30000)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrev() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevAllTypes() throws Exception {
        assertQuery("a\tb\tc\td\te\tf\tg\ti\tj\tl\tm\tp\tsum\tk\n" +
                        "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\t1970-01-01T00:00:00.000000Z\t0.15786635599554755\t1970-01-03T00:00:00.000000Z\n" +
                        "-2132716300\ttrue\tU\t0.38179758047769774\tNaN\t813\t2015-07-01T22:08:50.655Z\tHYRX\t-6186964045554120476\t34\t00000000 07 42 fc 31 79 5f 8b 81 2b 93\t1970-01-01T01:00:00.000000Z\t0.04142812470232493\t1970-01-03T00:00:00.000000Z\n" +
                        "-360860352\ttrue\tM\t0.456344569609078\tNaN\t1013\t2015-01-15T20:11:07.487Z\tHYRX\t5271904137583983788\t30\t00000000 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64\n" +
                        "00000010 0e 2c\t1970-01-01T02:00:00.000000Z\t0.6752509547112409\t1970-01-03T00:00:00.000000Z\n" +
                        "2060263242\tfalse\tL\tNaN\t0.3495\t869\t2015-05-15T18:43:06.827Z\tCPSW\t-5439556746612026472\t11\t\t1970-01-01T03:00:00.000000Z\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "502711083\tfalse\tH\t0.0171850098561398\t0.0977\t605\t2015-07-12T07:33:54.007Z\tVTJW\t-6187389706549636253\t32\t00000000 29 8e 29 5e 69 c6 eb ea c3 c9 73\t1970-01-01T04:00:00.000000Z\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "1569490116\tfalse\tZ\tNaN\t0.7611\t428\t2015-05-16T20:27:48.158Z\tVTJW\t-8671107786057422727\t26\t00000000 68 61 26 af 19 c4 95 94 36 53 49\t1970-01-01T00:00:00.000000Z\t0.15786635599554755\t1970-01-03T03:00:00.000000Z\n" +
                        "-2132716300\ttrue\tU\t0.38179758047769774\tNaN\t813\t2015-07-01T22:08:50.655Z\tHYRX\t-6186964045554120476\t34\t00000000 07 42 fc 31 79 5f 8b 81 2b 93\t1970-01-01T01:00:00.000000Z\t0.04142812470232493\t1970-01-03T03:00:00.000000Z\n" +
                        "-360860352\ttrue\tM\t0.456344569609078\tNaN\t1013\t2015-01-15T20:11:07.487Z\tHYRX\t5271904137583983788\t30\t00000000 82 89 2b 4d 5f f6 46 90 c3 b3 59 8e e5 61 2f 64\n" +
                        "00000010 0e 2c\t1970-01-01T02:00:00.000000Z\t0.6752509547112409\t1970-01-03T03:00:00.000000Z\n" +
                        "2060263242\tfalse\tL\tNaN\t0.3495\t869\t2015-05-15T18:43:06.827Z\tCPSW\t-5439556746612026472\t11\t\t1970-01-01T03:00:00.000000Z\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "502711083\tfalse\tH\t0.0171850098561398\t0.0977\t605\t2015-07-12T07:33:54.007Z\tVTJW\t-6187389706549636253\t32\t00000000 29 8e 29 5e 69 c6 eb ea c3 c9 73\t1970-01-01T04:00:00.000000Z\t0.22631523434159562\t1970-01-03T03:00:00.000000Z\n",
                "select a,b,c,d,e,f,g,i,j,l,m,p,sum(o), k from x sample by 3h fill(prev)",
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
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(0, 3600000000) p," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrevDuplicateKey() throws Exception {
        assertQuery("b\tb1\tb2\tsum\tk\n" +
                        "\t\t\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t\t\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "\t\t\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "\t\t\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\n" +
                        "\t\t\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\n" +
                        "\t\t\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\n",
                "select b, b, b, sum(a), k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tb1\tb2\tsum\tk\n" +
                        "\t\t\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T00:00:00.000000Z\n" +
                        "\t\t\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T06:00:00.000000Z\n" +
                        "\t\t\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T09:00:00.000000Z\n" +
                        "\t\t\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T12:00:00.000000Z\n" +
                        "\t\t\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T15:00:00.000000Z\n" +
                        "\t\t\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T18:00:00.000000Z\n" +
                        "\t\t\t86.08992670884706\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-03T21:00:00.000000Z\n" +
                        "\t\t\t86.08992670884706\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-04T00:00:00.000000Z\n" +
                        "\t\t\t54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-04T03:00:00.000000Z\n" +
                        "\t\t\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\tNaN\t1970-01-04T06:00:00.000000Z\n" +
                        "\t\t\t135.835983782176\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\tVTJW\tVTJW\t48.820511018586934\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\tRXGZ\tRXGZ\t23.90529010846525\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\tPEHN\tPEHN\t49.00510449885239\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\tHYRX\tHYRX\t12.026122412833129\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\tUVSD\tUVSD\t49.42890511958454\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\tKGHV\tKGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevDuplicateTimestamp1() throws Exception {
        assertQuery("b\tsum\tk\tk1\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k, k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\tk1\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevDuplicateTimestamp2() throws Exception {
        assertQuery("b\tsum\tk1\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k k1, k from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k1",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk1\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T03:00:00.000000Z\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T06:00:00.000000Z\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T09:00:00.000000Z\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T12:00:00.000000Z\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T15:00:00.000000Z\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T18:00:00.000000Z\t1970-01-03T18:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-03T21:00:00.000000Z\t1970-01-03T21:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T00:00:00.000000Z\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T03:00:00.000000Z\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\tNaN\t1970-01-04T06:00:00.000000Z\t1970-01-04T06:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillPrevEmptyBase() throws Exception {
        assertQuery(null,
                "select a,b,c,d,e,f,g,i,j,l,m,p,sum(o), k from x where 0!=0 sample by 3h fill(prev)",
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
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(0, 3600000000) p," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrevNoTimestamp() throws Exception {
        assertQuery("b\tsum\n" +
                        "\t11.427984775756228\n" +
                        "VTJW\t42.17768841969397\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\tNaN\n" +
                        "HYRX\tNaN\n" +
                        "\t120.87811633071126\n" +
                        "VTJW\t42.17768841969397\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t70.94360487171201\n" +
                        "HYRX\tNaN\n" +
                        "\t57.93466326862211\n" +
                        "VTJW\t42.17768841969397\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t81.46807944500559\n" +
                        "HYRX\t97.71103146051203\n" +
                        "\t26.922103479744898\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t81.46807944500559\n" +
                        "HYRX\t12.026122412833129\n" +
                        "\t150.48604795487125\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t84.45258177211063\n" +
                        "HYRX\t12.026122412833129\n" +
                        "\t172.06125086724973\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "\t86.08992670884706\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n",
                "select b, sum(a) from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\n" +
                        "\t11.427984775756228\n" +
                        "VTJW\t42.17768841969397\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\tNaN\n" +
                        "HYRX\tNaN\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t120.87811633071126\n" +
                        "VTJW\t42.17768841969397\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t70.94360487171201\n" +
                        "HYRX\tNaN\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t57.93466326862211\n" +
                        "VTJW\t42.17768841969397\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t81.46807944500559\n" +
                        "HYRX\t97.71103146051203\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t26.922103479744898\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t81.46807944500559\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t150.48604795487125\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t84.45258177211063\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t172.06125086724973\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t86.08992670884706\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t86.08992670884706\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t86.08992670884706\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t54.49155021518948\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\tNaN\n" +
                        "KGHV\tNaN\n" +
                        "\t135.835983782176\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\t49.42890511958454\n" +
                        "KGHV\tNaN\n" +
                        "\t135.835983782176\n" +
                        "VTJW\t48.820511018586934\n" +
                        "RXGZ\t23.90529010846525\n" +
                        "PEHN\t49.00510449885239\n" +
                        "HYRX\t12.026122412833129\n" +
                        "UVSD\t49.42890511958454\n" +
                        "KGHV\t67.52509547112409\n",
                false);
    }

    @Test
    public void testSampleFillPrevNoTimestampLong256AndChar() throws Exception {
        assertQuery("a\tb\tsum\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\tNaN\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\tNaN\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\tNaN\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\tNaN\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\tNaN\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\tNaN\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\tNaN\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\tNaN\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\tNaN\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\tNaN\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\tNaN\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\tNaN\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\tNaN\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\tNaN\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\tNaN\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\tNaN\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\tNaN\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\tNaN\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n",
                "select a, b, sum(c) from x sample by 3h fill(prev)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_long256() a," +
                        " rnd_char() b," +
                        " rnd_double() c, " +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_long256() a," +
                        " rnd_char() b," +
                        " rnd_double() c, " +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "a\tb\tsum\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\tNaN\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\tNaN\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\tNaN\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\tNaN\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\tNaN\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\tNaN\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\tNaN\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\tNaN\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\tNaN\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\tNaN\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\tNaN\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\tNaN\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\tNaN\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\tNaN\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\tNaN\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\tNaN\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\tNaN\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\tNaN\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\tNaN\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\tNaN\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\tNaN\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\tNaN\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\tNaN\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\tNaN\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\tNaN\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\tNaN\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\tNaN\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\t0.4971342426836798\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\tNaN\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\tNaN\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\tNaN\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\t0.4971342426836798\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\t0.44804689668613573\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\t0.2879973939681931\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\t0.24008362859107102\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\tNaN\n" +
                        "0x9f9b2131d49fcd1d6b8139815c50d3410010cde812ce60ee0010a928bb8b9650\tC\t0.2845577791213847\n" +
                        "0x797fa69eb8fec6cce8beef38cd7bb3d8db2d34586f6275fab5b2159a23565217\tX\t0.8423410920883345\n" +
                        "0x716de3d25dcc2d919fa2397a5d8c84c4c1e631285c1ab288c72bfc5230158059\tG\t0.3491070363730514\n" +
                        "0xc8b1863d4316f9c773b27651a916ab1b568bc2d7a4aa860483881d4171847cf3\tB\t0.5599161804800813\n" +
                        "0x61b1a0b0a559551538b73d329210d2774cdfb9e29522133c87aa0968faec6879\tF\t0.6693837147631712\n" +
                        "0x74ce62a98a4516952705e02c613acfc405374f5fbcef4819523eb59d99c647af\tY\t0.8799634725391621\n" +
                        "0x7f98b0c74238337e36ee542d654d22598a538661f350d0b46f06560981acb549\tO\t0.021651819007252326\n" +
                        "0xcec82869edec121bc2593f82b430328d84a09f29df637e3863eb3740c80f661e\tS\t0.6381607531178513\n" +
                        "0x6afe61bd7c4ae0d84c0094500fbffdfe76fb2001fe5dfb09acea66fbe47c5e39\tS\t0.9687423276940171\n" +
                        "0x94cfe42988a633de738bab883dc7e3323239ad1b0411a66a10bb226eb4243e36\tQ\t0.3435685332942956\n" +
                        "0x2bbfcf66bab932fc5ea744ebab75d542a937c9ce75e81607a1b56c3d802c4735\tG\t0.4138164748227684\n" +
                        "0x98c2d832d83de9934a0705e1136e872b3ad08d6037d3ce8155c06051ee52138b\tS\t0.7763904674818695\n" +
                        "0x60802a2ca499f211b771e27f939096b9c356f99ae70523b585b80cec619f9178\tT\t0.4900510449885239\n" +
                        "0x6a0accd425e948d49a77e857727e751a7d67d36a09a1b5bb2932c3ad61000d64\tZ\t0.38642336707855873\n" +
                        "0x687a84abb7bfac3ebedf29efb28cdcb1b75dccbdf1f8b84b9b27eba5e9cfa1e2\tI\t0.6590341607692226\n" +
                        "0x30d46a3a4749c41d7a902c77fa1a889c51686790e59377ca68653a6cd896f81e\tI\t0.5659429139861241\n" +
                        "0x997918f622d62989c009aea26fdde482ba37e200ad5b17cdada00dc8b85c1bc8\tV\t0.45659895188239796\n" +
                        "0x7d4f1da8fd48b2c3d364c241dde2cf90a7a8f4e549997e46516e1efd8bbcecf6\tS\t0.5778947915182423\n" +
                        "0xbbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea6162d6d100c964eee5\tG\t0.325403220015421\n" +
                        "0x7ebaf6ca993f8fc98b1309cf32d68bb8aa7dc4eccb68146fb37f1ec82752c7d7\tC\t0.49428905119584543\n" +
                        "0xaa1896d0ad3419d2910aa7b6d58506dc7c97a2cb4ac4b04722556b928447b584\tD\t0.4971342426836798\n" +
                        "0x0cb5f439cbc22e9d1f0481ab7acd1f4a77827c4f6b03027bc6dfacdd3f3c52b8\tO\t0.44804689668613573\n" +
                        "0xacb025f759cffbd0de9be4e331fe36e67dc859770af204938151081b8acafadd\tB\t0.2879973939681931\n" +
                        "0x9d6cb7b4fbf1fa48dbd7587f207765769b4bae41862e09ccb482cff57e9c5398\tK\t0.24008362859107102\n" +
                        "0xaf44c40a67ef5e1c5b3ef21223ee884965009e89eacf0aadd25adf928386cdd2\tQ\t0.7446000371089992\n",
                false);
    }

    @Test
    public void testSampleFillPrevNotKeyed() throws Exception {
        assertQuery("sum\tk\n" +
                        "0.8745454354091133\t1970-01-01T00:00:17.280000Z\n" +
                        "1.551810133791102\t1970-01-01T03:00:17.280000Z\n" +
                        "0.8214274286283418\t1970-01-01T06:00:17.280000Z\n" +
                        "1.2509938088155907\t1970-01-01T09:00:17.280000Z\n" +
                        "1.374822334421568\t1970-01-01T12:00:17.280000Z\n" +
                        "1.2326807412877587\t1970-01-01T15:00:17.280000Z\n" +
                        "2.1719710889714183\t1970-01-01T18:00:17.280000Z\n" +
                        "1.4447447064927308\t1970-01-01T21:00:17.280000Z\n" +
                        "1.6328006113717726\t1970-01-02T00:00:17.280000Z\n" +
                        "1.2084207597347858\t1970-01-02T03:00:17.280000Z\n" +
                        "2.3834635376399724\t1970-01-02T06:00:17.280000Z\n" +
                        "1.6181165075977018\t1970-01-02T09:00:17.280000Z\n" +
                        "1.618269955964484\t1970-01-02T12:00:17.280000Z\n" +
                        "2.090927105391142\t1970-01-02T15:00:17.280000Z\n" +
                        "1.2480423712293227\t1970-01-02T18:00:17.280000Z\n" +
                        "2.587279129812145\t1970-01-02T21:00:17.280000Z\n" +
                        "1.467047661180466\t1970-01-03T00:00:17.280000Z\n" +
                        "0.629161709851853\t1970-01-03T03:00:17.280000Z\n" +
                        "2.191264288796364\t1970-01-03T06:00:17.280000Z\n" +
                        "1.3805554422849617\t1970-01-03T09:00:17.280000Z\n" +
                        "1.8906117848689568\t1970-01-03T12:00:17.280000Z\n" +
                        "1.914394848761218\t1970-01-03T15:00:17.280000Z\n" +
                        "1.7077466009740325\t1970-01-03T18:00:17.280000Z\n" +
                        "2.9091868315808678\t1970-01-03T21:00:17.280000Z\n" +
                        "0.34488282893630495\t1970-01-04T00:00:17.280000Z\n" +
                        "1.7359881138274678\t1970-01-04T03:00:17.280000Z\n" +
                        "1.1047508985515524\t1970-01-04T06:00:17.280000Z\n" +
                        "0.9365397496939732\t1970-01-04T09:00:17.280000Z\n" +
                        "1.676203094841128\t1970-01-04T12:00:17.280000Z\n" +
                        "1.28493295522627\t1970-01-04T15:00:17.280000Z\n" +
                        "1.23855454246846\t1970-01-04T18:00:17.280000Z\n" +
                        "1.3334813459559705\t1970-01-04T21:00:17.280000Z\n" +
                        "0.8049508417119063\t1970-01-05T00:00:17.280000Z\n" +
                        "0.9618013985447664\t1970-01-05T03:00:17.280000Z\n",
                "select sum(o), k from x sample by 3h fill(prev)",
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
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(0, 3600000000) p," +
                        " timestamp_sequence(17280000, 3000000000) k" +
                        " from" +
                        " long_sequence(120)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false
        );
    }

    @Test
    public void testSampleFillPrevNotKeyedEmpty() throws Exception {
        assertQuery("sum\tk\n",
                "select sum(o), k from x sample by 3h fill(prev)",
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
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(0, 3600000000) p," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from " +
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
                        " rnd_byte(2,50) l," +
                        " rnd_bin(10, 20, 2) m," +
                        " rnd_str(5,16,2) n," +
                        " rnd_double(2) o," +
                        " timestamp_sequence(0, 3600000000) p," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "sum\tk\n" +
                        "1.7032973194368575\t1970-01-03T00:00:00.000000Z\n" +
                        "1.0412323041734997\t1970-01-03T03:00:00.000000Z\n",
                false
        );
    }

    @Test
    public void testSampleFillValue() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "\t11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "VTJW\t42.17768841969397\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T00:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T00:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T00:00:00.000000Z\n" +
                        "\t120.87811633071126\t1970-01-03T03:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T03:00:00.000000Z\n" +
                        "\t57.93466326862211\t1970-01-03T06:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t81.46807944500559\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T06:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T06:00:00.000000Z\n" +
                        "\t26.922103479744898\t1970-01-03T09:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T09:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T09:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T09:00:00.000000Z\n" +
                        "\t150.48604795487125\t1970-01-03T12:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t84.45258177211063\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T12:00:00.000000Z\n" +
                        "\t172.06125086724973\t1970-01-03T15:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T15:00:00.000000Z\n" +
                        "\t86.08992670884706\t1970-01-03T18:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T18:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T21:00:00.000000Z\n" +
                        "\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-04T00:00:00.000000Z\n" +
                        "\t54.49155021518948\t1970-01-04T03:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T03:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-04T03:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-04T03:00:00.000000Z\n" +
                        "\t135.835983782176\t1970-01-04T06:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T06:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T06:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-04T06:00:00.000000Z\n" +
                        "\t20.56\t1970-01-04T09:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T09:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-04T09:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillValueAllKeyTypes() throws Exception {
        assertQuery("b\th\ti\tj\tl\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t11.427984775756228\t42.17768311500549\t1432278050\t13216\t4\t5539350449504785212\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t12.026122412833129\t48.820507526397705\t458818940\t3282\t8\t-6253307669002054137\t1970-01-03T00:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t5.048190020054388\t0.11075139045715332\t66297136\t-5637\t7\t9036423629723776443\t1970-01-03T00:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t11.585982949541474\t81.64182305335999\t998315423\t-5585\t7\t8587391969565958670\t1970-01-03T03:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t19.751370382305055\t68.06873083114624\t544695670\t-1464\t6\t-5024542231726589509\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t84.3845956391477\t48.92743229866028\t1100812407\t-32358\t10\t5398991075259361292\t1970-01-03T03:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t85.93131480724348\t10.527276992797852\t2105201404\t5667\t8\t-8994301462266164776\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t63.412928948436154\t5.024611949920654\t1377625589\t-25710\t3\t2151565237758036093\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t85.84308438045007\t54.669004678726196\t903066492\t-2990\t4\t-1134031357796740497\t1970-01-03T06:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\tFFYUDEYY\t00000000 49 b4 59 7e 3b 08 a1 1e 38 8d 1b 9e f4 c8 39 09\t2015-09-16T21:59:49.857Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tGETJR\t\t2015-04-09T11:42:28.332Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tZVDZJ\t00000000 e3 f1 f1 1e ca 9c 1d 06 ac 37 c8 cd 82 89 2b 4d\t2015-08-26T10:57:26.275Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tLYXWCK\t00000000 47 dc d2 85 7f a5 b8 7b 4a 9d 46 7c 8d\t2015-07-13T12:15:31.895Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t\t00000000 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-08T06:16:03.023Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tVLJUM\t00000000 29 5e 69 c6 eb ea c3 c9 73 93 46 fe\t2015-06-28T03:15:43.251Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tHWVDKF\t00000000 f5 5d d0 eb 67 44 a7 6a 71 34 e0\t2015-12-05T03:07:39.553Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\tNZHZS\t\t2015-10-11T07:06:57.173Z\ttrue\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\tEBNDCQCE\t00000000 e9 0c ea 4e ea 8b f5 0f 2d b3\t2015-03-25T11:25:58.599Z\tfalse\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\tUIZUL\t00000000 54 52 d0 29 26 c5 aa da 18 ce 5f b2 8b 5c 54 90\t\ttrue\t21.485589614090927\t6.20274543762207\t358259591\t-29980\t8\t-8841102831894340636\t1970-01-03T09:00:00.000000Z\n",
                "select b, h, i, j, l, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " rnd_str(5,8,2) h," +
                        " rnd_bin(10, 20, 2) i," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) j," +
                        " rnd_boolean() l," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillValueAllTypes() throws Exception {
        assertQuery("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.19752505948932\t113.1212830543518\t2557447177\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.35983672154330515\t76.75672769546509\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.64256753596138\t55.224937200546265\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900255\t34.356850385665894\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599554755\t12.503039836883545\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.05940141744613\t92.16078519821167\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.64158914718531\t88.37421536445618\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.78118249687527\t103.11979651451111\t3029605432\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863680992\t42.020440101623535\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.60937843256664\t189.81727361679077\t3717804370\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107102\t76.57837271690369\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621126815\t139.30693864822388\t2587989045\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.6836863013701473\t10.643041133880615\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.17160551750754\t120.51888704299927\t2362241402\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n",
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.19752505948932\t113.1212830543518\t2557447177\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.35983672154330515\t76.75672769546509\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.64256753596138\t55.224937200546265\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900255\t34.356850385665894\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599554755\t12.503039836883545\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.05940141744613\t92.16078519821167\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.64158914718531\t88.37421536445618\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.78118249687527\t103.11979651451111\t3029605432\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863680992\t42.020440101623535\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.60937843256664\t189.81727361679077\t3717804370\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107102\t76.57837271690369\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621126815\t139.30693864822388\t2587989045\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.6836863013701473\t10.643041133880615\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.17160551750754\t120.51888704299927\t2362241402\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T21:00:00.000000Z\n" +
                        "\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T00:00:00.000000Z\n" +
                        "\t20.585069039325443\t98.84010553359985\t1278547815\t17250\t3\t-6703401424236463520\t1970-01-04T03:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T03:00:00.000000Z\n" +
                        "\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "EZGH\t5.0246156790690115\t38.42254281044006\t370796356\t5422\t3\t4959459375462458218\t1970-01-04T06:00:00.000000Z\n" +
                        "FLOP\t17.180291960857296\t5.158454179763794\t532016913\t-3028\t7\t2282781332678491916\t1970-01-04T06:00:00.000000Z\n" +
                        "WVDK\t54.66900921405317\t35.68110466003418\t874367915\t-23001\t10\t9089874911309539983\t1970-01-04T06:00:00.000000Z\n" +
                        "JOXP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T06:00:00.000000Z\n" +
                        "\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "EZGH\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "FLOP\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "WVDK\t20.56\t0.0\t0\t0\t0\t0\t1970-01-04T09:00:00.000000Z\n" +
                        "JOXP\t67.29405590773638\t76.06252431869507\t1165635863\t2316\t9\t-4547802916868961458\t1970-01-04T09:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillValueAllTypesAndTruncate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table x as " +
                            "(" +
                            "select" +
                            " rnd_double(0)*100 a," +
                            " rnd_symbol(5,4,4,1) b," +
                            " rnd_float(0)*100 c," +
                            " abs(rnd_int()) d," +
                            " rnd_short() e," +
                            " rnd_byte(3,10) f," +
                            " rnd_long() g," +
                            " timestamp_sequence(172800000000, 3600000000) k" +
                            " from" +
                            " long_sequence(20)" +
                            ") timestamp(k) partition by NONE",
                    sqlExecutionContext
            );

            try (final RecordCursorFactory factory = compiler.compile("select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0, 0)", sqlExecutionContext).getRecordCursorFactory()) {
                assertTimestamp("k", factory);
                String expected = "b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n" +
                        "\t74.19752505948932\t113.1212830543518\t2557447177\t868\t12\t-6307312481136788016\t1970-01-03T00:00:00.000000Z\n" +
                        "CPSW\t0.35983672154330515\t76.75672769546509\t113506296\t27809\t9\t-8889930662239044040\t1970-01-03T00:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T00:00:00.000000Z\n" +
                        "\t76.64256753596138\t55.224937200546265\t326010667\t-5741\t8\t7392877322819819290\t1970-01-03T03:00:00.000000Z\n" +
                        "CPSW\t13.450170570900255\t34.356850385665894\t410717394\t18229\t10\t6820495939660535106\t1970-01-03T03:00:00.000000Z\n" +
                        "PEHN\t15.786635599554755\t12.503039836883545\t264240638\t-7976\t6\t-8480005421611953360\t1970-01-03T03:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T03:00:00.000000Z\n" +
                        "\t85.05940141744613\t92.16078519821167\t301655269\t-14676\t12\t-2937111954994403426\t1970-01-03T06:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "PEHN\t86.64158914718531\t88.37421536445618\t1566901076\t-3017\t3\t-5028301966399563827\t1970-01-03T06:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T06:00:00.000000Z\n" +
                        "\t106.78118249687527\t103.11979651451111\t3029605432\t-2372\t12\t-1162868573414266742\t1970-01-03T09:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "RXGZ\t3.831785863680992\t42.020440101623535\t1254404167\t1756\t5\t8702525427024484485\t1970-01-03T09:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T09:00:00.000000Z\n" +
                        "\t117.60937843256664\t189.81727361679077\t3717804370\t-27064\t17\t2215137494070785317\t1970-01-03T12:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T12:00:00.000000Z\n" +
                        "HYRX\t24.008362859107102\t76.57837271690369\t2111250190\t-13252\t8\t7973684666911773753\t1970-01-03T12:00:00.000000Z\n" +
                        "\t28.087836621126815\t139.30693864822388\t2587989045\t11751\t17\t-8594661640328306402\t1970-01-03T15:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T15:00:00.000000Z\n" +
                        "HYRX\t2.6836863013701473\t10.643041133880615\t502711083\t-8221\t9\t-7709579215942154242\t1970-01-03T15:00:00.000000Z\n" +
                        "\t75.17160551750754\t120.51888704299927\t2362241402\t514\t11\t-2863260545700031392\t1970-01-03T18:00:00.000000Z\n" +
                        "CPSW\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "PEHN\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "RXGZ\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n" +
                        "HYRX\t20.56\t0.0\t0\t0\t0\t0\t1970-01-03T18:00:00.000000Z\n";

                assertCursor(expected, factory, false, true, false, false);
                // make sure we get the same outcome when we get factory to create new cursor
                assertCursor(expected, factory, false, true, false, false);
                // make sure strings, binary fields and symbols are compliant with expected record behaviour
                assertVariableColumns(factory, true);

                compiler.compile("truncate table x", sqlExecutionContext);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    sink.clear();
                    printer.print(cursor, factory.getMetadata(), true, sink);
                    TestUtils.assertEquals("b\tsum\tsum1\tsum2\tsum3\tsum4\tsum5\tk\n", sink);
                }
            }
        });
    }

    @Test
    public void testSampleFillValueBadType() throws Exception {
        assertFailure(
                "select b, sum_t(b), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_str(1,1,2) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                10,
                "Unsupported type"
        );
    }

    @Test
    public void testSampleFillValueEmpty() throws Exception {
        assertQuery("b\tsum\tk\n",
                "select b, sum(a), k from x sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillValueFromSubQuery() throws Exception {
        assertQuery("b\tsum\tk\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T02:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T08:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T08:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T14:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "\t40.455469747939254\t1970-01-03T17:00:00.000000Z\n",
                "select b, sum(a), k from (x latest by b) sample by 3h fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(277200000000, 3600000000) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "b\tsum\tk\n" +
                        "RXGZ\t23.90529010846525\t1970-01-03T02:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T02:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T05:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "HYRX\t12.026122412833129\t1970-01-03T08:00:00.000000Z\n" +
                        "VTJW\t48.820511018586934\t1970-01-03T08:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T08:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T11:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "PEHN\t49.00510449885239\t1970-01-03T14:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T14:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T17:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T20:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-03T23:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-04T02:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T05:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T05:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T05:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T05:00:00.000000Z\n" +
                        "UVSD\t49.42890511958454\t1970-01-04T05:00:00.000000Z\n" +
                        "\t20.56\t1970-01-04T05:00:00.000000Z\n" +
                        "KGHV\t20.56\t1970-01-04T05:00:00.000000Z\n" +
                        "RXGZ\t20.56\t1970-01-04T08:00:00.000000Z\n" +
                        "HYRX\t20.56\t1970-01-04T08:00:00.000000Z\n" +
                        "VTJW\t20.56\t1970-01-04T08:00:00.000000Z\n" +
                        "PEHN\t20.56\t1970-01-04T08:00:00.000000Z\n" +
                        "UVSD\t20.56\t1970-01-04T08:00:00.000000Z\n" +
                        "\t58.912164838797885\t1970-01-04T08:00:00.000000Z\n" +
                        "KGHV\t67.52509547112409\t1970-01-04T08:00:00.000000Z\n",
                false);
    }

    @Test
    public void testSampleFillValueInvalid() throws Exception {
        assertFailure(
                "select b, sum_t(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, none, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                94,
                "invalid number"
        );
    }

    @Test
    public void testSampleFillValueNotEnough() throws Exception {
        assertFailure(
                "select b, sum(a), sum(c), sum(d), sum(e), sum(f), sum(g), k from x sample by 3h fill(20.56, 0, 0, 0, 0)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_float(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_short() e," +
                        " rnd_byte(3,10) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                0,
                "not enough values"
        );
    }

    @Test
    public void testSampleFillValueNotKeyed() throws Exception {
        assertQuery("sum\tk\n" +
                        "11.427984775756228\t1970-01-03T00:00:00.000000Z\n" +
                        "20.56\t1970-01-03T00:30:00.000000Z\n" +
                        "42.17768841969397\t1970-01-03T01:00:00.000000Z\n" +
                        "20.56\t1970-01-03T01:30:00.000000Z\n" +
                        "23.90529010846525\t1970-01-03T02:00:00.000000Z\n" +
                        "20.56\t1970-01-03T02:30:00.000000Z\n" +
                        "70.94360487171201\t1970-01-03T03:00:00.000000Z\n" +
                        "20.56\t1970-01-03T03:30:00.000000Z\n" +
                        "87.99634725391621\t1970-01-03T04:00:00.000000Z\n" +
                        "20.56\t1970-01-03T04:30:00.000000Z\n" +
                        "32.881769076795045\t1970-01-03T05:00:00.000000Z\n" +
                        "20.56\t1970-01-03T05:30:00.000000Z\n" +
                        "97.71103146051203\t1970-01-03T06:00:00.000000Z\n" +
                        "20.56\t1970-01-03T06:30:00.000000Z\n" +
                        "81.46807944500559\t1970-01-03T07:00:00.000000Z\n" +
                        "20.56\t1970-01-03T07:30:00.000000Z\n" +
                        "57.93466326862211\t1970-01-03T08:00:00.000000Z\n" +
                        "20.56\t1970-01-03T08:30:00.000000Z\n" +
                        "12.026122412833129\t1970-01-03T09:00:00.000000Z\n",
                "select sum(a), k from x sample by 30m fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k) partition by NONE",
                "k",
                false);
    }

    @Test
    public void testSampleFillValueNotKeyedEmpty() throws Exception {
        assertQuery("sum\tk\n",
                "select sum(a), k from x sample by 30m fill(20.56)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(0)" +
                        ") timestamp(k) partition by NONE",
                "k",
                "insert into x select * from " +
                        "(" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(172800000000, 3600000000) k" +
                        " from" +
                        " long_sequence(10)" +
                        ") timestamp(k)",
                "sum\tk\n" +
                        "0.35983672154330515\t1970-01-03T00:00:00.000000Z\n" +
                        "20.56\t1970-01-03T00:30:00.000000Z\n" +
                        "76.75673070796104\t1970-01-03T01:00:00.000000Z\n" +
                        "20.56\t1970-01-03T01:30:00.000000Z\n" +
                        "62.173267078530984\t1970-01-03T02:00:00.000000Z\n" +
                        "20.56\t1970-01-03T02:30:00.000000Z\n" +
                        "63.81607531178513\t1970-01-03T03:00:00.000000Z\n" +
                        "20.56\t1970-01-03T03:30:00.000000Z\n" +
                        "57.93466326862211\t1970-01-03T04:00:00.000000Z\n" +
                        "20.56\t1970-01-03T04:30:00.000000Z\n" +
                        "12.026122412833129\t1970-01-03T05:00:00.000000Z\n" +
                        "20.56\t1970-01-03T05:30:00.000000Z\n" +
                        "48.820511018586934\t1970-01-03T06:00:00.000000Z\n" +
                        "20.56\t1970-01-03T06:30:00.000000Z\n" +
                        "26.922103479744898\t1970-01-03T07:00:00.000000Z\n" +
                        "20.56\t1970-01-03T07:30:00.000000Z\n" +
                        "52.98405941762054\t1970-01-03T08:00:00.000000Z\n" +
                        "20.56\t1970-01-03T08:30:00.000000Z\n" +
                        "84.45258177211063\t1970-01-03T09:00:00.000000Z\n",
                false);
    }

    private void assertSampleByIndexQuery(String expected, String query, String insert) throws Exception {
        assertSampleByIndexQuery(expected, query, insert, false);
    }

    private void assertSampleByIndexQuery(String expected, String query, String insert, boolean expectSize) throws Exception {
        String forceNoIndexQuery = query.replace("in ('b')", "in ('b', 'none')")
                .replace("in ('a')", "in ('a', 'none')");
        assertQuery(expected,
                forceNoIndexQuery,
                insert,
                "k",
                false,
                false,
                expectSize);

        assertQuery(expected,
                query,
                null,
                "k",
                false,
                false,
                expectSize);
    }

    private void assertWithSymbolColumnTop(String expected, String query) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("alter table xx drop column s", sqlExecutionContext);
            compiler.compile("alter table xx add s SYMBOL INDEX", sqlExecutionContext);
        });

        String forceNoIndexQuery = query.replace("and s = null", " ");

        assertQuery(expected,
                forceNoIndexQuery,
                null,
                "k",
                false);

        assertQuery(expected,
                query,
                null,
                "k",
                false);
    }
}
