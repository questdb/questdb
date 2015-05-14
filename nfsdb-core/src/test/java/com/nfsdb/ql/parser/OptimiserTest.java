/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalCachingFactory;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Rnd;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OptimiserTest extends AbstractTest {
    private final QueryParser parser = new QueryParser();
    private final Optimiser optimiser;
    private final StringSink sink = new StringSink();
    private final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
    private JournalCachingFactory f;

    public OptimiserTest() {
        this.optimiser = new Optimiser(factory);
    }

    @Before
    public void setUp() {
        f = new JournalCachingFactory(factory.getConfiguration());
    }

    @After
    public void tearDown() {
        f.close();
    }

    @Test
    public void testConstantCondition1() throws Exception {
        createTab();
        String plan = compile("select id, x, y from tab where x > 0 and 1 > 1").toString();
        Assert.assertTrue(plan.contains("NoOpJournalPartitionSource"));

    }

    @Test
    public void testConstantCondition2() throws Exception {
        createTab();
        String plan = compile("select id, x, y from tab where x > 0 or 1 = 1").toString();
        Assert.assertTrue(plan.contains("AllRowSource"));
        Assert.assertFalse(plan.contains("NoOpJournalPartitionSource"));
    }

    @Test
    public void testConstantCondition3() throws Exception {
        createTab();
        String plan = compile("select id, x, y from tab where 1 > 1 or 2 > 2").toString();
        Assert.assertTrue(plan.contains("NoOpJournalPartitionSource"));
    }

    @Test
    public void testIntervalAndIndexSearch() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 3600 * 24 * 10, Dates.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
        w.commit();

        final String expected = "ADM.L\t837.343750000000\t0.061431560665\t2015-02-12T10:00:04.000Z\n" +
                "BP.L\t564.425537109375\t0.000000003711\t2015-02-12T10:00:40.000Z\n" +
                "BP.L\t768.000000000000\t0.000000011709\t2015-02-12T10:01:18.000Z\n" +
                "BP.L\t512.000000000000\t74.948242187500\t2015-02-12T10:01:31.000Z\n" +
                "BP.L\t980.000000000000\t133.570312500000\t2015-02-12T10:02:14.000Z\n" +
                "ADM.L\t768.000000000000\t296.109375000000\t2015-02-12T10:02:35.000Z\n" +
                "BP.L\t807.750000000000\t705.548904418945\t2015-02-12T10:02:49.000Z\n" +
                "BP.L\t949.156250000000\t63.068359375000\t2015-02-12T10:02:53.000Z\n" +
                "ADM.L\t768.000000000000\t0.000047940810\t2015-02-12T10:03:19.000Z\n" +
                "BP.L\t968.953491210938\t0.029868379235\t2015-02-12T10:03:21.000Z\n" +
                "ADM.L\t512.000000000000\t0.000000000000\t2015-02-12T10:03:31.000Z\n" +
                "BP.L\t512.000000000000\t0.000000318310\t2015-02-12T10:03:56.000Z\n" +
                "BP.L\t788.000000000000\t55.569427490234\t2015-02-12T10:04:02.000Z\n" +
                "BP.L\t768.000000000000\t924.000000000000\t2015-02-12T10:04:04.000Z\n" +
                "ADM.L\t718.848632812500\t907.609375000000\t2015-02-12T10:04:13.000Z\n" +
                "ADM.L\t965.062500000000\t0.000000591804\t2015-02-12T10:04:22.000Z\n" +
                "ADM.L\t696.000000000000\t9.672361135483\t2015-02-12T10:04:25.000Z\n" +
                "BP.L\t992.000000000000\t0.750000000000\t2015-02-12T10:04:27.000Z\n" +
                "BP.L\t518.117187500000\t765.889160156250\t2015-02-12T10:04:33.000Z\n";
        assertThat(expected, "select sym, bid, ask, timestamp from q where timestamp = '2015-02-12T10:00:00;5m' and sym in ('BP.L','ADM.L') and bid > 500");
    }

    @Test
    public void testInvalidLatestByColumn1() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, bid, ask, timestamp from q latest by symx where sym in ('GKN.L') and ask > 100");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(49, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("nvalid column"));
        }
    }

    @Test
    public void testInvalidLatestByColumn2() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, bid, ask, timestamp from q latest by ask where sym in ('GKN.L') and ask > 100");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(49, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("symbol column"));
        }
    }

    @Test
    public void testInvalidLatestByColumn3() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, bid, ask, timestamp from q latest by mode where sym in ('GKN.L') and ask > 100");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(49, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("not indexed"));
        }
    }

    @Test
    public void testInvalidLiteralColumn() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, bid, ask, timestamp1 from q latest by sym where sym in ('GKN.L') and ask > 100");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(22, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("nvalid column"));
        }
    }

    @Test
    public void testInvalidVirtualColumn() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, (bid+ask2)/2, timestamp from q latest by sym where sym in ('GKN.L') and ask > 100");
            Assert.fail("Exception expected");
        } catch (InvalidColumnException e) {
            Assert.assertEquals(17, e.getPosition());
        }
    }

    @Test
    public void testInvalidWhereColumn1() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, bid, ask, timestamp from q where sym2 in ('GKN.L') and ask > 100");
            Assert.fail("Exception expected");
        } catch (InvalidColumnException e) {
            Assert.assertEquals(45, e.getPosition());
        }
    }

    @Test
    public void testInvalidWhereColumn2() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select sym, bid, ask, timestamp from q where sym in ('GKN.L') and ask2 > 100");
            Assert.fail("Exception expected");
        } catch (InvalidColumnException e) {
            Assert.assertEquals(66, e.getPosition());
        }
    }

    @Test
    public void testJournalDoesNotExist() throws Exception {
        try {
            compile("select id, x, y, timestamp from q where id = ");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(32, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testLatestBySym() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 3600 * 24, Dates.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
        w.commit();

        final String expected = "TLW.L\t0.000000000000\t0.000000048727\t2015-02-13T02:58:41.000Z\n" +
                "ADM.L\t0.000000106175\t0.102090202272\t2015-02-13T02:58:59.000Z\n" +
                "ABF.L\t0.000701488039\t382.432617187500\t2015-02-13T02:59:25.000Z\n" +
                "RRS.L\t161.155059814453\t809.607971191406\t2015-02-13T02:59:26.000Z\n" +
                "BP.L\t0.000003149229\t0.000005004517\t2015-02-13T02:59:40.000Z\n" +
                "GKN.L\t0.101824980229\t1024.000000000000\t2015-02-13T02:59:48.000Z\n" +
                "AGK.L\t0.905496925116\t72.000000000000\t2015-02-13T02:59:53.000Z\n" +
                "WTB.L\t0.006673692260\t348.000000000000\t2015-02-13T02:59:57.000Z\n" +
                "BT-A.L\t0.000000500809\t0.000879329862\t2015-02-13T02:59:58.000Z\n" +
                "LLOY.L\t0.000000328173\t288.000000000000\t2015-02-13T02:59:59.000Z\n";

        assertThat(expected, "select sym, bid, ask, timestamp from q latest by sym where bid < ask");
    }

    @Test
    public void testLatestBySymList() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 3600 * 24, Dates.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
        w.commit();

        final String expected = "BP.L\t0.000000253226\t1022.955993652344\t2015-02-13T02:59:34.000Z\n" +
                "GKN.L\t688.000000000000\t256.000000000000\t2015-02-13T02:59:50.000Z\n";
        assertThat(expected, "select sym, bid, ask, timestamp from q latest by sym where sym in ('GKN.L', 'BP.L') and ask > 100");
    }

    @Test
    public void testMissingEqualsArgument() throws Exception {
        factory.writer(Quote.class, "q");
        try {
            compile("select id, x, y, timestamp from q where id = ");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(43, e.getPosition());
        }
    }

    @Test
    public void testSearchByIntIdUnindexed() throws Exception {

        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $int("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        int[] ids = new int[4096];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = rnd.nextPositiveInt();
        }

        int mask = ids.length - 1;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");


        for (int i = 0; i < 100000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putInt(0, ids[rnd.nextInt() & mask]);
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "1148688404\t19.165769577026\t0.002435457427\t2015-03-12T00:00:10.150Z\n" +
                "1148688404\t0.000000444469\t2.694594800472\t2015-03-12T00:00:28.400Z\n" +
                "1148688404\t-640.000000000000\t-1024.000000000000\t2015-03-12T00:01:06.260Z\n" +
                "1148688404\t0.000000009509\t320.000000000000\t2015-03-12T00:01:26.010Z\n" +
                "1148688404\t0.000107977266\t362.467056274414\t2015-03-12T00:01:57.750Z\n" +
                "1148688404\t0.000000022366\t-451.815429687500\t2015-03-12T00:02:32.560Z\n" +
                "1148688404\t0.000003631694\t152.575393676758\t2015-03-12T00:02:35.210Z\n" +
                "1148688404\t-458.523437500000\t0.003299130592\t2015-03-12T00:03:09.590Z\n" +
                "1148688404\t-576.000000000000\t0.000000005352\t2015-03-12T00:03:15.080Z\n" +
                "1148688404\t0.000000010009\t0.000000336123\t2015-03-12T00:03:33.830Z\n" +
                "1148688404\t0.012756480370\t0.191264979541\t2015-03-12T00:03:56.200Z\n" +
                "1148688404\t-793.000000000000\t6.048339843750\t2015-03-12T00:04:01.350Z\n" +
                "1148688404\t0.000144788552\t10.723476886749\t2015-03-12T00:04:48.380Z\n" +
                "1148688404\t-467.990722656250\t5.262818336487\t2015-03-12T00:04:52.710Z\n" +
                "1148688404\t0.031378546730\t149.346038818359\t2015-03-12T00:05:01.020Z\n" +
                "1148688404\t0.000741893891\t-27.789062500000\t2015-03-12T00:05:19.110Z\n" +
                "1148688404\t0.000000032685\t0.000000002490\t2015-03-12T00:05:26.610Z\n" +
                "1148688404\t0.652305364609\t0.000000029041\t2015-03-12T00:06:56.860Z\n" +
                "1148688404\t-894.000000000000\t51.695074081421\t2015-03-12T00:08:46.620Z\n" +
                "1148688404\t695.000000000000\t0.145211979747\t2015-03-12T00:09:22.390Z\n" +
                "1148688404\t-334.488891601563\t0.000000393977\t2015-03-12T00:09:29.860Z\n" +
                "1148688404\t7.933303117752\t0.000516850792\t2015-03-12T00:10:13.730Z\n" +
                "1148688404\t435.498107910156\t1.287820875645\t2015-03-12T00:10:30.240Z\n" +
                "1148688404\t961.880340576172\t0.000168625862\t2015-03-12T00:10:41.190Z\n" +
                "1148688404\t-84.978515625000\t0.051617769524\t2015-03-12T00:10:46.200Z\n" +
                "1148688404\t0.000000544715\t0.000328194423\t2015-03-12T00:10:52.510Z\n" +
                "1148688404\t512.000000000000\t875.250000000000\t2015-03-12T00:11:46.390Z\n" +
                "1148688404\t0.000000010856\t0.028837248683\t2015-03-12T00:12:15.140Z\n" +
                "1148688404\t0.000027862162\t-896.000000000000\t2015-03-12T00:12:28.700Z\n" +
                "1148688404\t0.000000003071\t0.000025084717\t2015-03-12T00:12:36.370Z\n" +
                "1148688404\t0.000040687404\t0.007985642878\t2015-03-12T00:12:42.940Z\n" +
                "1148688404\t-961.937500000000\t-849.000000000000\t2015-03-12T00:12:49.940Z\n" +
                "1148688404\t0.000384466533\t87.682281494141\t2015-03-12T00:14:11.980Z\n" +
                "1148688404\t0.000000309420\t448.000000000000\t2015-03-12T00:15:38.730Z\n" +
                "1148688404\t29.022820472717\t-123.758422851563\t2015-03-12T00:16:30.770Z\n";
        assertThat(expected, "select id, x, y, timestamp from tab where id = 1148688404");
    }

    @Test
    public void testSearchByStringIdInUnindexed() throws Exception {

        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = new ObjHashSet<>();
        int n = 4 * 1024;
        for (int i = 0; i < n; i++) {
            names.add(rnd.nextString(15));
        }

        int mask = n - 1;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 100000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "VEGPIGSVMYWRTXV\t0.000015968965\t675.997558593750\n" +
                "VEGPIGSVMYWRTXV\t86.369699478149\t0.000017492367\n" +
                "JKEQQKQWPJVCFKV\t-141.875000000000\t2.248494863510\n" +
                "JKEQQKQWPJVCFKV\t-311.641113281250\t-398.023437500000\n" +
                "VEGPIGSVMYWRTXV\t3.659749031067\t0.000001526956\n" +
                "VEGPIGSVMYWRTXV\t-87.500000000000\t-778.964843750000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000841\t0.000000048359\n" +
                "JKEQQKQWPJVCFKV\t4.028919816017\t576.000000000000\n" +
                "VEGPIGSVMYWRTXV\t896.000000000000\t0.000000293617\n" +
                "VEGPIGSVMYWRTXV\t-1024.000000000000\t0.000001939648\n" +
                "VEGPIGSVMYWRTXV\t0.000019246366\t-1024.000000000000\n" +
                "VEGPIGSVMYWRTXV\t410.933593750000\t0.000000039558\n" +
                "JKEQQKQWPJVCFKV\t0.057562204078\t0.052935207263\n" +
                "VEGPIGSVMYWRTXV\t0.000000001681\t0.000000007821\n" +
                "VEGPIGSVMYWRTXV\t-1024.000000000000\t-921.363525390625\n" +
                "JKEQQKQWPJVCFKV\t0.000003027280\t43.346537590027\n" +
                "VEGPIGSVMYWRTXV\t0.000000009230\t99.335662841797\n" +
                "JKEQQKQWPJVCFKV\t266.000000000000\t0.000033699243\n" +
                "VEGPIGSVMYWRTXV\t5.966133117676\t0.000019340443\n" +
                "VEGPIGSVMYWRTXV\t0.000001273319\t0.000020025251\n" +
                "JKEQQKQWPJVCFKV\t0.007589547429\t0.016206960194\n" +
                "JKEQQKQWPJVCFKV\t-256.000000000000\t213.664222717285\n" +
                "VEGPIGSVMYWRTXV\t5.901823043823\t0.226934209466\n" +
                "VEGPIGSVMYWRTXV\t0.000033694661\t0.036246776581\n" +
                "JKEQQKQWPJVCFKV\t22.610988616943\t0.000000000000\n" +
                "VEGPIGSVMYWRTXV\t0.000000600285\t896.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000030440875\t0.000000002590\n" +
                "VEGPIGSVMYWRTXV\t-612.819580078125\t-768.000000000000\n" +
                "VEGPIGSVMYWRTXV\t652.960937500000\t-163.895019531250\n" +
                "JKEQQKQWPJVCFKV\t0.000001019223\t0.000861373846\n" +
                "VEGPIGSVMYWRTXV\t0.000000237054\t855.149673461914\n" +
                "JKEQQKQWPJVCFKV\t384.625000000000\t-762.664184570313\n" +
                "VEGPIGSVMYWRTXV\t0.000000003865\t269.064453125000\n" +
                "VEGPIGSVMYWRTXV\t1.651362478733\t640.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.772825062275\t701.435363769531\n" +
                "JKEQQKQWPJVCFKV\t191.932769775391\t0.000013081920\n" +
                "JKEQQKQWPJVCFKV\t416.812500000000\t0.000000003177\n" +
                "JKEQQKQWPJVCFKV\t0.000003838093\t810.968750000000\n" +
                "VEGPIGSVMYWRTXV\t0.042331939563\t368.000000000000\n" +
                "VEGPIGSVMYWRTXV\t0.038675817661\t-69.960937500000\n" +
                "VEGPIGSVMYWRTXV\t0.154417395592\t0.000000005908\n" +
                "JKEQQKQWPJVCFKV\t0.041989765130\t728.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000000\t-89.843750000000\n" +
                "VEGPIGSVMYWRTXV\t-224.000000000000\t247.625000000000\n";

        assertThat(expected, "select id, x,y from tab where id in ('JKEQQKQWPJVCFKV', 'VEGPIGSVMYWRTXV')");
/*

        RecordSource<? extends Record> rs = compile("select id, x,y from tab where id in ('JKEQQKQWPJVCFKV', 'VEGPIGSVMYWRTXV')");

        RecordSourcePrinter p = new RecordSourcePrinter(new StdoutSink());
        p.print(rs.prepareCursor(f), rs.getMetadata());
*/
    }

    @Test
    public void testSearchByStringIdIndexed() throws Exception {

        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(32).
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < 1024; i++) {
            names.add(rnd.nextString(15));
        }

        int mask = 1023;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");


        for (int i = 0; i < 100000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "XTPNHTDCEBYWXBB\t-292.000000000000\t0.000000006354\t2015-03-12T00:00:02.290Z\n" +
                "XTPNHTDCEBYWXBB\t7.197236061096\t2.818476676941\t2015-03-12T00:00:27.340Z\n" +
                "XTPNHTDCEBYWXBB\t0.000005481412\t1.312383592129\t2015-03-12T00:00:29.610Z\n" +
                "XTPNHTDCEBYWXBB\t446.081878662109\t0.000000051478\t2015-03-12T00:00:31.780Z\n" +
                "XTPNHTDCEBYWXBB\t-809.625000000000\t0.000000104467\t2015-03-12T00:00:33.860Z\n" +
                "XTPNHTDCEBYWXBB\t560.000000000000\t0.526266053319\t2015-03-12T00:00:37.440Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000004619\t0.000028377840\t2015-03-12T00:00:46.630Z\n" +
                "XTPNHTDCEBYWXBB\t-510.983673095703\t-512.000000000000\t2015-03-12T00:00:55.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000527720\t760.000000000000\t2015-03-12T00:01:03.780Z\n" +
                "XTPNHTDCEBYWXBB\t0.012854952831\t-292.297058105469\t2015-03-12T00:01:09.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.082818411291\t2.386151432991\t2015-03-12T00:01:10.310Z\n" +
                "XTPNHTDCEBYWXBB\t330.293212890625\t-268.000000000000\t2015-03-12T00:01:12.810Z\n" +
                "XTPNHTDCEBYWXBB\t278.125000000000\t0.077817678452\t2015-03-12T00:01:16.670Z\n" +
                "XTPNHTDCEBYWXBB\t-448.000000000000\t0.000001829988\t2015-03-12T00:02:02.260Z\n" +
                "XTPNHTDCEBYWXBB\t0.238381892443\t-935.843750000000\t2015-03-12T00:02:33.540Z\n" +
                "XTPNHTDCEBYWXBB\t0.097852131352\t-120.312500000000\t2015-03-12T00:02:41.600Z\n" +
                "XTPNHTDCEBYWXBB\t0.034327778034\t0.000000076055\t2015-03-12T00:02:41.860Z\n" +
                "XTPNHTDCEBYWXBB\t0.016777765006\t1.525665938854\t2015-03-12T00:02:47.630Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000245470\t0.000000012355\t2015-03-12T00:03:21.880Z\n" +
                "XTPNHTDCEBYWXBB\t38.482372283936\t156.359012603760\t2015-03-12T00:03:25.470Z\n" +
                "XTPNHTDCEBYWXBB\t960.000000000000\t-561.500000000000\t2015-03-12T00:03:52.790Z\n" +
                "XTPNHTDCEBYWXBB\t0.000048914401\t0.000535350249\t2015-03-12T00:03:53.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.315786086023\t-544.000000000000\t2015-03-12T00:04:02.560Z\n" +
                "XTPNHTDCEBYWXBB\t-512.000000000000\t512.000000000000\t2015-03-12T00:04:09.410Z\n" +
                "XTPNHTDCEBYWXBB\t0.000469007500\t0.000000003315\t2015-03-12T00:04:29.330Z\n" +
                "XTPNHTDCEBYWXBB\t473.774108886719\t0.005739651737\t2015-03-12T00:04:49.240Z\n" +
                "XTPNHTDCEBYWXBB\t77.079637527466\t-68.750000000000\t2015-03-12T00:04:54.540Z\n" +
                "XTPNHTDCEBYWXBB\t1017.250000000000\t256.000000000000\t2015-03-12T00:04:59.980Z\n" +
                "XTPNHTDCEBYWXBB\t979.558593750000\t0.034476440400\t2015-03-12T00:05:00.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.080838434398\t0.000240437294\t2015-03-12T00:05:16.590Z\n" +
                "XTPNHTDCEBYWXBB\t837.343750000000\t0.000000003163\t2015-03-12T00:05:22.150Z\n" +
                "XTPNHTDCEBYWXBB\t-708.738037109375\t12.065711975098\t2015-03-12T00:05:23.960Z\n" +
                "XTPNHTDCEBYWXBB\t73.905494689941\t968.143554687500\t2015-03-12T00:05:30.160Z\n" +
                "XTPNHTDCEBYWXBB\t858.125000000000\t0.004347450798\t2015-03-12T00:06:06.300Z\n" +
                "XTPNHTDCEBYWXBB\t191.156250000000\t692.151489257813\t2015-03-12T00:06:07.380Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000350446\t0.001085809752\t2015-03-12T00:06:14.550Z\n" +
                "XTPNHTDCEBYWXBB\t877.107116699219\t0.073764367029\t2015-03-12T00:06:26.310Z\n" +
                "XTPNHTDCEBYWXBB\t4.980149984360\t0.000000005301\t2015-03-12T00:06:33.470Z\n" +
                "XTPNHTDCEBYWXBB\t0.000937165081\t-204.000000000000\t2015-03-12T00:06:54.810Z\n" +
                "XTPNHTDCEBYWXBB\t756.876586914063\t-572.703125000000\t2015-03-12T00:06:56.120Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000022885\t0.689865306020\t2015-03-12T00:06:57.920Z\n" +
                "XTPNHTDCEBYWXBB\t723.500000000000\t-592.817382812500\t2015-03-12T00:07:17.570Z\n" +
                "XTPNHTDCEBYWXBB\t-285.125000000000\t-448.250000000000\t2015-03-12T00:07:20.480Z\n" +
                "XTPNHTDCEBYWXBB\t4.877287983894\t-870.000000000000\t2015-03-12T00:07:36.830Z\n" +
                "XTPNHTDCEBYWXBB\t-638.750000000000\t-859.125000000000\t2015-03-12T00:07:38.910Z\n" +
                "XTPNHTDCEBYWXBB\t757.085937500000\t-128.000000000000\t2015-03-12T00:07:45.970Z\n" +
                "XTPNHTDCEBYWXBB\t0.000024196771\t44.254640579224\t2015-03-12T00:07:56.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000002050660\t113.433692932129\t2015-03-12T00:08:25.690Z\n" +
                "XTPNHTDCEBYWXBB\t0.001966100186\t401.331298828125\t2015-03-12T00:08:31.180Z\n" +
                "XTPNHTDCEBYWXBB\t134.605468750000\t0.000778750400\t2015-03-12T00:08:34.070Z\n" +
                "XTPNHTDCEBYWXBB\t304.000000000000\t170.421752929688\t2015-03-12T00:08:36.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000029559\t0.000033108370\t2015-03-12T00:08:42.110Z\n" +
                "XTPNHTDCEBYWXBB\t0.064763752744\t-384.000000000000\t2015-03-12T00:08:49.670Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.016534221359\t2015-03-12T00:09:01.010Z\n" +
                "XTPNHTDCEBYWXBB\t0.060663623735\t0.377497851849\t2015-03-12T00:09:03.830Z\n" +
                "XTPNHTDCEBYWXBB\t0.000001439460\t0.000000291427\t2015-03-12T00:09:05.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000660118021\t0.000000001520\t2015-03-12T00:09:14.030Z\n" +
                "XTPNHTDCEBYWXBB\t394.622238159180\t0.245789200068\t2015-03-12T00:09:35.320Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t0.002625804045\t2015-03-12T00:10:04.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.021761201322\t-805.171875000000\t2015-03-12T00:10:10.920Z\n" +
                "XTPNHTDCEBYWXBB\t18.621844291687\t0.003388853336\t2015-03-12T00:10:24.380Z\n" +
                "XTPNHTDCEBYWXBB\t-514.108642578125\t66.830410003662\t2015-03-12T00:10:30.510Z\n" +
                "XTPNHTDCEBYWXBB\t1.720549345016\t0.000006926386\t2015-03-12T00:10:37.250Z\n" +
                "XTPNHTDCEBYWXBB\t-715.183959960938\t22.427126884460\t2015-03-12T00:10:39.680Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.629212051630\t2015-03-12T00:10:44.310Z\n" +
                "XTPNHTDCEBYWXBB\t257.433593750000\t0.000087903414\t2015-03-12T00:11:03.210Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000070390\t-270.520019531250\t2015-03-12T00:11:18.280Z\n" +
                "XTPNHTDCEBYWXBB\t-439.250000000000\t0.000000093325\t2015-03-12T00:11:25.080Z\n" +
                "XTPNHTDCEBYWXBB\t256.000000000000\t760.565032958984\t2015-03-12T00:11:35.220Z\n" +
                "XTPNHTDCEBYWXBB\t634.375000000000\t0.000000033359\t2015-03-12T00:11:55.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.000031026852\t0.000000000000\t2015-03-12T00:11:58.680Z\n" +
                "XTPNHTDCEBYWXBB\t90.977296829224\t0.000000124888\t2015-03-12T00:12:06.190Z\n" +
                "XTPNHTDCEBYWXBB\t0.845079660416\t0.000001311144\t2015-03-12T00:12:12.980Z\n" +
                "XTPNHTDCEBYWXBB\t-0.500000000000\t216.805793762207\t2015-03-12T00:12:28.700Z\n" +
                "XTPNHTDCEBYWXBB\t0.021825334989\t0.000000003128\t2015-03-12T00:12:29.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.307688817382\t516.472656250000\t2015-03-12T00:12:36.300Z\n" +
                "XTPNHTDCEBYWXBB\t43.792731285095\t0.000372541021\t2015-03-12T00:12:42.040Z\n" +
                "XTPNHTDCEBYWXBB\t-782.687500000000\t252.748397827148\t2015-03-12T00:12:48.780Z\n" +
                "XTPNHTDCEBYWXBB\t137.645996093750\t808.000000000000\t2015-03-12T00:13:09.280Z\n" +
                "XTPNHTDCEBYWXBB\t0.002546578180\t17.097163200378\t2015-03-12T00:13:27.120Z\n" +
                "XTPNHTDCEBYWXBB\t-264.875000000000\t-419.750000000000\t2015-03-12T00:13:40.020Z\n" +
                "XTPNHTDCEBYWXBB\t0.000221305789\t53.479209899902\t2015-03-12T00:13:40.660Z\n" +
                "XTPNHTDCEBYWXBB\t0.030516586266\t-612.226562500000\t2015-03-12T00:13:50.440Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t17.896668434143\t2015-03-12T00:13:53.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000091829\t0.000000000000\t2015-03-12T00:14:06.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000164877347\t0.000000009079\t2015-03-12T00:14:15.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000276606\t512.000000000000\t2015-03-12T00:14:31.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000034906\t-1024.000000000000\t2015-03-12T00:15:19.540Z\n" +
                "XTPNHTDCEBYWXBB\t478.680068969727\t0.000058549787\t2015-03-12T00:15:19.790Z\n" +
                "XTPNHTDCEBYWXBB\t430.000000000000\t639.000000000000\t2015-03-12T00:15:33.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000236331\t-960.000000000000\t2015-03-12T00:15:38.790Z\n" +
                "XTPNHTDCEBYWXBB\t81.210937500000\t0.000056687957\t2015-03-12T00:15:43.330Z\n" +
                "XTPNHTDCEBYWXBB\t648.112548828125\t0.000010239995\t2015-03-12T00:16:30.740Z\n";
        assertThat(expected, "select id, x, y, timestamp from tab where id in ('XTPNHTDCEBYWXBB')");
    }

    @Test
    public void testSearchByStringIdUnindexed() throws Exception {

        createTab();

        final String expected = "XTPNHTDCEBYWXBB\t-292.000000000000\t0.000000006354\t2015-03-12T00:00:02.290Z\n" +
                "XTPNHTDCEBYWXBB\t7.197236061096\t2.818476676941\t2015-03-12T00:00:27.340Z\n" +
                "XTPNHTDCEBYWXBB\t0.000005481412\t1.312383592129\t2015-03-12T00:00:29.610Z\n" +
                "XTPNHTDCEBYWXBB\t446.081878662109\t0.000000051478\t2015-03-12T00:00:31.780Z\n" +
                "XTPNHTDCEBYWXBB\t-809.625000000000\t0.000000104467\t2015-03-12T00:00:33.860Z\n" +
                "XTPNHTDCEBYWXBB\t560.000000000000\t0.526266053319\t2015-03-12T00:00:37.440Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000004619\t0.000028377840\t2015-03-12T00:00:46.630Z\n" +
                "XTPNHTDCEBYWXBB\t-510.983673095703\t-512.000000000000\t2015-03-12T00:00:55.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000527720\t760.000000000000\t2015-03-12T00:01:03.780Z\n" +
                "XTPNHTDCEBYWXBB\t0.012854952831\t-292.297058105469\t2015-03-12T00:01:09.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.082818411291\t2.386151432991\t2015-03-12T00:01:10.310Z\n" +
                "XTPNHTDCEBYWXBB\t330.293212890625\t-268.000000000000\t2015-03-12T00:01:12.810Z\n" +
                "XTPNHTDCEBYWXBB\t278.125000000000\t0.077817678452\t2015-03-12T00:01:16.670Z\n" +
                "XTPNHTDCEBYWXBB\t-448.000000000000\t0.000001829988\t2015-03-12T00:02:02.260Z\n" +
                "XTPNHTDCEBYWXBB\t0.238381892443\t-935.843750000000\t2015-03-12T00:02:33.540Z\n" +
                "XTPNHTDCEBYWXBB\t0.097852131352\t-120.312500000000\t2015-03-12T00:02:41.600Z\n" +
                "XTPNHTDCEBYWXBB\t0.034327778034\t0.000000076055\t2015-03-12T00:02:41.860Z\n" +
                "XTPNHTDCEBYWXBB\t0.016777765006\t1.525665938854\t2015-03-12T00:02:47.630Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000245470\t0.000000012355\t2015-03-12T00:03:21.880Z\n" +
                "XTPNHTDCEBYWXBB\t38.482372283936\t156.359012603760\t2015-03-12T00:03:25.470Z\n" +
                "XTPNHTDCEBYWXBB\t960.000000000000\t-561.500000000000\t2015-03-12T00:03:52.790Z\n" +
                "XTPNHTDCEBYWXBB\t0.000048914401\t0.000535350249\t2015-03-12T00:03:53.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.315786086023\t-544.000000000000\t2015-03-12T00:04:02.560Z\n" +
                "XTPNHTDCEBYWXBB\t-512.000000000000\t512.000000000000\t2015-03-12T00:04:09.410Z\n" +
                "XTPNHTDCEBYWXBB\t0.000469007500\t0.000000003315\t2015-03-12T00:04:29.330Z\n" +
                "XTPNHTDCEBYWXBB\t473.774108886719\t0.005739651737\t2015-03-12T00:04:49.240Z\n" +
                "XTPNHTDCEBYWXBB\t77.079637527466\t-68.750000000000\t2015-03-12T00:04:54.540Z\n" +
                "XTPNHTDCEBYWXBB\t1017.250000000000\t256.000000000000\t2015-03-12T00:04:59.980Z\n" +
                "XTPNHTDCEBYWXBB\t979.558593750000\t0.034476440400\t2015-03-12T00:05:00.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.080838434398\t0.000240437294\t2015-03-12T00:05:16.590Z\n" +
                "XTPNHTDCEBYWXBB\t837.343750000000\t0.000000003163\t2015-03-12T00:05:22.150Z\n" +
                "XTPNHTDCEBYWXBB\t-708.738037109375\t12.065711975098\t2015-03-12T00:05:23.960Z\n" +
                "XTPNHTDCEBYWXBB\t73.905494689941\t968.143554687500\t2015-03-12T00:05:30.160Z\n" +
                "XTPNHTDCEBYWXBB\t858.125000000000\t0.004347450798\t2015-03-12T00:06:06.300Z\n" +
                "XTPNHTDCEBYWXBB\t191.156250000000\t692.151489257813\t2015-03-12T00:06:07.380Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000350446\t0.001085809752\t2015-03-12T00:06:14.550Z\n" +
                "XTPNHTDCEBYWXBB\t877.107116699219\t0.073764367029\t2015-03-12T00:06:26.310Z\n" +
                "XTPNHTDCEBYWXBB\t4.980149984360\t0.000000005301\t2015-03-12T00:06:33.470Z\n" +
                "XTPNHTDCEBYWXBB\t0.000937165081\t-204.000000000000\t2015-03-12T00:06:54.810Z\n" +
                "XTPNHTDCEBYWXBB\t756.876586914063\t-572.703125000000\t2015-03-12T00:06:56.120Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000022885\t0.689865306020\t2015-03-12T00:06:57.920Z\n" +
                "XTPNHTDCEBYWXBB\t723.500000000000\t-592.817382812500\t2015-03-12T00:07:17.570Z\n" +
                "XTPNHTDCEBYWXBB\t-285.125000000000\t-448.250000000000\t2015-03-12T00:07:20.480Z\n" +
                "XTPNHTDCEBYWXBB\t4.877287983894\t-870.000000000000\t2015-03-12T00:07:36.830Z\n" +
                "XTPNHTDCEBYWXBB\t-638.750000000000\t-859.125000000000\t2015-03-12T00:07:38.910Z\n" +
                "XTPNHTDCEBYWXBB\t757.085937500000\t-128.000000000000\t2015-03-12T00:07:45.970Z\n" +
                "XTPNHTDCEBYWXBB\t0.000024196771\t44.254640579224\t2015-03-12T00:07:56.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000002050660\t113.433692932129\t2015-03-12T00:08:25.690Z\n" +
                "XTPNHTDCEBYWXBB\t0.001966100186\t401.331298828125\t2015-03-12T00:08:31.180Z\n" +
                "XTPNHTDCEBYWXBB\t134.605468750000\t0.000778750400\t2015-03-12T00:08:34.070Z\n" +
                "XTPNHTDCEBYWXBB\t304.000000000000\t170.421752929688\t2015-03-12T00:08:36.400Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000029559\t0.000033108370\t2015-03-12T00:08:42.110Z\n" +
                "XTPNHTDCEBYWXBB\t0.064763752744\t-384.000000000000\t2015-03-12T00:08:49.670Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.016534221359\t2015-03-12T00:09:01.010Z\n" +
                "XTPNHTDCEBYWXBB\t0.060663623735\t0.377497851849\t2015-03-12T00:09:03.830Z\n" +
                "XTPNHTDCEBYWXBB\t0.000001439460\t0.000000291427\t2015-03-12T00:09:05.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000660118021\t0.000000001520\t2015-03-12T00:09:14.030Z\n" +
                "XTPNHTDCEBYWXBB\t394.622238159180\t0.245789200068\t2015-03-12T00:09:35.320Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t0.002625804045\t2015-03-12T00:10:04.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.021761201322\t-805.171875000000\t2015-03-12T00:10:10.920Z\n" +
                "XTPNHTDCEBYWXBB\t18.621844291687\t0.003388853336\t2015-03-12T00:10:24.380Z\n" +
                "XTPNHTDCEBYWXBB\t-514.108642578125\t66.830410003662\t2015-03-12T00:10:30.510Z\n" +
                "XTPNHTDCEBYWXBB\t1.720549345016\t0.000006926386\t2015-03-12T00:10:37.250Z\n" +
                "XTPNHTDCEBYWXBB\t-715.183959960938\t22.427126884460\t2015-03-12T00:10:39.680Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000000000\t0.629212051630\t2015-03-12T00:10:44.310Z\n" +
                "XTPNHTDCEBYWXBB\t257.433593750000\t0.000087903414\t2015-03-12T00:11:03.210Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000070390\t-270.520019531250\t2015-03-12T00:11:18.280Z\n" +
                "XTPNHTDCEBYWXBB\t-439.250000000000\t0.000000093325\t2015-03-12T00:11:25.080Z\n" +
                "XTPNHTDCEBYWXBB\t256.000000000000\t760.565032958984\t2015-03-12T00:11:35.220Z\n" +
                "XTPNHTDCEBYWXBB\t634.375000000000\t0.000000033359\t2015-03-12T00:11:55.300Z\n" +
                "XTPNHTDCEBYWXBB\t0.000031026852\t0.000000000000\t2015-03-12T00:11:58.680Z\n" +
                "XTPNHTDCEBYWXBB\t90.977296829224\t0.000000124888\t2015-03-12T00:12:06.190Z\n" +
                "XTPNHTDCEBYWXBB\t0.845079660416\t0.000001311144\t2015-03-12T00:12:12.980Z\n" +
                "XTPNHTDCEBYWXBB\t-0.500000000000\t216.805793762207\t2015-03-12T00:12:28.700Z\n" +
                "XTPNHTDCEBYWXBB\t0.021825334989\t0.000000003128\t2015-03-12T00:12:29.420Z\n" +
                "XTPNHTDCEBYWXBB\t0.307688817382\t516.472656250000\t2015-03-12T00:12:36.300Z\n" +
                "XTPNHTDCEBYWXBB\t43.792731285095\t0.000372541021\t2015-03-12T00:12:42.040Z\n" +
                "XTPNHTDCEBYWXBB\t-782.687500000000\t252.748397827148\t2015-03-12T00:12:48.780Z\n" +
                "XTPNHTDCEBYWXBB\t137.645996093750\t808.000000000000\t2015-03-12T00:13:09.280Z\n" +
                "XTPNHTDCEBYWXBB\t0.002546578180\t17.097163200378\t2015-03-12T00:13:27.120Z\n" +
                "XTPNHTDCEBYWXBB\t-264.875000000000\t-419.750000000000\t2015-03-12T00:13:40.020Z\n" +
                "XTPNHTDCEBYWXBB\t0.000221305789\t53.479209899902\t2015-03-12T00:13:40.660Z\n" +
                "XTPNHTDCEBYWXBB\t0.030516586266\t-612.226562500000\t2015-03-12T00:13:50.440Z\n" +
                "XTPNHTDCEBYWXBB\t-1024.000000000000\t17.896668434143\t2015-03-12T00:13:53.350Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000091829\t0.000000000000\t2015-03-12T00:14:06.090Z\n" +
                "XTPNHTDCEBYWXBB\t0.000164877347\t0.000000009079\t2015-03-12T00:14:15.960Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000276606\t512.000000000000\t2015-03-12T00:14:31.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000034906\t-1024.000000000000\t2015-03-12T00:15:19.540Z\n" +
                "XTPNHTDCEBYWXBB\t478.680068969727\t0.000058549787\t2015-03-12T00:15:19.790Z\n" +
                "XTPNHTDCEBYWXBB\t430.000000000000\t639.000000000000\t2015-03-12T00:15:33.890Z\n" +
                "XTPNHTDCEBYWXBB\t0.000000236331\t-960.000000000000\t2015-03-12T00:15:38.790Z\n" +
                "XTPNHTDCEBYWXBB\t81.210937500000\t0.000056687957\t2015-03-12T00:15:43.330Z\n" +
                "XTPNHTDCEBYWXBB\t648.112548828125\t0.000010239995\t2015-03-12T00:16:30.740Z\n";
        assertThat(expected, "select id, x, y, timestamp from tab where id = 'XTPNHTDCEBYWXBB'");
    }

    @Test
    public void testVirtualColumnQuery() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 100);

        final String expected = "BT-A.L\t0.474883438625\t0.000001189157\t1.050231933594\n" +
                "ADM.L\t-51.014269662148\t104.021850585938\t0.006688738358\n" +
                "AGK.L\t-686.961853027344\t879.117187500000\t496.806518554688\n" +
                "ABF.L\t-383.000010317080\t768.000000000000\t0.000020634160\n" +
                "ABF.L\t-127.000000017899\t256.000000000000\t0.000000035797\n" +
                "WTB.L\t-459.332875207067\t920.625000000000\t0.040750414133\n" +
                "AGK.L\t-703.000000000000\t512.000000000000\t896.000000000000\n" +
                "RRS.L\t-5.478123126552\t12.923866510391\t0.032379742712\n" +
                "BT-A.L\t0.996734812157\t0.006530375686\t0.000000000000\n" +
                "ABF.L\t-359.000000008662\t0.000000017324\t720.000000000000\n" +
                "AGK.L\t-191.000000009850\t384.000000000000\t0.000000019700\n" +
                "ABF.L\t0.999416386211\t0.001165474765\t0.000001752813\n" +
                "RRS.L\t-347.652348756790\t1.507822513580\t695.796875000000\n" +
                "ADM.L\t-86.378168493509\t172.796875000000\t1.959461987019\n" +
                "RRS.L\t-470.449707034291\t0.000000006081\t942.899414062500\n" +
                "BP.L\t-723.414062500000\t424.828125000000\t1024.000000000000\n" +
                "HSBA.L\t-75.736694544117\t153.473033905029\t0.000355183205\n" +
                "RRS.L\t-489.548828125000\t632.921875000000\t348.175781250000\n" +
                "BT-A.L\t-92.000010057318\t186.000000000000\t0.000020114637\n" +
                "RRS.L\t-334.728804341285\t0.015470010694\t671.442138671875\n" +
                "HSBA.L\t0.969581946437\t0.000000009901\t0.060836097226\n" +
                "GKN.L\t-134.846217133105\t0.003103211522\t271.689331054688\n" +
                "BP.L\t-384.179687507322\t770.359375000000\t0.000000014643\n" +
                "LLOY.L\t-2.041434317827\t1.229880273342\t4.852988362312\n" +
                "TLW.L\t-382.690430340427\t0.000001305853\t767.380859375000\n" +
                "HSBA.L\t0.999757577623\t0.000000776007\t0.000484068747\n" +
                "RRS.L\t-291.082599617541\t583.609375000000\t0.555824235082\n" +
                "BP.L\t-234.659652709961\t296.544433593750\t174.774871826172\n" +
                "WTB.L\t-470.000000000000\t842.000000000000\t100.000000000000\n" +
                "RRS.L\t-181.231244396825\t364.462486267090\t0.000002526560\n" +
                "GKN.L\t0.999684159173\t0.000603844470\t0.000027837185\n" +
                "TLW.L\t-175.000000130841\t0.000000261681\t352.000000000000\n" +
                "GKN.L\t0.999937448983\t0.000125102033\t0.000000000000\n" +
                "AGK.L\t0.999129234003\t0.000000194258\t0.001741337735\n" +
                "ADM.L\t-108.185731784076\t218.371459960938\t0.000003607215\n" +
                "LLOY.L\t-527.821648597717\t1024.000000000000\t33.643297195435\n" +
                "BP.L\t-127.000587929302\t256.000000000000\t0.001175858604\n" +
                "HSBA.L\t-71.210969042524\t144.421875000000\t0.000063085048\n" +
                "BP.L\t-127.000000016025\t256.000000000000\t0.000000032050\n" +
                "GKN.L\t-415.000040076207\t0.000080152415\t832.000000000000\n" +
                "AGK.L\t-289.957031250000\t512.000000000000\t69.914062500000\n" +
                "AGK.L\t-450.494251251221\t768.000000000000\t134.988502502441\n" +
                "LLOY.L\t-293.859375000936\t0.000000001871\t589.718750000000\n" +
                "GKN.L\t-367.000001976696\t736.000000000000\t0.000003953393\n" +
                "AGK.L\t0.999999992240\t0.000000001374\t0.000000014146\n" +
                "LLOY.L\t-1.005833093077\t0.115072973073\t3.896593213081\n" +
                "BT-A.L\t-192.421875002549\t386.843750000000\t0.000000005098\n" +
                "LLOY.L\t-5.999457120895\t5.590153217316\t8.408761024475\n" +
                "GKN.L\t-4.042319541496\t0.000000248992\t10.084638834000\n" +
                "HSBA.L\t-81.109376058324\t0.000002116648\t164.218750000000\n" +
                "WTB.L\t0.999989964510\t0.000005107453\t0.000014963527\n" +
                "BT-A.L\t-468.790763854981\t629.480468750000\t310.101058959961\n" +
                "TLW.L\t0.694377524342\t0.000000049302\t0.611244902015\n" +
                "AGK.L\t-338.896525263786\t672.000000000000\t7.793050527573\n" +
                "TLW.L\t0.260076059727\t0.018715771381\t1.461132109165\n" +
                "ADM.L\t-352.977539062500\t655.625000000000\t52.330078125000\n" +
                "BP.L\t-59.514666617196\t0.000036359392\t121.029296875000\n" +
                "LLOY.L\t-131.905826912553\t265.807006835938\t0.004646989168\n" +
                "GKN.L\t-48.381265968084\t0.971607863903\t97.790924072266\n" +
                "LLOY.L\t-175.841796875000\t0.000000000000\t353.683593750000\n" +
                "LLOY.L\t-7.008397817612\t8.039016723633\t7.977778911591\n" +
                "ABF.L\t-318.007048395928\t638.000000000000\t0.014096791856\n" +
                "HSBA.L\t-409.112306014912\t0.000002654824\t820.224609375000\n" +
                "HSBA.L\t-149.046875020149\t300.093750000000\t0.000000040298\n" +
                "HSBA.L\t0.997081416281\t0.005052038119\t0.000785129319\n" +
                "BT-A.L\t0.936320396314\t0.127358488739\t0.000000718634\n" +
                "ADM.L\t0.999999965448\t0.000000009919\t0.000000059185\n" +
                "GKN.L\t0.979669743518\t0.040659694001\t0.000000818963\n" +
                "TLW.L\t-1.819448314155\t0.000012560774\t5.638884067535\n" +
                "BP.L\t-499.354459762573\t873.000000000000\t127.708919525146\n" +
                "HSBA.L\t-724.575195312500\t939.150390625000\t512.000000000000\n" +
                "ABF.L\t-488.316503390990\t978.632812500000\t0.000194281980\n" +
                "AGK.L\t-444.362694263458\t844.000000000000\t46.725388526917\n" +
                "HSBA.L\t-228.500000000000\t31.000000000000\t428.000000000000\n" +
                "ADM.L\t-36.921404135436\t75.842805862427\t0.000002408446\n" +
                "GKN.L\t-580.579162597656\t283.158325195313\t880.000000000000\n" +
                "ABF.L\t-481.575685286397\t0.000003385293\t965.151367187500\n" +
                "TLW.L\t0.804228177760\t0.031326758675\t0.360216885805\n" +
                "GKN.L\t-637.187500000000\t508.375000000000\t768.000000000000\n" +
                "ADM.L\t-5.150909269229\t12.290055274963\t0.011763263494\n" +
                "GKN.L\t-1.684180170298\t4.111308574677\t1.257051765919\n" +
                "RRS.L\t-113.000794559603\t0.000002205143\t228.001586914063\n" +
                "LLOY.L\t0.994362744171\t0.000000129186\t0.011274382472\n" +
                "ADM.L\t-8.878542166360\t19.756743907928\t0.000340424791\n" +
                "GKN.L\t0.999909967674\t0.000180012023\t0.000000052629\n" +
                "BT-A.L\t-252.331054687500\t400.000000000000\t106.662109375000\n" +
                "RRS.L\t-223.239476203918\t68.043695449829\t380.435256958008\n" +
                "ADM.L\t0.997952489638\t0.004094106262\t0.000000914462\n" +
                "BP.L\t-253.937500000000\t64.000000000000\t445.875000000000\n" +
                "WTB.L\t-2.006443221466\t0.000000157150\t6.012886285782\n" +
                "HSBA.L\t-303.487510681152\t497.000000000000\t111.975021362305\n" +
                "HSBA.L\t-282.980148315430\t549.125503540039\t18.834793090820\n" +
                "TLW.L\t-205.000000075030\t0.000000150060\t412.000000000000\n" +
                "RRS.L\t-19.750000003584\t0.000000007168\t41.500000000000\n" +
                "GKN.L\t-446.143188476563\t354.286376953125\t540.000000000000\n" +
                "GKN.L\t-185.000005207851\t0.000010415702\t372.000000000000\n" +
                "ADM.L\t-370.770515203476\t728.300781250000\t15.240249156952\n" +
                "RRS.L\t-223.348431229591\t448.000000000000\t0.696862459183\n" +
                "AGK.L\t-511.009589326801\t0.019178653602\t1024.000000000000\n" +
                "BP.L\t-705.000000000000\t1021.000000000000\t391.000000000000\n";

        assertThat(expected, "select sym, 1-(bid+ask)/2 mid, bid, ask from q");
    }

    private void assertThat(String expected, String query) throws JournalException, ParserException {
        RecordSource<? extends Record> rs = compile(query);

        sink.clear();
        printer.print(rs.prepareCursor(f), rs.getMetadata());
        Assert.assertEquals(expected, sink.toString());

        rs.reset();
        sink.clear();
        printer.print(rs.prepareCursor(f), rs.getMetadata());
        Assert.assertEquals(expected, sink.toString());
    }

    private RecordSource<? extends Record> compile(CharSequence query) throws ParserException, JournalException {
        parser.setContent(query);
        return optimiser.compile(parser.parse().getQueryModel());
    }

    private void createTab() throws JournalException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < 1024; i++) {
            names.add(rnd.nextString(15));
        }

        int mask = 1023;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 100000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();
    }

}
