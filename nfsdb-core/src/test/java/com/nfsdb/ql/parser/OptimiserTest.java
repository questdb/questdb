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
    public void testIntComparison() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").index().valueCountHint(128).
                        $double("x").
                        $double("y").
                        $int("i1").
                        $int("i2").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 128);

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putSym(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putInt(3, rnd.nextInt() & 63);
            ew.putInt(4, rnd.nextInt() & 63);
            ew.putDate(5, t += 10);
            ew.append();
        }
        w.commit();

        final String expected1 = "XSPEPTTKIBWFCKD\t0.000001672067\t-513.550415039063\t23\t14\n" +
                "XSPEPTTKIBWFCKD\t0.001000571705\t-118.208679199219\t34\t32\n" +
                "XSPEPTTKIBWFCKD\t0.255589209497\t-1024.000000000000\t52\t36\n" +
                "XSPEPTTKIBWFCKD\t0.000105848711\t-960.000000000000\t63\t29\n" +
                "XSPEPTTKIBWFCKD\t0.045759759843\t0.000000560194\t47\t33\n" +
                "XSPEPTTKIBWFCKD\t290.742401123047\t0.000001862155\t2\t1\n" +
                "XSPEPTTKIBWFCKD\t0.000186813500\t-827.140625000000\t36\t11\n" +
                "XSPEPTTKIBWFCKD\t294.856933593750\t-539.875854492188\t55\t42\n" +
                "XSPEPTTKIBWFCKD\t422.000000000000\t0.000001386965\t58\t54\n";

        assertThat(expected1, "select id,x,y,i1,i2 from tab where i1 >= i2 and x>=y  and x>=i1 and id = 'XSPEPTTKIBWFCKD'");

        final String expected2 = "XSPEPTTKIBWFCKD\t0.007556580706\t-444.759765625000\t14\t20\n" +
                "XSPEPTTKIBWFCKD\t0.002191273379\t-587.421875000000\t1\t33\n" +
                "XSPEPTTKIBWFCKD\t0.000000050401\t-873.569183349609\t34\t41\n" +
                "XSPEPTTKIBWFCKD\t0.000000002468\t-1009.435546875000\t7\t35\n" +
                "XSPEPTTKIBWFCKD\t102.474868774414\t-704.000000000000\t61\t63\n" +
                "XSPEPTTKIBWFCKD\t0.000166006441\t-400.250000000000\t23\t49\n" +
                "XSPEPTTKIBWFCKD\t256.000000000000\t-648.000000000000\t12\t46\n" +
                "XSPEPTTKIBWFCKD\t0.000000883287\t-844.890625000000\t4\t24\n";

        assertThat(expected2, "select id,x,y,i1,i2 from tab where i1 < i2 and x>=y  and y<i1 and id = 'XSPEPTTKIBWFCKD'");
    }

    @Test
    public void testIntMultiplication() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $int("x").
                        $int("y").
                        $str("z").
                        $ts()

        );

        Rnd rnd = new Rnd();

        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 1000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putInt(0, rnd.nextInt() & 255);
            ew.putInt(1, rnd.nextInt() & 255);
            ew.putStr(2, rnd.nextString(4));
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected1 = "29512\t-129\t119\t248\tCBJF\t2015-03-12T00:00:01.370Z\n" +
                "30906\t49\t202\t153\tCJZJ\t2015-03-12T00:00:07.470Z\n" +
                "2508\t113\t132\t19\tCGCJ\t2015-03-12T00:00:09.070Z\n";

        assertThat(expected1, "select x * y, x - y, x, y, z, timestamp from tab where z ~ '^C.*J+'");
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
            Assert.assertTrue(e.getMessage().contains("symbol or string column"));
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
    public void testLatestByStr() throws Exception {

        createIndexedTab();

        final String expected = "BVUDTGKDFEPWZYM\t0.000039040189\t-1024.000000000000\t2015-03-12T00:01:04.890Z\n" +
                "COPMLLOUWWZXQEL\t0.000000431389\t0.046957752667\t2015-03-12T00:01:10.010Z\n";

        assertThat(expected, "select id, x, y, timestamp from tab latest by id where id in ('COPMLLOUWWZXQEL', 'BVUDTGKDFEPWZYM')");
    }

    @Test
    public void testLatestByStrFilterOnSym() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(16).
                        $sym("sym").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 1024);

        ObjHashSet<String> syms = new ObjHashSet<>();
        for (int i = 0; i < 64; i++) {
            syms.add(rnd.nextString(10));
        }

        int mask = 1023;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(2, rnd.nextDouble());
            ew.putDouble(3, rnd.nextDouble());
            ew.putDate(4, t += 10);
            ew.putSym(1, syms.get(rnd.nextInt() & 63));
            ew.append();
        }
        w.commit();

        final String expected = "TEHIOFKMQPUNEUD\tMRFPKLNWQL\t0.020352731459\t0.165701057762\t2015-03-12T00:01:22.640Z\n";
        assertThat(expected, "select id, sym, x, y, timestamp from tab latest by id where id = 'TEHIOFKMQPUNEUD' and sym in ('MRFPKLNWQL')");
        assertThat(expected, "select id, sym, x, y, timestamp from tab latest by id where id = 'TEHIOFKMQPUNEUD' and sym = ('MRFPKLNWQL')");
        assertThat(expected, "select id, sym, x, y, timestamp from tab latest by id where id = 'TEHIOFKMQPUNEUD' and 'MRFPKLNWQL' = sym");
    }

    @Test
    public void testLatestByStrIrrelevantFilter() throws Exception {
        createIndexedTab();
        try {
            compile("select id, x, y, timestamp from tab latest by id where x > y");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(46, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("column expected"));
        }
    }

    @Test
    public void testLatestByStrNoFilter() throws Exception {
        createIndexedTab();
        try {
            compile("select id, x, y, timestamp from tab latest by id");
            Assert.fail("Exception expected");
        } catch (ParserException e) {
            Assert.assertEquals(46, e.getPosition());
            Assert.assertTrue(e.getMessage().contains("column expected"));
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
    public void testMultipleStrIdSearch() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(32).
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 1024);

        int mask = 1023;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");


        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "UHUTMTRRNGCIPFZ\t0.000006506322\t-261.000000000000\t2015-03-12T00:00:00.220Z\n" +
                "FZICFOQEVPXJYQR\t0.000000166602\t367.625000000000\t2015-03-12T00:00:00.260Z\n" +
                "FZICFOQEVPXJYQR\t57.308933258057\t28.255742073059\t2015-03-12T00:00:09.750Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000005319798\t-727.000000000000\t2015-03-12T00:00:10.060Z\n" +
                "FZICFOQEVPXJYQR\t-432.500000000000\t0.013725134078\t2015-03-12T00:00:13.470Z\n" +
                "FZICFOQEVPXJYQR\t-247.761962890625\t768.000000000000\t2015-03-12T00:00:15.170Z\n" +
                "UHUTMTRRNGCIPFZ\t438.929687500000\t0.000031495110\t2015-03-12T00:00:18.300Z\n" +
                "FZICFOQEVPXJYQR\t264.789741516113\t0.033011944033\t2015-03-12T00:00:19.630Z\n" +
                "FZICFOQEVPXJYQR\t6.671853065491\t1.936547994614\t2015-03-12T00:00:20.620Z\n" +
                "UHUTMTRRNGCIPFZ\t864.000000000000\t-1024.000000000000\t2015-03-12T00:00:25.970Z\n" +
                "UHUTMTRRNGCIPFZ\t0.002082723950\t0.000000001586\t2015-03-12T00:00:26.760Z\n" +
                "UHUTMTRRNGCIPFZ\t-976.561523437500\t0.446909941733\t2015-03-12T00:00:29.530Z\n" +
                "UHUTMTRRNGCIPFZ\t0.001273257891\t1.239676237106\t2015-03-12T00:00:31.270Z\n" +
                "UHUTMTRRNGCIPFZ\t-287.234375000000\t236.000000000000\t2015-03-12T00:00:33.720Z\n" +
                "FZICFOQEVPXJYQR\t1.589631736279\t128.217994689941\t2015-03-12T00:00:34.580Z\n" +
                "UHUTMTRRNGCIPFZ\t32.605212211609\t0.000000182797\t2015-03-12T00:00:35.120Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000029479873\t11.629675865173\t2015-03-12T00:00:35.710Z\n" +
                "UHUTMTRRNGCIPFZ\t269.668342590332\t0.000553555525\t2015-03-12T00:00:35.990Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000461809614\t64.250000000000\t2015-03-12T00:00:37.140Z\n" +
                "FZICFOQEVPXJYQR\t-572.296875000000\t0.000020149632\t2015-03-12T00:00:37.190Z\n" +
                "UHUTMTRRNGCIPFZ\t512.000000000000\t49.569551467896\t2015-03-12T00:00:40.250Z\n" +
                "FZICFOQEVPXJYQR\t0.000005206652\t0.272554814816\t2015-03-12T00:00:49.770Z\n" +
                "FZICFOQEVPXJYQR\t0.001125814480\t0.105613868684\t2015-03-12T00:01:06.100Z\n" +
                "UHUTMTRRNGCIPFZ\t704.000000000000\t44.546960830688\t2015-03-12T00:01:06.420Z\n" +
                "UHUTMTRRNGCIPFZ\t258.500000000000\t0.263136833906\t2015-03-12T00:01:07.450Z\n" +
                "FZICFOQEVPXJYQR\t192.000000000000\t-380.804687500000\t2015-03-12T00:01:08.610Z\n" +
                "FZICFOQEVPXJYQR\t56.567952156067\t0.086345635355\t2015-03-12T00:01:13.980Z\n" +
                "UHUTMTRRNGCIPFZ\t0.000097790253\t0.000000006182\t2015-03-12T00:01:17.060Z\n" +
                "FZICFOQEVPXJYQR\t128.000000000000\t469.091918945313\t2015-03-12T00:01:19.730Z\n" +
                "FZICFOQEVPXJYQR\t-592.000000000000\t0.000000797945\t2015-03-12T00:01:20.410Z\n" +
                "FZICFOQEVPXJYQR\t519.500000000000\t0.049629654735\t2015-03-12T00:01:22.360Z\n" +
                "FZICFOQEVPXJYQR\t24.736416816711\t92.901168823242\t2015-03-12T00:01:22.830Z\n" +
                "FZICFOQEVPXJYQR\t336.000000000000\t0.000000089523\t2015-03-12T00:01:26.920Z\n" +
                "FZICFOQEVPXJYQR\t0.044912695885\t64.000000000000\t2015-03-12T00:01:37.820Z\n";

        assertThat(expected, "select id, x, y, timestamp from tab where id in ('FZICFOQEVPXJYQR', 'UHUTMTRRNGCIPFZ')");
    }

    @Test
    public void testScaledDoubleComparison() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").index().valueCountHint(128).
                        $double("x").
                        $double("y").
                        $int("i1").
                        $int("i2").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 128);

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putSym(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putInt(3, rnd.nextInt() & 63);
            ew.putInt(4, rnd.nextInt() & 63);
            ew.putDate(5, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "YYVSYYEQBORDTQH\t0.000000012344\t0.000000017585\n" +
                "OXPKRGIIHYHBOQM\t0.000000042571\t0.000000046094\n" +
                "FUUTOMFUIOXLQLU\t0.000000009395\t0.000000017129\n" +
                "SCJOUOUIGENFELW\t0.000000000000\t0.000000003106\n" +
                "KIWIHBROKZKUTIQ\t0.000000001343\t0.000000006899\n" +
                "KBBQFNPOYNNCTFS\t0.000000001643\t0.000000010727\n" +
                "CIWXCYXGDHUWEPV\t0.000000000000\t0.000000007289\n" +
                "KFIJZZYNPPBXBHV\t0.000000007000\t0.000000009292\n" +
                "HBXOWVYUVVRDPCH\t0.000000000000\t0.000000005734\n" +
                "XWCKYLSUWDSWUGS\t0.000000010381\t0.000000020362\n" +
                "KJSMSSUQSRLTKVV\t0.000000003479\t0.000000006929\n" +
                "CNGZTOYTOXRSFPV\t0.000000000000\t0.000000000000\n" +
                "NMUREIJUHCLQCMZ\t0.000000001398\t0.000000004552\n" +
                "KJSMSSUQSRLTKVV\t0.000000004057\t0.000000009300\n" +
                "YSSMPGLUOHNZHZS\t0.000000003461\t0.000000005232\n" +
                "TRDLVSYLMSRHGKR\t0.000000001688\t0.000000005004\n" +
                "EOCVFFKMEKPFOYM\t0.000000002913\t0.000000000653\n" +
                "JUEBWVLOMPBETTT\t0.000000000000\t0.000000001650\n" +
                "VQEBNDCQCEHNOMV\t0.000000017353\t0.000000020155\n" +
                "JUEBWVLOMPBETTT\t0.000000120419\t0.000000111959\n" +
                "EIWFOQKYHQQUWQO\t0.000000080895\t0.000000081903\n" +
                "EVMLKCJBEVLUHLI\t0.000000005365\t0.000000003773\n" +
                "NZVDJIGSYLXGYTE\t0.000000022596\t0.000000017758\n" +
                "EOCVFFKMEKPFOYM\t0.000000011711\t0.000000006505\n" +
                "STYSWHLSWPFHXDB\t512.000000000000\t512.000000000000\n" +
                "IWEODDBHEVGXYHJ\t0.000000000773\t0.000000009342\n" +
                "KIWIHBROKZKUTIQ\t128.000000000000\t128.000000000000\n" +
                "VQEBNDCQCEHNOMV\t0.000000003251\t0.000000000000\n" +
                "BSQCNSFFLTRYZUZ\t-1024.000000000000\t-1024.000000000000\n" +
                "OPJEUKWMDNZZBBU\t-1024.000000000000\t-1024.000000000000\n" +
                "DOTSEDYYCTGQOLY\t0.000000004748\t0.000000004680\n" +
                "CMONRCXNUZFNWHF\t0.000000000000\t0.000000003728\n" +
                "HYBTVZNCLNXFSUW\t-1024.000000000000\t-1024.000000000000\n" +
                "EGMITINLKFNUHNR\t0.000000017782\t0.000000023362\n" +
                "UXBWYWRLHUHJECI\t0.000000009297\t0.000000009220\n" +
                "HBXOWVYUVVRDPCH\t-512.000000000000\t-512.000000000000\n";

        assertThat(expected, "select id, x, y from tab where eq(x, y, 0.00000001)");
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
        int n = 4 * 1024;
        ObjHashSet<String> names = getNames(rnd, n);

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
    public void testSearchByStringIdInUnindexed2() throws Exception {

        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        int n = 4 * 1024;
        ObjHashSet<String> names = getNames(rnd, n);

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

        final String expected = "JKEQQKQWPJVCFKV\t-141.875000000000\t2.248494863510\n" +
                "JKEQQKQWPJVCFKV\t-311.641113281250\t-398.023437500000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000841\t0.000000048359\n" +
                "JKEQQKQWPJVCFKV\t4.028919816017\t576.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.057562204078\t0.052935207263\n" +
                "JKEQQKQWPJVCFKV\t0.000003027280\t43.346537590027\n" +
                "JKEQQKQWPJVCFKV\t266.000000000000\t0.000033699243\n" +
                "JKEQQKQWPJVCFKV\t0.007589547429\t0.016206960194\n" +
                "JKEQQKQWPJVCFKV\t-256.000000000000\t213.664222717285\n" +
                "JKEQQKQWPJVCFKV\t22.610988616943\t0.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000030440875\t0.000000002590\n" +
                "JKEQQKQWPJVCFKV\t0.000001019223\t0.000861373846\n" +
                "JKEQQKQWPJVCFKV\t384.625000000000\t-762.664184570313\n" +
                "JKEQQKQWPJVCFKV\t0.772825062275\t701.435363769531\n" +
                "JKEQQKQWPJVCFKV\t191.932769775391\t0.000013081920\n" +
                "JKEQQKQWPJVCFKV\t416.812500000000\t0.000000003177\n" +
                "JKEQQKQWPJVCFKV\t0.000003838093\t810.968750000000\n" +
                "JKEQQKQWPJVCFKV\t0.041989765130\t728.000000000000\n" +
                "JKEQQKQWPJVCFKV\t0.000000000000\t-89.843750000000\n";

        assertThat(expected, "select id, x,y from tab where id in ('JKEQQKQWPJVCFKV')");
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
        ObjHashSet<String> names = getNames(rnd, 1024);

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
    public void testSearchIndexedStrNull() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < 128; i++) {
            names.add(rnd.nextString(15));
        }

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            if ((rnd.nextPositiveInt() % 10) == 0) {
                ew.putNull(0);
            } else {
                ew.putStr(0, names.get(rnd.nextInt() & mask));
            }
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        assertNullSearch();
    }

    @Test
    public void testSearchIndexedSymNull() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").index().valueCountHint(128).
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 128);

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            if ((rnd.nextPositiveInt() % 10) == 0) {
                ew.putNull(0);
            } else {
                ew.putSym(0, names.get(rnd.nextInt() & mask));
            }
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        assertNullSearch();
    }

    @Test
    public void testSearchUnindexedStrNull() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < 128; i++) {
            names.add(rnd.nextString(15));
        }

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            if ((rnd.nextPositiveInt() % 10) == 0) {
                ew.putNull(0);
            } else {
                ew.putStr(0, names.get(rnd.nextInt() & mask));
            }
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        assertNullSearch();
    }

    @Test
    public void testSearchUnindexedSymNull() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 128);

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            if ((rnd.nextPositiveInt() % 10) == 0) {
                ew.putNull(0);
            } else {
                ew.putSym(0, names.get(rnd.nextInt() & mask));
            }
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        assertNullSearch();
    }

    @Test
    public void testStrConcat() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("x").
                        $str("y").
                        $int("z").
                        $ts()

        );

        Rnd rnd = new Rnd();

        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 1000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, rnd.nextString(4));
            ew.putStr(1, rnd.nextString(2));
            ew.putInt(2, rnd.nextInt());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "CJOU-OU\n" +
                "CQJS-HG\n" +
                "CEJF-PB\n" +
                "CSJK-PY\n" +
                "CJHU-JY\n";

        assertThat(expected, "select x + '-' + y from tab where x ~ '^C.*J+'");
    }

    @Test
    public void testStrRegex() throws Exception {
        createTab();
        final String expecte = "KEQMMKDFIPNZVZR\t0.000000001530\t2015-03-12T00:00:03.470Z\n" +
                "KEQMMKDFIPNZVZR\t-832.000000000000\t2015-03-12T00:00:04.650Z\n" +
                "KEQMMKDFIPNZVZR\t446.187500000000\t2015-03-12T00:00:05.460Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000005636\t2015-03-12T00:00:24.210Z\n" +
                "KEQMMKDFIPNZVZR\t0.129930097610\t2015-03-12T00:00:52.220Z\n" +
                "KEQMMKDFIPNZVZR\t-677.094238281250\t2015-03-12T00:00:54.020Z\n" +
                "KEQMMKDFIPNZVZR\t-167.187500000000\t2015-03-12T00:00:58.090Z\n" +
                "KEQMMKDFIPNZVZR\t-512.000000000000\t2015-03-12T00:01:11.790Z\n" +
                "KEQMMKDFIPNZVZR\t0.055018983781\t2015-03-12T00:01:19.340Z\n" +
                "KEQMMKDFIPNZVZR\t-862.500000000000\t2015-03-12T00:01:24.430Z\n" +
                "KEQMMKDFIPNZVZR\t883.730468750000\t2015-03-12T00:01:28.870Z\n" +
                "KEQMMKDFIPNZVZR\t193.875000000000\t2015-03-12T00:01:39.320Z\n" +
                "KEQMMKDFIPNZVZR\t-608.000000000000\t2015-03-12T00:01:42.440Z\n" +
                "KEQMMKDFIPNZVZR\t-193.003417968750\t2015-03-12T00:01:47.820Z\n" +
                "KEQMMKDFIPNZVZR\t0.000002046971\t2015-03-12T00:01:55.420Z\n" +
                "KEQMMKDFIPNZVZR\t0.037930097431\t2015-03-12T00:01:55.790Z\n" +
                "KEQMMKDFIPNZVZR\t0.160599559546\t2015-03-12T00:02:08.830Z\n" +
                "KEQMMKDFIPNZVZR\t91.000000000000\t2015-03-12T00:02:19.120Z\n" +
                "KEQMMKDFIPNZVZR\t-1000.000000000000\t2015-03-12T00:02:22.680Z\n" +
                "KEQMMKDFIPNZVZR\t0.000015199104\t2015-03-12T00:02:23.520Z\n" +
                "KEQMMKDFIPNZVZR\t-480.000000000000\t2015-03-12T00:02:29.060Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000224731\t2015-03-12T00:02:31.260Z\n" +
                "KEQMMKDFIPNZVZR\t0.043443457223\t2015-03-12T00:02:40.400Z\n" +
                "KEQMMKDFIPNZVZR\t-554.125000000000\t2015-03-12T00:02:45.230Z\n" +
                "KEQMMKDFIPNZVZR\t0.418899938464\t2015-03-12T00:02:52.550Z\n" +
                "KEQMMKDFIPNZVZR\t0.000002813213\t2015-03-12T00:03:34.680Z\n" +
                "KEQMMKDFIPNZVZR\t-750.970703125000\t2015-03-12T00:03:43.830Z\n" +
                "KEQMMKDFIPNZVZR\t202.477161407471\t2015-03-12T00:03:59.950Z\n" +
                "KEQMMKDFIPNZVZR\t0.000296119222\t2015-03-12T00:04:06.200Z\n" +
                "KEQMMKDFIPNZVZR\t-1001.109375000000\t2015-03-12T00:04:12.750Z\n" +
                "KEQMMKDFIPNZVZR\t-350.539062500000\t2015-03-12T00:04:17.920Z\n" +
                "KEQMMKDFIPNZVZR\t0.270242959261\t2015-03-12T00:04:30.780Z\n" +
                "KEQMMKDFIPNZVZR\t640.000000000000\t2015-03-12T00:04:36.000Z\n" +
                "KEQMMKDFIPNZVZR\t242.000000000000\t2015-03-12T00:04:37.360Z\n" +
                "KEQMMKDFIPNZVZR\t354.109191894531\t2015-03-12T00:04:43.560Z\n" +
                "KEQMMKDFIPNZVZR\t608.000000000000\t2015-03-12T00:05:03.070Z\n" +
                "KEQMMKDFIPNZVZR\t-209.281250000000\t2015-03-12T00:05:18.460Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000009506\t2015-03-12T00:05:39.720Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000018168\t2015-03-12T00:05:40.690Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000002177\t2015-03-12T00:05:41.820Z\n" +
                "KEQMMKDFIPNZVZR\t0.000375485601\t2015-03-12T00:05:49.480Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000039768\t2015-03-12T00:05:55.150Z\n" +
                "KEQMMKDFIPNZVZR\t0.269348338246\t2015-03-12T00:06:09.200Z\n" +
                "KEQMMKDFIPNZVZR\t-671.500000000000\t2015-03-12T00:06:26.130Z\n" +
                "KEQMMKDFIPNZVZR\t0.001258826785\t2015-03-12T00:06:28.910Z\n" +
                "KEQMMKDFIPNZVZR\t0.978091150522\t2015-03-12T00:06:33.330Z\n" +
                "KEQMMKDFIPNZVZR\t44.780708312988\t2015-03-12T00:06:43.700Z\n" +
                "KEQMMKDFIPNZVZR\t-767.601562500000\t2015-03-12T00:06:44.600Z\n" +
                "KEQMMKDFIPNZVZR\t-890.500000000000\t2015-03-12T00:06:59.620Z\n" +
                "KEQMMKDFIPNZVZR\t0.000173775785\t2015-03-12T00:07:01.460Z\n" +
                "KEQMMKDFIPNZVZR\t0.000192599509\t2015-03-12T00:07:04.160Z\n" +
                "KEQMMKDFIPNZVZR\t18.733582496643\t2015-03-12T00:07:23.720Z\n" +
                "KEQMMKDFIPNZVZR\t31.429724693298\t2015-03-12T00:07:38.140Z\n" +
                "KEQMMKDFIPNZVZR\t0.000390803711\t2015-03-12T00:07:39.260Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000044603\t2015-03-12T00:07:49.970Z\n" +
                "KEQMMKDFIPNZVZR\t-881.380859375000\t2015-03-12T00:07:55.910Z\n" +
                "KEQMMKDFIPNZVZR\t-128.000000000000\t2015-03-12T00:08:00.360Z\n" +
                "KEQMMKDFIPNZVZR\t891.539062500000\t2015-03-12T00:08:14.330Z\n" +
                "KEQMMKDFIPNZVZR\t508.000000000000\t2015-03-12T00:08:21.190Z\n" +
                "KEQMMKDFIPNZVZR\t0.002558049746\t2015-03-12T00:08:31.860Z\n" +
                "KEQMMKDFIPNZVZR\t-736.000000000000\t2015-03-12T00:08:57.430Z\n" +
                "KEQMMKDFIPNZVZR\t-968.859375000000\t2015-03-12T00:09:27.030Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000008169\t2015-03-12T00:09:27.200Z\n" +
                "KEQMMKDFIPNZVZR\t399.000000000000\t2015-03-12T00:09:45.580Z\n" +
                "KEQMMKDFIPNZVZR\t0.000239236899\t2015-03-12T00:09:53.250Z\n" +
                "KEQMMKDFIPNZVZR\t-104.871093750000\t2015-03-12T00:10:01.070Z\n" +
                "KEQMMKDFIPNZVZR\t15.412450790405\t2015-03-12T00:10:04.140Z\n" +
                "KEQMMKDFIPNZVZR\t0.185059137642\t2015-03-12T00:10:15.850Z\n" +
                "KEQMMKDFIPNZVZR\t5.659068346024\t2015-03-12T00:10:26.050Z\n" +
                "KEQMMKDFIPNZVZR\t3.807189881802\t2015-03-12T00:10:59.590Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000677441\t2015-03-12T00:11:01.530Z\n" +
                "KEQMMKDFIPNZVZR\t61.405685424805\t2015-03-12T00:11:03.130Z\n" +
                "KEQMMKDFIPNZVZR\t-1024.000000000000\t2015-03-12T00:11:18.220Z\n" +
                "KEQMMKDFIPNZVZR\t641.500000000000\t2015-03-12T00:11:25.040Z\n" +
                "KEQMMKDFIPNZVZR\t0.000001824081\t2015-03-12T00:11:34.740Z\n" +
                "KEQMMKDFIPNZVZR\t0.000056925237\t2015-03-12T00:11:39.260Z\n" +
                "KEQMMKDFIPNZVZR\t128.000000000000\t2015-03-12T00:11:40.030Z\n" +
                "KEQMMKDFIPNZVZR\t0.000168198319\t2015-03-12T00:11:42.890Z\n" +
                "KEQMMKDFIPNZVZR\t0.000002674703\t2015-03-12T00:11:54.620Z\n" +
                "KEQMMKDFIPNZVZR\t0.001482222520\t2015-03-12T00:12:40.320Z\n" +
                "KEQMMKDFIPNZVZR\t56.829874038696\t2015-03-12T00:12:42.760Z\n" +
                "KEQMMKDFIPNZVZR\t41.603179931641\t2015-03-12T00:13:16.840Z\n" +
                "KEQMMKDFIPNZVZR\t164.312500000000\t2015-03-12T00:13:35.470Z\n" +
                "KEQMMKDFIPNZVZR\t-457.061523437500\t2015-03-12T00:13:45.640Z\n" +
                "KEQMMKDFIPNZVZR\t-512.000000000000\t2015-03-12T00:13:46.040Z\n" +
                "KEQMMKDFIPNZVZR\t0.000027407084\t2015-03-12T00:13:51.600Z\n" +
                "KEQMMKDFIPNZVZR\t-473.760742187500\t2015-03-12T00:13:57.560Z\n" +
                "KEQMMKDFIPNZVZR\t-512.000000000000\t2015-03-12T00:13:58.830Z\n" +
                "KEQMMKDFIPNZVZR\t74.750000000000\t2015-03-12T00:14:32.610Z\n" +
                "KEQMMKDFIPNZVZR\t982.715270996094\t2015-03-12T00:14:33.480Z\n" +
                "KEQMMKDFIPNZVZR\t0.235923126340\t2015-03-12T00:14:36.540Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000003422\t2015-03-12T00:14:48.360Z\n" +
                "KEQMMKDFIPNZVZR\t0.000304762289\t2015-03-12T00:15:01.280Z\n" +
                "KEQMMKDFIPNZVZR\t0.000188905338\t2015-03-12T00:15:08.640Z\n" +
                "KEQMMKDFIPNZVZR\t256.000000000000\t2015-03-12T00:15:09.740Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000017417\t2015-03-12T00:15:17.910Z\n" +
                "KEQMMKDFIPNZVZR\t75.859375000000\t2015-03-12T00:15:32.280Z\n" +
                "KEQMMKDFIPNZVZR\t0.091820014641\t2015-03-12T00:15:34.560Z\n" +
                "KEQMMKDFIPNZVZR\t0.000044015695\t2015-03-12T00:15:45.650Z\n" +
                "KEQMMKDFIPNZVZR\t0.000000003026\t2015-03-12T00:15:48.030Z\n" +
                "KEQMMKDFIPNZVZR\t-963.317260742188\t2015-03-12T00:15:49.270Z\n" +
                "KEQMMKDFIPNZVZR\t0.001303359750\t2015-03-12T00:16:08.870Z\n" +
                "KEQMMKDFIPNZVZR\t0.005202150205\t2015-03-12T00:16:14.750Z\n";

        assertThat(expecte, "select id, x, timestamp from tab where id ~ '^KE.*'");
    }

    @Test
    public void testSymRegex() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").index().valueCountHint(128).
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 128);

        int mask = 127;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putSym(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "EENNEBQQEMXDKXE\t0.005532926181\t2015-03-12T00:01:38.290Z\n" +
                "EDNKRCGKSQDCMUM\t201.500000000000\t2015-03-12T00:01:38.780Z\n" +
                "EVMLKCJBEVLUHLI\t-224.000000000000\t2015-03-12T00:01:39.040Z\n" +
                "EEHRUGPBMBTKVSB\t640.000000000000\t2015-03-12T00:01:39.140Z\n" +
                "EOCVFFKMEKPFOYM\t0.001286557002\t2015-03-12T00:01:39.260Z\n" +
                "ETJRSZSRYRFBVTM\t0.146399393678\t2015-03-12T00:01:39.460Z\n" +
                "ELLKKHTWNWIFFLR\t236.634628295898\t2015-03-12T00:01:39.600Z\n" +
                "EGMITINLKFNUHNR\t53.349147796631\t2015-03-12T00:01:39.850Z\n" +
                "EIWFOQKYHQQUWQO\t-617.734375000000\t2015-03-12T00:01:40.000Z\n";

        assertThat(expected, "select id, y, timestamp from tab latest by id where id ~ '^E.*'");
    }

    @Test
    public void testUnindexedIntNaN() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $int("id").
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");
        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            if (rnd.nextPositiveInt() % 10 == 0) {
                ew.putNull(0);
            } else {
                ew.putInt(0, rnd.nextInt());
            }
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();

        final String expected = "NaN\t768.000000000000\t-408.000000000000\n" +
                "NaN\t256.000000000000\t-455.750000000000\n" +
                "NaN\t525.287368774414\t-470.171875000000\n" +
                "NaN\t512.000000000000\t-425.962463378906\n" +
                "NaN\t553.796875000000\t-620.062500000000\n" +
                "NaN\t512.000000000000\t-958.144531250000\n" +
                "NaN\t304.062500000000\t-1024.000000000000\n" +
                "NaN\t600.000000000000\t-934.000000000000\n" +
                "NaN\t475.619140625000\t-431.078125000000\n" +
                "NaN\t864.000000000000\t-480.723571777344\n" +
                "NaN\t512.000000000000\t-585.500000000000\n" +
                "NaN\t183.876594543457\t-512.000000000000\n" +
                "NaN\t204.021038055420\t-453.903320312500\n" +
                "NaN\t272.363739013672\t-1024.000000000000\n" +
                "NaN\t973.989135742188\t-444.000000000000\n" +
                "NaN\t768.000000000000\t-1024.000000000000\n" +
                "NaN\t290.253906250000\t-960.000000000000\n" +
                "NaN\t263.580390930176\t-960.000000000000\n" +
                "NaN\t756.500000000000\t-1024.000000000000\n" +
                "NaN\t461.884765625000\t-921.996948242188\n" +
                "NaN\t512.000000000000\t-536.000000000000\n" +
                "NaN\t213.152450561523\t-811.783691406250\n" +
                "NaN\t121.591918945313\t-874.921630859375\n" +
                "NaN\t920.625000000000\t-512.000000000000\n";

        assertThat(expected, "select id, x, y from tab where id = NaN and x > 120 and y < -400");
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

    private void assertNullSearch() throws JournalException, ParserException {
        final String expected = "null\t256.000000000000\t-455.750000000000\n" +
                "null\t525.287368774414\t-470.171875000000\n" +
                "null\t512.000000000000\t-425.962463378906\n" +
                "null\t553.796875000000\t-620.062500000000\n" +
                "null\t512.000000000000\t-958.144531250000\n" +
                "null\t304.062500000000\t-1024.000000000000\n" +
                "null\t600.000000000000\t-934.000000000000\n" +
                "null\t475.619140625000\t-431.078125000000\n" +
                "null\t864.000000000000\t-480.723571777344\n" +
                "null\t512.000000000000\t-585.500000000000\n" +
                "null\t183.876594543457\t-512.000000000000\n" +
                "null\t204.021038055420\t-453.903320312500\n" +
                "null\t272.363739013672\t-1024.000000000000\n" +
                "null\t973.989135742188\t-444.000000000000\n" +
                "null\t768.000000000000\t-1024.000000000000\n" +
                "null\t290.253906250000\t-960.000000000000\n" +
                "null\t263.580390930176\t-960.000000000000\n" +
                "null\t756.500000000000\t-1024.000000000000\n" +
                "null\t461.884765625000\t-921.996948242188\n" +
                "null\t512.000000000000\t-536.000000000000\n" +
                "null\t213.152450561523\t-811.783691406250\n" +
                "null\t121.591918945313\t-874.921630859375\n" +
                "null\t920.625000000000\t-512.000000000000\n" +
                "null\t256.000000000000\t-488.625000000000\n" +
                "null\t361.391540527344\t-1024.000000000000\n";

        assertThat(expected, "select id, x, y from tab where id = null and x > 120 and y < -400");
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

    private void createIndexedTab() throws JournalException {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $str("id").index().buckets(16).
                        $double("x").
                        $double("y").
                        $ts()

        );

        Rnd rnd = new Rnd();
        ObjHashSet<String> names = getNames(rnd, 1024);

        int mask = 1023;
        long t = Dates.parseDateTime("2015-03-12T00:00:00.000Z");

        for (int i = 0; i < 10000; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putStr(0, names.get(rnd.nextInt() & mask));
            ew.putDouble(1, rnd.nextDouble());
            ew.putDouble(2, rnd.nextDouble());
            ew.putDate(3, t += 10);
            ew.append();
        }
        w.commit();
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
        ObjHashSet<String> names = getNames(rnd, 1024);

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

    private ObjHashSet<String> getNames(Rnd r, int n) {
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < n; i++) {
            names.add(r.nextString(15));
        }
        return names;
    }
}
