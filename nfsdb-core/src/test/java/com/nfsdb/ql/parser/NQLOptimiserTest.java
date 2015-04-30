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

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class NQLOptimiserTest extends AbstractTest {
    private final NQLParser parser = new NQLParser();
    private final NQLOptimiser optimiser;
    private final StringSink sink = new StringSink();
    private final RecordSourcePrinter printer = new RecordSourcePrinter(sink);

    public NQLOptimiserTest() {
        this.optimiser = new NQLOptimiser(factory);
    }

    @Test
    public void testIntervalAndIndexSearch() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 3600 * 24 * 10, Dates.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
        w.commit();

        final String expected = "BP.L\t518.117187500000\t765.889160156250\t2015-02-12T10:04:33.000Z\n" +
                "BP.L\t992.000000000000\t0.750000000000\t2015-02-12T10:04:27.000Z\n" +
                "BP.L\t768.000000000000\t924.000000000000\t2015-02-12T10:04:04.000Z\n" +
                "BP.L\t788.000000000000\t55.569427490234\t2015-02-12T10:04:02.000Z\n" +
                "BP.L\t512.000000000000\t0.000000318310\t2015-02-12T10:03:56.000Z\n" +
                "BP.L\t968.953491210938\t0.029868379235\t2015-02-12T10:03:21.000Z\n" +
                "BP.L\t949.156250000000\t63.068359375000\t2015-02-12T10:02:53.000Z\n" +
                "BP.L\t807.750000000000\t705.548904418945\t2015-02-12T10:02:49.000Z\n" +
                "BP.L\t980.000000000000\t133.570312500000\t2015-02-12T10:02:14.000Z\n" +
                "BP.L\t512.000000000000\t74.948242187500\t2015-02-12T10:01:31.000Z\n" +
                "BP.L\t768.000000000000\t0.000000011709\t2015-02-12T10:01:18.000Z\n" +
                "BP.L\t564.425537109375\t0.000000003711\t2015-02-12T10:00:40.000Z\n" +
                "ADM.L\t696.000000000000\t9.672361135483\t2015-02-12T10:04:25.000Z\n" +
                "ADM.L\t965.062500000000\t0.000000591804\t2015-02-12T10:04:22.000Z\n" +
                "ADM.L\t718.848632812500\t907.609375000000\t2015-02-12T10:04:13.000Z\n" +
                "ADM.L\t512.000000000000\t0.000000000000\t2015-02-12T10:03:31.000Z\n" +
                "ADM.L\t768.000000000000\t0.000047940810\t2015-02-12T10:03:19.000Z\n" +
                "ADM.L\t768.000000000000\t296.109375000000\t2015-02-12T10:02:35.000Z\n" +
                "ADM.L\t837.343750000000\t0.061431560665\t2015-02-12T10:00:04.000Z\n";
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
    public void testLatestBy() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");
        TestUtils.generateQuoteData(w, 3600 * 24, Dates.parseDateTime("2015-02-12T03:00:00.000Z"), Dates.SECOND_MILLIS);
        w.commit();

        final String expected = "GKN.L\t688.000000000000\t256.000000000000\t2015-02-13T02:59:50.000Z\n";
        assertThat(expected, "select sym, bid, ask, timestamp from q latest by sym where sym in ('GKN.L') and ask > 100");
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
        sink.clear();
        printer.print(compile(query));
        Assert.assertEquals(expected, sink.toString());
    }

    private RecordSource<? extends Record> compile(CharSequence query) throws ParserException, JournalException {
        parser.setContent(query);
        return optimiser.compile(parser.parse().getQueryModel());
    }

}
