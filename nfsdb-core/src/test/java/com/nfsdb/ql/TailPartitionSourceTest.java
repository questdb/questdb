/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.ql;

import com.nfsdb.JournalWriter;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.impl.AllRowSource;
import com.nfsdb.ql.impl.JournalSource;
import com.nfsdb.ql.impl.JournalTailPartitionSource;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Rows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TailPartitionSourceTest extends AbstractTest {

    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testTail() throws Exception {

        final String expected = "2014-04-28T02:05:06.000Z\tADM.L\t0.000000009919\t0.000000059185\t1290623970\t873017617\tFast trading\tLXE\n" +
                "2014-04-28T23:12:27.000Z\tABF.L\t0.040659694001\t0.000000818963\t2029231362\t400205299\tFast trading\tLXE\n" +
                "2014-04-29T20:19:48.000Z\tGKN.L\t0.000012560774\t5.638884067535\t1722682901\t1025310386\tFast trading\tLXE\n" +
                "2014-04-30T17:27:09.000Z\tBT-A.L\t873.000000000000\t127.708919525146\t1753567161\t256128916\tFast trading\tLXE\n" +
                "2014-05-01T14:34:30.000Z\tTLW.L\t939.150390625000\t512.000000000000\t447593202\t135792148\tFast trading\tLXE\n" +
                "2014-05-02T11:41:51.000Z\tBT-A.L\t978.632812500000\t0.000194281980\t1567482693\t8264817\tFast trading\tLXE\n" +
                "2014-05-03T08:49:12.000Z\tADM.L\t844.000000000000\t46.725388526917\t432358603\t1657803999\tFast trading\tLXE\n" +
                "2014-05-04T05:56:33.000Z\tRRS.L\t31.000000000000\t428.000000000000\t481303173\t739168384\tFast trading\tLXE\n" +
                "2014-05-05T03:03:54.000Z\tWTB.L\t75.842805862427\t0.000002408446\t1297032488\t1215601315\tFast trading\tLXE\n" +
                "2014-05-06T00:11:15.000Z\tADM.L\t283.158325195313\t880.000000000000\t357119340\t970294725\tFast trading\tLXE\n" +
                "2014-05-06T21:18:36.000Z\tBT-A.L\t0.000003385293\t965.151367187500\t844915022\t1889838659\tFast trading\tLXE\n" +
                "2014-05-07T18:25:57.000Z\tABF.L\t0.031326758675\t0.360216885805\t263487884\t1680503149\tFast trading\tLXE\n" +
                "2014-05-08T15:33:18.000Z\tADM.L\t508.375000000000\t768.000000000000\t1923884740\t1121347399\tFast trading\tLXE\n" +
                "2014-05-09T12:40:39.000Z\tRRS.L\t12.290055274963\t0.011763263494\t304383158\t2039181884\tFast trading\tLXE\n" +
                "2014-05-10T09:48:00.000Z\tRRS.L\t4.111308574677\t1.257051765919\t159178348\t604818378\tFast trading\tLXE\n" +
                "2014-05-11T06:55:21.000Z\tAGK.L\t0.000002205143\t228.001586914063\t1815322506\t1197959281\tFast trading\tLXE\n" +
                "2014-05-12T04:02:42.000Z\tAGK.L\t0.000000129186\t0.011274382472\t1060590724\t1159512064\tFast trading\tLXE\n" +
                "2014-05-13T01:10:03.000Z\tBP.L\t19.756743907928\t0.000340424791\t526923908\t1034870849\tFast trading\tLXE\n" +
                "2014-05-13T22:17:24.000Z\tADM.L\t0.000180012023\t0.000000052629\t1406232957\t378247895\tFast trading\tLXE\n" +
                "2014-05-14T19:24:45.000Z\tBP.L\t400.000000000000\t106.662109375000\t1440131320\t971963578\tFast trading\tLXE\n" +
                "2014-05-15T16:32:06.000Z\tWTB.L\t68.043695449829\t380.435256958008\t1204423553\t795316150\tFast trading\tLXE\n" +
                "2014-05-16T13:39:27.000Z\tBT-A.L\t0.004094106262\t0.000000914462\t2138704106\t655076307\tFast trading\tLXE\n" +
                "2014-05-17T10:46:48.000Z\tABF.L\t64.000000000000\t445.875000000000\t1848218326\t1023760162\tFast trading\tLXE\n" +
                "2014-05-18T07:54:09.000Z\tBT-A.L\t0.000000157150\t6.012886285782\t1247892921\t1890713369\tFast trading\tLXE\n" +
                "2014-05-19T05:01:30.000Z\tGKN.L\t497.000000000000\t111.975021362305\t24972718\t1583707719\tFast trading\tLXE\n" +
                "2014-05-20T02:08:51.000Z\tGKN.L\t549.125503540039\t18.834793090820\t957024901\t1487779338\tFast trading\tLXE\n" +
                "2014-05-20T23:16:12.000Z\tGKN.L\t0.000000150060\t412.000000000000\t1158267509\t980916820\tFast trading\tLXE\n" +
                "2014-05-21T20:23:33.000Z\tTLW.L\t0.000000007168\t41.500000000000\t1725416460\t1078921487\tFast trading\tLXE\n" +
                "2014-05-22T17:30:54.000Z\tBP.L\t354.286376953125\t540.000000000000\t1772028439\t1496904948\tFast trading\tLXE\n" +
                "2014-05-23T14:38:15.000Z\tABF.L\t0.000010415702\t372.000000000000\t1262895671\t1914805024\tFast trading\tLXE\n" +
                "2014-05-24T11:45:36.000Z\tRRS.L\t728.300781250000\t15.240249156952\t1926049591\t2113879331\tFast trading\tLXE\n" +
                "2014-05-25T08:52:57.000Z\tGKN.L\t448.000000000000\t0.696862459183\t1499957018\t1753521575\tFast trading\tLXE\n" +
                "2014-05-26T06:00:18.000Z\tLLOY.L\t0.019178653602\t1024.000000000000\t1445553836\t896453005\tFast trading\tLXE\n" +
                "2014-05-27T03:07:39.000Z\tRRS.L\t1021.000000000000\t391.000000000000\t856634079\t840591709\tFast trading\tLXE\n";

        JournalWriter<Quote> w = factory.writer(Quote.class);

        long lo = Dates.parseDateTime("2014-03-01T00:00:00.000Z");
        long hi = Dates.parseDateTime("2014-05-28T00:15:00.000Z");

        TestUtils.generateQuoteData(w, 100, new Interval(lo, hi));

        RecordSourcePrinter p = new RecordSourcePrinter(sink);

        p.print(
                new JournalSource(
                        new JournalTailPartitionSource(w, false, Rows.toRowID(1, 30))
                        , new AllRowSource()
                )
        );

        Assert.assertEquals(expected, sink.toString());
    }
}
