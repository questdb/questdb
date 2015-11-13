/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ql;

import com.nfsdb.JournalWriter;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.impl.TopRecordSource;
import com.nfsdb.ql.ops.constant.LongConstant;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TopRecordSourceTest extends AbstractTest {

    @Test
    public void testBottomSource() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(new TopRecordSource(compiler.compileSource("quote"), new LongConstant(99997), new LongConstant(100000)).prepareCursor(factory));

        final String expected = "2013-11-04T10:00:00.000Z\tBT-A.L\t168.000000000000\t0.001307277009\t319936098\t1456039311\tFast trading\tLXE\n" +
                "2013-11-04T10:00:00.000Z\tAGK.L\t0.000031983279\t878.000000000000\t819380635\t1732419403\tFast trading\tLXE\n" +
                "2013-11-04T10:00:00.000Z\tHSBA.L\t243.601509094238\t44.582113265991\t532679143\t345298132\tFast trading\tLXE\n";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testMiddleSource() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(new TopRecordSource(compiler.compileSource("quote"), new LongConstant(102), new LongConstant(112)).prepareCursor(factory));

        final String expected = "2013-09-04T10:00:00.000Z\tTLW.L\t0.003675992833\t0.000000006044\t233699709\t984001343\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tGKN.L\t0.000001392326\t0.000000010696\t1921077830\t83098719\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tHSBA.L\t125.000000000000\t113.359375000000\t347349195\t1619900957\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tTLW.L\t0.000000539488\t1.938893854618\t1012023467\t596418088\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tBP.L\t0.009742939146\t0.000000729716\t952785207\t94086655\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tBT-A.L\t10.085297346115\t0.293467730284\t1376102367\t166757857\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tABF.L\t488.272369384766\t342.142333984375\t1016986855\t1939793032\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tLLOY.L\t601.087127685547\t0.519029200077\t337891645\t1650060090\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tABF.L\t0.025374564342\t0.009976797737\t1448235215\t107181743\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tAGK.L\t335.908935546875\t492.000000000000\t1466344037\t79845289\tFast trading\tLXE\n";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testNoRows() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(new TopRecordSource(compiler.compileSource("quote"), new LongConstant(99997), new LongConstant(10)).prepareCursor(factory));

        Assert.assertEquals("", sink.toString());
    }

    @Test
    public void testTopSource() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(new TopRecordSource(compiler.compileSource("quote"), new LongConstant(0), new LongConstant(10)).prepareCursor(factory));

        final String expected = "2013-09-04T10:00:00.000Z\tBT-A.L\t0.000001189157\t1.050231933594\t1326447242\t948263339\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tADM.L\t104.021850585938\t0.006688738358\t1575378703\t1436881714\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tAGK.L\t879.117187500000\t496.806518554688\t1530831067\t339631474\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tABF.L\t768.000000000000\t0.000020634160\t426455968\t1432278050\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tABF.L\t256.000000000000\t0.000000035797\t1404198\t1153445279\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tWTB.L\t920.625000000000\t0.040750414133\t761275053\t1232884790\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tAGK.L\t512.000000000000\t896.000000000000\t422941535\t113506296\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tRRS.L\t12.923866510391\t0.032379742712\t2006313928\t2132716300\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tBT-A.L\t0.006530375686\t0.000000000000\t1890602616\t2137969456\tFast trading\tLXE\n" +
                "2013-09-04T10:00:00.000Z\tABF.L\t0.000000017324\t720.000000000000\t410717394\t458818940\tFast trading\tLXE\n";
        Assert.assertEquals(expected, sink.toString());
    }
}
