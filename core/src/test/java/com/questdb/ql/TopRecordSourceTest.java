/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.JournalWriter;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.model.Quote;
import com.questdb.ql.impl.TopRecordSource;
import com.questdb.ql.ops.constant.LongConstant;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TopRecordSourceTest extends AbstractTest {

    @Test
    public void testBottomSource() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(new TopRecordSource(compiler.compileSource(factory, "quote"), new LongConstant(99997), new LongConstant(100000)).prepareCursor(factory));

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
        p.printCursor(new TopRecordSource(compiler.compileSource(factory, "quote"), new LongConstant(102), new LongConstant(112)).prepareCursor(factory));

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
        p.printCursor(new TopRecordSource(compiler.compileSource(factory, "quote"), new LongConstant(99997), new LongConstant(10)).prepareCursor(factory));

        Assert.assertEquals("", sink.toString());
    }

    @Test
    public void testTopSource() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(new TopRecordSource(compiler.compileSource(factory, "quote"), new LongConstant(0), new LongConstant(10)).prepareCursor(factory));

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
