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

package com.nfsdb.lang;

import com.nfsdb.JournalWriter;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.lang.cst.impl.jsrc.TopRecordSource;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TopRecordSourceTest extends AbstractTest {
    @Test
    public void testTopSource() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);

        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.print(new TopRecordSource(10, w.rows()));

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
