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

package com.nfsdb.journal;

import com.nfsdb.journal.map.JournalHashMap;
import com.nfsdb.journal.map.JournalMap;
import com.nfsdb.journal.printer.JournalPrinter;
import com.nfsdb.journal.printer.appender.OutputStreamAppender;
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestData;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class JournalMapTest extends AbstractTest {

    private JournalWriter<Quote> w;

    @Before
    public void setUp() throws Exception {
        w = factory.writer(Quote.class);
        TestData.appendQuoteData2(w);
    }

    @Test
    public void testEagerMap() throws Exception {
        String expected = "2013-05-06T05:53:20.000Z\tGKN.L\t0.6692538497726956\t0.38655868492682444\t1851982635\t389597203\tFast trading\tSK\n" +
                "2013-05-06T19:46:40.000Z\tLLOY.L\t0.7193355196302277\t0.9404701555734597\t704067095\t1668582762\tFast trading\tGR\n" +
                "2013-05-07T04:06:40.000Z\tADM.L\t0.861499400556018\t0.5243336208514339\t648366676\t698699955\tFast trading\tSK\n" +
                "2013-05-07T05:30:00.000Z\tTLW.L\t0.5405954330001682\t0.23821846687340553\t1057951361\t1833806187\tFast trading\tLXE\n" +
                "2013-05-07T02:43:20.000Z\tRRS.L\t0.7051458116683191\t0.40922139034207683\t631143150\t997183581\tFast trading\tLXE\n" +
                "2013-05-06T17:00:00.000Z\tABF.L\t0.9643251238886018\t0.8129177499445661\t1945548281\t499978914\tFast trading\tSK\n" +
                "2013-05-06T01:43:20.000Z\tWTB.L\t0.4191390374952899\t0.601628162260468\t2104114979\t1431449453\tFast trading\tLXE\n" +
                "2013-05-06T22:33:20.000Z\tBP.L\t0.46114752131469783\t0.8979723557722048\t1579801637\t2117833235\tFast trading\tGR\n" +
                "2013-05-07T01:20:00.000Z\tAGK.L\t0.5217691943089976\t0.28956673106743125\t681718809\t1972465127\tFast trading\tLXE\n" +
                "2013-05-06T23:56:40.000Z\tBT-A.L\t0.8883345886748325\t0.7737938544972554\t1750740931\t1514907397\tFast trading\tGR";

        JournalMap<Quote> map = new JournalHashMap<>(w).eager();
        Assert.assertEquals(10, map.size());
        assertEquals(expected, map);
    }

    @Test
    public void testLazyMap() throws Exception {
        String expected = "2013-05-07T02:43:20.000Z\tRRS.L\t0.7051458116683191\t0.40922139034207683\t631143150\t997183581\tFast trading\tLXE\n" +
                "2013-05-06T23:56:40.000Z\tBT-A.L\t0.8883345886748325\t0.7737938544972554\t1750740931\t1514907397\tFast trading\tGR";
        JournalMap<Quote> map = new JournalHashMap<>(factory.reader(Quote.class));
        map.get("RRS.L");
        map.get("BT-A.L");
        Assert.assertEquals(2, map.size());
        assertEquals(expected, map);
        // if underlying journal is updated refresh should clear cache
        map.refresh();
        Assert.assertEquals(2, map.size());
    }

    private <T> void assertEquals(String expected, JournalMap<T> map) throws IOException {
        JournalPrinter p = new JournalPrinter();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        p.setAppender(new OutputStreamAppender(bos));
        TestUtils.configure(p, w.getMetadata());
        for (T q : map.values()) {
            p.out(q);
        }
        p.close();

        String separator = System.getProperty("line.separator");
        String[] actual = bos.toString(Files.UTF_8.name()).split(separator);
        String[] exp = expected.split("\n");
        Arrays.sort(actual);
        Arrays.sort(exp);
        Assert.assertArrayEquals(exp, actual);
    }

    @SuppressWarnings("unused")
    private <T> void out(JournalMap<T> map) throws IOException {
        JournalPrinter p = new JournalPrinter();
        TestUtils.configure(p, w.getMetadata());
        for (T q : map.values()) {
            p.out(q);
        }
        p.close();
    }
}
