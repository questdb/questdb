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

package com.nfsdb;

import com.nfsdb.collections.LongList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.factory.JournalCachingFactory;
import com.nfsdb.logging.Log;
import com.nfsdb.logging.LogFactory;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Interval;
import com.nfsdb.model.Quote;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.parser.QueryCompiler;
import com.nfsdb.query.api.QueryAllBuilder;
import com.nfsdb.query.api.QueryHeadBuilder;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class PerformanceTest extends AbstractTest {

    public static final int TEST_DATA_SIZE = 1000000;
    private static final Log LOG = LogFactory.getLog(PerformanceTest.class);
    private static boolean enabled = false;

    @BeforeClass
    public static void setUp() {
        enabled = System.getProperty("nfsdb.enable.perf.tests") != null;
    }

    @Test
    public void testAllBySymbolValueOverInterval() throws JournalException, NumericException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
        w.commit();

        try (Journal<Quote> journal = factory.reader(Quote.class)) {
            int count = 1000;
            Interval interval = new Interval(Dates.parseDateTime("2013-10-15T10:00:00.000Z"), Dates.parseDateTime("2013-10-05T10:00:00.000Z"));
            long t = 0;
            QueryAllBuilder<Quote> builder = journal.query().all().withKeys("LLOY.L").slice(interval);
            for (int i = -1000; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                builder.asResultSet();
            }
            LOG.info().$("journal.query().all().withKeys(\"LLOY.L\").slice(interval) (query only) latency: ").$((System.nanoTime() - t) / count / 1000).$("μs").$();
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Test
    public void testAllBySymbolValueOverIntervalNew() throws JournalException, ParserException, InterruptedException, NumericException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
        w.commit();

        JournalCachingFactory cf = new JournalCachingFactory(factory.getConfiguration());
        QueryCompiler compiler = new QueryCompiler(cf);

        int count = 1000;
        long t = 0;
        for (int i = -count; i < count; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            for (Record r : compiler.compile("quote where timestamp = '2013-10-05T10:00:00.000Z;10d' and sym = 'LLOY.L'")) {
            }
        }
        LOG.info().$("NEW journal.query().all().withKeys(\"LLOY.L\").slice(interval) (query only) latency: ").$((System.nanoTime() - t) / count / 1000).$("μs").$();

        cf.close();
    }

    @Test
    public void testIndexAppendAndReadSpeed() throws JournalException {
        File indexFile = new File(factory.getConfiguration().getJournalBase(), "index-test");
        int totalKeys = 30000;
        int totalValues = 20000000;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0)) {
            long valuesPerKey = totalValues / totalKeys;

            long t = System.nanoTime();
            long count = 0;
            for (int k = 0; k < totalKeys; k++) {
                for (int v = 0; v < valuesPerKey; v++) {
                    index.add(k, k * valuesPerKey + v);
                    count++;
                }
            }

            Assert.assertEquals(count, index.size());
            // make sure that ~20M items appended in under 1s
            t = System.nanoTime() - t;
            LOG.info().$("index append latency: ").$(t / totalValues).$("ns").$();
            if (enabled) {
                Assert.assertTrue("~20M items must be appended under 1s: " + TimeUnit.NANOSECONDS.toMillis(t), TimeUnit.NANOSECONDS.toMillis(t) < 1000);
            }

            for (int i = -10; i < 10; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                index.getValueCount(1025);
            }
            t = System.nanoTime() - t;
            LOG.info().$("index value count lookup latency: ").$(+t / 10).$("ns").$();
            if (enabled) {
                Assert.assertTrue("Count lookup must be under 150ns: " + t, t / 10 < 150);
            }

            LongList list = new LongList();
            for (int i = -10; i < 10; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                index.getValues(13567 + i, list);
            }
            t = System.nanoTime() - t;
            LOG.info().$("index values lookup latency: ").$(+t / 10).$("ns").$();
            if (enabled) {
                Assert.assertTrue("Values lookup must be under 1.5μs: " + t / 10, t / 10 < 1500);
            }
        }
    }

    @Test
    public void testJournalAppendAndReadSpeed() throws JournalException, ParserException, NumericException {
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        long t = 0;
        int count = 10;
        for (int i = -count; i < count; i++) {
            w.truncate();
            if (i == 0) {
                t = System.nanoTime();
            }
            TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
            w.commit();
        }


        long result = System.nanoTime() - t;
        LOG.info().$("append (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();
        if (enabled) {
            Assert.assertTrue("Append speed must be under 400ms (" + TimeUnit.NANOSECONDS.toMillis(result) + ")", TimeUnit.NANOSECONDS.toMillis(result) < 400);
        }

        for (int i = -count; i < count; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            Iterator<Quote> iterator = JournalIterators.bufferedIterator(w);
            int cnt = 0;
            while (iterator.hasNext()) {
                iterator.next();
                cnt++;
            }
            Assert.assertEquals(TEST_DATA_SIZE, cnt);
        }
        result = System.nanoTime() - t;
        LOG.info().$("read (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();
        if (enabled) {
            Assert.assertTrue("Read speed must be under 120ms (" + TimeUnit.NANOSECONDS.toMillis(result) + ")", TimeUnit.NANOSECONDS.toMillis(result) < 120);
        }

        for (int i = -count; i < count; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            RecordCursor<? extends Record> s = compiler.compile("quote");
            int cnt = 0;
            for (Record r : s) {
                r.getLong(0);
                r.getSym(1);
                r.getDouble(2);
                r.getDouble(3);
                r.getInt(4);
                r.getInt(5);
                r.getSym(6);
                r.getSym(7);
                cnt++;
            }
            Assert.assertEquals(TEST_DATA_SIZE, cnt);
        }
        result = System.nanoTime() - t;
        LOG.info().$("generic read (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();
        if (enabled) {
            Assert.assertTrue("Read speed must be under 60ms (" + TimeUnit.NANOSECONDS.toMillis(result) + ")", TimeUnit.NANOSECONDS.toMillis(result) < 60);
        }
    }

    @Test
    public void testLatestBySymbol() throws JournalException, NumericException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
        w.commit();

        try (Journal<Quote> journal = factory.reader(Quote.class)) {
            int count = 1000000;
            long t = 0;
            QueryHeadBuilder qhb = journal.query().head().withKeys();
            for (int i = -100000; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                qhb.asResultSet().read();
            }
            LOG.info().$("journal.query().head().withKeys() (query+read) latency: ").$((System.nanoTime() - t) / count).$("ns").$();
        }
    }
}
