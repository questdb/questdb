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

import com.nfsdb.journal.collections.LongArrayList;
import com.nfsdb.journal.column.SymbolIndex;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.query.api.QueryAllBuilder;
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class PerformanceTest extends AbstractTest {

    public static final int TEST_DATA_SIZE = 1000000;
    private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class);
    private static boolean enabled = false;

    @BeforeClass
    public static void setUp() throws Exception {
        enabled = System.getProperty("nfsdb.enable.perf.tests") != null;
    }

    @Test
    public void testJournalAppendAndReadSpeed() throws JournalException {
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        long t = 0;
        for (int i = -3; i < 5; i++) {
            w.truncate();
            if (i == 0) {
                t = System.nanoTime();
            }
            TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.toMillis("2013-10-05T10:00:00.000Z"), 1000);
            w.commit();
        }


        long result = System.nanoTime() - t;
        LOGGER.info("append (1M): " + TimeUnit.NANOSECONDS.toMillis(result / 5) + "ms");
        if (enabled) {
            Assert.assertTrue("Append speed must be under 750ms (" + TimeUnit.NANOSECONDS.toMillis(result) + ")", TimeUnit.NANOSECONDS.toMillis(result) < 700);
        }


        for (int i = -5; i < 10; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            Iterator<Quote> iterator = w.bufferedIterator();
            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            Assert.assertEquals(TEST_DATA_SIZE, count);
        }
        result = System.nanoTime() - t;
        LOGGER.info("read (1M): " + TimeUnit.NANOSECONDS.toMillis(result / 10) + "ms");
        if (enabled) {
            Assert.assertTrue("Read speed must be under 300ms (" + TimeUnit.NANOSECONDS.toMillis(result) + ")", TimeUnit.NANOSECONDS.toMillis(result) < 300);
        }
    }

    @Test
    public void testIndexAppendAndReadSpeed() throws JournalException {
        File indexFile = new File(factory.getConfiguration().getJournalBase(), "index-test");
        int totalKeys = 30000;
        int totalValues = 20000000;
        try (SymbolIndex index = new SymbolIndex(indexFile, totalKeys, totalValues, JournalMode.APPEND, 0)) {
            long valuesPerKey = totalValues / totalKeys;

            long t = System.nanoTime();
            long count = 0;
            for (int k = 0; k < totalKeys; k++) {
                for (int v = 0; v < valuesPerKey; v++) {
                    index.put(k, k * valuesPerKey + v);
                    count++;
                }
            }

            Assert.assertEquals(count, index.size());
            // make sure that ~20M items appended in under 1s
            t = System.nanoTime() - t;
            LOGGER.info("index append latency: " + t / totalValues + "ns");
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
            LOGGER.info("index value count lookup latency: " + t / 10 + "ns");
            if (enabled) {
                Assert.assertTrue("Count lookup must be under 150ns: " + t, t / 10 < 150);
            }

            LongArrayList list = new LongArrayList();
            for (int i = -10; i < 10; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                index.getValues(13567 + i, list);
            }
            t = System.nanoTime() - t;
            LOGGER.info("index values lookup latency: " + t / 10 + "ns");
            if (enabled) {
                Assert.assertTrue("Values lookup must be under 1.5μs: " + t / 10, t / 10 < 1500);
            }
        }
    }

    @Test
    public void testAllBySymbolValueOverInterval() throws JournalException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.toMillis("2013-10-05T10:00:00.000Z"), 1000);
        w.commit();

        try (Journal<Quote> journal = factory.reader(Quote.class)) {
            int count = 1000;
            Interval interval = Dates.interval(Dates.toMillis("2013-10-05T10:00:00.000Z"), Dates.toMillis("2013-10-15T10:00:00.000Z"));
            long t = System.nanoTime();
            QueryAllBuilder<Quote> builder = journal.query().all().withKeys("LLOY.L").limit(interval);
            for (int i = 0; i < count; i++) {
                builder.asResultSet();
            }
            LOGGER.info("allBySymbolValueOverInterval (query) latency: " + (System.nanoTime() - t) / count / 1000 + "μs");
        }
    }

    @Test
    public void testLatestBySymbol() throws JournalException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", TEST_DATA_SIZE);
        TestUtils.generateQuoteData(w, TEST_DATA_SIZE, Dates.toMillis("2013-10-05T10:00:00.000Z"), 1000);
        w.commit();

        try (Journal<Quote> journal = factory.reader(Quote.class)) {
            int count = 1000000;
            long t = System.nanoTime();
            for (int i = 0; i < count; i++) {
                journal.query().head().withKeys().asResultSet().read();
            }
            LOGGER.info("latestBySymbol (query+read) latency: " + (System.nanoTime() - t) / count + "ns");
        }
    }

}
