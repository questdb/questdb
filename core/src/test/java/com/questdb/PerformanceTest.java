/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb;

import com.questdb.cairo.*;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.common.Record;
import com.questdb.common.RecordCursor;
import com.questdb.ex.ParserException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.model.Quote;
import com.questdb.parser.sql.QueryCompiler;
import com.questdb.ql.RecordSource;
import com.questdb.std.LongList;
import com.questdb.std.NumericException;
import com.questdb.std.Rnd;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Interval;
import com.questdb.store.*;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.ReaderFactory;
import com.questdb.store.query.api.QueryAllBuilder;
import com.questdb.store.query.api.QueryHeadBuilder;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class PerformanceTest extends AbstractTest {

    private static final int TEST_DATA_SIZE = 1000000;
    private static final Log LOG = LogFactory.getLog(PerformanceTest.class);
    private static boolean enabled = false;

    @BeforeClass
    public static void setUp() {
        enabled = System.getProperty("questdb.enable.perf.tests") != null;
    }

    @Test
    public void testAllBySymbolValueOverInterval() throws JournalException, NumericException {

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, null, TEST_DATA_SIZE)) {
            TestUtils.generateQuoteData(w, TEST_DATA_SIZE, DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
            w.commit();
        }

        try (Journal<Quote> journal = getFactory().reader(Quote.class)) {
            int count = 1000;
            Interval interval = new Interval(DateFormatUtils.parseDateTime("2013-10-15T10:00:00.000Z"), DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z"));
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
    public void testAllBySymbolValueOverIntervalNew() throws JournalException, ParserException, NumericException {

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", TEST_DATA_SIZE)) {
            TestUtils.generateQuoteData(w, TEST_DATA_SIZE, DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
            w.commit();
        }

        QueryCompiler compiler = new QueryCompiler();

        Factory factory = getFactory();
        try (RecordSource src = compiler.compile(factory, "quote where timestamp = '2013-10-05T10:00:00.000Z;10d' and sym = 'LLOY.L'")) {
            int count = 1000;
            long t = 0;
            for (int i = -count; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                RecordCursor c = src.prepareCursor(factory);
                try {
                    for (; c.hasNext(); ) {
                        c.next();
                    }
                } finally {
                    c.releaseCursor();
                }
            }
            LOG.info().$("NEW journal.query().all().withKeys(\"LLOY.L\").slice(interval) (query only) latency: ").$((System.nanoTime() - t) / count / 1000).$("μs").$();
        }
    }

    @Test
    public void testCairoPerformance() throws NumericException {

        int count = 10;
        long t = 0;
        long result;

        CairoConfiguration configuration = new DefaultCairoConfiguration(getFactory().getConfiguration().getJournalBase().getAbsolutePath());
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        try (TableModel model = new TableModel(configuration, "quote", PartitionBy.NONE)
                .timestamp()
                .col("sym", ColumnType.SYMBOL)
                .col("bid", ColumnType.DOUBLE)
                .col("ask", ColumnType.DOUBLE)
                .col("bidSize", ColumnType.INT)
                .col("askSize", ColumnType.INT)
                .col("mode", ColumnType.SYMBOL).symbolCapacity(2)
                .col("ex", ColumnType.SYMBOL).symbolCapacity(2)) {
            CairoTestUtils.create(model);
        }
        try (TableWriter w = new TableWriter(configuration, "quote")) {
            for (int i = -count; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                w.truncate();
                long timestamp = DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z");
                Rnd r = new Rnd();
                int n = symbols.length - 1;
                for (int i1 = 0; i1 < TEST_DATA_SIZE; i1++) {
                    TableWriter.Row row = w.newRow(timestamp);
                    row.putSym(1, symbols[Math.abs(r.nextInt() % n)]);
                    row.putDouble(2, Math.abs(r.nextDouble2()));
                    row.putDouble(3, Math.abs(r.nextDouble2()));
                    row.putInt(4, Math.abs(r.nextInt()));
                    row.putInt(5, Math.abs(r.nextInt()));
                    row.putSym(6, "LXE");
                    row.putSym(7, "Fast trading");
                    row.append();
                    timestamp += (long) 1000;
                }
                w.commit();
            }
            result = System.nanoTime() - t;
        }

        LOG.info().$("Cairo append (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();

        try (TableReader reader = new TableReader(configuration, "quote")) {
            for (int i = -count; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }

                com.questdb.cairo.sql.RecordCursor cursor = reader.getCursor();
                while (cursor.hasNext()) {
                    com.questdb.cairo.sql.Record r = cursor.next();
                    r.getDate(0);
                    r.getSym(1);
                    r.getDouble(2);
                    r.getDouble(3);
                    r.getInt(4);
                    r.getInt(5);
                    r.getSym(6);
                    r.getSym(7);
                }
            }
            result = (System.nanoTime() - t) / count;
        }
        LOG.info().$("Cairo read (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result)).$("ms").$();
    }

    @Test
    public void testIndexAppendAndReadSpeed() throws JournalException {
        File indexFile = new File(getFactory().getConfiguration().getJournalBase(), "index-test");
        int totalKeys = 30000;
        int totalValues = 20000000;
        try (KVIndex index = new KVIndex(indexFile, totalKeys, totalValues, 1, JournalMode.APPEND, 0, false)) {
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
        int count = 10;
        long t = 0;
        long result;

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", TEST_DATA_SIZE)) {
            for (int i = -count; i < count; i++) {
                w.truncate();
                if (i == 0) {
                    t = System.nanoTime();
                }
                TestUtils.generateQuoteData(w, TEST_DATA_SIZE, DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
                w.commit();
            }


            result = System.nanoTime() - t;
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
        }

        ReaderFactory readerFactory = getFactory();
        try (RecordSource rs = compile("quote")) {
            for (int i = -count; i < count; i++) {
                if (i == 0) {
                    t = System.nanoTime();
                }
                RecordCursor s = rs.prepareCursor(readerFactory);
                try {
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
                } finally {
                    s.releaseCursor();
                }
            }
        }
        result = System.nanoTime() - t;
        LOG.info().$("generic read (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();
        if (enabled) {
            Assert.assertTrue("Read speed must be under 60ms (" + TimeUnit.NANOSECONDS.toMillis(result) + ")", TimeUnit.NANOSECONDS.toMillis(result) < 60);
        }
    }

    @Test
    public void testLatestBySymbol() throws JournalException, NumericException {

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, null, TEST_DATA_SIZE)) {
            TestUtils.generateQuoteData(w, TEST_DATA_SIZE, DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
            w.commit();
        }

        try (Journal<Quote> journal = getFactory().reader(Quote.class)) {
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

    @Test
    public void testRawAppendPerformance() throws JournalException, NumericException {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", TEST_DATA_SIZE)) {
            long t = 0;
            int count = 10;
            for (int i = -count; i < count; i++) {
                w.truncate();
                if (i == 0) {
                    t = System.nanoTime();
                }
                TestUtils.generateQuoteData2(w, TEST_DATA_SIZE, DateFormatUtils.parseDateTime("2013-10-05T10:00:00.000Z"), 1000);
                w.commit();
            }
            long result = System.nanoTime() - t;
            LOG.info().$("raw append (1M): ").$(TimeUnit.NANOSECONDS.toMillis(result / count)).$("ms").$();
        }
    }

}
