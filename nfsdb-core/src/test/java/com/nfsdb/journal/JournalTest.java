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

import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.model.Trade2;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestData;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.tx.TxFuture;
import com.nfsdb.journal.tx.TxListener;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Files;
import com.nfsdb.journal.utils.Lists;
import com.nfsdb.journal.utils.Rows;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class JournalTest extends AbstractTest {

    @Test
    public void testMaxRowIDBlankJournal() throws Exception {
        Journal<Quote> journal = factory.writer(Quote.class);
        Assert.assertEquals(-1, journal.getMaxRowID());
    }

    @Test
    public void testMaxRowIDForJournalWithEmptyPartition() throws Exception {
        JournalWriter<Quote> journal = factory.writer(Quote.class);
        journal.getAppendPartition(System.currentTimeMillis());
        Assert.assertEquals(-1, journal.getMaxRowID());
    }

    @Test
    public void testMaxRowID() throws JournalException {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100);

        long maxRowID = w.getMaxRowID();
        int partitionID = Rows.toPartitionIndex(maxRowID);
        long localRowID = Rows.toLocalRowID(maxRowID);
        Assert.assertEquals(w.lastNonEmpty().getPartitionIndex(), partitionID);
        Assert.assertEquals(w.lastNonEmpty().size() - 1, localRowID);
        Assert.assertEquals(35184372088864L, maxRowID);
    }

    @Test
    public void testIncrementRowID() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        ResultSet<Quote> rs = w.query().all().asResultSet();

        for (int i = 1; i < rs.size(); i++) {
            Assert.assertEquals(rs.getRowID(i), w.incrementRowID(rs.getRowID(i - 1)));
        }
    }

    @Test
    public void testDecrementRowID() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        ResultSet<Quote> rs = w.query().all().asResultSet();

        for (int i = 1; i < rs.size(); i++) {
            Assert.assertEquals(rs.getRowID(i - 1), w.decrementRowID(rs.getRowID(i)));
        }
    }

    @Test(expected = JournalException.class)
    public void testAddPartitionOutOfOrder() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.getAppendPartition(Dates.toMillis("2012-02-10T10:00:00.000Z"));
        w.getAppendPartition(Dates.toMillis("2012-01-10T10:00:00.000Z"));
    }

    @Test
    public void testReadableColumns() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestData.appendQuoteData1(w);

        String expected = "null\tALDW\t0.25400183821760114\t0.0\t0\t0\tnull\tnull\n" +
                "null\tHSBA.L\t0.2534455148850241\t0.0\t0\t0\tnull\tnull\n" +
                "null\tALDW\t0.8527617099649412\t0.0\t0\t0\tnull\tnull\n" +
                "null\tAMD\t0.7900175963007109\t0.0\t0\t0\tnull\tnull\n" +
                "null\tAMD\t0.3667176571649916\t0.0\t0\t0\tnull\tnull\n" +
                "null\tHSBA.L\t0.08280315349114653\t0.0\t0\t0\tnull\tnull\n" +
                "null\tAMD\t0.32594344090522576\t0.0\t0\t0\tnull\tnull\n" +
                "null\tAMD\t0.05533504314019688\t0.0\t0\t0\tnull\tnull\n" +
                "null\tAMD\t0.061826046796662926\t0.0\t0\t0\tnull\tnull\n" +
                "null\tHSBA.L\t0.30903524429086027\t0.0\t0\t0\tnull\tnull";

        Journal<Quote> r = factory.reader(Quote.class).setReadColumns("sym", "bid");
        TestUtils.assertEquals(expected, r.query().all().asResultSet().subset(90, 100));
    }

    @Test
    public void testReindex() throws JournalException {
        File path;
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            TestData.appendQuoteData1(w);
            path = w.getLocation();
        }

        Files.deleteOrException(new File(path, "2013-02/sym.r"));
        Files.deleteOrException(new File(path, "2013-02/sym.k"));

        Journal<Quote> journal = factory.reader(Quote.class);
        try {
            journal.query().head().withKeys().asResultSet().read();
            Assert.fail("Expected exception here");
        } catch (JournalException e) {
            // do nothing
        }
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.rebuildIndexes();
        }
        Assert.assertEquals(3, journal.query().head().withKeys().asResultSet().read().length);
    }

    @Test
    public void testSizeAfterCompaction() throws JournalException {
        long sizeAfterCompaction;
        try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 1000000)) {
            TestData.appendQuoteData2(w);
        }

        try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote")) {
            File f = new File(w.getLocation(), "2013-03/sym.d");
            long size = f.length();
            w.compact();
            sizeAfterCompaction = f.length();
            Assert.assertTrue(sizeAfterCompaction < size);
        }

        try (Journal<Quote> r = factory.reader(Quote.class, "quote")) {
            Assert.assertEquals(1000, r.query().all().size());
            File f = new File(r.getLocation(), "2013-03/sym.d");
            Assert.assertEquals(sizeAfterCompaction, f.length());
        }
    }

    @Test
    public void testAppendReadBitSet() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        Quote q = new Quote().setSym("A").setAsk(10);
        Assert.assertFalse(q.isSetBid());
        Assert.assertTrue(q.isSetAsk());
        Assert.assertTrue(q.isSetSym());
        Assert.assertFalse(q.isSetEx());
        w.append(q);
        Quote q2 = w.query().all().asResultSet().readFirst();
        Assert.assertFalse(q2.isSetBid());
        Assert.assertTrue(q2.isSetAsk());
        Assert.assertTrue(q.isSetSym());
        Assert.assertFalse(q.isSetEx());
    }

    @Test
    public void testSingleWriterModel() throws Exception {
        JournalWriter<Quote> writer = factory.writer(Quote.class);
        Assert.assertTrue(writer != null);

        try {
            factory.writer(Quote.class);
            Assert.fail("Able to open second writer - error");
        } catch (JournalException e) {
            // ignore
        }
        // check if we can open a reader
        Assert.assertTrue(factory.reader(Quote.class) != null);
        // check if we can open writer in alt location
        Assert.assertTrue(factory.writer(Quote.class, "test-quote") != null);
    }

    @Test
    public void testMaxRowIDOnReader() throws Exception {
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 1000, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
            w.commit();
        }

        Journal<Quote> r = factory.reader(Quote.class).setReadColumns("sym");
        Assert.assertEquals(999, r.getMaxRowID());
        Assert.assertNull(r.lastNonEmpty().getAbstractColumn(3));
    }

    @Test
    public void testMaxRowIDOnEmptyReader() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.close();

        Journal<Quote> r = factory.reader(Quote.class).setReadColumns("sym");
        Assert.assertEquals(-1, r.getMaxRowID());
        Assert.assertNull(r.lastNonEmpty());
    }

    @Test
    public void testTxRollbackToEmpty() throws Exception {
        int SIZE = 100000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE)) {
            w.append(origin);
            w.rollback();

            Assert.assertEquals(0, w.size());
            Assert.assertEquals(1, w.getPartitionCount());
            Assert.assertEquals(0, w.getSymbolTable("sym").size());
            Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, w.getSymbolTable("sym").getQuick("LLOY.L"));
            Assert.assertNull(w.lastNonEmpty());
            Partition<Quote> p = w.getPartition(w.getPartitionCount() - 1, false);
            Assert.assertNotNull(p);
            Assert.assertEquals(0, p.getIndexForColumn("sym").size());
            Assert.assertEquals(0, p.getIndexForColumn("ex").size());
        }
    }

    @Test
    public void testTxRollbackSamePartition() throws Exception {
        int SIZE = 50000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE);
        w.append(origin);
        w.commit();
        TestUtils.generateQuoteData(w, 20, Dates.toMillis("2014-03-30T00:11:00Z"), 100);
        Assert.assertEquals(50020, w.size());
        Assert.assertEquals(50000, origin.size());
        w.rollback();
        TestUtils.assertEquals(origin, w);
    }

    @Test
    public void testTxRollbackMultiplePartitions() throws Exception {
        int SIZE = 50000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE);
        w.append(origin);
        w.commit();

        TestUtils.generateQuoteData(w, 50000, Dates.toMillis("2014-03-30T00:11:00Z"), 100000);

        Assert.assertEquals(100000, w.size());
        Assert.assertEquals(50000, origin.size());

        w.rollback();
        TestUtils.assertEquals(origin, w);
    }

    @Test
    public void testTxRefresh() throws Exception {
        int SIZE = 50000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE);
        w.append(origin);

        // check that refresh does not affect uncommitted changes
        w.refresh();
        TestUtils.assertEquals(origin, w);
    }

    @Test
    public void testOfflinePartition() throws Exception {
        int SIZE = 50000;
        File location;
        try (JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
            origin.commit();
            location = new File(origin.getLocation(), "2014-03");
        }

        Files.deleteOrException(location);

        try (JournalWriter<Quote> origin = factory.writer(Quote.class, "origin")) {
            Assert.assertEquals(25914, origin.size());
            TestUtils.generateQuoteData(origin, 3000, Dates.toMillis("2014-03-30T00:11:00Z"), 10000);
            Assert.assertEquals(28914, origin.size());
            origin.rollback();
            Assert.assertEquals(25914, origin.size());
        }
    }

    @Test
    public void testTxRollbackLag() throws JournalException {
        int SIZE = 150000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.append(origin.query().all().asResultSet().subset(0, 100000));
        w.commit();
        w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(100000, 120000).read()));
        w.commit();

        Journal<Quote> r = factory.reader(Quote.class);
        TestUtils.assertEquals(w, r);
        w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(120000, 150000).read()));
        w.rollback();
        TestUtils.assertEquals(w, r);
        w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(120000, 150000).read()));
        w.commit();

        r.refresh();
        TestUtils.assertEquals(w, r);
        TestUtils.assertDataEquals(origin, w);
    }

    @Test
    public void testTxLagTumbleDrier() throws Exception {
        int SIZE = 1000000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE / 12);
        JournalWriter<Quote> w = factory.writer(Quote.class, "q", SIZE / 12);

        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        ResultSet<Quote> originRs = origin.query().all().asResultSet();
        int blockSize = 5130;
        Random rnd = new Random(System.currentTimeMillis());

        try {
            for (int i = 0; i < originRs.size(); ) {
                int d = Math.min(i + blockSize, originRs.size());
                ResultSet<Quote> rs = originRs.subset(i, d);
                w.appendIrregular(Lists.asList(rs.read()));

                if (rnd.nextBoolean()) {
                    w.commit();
                    i += blockSize;
                } else {
                    w.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertFalse(w.isTxActive());
        TestUtils.assertDataEquals(origin, w);
    }

    @Test
    public void testTxTumbleDrier() throws Exception {
        int SIZE = 1000000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE / 12);
        JournalWriter<Quote> w = factory.writer(Quote.class, "q", SIZE / 12);

        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        ResultSet<Quote> originRs = origin.query().all().asResultSet();
        int blockSize = 5130;
        Random rnd = new Random(System.currentTimeMillis());

        try {
            for (int i = 0; i < originRs.size(); ) {
                int d = Math.min(i + blockSize, originRs.size());
                ResultSet<Quote> rs = originRs.subset(i, d);
                w.append(rs);

                if (rnd.nextBoolean()) {
                    w.commit();
                    i += blockSize;
                } else {
                    w.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertFalse(w.isTxActive());
        TestUtils.assertDataEquals(origin, w);
    }


    @Test
    public void testLargeClass() throws Exception {
        try (JournalWriter<Trade2> writer = factory.writer(Trade2.class)) {
            Trade2 trade = new Trade2();
            trade.setStop87(10);
            Assert.assertTrue(trade.isSetStop87());
            writer.append(trade);
            writer.commit();

            Trade2 readTrade = writer.query().all().asResultSet().readFirst();
            Assert.assertTrue(readTrade.isSetStop87());
        }
    }

    @Test
    public void testTxListener() throws Exception {
        int SIZE = 10000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");

        TestUtils.generateQuoteData(origin, SIZE, Dates.toMillis("2014-01-30T00:11:00Z"), SIZE);
        origin.commit();

        TestTxListener lsnr = new TestTxListener();
        w.setTxListener(lsnr);


        w.append(origin.query().all().asResultSet().subset(0, 1000));
        w.commit();
        Assert.assertTrue(lsnr.isNotifyAsyncNoWait());
        Assert.assertFalse(lsnr.isNotifyAsync());
        Assert.assertFalse(lsnr.isNotifySync());

        lsnr.reset();

        w.append(origin.query().all().asResultSet().subset(1000, 2000));
        TxFuture future = w.commitAsync();
        Assert.assertNotNull(future);
        Assert.assertFalse(lsnr.isNotifyAsyncNoWait());
        Assert.assertTrue(lsnr.isNotifyAsync());
        Assert.assertFalse(lsnr.isNotifySync());


        lsnr.reset();

        w.append(origin.query().all().asResultSet().subset(2000, 4000));
        Assert.assertTrue(w.commitAndWait(100, TimeUnit.MILLISECONDS));
        Assert.assertFalse(lsnr.isNotifyAsyncNoWait());
        Assert.assertFalse(lsnr.isNotifyAsync());
        Assert.assertTrue(lsnr.isNotifySync());
    }

    @Test
    public void testFirstSymbolNull() throws JournalException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 1000);
        long timestamp = Dates.toMillis("2013-10-05T10:00:00.000Z");
        Quote q = new Quote();
        for (int i = 0; i < 3; i++) {
            w.clearObject(q);
            if (i == 0) {
                q.setAsk(123);
                Assert.assertTrue(q.isSetAsk());
            } else {
                Assert.assertFalse(q.isSetAsk());
            }


            q.setTimestamp(timestamp);
            w.append(q);
        }

        w.commit();
        w.close();

        Journal<Quote> r = factory.reader(Quote.class, "quote");
        q = r.read(0);
        Quote q1 = r.read(1);

        Assert.assertNull(q.getSym());
        Assert.assertTrue(q.isSetAsk());

        Assert.assertFalse(q1.isSetAsk());
    }

    @Test
    public void testOpenJournalWithWrongPartitionType() throws Exception {
        JournalWriter<Quote> w = factory.writer(new JournalKey<>(Quote.class, "quote", PartitionType.NONE));
        TestUtils.generateQuoteData(w, 1000);
        w.close();

        try {
            factory.writer(new JournalKey<>(Quote.class, "quote", PartitionType.MONTH));
            Assert.fail("Exception expected");
        } catch (JournalException e) {
            // expect exception
        }

        JournalFactory f2 = new JournalFactory(new JournalConfiguration("/db-factory-test.xml", factory.getConfiguration().getJournalBase()).build());
        try {
            f2.writer(new JournalKey<>(Quote.class, "quote", PartitionType.NONE));
            Assert.fail("Exception expected");
        } catch (JournalException e) {
            // expect exception
        }

    }

    private static class TestTxListener implements TxListener {

        private boolean notifyAsyncNoWait = false;
        private boolean notifySync = false;
        private boolean notifyAsync = false;

        public void reset() {
            notifyAsyncNoWait = false;
            notifySync = false;
            notifyAsync = false;
        }

        public boolean isNotifyAsyncNoWait() {
            return notifyAsyncNoWait;
        }

        public boolean isNotifySync() {
            return notifySync;
        }

        public boolean isNotifyAsync() {
            return notifyAsync;
        }

        @Override
        public boolean notifySync(long timeout, TimeUnit unit) {
            notifySync = true;
            return true;
        }

        @Override
        public void notifyAsyncNoWait() {
            notifyAsyncNoWait = true;
        }

        @Override
        public TxFuture notifyAsync() {
            notifyAsync = true;
            return new TxFuture() {
                @Override
                public boolean waitFor(long time, TimeUnit unit) {
                    return true;
                }
            };
        }
    }
}
