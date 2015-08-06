/*
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
 */

package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.query.ResultSet;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.storage.TxListener;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestData;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Files;
import com.nfsdb.utils.Rnd;
import com.nfsdb.utils.Rows;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class JournalTest extends AbstractTest {

    @Test(expected = JournalException.class)
    public void testAddPartitionOutOfOrder() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.getAppendPartition(Dates.parseDateTime("2012-02-10T10:00:00.000Z"));
        w.getAppendPartition(Dates.parseDateTime("2012-01-10T10:00:00.000Z"));
    }

    @Test
    public void testAppendBreak() throws Exception {
        Rnd random = new Rnd(System.nanoTime(), System.currentTimeMillis());
        JournalWriter<TestEntity> w = factory.writer(TestEntity.class);
        try {
            w.append(new TestEntity().setSym("ABC").setDStr("test1"));
            w.append(new TestEntity().setSym("ABC").setDStr(random.nextString(100)));
            w.append(new TestEntity().setSym("ABC").setDStr(random.nextString(70000)).setTimestamp(-1));
        } catch (Exception e) {
            // OK
        } finally {
            w.commit();
        }
        w.append(new TestEntity().setSym("ABC").setDStr(random.nextString(300)));

        Assert.assertEquals(3, w.query().all().withKeys("ABC").asResultSet().size());
        Assert.assertEquals(1, w.query().head().withKeys("ABC").asResultSet().size());
    }

    @Test
    public void testCreateNewReader() throws Exception {
        Assert.assertEquals(0, factory.reader(Quote.class, "brand-new").size());
        Assert.assertEquals(0, factory.bulkReader(Quote.class, "brand-new2").size());

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
    public void testMaxRowID() throws JournalException {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100);

        long maxRowID = w.getMaxRowID();
        int partitionID = Rows.toPartitionIndex(maxRowID);
        long localRowID = Rows.toLocalRowID(maxRowID);
        Assert.assertEquals(w.getLastPartition().getPartitionIndex(), partitionID);
        Assert.assertEquals(w.getLastPartition().size() - 1, localRowID);
        Assert.assertEquals(35184372088864L, maxRowID);
    }

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
    public void testMaxRowIDOnEmptyReader() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.close();

        Journal<Quote> r = factory.reader(Quote.class).select("sym");
        Assert.assertEquals(-1, r.getMaxRowID());
        Assert.assertNull(r.getLastPartition());
    }

    @Test
    public void testMaxRowIDOnReader() throws Exception {
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 1000, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
            w.commit();
        }

        Journal<Quote> r = factory.reader(Quote.class).select("sym");
        Assert.assertEquals(999, r.getMaxRowID());
    }

    @Test
    public void testOfflinePartition() throws Exception {
        int SIZE = 50000;
        File location;
        try (JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
            origin.commit();
            location = new File(origin.getLocation(), "2014-03");
        }

        Files.deleteOrException(location);

        try (JournalWriter<Quote> origin = factory.writer(Quote.class, "origin")) {
            Assert.assertEquals(25914, origin.size());
            TestUtils.generateQuoteData(origin, 3000, Dates.parseDateTime("2014-03-30T00:11:00Z"), 10000);
            Assert.assertEquals(28914, origin.size());
            origin.rollback();
            Assert.assertEquals(25914, origin.size());
        }
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

        JournalFactory f2 = new JournalFactory(new JournalConfigurationBuilder() {{
            $(Quote.class)
                    .$sym("mode");
        }}.build(factory.getConfiguration().getJournalBase()));

        try {
            f2.writer(new JournalKey<>(Quote.class, "quote"));
            Assert.fail("Exception expected");
        } catch (JournalException e) {
            // expect exception
        }
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

        Journal<Quote> r = factory.reader(Quote.class).select("sym", "bid");
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
    public void testRollbackToMiddle() throws Exception {

        JournalWriter<Quote> w = factory.writer(Quote.class);
        JournalWriter<Quote> w2 = factory.writer(Quote.class, "ctrl");
        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();
        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();
        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();

        long pin = w.getTxPin();

        w2.append(w);
        w2.commit();

        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();
        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();
        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();
        TestUtils.generateQuoteData(w, 1000, w.getMaxTimestamp());
        w.commit();

        w.rollback(3, pin);

        TestUtils.assertDataEquals(w2, w);

        Assert.assertEquals(3, w.getTxn());
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
        Assert.assertTrue(factory.writer(Quote.class, "test-Quote") != null);
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
    public void testTxLagTumbleDrier() throws Exception {
        int SIZE = 1000000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE / 12);
        JournalWriter<Quote> w = factory.writer(Quote.class, "q", SIZE / 12);

        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        ResultSet<Quote> originRs = origin.query().all().asResultSet();
        int blockSize = 5130;
        Rnd rnd = new Rnd(System.currentTimeMillis(), System.nanoTime());

        try {
            for (int i = 0; i < originRs.size(); ) {
                int d = Math.min(i + blockSize, originRs.size());
                w.mergeAppend(originRs.subset(i, d));

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
    public void testTxListener() throws Exception {
        int SIZE = 10000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        JournalWriter<Quote> w = factory.writer(Quote.class, "q");

        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), SIZE);
        origin.commit();

        TestTxListener lsnr = new TestTxListener();
        w.setTxListener(lsnr);


        w.append(origin.query().all().asResultSet().subset(0, 1000));
        w.commit();
        Assert.assertTrue(lsnr.isNotifyAsyncNoWait());
    }

    @Test
    public void testTxRefresh() throws Exception {
        int SIZE = 50000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE);
        w.append(origin);

        // check that refresh does not affect uncommitted changes
        w.refresh();
        TestUtils.assertEquals(origin, w);
    }

    @Test
    public void testTxRollbackLag() throws JournalException {
        int SIZE = 150000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.append(origin.query().all().asResultSet().subset(0, 100000));
        w.commit();
        w.mergeAppend(origin.query().all().asResultSet().subset(100000, 120000));
        w.commit();

        Journal<Quote> r = factory.reader(Quote.class);
        TestUtils.assertEquals(w, r);
        w.mergeAppend(origin.query().all().asResultSet().subset(120000, 150000));
        w.rollback();
        TestUtils.assertEquals(w, r);
        w.mergeAppend(origin.query().all().asResultSet().subset(120000, 150000));
        w.commit();

        r.refresh();
        TestUtils.assertEquals(w, r);
        TestUtils.assertDataEquals(origin, w);
    }

    @Test
    public void testTxRollbackMultiplePartitions() throws Exception {
        int SIZE = 50000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE);
        w.append(origin);
        w.commit();

        TestUtils.generateQuoteData(w, 50000, Dates.parseDateTime("2014-03-30T00:11:00Z"), 100000);

        Assert.assertEquals(100000, w.size());
        Assert.assertEquals(50000, origin.size());

        w.rollback();
        TestUtils.assertEquals(origin, w);
    }

    @Test
    public void testTxRollbackSamePartition() throws Exception {
        int SIZE = 50000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();
        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE);
        w.append(origin);
        w.commit();
        TestUtils.generateQuoteData(w, 20, Dates.parseDateTime("2014-03-30T00:11:00Z"), 100);
        Assert.assertEquals(50020, w.size());
        Assert.assertEquals(50000, origin.size());
        w.rollback();
        TestUtils.assertEquals(origin, w);
    }

    @Test
    public void testTxRollbackToEmpty() throws Exception {
        int SIZE = 100000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE);
        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote", SIZE)) {
            w.append(origin);
            w.rollback();

            Assert.assertEquals(0, w.size());
            Assert.assertEquals(1, w.getPartitionCount());
            Assert.assertEquals(0, w.getSymbolTable("sym").size());
            Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, w.getSymbolTable("sym").getQuick("LLOY.L"));
            Assert.assertNull(w.getLastPartition());
            Partition<Quote> p = w.getPartition(w.getPartitionCount() - 1, false);
            Assert.assertNotNull(p);
            Assert.assertEquals(0, p.getIndexForColumn("sym").size());
        }
    }

    @Test
    public void testTxTumbleDrier() throws Exception {
        int SIZE = 1000000;
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin", SIZE / 12);
        JournalWriter<Quote> w = factory.writer(Quote.class, "q", SIZE / 12);

        TestUtils.generateQuoteData(origin, SIZE, Dates.parseDateTime("2014-01-30T00:11:00Z"), 100000);
        origin.commit();

        ResultSet<Quote> originRs = origin.query().all().asResultSet();
        int blockSize = 5130;
        Rnd rnd = new Rnd(System.currentTimeMillis(), System.nanoTime());

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

    private static class TestTxListener implements TxListener {

        private boolean notifyAsyncNoWait = false;

        public boolean isNotifyAsyncNoWait() {
            return notifyAsyncNoWait;
        }

        @Override
        public void onCommit() {
            notifyAsyncNoWait = true;
        }

        @Override
        public void onError() {

        }

        public void reset() {
            notifyAsyncNoWait = false;
        }
    }
}
