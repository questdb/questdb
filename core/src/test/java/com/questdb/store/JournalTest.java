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

package com.questdb.store;

import com.questdb.common.PartitionBy;
import com.questdb.common.SymbolTable;
import com.questdb.model.Quote;
import com.questdb.model.TestEntity;
import com.questdb.std.Files;
import com.questdb.std.NumericException;
import com.questdb.std.Rnd;
import com.questdb.std.Rows;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.JournalConfigurationBuilder;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.store.query.ResultSet;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestData;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JournalTest extends AbstractTest {

    @Test(expected = JournalException.class)
    public void testAddPartitionOutOfOrder() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            w.getAppendPartition(DateFormatUtils.parseDateTime("2012-02-10T10:00:00.000Z"));
            w.getAppendPartition(DateFormatUtils.parseDateTime("2012-01-10T10:00:00.000Z"));
        }
    }

    @Test
    public void testAppendBreak() throws Exception {
        Rnd random = new Rnd(System.nanoTime(), System.currentTimeMillis());
        try (JournalWriter<TestEntity> w = getFactory().writer(TestEntity.class)) {
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
    }

    @Test
    public void testDecrementRowID() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 1000);

            ResultSet<Quote> rs = w.query().all().asResultSet();

            for (int i = 1; i < rs.size(); i++) {
                Assert.assertEquals(rs.getRowID(i - 1), w.decrementRowID(rs.getRowID(i)));
            }
        }
    }

    @Test
    public void testIncrementRowID() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 1000);

            ResultSet<Quote> rs = w.query().all().asResultSet();

            for (int i = 1; i < rs.size(); i++) {
                Assert.assertEquals(rs.getRowID(i), w.incrementRowID(rs.getRowID(i - 1)));
            }
        }
    }

    @Test
    public void testInvalidColumnName() {
        File base = getFactory().getConfiguration().getJournalBase();
        File dir = new File(base, "x");
        Assert.assertFalse(dir.exists());
        try {
            getFactory().writer(new JournalStructure("x").$sym("x").index().$sym("y").index().$sym("z\0is\0bad").index().$());
            Assert.fail();
        } catch (JournalException ignore) {
        }

        getFactory().expire();

        assertTrue(dir.exists());
        assertTrue(Files.delete(dir));
    }

    @Test
    public void testMaxRowID() throws JournalException, NumericException {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 100);

            long maxRowID = w.getMaxRowID();
            int partitionID = Rows.toPartitionIndex(maxRowID);
            long localRowID = Rows.toLocalRowID(maxRowID);
            Assert.assertEquals(w.getLastPartition().getPartitionIndex(), partitionID);
            Assert.assertEquals(w.getLastPartition().size() - 1, localRowID);
            Assert.assertEquals(35184372088864L, maxRowID);
        }
    }

    @Test
    public void testMaxRowIDBlankJournal() throws Exception {
        try (Journal<Quote> journal = getFactory().writer(Quote.class)) {
            Assert.assertEquals(-1, journal.getMaxRowID());
        }
    }

    @Test
    public void testMaxRowIDForJournalWithEmptyPartition() throws Exception {
        try (JournalWriter<Quote> journal = getFactory().writer(Quote.class)) {
            journal.getAppendPartition(System.currentTimeMillis());
            Assert.assertEquals(-1, journal.getMaxRowID());
        }
    }

    @Test
    public void testMaxRowIDOnEmptyReader() throws Exception {
        getFactory().writer(Quote.class).close();

        try (Journal<Quote> r = getFactory().reader(Quote.class).select("sym")) {
            Assert.assertEquals(-1, r.getMaxRowID());
            Assert.assertNull(r.getLastPartition());
        }
    }

    @Test
    public void testMaxRowIDOnReader() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 1000, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            w.commit();
        }

        try (Journal<Quote> r = getFactory().reader(Quote.class).select("sym")) {
            Assert.assertEquals(999, r.getMaxRowID());
        }
    }

    @Test
    public void testOfflinePartition() throws Exception {
        int SIZE = 50000;
        File location;
        final String name = "origin";
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, name, SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            origin.commit();
            location = new File(origin.getLocation(), "2014-03");
        }

        getFactory().lock(name);
        try {
            Files.deleteOrException(location);
        } finally {
            getFactory().unlock(name);
        }

        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, name)) {
            Assert.assertEquals(25914, origin.size());
            TestUtils.generateQuoteData(origin, 3000, DateFormatUtils.parseDateTime("2014-03-30T00:11:00.000Z"), 10000);
            Assert.assertEquals(28914, origin.size());
            origin.rollback();
            Assert.assertEquals(25914, origin.size());
        }
    }

    @Test
    public void testOpenJournalWithWrongPartitionType() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(new JournalKey<>(Quote.class, "quote", PartitionBy.NONE))) {
            TestUtils.generateQuoteData(w, 1000);
        }

        try {
            getFactory().writer(new JournalKey<>(Quote.class, "quote", PartitionBy.MONTH));
            Assert.fail("Exception expected");
        } catch (JournalException e) {
            // expect exception
        }

        try (Factory f2 = new Factory(new JournalConfigurationBuilder() {{
            $(Quote.class)
                    .$sym("mode");
        }}.build(factoryContainer.getConfiguration().getJournalBase()))) {
            f2.writer(new JournalKey<>(Quote.class, "quote"));
            Assert.fail("Exception expected");
        } catch (JournalException e) {
            // expect exception
        }
    }

    @Test
    public void testReadableColumns() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestData.appendQuoteData1(w);
        }

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

        try (Journal<Quote> r = getFactory().reader(Quote.class).select("sym", "bid")) {
            TestUtils.assertEquals(expected, r.query().all().asResultSet().subset(90, 100));
        }
    }

    @Test
    public void testReindex() throws JournalException, NumericException {
        File path;
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestData.appendQuoteData1(w);
            path = w.getLocation();
        }

        getFactory().lock(Quote.class.getName());
        try {
            Files.deleteOrException(new File(path, "2013-02/sym.r"));
            Files.deleteOrException(new File(path, "2013-02/sym.k"));
        } finally {
            getFactory().unlock(Quote.class.getName());
        }

        try (Journal<Quote> journal = getFactory().reader(Quote.class)) {
            try {
                journal.query().head().withKeys().asResultSet().read();
                Assert.fail("Expected exception here");
            } catch (JournalException e) {
                // do nothing
            }
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
                w.rebuildIndexes();
            }
            Assert.assertEquals(3, journal.query().head().withKeys().asResultSet().read().length);
        }
    }

    @Test
    public void testRollbackToMiddle() throws Exception {

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            try (JournalWriter<Quote> w2 = getFactory().writer(Quote.class, "ctrl")) {
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
        }
    }

    @Test
    public void testSingleWriterModel() throws Exception {
        try (JournalWriter<Quote> writer = getFactory().writer(Quote.class)) {
            assertNotNull(writer);

            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicInteger errors = new AtomicInteger();
            new Thread(() -> {
                try {
                    getFactory().writer(Quote.class);
                    errors.incrementAndGet();
                } catch (JournalException e) {
                    // ignore
                }
                finished.countDown();
            }).start();

            assertTrue(finished.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());

            // check if we can open a reader
            try (Journal<Quote> r = getFactory().reader(Quote.class)) {
                assertNotNull(r);
            }

            // check if we can open writer in alt location
            try (JournalWriter w = getFactory().writer(Quote.class, "test-Quote")) {
                assertNotNull(w);
            }
        }
    }

    @Test
    public void testSizeAfterCompaction() throws JournalException, NumericException {
        long sizeAfterCompaction;
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", 1000000)) {
            TestData.appendQuoteData2(w);
        }

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote")) {
            File f = new File(w.getLocation(), "2013-03/sym.d");
            long size = f.length();
            w.compact();
            sizeAfterCompaction = f.length();
            assertTrue(sizeAfterCompaction < size);
        }

        try (Journal<Quote> r = getFactory().reader(Quote.class, "quote")) {
            Assert.assertEquals(1000, r.query().all().size());
            File f = new File(r.getLocation(), "2013-03/sym.d");
            Assert.assertEquals(sizeAfterCompaction, f.length());
        }
    }

    @Test
    public void testTxLagTumbleDrier() throws Exception {
        int SIZE = 1000000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE / 12)) {
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q", SIZE / 12)) {

                TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
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
        }
    }

    @Test
    public void testTxListener() throws Exception {
        int SIZE = 10000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q")) {

                TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), SIZE);
                origin.commit();

                TestJournalListener lsnr = new TestJournalListener();
                w.setJournalListener(lsnr);
                w.append(origin.query().all().asResultSet().subset(0, 1000));
                w.commit();
                assertTrue(lsnr.isNotifyAsyncNoWait());
            }
        }
    }

    @Test
    public void testTxRefresh() throws Exception {
        int SIZE = 50000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            origin.commit();

            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", SIZE)) {
                w.append(origin);
                // check that refresh does not affect uncommitted changes
                w.refresh();
                TestUtils.assertEquals(origin, w);
            }
        }
    }

    @Test
    public void testTxRollbackLag() throws JournalException, NumericException {
        int SIZE = 150000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            origin.commit();

            try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
                w.append(origin.query().all().asResultSet().subset(0, 100000));
                w.commit();
                w.mergeAppend(origin.query().all().asResultSet().subset(100000, 120000));
                w.commit();

                try (Journal<Quote> r = getFactory().reader(Quote.class)) {
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
            }
        }
    }

    @Test
    public void testTxRollbackMultiplePartitions() throws Exception {
        int SIZE = 50000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            origin.commit();

            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", SIZE)) {
                w.append(origin);
                w.commit();

                TestUtils.generateQuoteData(w, 50000, DateFormatUtils.parseDateTime("2014-03-30T00:11:00.000Z"), 100000);

                Assert.assertEquals(100000, w.size());
                Assert.assertEquals(50000, origin.size());

                w.rollback();
                TestUtils.assertEquals(origin, w);
            }
        }
    }

    @Test
    public void testTxRollbackSamePartition() throws Exception {
        int SIZE = 50000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            origin.commit();
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", SIZE)) {
                w.append(origin);
                w.commit();
                TestUtils.generateQuoteData(w, 20, DateFormatUtils.parseDateTime("2014-03-30T00:11:00.000Z"), 100);
                Assert.assertEquals(50020, w.size());
                Assert.assertEquals(50000, origin.size());
                w.rollback();
                TestUtils.assertEquals(origin, w);
            }
        }
    }

    @Test
    public void testTxRollbackToEmpty() throws Exception {
        int SIZE = 100000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE)) {
            TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
            origin.commit();

            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "quote", SIZE)) {
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
    }

    @Test
    public void testTxTumbleDrier() throws Exception {
        int SIZE = 1000000;
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin", SIZE / 12)) {
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class, "q", SIZE / 12)) {

                TestUtils.generateQuoteData(origin, SIZE, DateFormatUtils.parseDateTime("2014-01-30T00:11:00.000Z"), 100000);
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
        }
    }

    private static class TestJournalListener implements JournalListener {

        private boolean notifyAsyncNoWait = false;

        public boolean isNotifyAsyncNoWait() {
            return notifyAsyncNoWait;
        }

        @Override
        public void onCommit() {
            notifyAsyncNoWait = true;
        }

        @Override
        public void onEvent(int event) {

        }

        public void reset() {
            notifyAsyncNoWait = false;
        }
    }
}
