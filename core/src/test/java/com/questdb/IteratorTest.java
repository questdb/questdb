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

package com.questdb;

import com.questdb.ex.JournalException;
import com.questdb.ex.NumericException;
import com.questdb.factory.JournalFactory;
import com.questdb.iter.*;
import com.questdb.misc.Dates;
import com.questdb.misc.Interval;
import com.questdb.model.Quote;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IteratorTest extends AbstractTest {

    private final Comparator<Quote> comparator = new Comparator<Quote>() {
        @Override
        public int compare(Quote o1, Quote o2) {
            long x = o1.getTimestamp();
            long y = o2.getTimestamp();
            return (x < y) ? -1 : ((x == y) ? 0 : 1);
        }
    };

    @Test
    @SuppressWarnings("unused")
    public void testBufferedIncrementIterator() throws Exception {
        Journal<Quote> r = factory.reader(Quote.class);
        JournalWriter<Quote> w = factory.writer(Quote.class);
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, 10000);

        int count = 0;
        for (Quote q : JournalIterators.incrementBufferedIterator(r)) {
            count++;
        }

        Assert.assertEquals(0, count);
        w.append(origin.query().all().asResultSet().subset(0, 5000));
        w.commit();

        for (Quote q : JournalIterators.incrementBufferedIterator(r)) {
            count++;
        }
        Assert.assertEquals(5000, count);
        w.append(origin.query().all().asResultSet().subset(5000, 10000));
        w.commit();

        count = 0;
        for (Quote q : JournalIterators.incrementBufferedIterator(r)) {
            count++;
        }
        Assert.assertEquals(5000, count);
    }

    @Test
    public void testEmptyPartitionFollowedByNonEmpty() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.getAppendPartition(Dates.parseDateTime("2012-01-10T10:00:00.000Z"));
        w.append(new Quote().setSym("TST").setTimestamp(Dates.parseDateTime("2012-02-10T10:00:00.000Z")));

        Assert.assertTrue(w.iterator().hasNext());
    }

    @Test
    @SuppressWarnings("unused")
    public void testIncrementIterator() throws Exception {
        Journal<Quote> r = factory.reader(Quote.class);
        JournalWriter<Quote> w = factory.writer(Quote.class);
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, 10000);

        int count = 0;
        for (Quote q : JournalIterators.incrementIterator(r)) {
            count++;
        }

        Assert.assertEquals(0, count);
        w.append(origin.query().all().asResultSet().subset(0, 5000));
        w.commit();

        for (Quote q : JournalIterators.incrementIterator(r)) {
            count++;
        }
        Assert.assertEquals(5000, count);
        w.append(origin.query().all().asResultSet().subset(5000, 10000));
        w.commit();

        count = 0;
        for (Quote q : JournalIterators.incrementIterator(r)) {
            count++;
        }
        Assert.assertEquals(5000, count);
    }

    @Test
    public void testJournalIterator() throws JournalException, NumericException {

        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        Journal<Quote> r = factory.reader(Quote.class);

        List<Quote> posList = new ArrayList<>((int) r.size());
        for (Quote q : r) {
            posList.add(q);
        }

        int i = 0;
        for (Quote q : JournalIterators.bufferedIterator(r)) {
            Assert.assertEquals(q, posList.get(i++));
        }
        i = 0;
        for (Quote q : r.query().all().bufferedIterator()) {
            Assert.assertEquals(q, posList.get(i++));
        }
        i = 0;
        for (Quote q : r.getPartition(0, true).bufferedIterator()) {
            Assert.assertEquals(q, posList.get(i++));
        }
    }

    @Test
    public void testJournalParallelIterator() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000);
        Journal<Quote> r = factory.reader(Quote.class);
        Journal<Quote> r2 = factory.reader(Quote.class);
        try (ConcurrentIterator<Quote> it = JournalIterators.concurrentIterator(r)) {
            TestUtils.assertEquals(JournalIterators.bufferedIterator(r2), it);
        }
    }

    @Test
    public void testMerge() throws Exception {
        populateQuotes();
        List<Journal<Quote>> journals = new ArrayList<Journal<Quote>>() {{
            add(factory.reader(Quote.class, "quote-0"));
            add(factory.reader(Quote.class, "quote-1"));
            add(factory.reader(Quote.class, "quote-2"));
            add(factory.reader(Quote.class, "quote-3"));
            add(factory.reader(Quote.class, "quote-4"));
        }};

        List<JournalIterator<Quote>> list = new ArrayList<>();
        for (int i = 0; i < journals.size(); i++) {
            list.add(JournalIterators.bufferedIterator(journals.get(i)));
        }

        long ts = 0;
        for (Quote q : MergingIterator.merge(list, comparator)) {
            Assert.assertTrue(ts <= q.getTimestamp());
            ts = q.getTimestamp();
        }
    }

    @Test
    public void testMergeAppend() throws Exception {
        populateQuotes();
        List<Journal<Quote>> journals = new ArrayList<Journal<Quote>>() {{
            add(factory.reader(Quote.class, "quote-0"));
            add(factory.reader(Quote.class, "quote-1"));
            add(factory.reader(Quote.class, "quote-2"));
            add(factory.reader(Quote.class, "quote-3"));
            add(factory.reader(Quote.class, "quote-4"));
        }};

        JournalWriter<Quote> writer = factory.writer(Quote.class, "quote-merge");
        writer.mergeAppend(JournalIterators.bufferedIterator(journals.get(3)));
        writer.commit();


        List<JournalPeekingIterator<Quote>> list = new ArrayList<>();
        for (int i = 0; i < journals.size(); i++) {
            list.add(JournalIterators.bufferedIterator(journals.get(i)));
        }
        writer.mergeAppend(MergingPeekingIterator.mergePeek(list, comparator));
        writer.commit();
        Assert.assertEquals(60000, writer.size());
        TestUtils.assertOrder(JournalIterators.bufferedIterator(writer));
    }

    @Test
    public void testMergePeeking() throws Exception {
        populateQuotes();
        List<Journal<Quote>> journals = new ArrayList<Journal<Quote>>() {{
            add(factory.reader(Quote.class, "quote-0"));
            add(factory.reader(Quote.class, "quote-1"));
            add(factory.reader(Quote.class, "quote-2"));
            add(factory.reader(Quote.class, "quote-3"));
            add(factory.reader(Quote.class, "quote-4"));
        }};

        List<JournalPeekingIterator<Quote>> list = new ArrayList<>();
        for (int i = 0; i < journals.size(); i++) {
            list.add(JournalIterators.bufferedIterator(journals.get(i)));
        }

        long ts = 0;
        for (Quote q : MergingPeekingIterator.mergePeek(list, comparator)) {
            Assert.assertTrue(ts <= q.getTimestamp());
            ts = q.getTimestamp();
        }
    }

    @Test
    public void testMergingIterator() throws Exception {
        ArrayList<Integer> listA = new ArrayList<Integer>() {{
            add(1);
            add(3);
            add(4);
            add(4);
            add(5);
            add(7);
            add(9);
            add(11);
            add(13);
            add(14);
            add(15);
            add(17);
        }};

        ArrayList<Integer> listB = new ArrayList<Integer>() {{
            add(1);
            add(2);
            add(2);
            add(4);
            add(6);
            add(6);
            add(8);
            add(10);
        }};

        MergingIterator<Integer> iterator = new MergingIterator<Integer>().$new(listA.iterator(), listB.iterator(), new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });

        int expected[] = {1, 1, 2, 2, 3, 4, 4, 4, 5, 6, 6, 7, 8, 9, 10, 11, 13, 14, 15, 17};
        int i = 0;
        for (int a : iterator) {
            Assert.assertEquals(expected[i++], a);
        }
    }

    @Test
    public void testResultSetParallelIterator() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000, new Interval("2014-01-01T00:00:00.000Z", "2014-02-10T00:00:00.000Z"));

        Journal<Quote> r1 = factory.reader(Quote.class);
        Journal<Quote> r2 = factory.reader(Quote.class);

        try (ConcurrentIterator<Quote> expected = JournalIterators.concurrentIterator(r1)) {
            try (ConcurrentIterator<Quote> actual = r2.query().all().concurrentIterator()) {
                TestUtils.assertEquals(expected, actual);
            }
        }
    }

    private void populateQuotes() throws InterruptedException {
        int count = 5;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(count);
        CountDownLatch countDownLatch = new CountDownLatch(count);
        ExecutorService service = Executors.newCachedThreadPool();
        try {
            for (int i = 0; i < count; i++) {
                service.submit(new Generator(factory, cyclicBarrier, i, countDownLatch));
            }
            countDownLatch.await();
        } finally {
            service.shutdown();
        }
    }

    private static class Generator implements Runnable {
        private final JournalFactory factory;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final int index;

        private Generator(JournalFactory factory, CyclicBarrier barrier, int index, CountDownLatch latch) {
            this.factory = factory;
            this.barrier = barrier;
            this.index = index;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                try (JournalWriter<Quote> w = factory.writer(Quote.class, "quote-" + index)) {
                    barrier.await();

                    Quote p = new Quote();

                    long t = System.currentTimeMillis();
                    for (int i = 0; i < 10000; i++) {
                        p.setTimestamp(t + i);
                        p.setSym(String.valueOf(i % 20));
                        p.setAsk(i * 1.04598 + i);
                        w.append(p);
                    }
                    w.commit();
                    latch.countDown();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
