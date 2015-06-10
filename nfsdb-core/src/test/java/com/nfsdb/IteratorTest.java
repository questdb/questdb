/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.model.Quote;
import com.nfsdb.query.iterator.*;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Interval;
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
        for (Quote q : r.incrementBuffered()) {
            count++;
        }

        Assert.assertEquals(0, count);
        w.append(origin.query().all().asResultSet().subset(0, 5000));
        w.commit();

        for (Quote q : r.incrementBuffered()) {
            count++;
        }
        Assert.assertEquals(5000, count);
        w.append(origin.query().all().asResultSet().subset(5000, 10000));
        w.commit();

        count = 0;
        for (Quote q : r.incrementBuffered()) {
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
        for (Quote q : r.increment()) {
            count++;
        }

        Assert.assertEquals(0, count);
        w.append(origin.query().all().asResultSet().subset(0, 5000));
        w.commit();

        for (Quote q : r.increment()) {
            count++;
        }
        Assert.assertEquals(5000, count);
        w.append(origin.query().all().asResultSet().subset(5000, 10000));
        w.commit();

        count = 0;
        for (Quote q : r.increment()) {
            count++;
        }
        Assert.assertEquals(5000, count);
    }

    @Test
    public void testJournalIterator() throws JournalException {

        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        Journal<Quote> r = factory.reader(Quote.class);

        List<Quote> posList = new ArrayList<>((int) r.size());
        for (Quote q : r) {
            posList.add(q);
        }

        int i = 0;
        for (Quote q : r.bufferedIterator()) {
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
        try (ConcurrentIterator<Quote> it = r.concurrentIterator()) {
            TestUtils.assertEquals(r2.bufferedIterator(), it);
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
            list.add(journals.get(i).bufferedIterator());
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
        writer.mergeAppend(journals.get(3).bufferedIterator());
        writer.commit();


        List<JournalPeekingIterator<Quote>> list = new ArrayList<>();
        for (int i = 0; i < journals.size(); i++) {
            list.add(journals.get(i).bufferedIterator());
        }
        writer.mergeAppend(MergingPeekingIterator.mergePeek(list, comparator));
        writer.commit();
        Assert.assertEquals(60000, writer.size());
        TestUtils.assertOrder(writer.bufferedIterator());
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
            list.add(journals.get(i).bufferedIterator());
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
    public void testPartitionParallelIterator() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000, new Interval("2014-01-01T00:00:00.000Z", "2014-02-10T00:00:00.000Z"));

        Journal<Quote> r1 = factory.reader(Quote.class);
        Journal<Quote> r2 = factory.reader(Quote.class);

        Partition<Quote> p1 = r1.getPartition(0, true);
        Partition<Quote> p2 = r2.getPartition(0, true);

        try (ConcurrentIterator<Quote> it = p1.parallelIterator()) {
            TestUtils.assertEquals(p2.bufferedIterator(), it);
        }
    }

    @Test
    public void testResultSetParallelIterator() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000, new Interval("2014-01-01T00:00:00.000Z", "2014-02-10T00:00:00.000Z"));

        Journal<Quote> r1 = factory.reader(Quote.class);
        Journal<Quote> r2 = factory.reader(Quote.class);

        try (ConcurrentIterator<Quote> expected = r1.concurrentIterator()) {
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
