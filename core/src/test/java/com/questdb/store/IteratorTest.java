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

import com.questdb.model.Quote;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Interval;
import com.questdb.store.factory.WriterFactory;
import com.questdb.store.query.iter.*;
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

    private final Comparator<Quote> comparator = (o1, o2) -> {
        long x = o1.getTimestamp();
        long y = o2.getTimestamp();
        return Long.compare(x, y);
    };

    @Test
    @SuppressWarnings("unused")
    public void testBufferedIncrementIterator() throws Exception {
        getFactory().writer(Quote.class).close();
        try (Journal<Quote> r = getFactory().reader(Quote.class)) {
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
                try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
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
            }
        }
    }

    @Test
    public void testEmptyPartitionFollowedByNonEmpty() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            w.getAppendPartition(DateFormatUtils.parseDateTime("2012-01-10T10:00:00.000Z"));
            w.append(new Quote().setSym("TST").setTimestamp(DateFormatUtils.parseDateTime("2012-02-10T10:00:00.000Z")));
            Assert.assertTrue(w.iterator().hasNext());
        }
    }

    @Test
    @SuppressWarnings("unused")
    public void testIncrementIterator() throws Exception {
        // create empty
        getFactory().writer(Quote.class).close();
        try (Journal<Quote> r = getFactory().reader(Quote.class)) {
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
                try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
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
            }
        }
    }

    @Test
    public void testJournalIterator() throws JournalException, NumericException {

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 1000);

            try (Journal<Quote> r = getFactory().reader(Quote.class)) {

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
        }
    }

    @Test
    public void testJournalParallelIterator() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 100000);
            try (Journal<Quote> r = getFactory().reader(Quote.class)) {
                try (Journal<Quote> r2 = getFactory().reader(Quote.class)) {
                    try (ConcurrentIterator<Quote> it = JournalIterators.concurrentIterator(r)) {
                        TestUtils.assertEquals(JournalIterators.bufferedIterator(r2), it);
                    }
                }
            }
        }
    }

    @Test
    public void testMerge() throws Exception {
        populateQuotes();
        ObjList<Journal<Quote>> journals = new ObjList<Journal<Quote>>() {{
            add(getFactory().reader(Quote.class, "quote-0"));
            add(getFactory().reader(Quote.class, "quote-1"));
            add(getFactory().reader(Quote.class, "quote-2"));
            add(getFactory().reader(Quote.class, "quote-3"));
            add(getFactory().reader(Quote.class, "quote-4"));
        }};

        try {

            List<JournalIterator<Quote>> list = new ArrayList<>();
            for (int i = 0; i < journals.size(); i++) {
                list.add(JournalIterators.bufferedIterator(journals.get(i)));
            }

            long ts = 0;
            for (Quote q : MergingIterator.merge(list, comparator)) {
                Assert.assertTrue(ts <= q.getTimestamp());
                ts = q.getTimestamp();
            }
        } finally {
            for (int i = 0, n = journals.size(); i < n; i++) {
                journals.getQuick(i).close();
            }
        }
    }

    @Test
    public void testMergeAppend() throws Exception {
        populateQuotes();
        ObjList<Journal<Quote>> journals = new ObjList<Journal<Quote>>() {{
            add(getFactory().reader(Quote.class, "quote-0"));
            add(getFactory().reader(Quote.class, "quote-1"));
            add(getFactory().reader(Quote.class, "quote-2"));
            add(getFactory().reader(Quote.class, "quote-3"));
            add(getFactory().reader(Quote.class, "quote-4"));
        }};

        try {

            try (JournalWriter<Quote> writer = getFactory().writer(Quote.class, "quote-merge")) {
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
        } finally {
            for (int i = 0, n = journals.size(); i < n; i++) {
                journals.getQuick(i).close();
            }
        }
    }

    @Test
    public void testMergePeeking() throws Exception {
        populateQuotes();
        ObjList<Journal<Quote>> journals = new ObjList<Journal<Quote>>() {{
            add(getFactory().reader(Quote.class, "quote-0"));
            add(getFactory().reader(Quote.class, "quote-1"));
            add(getFactory().reader(Quote.class, "quote-2"));
            add(getFactory().reader(Quote.class, "quote-3"));
            add(getFactory().reader(Quote.class, "quote-4"));
        }};

        try {
            List<JournalPeekingIterator<Quote>> list = new ArrayList<>();
            for (int i = 0; i < journals.size(); i++) {
                list.add(JournalIterators.bufferedIterator(journals.get(i)));
            }

            long ts = 0;
            for (Quote q : MergingPeekingIterator.mergePeek(list, comparator)) {
                Assert.assertTrue(ts <= q.getTimestamp());
                ts = q.getTimestamp();
            }
        } finally {
            for (int i = 0, n = journals.size(); i < n; i++) {
                journals.getQuick(i).close();
            }
        }

    }

    @Test
    public void testMergingIterator() {
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

        MergingIterator<Integer> iterator = new MergingIterator<Integer>().$new(listA.iterator(), listB.iterator(), Integer::compareTo);

        int expected[] = {1, 1, 2, 2, 3, 4, 4, 4, 5, 6, 6, 7, 8, 9, 10, 11, 13, 14, 15, 17};
        int i = 0;
        for (int a : iterator) {
            Assert.assertEquals(expected[i++], a);
        }
    }

    @Test
    public void testResultSetParallelIterator() throws Exception {
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            TestUtils.generateQuoteData(w, 100000, new Interval("2014-01-01T00:00:00.000Z", "2014-02-10T00:00:00.000Z"));
            try (
                    Journal<Quote> r1 = getFactory().reader(Quote.class);
                    Journal<Quote> r2 = getFactory().reader(Quote.class);
                    ConcurrentIterator<Quote> expected = JournalIterators.concurrentIterator(r1);
                    ConcurrentIterator<Quote> actual = r2.query().all().concurrentIterator()) {

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
                service.submit(new Generator(getFactory(), cyclicBarrier, i, countDownLatch));
            }
            countDownLatch.await();
        } finally {
            service.shutdown();
        }
    }

    private static class Generator implements Runnable {
        private final WriterFactory factory;
        private final CyclicBarrier barrier;
        private final CountDownLatch latch;
        private final int index;

        private Generator(WriterFactory factory, CyclicBarrier barrier, int index, CountDownLatch latch) {
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
