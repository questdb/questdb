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

import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.iterators.*;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import gnu.trove.list.array.TIntArrayList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class IteratorTest extends AbstractTest {

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
    public void testEmptyPartitionFollowedByNonEmpty() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        w.getAppendPartition(Dates.toMillis("2012-01-10T10:00:00.000Z"));
        w.append(new Quote().setSym("TST").setTimestamp(Dates.toMillis("2012-02-10T10:00:00.000Z")));

        Assert.assertTrue(w.iterator().hasNext());
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
        TIntArrayList result = new TIntArrayList();
        for (int a : iterator) {
            result.add(a);
        }

        Assert.assertArrayEquals(expected, result.toArray());
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
    public void testPartitionParallelIterator() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 100000, Dates.interval("2014-01-01T00:00:00.000Z", "2014-02-10T00:00:00.000Z"));

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
        TestUtils.generateQuoteData(w, 100000, Dates.interval("2014-01-01T00:00:00.000Z", "2014-02-10T00:00:00.000Z"));

        Journal<Quote> r1 = factory.reader(Quote.class);
        Journal<Quote> r2 = factory.reader(Quote.class);

        try (ConcurrentIterator<Quote> expected = r1.concurrentIterator()) {
            try (ConcurrentIterator<Quote> actual = r2.query().all().concurrentIterator()) {
                TestUtils.assertEquals(expected, actual);
            }
        }
    }

    @Test
    public void testJournalRowIterator() throws Exception {
        JournalWriter<Quote> w = factory.writer(Quote.class);
        TestUtils.generateQuoteData(w, 1000);

        JournalIterator<Quote> expected = w.bufferedIterator();
        JournalRowBufferedIterator<Quote> actual = w.bufferedRowIterator();

        while (true) {
            boolean expectedHasNext = expected.hasNext();
            boolean actualHasNext = actual.hasNext();

            Assert.assertEquals(expectedHasNext, actualHasNext);

            if (!expectedHasNext) {
                break;
            }

            Quote e = expected.next();
            JournalRow<Quote> a = actual.next();

            Assert.assertEquals(e, a.getObject());
        }
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
}
