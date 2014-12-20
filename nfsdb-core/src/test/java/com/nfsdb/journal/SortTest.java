/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.model.TestEntity;
import com.nfsdb.journal.query.api.Query;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SortTest extends AbstractTest {

    private Query<TestEntity> q;

    @Before
    public void setUp() throws Exception {
        JournalWriter<TestEntity> w = factory.writer(TestEntity.class);
        TestUtils.generateTestEntityData(w, 1000, Dates.parseDateTime("2012-05-15T10:55:00.000Z"), 100000);
        q = w.query();
    }

    @Test
    public void testSortDouble() throws JournalException {
        double last = -1;
        for (TestEntity p : q.all().asResultSet().sort("aDouble").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last <= p.getADouble());
            last = p.getADouble();
        }
    }

    @Test
    public void testSortDoubleDesc() throws JournalException {
        double last = Double.MAX_VALUE;
        for (TestEntity p : q.all().asResultSet().sort(ResultSet.Order.DESC, "aDouble").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last >= p.getADouble());
            last = p.getADouble();
        }
    }

    @Test
    public void testSortInt() throws JournalException {
        int last = -1;
        for (TestEntity p : q.all().asResultSet().sort("anInt").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last <= p.getAnInt());
            last = p.getAnInt();
        }
    }

    @Test(expected = JournalRuntimeException.class)
    public void testSortInvalidColumn() throws JournalException {
        q.all().asResultSet().sort("badColumn").read();
    }

    @Test
    public void testDefaultSort() throws JournalException {
        long last = 0;
        for (TestEntity p : q.all().asResultSet().sort("anInt").sort().bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last <= p.getTimestamp());
            last = p.getTimestamp();
        }
    }

    @Test
    public void testReadSortedTimestamps() throws JournalException {
        long[] result = q.all().asResultSet().sort("anInt").sort().readTimestamps();

        long last = 0;
        for (long t : result) {
            Assert.assertTrue("Journal records are out of order", last <= t);
            last = t;
        }
    }

    @Test
    public void testMaxResultSetTimestamp() throws JournalException {
        OrderedResultSet<TestEntity> rs = q.all().asResultSet();
        Assert.assertEquals(rs.getMaxTimestamp(), rs.readLast().getTimestamp());
    }

    @Test
    public void testSortSymbol() throws Exception {
        String last = "";
        for (TestEntity v : q.all().asResultSet().sort("sym").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last.compareTo(v.getSym() == null ? "" : v.getSym()) <= 0);
            last = v.getSym() == null ? "" : v.getSym();
        }
    }

    @Test
    public void testSortStrings() throws Exception {
        String last = "";
        for (TestEntity v : q.all().asResultSet().sort("bStr").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last.compareTo(v.getBStr()) <= 0);
            last = v.getBStr();
        }
    }
}
