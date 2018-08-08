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

import com.questdb.model.TestEntity;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.query.OrderedResultSet;
import com.questdb.store.query.ResultSet;
import com.questdb.store.query.api.Query;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SortTest extends AbstractTest {

    private Query<TestEntity> q;
    private JournalWriter<TestEntity> w;

    @Before
    public void setUp() throws Exception {
        w = getFactory().writer(TestEntity.class);
        TestUtils.generateTestEntityData(w, 1000, DateFormatUtils.parseDateTime("2012-05-15T10:55:00.000Z"), 100000);
        q = w.query();
    }

    @After
    public void tearDown() {
        w.close();
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
    public void testMaxResultSetTimestamp() throws JournalException {
        OrderedResultSet<TestEntity> rs = q.all().asResultSet();
        Assert.assertEquals(rs.getMaxTimestamp(), rs.readLast().getTimestamp());
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
    public void testSortStrings() throws Exception {
        String last = "";
        for (TestEntity v : q.all().asResultSet().sort("bStr").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last.compareTo(v.getBStr()) <= 0);
            last = v.getBStr();
        }
    }

    @Test
    public void testSortSymbol() throws Exception {
        String last = "";
        for (TestEntity v : q.all().asResultSet().sort("sym").bufferedIterator()) {
            Assert.assertTrue("Journal records are out of order", last.compareTo(v.getSym() == null ? "" : v.getSym().toString()) <= 0);
            last = v.getSym() == null ? "" : v.getSym().toString();
        }
    }
}
