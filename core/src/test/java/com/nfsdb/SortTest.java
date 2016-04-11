/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 *
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.TestEntity;
import com.nfsdb.query.OrderedResultSet;
import com.nfsdb.query.ResultSet;
import com.nfsdb.query.api.Query;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
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
            Assert.assertTrue("Journal records are out of order", last.compareTo(v.getSym() == null ? "" : v.getSym()) <= 0);
            last = v.getSym() == null ? "" : v.getSym();
        }
    }
}
