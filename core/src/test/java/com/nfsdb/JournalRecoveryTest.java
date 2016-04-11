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

import com.nfsdb.misc.Interval;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JournalRecoveryTest extends AbstractTest {

    @Test
    public void testLagRecovery() throws Exception {
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, 100000, new Interval("2013-01-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"));

        try (Journal<Quote> r = factory.reader(Quote.class, "origin")) {
            Assert.assertEquals(100000, r.size());
        }

        long ts;
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setCommitOnClose(false);
            w.append(origin.query().all().asResultSet().subset(0, 15000));
            w.mergeAppend(origin.query().all().asResultSet().subset(15000, 17000));
            w.commit();
            ts = w.getMaxTimestamp();

            w.mergeAppend(origin.query().all().asResultSet().subset(16000, 27000));
            w.mergeAppend(origin.query().all().asResultSet().subset(23000, 37000));
            Assert.assertTrue(ts < w.getMaxTimestamp());
            Assert.assertEquals(37672, w.size());
        }

        try (Journal<Quote> w = factory.reader(Quote.class)) {
            Assert.assertEquals(ts, w.getMaxTimestamp());
            Assert.assertEquals(17000, w.size());
        }

        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            Assert.assertEquals(ts, w.getMaxTimestamp());
            Assert.assertEquals(17000, w.size());
        }
    }

    @Test
    public void testRecovery() throws Exception {
        long ts;
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setCommitOnClose(false);
            Assert.assertFalse(w.isCommitOnClose());
            TestUtils.generateQuoteData(w, 10000, new Interval("2013-01-01T00:00:00.000Z", "2013-02-28T12:55:00.000Z"));
            ts = w.getMaxTimestamp();
            TestUtils.generateQuoteData(w, 10000, new Interval("2013-03-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"), false);
            Assert.assertTrue(w.getMaxTimestamp() > ts);
        }

        try (Journal<Quote> w = factory.reader(Quote.class)) {
            Assert.assertEquals(ts, w.getMaxTimestamp());
            Assert.assertEquals(10000, w.size());
        }

        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setCommitOnClose(false);
            Assert.assertEquals(ts, w.getMaxTimestamp());
            Assert.assertEquals(10000, w.size());
        }
    }
}
