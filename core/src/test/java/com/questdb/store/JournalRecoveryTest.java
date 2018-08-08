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
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JournalRecoveryTest extends AbstractTest {

    @Test
    public void testLagRecovery() throws Exception {
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, 100000, new Interval("2013-01-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"));

            try (Journal<Quote> r = getFactory().reader(Quote.class, "origin")) {
                Assert.assertEquals(100000, r.size());
            }

            long ts;
            try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
                w.disableCommitOnClose();
                w.append(origin.query().all().asResultSet().subset(0, 15000));
                w.mergeAppend(origin.query().all().asResultSet().subset(15000, 17000));
                w.commit();
                ts = w.getMaxTimestamp();

                w.mergeAppend(origin.query().all().asResultSet().subset(16000, 27000));
                w.mergeAppend(origin.query().all().asResultSet().subset(23000, 37000));
                Assert.assertTrue(ts < w.getMaxTimestamp());
                Assert.assertEquals(37672, w.size());
            }

            // make sure journal is closed in pool
            getFactory().lock(Quote.class.getName());
            getFactory().unlock(Quote.class.getName());

            try (Journal<Quote> w = getFactory().reader(Quote.class)) {
                Assert.assertEquals(ts, w.getMaxTimestamp());
                Assert.assertEquals(17000, w.size());
            }

            try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
                Assert.assertEquals(ts, w.getMaxTimestamp());
                Assert.assertEquals(17000, w.size());
            }
        }
    }

    @Test
    public void testRecovery() throws Exception {
        long ts;
        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            w.disableCommitOnClose();
            Assert.assertFalse(w.isCommitOnClose());
            TestUtils.generateQuoteData(w, 10000, new Interval("2013-01-01T00:00:00.000Z", "2013-02-28T12:55:00.000Z"));
            ts = w.getMaxTimestamp();
            TestUtils.generateQuoteData(w, 10000, new Interval("2013-03-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"), false);
            Assert.assertTrue(w.getMaxTimestamp() > ts);
        }

        // make sure journal is closed in pool
        getFactory().lock(Quote.class.getName());
        getFactory().unlock(Quote.class.getName());

        try (Journal<Quote> w = getFactory().reader(Quote.class)) {
            Assert.assertEquals(ts, w.getMaxTimestamp());
            Assert.assertEquals(10000, w.size());
        }

        try (JournalWriter<Quote> w = getFactory().writer(Quote.class)) {
            w.disableCommitOnClose();
            Assert.assertEquals(ts, w.getMaxTimestamp());
            Assert.assertEquals(10000, w.size());
        }
    }
}
