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

import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

public class JournalRecoveryTest extends AbstractTest {

    @Test
    public void testRecovery() throws Exception {
        long ts;
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setCommitOnClose(false);
            Assert.assertFalse(w.isCommitOnClose());
            TestUtils.generateQuoteData(w, 10000, Dates.interval("2013-01-01T00:00:00.000Z", "2013-02-28T12:55:00.000Z"));
            ts = w.getMaxTimestamp();
            TestUtils.generateQuoteData(w, 10000, Dates.interval("2013-03-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"), false);
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

    @Test
    public void testLagRecovery() throws Exception {
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, 100000, Dates.interval("2013-01-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"));

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
}
