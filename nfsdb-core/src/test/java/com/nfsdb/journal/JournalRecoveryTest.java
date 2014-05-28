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

import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class JournalRecoveryTest extends AbstractTest {

    @Test
    public void testRecovery() throws Exception {
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setAutoCommit(false);
            Assert.assertFalse(w.isAutoCommit());
            TestUtils.generateQuoteData(w, 10000, Dates.interval("2013-01-01T00:00:00.000Z", "2013-03-30T12:55:00.000Z"));
            Assert.assertEquals("2013-03-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
            TestUtils.generateQuoteData(w, 10000, Dates.interval("2013-03-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"), false);
            Assert.assertEquals("2013-05-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
        }

        try (Journal<Quote> w = factory.reader(Quote.class)) {
            Assert.assertEquals("2013-03-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
            Assert.assertEquals(9999, w.size());
        }

        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setAutoCommit(false);
            Assert.assertEquals("2013-03-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
            Assert.assertEquals(9999, w.size());
        }
    }

    @Test
    public void testLagRecovery() throws Exception {
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, 100000, Dates.interval("2013-01-01T00:00:00.000Z", "2013-05-30T12:55:00.000Z"));

        try (Journal<Quote> r = factory.reader(Quote.class, "origin")) {
            Assert.assertEquals(100000, r.size());
        }

        Random r = new Random(System.currentTimeMillis());
        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            w.setAutoCommit(false);
            w.append(origin.query().all().asResultSet().subset(0, 15000));
            w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(15000, 17000).shuffle(r).read()));
            w.commit();
            w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(17000, 27000).shuffle(r).read()));
            w.appendIrregular(Lists.asList(origin.query().all().asResultSet().subset(27000, 37000).shuffle(r).read()));
            Assert.assertEquals("2013-02-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
            Assert.assertEquals(37000, w.size());
        }

        try (Journal<Quote> w = factory.reader(Quote.class)) {
            Assert.assertEquals("2013-01-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
            Assert.assertEquals(17000, w.size());
        }

        try (JournalWriter<Quote> w = factory.writer(Quote.class)) {
            Assert.assertEquals("2013-01-01T00:00:00.000Z", Dates.toString(w.getMaxTimestamp()));
            Assert.assertEquals(17000, w.size());
        }
    }
}
