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
import com.nfsdb.journal.test.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimestampTest extends AbstractTest {

    @Test
    public void testHardAndSoftTimestamps() throws JournalException {
        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            // make sure initial timestamp is zero
            // also, prime cache with values if there is caching going on
            Assert.assertEquals(0, journal.getAppendTimestampLo());
            Assert.assertEquals(0, journal.getMaxTimestamp());

            Quote Quote1 = new Quote().setSym("123").setTimestamp(Dates.toMillis("2011-09-10T10:00:00Z"));
            Quote Quote2 = new Quote().setSym("345").setTimestamp(Dates.toMillis("2011-09-11T10:00:00Z"));
            journal.append(Quote1, Quote2);
            journal.commit();

            // make sure both hard and soft timestamps are the same
            // because we are not touching lag partition
            // and also both timestamps equal to max of two timestamps we inserted
            Assert.assertEquals(Dates.toMillis("2011-09-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());
        }

        List<Quote> data = new ArrayList<>();

        // open journal again and check that timestamps are ok
        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            Assert.assertEquals(Dates.toMillis("2011-09-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());

            // utc add some more data, which goes into new partition
            Quote Quote3 = new Quote().setSym("333").setTimestamp(Dates.toMillis("2012-08-11T10:00:00Z"));
            journal.append(Quote3);
            // make sure timestamps moved on
            Assert.assertEquals(Dates.toMillis("2012-08-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());


            // populate lag (lag is configured to 48 hours)
            Quote Quote4 = new Quote().setSym("444").setTimestamp(Dates.toMillis("2012-08-11T15:00:00Z"));
            data.add(Quote4);
            journal.appendLag(data);
            journal.commit();

            // check that hard timestamp hasn't changed
            Assert.assertEquals(Dates.toMillis("2012-08-11T10:00:00Z"), journal.getAppendTimestampLo());
            // check that soft timestamp has changed
            Assert.assertEquals(Dates.toMillis("2012-08-11T15:00:00Z"), journal.getMaxTimestamp());
        }

        // reopen journal and check timestamps
        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            Assert.assertEquals(Dates.toMillis("2012-08-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(Dates.toMillis("2012-08-11T15:00:00Z"), journal.getMaxTimestamp());

            // append timestamp that would make lag shift

            Quote Quote5 = new Quote().setSym("555").setTimestamp(Dates.toMillis("2012-08-12T16:00:00Z"));
            data.clear();
            data.add(Quote5);
            journal.appendLag(data);

            Assert.assertEquals("2012-08-11T10:00:00.000Z", Dates.toString(journal.getAppendTimestampLo()));
            Assert.assertEquals("2012-08-12T16:00:00.000Z", Dates.toString(journal.getMaxTimestamp()));

            // create new empty partition
            journal.getAppendPartition(Dates.toMillis("2013-08-12T16:00:00Z"));

            // check timestamps again
            Assert.assertEquals("2012-08-11T10:00:00.000Z", Dates.toString(journal.getAppendTimestampLo()));
            Assert.assertEquals("2012-08-12T16:00:00.000Z", Dates.toString(journal.getMaxTimestamp()));
        }
    }

    @Test
    public void testLagOnEmptyJournal() throws JournalException {
        Quote Quote1 = new Quote().setSym("123").setTimestamp(Dates.toMillis("2011-09-10T10:00:00Z"));
        Quote Quote2 = new Quote().setSym("345").setTimestamp(Dates.toMillis("2011-09-11T10:00:00Z"));
        List<Quote> data = new ArrayList<>();
        data.add(Quote1);
        data.add(Quote2);

        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            journal.appendLag(data);
            journal.appendLag(data);
        }
    }

}
