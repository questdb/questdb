/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb;


import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimestampTest extends AbstractTest {

    @Test
    public void testHardAndSoftTimestamps() throws JournalException, NumericException {
        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            // make sure initial timestamp is zero
            // also, prime cache with values if there is caching going on
            Assert.assertEquals(0, journal.getAppendTimestampLo());
            Assert.assertEquals(0, journal.getMaxTimestamp());

            Quote quote21 = new Quote().setSym("123").setTimestamp(Dates.parseDateTime("2011-09-10T10:00:00Z"));
            Quote quote22 = new Quote().setSym("345").setTimestamp(Dates.parseDateTime("2011-09-11T10:00:00Z"));
            journal.append(quote21, quote22);
            journal.commit();

            // make sure both hard and soft timestamps are the same
            // because we are not touching lag partition
            // and also both timestamps equal to max of two timestamps we inserted
            Assert.assertEquals(Dates.parseDateTime("2011-09-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());
        }

        List<Quote> data = new ArrayList<>();

        // open journal again and check that timestamps are ok
        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            Assert.assertEquals(Dates.parseDateTime("2011-09-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());

            // utc add some more data, which goes into new partition
            Quote quote23 = new Quote().setSym("333").setTimestamp(Dates.parseDateTime("2012-08-11T10:00:00Z"));
            journal.append(quote23);
            // make sure timestamps moved on
            Assert.assertEquals(Dates.parseDateTime("2012-08-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());


            // populate lag (lag is configured to 48 hours)
            Quote quote24 = new Quote().setSym("444").setTimestamp(Dates.parseDateTime("2012-08-11T15:00:00Z"));
            data.add(quote24);
            journal.mergeAppend(data);
            journal.commit();

            // check that hard timestamp hasn't changed
            Assert.assertEquals(Dates.parseDateTime("2012-08-11T10:00:00Z"), journal.getAppendTimestampLo());
            // check that soft timestamp has changed
            Assert.assertEquals(Dates.parseDateTime("2012-08-11T15:00:00Z"), journal.getMaxTimestamp());
        }

        // reopen journal and check timestamps
        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            Assert.assertEquals(Dates.parseDateTime("2012-08-11T10:00:00Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(Dates.parseDateTime("2012-08-11T15:00:00Z"), journal.getMaxTimestamp());

            // append timestamp that would make lag shift

            Quote quote25 = new Quote().setSym("555").setTimestamp(Dates.parseDateTime("2012-08-12T16:00:00Z"));
            data.clear();
            data.add(quote25);
            journal.mergeAppend(data);

            Assert.assertEquals("2012-08-11T10:00:00.000Z", Dates.toString(journal.getAppendTimestampLo()));
            Assert.assertEquals("2012-08-12T16:00:00.000Z", Dates.toString(journal.getMaxTimestamp()));

            // create new empty partition
            journal.getAppendPartition(Dates.parseDateTime("2013-08-12T16:00:00Z"));

            // check timestamps again
            Assert.assertEquals("2012-08-11T10:00:00.000Z", Dates.toString(journal.getAppendTimestampLo()));
            Assert.assertEquals("2012-08-12T16:00:00.000Z", Dates.toString(journal.getMaxTimestamp()));
        }
    }

    @Test
    public void testLagOnEmptyJournal() throws JournalException, NumericException {
        Quote quote21 = new Quote().setSym("123").setTimestamp(Dates.parseDateTime("2011-09-10T10:00:00Z"));
        Quote quote22 = new Quote().setSym("345").setTimestamp(Dates.parseDateTime("2011-09-11T10:00:00Z"));
        List<Quote> data = new ArrayList<>();
        data.add(quote21);
        data.add(quote22);

        try (JournalWriter<Quote> journal = factory.writer(Quote.class)) {
            journal.mergeAppend(data);
            journal.mergeAppend(data);
        }
        System.out.println("quit");
    }

}
