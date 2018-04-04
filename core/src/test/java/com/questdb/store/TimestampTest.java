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
import com.questdb.std.NumericException;
import com.questdb.std.ex.JournalException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Dates;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimestampTest extends AbstractTest {

    @Test
    public void testHardAndSoftTimestamps() throws JournalException, NumericException {
        try (JournalWriter<Quote> journal = getFactory().writer(Quote.class)) {
            // make sure initial timestamp is zero
            // also, prime cache with values if there is caching going on
            Assert.assertEquals(0, journal.getAppendTimestampLo());
            Assert.assertEquals(0, journal.getMaxTimestamp());

            Quote quote21 = new Quote().setSym("123").setTimestamp(DateFormatUtils.parseDateTime("2011-09-10T10:00:00.000Z"));
            Quote quote22 = new Quote().setSym("345").setTimestamp(DateFormatUtils.parseDateTime("2011-09-11T10:00:00.000Z"));
            journal.append(quote21, quote22);
            journal.commit();

            // make sure both hard and soft timestamps are the same
            // because we are not touching lag partition
            // and also both timestamps equal to max of two timestamps we inserted
            Assert.assertEquals(DateFormatUtils.parseDateTime("2011-09-11T10:00:00.000Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());
        }

        List<Quote> data = new ArrayList<>();

        // open journal again and check that timestamps are ok
        try (JournalWriter<Quote> journal = getFactory().writer(Quote.class)) {
            Assert.assertEquals(DateFormatUtils.parseDateTime("2011-09-11T10:00:00.000Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());

            // utc add some more data, which goes into new partition
            Quote quote23 = new Quote().setSym("333").setTimestamp(DateFormatUtils.parseDateTime("2012-08-11T10:00:00.000Z"));
            journal.append(quote23);
            // make sure timestamps moved on
            Assert.assertEquals(DateFormatUtils.parseDateTime("2012-08-11T10:00:00.000Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(journal.getMaxTimestamp(), journal.getAppendTimestampLo());


            // populate lag (lag is configured to 48 hours)
            Quote quote24 = new Quote().setSym("444").setTimestamp(DateFormatUtils.parseDateTime("2012-08-11T15:00:00.000Z"));
            data.add(quote24);
            journal.mergeAppend(data);
            journal.commit();

            // check that hard timestamp hasn't changed
            Assert.assertEquals(DateFormatUtils.parseDateTime("2012-08-11T10:00:00.000Z"), journal.getAppendTimestampLo());
            // check that soft timestamp has changed
            Assert.assertEquals(DateFormatUtils.parseDateTime("2012-08-11T15:00:00.000Z"), journal.getMaxTimestamp());
        }

        // reopen journal and check timestamps
        try (JournalWriter<Quote> journal = getFactory().writer(Quote.class)) {
            Assert.assertEquals(DateFormatUtils.parseDateTime("2012-08-11T10:00:00.000Z"), journal.getAppendTimestampLo());
            Assert.assertEquals(DateFormatUtils.parseDateTime("2012-08-11T15:00:00.000Z"), journal.getMaxTimestamp());

            // append timestamp that would make lag shift

            Quote quote25 = new Quote().setSym("555").setTimestamp(DateFormatUtils.parseDateTime("2012-08-12T16:00:00.000Z"));
            data.clear();
            data.add(quote25);
            journal.mergeAppend(data);

            Assert.assertEquals("2012-08-11T10:00:00.000Z", Dates.toString(journal.getAppendTimestampLo()));
            Assert.assertEquals("2012-08-12T16:00:00.000Z", Dates.toString(journal.getMaxTimestamp()));

            // create new empty partition
            journal.getAppendPartition(DateFormatUtils.parseDateTime("2013-08-12T16:00:00.000Z"));

            // check timestamps again
            Assert.assertEquals("2012-08-11T10:00:00.000Z", Dates.toString(journal.getAppendTimestampLo()));
            Assert.assertEquals("2012-08-12T16:00:00.000Z", Dates.toString(journal.getMaxTimestamp()));
        }
    }

    @Test
    public void testLagOnEmptyJournal() throws JournalException, NumericException {
        Quote quote21 = new Quote().setSym("123").setTimestamp(DateFormatUtils.parseDateTime("2011-09-10T10:00:00.000Z"));
        Quote quote22 = new Quote().setSym("345").setTimestamp(DateFormatUtils.parseDateTime("2011-09-11T10:00:00.000Z"));
        List<Quote> data = new ArrayList<>();
        data.add(quote21);
        data.add(quote22);

        try (JournalWriter<Quote> journal = getFactory().writer(Quote.class)) {
            journal.mergeAppend(data);
            journal.mergeAppend(data);
        }
    }

}
