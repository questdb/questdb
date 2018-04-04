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
import com.questdb.std.time.Interval;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionTest extends AbstractTest {

    @Test
    public void testIndexOf() throws JournalException, NumericException {
        try (JournalWriter<Quote> journal = getFactory().writer(Quote.class)) {

            long ts1 = DateFormatUtils.parseDateTime("2012-06-05T00:00:00.000Z");
            long ts2 = DateFormatUtils.parseDateTime("2012-07-03T00:00:00.000Z");
            long ts3 = DateFormatUtils.parseDateTime("2012-06-04T00:00:00.000Z");
            long ts4 = DateFormatUtils.parseDateTime("2012-06-06T00:00:00.000Z");

            Quote q9 = new Quote().setSym("S5").setTimestamp(ts3);
            Quote q10 = new Quote().setSym("S5").setTimestamp(ts4);

            Quote q1 = new Quote().setSym("S1").setTimestamp(ts1);
            Quote q2 = new Quote().setSym("S2").setTimestamp(ts1);
            Quote q3 = new Quote().setSym("S3").setTimestamp(ts1);
            Quote q4 = new Quote().setSym("S4").setTimestamp(ts1);

            Quote q5 = new Quote().setSym("S1").setTimestamp(ts2);
            Quote q6 = new Quote().setSym("S2").setTimestamp(ts2);
            Quote q7 = new Quote().setSym("S3").setTimestamp(ts2);
            Quote q8 = new Quote().setSym("S4").setTimestamp(ts2);

            journal.append(q9);
            journal.append(q1);
            journal.append(q2);
            journal.append(q3);
            journal.append(q4);
            journal.append(q10);

            journal.append(q5);
            journal.append(q6);
            journal.append(q7);
            journal.append(q8);

            Assert.assertEquals(2, journal.getPartitionCount());

            long tsA = DateFormatUtils.parseDateTime("2012-06-15T00:00:00.000Z");

            Partition<Quote> partition1 = getPartitionForTimestamp(journal, tsA).open();
            Assert.assertNotNull("getPartition(timestamp) failed", partition1);

            Assert.assertEquals(-2, partition1.indexOf(tsA, BSearchType.NEWER_OR_SAME));
            Assert.assertEquals(-1, partition1.indexOf(DateFormatUtils.parseDateTime("2012-06-03T00:00:00.000Z"), BSearchType.OLDER_OR_SAME));
            Assert.assertEquals(0, partition1.indexOf(DateFormatUtils.parseDateTime("2012-06-03T00:00:00.000Z"), BSearchType.NEWER_OR_SAME));

            Assert.assertEquals(4, partition1.indexOf(ts1, BSearchType.OLDER_OR_SAME));
            Assert.assertEquals(1, partition1.indexOf(ts1, BSearchType.NEWER_OR_SAME));

            Partition<Quote> p = journal.openOrCreateLagPartition();
            long result = p.indexOf(DateFormatUtils.parseDateTime("2012-06-15T00:00:00.000Z"), BSearchType.OLDER_OR_SAME);
            Assert.assertEquals(-1, result);
        }
    }

    private <T> Partition<T> getPartitionForTimestamp(Journal<T> journal, long timestamp) throws JournalException {
        int sz = journal.getPartitionCount();
        for (int i = 0; i < sz; i++) {
            Partition<T> result = journal.getPartition(i, false);
            Interval interval = result.getInterval();
            if (interval == null || interval.contains(timestamp)) {
                return result;
            }
        }

        if (journal.getPartition(0, false).getInterval().isAfter(timestamp)) {
            return journal.getPartition(0, false);
        } else {
            return journal.getPartition(sz - 1, false);
        }
    }

}
