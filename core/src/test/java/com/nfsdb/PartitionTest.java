/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Interval;
import com.nfsdb.model.Quote;
import com.nfsdb.store.BSearchType;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionTest extends AbstractTest {

    public <T> Partition<T> getPartitionForTimestamp(Journal<T> journal, long timestamp) throws JournalException {
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

    @Test
    public void testIndexOf() throws JournalException, NumericException {
        JournalWriter<Quote> journal = factory.writer(Quote.class);

        long ts1 = Dates.parseDateTime("2012-06-05T00:00:00.000");
        long ts2 = Dates.parseDateTime("2012-07-03T00:00:00.000");
        long ts3 = Dates.parseDateTime("2012-06-04T00:00:00.000");
        long ts4 = Dates.parseDateTime("2012-06-06T00:00:00.000");

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

        long tsA = Dates.parseDateTime("2012-06-15T00:00:00.000");

        Partition<Quote> partition1 = getPartitionForTimestamp(journal, tsA).open();
        Assert.assertNotNull("getPartition(timestamp) failed", partition1);

        Assert.assertEquals(-2, partition1.indexOf(tsA, BSearchType.NEWER_OR_SAME));
        Assert.assertEquals(-1, partition1.indexOf(Dates.parseDateTime("2012-06-03T00:00:00.000"), BSearchType.OLDER_OR_SAME));
        Assert.assertEquals(0, partition1.indexOf(Dates.parseDateTime("2012-06-03T00:00:00.000"), BSearchType.NEWER_OR_SAME));

        Assert.assertEquals(4, partition1.indexOf(ts1, BSearchType.OLDER_OR_SAME));
        Assert.assertEquals(1, partition1.indexOf(ts1, BSearchType.NEWER_OR_SAME));

        Partition<Quote> p = journal.openOrCreateLagPartition();
        long result = p.indexOf(Dates.parseDateTime("2012-06-15T00:00:00.000"), BSearchType.OLDER_OR_SAME);
        Assert.assertEquals(-1, result);
    }

}
