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
import com.nfsdb.ex.NumericException;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Interval;
import com.nfsdb.model.Quote;
import com.nfsdb.store.BSearchType;
import com.nfsdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class PartitionTest extends AbstractTest {

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
