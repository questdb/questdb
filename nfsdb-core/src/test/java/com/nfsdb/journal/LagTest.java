/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.AbstractTest;
import com.nfsdb.journal.test.tools.TestData;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

public class LagTest extends AbstractTest {

    private JournalWriter<Quote> rw;

    @Before
    public void setUp() throws Exception {
        rw = factory.writer(Quote.class);
    }

    @Test
    public void testOpenWithLag() throws JournalException {

        Partition<Quote> partition = rw.openOrCreateLagPartition();
        Quote v1 = new Quote().setSym("1").setTimestamp(Dates.parseDateTime("2012-06-11T00:00:00Z"));
        Quote v2 = new Quote().setSym("2").setTimestamp(Dates.parseDateTime("2012-06-11T10:00:00Z"));
        Quote v3 = new Quote().setSym("2").setTimestamp(Dates.parseDateTime("2012-06-11T06:00:00Z"));

        rw.append(v1);
        partition.append(v2);
        partition.append(v3);
        Assert.assertEquals(2, partition.size());

        Assert.assertEquals(2, rw.getPartitionCount());
        Assert.assertEquals(partition, rw.getPartition(1, true));
    }

    @Test
    public void testLagWorkflow() throws JournalException {

        Quote v1 = new Quote().setSym("1").setTimestamp(Dates.parseDateTime("2012-06-10T00:00:00Z"));
        Quote v2 = new Quote().setSym("2").setTimestamp(Dates.parseDateTime("2012-06-10T10:00:00Z"));
        Quote v3 = new Quote().setSym("2").setTimestamp(Dates.parseDateTime("2012-06-10T16:00:00Z"));
        Quote v4 = new Quote().setSym("3").setTimestamp(Dates.parseDateTime("2012-06-10T19:00:00Z"));
        Quote v5 = new Quote().setSym("4").setTimestamp(Dates.parseDateTime("2012-06-10T22:00:00Z"));

        rw.append(v1);

        Partition<Quote> p = rw.openOrCreateLagPartition();
        p.append(v2);
        p.append(v3);
        p.append(v4);
        p.append(v5);

        Quote v6 = new Quote().setSym("5").setTimestamp(Dates.parseDateTime("2012-06-11T08:00:00Z"));
        List<Quote> data = new ArrayList<>();
        data.add(v6);
        rw.mergeAppend(data);
        rw.commit();

        Assert.assertEquals(6, rw.size());
        Assert.assertEquals(5, rw.openOrCreateLagPartition().size());

        rw.close();

        rw = factory.writer(Quote.class);
        Assert.assertEquals(6, rw.size());
        Assert.assertEquals(5, rw.openOrCreateLagPartition().size());
        rw.purgeTempPartitions();
    }

    @Test
    public void testLagAppendScenarios() throws JournalException {

        // initial data
        List<Quote> data1 = new ArrayList<>();
        data1.add(new Quote().setSym("S1").setTimestamp(Dates.toMillis(2013, 1, 10, 10, 0)));
        data1.add(new Quote().setSym("S2").setTimestamp(Dates.toMillis(2013, 1, 10, 14, 0)));

        rw.mergeAppend(data1);
        rw.commit();

        Journal<Quote> reader = factory.reader(Quote.class);
        reader.query().all().asResultSet().read();

        // simple append scenario
        List<Quote> data2 = new ArrayList<>();
        data2.add(new Quote().setSym("S3").setTimestamp(Dates.toMillis(2013, 1, 10, 15, 0)));
        data2.add(new Quote().setSym("S4").setTimestamp(Dates.toMillis(2013, 1, 10, 16, 0)));
        rw.mergeAppend(data2);

        // simple append + lag split (30 days increment)
        List<Quote> data3 = new ArrayList<>();
        data3.add(new Quote().setSym("S8").setTimestamp(Dates.toMillis(2013, 2, 10, 15, 0)));
        data3.add(new Quote().setSym("S9").setTimestamp(Dates.toMillis(2013, 2, 10, 16, 0)));
        rw.mergeAppend(data3);

        // data on fully above lag
        List<Quote> data4 = new ArrayList<>();
        data4.add(new Quote().setSym("S6").setTimestamp(Dates.toMillis(2013, 2, 10, 10, 0)));
        data4.add(new Quote().setSym("S7").setTimestamp(Dates.toMillis(2013, 2, 10, 11, 0)));
        rw.mergeAppend(data4);

        // lag is fully inside data
        List<Quote> data5 = new ArrayList<>();
        data5.add(new Quote().setSym("S5").setTimestamp(Dates.toMillis(2013, 2, 10, 9, 0)));
        data5.add(new Quote().setSym("S10").setTimestamp(Dates.toMillis(2013, 2, 10, 17, 0)));
        rw.mergeAppend(data5);

        // lag and data have equal boundaries
        List<Quote> data6 = new ArrayList<>();
        data6.add(new Quote().setSym("near-S5").setTimestamp(Dates.toMillis(2013, 2, 10, 9, 0)));
        data6.add(new Quote().setSym("near-S10").setTimestamp(Dates.toMillis(2013, 2, 10, 17, 0)));
        rw.mergeAppend(data6);

        // bottom part of data overlaps top part of lag
        List<Quote> data7 = new ArrayList<>();
        data7.add(new Quote().setSym("after-S4").setTimestamp(Dates.toMillis(2013, 2, 9, 9, 0)));
        data7.add(new Quote().setSym("after-S9").setTimestamp(Dates.toMillis(2013, 2, 10, 16, 30)));
        rw.mergeAppend(data7);

        // top part of data overlaps bottom part of lag
        List<Quote> data8 = new ArrayList<>();
        data8.add(new Quote().setSym("after-S8").setTimestamp(Dates.toMillis(2013, 2, 10, 15, 30)));
        data8.add(new Quote().setSym("after-S10").setTimestamp(Dates.toMillis(2013, 2, 10, 18, 30)));
        rw.mergeAppend(data8);

        // data is fully inside of lag
        List<Quote> data9 = new ArrayList<>();
        data9.add(new Quote().setSym("after-S6").setTimestamp(Dates.toMillis(2013, 2, 10, 10, 30)));
        data9.add(new Quote().setSym("before-S10").setTimestamp(Dates.toMillis(2013, 2, 10, 16, 45)));
        rw.mergeAppend(data9);

        // full discard
        List<Quote> data10 = new ArrayList<>();
        data10.add(new Quote().setSym("discard-S1").setTimestamp(Dates.toMillis(2013, 1, 1, 10, 30)));
        data10.add(new Quote().setSym("discard-S2").setTimestamp(Dates.toMillis(2013, 1, 1, 16, 45)));
        rw.mergeAppend(data10);

        // full discard
        List<Quote> data11 = new ArrayList<>();
        data11.add(new Quote().setSym("discard-S3").setTimestamp(Dates.toMillis(2013, 1, 1, 10, 30)));
        data11.add(new Quote().setSym("before-S6").setTimestamp(Dates.toMillis(2013, 2, 10, 9, 45)));
        rw.mergeAppend(data11);

        String expected[] = {"S1", "S2", "S3", "S4", "after-S4", "S5", "near-S5", "before-S6", "S6", "after-S6", "S7"
                , "S8", "after-S8", "S9", "after-S9", "before-S10", "S10", "near-S10", "after-S10"};
        int i = 0;

        for (Quote p : rw) {
            Assert.assertEquals("Order incorrect", expected[i++], p.getSym());
        }

        rw.commit();

        Assert.assertEquals("Journal size mismatch", 19L, rw.size());
        Assert.assertEquals("Partition count mismatch", 3, rw.getPartitionCount());
        Assert.assertEquals("Lag size mismatch", 14L, rw.openOrCreateLagPartition().size());

        rw.close();

        reader.refresh();
        reader.query().all().asResultSet().read();


        String[] tempDirs = reader.getLocation().list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("temp") && !name.endsWith(".lock");
            }
        });

        Assert.assertEquals(2, tempDirs.length);
    }

    @Test
    public void testLagDelete() throws Exception {
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestData.appendQuoteData2(origin);

        rw.mergeAppend(origin.query().all().asResultSet().subset(0, 300));
        rw.commit();

        String lagName;
        try (Journal<Quote> r = factory.reader(Quote.class)) {
            Assert.assertEquals(300, r.size());
            lagName = r.getIrregularPartition().getName();
        }

        rw.mergeAppend(origin.query().all().asResultSet().subset(300, 400));
        rw.mergeAppend(origin.query().all().asResultSet().subset(400, 500));
        rw.mergeAppend(origin.query().all().asResultSet().subset(500, 600));

        try (Journal<Quote> r = factory.reader(Quote.class)) {
            Assert.assertEquals(300, r.size());
            Assert.assertEquals(lagName, r.getIrregularPartition().getName());
        }
    }
}
