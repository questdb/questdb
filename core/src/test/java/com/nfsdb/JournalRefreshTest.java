/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.query.ResultSet;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JournalRefreshTest extends AbstractTest {
    private JournalWriter<Quote> rw;

    @Before
    public void before() throws JournalException {
        rw = factory.writer(Quote.class);
    }

    @Test
    public void testIllegalArgExceptionInStorage() throws JournalException {
        rw.append(new Quote().setMode("A").setSym("B").setEx("E1").setAsk(10).setAskSize(1000).setBid(9).setBidSize(900).setTimestamp(System.currentTimeMillis()));
        rw.compact();
        rw.commit();

        Journal<Quote> reader = factory.reader(Quote.class);
        reader.query().all().asResultSet().read();
        rw.close();

        JournalWriter<Quote> writer = factory.writer(Quote.class);
        writer.append(new Quote().setMode("A").setSym("B").setEx("E1").setAsk(10).setAskSize(1000).setBid(9).setBidSize(900).setTimestamp(System.currentTimeMillis()));

        Quote expected = new Quote().setMode("A").setSym("B22").setEx("E1").setAsk(10).setAskSize(1000).setBid(9).setBidSize(900).setTimestamp(System.currentTimeMillis());
        writer.append(expected);
        writer.commit();

        reader.refresh();
        ResultSet<Quote> rs = reader.query().all().asResultSet();
        // at this point we used to get an IllegalArgumentException because we
        // were reaching outside of buffer of compacted column
        Quote q = rs.read(rs.size() - 1);
        Assert.assertEquals(expected, q);
    }

    @Test
    public void testLagDetach() throws Exception {
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        Journal<Quote> reader = factory.reader(Quote.class);

        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2014-02-10T02:00:00.000Z"));
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2014-02-10T10:00:00.000Z"));

        rw.append(origin.query().all().asResultSet().subset(0, 500));
        rw.commit();
        reader.refresh();
        Assert.assertEquals(rw.size(), reader.size());

        rw.append(origin.query().all().asResultSet().subset(500, 600));
        rw.commit();
        reader.refresh();
        Assert.assertEquals(rw.size(), reader.size());

        rw.mergeAppend(origin.query().all().asResultSet().subset(500, 600));
        rw.commit();
        reader.refresh();
        Assert.assertEquals(rw.size(), reader.size());

        rw.removeIrregularPartition();
        rw.commit();
        reader.refresh();
        Assert.assertEquals(rw.size(), reader.size());
    }

    @Test
    public void testPartitionRescan() throws Exception {
        Journal<Quote> reader = factory.reader(Quote.class);

        Assert.assertEquals(0, reader.size());
        TestUtils.generateQuoteData(rw, 1001);
        reader.refresh();
        Assert.assertEquals(1001, reader.size());

        TestUtils.generateQuoteData(rw, 302, Dates.parseDateTime("2014-02-10T10:00:00.000Z"));
        reader.refresh();
        Assert.assertEquals(1001, reader.size());

        rw.commit();
        reader.refresh();
        Assert.assertEquals(1303, reader.size());
    }

    @Test
    public void testReadConsistency() throws JournalException {

        Quote q1 = new Quote().setSym("ABC").setEx("LN");
        Quote q2 = new Quote().setSym("EFG").setEx("SK");

        rw.append(q1);
        rw.close();

        rw = factory.writer(Quote.class);

        Journal<Quote> r = factory.reader(Quote.class);

        for (Quote v : r) {
            Assert.assertEquals(q1, v);
        }

        rw.append(q2);

        for (Quote v : r) {
            Assert.assertEquals(q1, v);
        }
    }

    @Test
    public void testRefreshScenarios() throws JournalException {
        // initial data
        rw.append(new Quote().setSym("IMO-1").setTimestamp(Dates.toMillis(2013, 1, 10, 10, 0)));
        rw.append(new Quote().setSym("IMO-2").setTimestamp(Dates.toMillis(2013, 1, 10, 14, 0)));
        rw.commit();

        Journal<Quote> r = factory.reader(Quote.class);
        Assert.assertEquals(2, r.size());

        // append data to same partition
        rw.append(new Quote().setSym("IMO-1").setTimestamp(Dates.toMillis(2013, 1, 10, 15, 0)));
        rw.append(new Quote().setSym("IMO-2").setTimestamp(Dates.toMillis(2013, 1, 10, 16, 0)));
        rw.commit();

        // check that size didn't change before we call refresh
        Assert.assertEquals(2, r.size());
        // check that we see two more rows after refresh

        r.refresh();
        Assert.assertEquals(4, r.size());

        // append data to new partition
        rw.append(new Quote().setSym("IMO-3").setTimestamp(Dates.toMillis(2013, 2, 10, 15, 0)));
        rw.append(new Quote().setSym("IMO-4").setTimestamp(Dates.toMillis(2013, 2, 10, 16, 0)));

        // check that size didn't change before we call refresh
        Assert.assertEquals(4, r.size());
        // check that we don't see rows even if we refresh
        r.refresh();
        Assert.assertEquals(4, r.size());

        rw.commit();
        // check that we see two more rows after refresh
        r.refresh();
        Assert.assertEquals(6, r.size());

        List<Quote> data = new ArrayList<>();
        data.add(new Quote().setSym("IMO-5").setTimestamp(Dates.toMillis(2013, 3, 10, 15, 0)));
        data.add(new Quote().setSym("IMO-6").setTimestamp(Dates.toMillis(2013, 3, 10, 16, 0)));
        rw.mergeAppend(data);

        rw.commit();

        // check that size didn't change before we call refresh
        Assert.assertEquals(6, r.size());
        // check that we see two more rows after refresh
        r.refresh();
        Assert.assertEquals(8, r.size());
    }

    @Test
    public void testTruncateRefresh() throws Exception {
        TestUtils.generateQuoteData(rw, 1000, Dates.parseDateTime("2013-09-04T10:00:00.000Z"));
        rw.commit();

        Journal<Quote> r = factory.reader(Quote.class);

        Assert.assertEquals(10, r.getSymbolTable("sym").size());
        r.getSymbolTable("sym").preLoad();

        rw.truncate();

        Assert.assertTrue(r.refresh());
        Assert.assertEquals(0, r.size());
        Assert.assertEquals(0, r.getSymbolTable("sym").size());
    }

}
