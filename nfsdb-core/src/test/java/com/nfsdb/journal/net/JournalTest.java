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

package com.nfsdb.journal.net;

import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.test.tools.TestUtils;
import com.nfsdb.journal.utils.Dates;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JournalTest extends AbstractJournalTest {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtils.generateQuoteData(origin, 1000);
    }

    @Test
    public void testConsumerSmallerThanProducer() throws Exception {
        master.append(origin);
        master.commit();
        slave.append(origin.query().all().asResultSet().subset(0, 655));
        slave.commit();
        Assert.assertEquals(655, slave.size());
        executeSequence(true);
    }

    @Test
    public void testConsumerEqualToProducer() throws Exception {
        master.append(origin);
        master.commit();
        slave.append(origin);
        slave.commit();
        executeSequence(false);
    }

    @Test
    public void testEmptyConsumerAndProducer() throws Exception {
        executeSequence(false);
    }

    @Test
    public void testEmptyConsumerAndPopulatedProducer() throws Exception {
        master.append(origin);
        master.commit();
        executeSequence(true);
        Assert.assertEquals(1000, slave.size());
    }

    @Test
    public void testConsumerLargerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 550));
        master.commit();
        slave.append(origin);
        slave.commit();
        executeSequence(false);
    }

    @Test
    public void testConsumerPartitionEdge() throws Exception {
        origin.truncate();

        TestUtils.generateQuoteData(origin, 500, Dates.toMillis("2013-10-01T00:00:00.000Z"));
        TestUtils.generateQuoteData(origin, 500, Dates.toMillis("2013-11-01T00:00:00.000Z"));
        TestUtils.generateQuoteData(origin, 500, Dates.toMillis("2013-12-01T00:00:00.000Z"));

        master.append(origin);
        master.commit();
        slave.append(origin.query().all().asResultSet().subset(0, 500));
        slave.commit();
        Assert.assertEquals(1, slave.getPartitionCount());
        executeSequence(true);
    }

    @Test
    public void testConsumerReset() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 550));
        master.commit();
        slave.append(origin.query().all().asResultSet().subset(0, 200));
        slave.commit();
        executeSequence(true);
        master.append(origin.query().all().asResultSet().subset(550, 1000));
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testLagConsumerSmallerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 350));
        master.appendLag(origin.query().all().asResultSet().subset(350, 600));
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testLagReplace() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 350));
        master.appendLag(origin.query().all().asResultSet().subset(350, 600));
        master.commit();

        slave.append(origin.query().all().asResultSet().subset(0, 350));
        slave.appendLag(origin.query().all().asResultSet().subset(350, 600));
        slave.commit();

        master.appendLag(origin.query().all().asResultSet().subset(600, 1000));
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testEmptyPartitionAdd() throws Exception {
        master.append(origin);
        master.getAppendPartition(Dates.toMillis("2013-12-01T00:00:00.000Z"));
        master.append(new Quote().setTimestamp(Dates.toMillis("2014-01-01T00:00:00.000Z")));
        master.commit();
        executeSequence(true);
        Assert.assertEquals(master.getPartitionCount(), slave.getPartitionCount());
    }
}
