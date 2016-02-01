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

package com.nfsdb.net.ha;

import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.TestUtils;
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
    public void testConsumerEqualToProducer() throws Exception {
        master.append(origin);
        master.commit(false, 101L, 10);
        slave.append(origin);
        slave.commit(false, 101L, 10);
        executeSequence(false);
    }

    @Test
    public void testConsumerLargerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 550));
        master.commit(false, 101L, 10);
        slave.append(origin);
        slave.commit(false, 101L, 10);
        executeSequence(false);
    }

    @Test
    public void testConsumerPartitionEdge() throws Exception {
        origin.truncate();

        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-10-01T00:00:00.000Z"));
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-11-01T00:00:00.000Z"));
        TestUtils.generateQuoteData(origin, 500, Dates.parseDateTime("2013-12-01T00:00:00.000Z"));

        master.append(origin.query().all().asResultSet().subset(0, 500));
        master.commit(false, 101L, 10);
        master.append(origin.query().all().asResultSet().subset(500, 1500));
        master.commit(false, 102L, 20);
        slave.append(origin.query().all().asResultSet().subset(0, 500));
        slave.commit(false, 101L, 10);
        Assert.assertEquals(1, slave.getPartitionCount());
        executeSequence(true);
    }

    @Test
    public void testConsumerReset() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 200));
        master.commit(false, 101L, 10);
        master.append(origin.query().all().asResultSet().subset(200, 550));
        master.commit(false, 102L, 20);
        slave.append(origin.query().all().asResultSet().subset(0, 200));
        slave.commit(false, 101L, 10);
        executeSequence(true);
        master.append(origin.query().all().asResultSet().subset(550, 1000));
        master.commit(false, 103L, 30);
        executeSequence(true);
    }

    @Test
    public void testConsumerSmallerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 655));
        master.commit(false, 101L, 10);
        master.append(origin.query().all().asResultSet().subset(655, (int) origin.size()));
        master.commit(false, 102L, 20);

        slave.append(origin.query().all().asResultSet().subset(0, 655));
        slave.commit(false, 101L, 10);
        Assert.assertEquals(655, slave.size());
        executeSequence(true);
    }

    @Test
    public void testEmptyConsumerAndPopulatedProducer() throws Exception {
        master.append(origin);
        master.commit();
        executeSequence(true);
        Assert.assertEquals(1000, slave.size());
    }

    @Test
    public void testEmptyConsumerAndProducer() throws Exception {
        executeSequence(false);
    }

    @Test
    public void testEmptyPartitionAdd() throws Exception {
        master.append(origin);
        master.getAppendPartition(Dates.parseDateTime("2013-12-01T00:00:00.000Z"));
        master.append(new Quote().setTimestamp(Dates.parseDateTime("2014-01-01T00:00:00.000Z")));
        master.commit();
        executeSequence(true);
        Assert.assertEquals(master.getPartitionCount(), slave.getPartitionCount());
    }

    @Test
    public void testLagConsumerSmallerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 350));
        master.mergeAppend(origin.query().all().asResultSet().subset(350, 600));
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testLagReplace() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 350));
        master.mergeAppend(origin.query().all().asResultSet().subset(350, 600));
        master.commit(false, 101L, 10);

        slave.append(origin.query().all().asResultSet().subset(0, 350));
        slave.mergeAppend(origin.query().all().asResultSet().subset(350, 600));
        slave.commit(false, 101L, 10);

        master.mergeAppend(origin.query().all().asResultSet().subset(600, 1000));
        master.commit(false, 102L, 20);
        executeSequence(true);
    }
}
