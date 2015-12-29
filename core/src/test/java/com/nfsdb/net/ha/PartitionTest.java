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

package com.nfsdb.net.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.Partition;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.misc.Dates;
import com.nfsdb.model.Quote;
import com.nfsdb.net.ha.comsumer.JournalClientStateConsumer;
import com.nfsdb.net.ha.comsumer.JournalSymbolTableConsumer;
import com.nfsdb.net.ha.comsumer.PartitionDeltaConsumer;
import com.nfsdb.net.ha.model.IndexedJournal;
import com.nfsdb.net.ha.producer.JournalClientStateProducer;
import com.nfsdb.net.ha.producer.JournalSymbolTableProducer;
import com.nfsdb.net.ha.producer.PartitionDeltaProducer;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartitionTest extends AbstractTest {

    private static final long timestamp = Dates.parseDateTimeQuiet("2013-12-12T00:00:00.000Z");
    private JournalWriter<Quote> origin;
    private JournalWriter<Quote> master;
    private JournalWriter<Quote> slave;

    private PartitionDeltaProducer producer;
    private PartitionDeltaConsumer consumer;
    private MockByteChannel channel;

    private Partition<Quote> masterPartition;
    private Partition<Quote> slavePartition;

    @Before
    public void setUp() throws Exception {
        origin = factory.writer(Quote.class, "origin");
        master = factory.writer(Quote.class, "master");
        slave = factory.writer(Quote.class, "slave");

        masterPartition = master.getAppendPartition(timestamp);
        slavePartition = slave.getAppendPartition(timestamp);

        producer = new PartitionDeltaProducer(masterPartition);
        consumer = new PartitionDeltaConsumer(slavePartition);
        channel = new MockByteChannel();

        TestUtils.generateQuoteData(origin, 1000, timestamp);
    }

    @Test
    public void testConsumerEqualToProducer() throws Exception {
        master.append(origin);
        slave.append(origin);

        Assert.assertEquals(1000, masterPartition.size());
        Assert.assertEquals(1000, slavePartition.size());

        producer.configure(slave.size());
        Assert.assertFalse(producer.hasContent());
    }

    @Test
    public void testConsumerLargerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 700));
        slave.append(origin);

        producer.configure(slave.size());
        Assert.assertFalse(producer.hasContent());
    }

    @Test
    public void testConsumerReset() throws Exception {
        master.append(origin);
        slave.append(origin.query().all().asResultSet().subset(0, 600));
        producer.configure(slave.size());
        Assert.assertTrue(producer.hasContent());

        syncSymbolTables();

        producer.write(channel);
        consumer.read(channel);
        comparePartitions();

        TestUtils.generateQuoteData(master, 200, Dates.parseDateTime("2014-01-01T00:00:00.000Z"));
        producer.configure(slave.size());
        producer.write(channel);
        consumer.read(channel);
        comparePartitions();
    }

    @Test
    public void testConsumerSmallerThanProducer() throws Exception {
        master.append(origin);
        slave.append(origin.query().all().asResultSet().subset(0, 700));

        Assert.assertEquals(1000, masterPartition.size());
        Assert.assertEquals(700, slavePartition.size());

        producer.configure(slave.size());

        Assert.assertTrue(producer.hasContent());
        producer.write(channel);
        consumer.read(channel);

        comparePartitions();
    }

    @Test
    public void testEmptyConsumerAndPopulatedProducer() throws Exception {
        master.append(origin);
        producer.configure(slave.size());
        Assert.assertTrue(producer.hasContent());

        syncSymbolTables();

        producer.write(channel);
        consumer.read(channel);
        comparePartitions();
    }

    @Test
    public void testEmptyConsumerAndProducer() throws Exception {
        producer.configure(slave.size());
        Assert.assertFalse(producer.hasContent());
    }

    private void comparePartitions() {
        Assert.assertEquals(masterPartition.size(), slavePartition.size());

        for (int i = 0; i < slavePartition.size(); i++) {
            Assert.assertEquals(masterPartition.read(i), slavePartition.read(i));
        }
    }

    private void syncSymbolTables() throws JournalNetworkException {

        JournalClientStateProducer sp = new JournalClientStateProducer();
        JournalClientStateConsumer sc = new JournalClientStateConsumer();

        sp.write(channel, new IndexedJournal(0, slave));
        sc.read(channel);

        JournalSymbolTableProducer p = new JournalSymbolTableProducer(master);
        JournalSymbolTableConsumer c = new JournalSymbolTableConsumer(slave);

        p.configure(master.find(sc.getValue().getTxn(), sc.getValue().getTxPin()));

        p.write(channel);
        c.read(channel);
    }
}
