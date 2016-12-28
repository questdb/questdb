/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.JournalWriter;
import com.questdb.Partition;
import com.questdb.ex.JournalNetworkException;
import com.questdb.misc.Dates;
import com.questdb.model.Quote;
import com.questdb.net.ha.comsumer.JournalClientStateConsumer;
import com.questdb.net.ha.comsumer.JournalSymbolTableConsumer;
import com.questdb.net.ha.comsumer.PartitionDeltaConsumer;
import com.questdb.net.ha.model.IndexedJournal;
import com.questdb.net.ha.producer.JournalClientStateProducer;
import com.questdb.net.ha.producer.JournalSymbolTableProducer;
import com.questdb.net.ha.producer.PartitionDeltaProducer;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
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
        origin = getWriterFactory().writer(Quote.class, "origin");
        master = getWriterFactory().writer(Quote.class, "master");
        slave = getWriterFactory().writer(Quote.class, "slave");

        masterPartition = master.getAppendPartition(timestamp);
        slavePartition = slave.getAppendPartition(timestamp);

        producer = new PartitionDeltaProducer(masterPartition);
        consumer = new PartitionDeltaConsumer(slavePartition);
        channel = new MockByteChannel();

        TestUtils.generateQuoteData(origin, 1000, timestamp);
    }

    @After
    public void tearDown() throws Exception {
        origin.close();
        master.close();
        slave.close();
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
