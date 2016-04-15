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

package com.nfsdb.net.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.Partition;
import com.nfsdb.ex.JournalNetworkException;
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
