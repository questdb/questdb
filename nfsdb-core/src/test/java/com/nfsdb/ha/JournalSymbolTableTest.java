/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ha;

import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.comsumer.JournalClientStateConsumer;
import com.nfsdb.ha.comsumer.JournalSymbolTableConsumer;
import com.nfsdb.ha.model.IndexedJournal;
import com.nfsdb.ha.producer.JournalClientStateProducer;
import com.nfsdb.ha.producer.JournalSymbolTableProducer;
import com.nfsdb.model.Quote;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JournalSymbolTableTest extends AbstractTest {

    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final JournalClientStateConsumer journalClientStateConsumer = new JournalClientStateConsumer();
    private JournalWriter<Quote> origin;
    private JournalWriter<Quote> master;
    private JournalWriter<Quote> slave;
    private MockByteChannel channel;
    private JournalSymbolTableProducer journalSymbolTableProducer;
    private JournalSymbolTableConsumer journalSymbolTableConsumer;

    @Before
    public void setUp() throws Exception {
        origin = factory.writer(Quote.class, "origin");
        master = factory.writer(Quote.class, "master");
        slave = factory.writer(Quote.class, "slave");

        channel = new MockByteChannel();

        journalSymbolTableProducer = new JournalSymbolTableProducer(master);
        journalSymbolTableConsumer = new JournalSymbolTableConsumer(slave);

        origin.append(new Quote().setSym("AB").setEx("EX1").setMode("M1"));
        origin.append(new Quote().setSym("CD").setEx("EX2").setMode("M2"));
        origin.append(new Quote().setSym("EF").setEx("EX3").setMode("M2"));
        origin.append(new Quote().setSym("GH").setEx("EX3").setMode("M3"));

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
        slave.append(origin);
        slave.commit(false, 101L, 10);
        master.append(origin.query().all().asResultSet().subset(0, 3));
        master.commit(false, 101L, 10);
        executeSequence(false);
    }

    @Test
    public void testConsumerSmallerThanProducer() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 2));
        master.commit(false, 101L, 10);
        master.append(origin.query().all().asResultSet().subset(2, 4));
        master.commit(false, 102L, 20);

        slave.append(origin.query().all().asResultSet().subset(0, 2));
        slave.commit(false, 101L, 10);
        executeSequence(true);
    }

    @Test
    public void testEmptyConsumerAndPopulatedProducer() throws Exception {
        master.append(origin);
        master.commit();
        executeSequence(true);
    }

    @Test
    public void testEmptyConsumerAndProducer() throws Exception {
        executeSequence(false);
    }

    private void executeSequence(boolean expectContent) throws JournalNetworkException {
        journalClientStateProducer.write(channel, new IndexedJournal(0, slave));
        journalClientStateConsumer.read(channel);

        journalSymbolTableProducer.configure(master.find(journalClientStateConsumer.getValue().getTxn(), journalClientStateConsumer.getValue().getTxPin()));
        Assert.assertEquals(expectContent, journalSymbolTableProducer.hasContent());
        if (expectContent) {
            journalSymbolTableProducer.write(channel);
            journalSymbolTableConsumer.read(channel);
            TestUtils.compareSymbolTables(master, slave);
        }
    }
}
