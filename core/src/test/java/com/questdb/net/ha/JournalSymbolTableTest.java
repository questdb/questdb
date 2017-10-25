/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.ex.JournalNetworkException;
import com.questdb.model.Quote;
import com.questdb.net.ha.comsumer.JournalClientStateConsumer;
import com.questdb.net.ha.comsumer.JournalSymbolTableConsumer;
import com.questdb.net.ha.model.IndexedJournal;
import com.questdb.net.ha.producer.JournalClientStateProducer;
import com.questdb.net.ha.producer.JournalSymbolTableProducer;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.After;
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
        origin = getFactory().writer(Quote.class, "origin");
        master = getFactory().writer(Quote.class, "master");
        slave = getFactory().writer(Quote.class, "slave");

        channel = new MockByteChannel();

        journalSymbolTableProducer = new JournalSymbolTableProducer(master);
        journalSymbolTableConsumer = new JournalSymbolTableConsumer(slave);

        origin.append(new Quote().setSym("AB").setEx("EX1").setMode("M1"));
        origin.append(new Quote().setSym("CD").setEx("EX2").setMode("M2"));
        origin.append(new Quote().setSym("EF").setEx("EX3").setMode("M2"));
        origin.append(new Quote().setSym("GH").setEx("EX3").setMode("M3"));
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
