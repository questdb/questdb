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
import com.questdb.ex.JournalNetworkException;
import com.questdb.model.RDFNode;
import com.questdb.net.ha.comsumer.JournalClientStateConsumer;
import com.questdb.net.ha.comsumer.JournalSymbolTableConsumer;
import com.questdb.net.ha.model.IndexedJournal;
import com.questdb.net.ha.producer.JournalClientStateProducer;
import com.questdb.net.ha.producer.JournalSymbolTableProducer;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LinkedJournalSymbolTableTest extends AbstractTest {

    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final JournalClientStateConsumer journalClientStateConsumer = new JournalClientStateConsumer();
    private JournalWriter<RDFNode> origin;
    private JournalWriter<RDFNode> master;
    private JournalWriter<RDFNode> slave;
    private MockByteChannel channel;
    private JournalSymbolTableProducer journalSymbolTableProducer;
    private JournalSymbolTableConsumer journalSymbolTableConsumer;

    @Before
    public void setUp() throws Exception {
        origin = factory.writer(RDFNode.class, "origin");
        master = factory.writer(RDFNode.class, "master");
        slave = factory.writer(RDFNode.class, "slave");

        channel = new MockByteChannel();

        journalSymbolTableProducer = new JournalSymbolTableProducer(master);
        journalSymbolTableConsumer = new JournalSymbolTableConsumer(slave);

        origin.append(new RDFNode().setObj("O1").setSubj("S1"));
        origin.append(new RDFNode().setObj("O2").setSubj("S1"));
        origin.append(new RDFNode().setObj("O3").setSubj("S2"));
        origin.append(new RDFNode().setObj("S2").setSubj("S1"));
    }

    @Test
    public void testSameAsSymbolTable() throws Exception {
        master.append(origin.query().all().asResultSet().subset(0, 2));
        master.commit(false, 101L, 10);

        master.append(origin.query().all().asResultSet().subset(2, 4));
        master.commit(false, 102L, 20);

        slave.append(origin.query().all().asResultSet().subset(0, 2));
        slave.commit(false, 101L, 10);
        executeSequence(true);
    }

    private void executeSequence(boolean expectContent) throws JournalNetworkException {
        journalClientStateProducer.write(channel, new IndexedJournal(0, slave));
        journalClientStateConsumer.read(channel);

//        journalSymbolTableProducer.configure(journalClientStateConsumer.getValue());
        journalSymbolTableProducer.configure(master.find(journalClientStateConsumer.getValue().getTxn(), journalClientStateConsumer.getValue().getTxPin()));

        Assert.assertEquals(expectContent, journalSymbolTableProducer.hasContent());
        if (expectContent) {
            journalSymbolTableProducer.write(channel);
            journalSymbolTableConsumer.read(channel);
            TestUtils.compareSymbolTables(master, slave);
        }
    }
}
