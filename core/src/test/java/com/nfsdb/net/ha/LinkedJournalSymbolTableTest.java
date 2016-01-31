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

import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.model.RDFNode;
import com.nfsdb.net.ha.comsumer.JournalClientStateConsumer;
import com.nfsdb.net.ha.comsumer.JournalSymbolTableConsumer;
import com.nfsdb.net.ha.model.IndexedJournal;
import com.nfsdb.net.ha.producer.JournalClientStateProducer;
import com.nfsdb.net.ha.producer.JournalSymbolTableProducer;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
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
