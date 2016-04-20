/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.net.ha;

import com.questdb.JournalWriter;
import com.questdb.ex.JournalNetworkException;
import com.questdb.model.Quote;
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
