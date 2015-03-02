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

package com.nfsdb.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.ha.comsumer.JournalDeltaConsumer;
import com.nfsdb.ha.config.NetworkConfig;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.ha.model.Command;
import com.nfsdb.ha.model.IndexedJournal;
import com.nfsdb.ha.model.IndexedJournalKey;
import com.nfsdb.ha.producer.JournalClientStateProducer;
import com.nfsdb.ha.protocol.CommandConsumer;
import com.nfsdb.ha.protocol.CommandProducer;
import com.nfsdb.ha.protocol.commands.IntResponseConsumer;
import com.nfsdb.ha.protocol.commands.SetKeyRequestProducer;
import com.nfsdb.ha.protocol.commands.StringResponseConsumer;
import com.nfsdb.model.Quote;
import com.nfsdb.model.Trade;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

public class JournalServerAgentTest extends AbstractTest {

    private final CommandProducer commandProducer = new CommandProducer();
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final SetKeyRequestProducer setKeyRequestProducer = new SetKeyRequestProducer();
    private final StringResponseConsumer stringResponseConsumer = new StringResponseConsumer();
    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private MockByteChannel channel;
    private JournalWriter<Quote> quoteWriter;
    private JournalWriter<Trade> tradeWriter;
    private JournalServer server;
    private JournalServerAgent agent;

    @Before
    public void setUp() throws Exception {
        channel = new MockByteChannel();
        quoteWriter = factory.writer(Quote.class);
        tradeWriter = factory.writer(Trade.class);
        ServerConfig config = new ServerConfig() {{
            setHeartbeatFrequency(100);
            setEnableMultiCast(false);
        }};

        server = new JournalServer(config, factory);
        server.publish(quoteWriter);
        agent = new JournalServerAgent(server, new InetSocketAddress(NetworkConfig.DEFAULT_DATA_PORT), null);
    }

    @Test
    public void testIncrementalInteraction() throws Exception {
        JournalWriter<Quote> origin = factory.writer(Quote.class, "origin");
        TestUtils.generateQuoteData(origin, 200);

        server.start();
        JournalWriter<Quote> quoteClientWriter = factory.writer(Quote.class, "client");

        JournalDeltaConsumer quoteDeltaConsumer = new JournalDeltaConsumer(quoteClientWriter);

        // send quote journal key
        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getKey()));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("OK", stringResponseConsumer.getValue());

        // send quote state
        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(0, quoteClientWriter));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("OK", stringResponseConsumer.getValue());

        quoteWriter.append(origin.query().all().asResultSet().subset(0, 100));
        quoteWriter.commit();

        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        agent.process(channel);

        commandConsumer.reset();
        commandConsumer.read(channel);
        Assert.assertEquals(Command.JOURNAL_DELTA_CMD, commandConsumer.getValue());

        intResponseConsumer.reset();
        intResponseConsumer.read(channel);
        Assert.assertEquals(0, intResponseConsumer.getValue());
        quoteDeltaConsumer.reset();
        quoteDeltaConsumer.read(channel);
        Assert.assertTrue(quoteDeltaConsumer.isComplete());
        Assert.assertEquals(100, quoteClientWriter.size());

        commandConsumer.reset();
        commandConsumer.read(channel);
        Assert.assertEquals(Command.SERVER_READY_CMD, commandConsumer.getValue());

        quoteWriter.append(origin.query().all().asResultSet().subset(100, 200));
        quoteWriter.commit();

        // send quote state
        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(0, quoteClientWriter));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("OK", stringResponseConsumer.getValue());

        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        agent.process(channel);

        commandConsumer.reset();
        commandConsumer.read(channel);
        Assert.assertEquals(Command.JOURNAL_DELTA_CMD, commandConsumer.getValue());

        intResponseConsumer.reset();
        intResponseConsumer.read(channel);
        Assert.assertEquals(0, intResponseConsumer.getValue());
        quoteDeltaConsumer.reset();
        quoteDeltaConsumer.read(channel);
        Assert.assertTrue(quoteDeltaConsumer.isComplete());
        Assert.assertEquals(200, quoteClientWriter.size());

        commandConsumer.reset();
        commandConsumer.read(channel);
        Assert.assertEquals(Command.SERVER_READY_CMD, commandConsumer.getValue());

        server.halt();
    }

    @Test
    public void testJournalIndexCorrectness() throws Exception {
        server.publish(tradeWriter);
        server.start();

        Journal<Quote> quoteClientWriter = factory.writer(Quote.class, "client");

        // send quote journal key
//        commandProducer.write(channel, Command.SET_KEY_CMD);
//        setKeyRequestProducer.write(channel, new IndexedJournalKey(3, quoteWriter.getKey()));
//        agent.process(channel);
//        stringResponseConsumer.reset();
//        stringResponseConsumer.read(channel);
//        Assert.assertTrue(stringResponseConsumer.isComplete());
//        Assert.assertEquals("Journal index is too large. Max 1", stringResponseConsumer.getValue());


        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getKey()));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("OK", stringResponseConsumer.getValue());

        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(1, quoteClientWriter));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("Journal index does not match key request", stringResponseConsumer.getValue());

        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(0, quoteClientWriter));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("OK", stringResponseConsumer.getValue());

        server.halt();
    }

    @Test
    public void testSetKeyRequestResponse() throws Exception {
        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getKey()));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("OK", stringResponseConsumer.getValue());

        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, tradeWriter.getKey()));
        agent.process(channel);
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        Assert.assertTrue(stringResponseConsumer.isComplete());
        Assert.assertEquals("Requested key not exported: JournalKey{id=com.nfsdb.model.Trade, location='null', partitionType=DEFAULT, recordHint=0, ordered=true}", stringResponseConsumer.getValue());
    }
}
