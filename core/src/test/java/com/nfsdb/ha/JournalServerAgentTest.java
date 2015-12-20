/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ha;

import com.nfsdb.Journal;
import com.nfsdb.JournalWriter;
import com.nfsdb.ha.comsumer.HugeBufferConsumer;
import com.nfsdb.ha.comsumer.JournalDeltaConsumer;
import com.nfsdb.ha.config.NetworkConfig;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.ha.model.Command;
import com.nfsdb.ha.model.IndexedJournal;
import com.nfsdb.ha.model.IndexedJournalKey;
import com.nfsdb.ha.producer.JournalClientStateProducer;
import com.nfsdb.ha.protocol.CommandConsumer;
import com.nfsdb.ha.protocol.CommandProducer;
import com.nfsdb.ha.protocol.commands.CharSequenceResponseConsumer;
import com.nfsdb.ha.protocol.commands.IntResponseConsumer;
import com.nfsdb.ha.protocol.commands.SetKeyRequestProducer;
import com.nfsdb.model.Quote;
import com.nfsdb.model.Trade;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;

public class JournalServerAgentTest extends AbstractTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private final CommandProducer commandProducer = new CommandProducer();
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final SetKeyRequestProducer setKeyRequestProducer = new SetKeyRequestProducer();
    private final CharSequenceResponseConsumer charSequenceResponseConsumer = new CharSequenceResponseConsumer();
    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private MockByteChannel channel;
    private JournalWriter<Quote> quoteWriter;
    private JournalWriter<Trade> tradeWriter;
    private JournalServer server;
    private JournalServerAgent agent;
    private HugeBufferConsumer hugeBufferConsumer;

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
        hugeBufferConsumer = new HugeBufferConsumer(temp.newFile());
    }

    @After
    public void tearDown() {
        agent.close();
        hugeBufferConsumer.free();
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
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());
        hugeBufferConsumer.read(channel);

        // send quote state
        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(0, quoteClientWriter));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());

        quoteWriter.append(origin.query().all().asResultSet().subset(0, 100));
        quoteWriter.commit();

        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        agent.process(channel);

        commandConsumer.read(channel);
        Assert.assertEquals(Command.JOURNAL_DELTA_CMD, commandConsumer.getValue());

        Assert.assertEquals(0, intResponseConsumer.getValue(channel));
        quoteDeltaConsumer.read(channel);
        Assert.assertEquals(100, quoteClientWriter.size());

        commandConsumer.read(channel);
        Assert.assertEquals(Command.SERVER_READY_CMD, commandConsumer.getValue());

        quoteWriter.append(origin.query().all().asResultSet().subset(100, 200));
        quoteWriter.commit();

        // send quote state
        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(0, quoteClientWriter));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());

        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        agent.process(channel);

        commandConsumer.read(channel);
        Assert.assertEquals(Command.JOURNAL_DELTA_CMD, commandConsumer.getValue());

        Assert.assertEquals(0, intResponseConsumer.getValue(channel));
        quoteDeltaConsumer.read(channel);
        Assert.assertEquals(200, quoteClientWriter.size());

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
//        charSequenceResponseConsumer.reset();
//        charSequenceResponseConsumer.read(channel);
//        Assert.assertTrue(charSequenceResponseConsumer.isComplete());
//        Assert.assertEquals("Journal index is too large. Max 1", charSequenceResponseConsumer.getValue());


        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getKey()));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());
        hugeBufferConsumer.read(channel);

        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(1, quoteClientWriter));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("Journal index does not match key request", charSequenceResponseConsumer.getValue());

        commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
        journalClientStateProducer.write(channel, new IndexedJournal(0, quoteClientWriter));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());

        server.halt();
    }

    @Test
    public void testSetKeyRequestResponse() throws Exception {
        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getKey()));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());
        hugeBufferConsumer.read(channel);

        commandProducer.write(channel, Command.SET_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, tradeWriter.getKey()));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("Requested key not exported: JournalKey{id=com.nfsdb.model.Trade, location='null', partitionType=DEFAULT, recordHint=0, ordered=true}", charSequenceResponseConsumer.getValue());
    }
}
