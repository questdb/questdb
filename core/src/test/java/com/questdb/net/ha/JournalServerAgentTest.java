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

import com.questdb.misc.Chars;
import com.questdb.model.Quote;
import com.questdb.model.Trade;
import com.questdb.net.ha.comsumer.HugeBufferConsumer;
import com.questdb.net.ha.comsumer.JournalDeltaConsumer;
import com.questdb.net.ha.config.NetworkConfig;
import com.questdb.net.ha.config.ServerConfig;
import com.questdb.net.ha.model.Command;
import com.questdb.net.ha.model.IndexedJournal;
import com.questdb.net.ha.model.IndexedJournalKey;
import com.questdb.net.ha.producer.JournalClientStateProducer;
import com.questdb.net.ha.protocol.CommandConsumer;
import com.questdb.net.ha.protocol.CommandProducer;
import com.questdb.net.ha.protocol.commands.CharSequenceResponseConsumer;
import com.questdb.net.ha.protocol.commands.IntResponseConsumer;
import com.questdb.net.ha.protocol.commands.SetKeyRequestProducer;
import com.questdb.store.Journal;
import com.questdb.store.JournalWriter;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
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
        quoteWriter = getFactory().writer(Quote.class);
        tradeWriter = getFactory().writer(Trade.class);
        ServerConfig config = new ServerConfig() {{
            setHeartbeatFrequency(100);
            setEnableMultiCast(false);
        }};

        server = new JournalServer(config, getFactory());
        server.publish(quoteWriter);
        agent = new JournalServerAgent(server, new InetSocketAddress(NetworkConfig.DEFAULT_DATA_PORT), null);
        hugeBufferConsumer = new HugeBufferConsumer(temp.newFile());
    }

    @After
    public void tearDown() {
        quoteWriter.close();
        tradeWriter.close();
        server.halt();
        agent.close();
        hugeBufferConsumer.free();
    }

    @Test
    public void testIncrementalInteraction() throws Exception {
        try (JournalWriter<Quote> origin = getFactory().writer(Quote.class, "origin")) {
            TestUtils.generateQuoteData(origin, 200);

            server.start();
            try (JournalWriter<Quote> quoteClientWriter = getFactory().writer(Quote.class, "client")) {

                JournalDeltaConsumer quoteDeltaConsumer = new JournalDeltaConsumer(quoteClientWriter);

                // send quote journal key
                commandProducer.write(channel, Command.ADD_KEY_CMD);
                setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getMetadata().getKey()));
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
                Assert.assertEquals(Command.JOURNAL_DELTA_CMD, commandConsumer.getCommand());

                Assert.assertEquals(0, intResponseConsumer.getValue(channel));
                quoteDeltaConsumer.read(channel);
                Assert.assertEquals(100, quoteClientWriter.size());

                commandConsumer.read(channel);
                Assert.assertEquals(Command.SERVER_READY_CMD, commandConsumer.getCommand());

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
                Assert.assertEquals(Command.JOURNAL_DELTA_CMD, commandConsumer.getCommand());

                Assert.assertEquals(0, intResponseConsumer.getValue(channel));
                quoteDeltaConsumer.read(channel);
                Assert.assertEquals(200, quoteClientWriter.size());

                commandConsumer.read(channel);
                Assert.assertEquals(Command.SERVER_READY_CMD, commandConsumer.getCommand());
            }
        }
    }

    @Test
    public void testJournalIndexCorrectness() throws Exception {
        server.publish(tradeWriter);
        server.start();

        try (Journal<Quote> quoteClientWriter = getFactory().writer(Quote.class, "client")) {

            // send quote journal key
//        commandProducer.write(channel, Command.ADD_KEY_CMD);
//        setKeyRequestProducer.write(channel, new IndexedJournalKey(3, quoteWriter.getKey()));
//        agent.process(channel);
//        charSequenceResponseConsumer.reset();
//        charSequenceResponseConsumer.read(channel);
//        Assert.assertTrue(charSequenceResponseConsumer.isComplete());
//        Assert.assertEquals("Journal index is too large. Max 1", charSequenceResponseConsumer.getValue());


            commandProducer.write(channel, Command.ADD_KEY_CMD);
            setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getMetadata().getKey()));
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
        }
    }

    @Test
    public void testSetKeyRequestResponse() throws Exception {
        commandProducer.write(channel, Command.ADD_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, quoteWriter.getMetadata().getKey()));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        TestUtils.assertEquals("OK", charSequenceResponseConsumer.getValue());
        hugeBufferConsumer.read(channel);

        commandProducer.write(channel, Command.ADD_KEY_CMD);
        setKeyRequestProducer.write(channel, new IndexedJournalKey(0, tradeWriter.getMetadata().getKey()));
        agent.process(channel);
        charSequenceResponseConsumer.read(channel);
        Assert.assertTrue(Chars.startsWith(charSequenceResponseConsumer.getValue(), "Requested key not exported"));
    }
}
