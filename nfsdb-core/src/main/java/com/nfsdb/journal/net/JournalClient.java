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

package com.nfsdb.journal.net;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalKey;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.journal.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.factory.JournalWriterFactory;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.net.comsumer.JournalDeltaConsumer;
import com.nfsdb.journal.net.config.ClientConfig;
import com.nfsdb.journal.net.model.Command;
import com.nfsdb.journal.net.model.IndexedJournal;
import com.nfsdb.journal.net.model.IndexedJournalKey;
import com.nfsdb.journal.net.producer.JournalClientStateProducer;
import com.nfsdb.journal.net.protocol.CommandConsumer;
import com.nfsdb.journal.net.protocol.CommandProducer;
import com.nfsdb.journal.net.protocol.commands.IntResponseConsumer;
import com.nfsdb.journal.net.protocol.commands.SetKeyRequestProducer;
import com.nfsdb.journal.net.protocol.commands.StringResponseConsumer;
import com.nfsdb.journal.tx.TxListener;
import gnu.trove.list.array.TByteArrayList;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JournalClient {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger LOGGER = Logger.getLogger(JournalClient.class);
    private final static ThreadFactory CLIENT_THREAD_FACTORY = new NamedDaemonThreadFactory("journal-client", false);
    private final List<JournalKey> remoteKeys = new ArrayList<>();
    private final List<JournalWriter> writers = new ArrayList<>();
    private final List<JournalDeltaConsumer> deltaConsumers = new ArrayList<>();
    private final TByteArrayList statusSentList = new TByteArrayList();
    private final JournalWriterFactory factory;
    private final CommandProducer commandProducer = new CommandProducer();
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final SetKeyRequestProducer setKeyRequestProducer = new SetKeyRequestProducer();
    private final StringResponseConsumer stringResponseConsumer = new StringResponseConsumer();
    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private final ClientConfig config;
    private final ExecutorService service;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private SocketChannel channel;
    private StatsCollectingReadableByteChannel statsChannel;
    private Future handlerFuture;

    public JournalClient(JournalWriterFactory factory) {
        this(new ClientConfig(), factory);
    }

    public JournalClient(ClientConfig config, JournalWriterFactory factory) {
        this.config = config;
        this.factory = factory;
        this.service = Executors.newCachedThreadPool(CLIENT_THREAD_FACTORY);
    }

    public <T> void subscribe(Class<T> clazz) throws JournalException {
        subscribe(clazz, (TxListener) null);
    }

    /**
     * Configures client to subscribe given journal class when client is started
     * and connected. Journals of given class at default location are opened on
     * both client and server. Optionally provided listener will be called back
     * when client journal is committed. Listener is called synchronously with
     * client thread, so callback implementation must be fast.
     *
     * @param clazz      journal class on both client and server
     * @param txListener callback listener to get receive commit notifications.
     * @param <T>        generics to comply with Journal API.
     * @throws com.nfsdb.journal.exceptions.JournalException if local writer cannot be opened.
     */
    public <T> void subscribe(Class<T> clazz, TxListener txListener) throws JournalException {
        add(new JournalKey<>(clazz), factory.writer(clazz), txListener);
    }

    @SuppressWarnings("unused")
    public <T> void subscribe(Class<T> clazz, String location) throws JournalException {
        subscribe(clazz, location, (TxListener) null);
    }

    public <T> void subscribe(Class<T> clazz, String location, TxListener txListener) throws JournalException {
        add(new JournalKey<>(clazz, location), factory.writer(clazz, location), txListener);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local) throws JournalException {
        subscribe(clazz, remote, local, null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, TxListener txListener) throws JournalException {
        add(new JournalKey<>(clazz, remote), factory.writer(clazz, local), txListener);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, int recordHint) throws JournalException {
        sync(clazz, remote, local, recordHint, null);
    }

    public <T> void sync(Class<T> clazz, String remote, String local, int recordHint, TxListener txListener) throws JournalException {
        add(new JournalKey<>(clazz, remote), factory.writer(clazz, local, recordHint), txListener);
    }

    public void start() throws JournalNetworkException {
        if (!isRunning()) {
            channel = config.openSocketChannel();
            try {
                statsChannel = new StatsCollectingReadableByteChannel(channel.getRemoteAddress());
            } catch (IOException e) {
                throw new JournalNetworkException("Cannot get remote address", e);
            }
            sendKeys();
            sendState();
            running.set(true);
            counter.incrementAndGet();
            handlerFuture = service.submit(new Handler());
        }
    }

    public void halt() throws JournalNetworkException {
        running.set(false);
        try {
            if (handlerFuture != null) {
                handlerFuture.get();
                handlerFuture = null;
            }
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            channel = null;

            for (JournalWriter w : writers) {
                w.close();
            }
        } catch (Exception e) {
            throw new JournalNetworkException(e);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    private <T> void add(JournalKey<T> remoteKey, JournalWriter<T> writer, TxListener txListener) {
        remoteKeys.add(remoteKey);
        deltaConsumers.add(new JournalDeltaConsumer(writer));
        writers.add(writer);
        statusSentList.add((byte) 0);
        if (txListener != null) {
            writer.setTxListener(txListener);
        }
    }

    private void fail(boolean condition, String message) throws JournalNetworkException {
        if (!condition) {
            throw new JournalNetworkException(message);
        }
    }

    private void checkAck() throws JournalNetworkException {
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        fail(stringResponseConsumer.isComplete(), "Incomplete response");
        fail("OK".equals(stringResponseConsumer.getValue()), stringResponseConsumer.getValue());
    }

    private void sendKeys() throws JournalNetworkException {
        for (int i = 0; i < remoteKeys.size(); i++) {
            JournalKey key = remoteKeys.get(i);
            commandProducer.write(channel, Command.SET_KEY_CMD);
            setKeyRequestProducer.write(channel, new IndexedJournalKey(i, key));
            checkAck();
        }
    }

    private void sendState() throws JournalNetworkException {
        for (int i = 0; i < writers.size(); i++) {
            if (statusSentList.get(i) == 0) {
                Journal journal = writers.get(i);
                commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
                journalClientStateProducer.write(channel, new IndexedJournal(i, journal));
                checkAck();
                statusSentList.set(i, (byte) 1);
            }
        }
        sendReady();
    }

    private void sendDisconnect() throws JournalNetworkException {
        commandProducer.write(channel, Command.CLIENT_DISCONNECT);
    }

    private void sendReady() throws JournalNetworkException {
        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        LOGGER.debug("Client ready: " + channel);
    }

    private final class Handler implements Runnable {
        @Override
        public void run() {
            JournalDeltaConsumer deltaConsumer = null;
            boolean loop = true;
            while (loop) {
                try {
                    commandConsumer.read(channel);
                    if (commandConsumer.isComplete()) {
                        switch (commandConsumer.getValue()) {
                            case JOURNAL_DELTA_CMD:
                                statsChannel.setDelegate(channel);
                                intResponseConsumer.read(statsChannel);
                                if (intResponseConsumer.isComplete()) {
                                    int index = intResponseConsumer.getValue();
                                    deltaConsumer = deltaConsumers.get(index);
                                    deltaConsumer.read(statsChannel);
                                    statusSentList.set(index, (byte) 0);
                                }
                                statsChannel.logStats();
                                break;
                            case SERVER_READY_CMD:
                                if (isRunning()) {
                                    sendState();
                                } else {
                                    sendDisconnect();
                                    loop = false;
                                }
                                break;
                            case SERVER_HEARTBEAT:
                                if (isRunning()) {
                                    sendReady();
                                } else {
                                    sendDisconnect();
                                    loop = false;
                                }
                                break;
                            default:
                                LOGGER.warn("Unknown command: ", commandConsumer.getValue());
                        }
                        commandConsumer.reset();
                        intResponseConsumer.reset();
                        if (deltaConsumer != null) {
                            deltaConsumer.reset();
                        }
                    }

                } catch (JournalDisconnectedChannelException e) {
                    running.set(false);
                    break;
                } catch (JournalNetworkException e) {
                    running.set(false);
                    LOGGER.error("Network error", e);
                    break;
                }
            }
            LOGGER.info("Client disconnecting");
            counter.decrementAndGet();
            // set future to null to prevent deadlock
            handlerFuture = null;
            service.shutdown();
        }
    }
}
