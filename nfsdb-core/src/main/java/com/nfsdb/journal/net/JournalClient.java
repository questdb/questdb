/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
import com.nfsdb.journal.PartitionType;
import com.nfsdb.journal.collections.DirectIntList;
import com.nfsdb.journal.concurrent.NamedDaemonThreadFactory;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.factory.JournalWriterFactory;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.net.auth.AuthConfigurationException;
import com.nfsdb.journal.net.auth.AuthFailureException;
import com.nfsdb.journal.net.auth.CredentialProvider;
import com.nfsdb.journal.net.comsumer.JournalDeltaConsumer;
import com.nfsdb.journal.net.config.ClientConfig;
import com.nfsdb.journal.net.config.SslConfig;
import com.nfsdb.journal.net.model.Command;
import com.nfsdb.journal.net.model.IndexedJournal;
import com.nfsdb.journal.net.model.IndexedJournalKey;
import com.nfsdb.journal.net.producer.JournalClientStateProducer;
import com.nfsdb.journal.net.protocol.CommandConsumer;
import com.nfsdb.journal.net.protocol.CommandProducer;
import com.nfsdb.journal.net.protocol.Version;
import com.nfsdb.journal.net.protocol.commands.*;
import com.nfsdb.journal.tx.TxListener;

import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class JournalClient {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger LOGGER = Logger.getLogger(JournalClient.class);
    private final static ThreadFactory CLIENT_THREAD_FACTORY = new NamedDaemonThreadFactory("journal-client", false);
    private final List<JournalKey> remoteKeys = new ArrayList<>();
    private final List<JournalKey> localKeys = new ArrayList<>();
    private final List<TxListener> listeners = new ArrayList<>();
    private final List<JournalWriter> writers = new ArrayList<>();
    private final List<JournalDeltaConsumer> deltaConsumers = new ArrayList<>();
    private final DirectIntList statusSentList = new DirectIntList();
    private final JournalWriterFactory factory;
    private final CommandProducer commandProducer = new CommandProducer();
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final SetKeyRequestProducer setKeyRequestProducer = new SetKeyRequestProducer();
    private final StringResponseConsumer stringResponseConsumer = new StringResponseConsumer();
    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private final IntResponseProducer intResponseProducer = new IntResponseProducer();
    private final ByteArrayResponseProducer byteArrayResponseProducer = new ByteArrayResponseProducer();

    private final ClientConfig config;
    private final ExecutorService service;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CredentialProvider credentialProvider;
    private final DisconnectCallback disconnectCallback = new DisconnectCallback();
    private ByteChannel channel;
    private StatsCollectingReadableByteChannel statsChannel;
    private Future handlerFuture;

    public JournalClient(JournalWriterFactory factory) {
        this(factory, null);
    }

    public JournalClient(JournalWriterFactory factory, CredentialProvider credentialProvider) {
        this(new ClientConfig(), factory, credentialProvider);
    }

    public JournalClient(ClientConfig config, JournalWriterFactory factory) {
        this(config, factory, null);
    }

    public JournalClient(ClientConfig config, JournalWriterFactory factory, CredentialProvider credentialProvider) {
        this.config = config;
        this.factory = factory;
        this.service = Executors.newCachedThreadPool(CLIENT_THREAD_FACTORY);
        this.credentialProvider = credentialProvider;
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
        subscribe(clazz, remote, local, recordHint, null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, int recordHint, TxListener txListener) throws JournalException {
        add(new JournalKey<>(clazz, remote, PartitionType.DEFAULT, recordHint), factory.writer(clazz, local, recordHint), txListener);
    }

    public void start() throws JournalNetworkException {
        if (!isRunning()) {
            handshake();
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
            close0();
            statusSentList.free();
        } catch (Exception e) {
            throw new JournalNetworkException(e);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    private void close0() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
        channel = null;

        for (int i = 0, sz = writers.size(); i < sz; i++) {
            writers.get(i).close();
        }

        writers.clear();
        statusSentList.clear();
        deltaConsumers.clear();
        commandConsumer.reset();
        stringResponseConsumer.reset();
        intResponseConsumer.reset();
    }

    private void handshake() throws JournalNetworkException {
        SocketChannel channel = config.openSocketChannel();
        try {
            statsChannel = new StatsCollectingReadableByteChannel(channel.getRemoteAddress());
        } catch (IOException e) {
            throw new JournalNetworkException("Cannot get remote address", e);
        }

        SslConfig sslConfig = config.getSslConfig();
        if (sslConfig.isSecure()) {
            this.channel = new SecureByteChannel(channel, sslConfig);
        } else {
            this.channel = channel;
        }

        sendProtocolVersion();
        sendKeys();
        checkAuthAndSendCredential();
        sendState();
        running.set(true);
        counter.incrementAndGet();
    }

    private void checkAuthAndSendCredential() throws JournalNetworkException {
        commandProducer.write(channel, Command.HANDSHAKE_COMPLETE);
        switch (readString()) {
            case "AUTH":
                if (credentialProvider == null) {
                    throw new AuthConfigurationException();
                }
                commandProducer.write(channel, Command.AUTHORIZATION);
                byteArrayResponseProducer.write(channel, getToken());
                String response = readString();
                if (!"OK".equals(response)) {
                    throw new AuthFailureException(response);
                }
                break;
            case "OK":
                break;
            default:
                fail(true, "Unknown server response");
        }
    }

    private byte[] getToken() throws JournalNetworkException {
        try {
            return credentialProvider.createToken();
        } catch (Exception e) {
            halt();
            throw new JournalNetworkException(e);
        }
    }

    private String readString() throws JournalNetworkException {
        stringResponseConsumer.reset();
        stringResponseConsumer.read(channel);
        fail(stringResponseConsumer.isComplete(), "Incomplete response");
        return stringResponseConsumer.getValue();
    }

    private void sendProtocolVersion() throws JournalNetworkException {
        commandProducer.write(channel, Command.PROTOCOL_VERSION);
        intResponseProducer.write(channel, Version.PROTOCOL_VERSION);
        checkAck();
    }

    private <T> void add(JournalKey<T> remoteKey, JournalWriter<T> writer, TxListener txListener) {
        remoteKeys.add(remoteKey);
        localKeys.add(writer.getKey());
        listeners.add(txListener);
        add0(writer, txListener);
    }

    private <T> void add0(JournalWriter<T> writer, TxListener txListener) {
        deltaConsumers.add(new JournalDeltaConsumer(writer.setCommitOnClose(false)));
        writers.add(writer);
        statusSentList.add(0);
        if (txListener != null) {
            writer.setTxListener(txListener);
        }
    }

    @SuppressWarnings("unchecked")
    private void reopenWriters() throws JournalException {
        for (int i = 0, sz = localKeys.size(); i < sz; i++) {
            add0(factory.writer(localKeys.get(i)), listeners.get(0));
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
        for (int i = 0, sz = remoteKeys.size(); i < sz; i++) {
            JournalKey key = remoteKeys.get(i);
            commandProducer.write(channel, Command.SET_KEY_CMD);
            setKeyRequestProducer.write(channel, new IndexedJournalKey(i, key));
            checkAck();
        }
    }

    private void sendState() throws JournalNetworkException {
        for (int i = 0, sz = writers.size(); i < sz; i++) {
            if (statusSentList.get(i) == 0) {
                Journal journal = writers.get(i);
                commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
                journalClientStateProducer.write(channel, new IndexedJournal(i, journal));
                checkAck();
                statusSentList.set(i, 1);
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


    private enum DisconnectReason {
        UNKNOWN, CLIENT_HALT, CLIENT_EXCEPTION, BROKEN_CHANNEL, CLIENT_ERROR
    }

    private final class DisconnectCallback {
        private void onDisconnect(DisconnectReason reason) {
            switch (reason) {
                case BROKEN_CHANNEL:
                case UNKNOWN:
                    int retryCount = config.getReconnectPolicy().getRetryCount();
                    int loginRetryCount = config.getReconnectPolicy().getLoginRetryCount();
                    boolean connected = false;
                    while (running.get() && !connected && retryCount-- > 0 && loginRetryCount > 0) {
                        try {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(config.getReconnectPolicy().getSleepBetweenRetriesMillis()));
                            LOGGER.info("Retrying reconnect ... [" + (retryCount + 1) + "]");
                            close0();
                            reopenWriters();
                            handshake();
                            connected = true;
                        } catch (AuthConfigurationException | AuthFailureException e) {
                            loginRetryCount--;
                        } catch (IOException | JournalNetworkException | JournalException ignored) {
                        }
                    }

                    if (connected) {
                        handlerFuture = service.submit(new Handler());
                    } else {
                        disconnect();
                    }
                    break;
                default:
                    disconnect();
            }
        }

        private void disconnect() {
            LOGGER.info("Client disconnecting");
            counter.decrementAndGet();
            running.set(false);
            // set future to null to prevent deadlock
            handlerFuture = null;
            service.shutdown();
        }
    }

    private final class Handler implements Runnable {
        @Override
        public void run() {
            JournalDeltaConsumer deltaConsumer = null;
            DisconnectReason reason = DisconnectReason.UNKNOWN;
            try {
                OUT:
                while (true) {
                    assert channel != null;
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
                                    statusSentList.set(index, 0);
                                }
                                statsChannel.logStats();
                                break;
                            case SERVER_READY_CMD:
                                if (isRunning()) {
                                    sendState();
                                } else {
                                    sendDisconnect();
                                    reason = DisconnectReason.CLIENT_HALT;
                                    break OUT;
                                }
                                break;
                            case SERVER_HEARTBEAT:
                                if (isRunning()) {
                                    sendReady();
                                } else {
                                    sendDisconnect();
                                    reason = DisconnectReason.CLIENT_HALT;
                                    break OUT;
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
                }
            } catch (JournalNetworkException e) {
                LOGGER.error("Network error. Server died?", e);
                reason = DisconnectReason.BROKEN_CHANNEL;
            } catch (Throwable e) {
                LOGGER.error("Unhandled exception in client", e);
                if (e instanceof Error) {
                    reason = DisconnectReason.CLIENT_ERROR;
                    throw e;
                } else {
                    reason = DisconnectReason.CLIENT_EXCEPTION;
                }
            } finally {
                disconnectCallback.onDisconnect(reason);
            }
        }
    }
}
