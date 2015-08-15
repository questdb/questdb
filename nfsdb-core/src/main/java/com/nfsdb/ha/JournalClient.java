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

import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.PartitionType;
import com.nfsdb.collections.IntList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.IncompatibleJournalException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.ha.auth.AuthConfigurationException;
import com.nfsdb.ha.auth.AuthFailureException;
import com.nfsdb.ha.auth.CredentialProvider;
import com.nfsdb.ha.comsumer.HugeBufferConsumer;
import com.nfsdb.ha.comsumer.JournalDeltaConsumer;
import com.nfsdb.ha.config.ClientConfig;
import com.nfsdb.ha.config.ServerNode;
import com.nfsdb.ha.config.SslConfig;
import com.nfsdb.ha.model.Command;
import com.nfsdb.ha.model.IndexedJournal;
import com.nfsdb.ha.model.IndexedJournalKey;
import com.nfsdb.ha.producer.JournalClientStateProducer;
import com.nfsdb.ha.protocol.CommandConsumer;
import com.nfsdb.ha.protocol.CommandProducer;
import com.nfsdb.ha.protocol.Version;
import com.nfsdb.ha.protocol.commands.*;
import com.nfsdb.logging.Logger;
import com.nfsdb.storage.TxListener;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Files;
import com.nfsdb.utils.NamedDaemonThreadFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class JournalClient {
    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Logger LOGGER = Logger.getLogger(JournalClient.class);
    private final static ThreadFactory CLIENT_THREAD_FACTORY = new NamedDaemonThreadFactory("journal-client", false);
    private final ObjList<JournalKey> remoteKeys = new ObjList<>();
    private final ObjList<JournalKey> localKeys = new ObjList<>();
    private final ObjList<TxListener> listeners = new ObjList<>();
    private final ObjList<JournalWriter> writers = new ObjList<>();
    private final ObjList<JournalDeltaConsumer> deltaConsumers = new ObjList<>();
    private final IntList statusSentList = new IntList();
    private final JournalWriterFactory factory;
    private final CommandProducer commandProducer = new CommandProducer();
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final SetKeyRequestProducer setKeyRequestProducer = new SetKeyRequestProducer();
    private final CharSequenceResponseConsumer charSequenceResponseConsumer = new CharSequenceResponseConsumer();
    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private final IntResponseProducer intResponseProducer = new IntResponseProducer();
    private final ByteArrayResponseProducer byteArrayResponseProducer = new ByteArrayResponseProducer();

    private final ClientConfig config;
    private final ExecutorService service;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CredentialProvider credentialProvider;
    private final DisconnectCallbackImpl disconnectCallback = new DisconnectCallbackImpl();
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

    public void halt() {
        if (running.compareAndSet(true, false)) {
            if (handlerFuture != null) {
                try {
                    handlerFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Exception while waiting for client to shutdown gracefully", e);
                } finally {
                    handlerFuture = null;
                }
            }
            close0();
            free();
        } else {
            closeChannel();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public void setDisconnectCallback(DisconnectCallback callback) {
        this.disconnectCallback.next = callback;
    }

    public void start() throws JournalNetworkException {
        if (running.compareAndSet(false, true)) {
            handshake();
            handlerFuture = service.submit(new Handler());
        }
    }

    public <T> void subscribe(Class<T> clazz) {
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
     */
    public <T> void subscribe(Class<T> clazz, TxListener txListener) {
        subscribe(new JournalKey<>(clazz), new JournalKey<>(clazz), txListener);
    }

    @SuppressWarnings("unused")
    public <T> void subscribe(Class<T> clazz, String location) {
        subscribe(clazz, location, (TxListener) null);
    }

    public <T> void subscribe(Class<T> clazz, String location, TxListener txListener) {
        subscribe(new JournalKey<>(clazz, location), new JournalKey<>(clazz, location), txListener);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local) {
        subscribe(clazz, remote, local, null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, TxListener txListener) {
        subscribe(new JournalKey<>(clazz, remote), new JournalKey<>(clazz, local), txListener);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, int recordHint) {
        subscribe(clazz, remote, local, recordHint, null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, int recordHint, TxListener txListener) {
        subscribe(new JournalKey<>(clazz, remote, PartitionType.DEFAULT, recordHint), new JournalKey<>(clazz, local, PartitionType.DEFAULT, recordHint), txListener);
    }

    public <T> void subscribe(JournalKey<T> remoteKey, JournalWriter<T> writer, TxListener txListener) {
        remoteKeys.add(remoteKey);
        localKeys.add(writer.getKey());
        listeners.add(txListener);
        set0(remoteKeys.size() - 1, writer, txListener);
    }

    public void subscribe(JournalKey remote, JournalKey local, TxListener txListener) {
        remoteKeys.add(remote);
        localKeys.add(local);
        listeners.add(txListener);
    }

    private void checkAck() throws JournalNetworkException {
        charSequenceResponseConsumer.read(channel);
        fail(Chars.equals("OK", charSequenceResponseConsumer.getValue()), charSequenceResponseConsumer.getValue().toString());
    }

    private void checkAuthAndSendCredential() throws JournalNetworkException {
        commandProducer.write(channel, Command.HANDSHAKE_COMPLETE);
        CharSequence cs = readString();
        if (Chars.equals("AUTH", cs)) {
            if (credentialProvider == null) {
                throw new AuthConfigurationException();
            }
            commandProducer.write(channel, Command.AUTHORIZATION);
            byteArrayResponseProducer.write(channel, getToken());
            CharSequence response = readString();
            if (!Chars.equals("OK", response)) {
                throw new AuthFailureException(response.toString());
            }
        } else if (!Chars.equals("OK", cs)) {
            fail(true, "Unknown server response");
        }
    }

    private void close0() {

        closeChannel();
        for (int i = 0, sz = writers.size(); i < sz; i++) {
            writers.getQuick(i).close();
        }

        writers.clear();
        statusSentList.clear();
        deltaConsumers.clear();
    }

    private void closeChannel() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException e) {
                LOGGER.error("Error closing channel", e);
            } finally {
                channel = null;
            }
        }
    }

    private void fail(boolean condition, String message) throws JournalNetworkException {
        if (!condition) {
            throw new JournalNetworkException(message);
        }
    }

    private void free() {
        for (int i = 0, k = deltaConsumers.size(); i < k; i++) {
            deltaConsumers.getQuick(i).free();
        }
        commandConsumer.free();
        charSequenceResponseConsumer.free();
        intResponseConsumer.free();
    }

    private byte[] getToken() throws JournalNetworkException {
        try {
            return credentialProvider.createToken();
        } catch (Exception e) {
            halt();
            throw new JournalNetworkException(e);
        }
    }

    private void handshake() throws JournalNetworkException {
        openChannel(null);
        sendProtocolVersion();
        sendKeys();
        checkAuthAndSendCredential();
        sendState();
        counter.incrementAndGet();
    }

    private void openChannel(ServerNode node) throws JournalNetworkException {
        if (this.channel == null || node != null) {
            if (channel != null) {
                closeChannel();
            }
            SocketChannel channel = node == null ? config.openSocketChannel() : config.openSocketChannel(node);
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
        }
    }

    private CharSequence readString() throws JournalNetworkException {
        charSequenceResponseConsumer.read(channel);
        return charSequenceResponseConsumer.getValue();
    }

    private void sendDisconnect() throws JournalNetworkException {
        commandProducer.write(channel, Command.CLIENT_DISCONNECT);
    }

    @SuppressFBWarnings({"PL_PARALLEL_LISTS"})
    private void sendKeys() throws JournalNetworkException {
        for (int i = 0, sz = remoteKeys.size(); i < sz; i++) {
            commandProducer.write(channel, Command.SET_KEY_CMD);
            setKeyRequestProducer.write(channel, new IndexedJournalKey(i, remoteKeys.getQuick(i)));
            checkAck();


            JournalMetadata metadata;
            File file = Files.makeTempFile();
            try {
                try (HugeBufferConsumer h = new HugeBufferConsumer(file)) {
                    h.read(channel);
                    metadata = new JournalMetadata(h.getHb());
                } catch (JournalException e) {
                    throw new JournalNetworkException(e);
                }
            } finally {
                Files.delete(file);
            }


            try {
                if (writers.getQuiet(i) == null) {
                    set0(i, factory.writer(new JournalStructure(metadata).location(localKeys.getQuick(i).derivedLocation())), listeners.getQuick(i));
                }
            } catch (JournalException e) {
                throw new JournalNetworkException(e);
            }
        }
    }

    private void sendProtocolVersion() throws JournalNetworkException {
        commandProducer.write(channel, Command.PROTOCOL_VERSION);
        intResponseProducer.write(channel, Version.PROTOCOL_VERSION);
        checkAck();
    }

    private void sendReady() throws JournalNetworkException {
        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        LOGGER.debug("Client ready: " + channel);
    }

    private void sendState() throws JournalNetworkException {
        for (int i = 0, sz = writers.size(); i < sz; i++) {
            if (statusSentList.get(i) == 0) {
                commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
                journalClientStateProducer.write(channel, new IndexedJournal(i, writers.getQuick(i)));
                checkAck();
                statusSentList.setQuick(i, 1);
            }
        }
        sendReady();
    }

    private <T> void set0(int index, JournalWriter<T> writer, TxListener txListener) {
        statusSentList.extendAndSet(index, 0);
        deltaConsumers.extendAndSet(index, new JournalDeltaConsumer(writer.setCommitOnClose(false)));
        writers.extendAndSet(index, writer);
        if (txListener != null) {
            writer.setTxListener(txListener);
        }
    }

    public enum DisconnectReason {
        UNKNOWN, CLIENT_HALT, CLIENT_EXCEPTION, BROKEN_CHANNEL, CLIENT_ERROR, INCOMPATIBLE_JOURNAL
    }

    public interface DisconnectCallback {
        void onDisconnect(DisconnectReason reason);
    }

    private final class DisconnectCallbackImpl implements DisconnectCallback {
        private DisconnectCallback next;

        public void onDisconnect(DisconnectReason reason) {
            switch (reason) {
                case BROKEN_CHANNEL:
                case UNKNOWN:
                    int retryCount = config.getReconnectPolicy().getRetryCount();
                    int loginRetryCount = config.getReconnectPolicy().getLoginRetryCount();
                    boolean connected = false;
                    while (running.get() && !connected && retryCount-- > 0 && loginRetryCount > 0) {
                        try {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(config.getReconnectPolicy().getSleepBetweenRetriesMillis()));
                            LOGGER.info("Retrying reconnect ... [" + (retryCount + 1) + ']');
                            close0();
                            handshake();
                            connected = true;
                        } catch (AuthConfigurationException | AuthFailureException e) {
                            loginRetryCount--;
                        } catch (JournalNetworkException ignored) {
                            LOGGER.warn("Error during disconnect", ignored);
                        }
                    }

                    if (connected) {
                        handlerFuture = service.submit(new Handler());
                    } else {
                        disconnect(reason);
                    }
                    break;
                default:
                    disconnect(reason);
            }
        }

        private void disconnect(DisconnectReason reason) {
            LOGGER.info("Client disconnecting");
            counter.decrementAndGet();
            running.set(false);
            // set future to null to prevent deadlock
            handlerFuture = null;
            service.shutdown();

            if (next != null) {
                next.onDisconnect(reason);
            }
        }
    }

    private final class Handler implements Runnable {
        @Override
        public void run() {
            DisconnectReason reason = DisconnectReason.UNKNOWN;
            try {
                OUT:
                while (true) {
                    assert channel != null;
                    commandConsumer.read(channel);
                    switch (commandConsumer.getValue()) {
                        case JOURNAL_DELTA_CMD:
                            statsChannel.setDelegate(channel);
                            int index = intResponseConsumer.getValue(statsChannel);
                            deltaConsumers.getQuick(index).read(statsChannel);
                            statusSentList.set(index, 0);
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
                        case SERVER_SHUTDOWN:
                            reason = DisconnectReason.BROKEN_CHANNEL;
                            break OUT;
                        default:
                            LOGGER.warn("Unknown command: ", commandConsumer.getValue());
                    }
                }
            } catch (IncompatibleJournalException e) {
                LOGGER.error(e.getMessage());
                reason = DisconnectReason.INCOMPATIBLE_JOURNAL;
            } catch (JournalNetworkException e) {
                LOGGER.error("Network error. Server died?");
                LOGGER.debug("Network error details:", e);
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
