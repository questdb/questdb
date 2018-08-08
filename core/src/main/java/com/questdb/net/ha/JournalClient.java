/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.ex.IncompatibleJournalException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.mp.MPSequence;
import com.questdb.mp.RingQueue;
import com.questdb.mp.SCSequence;
import com.questdb.mp.Sequence;
import com.questdb.net.SecureSocketChannel;
import com.questdb.net.SslConfig;
import com.questdb.net.StatsCollectingReadableByteChannel;
import com.questdb.net.ha.auth.AuthenticationConfigException;
import com.questdb.net.ha.auth.AuthenticationProviderException;
import com.questdb.net.ha.auth.CredentialProvider;
import com.questdb.net.ha.auth.UnauthorizedException;
import com.questdb.net.ha.comsumer.HugeBufferConsumer;
import com.questdb.net.ha.comsumer.JournalDeltaConsumer;
import com.questdb.net.ha.config.ClientConfig;
import com.questdb.net.ha.model.Command;
import com.questdb.net.ha.model.IndexedJournal;
import com.questdb.net.ha.model.IndexedJournalKey;
import com.questdb.net.ha.producer.JournalClientStateProducer;
import com.questdb.net.ha.protocol.CommandConsumer;
import com.questdb.net.ha.protocol.CommandProducer;
import com.questdb.net.ha.protocol.Version;
import com.questdb.net.ha.protocol.commands.*;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.Files;
import com.questdb.store.*;
import com.questdb.store.factory.WriterFactory;
import com.questdb.store.factory.configuration.JournalMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class JournalClient {

    public static final int MSG_SUBSCRIBE = 0;
    public static final int MSG_HALT = 1;
    public static final int MSG_UNSUBSCRIBE = 2;

    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Log LOG = LogFactory.getLog(JournalClient.class);
    private final ObjList<JournalWriter> writers = new ObjList<>();
    private final ObjList<JournalWriter> writersToClose = new ObjList<>();
    private final ObjList<JournalDeltaConsumer> deltaConsumers = new ObjList<>();
    private final IntList statusSentList = new IntList();
    private final CharSequenceHashSet subscribedJournals = new CharSequenceHashSet();
    private final WriterFactory factory;
    private final CommandProducer commandProducer = new CommandProducer();
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final ObjList<SubscriptionHolder> subscriptions = new ObjList<>();
    private final SetKeyRequestProducer setKeyRequestProducer = new SetKeyRequestProducer();
    private final CharSequenceResponseConsumer charSequenceResponseConsumer = new CharSequenceResponseConsumer();
    private final JournalClientStateProducer journalClientStateProducer = new JournalClientStateProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private final IntResponseProducer intResponseProducer = new IntResponseProducer();
    private final ByteArrayResponseProducer byteArrayResponseProducer = new ByteArrayResponseProducer();
    private final ClientConfig config;
    private final CredentialProvider credentialProvider;
    private final RingQueue<SubscriptionHolder> subscriptionQueue = new RingQueue<>(SubscriptionHolder.FACTORY, 64);
    private final Sequence subscriptionPubSequence = new MPSequence(subscriptionQueue.getCapacity());
    private final Sequence subscriptionSubSequence = new SCSequence();
    private final CountDownLatch haltLatch = new CountDownLatch(1);
    private final Callback callback;
    private ByteChannel channel;
    private StatsCollectingReadableByteChannel statsChannel;
    private volatile boolean running = false;

    public JournalClient(WriterFactory factory) {
        this(factory, null);
    }

    public JournalClient(WriterFactory factory, CredentialProvider credentialProvider) {
        this(new ClientConfig(), factory, credentialProvider, null);
    }

    public JournalClient(ClientConfig config, WriterFactory factory) {
        this(config, factory, null, null);
    }

    public JournalClient(ClientConfig config, WriterFactory factory, CredentialProvider credentialProvider) {
        this(config, factory, credentialProvider, null);
    }

    public JournalClient(ClientConfig config, WriterFactory factory, CredentialProvider credentialProvider, Callback callback) {
        this.config = config;
        this.factory = factory;
        this.credentialProvider = credentialProvider;
        this.callback = callback;
        subscriptionPubSequence.then(subscriptionSubSequence).then(subscriptionPubSequence);
    }

    public void halt() {
        long cursor = subscriptionPubSequence.next();
        if (cursor < 0) {
            throw new JournalRuntimeException("start client before subscribing");
        }

        SubscriptionHolder h = subscriptionQueue.get(cursor);
        h.type = MSG_HALT;
        subscriptionPubSequence.done(cursor);

        try {
            if (!haltLatch.await(5, TimeUnit.SECONDS)) {
                closeChannel();
            }
        } catch (InterruptedException e) {
            LOG.error().$("Got interrupted while halting journal client").$();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void start() {
        new Handler().start();
    }

    public <T> void subscribe(Class<T> clazz) {
        subscribe(clazz, (JournalListener) null);
    }

    @SuppressWarnings("unused")
    public <T> void subscribe(Class<T> clazz, String location) {
        subscribe(clazz, location, (JournalListener) null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local) {
        subscribe(clazz, remote, local, null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, JournalListener journalListener) {
        subscribe(new JournalKey<>(clazz, remote), new JournalKey<>(clazz, local), journalListener);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, int recordHint) {
        subscribe(clazz, remote, local, recordHint, null);
    }

    public <T> void subscribe(Class<T> clazz, String remote, String local, int recordHint, JournalListener journalListener) {
        subscribe(new JournalKey<>(clazz, remote, PartitionBy.DEFAULT, recordHint), new JournalKey<>(clazz, local, PartitionBy.DEFAULT, recordHint), journalListener);
    }

    public <T> void subscribe(JournalKey<T> remoteKey, JournalWriter<T> writer, JournalListener journalListener) {
        subscribe(remoteKey, writer.getMetadata().getKey(), journalListener, writer);
    }

    public void subscribe(JournalKey remote, JournalKey local, JournalListener journalListener) {
        subscribe(remote, local, journalListener, null);
    }

    private void checkAck() throws JournalNetworkException {
        charSequenceResponseConsumer.read(channel);
        CharSequence value = charSequenceResponseConsumer.getValue();
        fail(Chars.equals("OK", value), value);
    }

    private void checkAuthAndSendCredential() throws
            JournalNetworkException,
            AuthenticationProviderException,
            UnauthorizedException,
            AuthenticationConfigException {

        commandProducer.write(channel, Command.HANDSHAKE_COMPLETE);
        CharSequence cs = readString();
        if (Chars.equals("AUTH", cs)) {
            if (credentialProvider == null) {
                throw new AuthenticationConfigException();
            }
            commandProducer.write(channel, Command.AUTHORIZATION);
            byteArrayResponseProducer.write(channel, getToken());
            CharSequence response = readString();
            if (!Chars.equals("OK", response)) {
                LOG.error().$(response).$();
                throw new UnauthorizedException();
            }
        } else if (!Chars.equals("OK", cs)) {
            fail(true, "Unknown server response");
        }
    }

    private void close0() {
        for (int i = 0, sz = writersToClose.size(); i < sz; i++) {
            writersToClose.getQuick(i).close();
        }
        writersToClose.clear();
        writers.clear();

        for (int i = 0, k = deltaConsumers.size(); i < k; i++) {
            deltaConsumers.getQuick(i).free();
        }
        deltaConsumers.clear();

        commandConsumer.free();
        charSequenceResponseConsumer.free();
        intResponseConsumer.free();
        statusSentList.clear();
    }

    private void closeChannel() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (Throwable e) {
                LOG.error().$("Error closing channel").$(e).$();
            }
        }
    }

    private void fail(boolean condition, CharSequence message) throws JournalNetworkException {
        if (!condition) {
            throw new JournalNetworkException(message.toString());
        }
    }

    private byte[] getToken() throws AuthenticationProviderException {
        try {
            return credentialProvider.createToken();
        } catch (Throwable e) {
            LOG.error().$("Error in credential provider: ").$(e).$();
            throw new AuthenticationProviderException();
        }
    }

    private void notifyCallback(int event) {
        if (callback != null) {
            callback.onEvent(event);
        }
    }

    private void openChannel() throws JournalNetworkException {
        if (this.channel == null || !this.channel.isOpen()) {
            SocketChannel channel = config.openSocketChannel();
            try {
                statsChannel = new StatsCollectingReadableByteChannel(channel.getRemoteAddress());
            } catch (IOException e) {
                throw new JournalNetworkException("Cannot get remote address", e);
            }

            SslConfig sslConfig = config.getSslConfig();
            if (sslConfig.isSecure()) {
                this.channel = new SecureSocketChannel(channel, sslConfig);
            } else {
                this.channel = channel;
            }
        }
    }

    private CharSequence readString() throws JournalNetworkException {
        charSequenceResponseConsumer.read(channel);
        return charSequenceResponseConsumer.getValue();
    }

    private void resubscribe() {
        for (int i = 0, n = subscriptions.size(); i < n; i++) {
            SubscriptionHolder h = subscriptions.get(i);
            subscribeOne(i, h, h.local.getName(), false);
        }
    }

    private void sendDisconnect() throws JournalNetworkException {
        commandProducer.write(channel, Command.CLIENT_DISCONNECT);
    }

    private void sendProtocolVersion() throws JournalNetworkException {
        commandProducer.write(channel, Command.PROTOCOL_VERSION);
        intResponseProducer.write(channel, Version.PROTOCOL_VERSION);
        checkAck();
    }

    private void sendReady() throws JournalNetworkException {
        commandProducer.write(channel, Command.CLIENT_READY_CMD);
        LOG.debug().$("Client ready: ").$(channel).$();
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
    }

    private void subscribe(JournalKey remote, JournalKey local, JournalListener journalListener, JournalWriter writer) {
        long cursor = subscriptionPubSequence.next();
        if (cursor < 0) {
            throw new JournalRuntimeException("start client before subscribing");
        }

        SubscriptionHolder h = subscriptionQueue.get(cursor);
        h.type = MSG_SUBSCRIBE;
        h.remote = remote;
        h.local = local;
        h.listener = journalListener;
        h.writer = writer;
        subscriptionPubSequence.done(cursor);
    }

    /**
     * Configures client to subscribe given journal class when client is started
     * and connected. Journals of given class at default location are opened on
     * both client and server. Optionally provided listener will be called back
     * when client journal is committed. Listener is called synchronously with
     * client thread, so callback implementation must be fast.
     *
     * @param clazz           journal class on both client and server
     * @param journalListener callback listener to get receive commit notifications.
     * @param <T>             generics to comply with Journal API.
     */
    private <T> void subscribe(Class<T> clazz, JournalListener journalListener) {
        subscribe(new JournalKey<>(clazz), new JournalKey<>(clazz), journalListener);
    }

    private <T> void subscribe(Class<T> clazz, String location, JournalListener journalListener) {
        subscribe(new JournalKey<>(clazz, location), new JournalKey<>(clazz, location), journalListener);
    }

    private void subscribeOne(int index, SubscriptionHolder holder, String name, boolean newSubscription) {

        if (newSubscription) {
            SubscriptionHolder sub = new SubscriptionHolder();
            sub.local = holder.local;
            sub.remote = holder.remote;
            sub.listener = holder.listener;
            sub.writer = holder.writer;
            subscriptions.add(sub);
        }

        JournalWriter<?> writer = writers.getQuiet(index);
        try {

            commandProducer.write(channel, Command.ADD_KEY_CMD);
            setKeyRequestProducer.write(channel, new IndexedJournalKey(index, holder.remote));
            checkAck();

            //todo: do we really have to use file here?
            JournalMetadata<?> metadata;
            File file = Files.makeTempFile();
            try {
                try (HugeBufferConsumer h = new HugeBufferConsumer(file)) {
                    h.read(channel);
                    metadata = new JournalMetadata(h.getHb(), name);
                } catch (JournalException e) {
                    throw new JournalNetworkException(e);
                }
            } finally {
                com.questdb.store.Files.delete(file);
            }

            boolean validate = true;
            if (writer == null) {
                if (holder.writer == null) {
                    try {
                        writer = factory.writer(metadata);
                    } catch (JournalException e) {
                        LOG.error().$("Failed to create writer: ").$(e).$();
                        unsubscribe(index, null, holder, JournalEvents.EVT_JNL_INCOMPATIBLE);
                        return;
                    }
                    writersToClose.add(writer);
                    validate = false;
                } else {
                    writer = holder.writer;
                }

                writer.disableCommitOnClose();
                statusSentList.extendAndSet(index, 0);
                deltaConsumers.extendAndSet(index, new JournalDeltaConsumer(writer));
                writers.extendAndSet(index, writer);
                writer.setJournalListener(holder.listener);
            } else {
                statusSentList.setQuick(index, 0);
            }

            if (validate && !metadata.isCompatible(writer.getMetadata(), false)) {
                LOG.error().$("Journal ").$(holder.local.getName()).$(" is not compatible with ").$(holder.remote.getName()).$("(remote)").$();
                unsubscribe(index, writer, holder, JournalEvents.EVT_JNL_INCOMPATIBLE);
                return;
            }

            commandProducer.write(channel, Command.DELTA_REQUEST_CMD);
            journalClientStateProducer.write(channel, new IndexedJournal(index, writer));
            checkAck();
            statusSentList.setQuick(index, 1);

            if (holder.listener != null) {
                holder.listener.onEvent(JournalEvents.EVT_JNL_SUBSCRIBED);
            }
            LOG.info().$("Subscribed ").$(name).$(" to ").$(holder.remote.getName()).$("(remote)").$();
        } catch (JournalNetworkException e) {
            LOG.error().$("Failed to subscribe ").$(name).$(" to ").$(holder.remote.getName()).$("(remote)").$();
            unsubscribe(index, writer, holder, JournalEvents.EVT_JNL_SERVER_ERROR);
        }
    }

    private void unsubscribe(int index, JournalWriter writer, SubscriptionHolder holder, int reason) {
        JournalDeltaConsumer deltaConsumer = deltaConsumers.getQuiet(index);
        if (deltaConsumer != null) {
            deltaConsumer.free();
        }

        if (writer != null && writersToClose.remove(writer) > -1) {
            writer.close();
        }

        if (index < writers.size()) {
            writers.setQuick(index, null);
        }

        try {
            commandProducer.write(channel, Command.REMOVE_KEY_CMD);
            setKeyRequestProducer.write(channel, new IndexedJournalKey(index, holder.remote));
            checkAck();
        } catch (JournalNetworkException e) {
            LOG.error().$("Failed to unsubscribe journal ").$(holder.remote.getName()).$(e).$();
            notifyCallback(JournalClientEvents.EVT_UNSUB_REJECT);
        }

        if (reason == JournalEvents.EVT_JNL_INCOMPATIBLE) {
            // remove from duplicate check set
            subscribedJournals.remove(holder.local.getName());

            // remove from re-subscription list
            for (int i = 0, n = subscriptions.size(); i < n; i++) {
                SubscriptionHolder h = subscriptions.getQuick(i);
                if (h.local.getName().equals(holder.local.getName())) {
                    subscriptions.remove(i);
                    break;
                }
            }
        }

        if (holder.listener != null) {
            holder.listener.onEvent(reason);
        }
    }

    public interface Callback {
        void onEvent(int evt);
    }

    private static class SubscriptionHolder {
        private static final ObjectFactory<SubscriptionHolder> FACTORY = SubscriptionHolder::new;

        private int type = 0;
        private JournalKey remote;
        private JournalKey local;
        private JournalListener listener;
        private JournalWriter writer;
    }

    private final class Handler extends Thread {

        public boolean isRunning() {
            long cursor = subscriptionSubSequence.next();
            if (cursor < 0) {
                return true;
            }

            long available = subscriptionSubSequence.available();
            while (cursor < available) {
                SubscriptionHolder holder = subscriptionQueue.get(cursor++);
                if (holder.type == MSG_HALT) {
                    return false;
                }
            }

            return true;
        }

        public boolean processSubscriptionQueue() {
            long cursor = subscriptionSubSequence.next();
            if (cursor < 0) {
                return true;
            }

            long available = subscriptionSubSequence.available();

            int i = writers.size();

            while (cursor < available) {

                SubscriptionHolder holder = subscriptionQueue.get(cursor++);

                switch (holder.type) {
                    case MSG_SUBSCRIBE:
                        String name = holder.local.getName();

                        if (subscribedJournals.add(name)) {
                            subscribeOne(i++, holder, name, true);
                        } else {
                            if (holder.listener != null) {
                                holder.listener.onEvent(JournalEvents.EVT_JNL_ALREADY_SUBSCRIBED);
                            }
                            LOG.error().$("Already subscribed ").$(name).$();
                        }
                        break;
                    case MSG_UNSUBSCRIBE:
                        break;
                    case MSG_HALT:
                        return false;
                    default:
                        LOG.error().$("Ignored unknown message: ").$(holder.type).$();
                        break;
                }
            }

            subscriptionSubSequence.done(available - 1);
            return true;
        }

        @Override
        public void run() {

            running = true;
            notifyCallback(JournalClientEvents.EVT_RUNNING);
            int event = JournalClientEvents.EVT_NONE;
            boolean connected = false;

            try {
                while (true) {
                    // reconnect code
                    if (!connected) {
                        int retryCount = config.getReconnectPolicy().getRetryCount();
                        int loginRetryCount = config.getReconnectPolicy().getLoginRetryCount();
                        do {
                            try {
                                closeChannel();

                                // if we cannot connect - move on to retry
                                try {
                                    openChannel();
                                    counter.incrementAndGet();
                                } catch (JournalNetworkException e) {
                                    if (retryCount-- > 0) {
                                        continue;
                                    } else {
                                        break;
                                    }
                                }

                                sendProtocolVersion();
                                checkAuthAndSendCredential();
                                resubscribe();
                                sendReady();
                                connected = true;
                                notifyCallback(JournalClientEvents.EVT_CONNECTED);
                            } catch (UnauthorizedException e) {
                                notifyCallback(JournalClientEvents.EVT_AUTH_ERROR);
                                loginRetryCount--;
                            } catch (AuthenticationConfigException | AuthenticationProviderException e) {
                                closeChannel();
                                close0();
                                notifyCallback(JournalClientEvents.EVT_AUTH_CONFIG_ERROR);
                                return;
                            } catch (JournalNetworkException e) {
                                LOG.info().$(e.getMessage()).$();
                                closeChannel();
                            }

                            if (!connected && retryCount-- > 0 && loginRetryCount > 0) {
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(config.getReconnectPolicy().getSleepBetweenRetriesMillis()));
                                LOG.info().$("Retrying reconnect ... [").$(retryCount + 1).$(']').$();
                            } else {
                                break;
                            }
                        } while (true);

                        if (!connected && (retryCount == 0 || loginRetryCount == 0)) {
                            event = JournalClientEvents.EVT_SERVER_ERROR;
                        }
                    }


                    // protocol code

                    try {
                        if (connected && channel.isOpen() && isRunning()) {
                            commandConsumer.read(channel);
                            byte cmd = commandConsumer.getCommand();
                            switch (cmd) {
                                case Command.JOURNAL_DELTA_CMD:
                                    statsChannel.setDelegate(channel);
                                    int index = intResponseConsumer.getValue(statsChannel);
                                    deltaConsumers.getQuick(index).read(statsChannel);
                                    statusSentList.set(index, 0);
                                    statsChannel.logStats();
                                    break;
                                case Command.SERVER_READY_CMD:
                                    sendState();
                                    sendReady();
                                    break;
                                case Command.SERVER_HEARTBEAT:
                                    if (processSubscriptionQueue()) {
                                        sendReady();
                                    } else {
                                        event = JournalClientEvents.EVT_CLIENT_HALT;
                                    }
                                    break;
                                case Command.SERVER_SHUTDOWN:
                                    connected = false;
                                    break;
                                default:
                                    LOG.info().$("Unknown command: ").$(cmd).$();
                                    break;
                            }
                        } else if (event == JournalClientEvents.EVT_NONE) {
                            event = JournalClientEvents.EVT_CLIENT_HALT;
                        }
                    } catch (IncompatibleJournalException e) {
                        // unsubscribe journal
                        LOG.error().$(e.getMessage()).$();
                        event = JournalClientEvents.EVT_INCOMPATIBLE_JOURNAL;
                    } catch (JournalNetworkException e) {
                        LOG.error().$("Network error. Server died?").$();
                        LOG.debug().$("Network error details: ").$(e).$();
                        notifyCallback(JournalClientEvents.EVT_SERVER_DIED);
                        connected = false;
                    } catch (Throwable e) {
                        LOG.error().$("Unhandled exception in client").$(e).$();
                        event = JournalClientEvents.EVT_CLIENT_EXCEPTION;
                    }

                    if (event != JournalClientEvents.EVT_NONE) {
                        // client gracefully disconnects
                        if (channel != null && channel.isOpen()) {
                            sendDisconnect();
                        }
                        closeChannel();
                        close0();
                        notifyCallback(event);
                        break;
                    }
                }
            } catch (Throwable e) {
                LOG.error().$("Fatal exception when closing client").$(e).$();
                closeChannel();
                close0();
            } finally {
                running = false;
                notifyCallback(JournalClientEvents.EVT_TERMINATED);
                haltLatch.countDown();
                LOG.info().$("Terminated").$();
            }
        }
    }
}
