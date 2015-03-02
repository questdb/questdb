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
import com.nfsdb.JournalKey;
import com.nfsdb.collections.DirectIntList;
import com.nfsdb.collections.IntIntHashMap;
import com.nfsdb.exceptions.ClusterLossException;
import com.nfsdb.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.auth.AuthorizationHandler;
import com.nfsdb.ha.bridge.JournalEvent;
import com.nfsdb.ha.bridge.JournalEventHandler;
import com.nfsdb.ha.bridge.JournalEventProcessor;
import com.nfsdb.ha.comsumer.JournalClientStateConsumer;
import com.nfsdb.ha.config.ServerConfig;
import com.nfsdb.ha.model.Command;
import com.nfsdb.ha.model.IndexedJournalKey;
import com.nfsdb.ha.model.JournalClientState;
import com.nfsdb.ha.producer.JournalDeltaProducer;
import com.nfsdb.ha.protocol.CommandConsumer;
import com.nfsdb.ha.protocol.CommandProducer;
import com.nfsdb.ha.protocol.Version;
import com.nfsdb.ha.protocol.commands.*;
import com.nfsdb.utils.Lists;

import java.net.SocketAddress;
import java.nio.channels.ByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class JournalServerAgent {

    private static final byte JOURNAL_INDEX_NOT_FOUND = -1;
    private final IntIntHashMap writerToReaderMap = new IntIntHashMap();
    private final DirectIntList readerToWriterMap = new DirectIntList();
    private final JournalServer server;
    private final CommandConsumer commandConsumer = new CommandConsumer();
    private final CommandProducer commandProducer = new CommandProducer();
    private final SetKeyRequestConsumer setKeyRequestConsumer = new SetKeyRequestConsumer();
    private final StringResponseProducer stringResponseProducer = new StringResponseProducer();
    private final JournalClientStateConsumer journalClientStateConsumer = new JournalClientStateConsumer();
    private final IntResponseProducer intResponseProducer = new IntResponseProducer();
    private final IntResponseConsumer intResponseConsumer = new IntResponseConsumer();
    private final List<Journal> readers = new ArrayList<>();
    private final List<JournalDeltaProducer> producers = new ArrayList<>();
    private final List<JournalClientState> clientStates = new ArrayList<>();
    private final StatsCollectingWritableByteChannel statsChannel;
    private final JournalEventProcessor eventProcessor;
    private final EventHandler handler = new EventHandler();
    private final AuthorizationHandler authorizationHandler;
    private final ByteArrayResponseConsumer byteArrayResponseConsumer = new ByteArrayResponseConsumer();
    private final SocketAddress socketAddress;
    private boolean authorized;

    public JournalServerAgent(JournalServer server, SocketAddress socketAddress, AuthorizationHandler authorizationHandler) {
        this.server = server;
        this.socketAddress = socketAddress;
        this.statsChannel = new StatsCollectingWritableByteChannel(socketAddress);
        this.eventProcessor = new JournalEventProcessor(server.getBridge());
        this.authorizationHandler = authorizationHandler;
        this.authorized = authorizationHandler == null;
        readerToWriterMap.zero(JOURNAL_INDEX_NOT_FOUND);
    }

    public void close() {
        server.getBridge().removeAgentSequence(eventProcessor.getSequence());
        readerToWriterMap.free();
        journalClientStateConsumer.free();
        commandConsumer.free();
        setKeyRequestConsumer.free();
        intResponseConsumer.free();
        byteArrayResponseConsumer.free();
    }

    public void process(ByteChannel channel) throws JournalNetworkException {
        commandConsumer.read(channel);
        if (commandConsumer.isComplete()) {
            switch (commandConsumer.getValue()) {
                case CLUSTER_VOTE:
                    checkAuthorized(channel);
                    intResponseConsumer.read(channel);
                    if (intResponseConsumer.isComplete()) {
                        int inst = intResponseConsumer.getValue();
                        boolean loss = !server.isAlpha() && inst > server.getServerInstance();
                        intResponseConsumer.reset();
                        commandConsumer.reset();

                        if (loss) {
                            ok(channel);
                            throw new ClusterLossException(inst);
                        } else {
                            error(channel, server.isAlpha() ? "WIN" : "OUT");
                        }
                    }
                    break;
                case SET_KEY_CMD:
                    server.getLogger().msg()
                            .setLevel(ServerLogMsg.Level.TRACE)
                            .setSocketAddress(socketAddress)
                            .setMessage("SetKey command received")
                            .send();
                    setKeyRequestConsumer.read(channel);
                    if (setKeyRequestConsumer.isComplete()) {
                        setClientKey(channel, setKeyRequestConsumer.getValue());
                        setKeyRequestConsumer.reset();
                        commandConsumer.reset();
                    }
                    break;
                case DELTA_REQUEST_CMD:
                    checkAuthorized(channel);
                    server.getLogger().msg()
                            .setLevel(ServerLogMsg.Level.TRACE)
                            .setSocketAddress(socketAddress)
                            .setMessage("DeltaRequest command received")
                            .send();
                    journalClientStateConsumer.read(channel);
                    if (journalClientStateConsumer.isComplete()) {
                        storeDeltaRequest(channel, journalClientStateConsumer.getValue());
                        journalClientStateConsumer.reset();
                        commandConsumer.reset();
                    }
                    break;
                case CLIENT_READY_CMD:
                    checkAuthorized(channel);
                    statsChannel.setDelegate(channel);
                    dispatch(statsChannel);
                    statsChannel.logStats();
                    commandConsumer.reset();
                    break;
                case CLIENT_DISCONNECT:
                    throw new JournalDisconnectedChannelException();
                case PROTOCOL_VERSION:
                    intResponseConsumer.read(channel);
                    if (intResponseConsumer.isComplete()) {
                        checkProtocolVersion(channel, intResponseConsumer.getValue());
                        intResponseConsumer.reset();
                        commandConsumer.reset();
                    }
                    break;
                case HANDSHAKE_COMPLETE:
                    if (authorized) {
                        ok(channel);
                    } else {
                        stringResponseProducer.write(channel, "AUTH");
                    }
                    commandConsumer.reset();
                    break;
                case AUTHORIZATION:
                    byteArrayResponseConsumer.read(channel);
                    if (byteArrayResponseConsumer.isComplete()) {
                        authorize(channel, byteArrayResponseConsumer.getValue());
                    }
                    byteArrayResponseConsumer.reset();
                    commandConsumer.reset();
                    break;
                default:
                    throw new JournalNetworkException("Corrupt channel");
            }
        }
    }

    private void authorize(WritableByteChannel channel, byte[] value) throws JournalNetworkException {
        if (!authorized) {
            try {
                ArrayList<JournalKey> keys = new ArrayList<>(readers.size());
                for (int i = 0, sz = readers.size(); i < sz; i++) {
                    keys.add(readers.get(i).getKey());
                }
                authorized = authorizationHandler.isAuthorized(value, keys);
            } catch (Throwable e) {
                server.getLogger().msg()
                        .setLevel(ServerLogMsg.Level.ERROR)
                        .setSocketAddress(socketAddress)
                        .setMessage("Exception in authorization handler:")
                        .setException(e)
                        .send();
                authorized = false;
            }
        }

        if (authorized) {
            ok(channel);
        } else {
            error(channel, "Authorization failed");
        }
    }

    private void checkAuthorized(WritableByteChannel channel) throws JournalNetworkException {
        if (!authorized) {
            error(channel, "NOT AUTHORIZED");
            throw new JournalDisconnectedChannelException();
        }
    }


    private void checkProtocolVersion(ByteChannel channel, int version) throws JournalNetworkException {
        if (version == Version.PROTOCOL_VERSION) {
            ok(channel);
        } else {
            error(channel, "Unsupported protocol version. Client: " + version + ", Server: " + Version.PROTOCOL_VERSION);
        }
    }

    private <T> void createReader(int index, JournalKey<T> key) throws JournalException {

        Lists.advance(readers, index);

        Journal<?> journal = readers.get(index);
        if (journal == null) {
            journal = server.getFactory().reader(key);
            readers.set(index, journal);
        }

        Lists.advance(producers, index);

        JournalDeltaProducer producer = producers.get(index);
        if (producer == null) {
            producer = new JournalDeltaProducer(journal);
            producers.set(index, producer);
        }
    }

    private void dispatch(WritableByteChannel channel) throws JournalNetworkException {
        if (!processJournalEvents(channel, false)) {
            processJournalEvents(channel, true);
        }
    }

    private boolean dispatch0(WritableByteChannel channel, int journalIndex) {
        long time = System.currentTimeMillis();
        JournalClientState state = clientStates.get(journalIndex);

        // x1 is clientStateValid
        // x2 is writerUpdateReceived
        // x3 is clientStateSynchronised
        // 1 is true
        // 0 is false
        //
        // x1 = 1 && x3 = 0 -> send (brand new, unvalidated request)
        // x1 = 0 - don't send (not client state, don't know what to send)
        // x1 = 1 && x2 = 1 ->  send (either new request, or previously validated but not sent with and update)

        if (state == null || state.isClientStateInvalid() ||
                (state.isWaitingOnEvents() && time - state.getClientStateSyncTime() <= ServerConfig.SYNC_TIMEOUT)) {
            return false;
        }


        try {
            boolean dataSent = dispatchProducer(channel, state.getTxn(), state.getTxPin(), getProducer(journalIndex), journalIndex);
            if (dataSent) {
                state.invalidateClientState();
            } else {
                state.setClientStateSyncTime(time);
            }
            return dataSent;
        } catch (Exception e) {
            server.getLogger().msg()
                    .setLevel(ServerLogMsg.Level.ERROR)
                    .setSocketAddress(socketAddress)
                    .setMessage("Client appears to be refusing new data from server, corrupt client")
                    .setException(e)
                    .send();
            return false;
        }
    }

    private boolean dispatchProducer(
            WritableByteChannel channel
            , long txn
            , long txPin
            , JournalDeltaProducer journalDeltaProducer
            , int index) throws JournalNetworkException, JournalException {

        journalDeltaProducer.configure(txn, txPin);
        if (journalDeltaProducer.hasContent()) {
            server.getLogger().msg().setMessage("Sending data").setSocketAddress(socketAddress).send();
            commandProducer.write(channel, Command.JOURNAL_DELTA_CMD);
            intResponseProducer.write(channel, index);
            journalDeltaProducer.write(channel);
            return true;
        }
        return false;

    }

    private void error(WritableByteChannel channel, String message) throws JournalNetworkException {
        error(channel, message, null);
    }

    private void error(WritableByteChannel channel, String message, Exception e) throws JournalNetworkException {
        stringResponseProducer.write(channel, message);
        server.getLogger().msg()
                .setLevel(ServerLogMsg.Level.INFO)
                .setSocketAddress(socketAddress)
                .setMessage(message)
                .setException(e)
                .send();
    }

    private JournalDeltaProducer getProducer(int index) {
        return producers.get(index);
    }

    private void ok(WritableByteChannel channel) throws JournalNetworkException {
        stringResponseProducer.write(channel, "OK");
    }

    private boolean processJournalEvents(final WritableByteChannel channel, boolean blocking) throws JournalNetworkException {

        handler.setChannel(channel);
        boolean dataSent = false;
        if (eventProcessor.process(handler, blocking)) {
            dataSent = handler.isDataSent();

            // handler would have dispatched those journals, which received updates
            // this loop does two things:
            // 1. attempts to dispatch0 journals that didn't receive updates, dispatch0 method would check timeout and decide.
            // 2. reset writer update received status
            for (int i = 0, sz = clientStates.size(); i < sz; i++) {
                JournalClientState state = clientStates.get(i);
                if (state.isWaitingOnEvents()) {
                    dataSent = dispatch0(channel, i) || dataSent;
                }
                state.setWaitingOnEvents(true);
            }

            if (dataSent) {
                commandProducer.write(channel, Command.SERVER_READY_CMD);
            } else if (blocking) {
                server.getLogger().msg()
                        .setLevel(ServerLogMsg.Level.INFO)
                        .setSocketAddress(socketAddress)
                        .setMessage("Client appears to be refusing new data from server, corrupt client")
                        .send();
            }
        } else {
            if (server.isRunning()) {
                commandProducer.write(channel, Command.SERVER_HEARTBEAT);
                server.getLogger().msg()
                        .setLevel(ServerLogMsg.Level.TRACE)
                        .setSocketAddress(socketAddress)
                        .setMessage("Heartbeat")
                        .send();
            } else {
                commandProducer.write(channel, Command.SERVER_SHUTDOWN);
            }
        }
        return dataSent;
    }

    @SuppressWarnings("unchecked")
    private void setClientKey(ByteChannel channel, IndexedJournalKey indexedKey) throws JournalNetworkException {
        JournalKey<?> readerKey = indexedKey.getKey();
        IndexedJournalKey augmentedReaderKey = server.getWriterIndex0(readerKey);
        if (augmentedReaderKey == null) {
            error(channel, "Requested key not exported: " + readerKey);
        } else {
            writerToReaderMap.put(augmentedReaderKey.getIndex(), indexedKey.getIndex());
            readerToWriterMap.extendAndSet(indexedKey.getIndex(), augmentedReaderKey.getIndex());
            try {
                createReader(indexedKey.getIndex(), augmentedReaderKey.getKey());
                ok(channel);
            } catch (JournalException e) {
                error(channel, "Could not created reader for key: " + readerKey, e);
            }
        }
    }

    private void storeDeltaRequest(WritableByteChannel channel, JournalClientState request) throws JournalNetworkException {
        int index = request.getJournalIndex();

        if (readerToWriterMap.get(index) == JOURNAL_INDEX_NOT_FOUND) {
            error(channel, "Journal index does not match key request");
        } else {
            Lists.advance(clientStates, index);

            JournalClientState r = clientStates.get(index);
            if (r == null) {
                r = new JournalClientState();
                clientStates.set(index, r);
            }
            request.deepCopy(r);
            r.invalidateClientState();
            r.setClientStateSyncTime(0);
            r.setWaitingOnEvents(true);

            ok(channel);
        }
    }

    private class EventHandler implements JournalEventHandler {

        private WritableByteChannel channel;
        private boolean dataSent = false;

        @Override
        public void handle(JournalEvent event) {
            int journalIndex = writerToReaderMap.get(event.getIndex());
            if (journalIndex != JOURNAL_INDEX_NOT_FOUND) {
                JournalClientState status = clientStates.get(journalIndex);
                if (status != null && status.isWaitingOnEvents()) {
                    status.setWaitingOnEvents(false);
                    dataSent = dispatch0(channel, journalIndex) || dataSent;
                }
            }
        }

        public boolean isDataSent() {
            return dataSent;
        }

        public void setChannel(WritableByteChannel channel) {
            this.channel = channel;
            this.dataSent = false;
        }
    }
}
