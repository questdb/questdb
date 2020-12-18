package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.Path;

public class SlaveReplicationService {
    private static final Log LOG = LogFactory.getLog(SlaveReplicationService.class);
    private static final AtomicLong NEXT_PEER_ID = new AtomicLong();
    private final CairoConfiguration configuration;
    private final NetworkFacade nf;
    private final FilesFacade ff;
    private final CairoEngine engine;
    private final CharSequence root;
    private final ReplicationSlaveConnectionMultiplexer slaveConnectionMux;
    private final SequencedQueue<SlaveReplicationInstructionEvent> instructionQueue;

    public SlaveReplicationService(CairoConfiguration configuration, NetworkFacade nf, CairoEngine engine, WorkerPool workerPool) {
        this.configuration = configuration;
        this.nf = nf;
        this.ff = configuration.getFilesFacade();
        this.root = configuration.getRoot();
        this.engine = engine;
        int instructionQueueLen = 2;
        int connectionCallbackQueueLen = 8;
        int newConnectionQueueLen = 2;
        ReplicationSlaveControllerJob job = new ReplicationSlaveControllerJob();
        slaveConnectionMux = new ReplicationSlaveConnectionMultiplexer(nf, workerPool, connectionCallbackQueueLen, newConnectionQueueLen, job::handleReplicatingPeerDiconnection);
        workerPool.assign(job);
        workerPool.assign(0, job::close);
        instructionQueue = SequencedQueue.createMultipleProducerSingleConsumerQueue(instructionQueueLen, SlaveReplicationInstructionEvent::new);
    }

    public boolean tryAdd(SlaveReplicationConfiguration replicationConf) {
        long seq;
        do {
            seq = instructionQueue.getProducerSeq().next();
            if (seq >= 0) {
                try {
                    SlaveReplicationInstructionEvent event = instructionQueue.getEvent(seq);
                    event.assignAddEvent(replicationConf);
                    return true;
                } finally {
                    instructionQueue.getProducerSeq().done(seq);
                }
            }
        } while (seq == -2);
        return false;
    }

    public boolean tryRemove(CharSequence tableName) {
        long seq;
        do {
            seq = instructionQueue.getProducerSeq().next();
            if (seq >= 0) {
                try {
                    SlaveReplicationInstructionEvent event = instructionQueue.getEvent(seq);
                    event.assignRemoveEvent(tableName);
                    return true;
                } finally {
                    instructionQueue.getProducerSeq().done(seq);
                }
            }
        } while (seq == -2);
        return false;
    }

    public static class SlaveReplicationConfiguration {
        private CharSequence tableName;
        private ObjList<CharSequence> masterIps;
        private IntList masterPorts;

        public SlaveReplicationConfiguration(CharSequence tableName, ObjList<CharSequence> masterIps, IntList masterPorts) {
            super();
            this.tableName = tableName;
            this.masterIps = masterIps;
            this.masterPorts = masterPorts;
        }
    }

    private static class SlaveReplicationInstructionEvent {
        private static final byte ADD_EVENT_TYPE = 1;
        private static final byte REMOVE_EVENT_TYPE = 2;
        private byte eventType;
        private SlaveReplicationConfiguration replicationConf;
        private CharSequence tableName;

        private void assignAddEvent(SlaveReplicationConfiguration replicationConf) {
            eventType = ADD_EVENT_TYPE;
            this.replicationConf = replicationConf;
        }

        private void assignRemoveEvent(CharSequence tableName) {
            eventType = REMOVE_EVENT_TYPE;
            this.tableName = tableName;
        }
    }

    private class ReplicationSlaveControllerJob extends SynchronizedJob {
        private Path path = new Path();
        private final DirectCharSequence charSeq = new DirectCharSequence();
        private final CharSequenceHashSet replicatedTableNames = new CharSequenceHashSet();
        private final ObjList<SlaveReplicationHandler> initialisingHandlers = new ObjList<SlaveReplicationHandler>();
        private final ObjList<SlaveReplicationHandler> streamingHandlers = new ObjList<SlaveReplicationHandler>();
        private final ObjList<SlaveReplicationHandler> distressedHandlers = new ObjList<SlaveReplicationHandler>();
        private final LongHashSet stoppingPeerIds = new LongHashSet();

        @Override
        protected boolean runSerially() {
            boolean busy = false;
            if (handleInstructionTasks()) {
                busy = true;
            }
            if (handleInitialisingConnections()) {
                busy = true;
            }
            // TODO need to handle distressed
            return busy;
        }

        private boolean handleInstructionTasks() {
            boolean busy = false;
            long seq;
            do {
                while ((seq = instructionQueue.getConsumerSeq().next()) >= 0) {
                    try {
                        busy = false;
                        SlaveReplicationInstructionEvent event = instructionQueue.getEvent(seq);
                        switch (event.eventType) {
                            case SlaveReplicationInstructionEvent.ADD_EVENT_TYPE: {
                                SlaveReplicationConfiguration addReplicationConf = event.replicationConf;
                                if (!replicatedTableNames.contains(addReplicationConf.tableName)) {
                                    SlaveReplicationHandler handler = new SlaveReplicationHandler(configuration);
                                    handler.of(addReplicationConf);
                                    initialisingHandlers.add(handler);
                                    replicatedTableNames.add(addReplicationConf.tableName);
                                    LOG.info().$("starting replication [tableName=").$(addReplicationConf.tableName).$(']').$();
                                    handler.start();
                                } else {
                                    LOG.error().$("ignoring duplicate add replication instruction [tableName=").$(addReplicationConf.tableName).$(']').$();
                                }
                            }
                                break;

                            case SlaveReplicationInstructionEvent.REMOVE_EVENT_TYPE: {
                                CharSequence tableName = event.tableName;
                                if (replicatedTableNames.contains(tableName)) {
                                    LOG.info().$("ending replication [tableName=").$(tableName).$(']').$();
                                    replicatedTableNames.remove(tableName);
                                    for (int n = 0, sz = initialisingHandlers.size(); n < sz; n++) {
                                        SlaveReplicationHandler handler = initialisingHandlers.get(n);
                                        if (handler.replicationConf.tableName.equals(tableName)) {
                                            stoppingPeerIds.add(handler.peerId);
                                            handler.stop();
                                            break;
                                        }
                                    }
                                    for (int n = 0, sz = streamingHandlers.size(); n < sz; n++) {
                                        SlaveReplicationHandler handler = streamingHandlers.get(n);
                                        if (handler.replicationConf.tableName.equals(tableName)) {
                                            stoppingPeerIds.add(handler.peerId);
                                            handler.stop();
                                            break;
                                        }
                                    }
                                } else {
                                    LOG.error().$("ignoring remove replication instruction [tableName=").$(tableName).$(']').$();
                                }
                            }
                                break;
                        }
                    } finally {
                        instructionQueue.getConsumerSeq().done(seq);
                    }
                }
            } while (seq == -2);
            return busy;
        }

        private boolean handleInitialisingConnections() {
            boolean busy = false;
            for (int n = 0, sz = initialisingHandlers.size(); n < sz; n++) {
                SlaveReplicationHandler handler = initialisingHandlers.get(n);
                if (handler.handleConnections()) {
                    busy = true;
                }
            }
            return busy;
        }

        private void handleReplicatingPeerDiconnection(long peerId, long fd) {
            for (int n = 0, sz = streamingHandlers.size(); n < sz; n++) {
                SlaveReplicationHandler handler = streamingHandlers.get(n);
                if (handler.peerId == peerId) {
                    handler.handleStreamingDisconnection(fd);
                }
            }
        }

        private void handleStopped(SlaveReplicationHandler stoppedHandler) {
            initialisingHandlers.remove(stoppedHandler);
            streamingHandlers.remove(stoppedHandler);
            if (stoppingPeerIds.remove(stoppedHandler.peerId) == -1) {
                distressedHandlers.add(stoppedHandler);
                LOG.info().$("replication distressed [tableName=").$(stoppedHandler.getTableName()).$(", peerId=").$(stoppedHandler.peerId).$(']').$();
            } else {
                // TODO recycle handlers
                stoppedHandler.clear();
                LOG.info().$("replication stopped [tableName=").$(stoppedHandler.getTableName()).$(", peerId=").$(stoppedHandler.peerId).$(']').$();
            }
        }

        public void close() {
            if (null != path) {
                Misc.freeObjList(initialisingHandlers);
                Misc.freeObjList(streamingHandlers);
                Misc.freeObjList(distressedHandlers);
                slaveConnectionMux.close();
                path.close();
                path = null;
            }
        }

        private class SlaveReplicationHandler implements Closeable {
            private final SlaveWriterImpl slaveWriter;
            private ObjList<InitialSlaveConnection> connections = new ObjList<>();
            private SlaveReplicationConfiguration replicationConf;
            private long peerId;
            private int nReady;
            private TableWriter writer;
            private int masterTableId;
            private boolean streaming;
            private int nConnectionsToEnqueue;
            private boolean stopping;
            private boolean stopped;

            private SlaveReplicationHandler(CairoConfiguration configuration) {
                super();
                slaveWriter = new SlaveWriterImpl(configuration);
            }

            SlaveReplicationHandler of(SlaveReplicationConfiguration replicationConf) {
                this.replicationConf = replicationConf;
                peerId = NEXT_PEER_ID.incrementAndGet();
                nReady = 0;
                writer = null;
                streaming = false;
                masterTableId = 0;
                stopping = false;
                stopped = false;
                return this;
            }

            private void start() {
                assert connections.size() == 0;
                for (int n = 0, sz = replicationConf.masterIps.size(); n < sz; n++) {
                    InitialSlaveConnection connection = new InitialSlaveConnection(nf);
                    connection.of(replicationConf.masterIps.get(n), replicationConf.masterPorts.get(n));
                    connections.add(connection);
                }
            }

            private boolean handleConnections() {
                boolean busy = false;
                if (!stopping) {
                    if (streaming) {
                        return handleTranstionToStreaming();
                    }

                    int sz = nReady == connections.size() ? 1 : connections.size();
                    for (int n = 0; n < sz; n++) {
                        InitialSlaveConnection connection = connections.get(n);
                        if (connection.handleIO()) {
                            if (connection.isDisconnected()) {
                                LOG.info().$("disconnected [peerId=").$(peerId).$(", fd=").$(connection.fd).$(']').$();
                                handleFailure();
                                return true;
                            }
                            busy = true;
                        }
                    }
                } else {
                    return handleStopping();
                }
                return busy;
            }

            private CharSequence getTableName() {
                return replicationConf.tableName;
            }

            private void handleFailure() {
                handleStopping();
            }

            private boolean handleStopping() {
                if (!stopped) {
                    if (!stopping) {
                        LOG.info().$("stopping replication [tableName=").$(replicationConf.tableName).$(", masterTableId=").$(masterTableId).$(']').$();
                        stopping = true;
                    }
                    if (streaming) {
                        if (!slaveConnectionMux.tryStopPeer(peerId)) {
                            return true;
                        }
                    } else {
                        while (connections.size() > 0) {
                            InitialSlaveConnection connection = connections.get(0);
                            connection.close();
                            connections.remove(0);
                        }
                    }

                    if (connections.size() == 0) {
                        stopped = true;
                        handleStopped(this);
                    }
                }
                return false;
            }

            private void stop() {
                handleStopping();
            }

            private void clear() {
                slaveWriter.clear();
                if (null != writer) {
                    writer.close();
                    writer = null;
                }
                Misc.freeObjList(connections);
                connections.clear();
            }

            private void handleTableInfo(int masterTableId, long masterRowCount, long masterTableStructureVersion) {
                nReady++;
                if (nReady == connections.size()) {
                    if (0 == masterTableId) {
                        // TODO No table on master
                        throw new RuntimeException("Not implemented");
                    }

                    LOG.info().$("all connections ready [tableName=").$(replicationConf.tableName).$(", masterTableId=").$(masterTableId).$(", rowCount=").$(masterRowCount)
                            .$(", tableStructureVersion=").$(masterTableStructureVersion).$(']').$();
                    if (engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, getTableName()) == TableUtils.TABLE_EXISTS) {
                        writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, getTableName());
                        long localTableStructureVersion = writer.getStructureVersion();
                        if (localTableStructureVersion == masterTableStructureVersion) {
                            long localRowCount = writer.size();
                            connections.get(0).setupRequestReplicationStream(masterTableId, masterTableStructureVersion, localRowCount);
                            return;
                        } else {
                            // TODO No table on slave
                            throw new RuntimeException("Tables dont match");
                        }
                    } else {
                        // TODO No table on slave
                        throw new RuntimeException("Not implemented");
                    }
                }
            }

            private boolean handleTranstionToStreaming() {
                if (nConnectionsToEnqueue == 0) {
                    if (!slaveConnectionMux.tryAddSlaveWriter(masterTableId, slaveWriter)) {
                        return true;
                    }
                    nConnectionsToEnqueue = connections.size();
                }

                while (nConnectionsToEnqueue > 0) {
                    int n = connections.size() - nConnectionsToEnqueue;
                    InitialSlaveConnection connection = connections.get(n);
                    if (!slaveConnectionMux.tryAddConnection(peerId, connection.fd)) {
                        return true;
                    }
                    nConnectionsToEnqueue--;
                }

                initialisingHandlers.remove(this);
                streamingHandlers.add(this);

                return false;
            }

            private void handleStartOfReplicationStream(int masterTableId) {
                assert !streaming;
                LOG.info().$("starting streaming [tableName=").$(getTableName()).$(", masterTableId=").$(masterTableId).$(']').$();
                streaming = true;
                this.masterTableId = masterTableId;
                nConnectionsToEnqueue = 0;
                slaveWriter.of(writer);
                handleTranstionToStreaming();
            }

            private void handleStreamingDisconnection(long fd) {
                for (int n = 0, sz = connections.size(); n < sz; n++) {
                    InitialSlaveConnection connection = connections.get(n);
                    if (connection.fd == fd) {
                        connection.close();
                        connections.remove(n);
                        handleStopping();
                        return;
                    }
                }
            }

            @Override
            public void close() {
                if (null != connections) {
                    clear();
                    slaveWriter.close();
                    connections = null;
                }
            }

            private class InitialSlaveConnection extends AbstractFramedConnection implements Closeable {
                private CharSequence inetAddress;
                private int inetPort;
                private boolean connected;
                private int masterTableId;

                protected InitialSlaveConnection(NetworkFacade nf) {
                    super(nf);
                }

                private InitialSlaveConnection of(CharSequence inetAddress, int inetPort) {
                    this.inetAddress = inetAddress;
                    this.inetPort = inetPort;
                    masterTableId = 0;
                    if (!init(-1)) {
                        LOG.error().$("failed to allocate buffer").$();
                        return null;
                    }
                    connected = false;
                    return this;
                }

                @Override
                protected boolean handleIO() {
                    if (!connected) {
                        if (!connect()) {
                            handleFailure();
                            return true;
                        }
                        return false;
                    }

                    return super.handleIO();
                }

                private boolean connect() {
                    if (fd == -1) {
                        fd = nf.socketTcp(false);
                    }
                    long sockaddr = nf.sockaddr(inetAddress, inetPort);
                    long rc = nf.connect(fd, sockaddr);
                    nf.freeSockAddr(sockaddr);
                    if (rc != 0) {
                        int errno = nf.errno();
                        if (errno != Net.EISCONN) {
                            if (errno == Net.EINPROGRESS || errno == Net.EALREADY || errno == Net.EWOULDBLOCK) {
                                // POSIX (linux, FreeBSD, Darwin) specifies that non blocking calls to connect can return EINPROGRESS
                                // (initial
                                // call) or EALREADY (subsequent calls)
                                // Windows has to be different and so returns EWOULDBLOCK or EALREADY
                                return true;
                            }
                            LOG.error().$("failed to connect tcp socket [tableName=").$(replicationConf.tableName).$(", fd=").$(fd).$(", ipAddress=").$(", ipAddress=").$(inetAddress)
                                    .$(", port=").$(inetPort).$(", errno=").$(errno).$(']').$();
                            return false;
                        }
                    }
                    if (nf.setTcpNoDelay(fd, true) != 0) {
                        LOG.info().$("failed to set TCP no delay [tableName=").$(replicationConf.tableName).$(", fd=").$(fd).$(", ipAddress=").$(", ipAddress=").$(inetAddress).$(", port=")
                                .$(inetPort).$(']').$();
                    }
                    setupRequestTableInfo();
                    connected = true;
                    LOG.info().$("connected [tableName=").$(replicationConf.tableName).$(", fd=").$(fd).$(", ipAddress=").$(", ipAddress=").$(inetAddress).$(", port=")
                            .$(inetPort).$(']').$();
                    return true;
                }

                private void setupRequestTableInfo() {
                    int frameLen = TableReplicationStreamHeaderSupport.RTI_HEADER_SIZE + getTableName().length() * 2;
                    Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RTI_PROTOCOL_VERSION, TableReplicationStreamHeaderSupport.PROTOCOL_VERSION);
                    resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_REQUEST_TABLE_INFO, frameLen);
                    long p = bufferAddress + TableReplicationStreamHeaderSupport.RTI_HEADER_SIZE;
                    for (int n = 0, sz = getTableName().length(); n < sz; n++) {
                        Unsafe.getUnsafe().putChar(p, getTableName().charAt(n));
                        p += Character.BYTES;
                    }
                }

                private void setupRequestReplicationStream(int masterTableId, long tableStructureVersion, long initialRowCount) {
                    int frameLen = TableReplicationStreamHeaderSupport.RRS_HEADER_SIZE;
                    resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_REQUEST_REPLICATION_STREAM, frameLen);
                    Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, masterTableId);
                    Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RRS_TABLE_STRUCTURE_VERSION, tableStructureVersion);
                    Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RRS_INITIAL_ROW_COUNT, initialRowCount);
                }

                @Override
                protected boolean handleFrame(byte frameType) {
                    switch (frameType) {
                        case TableReplicationStreamHeaderSupport.FRAME_TYPE_TABLE_INFO:
                            handleTableInfo();
                            return true;
                        case TableReplicationStreamHeaderSupport.FRAME_TYPE_START_OF_REPLICATION_STREAM:
                            handleStartOfReplicationStream();
                            return true;
                    }
                    return false;
                }

                private void handleTableInfo() {
                    if (this.masterTableId != 0) {
                        LOG.info().$("received duplicate table info frame [fd=").$(fd).$(", tableName=").$(getTableName()).$(masterTableId).$(']').$();
                        handleFailure();
                        return;
                    }

                    long tableNameLo = bufferAddress + TableReplicationStreamHeaderSupport.TI_HEADER_SIZE;
                    long tableNameHi = bufferAddress + bufferLen;
                    CharSequence frameTableName = charSeq.of(tableNameLo, tableNameHi);
                    masterTableId = Unsafe.getUnsafe().getInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                    long rowCount = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_TI_TABLE_ROW_COUNT);
                    long tableStructureVersion = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_TI_TABLE_STRUCTURE_VERSION);
                    if (frameTableName.equals(getTableName())) {
                        if (masterTableId != 0) {
                            LOG.info().$("received table info [fd=").$(fd).$(", tableName=").$(getTableName()).$(", masterTableId=").$(masterTableId).$(", rowCount=").$(rowCount)
                                    .$(", tableStructureVersion=").$(tableStructureVersion).$(']').$();
                        } else {
                            LOG.info().$("received table missing [fd=").$(fd).$(", tableName=").$(getTableName()).$(masterTableId).$(']').$();
                            handleFailure();
                            return;
                        }
                    } else {
                        LOG.info().$("received frame for incorrect table [fd=").$(fd).$(", tableName=").$(getTableName()).$(masterTableId).$(']').$();
                        handleFailure();
                        return;
                    }
                    resetReading();
                    SlaveReplicationHandler.this.handleTableInfo(masterTableId, rowCount, tableStructureVersion);
                }

                private void handleStartOfReplicationStream() {
                    // TODO: Validate response
                    SlaveReplicationHandler.this.handleStartOfReplicationStream(masterTableId);
                }
            }
        }
    }
}
