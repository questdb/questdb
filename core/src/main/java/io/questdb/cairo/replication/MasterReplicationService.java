package io.questdb.cairo.replication;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.OnePageMemory;
import io.questdb.cairo.TablePageFrameCursor;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReplicationRecordCursorFactory;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.replication.ReplicationMasterConnectionDemultiplexer.ReplicationMasterCallbacks;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorFrame;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorResult;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SynchronizedJob;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.std.str.Path;

public class MasterReplicationService {
    private static final Log LOG = LogFactory.getLog(MasterReplicationService.class);
    private static final AtomicLong NEXT_PEER_ID = new AtomicLong();
    private final NetworkFacade nf;
    private final FilesFacade ff;
    private final CairoEngine engine;
    private final CharSequence root;
    private final ReplicationMasterConnectionDemultiplexer masterConnectionDemux;

    public MasterReplicationService(NetworkFacade nf, FilesFacade ff, CharSequence root, CairoEngine engine, MasterReplicationConfiguration masterReplicationConf, WorkerPool workerPool) {
        this.nf = nf;
        this.ff = ff;
        this.engine = engine;
        this.root = root;

        int sz = masterReplicationConf.masterIps.size();
        LongList listenFds = new LongList(sz);
        for (int n = 0; n < sz; n++) {
            long listenFd = nf.socketTcp(true);
            CharSequence ipv4Address = masterReplicationConf.masterIps.get(n);
            int port = masterReplicationConf.masterPorts.get(n);
            if (!nf.bindTcp(listenFd, ipv4Address, port)) {
                LOG.error().$("replication master failed to bind to ").$(ipv4Address).$(':').$(port).$();
                nf.close(listenFd);
                continue;
            }
            nf.listen(listenFd, masterReplicationConf.backlog);
            listenFds.add(listenFd);
            nf.configureNonBlocking(listenFd);
            LOG.info().$("replication master listening on ").$(ipv4Address).$(':').$(port).$(" [fd=").$(listenFd).$(']').$();
        }
        ReplicationMasterControllerJob job = new ReplicationMasterControllerJob(listenFds);
        // TODO: Dynamic configuration
        int connectionCallbackQueueLen = 8;
        int newConnectionQueueLen = 4;
        int sendFrameQueueLen = 64;
        ReplicationMasterCallbacks callbacks = new ReplicationMasterCallbacks() {
            @Override
            public void onSlaveReadyToCommit(long peerId, int tableId) {
                job.onSlaveReadyToCommit(peerId, tableId);
            }

            @Override
            public void onPeerDisconnected(long peerId, long fd) {
                job.onStreamingPeerDisconnected(peerId, fd);
            }
        };
        masterConnectionDemux = new ReplicationMasterConnectionDemultiplexer(nf, workerPool, connectionCallbackQueueLen,
                newConnectionQueueLen, sendFrameQueueLen, callbacks);
        workerPool.assign(job);
        workerPool.assign(0, job::close);
    }

    public static class MasterReplicationConfiguration {
        private ObjList<CharSequence> masterIps;
        private IntList masterPorts;
        private int backlog;

        public MasterReplicationConfiguration(ObjList<CharSequence> masterIps, IntList masterPorts, int backlog) {
            super();
            this.masterIps = masterIps;
            this.masterPorts = masterPorts;
            this.backlog = backlog;
        }
    }

    private class ReplicationMasterControllerJob extends SynchronizedJob {
        private final SqlExecutionContext sqlExecutionContext;
        private final Path path = new Path();
        private final DirectCharSequence charSeq = new DirectCharSequence();
        private final OnePageMemory tempMem = new OnePageMemory();
        private LongList listenFds;
        private final IntObjHashMap<CharSequence> tableNameById;
        private ObjList<InitialMasterConnection> initialisingConnections;
        private ObjList<StreamingTask> streamingTasks;
        private LongObjHashMap<StreamingTask> streamingTaskByPeerId;
        private ObjList<InitialMasterConnection> connectionCache;
        private boolean busy;

        private ReplicationMasterControllerJob(LongList listenFds) {
            this.listenFds = listenFds;
            sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllCairoSecurityContext.INSTANCE, null, null, -1, null);
            tableNameById = new IntObjHashMap<>();
            initialisingConnections = new ObjList<>();
            streamingTasks = new ObjList<>();
            streamingTaskByPeerId = new LongObjHashMap<>();
            connectionCache = new ObjList<>();
        }

        @Override
        protected boolean runSerially() {
            busy = false;
            if (null != listenFds) {
                acceptConnections();
                handleInitialsingConnections();
                handleStreamingTasks();
                if (masterConnectionDemux.handleTasks()) {
                    busy = true;
                }
            }
            return busy;
        }

        private void acceptConnections() {
            for (int n = 0, sz = listenFds.size(); n < sz; n++) {
                long listenFd = listenFds.get(n);
                long clientFd = nf.accept(listenFd);
                if (clientFd < 0) {
                    if (nf.errno() != Net.EWOULDBLOCK) {
                        LOG.error().$("could not accept [ret=").$(clientFd).$(", errno=").$(nf.errno()).$(']').$();
                    }
                    continue;
                }

                busy = true;
                InitialMasterConnection connection;
                if (connectionCache.size() > 0) {
                    int i = connectionCache.size() - 1;
                    connection = connectionCache.get(i);
                    connectionCache.remove(i);
                } else {
                    connection = new InitialMasterConnection(nf);
                }
                nf.configureNonBlocking(clientFd);
                if (nf.setTcpNoDelay(clientFd, true) != 0) {
                    LOG.info().$("failed to set TCP no delay [fd=").$(clientFd).$(']').$();
                }
                connection.of(clientFd);
                initialisingConnections.add(connection);
                LOG.info().$("peer connected [ip=").$ip(nf.getPeerIP(clientFd)).$(", fd=").$(clientFd).$(']').$();
            }
        }

        private void handleInitialsingConnections() {
            int n = 0;
            int sz = initialisingConnections.size();
            while (n < sz) {
                InitialMasterConnection connection = initialisingConnections.get(n);
                if (connection.handleIO()) {
                    if (connection.isDisconnected()) {
                        initialisingConnections.remove(n);
                        sz--;
                        connection.close();
                        connectionCache.add(connection);
                        continue;
                    }
                }
                n++;
            }
        }

        private void handleStreamingTasks() {
            for (int n = 0, sz = streamingTasks.size(); n < sz; n++) {
                StreamingTask task = streamingTasks.get(n);
                task.handleTask();
            }
        }

        private void onSlaveReadyToCommit(long peerId, int tableId) {
            // TODO check consistency level
            StreamingTask task = streamingTaskByPeerId.get(peerId);
            task.handleSlaveReadyToCommit();
        }

        private void onStreamingPeerDisconnected(long peerId, long fd) {
            // TODO Auto-generated method stub
            throw new RuntimeException();
        }

        public void close() {
            if (null != listenFds) {
                masterConnectionDemux.close();
                Misc.freeObjList(initialisingConnections);
                initialisingConnections.clear();
                Misc.freeObjList(streamingTasks);
                streamingTasks.clear();
                streamingTaskByPeerId.clear();
                for (int n = 0, sz = listenFds.size(); n < sz; n++) {
                    nf.close(listenFds.get(n), LOG);
                }
                Misc.freeObjList(connectionCache);
                connectionCache.clear();
                path.close();
                tempMem.close();
                listenFds = null;
            }
        }

        private class InitialMasterConnection extends AbstractFramedConnection {
            private int tableId;
            private long uuid1;
            private long uuid2;
            private long uuid3;
            private long startingRowCount;

            protected InitialMasterConnection(NetworkFacade nf) {
                super(nf);
            }

            private InitialMasterConnection of(long fd) {
                if (!init(fd)) {
                    LOG.error().$("failed to allocate buffer [fd=").$(fd).$(']').$();
                    close();
                    return this;
                }
                tableId = 0;
                resetReading();
                return this;
            }

            @Override
            protected boolean handleFrame(byte frameType) {
                switch (frameType) {
                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_REQUEST_TABLE_INFO:
                        handleRequestTableInfo();
                        return true;
                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_REQUEST_REPLICATION_STREAM:
                        handleRequestReplicationStream();
                        return true;
                }
                return false;
            }

            private void handleRequestTableInfo() {
                long tableNameLo = bufferAddress + TableReplicationStreamHeaderSupport.RTI_HEADER_SIZE;
                long tableNameHi = bufferAddress + bufferLen;
                CharSequence tableName = charSeq.of(tableNameLo, tableNameHi);
                LOG.info().$("received request for table info [fd=").$(fd).$(", tableName=").$(tableName).$(']').$();
                int protocolVersion = Unsafe.getUnsafe().getInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RTI_PROTOCOL_VERSION);
                if (protocolVersion != TableReplicationStreamHeaderSupport.PROTOCOL_VERSION) {
                    // TODO handle protocol mismatch
                    throw new RuntimeException("Not implemented");
                }
                int masterTableId;
                long rowCount;
                long tableStructureVersion;
                if (engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, tableName) == TableUtils.TABLE_EXISTS) {
                    TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                    rowCount = reader.size();
                    tableStructureVersion = reader.getVersion();
                    masterTableId = reader.getMetadata().getId();
                    reader.close();

                    if (null == tableNameById.get(masterTableId)) {
                        tableNameById.put(masterTableId, tableName.toString());
                    }
                } else {
                    masterTableId = 0;
                    rowCount = Long.MIN_VALUE;
                    tableStructureVersion = Long.MIN_VALUE;
                }
                uuid1 = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RTI_UUID_1);
                uuid2 = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RTI_UUID_2);
                uuid3 = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RTI_UUID_3);

                int frameLen = TableReplicationStreamHeaderSupport.TI_HEADER_SIZE + tableName.length() * 2;
                resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_TABLE_INFO, frameLen);
                Unsafe.getUnsafe().copyMemory(tableNameLo, bufferAddress + TableReplicationStreamHeaderSupport.TI_HEADER_SIZE, tableNameHi - tableNameLo);
                Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, masterTableId);
                Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_TI_TABLE_ROW_COUNT, rowCount);
                Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_TI_TABLE_STRUCTURE_VERSION, tableStructureVersion);
                resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_TABLE_INFO, frameLen);
            }

            private void handleRequestReplicationStream() {
                int masterTableId = Unsafe.getUnsafe().getInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                long requestedTableStructureVersion = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RRS_TABLE_STRUCTURE_VERSION);
                startingRowCount = Unsafe.getUnsafe().getLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_RRS_INITIAL_ROW_COUNT);
                CharSequence tableName = tableNameById.get(masterTableId);
                if (null != tableName && engine.getStatus(AllowAllCairoSecurityContext.INSTANCE, path, tableName) == TableUtils.TABLE_EXISTS) {
                    TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                    try {
                        long tableStructureVersion = reader.getVersion();
                        if (tableStructureVersion != requestedTableStructureVersion) {
                            // TODO Support structure mismatch
                            throw new RuntimeException("Not implemented");
                        }

                        path.of(root).concat(tableName).concat(TableUtils.META_FILE_NAME).$();
                        int metaSize = (int) ff.length(path);
                        tempMem.of(ff, path, ff.getPageSize(), metaSize);

                        int frameLen = TableReplicationStreamHeaderSupport.SORS_HEADER_SIZE + metaSize;
                        resetWriting(TableReplicationStreamHeaderSupport.FRAME_TYPE_START_OF_REPLICATION_STREAM, frameLen);
                        Unsafe.getUnsafe().putInt(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID, masterTableId);
                        Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_SORS_TABLE_STRUCTURE_VERSION, tableStructureVersion);
                        Unsafe.getUnsafe().putLong(bufferAddress + TableReplicationStreamHeaderSupport.OFFSET_SORS_INITIAL_ROW_COUNT, startingRowCount);
                        Unsafe.getUnsafe().copyMemory(tempMem.addressOf(0), bufferAddress + TableReplicationStreamHeaderSupport.SORS_HEADER_SIZE, metaSize);
                        tableId = masterTableId;
                    } finally {
                        reader.close();
                    }
                } else {
                    // TODO invalid request
                    throw new RuntimeException("Not implemented");
                }
            }

            @Override
            protected void onFinishedWriting() {
                if (tableId == 0) {
                    super.onFinishedWriting();
                } else {
                    // TODO recycle StreamingTask
                    StreamingTask streamingTask = new StreamingTask().of(tableId, startingRowCount, uuid1, uuid2, uuid3);
                    streamingTasks.add(streamingTask);
                    streamingTaskByPeerId.put(streamingTask.peerId, streamingTask);
                }
            }
        }

        private class StreamingTask implements Closeable {
            private ReplicationStreamGenerator streamGenerator = new ReplicationStreamGenerator();
            private TableReplicationRecordCursorFactory factory;
            private ObjList<InitialMasterConnection> connections = new ObjList<>();
            private IntList initialSymbolCounts = new IntList();
            private long peerId;
            private int tableId;
            private long nRow;
            private int nConnectionsToEnqueue;
            private TableReader reader;
            private boolean streaming;
            private boolean waitingForReadyToCommit;
            private boolean slaveReadyToCommit;
            private ReplicationStreamGeneratorFrame frame;

            private StreamingTask of(int tableId, long nRow, long uuid1, long uuid2, long uuid3) {
                peerId = NEXT_PEER_ID.incrementAndGet();
                this.tableId = tableId;
                this.nRow = nRow;
                frame = null;
                resetStreaming();
                int n = 0;
                while (n < initialisingConnections.size()) {
                    InitialMasterConnection connection = initialisingConnections.get(n);
                    if (connection.uuid1 == uuid1 && connection.uuid2 == uuid2 && connection.uuid3 == uuid3) {
                        initialisingConnections.remove(n);
                        connections.add(connection);
                    } else {
                        n++;
                    }
                }
                nConnectionsToEnqueue = connections.size();

                CharSequence tableName = tableNameById.get(tableId);
                reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName);
                LOG.info().$("replication streaming starting [peerId=").$(peerId).$(", tableName=").$(tableName).$(", nRow=").$(nRow).$(']').$();

                // TODO initialSymbolCounts need to be passed in
                for (int columnIndex = 0, sz = reader.getMetadata().getColumnCount(); columnIndex < sz; columnIndex++) {
                    if (reader.getMetadata().getColumnType(columnIndex) == ColumnType.SYMBOL) {
                        initialSymbolCounts.add(0);
                    } else {
                        initialSymbolCounts.add(-1);
                    }
                }
                factory = new TableReplicationRecordCursorFactory(engine, tableName, Long.MAX_VALUE);

                return this;
            }

            private void handleTask() {
                if (nConnectionsToEnqueue == 0) {
                    if (!streaming) {
                        if (null != frame) {
                            if (!masterConnectionDemux.tryQueueSendFrame(peerId, frame)) {
                                // Queue are full, consumers are slow
                                return;
                            }
                            frame = null;
                        }

                        reader.readTxn();
                        if (reader.size() > nRow) {
                            TablePageFrameCursor cursor = factory.getPageFrameCursorFrom(sqlExecutionContext, reader.getMetadata().getTimestampIndex(), nRow);
                            nRow += cursor.size();
                            // TODO nConcurrentFrames needs to be set appropriately so that the queues can be filled
                            streamGenerator.of(tableId, connections.size(), cursor, reader.getMetadata(), initialSymbolCounts);
                            streaming = true;
                        }
                    }
                    busy = true;

                    if (!waitingForReadyToCommit) {
                        // Queue frames to send to slave
                        while (true) {
                            if (null == frame) {
                                ReplicationStreamGeneratorResult streamResult = streamGenerator.nextDataFrame();
                                if (null != streamResult) {
                                    if (streamResult.isRetry()) {
                                        // streamGenerator ran out of free frames, possibly increase streamGenerator
                                        return;
                                    }
                                    frame = streamResult.getFrame();
                                } else {
                                    waitingForReadyToCommit = true;
                                    break;
                                }
                            }
                            if (!masterConnectionDemux.tryQueueSendFrame(peerId, frame)) {
                                // Queue are full, consumers are slow
                                return;
                            }
                            frame = null;
                        }
                    }
                    assert waitingForReadyToCommit;

                    if (slaveReadyToCommit) {
                        ReplicationStreamGeneratorResult streamResult = streamGenerator.generateCommitBlockFrame();
                        if (!streamResult.isRetry()) {
                            frame = streamResult.getFrame();
                            resetStreaming();
                        }
                    }
                } else {
                    busy = true;
                    while (nConnectionsToEnqueue > 0) {
                        int n = connections.size() - nConnectionsToEnqueue;
                        InitialMasterConnection connection = connections.get(n);
                        if (!masterConnectionDemux.tryAddConnection(peerId, connection.fd)) {
                            return;
                        }
                        nConnectionsToEnqueue--;
                    }
                }
            }

            private void resetStreaming() {
                streaming = false;
                waitingForReadyToCommit = false;
                slaveReadyToCommit = false;
            }

            private void handleSlaveReadyToCommit() {
                slaveReadyToCommit = true;
            }

            private void clear() {
                if (null != reader) {
                    factory.close();
                    factory = null;
                    reader.close();
                    reader = null;
                }
                for (int n = 0, sz = connections.size(); n < sz; n++) {
                    InitialMasterConnection connection = connections.get(n);
                    connection.close();
                    connectionCache.add(connection);
                }
                connections.clear();
                initialSymbolCounts.clear();
            }

            @Override
            public void close() throws IOException {
                if (null != streamGenerator) {
                    clear();
                    streamGenerator.close();
                    streamGenerator = null;
                }
            }
        }
    }
}
