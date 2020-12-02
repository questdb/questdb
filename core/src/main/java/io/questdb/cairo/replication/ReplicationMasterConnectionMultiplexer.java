package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorFrame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class ReplicationMasterConnectionMultiplexer implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionMultiplexer.class);
    private final FilesFacade ff;
    private ReplicationMasterCallbacks callbacks;
    private LongObjHashMap<SlaveDetails> slaveById = new LongObjHashMap<>();
    private ObjList<SlaveDetails> slaves = new ObjList<>();
    private int nWorkers;
    private final ConnectionJobProducerQueue connectionProducerQueue;
    private final ConnectionJobConsumerQueue[] connectionConsumerQueues;

    public ReplicationMasterConnectionMultiplexer(FilesFacade ff, WorkerPool senderWorkerPool, int producerQueueLen, int consumerQueueLen, ReplicationMasterCallbacks callbacks) {
        super();
        this.ff = ff;
        this.callbacks = callbacks;

        nWorkers = senderWorkerPool.getWorkerCount();
        connectionProducerQueue = new ConnectionJobProducerQueue(producerQueueLen);
        connectionConsumerQueues = new ConnectionJobConsumerQueue[nWorkers];
        for (int n = 0; n < nWorkers; n++) {
            final ConnectionJobConsumerQueue consumerQueue = new ConnectionJobConsumerQueue(consumerQueueLen);
            SlaveConnectionJob sendJob = new SlaveConnectionJob(consumerQueue);
            connectionConsumerQueues[n] = consumerQueue;
            senderWorkerPool.assign(n, sendJob);
        }
    }

    boolean tryAddConnection(long slaveId, long fd) {
        LOG.info().$("slave connected [slaveId=").$(slaveId).$(", fd=").$(fd).$(']').$();
        SlaveDetails slaveDetails = getSlaveDetails(slaveId);
        return slaveDetails.tryAddConnection(fd);
    }

    boolean tryQueueSendFrame(long slaveId, ReplicationStreamGeneratorFrame frame) {
        SlaveDetails slaveDetails = getSlaveDetails(slaveId);
        return slaveDetails.tryQueueSendFrame(frame);
    }

    boolean handleTasks() {
        boolean busy = false;
        long seq;
        while ((seq = connectionProducerQueue.consumerSeq.next()) >= 0) {
            ConnectionJobProducerEvent event = connectionProducerQueue.queue.get(seq);
            try {
                long slaveId = event.slaveId;
                switch (event.eventType) {
                    case SlaveReadyToCommit:
                        callbacks.onSlaveReadyToCommit(slaveId, event.tableId);
                        break;
                    case SlaveDisconnected:
                        SlaveDetails slaveDetails = getSlaveDetails(slaveId);
                        long fd = event.fd;
                        slaveDetails.removeConnection(fd);
                        callbacks.onSlaveDisconnected(slaveId, fd);
                        break;
                }
            } finally {
                event.clear();
                connectionProducerQueue.consumerSeq.done(seq);
            }
        }
        return busy;
    }

    private SlaveDetails getSlaveDetails(long slaveId) {
        SlaveDetails slaveDetails = slaveById.get(slaveId);
        if (null == slaveDetails) {
            slaveDetails = new SlaveDetails(slaveId);
            slaves.add(slaveDetails);
            slaveById.put(slaveId, slaveDetails);
        }
        return slaveDetails;
    }

    @Override
    public void close() {
        if (null != slaveById) {
            Misc.freeObjList(slaves);
            slaves = null;
            slaveById.clear();
            slaveById = null;
            callbacks = null;
        }
    }

    interface ReplicationMasterCallbacks {
        void onSlaveReadyToCommit(long slaveId, int tableId);

        void onSlaveDisconnected(long slaveId, long fd);
    }

    private class SlaveDetails implements Closeable {
        private long slaveId = Long.MIN_VALUE;
        private IntList nAssignedByWorkerId = new IntList();
        private ObjList<SlaveConnection> connections;
        private ObjList<SlaveConnection> connectionCache;

        private SlaveDetails(long slaveId) {
            super();
            this.slaveId = slaveId;
            nAssignedByWorkerId = new IntList(nWorkers);
            for (int nWorker = 0; nWorker < nWorkers; nWorker++) {
                nAssignedByWorkerId.add(0);
            }
            connections = new ObjList<>();
            connectionCache = new ObjList<>();
        }

        private boolean tryAddConnection(long fd) {
            int nMinAssigned = Integer.MAX_VALUE;
            int workerId = Integer.MAX_VALUE;
            for (int nWorker = 0; nWorker < nWorkers; nWorker++) {
                int nAssigned = nAssignedByWorkerId.getQuick(nWorker);
                if (nAssigned < nMinAssigned) {
                    nMinAssigned = nAssigned;
                    workerId = nWorker;
                }
            }

            ConnectionJobConsumerQueue queue = connectionConsumerQueues[workerId];
            long seq = queue.producerSeq.next();
            if (seq >= 0) {
                try {
                    ConnectionJobConsumerEvent event = queue.queue.get(seq);
                    SlaveConnection connection;
                    if (connectionCache.size() > 0) {
                        int n = connectionCache.size() - 1;
                        connection = connectionCache.getQuick(n);
                        connectionCache.remove(n);

                    } else {
                        connection = new SlaveConnection();
                    }
                    connection.of(slaveId, fd, workerId);
                    event.addedConnection = connection;
                    nAssignedByWorkerId.set(workerId, nAssignedByWorkerId.getQuick(workerId) + 1);
                    connections.add(connection);
                } finally {
                    queue.producerSeq.done(seq);
                }
                LOG.info().$("assigned connection [workerId=").$(workerId).$(", fd=").$(fd).$(']').$();
                return true;
            }

            return false;
        }

        private void removeConnection(long fd) {
            for (int n = 0, sz = connections.size(); n < sz; n++) {
                SlaveConnection slaveConnection = connections.get(n);
                if (slaveConnection.fd == fd) {
                    connections.remove(n);
                    nAssignedByWorkerId.set(slaveConnection.workerId, nAssignedByWorkerId.getQuick(slaveConnection.workerId) - 1);
                    slaveConnection.clear();
                    connectionCache.add(slaveConnection);
                    return;
                }
            }
        }

        @Override
        public void close() {
            if (null != connections) {
                slaveId = Long.MIN_VALUE;
                Misc.freeObjList(connectionCache);
                connectionCache = null;
                Misc.freeObjList(connections);
                connections = null;
                nAssignedByWorkerId.clear();
                nAssignedByWorkerId = null;
            }
        }

        boolean tryQueueSendFrame(ReplicationStreamGeneratorFrame frame) {
            int connectionId = frame.getThreadId() % connections.size();
            SlaveConnection connection = connections.getQuick(connectionId);
            return connection.tryQueueSendFrame(frame);
        }
    }

    private class SlaveConnection implements Closeable {
        private long slaveId = Long.MIN_VALUE;
        private long fd = -1;
        private int workerId;
        private final AtomicReference<ReplicationStreamGeneratorFrame> queuedSendFrame = new AtomicReference<>();
        private ReplicationStreamGeneratorFrame activeSendFrame;
        private long sendAddress;
        private long sendOffset;
        private long sendLength;
        private boolean sendingHeader;
        private boolean disconnected;
        private long receiveAddress;
        private long receiveBufSz;
        private long receiveOffset;
        private long receiveLen;
        private byte receiveFrameType;

        private SlaveConnection of(long slaveId, long fd, int workerId) {
            assert null == queuedSendFrame.get();
            assert null == activeSendFrame;
            this.slaveId = slaveId;
            this.fd = fd;
            this.workerId = workerId;
            disconnected = false;
            receiveBufSz = TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE;
            receiveAddress = Unsafe.malloc(receiveBufSz);
            receiveOffset = 0;
            receiveLen = TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE;
            receiveFrameType = TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN;
            return this;
        }

        boolean tryQueueSendFrame(ReplicationStreamGeneratorFrame frame) {
            return queuedSendFrame.compareAndSet(null, frame);
        }

        boolean handleSendTask() {
            assert !disconnected;
            boolean wroteSomething = false;
            while (true) {
                if (null == activeSendFrame) {
                    activeSendFrame = queuedSendFrame.getAndSet(null);
                    if (null == activeSendFrame) {
                        return false;
                    }

                    sendAddress = activeSendFrame.getFrameHeaderAddress();
                    sendOffset = 0;
                    sendLength = activeSendFrame.getFrameHeaderLength();
                    sendingHeader = true;
                }

                assert sendAddress != 0;
                assert sendLength > 0;
                assert sendOffset < sendLength;
                long nWritten = ff.write(fd, sendAddress, sendLength, sendOffset);
                if (nWritten > 0) {
                    if (nWritten == sendLength) {
                        wroteSomething = true;
                        if (sendingHeader && activeSendFrame.getFrameDataLength() > 0) {
                            sendAddress = activeSendFrame.getFrameDataAddress();
                            sendOffset = 0;
                            sendLength = activeSendFrame.getFrameDataLength();
                            sendingHeader = false;
                        } else {
                            activeSendFrame.complete();
                            activeSendFrame = null;
                        }
                    } else {
                        sendOffset += nWritten;
                        sendLength -= nWritten;
                        // OS send buffer full, return busy since we wrote some data
                        return true;
                    }
                } else {
                    if (nWritten < 0) {
                        if (tryHandleDisconnect()) {
                            LOG.info().$("socket peer disconnected when writing [fd=").$(fd).$(']').$();
                            return false;
                        } else {
                            return true;
                        }
                    }
                    // OS send buffer full, if nothing was written return not busy due to back pressure
                    return wroteSomething;
                }
            }
        }

        boolean handleReceiveTask() {
            assert !disconnected;
            boolean readSomething = false;
            while (true) {
                if (receiveFrameType == TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN) {
                    long len = receiveLen - receiveOffset;
                    long nRead = ff.read(fd, receiveAddress, len, receiveOffset);
                    if (nRead > 0) {
                        readSomething = true;
                        receiveOffset += nRead;
                        if (receiveOffset >= TableReplicationStreamHeaderSupport.MIN_HEADER_SIZE) {
                            byte frameType = Unsafe.getUnsafe().getByte(receiveAddress + TableReplicationStreamHeaderSupport.OFFSET_FRAME_TYPE);
                            receiveLen = TableReplicationStreamHeaderSupport.getFrameHeaderSize(frameType);
                            if (receiveOffset < receiveLen) {
                                // Read the rest of the header
                                continue;
                            }
                            receiveFrameType = frameType;
                        } else {
                            return readSomething;
                        }
                    } else {
                        if (nRead < 0) {
                            if (tryHandleDisconnect()) {
                                LOG.info().$("socket peer disconnected when reading [fd=").$(fd).$(']').$();
                                return false;
                            } else {
                                return true;
                            }
                        }
                        return readSomething;
                    }
                }

                switch (receiveFrameType) {
                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_SLAVE_COMMIT_READY:
                        int masterTableId = Unsafe.getUnsafe().getByte(receiveAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                        if (!tryHandleSlaveCommitReady(masterTableId)) {
                            return true;
                        }
                        receiveFrameType = TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN;
                        break;

                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN:
                        break;
                    default:
                        if (tryHandleDisconnect()) {
                            LOG.error().$("received unrecognized frame type ").$(receiveFrameType).$(" [fd=").$(fd).$(']').$();
                        }
                }

            }
        }

        boolean isDisconnected() {
            return disconnected;
        }

        private boolean tryHandleDisconnect() {
            long seq = connectionProducerQueue.consumerSeq.next();
            if (seq >= 0) {
                try {
                    ConnectionJobProducerEvent event = connectionProducerQueue.queue.get(seq);
                    event.assignDisconnected(slaveId, fd);
                } finally {
                    connectionProducerQueue.consumerSeq.done(seq);
                }
                return true;
            }
            return false;
        }

        private boolean tryHandleSlaveCommitReady(int masterTableId) {
            long seq = connectionProducerQueue.producerSeq.next();
            if (seq >= 0) {
                try {
                    ConnectionJobProducerEvent event = connectionProducerQueue.queue.get(seq);
                    event.assignSlaveComitReady(slaveId, masterTableId);
                } finally {
                    connectionProducerQueue.producerSeq.done(seq);
                }
                return true;
            }
            return false;
        }

        void clear() {
            reset();
        }

        @Override
        public void close() {
            if (receiveAddress != 0) {
                reset();
                Unsafe.free(receiveAddress, receiveBufSz);
                receiveAddress = 0;
            }
        }

        private void reset() {
            queuedSendFrame.set(null);
            activeSendFrame = null;
            slaveId = Long.MIN_VALUE;
            fd = -1;
        }
    }

    private static class ConnectionJobConsumerQueue {
        private final Sequence producerSeq;
        private final Sequence consumerSeq;
        private final RingQueue<ConnectionJobConsumerEvent> queue;

        public ConnectionJobConsumerQueue(int queueLen) {
            super();
            this.producerSeq = new SPSequence(queueLen);
            this.consumerSeq = new SCSequence();
            this.queue = new RingQueue<>(ConnectionJobConsumerEvent::new, queueLen);
            producerSeq.then(consumerSeq).then(producerSeq);
        }
    }

    private static class ConnectionJobConsumerEvent {
        private SlaveConnection addedConnection;

        private void clear() {
            addedConnection = null;
        }
    }

    private static class ConnectionJobProducerQueue {
        private final Sequence producerSeq;
        private final Sequence consumerSeq;
        private final RingQueue<ConnectionJobProducerEvent> queue;

        public ConnectionJobProducerQueue(int queueLen) {
            super();
            this.producerSeq = new MPSequence(queueLen);
            this.consumerSeq = new SCSequence();
            this.queue = new RingQueue<>(ConnectionJobProducerEvent::new, queueLen);
            producerSeq.then(consumerSeq).then(producerSeq);
        }
    }

    private static class ConnectionJobProducerEvent {
        private enum EventType {
            SlaveDisconnected, SlaveReadyToCommit
        };

        private EventType eventType;
        private long slaveId;
        private long fd;
        private int tableId;

        void assignDisconnected(long slaveId, long fd) {
            assert eventType == null;
            eventType = EventType.SlaveDisconnected;
            this.slaveId = slaveId;
            this.fd = fd;
        }

        void assignSlaveComitReady(long slaveId, int masterTableId) {
            assert eventType == null;
            eventType = EventType.SlaveReadyToCommit;
            this.slaveId = slaveId;
            tableId = masterTableId;
        }

        void clear() {
            eventType = null;
        }
    }

    private class SlaveConnectionJob implements Job {
        private final ConnectionJobConsumerQueue consumerQueue;
        private final ObjList<SlaveConnection> connections = new ObjList<>();
        private boolean busy;

        private SlaveConnectionJob(ConnectionJobConsumerQueue consumerQueue) {
            super();
            this.consumerQueue = consumerQueue;
        }

        @Override
        public boolean run(int workerId) {
            busy = false;
            int nConnection = 0;
            while (nConnection < connections.size()) {
                SlaveConnection connection = connections.get(nConnection);
                if (connection.handleSendTask()) {
                    busy = true;
                } else {
                }
                if (connection.handleReceiveTask()) {
                    busy = true;
                }
                if (connection.isDisconnected()) {
                    connections.remove(nConnection);
                } else {
                    nConnection++;
                }
            }
            handleConsumerEvents();
            return busy;
        }

        private void handleConsumerEvents() {
            long seq = consumerQueue.consumerSeq.next();
            if (seq >= 0) {
                ConnectionJobConsumerEvent event = consumerQueue.queue.get(seq);
                try {
                    connections.add(event.addedConnection);
                    busy = true;
                } finally {
                    event.clear();
                    consumerQueue.consumerSeq.done(seq);
                }
            }
        }
    }
}
