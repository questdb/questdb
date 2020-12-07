package io.questdb.cairo.replication;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionJobEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionJobQueue;
import io.questdb.cairo.replication.ReplicationPeerDetails.PeerConnection;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorFrame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Unsafe;

public class ReplicationMasterConnectionMultiplexer implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionMultiplexer.class);
    private final FilesFacade ff;
    private ReplicationMasterCallbacks callbacks;
    private LongObjHashMap<SlavePeerDetails> peerById = new LongObjHashMap<>();
    private ObjList<SlavePeerDetails> peers = new ObjList<>();
    private int nWorkers;
    private final ConnectionJobQueue connectionProducerQueue;
    private final ConnectionJobQueue[] connectionConsumerQueues;

    private static final ConnectionJobQueue createConnectionJobConsumerQueue(int queueLen, ObjectFactory<?> eventFactory) {
        Sequence producerSeq = new SPSequence(queueLen);
        Sequence consumerSeq = new SCSequence();
        RingQueue<?> queue = new RingQueue<>(eventFactory, queueLen);
        producerSeq.then(consumerSeq).then(producerSeq);
        return new ConnectionJobQueue(producerSeq, consumerSeq, queue);
    }

    private static final ConnectionJobQueue createConnectionJobProducerQueue(int queueLen, ObjectFactory<?> eventFactory) {
        Sequence producerSeq = new MPSequence(queueLen);
        Sequence consumerSeq = new SCSequence();
        RingQueue<?> queue = new RingQueue<>(eventFactory, queueLen);
        producerSeq.then(consumerSeq).then(producerSeq);
        return new ConnectionJobQueue(producerSeq, consumerSeq, queue);
    }

    public ReplicationMasterConnectionMultiplexer(FilesFacade ff, WorkerPool senderWorkerPool, int producerQueueLen, int consumerQueueLen, ReplicationMasterCallbacks callbacks) {
        super();
        this.ff = ff;
        this.callbacks = callbacks;

        nWorkers = senderWorkerPool.getWorkerCount();
        connectionProducerQueue = createConnectionJobProducerQueue(producerQueueLen, ConnectionJobProducerEvent::new);
        connectionConsumerQueues = new ConnectionJobQueue[nWorkers];
        for (int n = 0; n < nWorkers; n++) {
            final ConnectionJobQueue consumerQueue = createConnectionJobConsumerQueue(consumerQueueLen, ConnectionJobConsumerEvent::new);
            SlaveConnectionJob sendJob = new SlaveConnectionJob(consumerQueue);
            connectionConsumerQueues[n] = consumerQueue;
            senderWorkerPool.assign(n, sendJob);
        }
    }

    boolean tryAddConnection(long peerId, long fd) {
        LOG.info().$("slave connected [peerId=").$(peerId).$(", fd=").$(fd).$(']').$();
        SlavePeerDetails slaveDetails = getSlaveDetails(peerId);
        return slaveDetails.tryAddConnection(fd);
    }

    boolean tryQueueSendFrame(long peerId, ReplicationStreamGeneratorFrame frame) {
        SlavePeerDetails slaveDetails = getSlaveDetails(peerId);
        return slaveDetails.tryQueueSendFrame(frame);
    }

    boolean handleTasks() {
        boolean busy = false;
        long seq;
        while ((seq = connectionProducerQueue.getConsumerSeq().next()) >= 0) {
            ConnectionJobProducerEvent event = connectionProducerQueue.getEvent(seq);
            try {
                long peerId = event.slaveId;
                switch (event.eventType) {
                    case SlaveReadyToCommit:
                        callbacks.onSlaveReadyToCommit(peerId, event.tableId);
                        break;
                    case SlaveDisconnected:
                        SlavePeerDetails slaveDetails = getSlaveDetails(peerId);
                        long fd = event.fd;
                        slaveDetails.removeConnection(fd);
                        callbacks.onSlaveDisconnected(peerId, fd);
                        break;
                }
            } finally {
                event.clear();
                connectionProducerQueue.getConsumerSeq().done(seq);
            }
        }
        return busy;
    }

    private SlavePeerDetails getSlaveDetails(long peerId) {
        SlavePeerDetails slaveDetails = peerById.get(peerId);
        if (null == slaveDetails) {
            slaveDetails = new SlavePeerDetails(peerId, nWorkers, connectionConsumerQueues);
            peers.add(slaveDetails);
            peerById.put(peerId, slaveDetails);
        }
        return slaveDetails;
    }

    @Override
    public void close() {
        if (null != peerById) {
            Misc.freeObjList(peers);
            peers = null;
            peerById.clear();
            peerById = null;
            callbacks = null;
        }
    }

    interface ReplicationMasterCallbacks {
        void onSlaveReadyToCommit(long slaveId, int tableId);

        void onSlaveDisconnected(long slaveId, long fd);
    }

    private class SlavePeerDetails extends ReplicationPeerDetails {
        private SlavePeerDetails(long slaveId, int nWorkers, ConnectionJobQueue[] connectionConsumerQueues) {
            super(slaveId, nWorkers, connectionConsumerQueues, SlaveConnection::new);
        }

        boolean tryQueueSendFrame(ReplicationStreamGeneratorFrame frame) {
            SlaveConnection connection = getConnection(frame.getThreadId());
            return connection.tryQueueSendFrame(frame);
        }
    }

    private class SlaveConnection implements PeerConnection {
        private long peerId = Long.MIN_VALUE;
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

        @Override
        public PeerConnection of(long slaveId, long fd, int workerId) {
            assert null == queuedSendFrame.get();
            assert null == activeSendFrame;
            this.peerId = slaveId;
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

        @Override
        public long getFd() {
            return fd;
        }

        @Override
        public int getWorkertId() {
            return workerId;
        }

        boolean tryQueueSendFrame(ReplicationStreamGeneratorFrame frame) {
            return queuedSendFrame.compareAndSet(null, frame);
        }

        @Override
        public boolean handleSendTask() {
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

        @Override
        public boolean handleReceiveTask() {
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

        @Override
        public boolean isDisconnected() {
            return disconnected;
        }

        private boolean tryHandleDisconnect() {
            long seq = connectionProducerQueue.getConsumerSeq().next();
            if (seq >= 0) {
                try {
                    ConnectionJobProducerEvent event = connectionProducerQueue.getEvent(seq);
                    event.assignDisconnected(peerId, fd);
                } finally {
                    connectionProducerQueue.getConsumerSeq().done(seq);
                }
                return true;
            }
            return false;
        }

        private boolean tryHandleSlaveCommitReady(int masterTableId) {
            long seq = connectionProducerQueue.getProducerSeq().next();
            if (seq >= 0) {
                try {
                    ConnectionJobProducerEvent event = connectionProducerQueue.getEvent(seq);
                    event.assignSlaveComitReady(peerId, masterTableId);
                } finally {
                    connectionProducerQueue.getProducerSeq().done(seq);
                }
                return true;
            }
            return false;
        }

        @Override
        public void clear() {
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
            peerId = Long.MIN_VALUE;
            fd = -1;
        }
    }

    private static class ConnectionJobConsumerEvent implements ConnectionJobEvent {
        private enum EventType {
            SlaveConnected
        };

        private EventType eventType;
        private SlaveConnection addedConnection;

        @Override
        public void assignPeerConnected(PeerConnection connection) {
            assert eventType == null;
            eventType = EventType.SlaveConnected;
            addedConnection = (SlaveConnection) connection;
        }

        @Override
        public void clear() {
            eventType = null;
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

    private class SlaveConnectionJob extends ConnectionJob<ConnectionJobConsumerEvent> {
        private SlaveConnectionJob(ConnectionJobQueue consumerQueue) {
            super(consumerQueue);
        }

        @Override
        protected boolean isBlocked() {
            return false;
        }

        @Override
        protected void handleConsumerEvent(ConnectionJobConsumerEvent event) {
            switch (event.eventType) {
                case SlaveConnected:
                    addConnection(event.addedConnection);
            }
        }
    }
}
