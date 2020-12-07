package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.PeerConnection;
import io.questdb.cairo.replication.ReplicationPeerDetails.SequencedQueue;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorFrame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongObjHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class ReplicationMasterConnectionDemultiplexer implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionDemultiplexer.class);
    private final FilesFacade ff;
    private final int sendFrameQueueLen;
    private ReplicationMasterCallbacks callbacks;
    private LongObjHashMap<ReplicationPeerDetails> peerById = new LongObjHashMap<>();
    private ObjList<ReplicationPeerDetails> peers = new ObjList<>();
    private int nWorkers;
    private final SequencedQueue<ConnectionCallbackEvent> connectionCallbackQueue;
    private final ConnectionWorkerJob[] connectionWorkerJobs;

    public ReplicationMasterConnectionDemultiplexer(
            FilesFacade ff,
            WorkerPool senderWorkerPool,
            int connectionCallbackQueueLen,
            int newConnectionQueueLen,
            int sendFrameQueueLen,
            ReplicationMasterCallbacks callbacks
    ) {
        super();
        this.ff = ff;
        this.callbacks = callbacks;
        this.sendFrameQueueLen = sendFrameQueueLen;

        nWorkers = senderWorkerPool.getWorkerCount();
        connectionCallbackQueue = SequencedQueue.createMultipleProducerSingleConsumerQueue(connectionCallbackQueueLen, ConnectionCallbackEvent::new);
        connectionWorkerJobs = new ConnectionWorkerJob[nWorkers];
        for (int n = 0; n < nWorkers; n++) {
            final SequencedQueue<ConnectionWorkerEvent> consumerQueue = SequencedQueue.createSingleProducerSingleConsumerQueue(newConnectionQueueLen, ConnectionWorkerEvent::new);
            ConnectionWorkerJob sendJob = new ConnectionWorkerJob(consumerQueue);
            connectionWorkerJobs[n] = sendJob;
            senderWorkerPool.assign(n, sendJob);
        }
    }

    boolean tryAddConnection(long peerId, long fd) {
        LOG.info().$("peer connected [peerId=").$(peerId).$(", fd=").$(fd).$(']').$();
        ReplicationPeerDetails peerDetails = getPeerDetails(peerId);
        return peerDetails.tryAddConnection(fd);
    }

    boolean tryQueueSendFrame(long peerId, ReplicationStreamGeneratorFrame frame) {
        SlavePeerDetails slaveDetails = getPeerDetails(peerId);
        return slaveDetails.tryQueueSendFrame(frame);
    }

    boolean handleTasks() {
        boolean busy = false;
        long seq;
        while ((seq = connectionCallbackQueue.getConsumerSeq().next()) >= 0) {
            ConnectionCallbackEvent event = connectionCallbackQueue.getEvent(seq);
            try {
                long peerId = event.slaveId;
                switch (event.eventType) {
                    case SlaveReadyToCommit:
                        callbacks.onSlaveReadyToCommit(peerId, event.tableId);
                        break;
                    case SlaveDisconnected:
                        ReplicationPeerDetails peerDetails = getPeerDetails(peerId);
                        long fd = event.fd;
                        peerDetails.removeConnection(fd);
                        callbacks.onPeerDisconnected(peerId, fd);
                        break;
                }
            } finally {
                event.clear();
                connectionCallbackQueue.getConsumerSeq().done(seq);
            }
        }
        return busy;
    }

    @SuppressWarnings("unchecked")
    private <T extends ReplicationPeerDetails> T getPeerDetails(long peerId) {
        ReplicationPeerDetails peerDetails = peerById.get(peerId);
        if (null == peerDetails) {
            peerDetails = new SlavePeerDetails(peerId, nWorkers, connectionWorkerJobs);
            peers.add(peerDetails);
            peerById.put(peerId, peerDetails);
        }
        return (T) peerDetails;
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

        void onPeerDisconnected(long slaveId, long fd);
    }

    private class SlavePeerDetails extends ReplicationPeerDetails {
        private SlavePeerDetails(long slaveId, int nWorkers, ConnectionWorkerJob[] connectionWorkerJobs) {
            super(slaveId, nWorkers, connectionWorkerJobs, SlaveConnection::new);
        }

        boolean tryQueueSendFrame(ReplicationStreamGeneratorFrame frame) {
            SlaveConnection connection = getConnection(frame.getThreadId());
            SequencedQueue<SendFrameEvent> consumerQueue = connection.getConnectionQueue();
            long seq = consumerQueue.getProducerSeq().next();
            if (seq >= 0) {
                try {
                    consumerQueue.getEvent(seq).frame = frame;
                } finally {
                    consumerQueue.getProducerSeq().done(seq);
                }
                return true;
            }
            return false;
        }
    }

    private static class SendFrameEvent {
        private ReplicationStreamGeneratorFrame frame;
    }

    private class SlaveConnection implements PeerConnection<SendFrameEvent> {
        private final SequencedQueue<SendFrameEvent> sendFrameQueue;
        private long peerId = Long.MIN_VALUE;
        private long fd = -1;
        private int workerId;
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

        private SlaveConnection() {
            this.sendFrameQueue = SequencedQueue.createSingleProducerSingleConsumerQueue(sendFrameQueueLen, SendFrameEvent::new);
        }

        @Override
        public SlaveConnection of(long slaveId, long fd, int workerId) {
            assert sendFrameQueue.getConsumerSeq().next() == -1; // Queue is empty
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

        @Override
        public SequencedQueue<SendFrameEvent> getConnectionQueue() {
            return sendFrameQueue;
        }

        @Override
        public boolean handleSendTask() {
            assert !disconnected;
            boolean wroteSomething = false;
            while (true) {
                if (null == activeSendFrame) {
                    long seq = sendFrameQueue.getConsumerSeq().next();
                    if (seq >= 0) {
                        SendFrameEvent event = sendFrameQueue.getEvent(seq);
                        try {
                            activeSendFrame = event.frame;
                        } finally {
                            event.frame = null;
                            sendFrameQueue.getConsumerSeq().done(seq);
                        }
                    } else {
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
            long seq = connectionCallbackQueue.getConsumerSeq().next();
            if (seq >= 0) {
                try {
                    ConnectionCallbackEvent event = connectionCallbackQueue.getEvent(seq);
                    event.assignDisconnected(peerId, fd);
                } finally {
                    connectionCallbackQueue.getConsumerSeq().done(seq);
                }
                return true;
            }
            return false;
        }

        private boolean tryHandleSlaveCommitReady(int masterTableId) {
            long seq = connectionCallbackQueue.getProducerSeq().next();
            if (seq >= 0) {
                try {
                    ConnectionCallbackEvent event = connectionCallbackQueue.getEvent(seq);
                    event.assignSlaveComitReady(peerId, masterTableId);
                } finally {
                    connectionCallbackQueue.getProducerSeq().done(seq);
                }
                return true;
            }
            return false;
        }

        @Override
        public void clear() {
            long seq;
            while ((seq = sendFrameQueue.getConsumerSeq().next()) >= 0) {
                SendFrameEvent event = sendFrameQueue.getEvent(seq);
                try {
                    event.frame.cancel();
                } finally {
                    event.frame = null;
                    sendFrameQueue.getConsumerSeq().done(seq);
                }
            }

            assert seq == -1; // There cannot be contention
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
            activeSendFrame = null;
            peerId = Long.MIN_VALUE;
            fd = -1;
        }
    }

    private static class ConnectionCallbackEvent {
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
}
