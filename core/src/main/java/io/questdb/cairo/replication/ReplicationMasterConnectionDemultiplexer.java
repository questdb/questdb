package io.questdb.cairo.replication;

import io.questdb.cairo.replication.ReplicationMasterConnectionDemultiplexer.MasterConnectionCallbackEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionCallbackEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.FanOutSequencedQueue;
import io.questdb.cairo.replication.ReplicationPeerDetails.PeerConnection;
import io.questdb.cairo.replication.ReplicationPeerDetails.SequencedQueue;
import io.questdb.cairo.replication.ReplicationStreamGenerator.ReplicationStreamGeneratorFrame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;

public class ReplicationMasterConnectionDemultiplexer extends AbstractMultipleConnectionManager<ConnectionWorkerEvent, MasterConnectionCallbackEvent> {
    private static final Log LOG = LogFactory.getLog(ReplicationMasterConnectionDemultiplexer.class);
    private final int sendFrameQueueLen;
    private ReplicationMasterCallbacks callbacks;

    public ReplicationMasterConnectionDemultiplexer(
            FilesFacade ff,
            WorkerPool senderWorkerPool,
            int connectionCallbackQueueLen,
            int newConnectionQueueLen,
            int sendFrameQueueLen,
            ReplicationMasterCallbacks callbacks
    ) {
        super(ff, senderWorkerPool, connectionCallbackQueueLen, newConnectionQueueLen, ConnectionWorkerEvent::new, MasterConnectionCallbackEvent::new);
        this.callbacks = callbacks;
        this.sendFrameQueueLen = sendFrameQueueLen;
    }

    boolean tryQueueSendFrame(long peerId, ReplicationStreamGeneratorFrame frame) {
        SlavePeerDetails slaveDetails = getPeerDetails(peerId);
        return slaveDetails.tryQueueSendFrame(frame);
    }

    @Override
    ConnectionWorkerJob<ConnectionWorkerEvent, MasterConnectionCallbackEvent> createConnectionWorkerJob(int nWorker, FanOutSequencedQueue<ConnectionWorkerEvent> connectionWorkerQueue) {
        return new ConnectionWorkerJob<ConnectionWorkerEvent, MasterConnectionCallbackEvent>(nWorker, connectionWorkerQueue) {
            @Override
            protected void handleConsumerEvent(ConnectionWorkerEvent event) {
            }
        };
    }

    @Override
    ReplicationPeerDetails createNewReplicationPeerDetails(long peerId) {
        return new SlavePeerDetails(ff, connectionCallbackQueue, sendFrameQueueLen, peerId, nWorkers, connectionWorkerJobs);
    }

    @Override
    boolean handleTasks() {
        boolean busy = false;
        long seq;
        while ((seq = connectionCallbackQueue.getConsumerSeq().next()) >= 0) {
            MasterConnectionCallbackEvent event = connectionCallbackQueue.getEvent(seq);
            try {
                long peerId = event.peerId;
                switch (event.eventType) {
                    case MasterConnectionCallbackEvent.SLAVE_READEY_TOCOMMIT_EVENT_TYPE:
                        callbacks.onSlaveReadyToCommit(peerId, event.tableId);
                        break;
                    case ConnectionCallbackEvent.PEER_DISCONNECTED_EVENT_TYPE:
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

    interface ReplicationMasterCallbacks {
        void onSlaveReadyToCommit(long peerId, int tableId);

        void onPeerDisconnected(long peerId, long fd);
    }

    private static class SlavePeerDetails extends ReplicationPeerDetails {
        private SlavePeerDetails(
                FilesFacade ff,
                SequencedQueue<MasterConnectionCallbackEvent> connectionCallbackQueue,
                int sendFrameQueueLen,
                long slaveId,
                int nWorkers,
                ConnectionWorkerJob<?, ?>[] connectionWorkerJobs
        ) {
            super(slaveId, nWorkers, connectionWorkerJobs, () -> {
                return new SlaveConnection(ff, connectionCallbackQueue, sendFrameQueueLen);
            });
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

    private static class SlaveConnection extends PeerConnection<MasterConnectionCallbackEvent> {
        private final SequencedQueue<SendFrameEvent> sendFrameQueue;
        private ReplicationStreamGeneratorFrame activeSendFrame;
        private long sendAddress;
        private long sendOffset;
        private long sendLength;
        private boolean sendingHeader;
        private long receiveAddress;
        private long receiveBufSz;
        private long receiveOffset;
        private long receiveLen;
        private byte receiveFrameType;

        private SlaveConnection(FilesFacade ff, SequencedQueue<MasterConnectionCallbackEvent> connectionCallbackQueue, int sendFrameQueueLen) {
            super(ff, connectionCallbackQueue);
            this.sendFrameQueue = SequencedQueue.createSingleProducerSingleConsumerQueue(sendFrameQueueLen, SendFrameEvent::new);
        }

        @Override
        public SlaveConnection of(long slaveId, long fd, ConnectionWorkerJob<?, ?> workerJob) {
            assert sendFrameQueue.getConsumerSeq().next() == -1; // Queue is empty
            assert null == activeSendFrame;
            super.of(slaveId, fd, workerJob);
            receiveBufSz = TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE;
            receiveAddress = Unsafe.malloc(receiveBufSz);
            receiveOffset = 0;
            receiveLen = TableReplicationStreamHeaderSupport.MAX_HEADER_SIZE;
            receiveFrameType = TableReplicationStreamHeaderSupport.FRAME_TYPE_UNKNOWN;
            return this;
        }

        SequencedQueue<SendFrameEvent> getConnectionQueue() {
            return sendFrameQueue;
        }

        @Override
        public IOResult handleIO() {
            IOResult sendRc = handleSendTask();
            IOResult recvRc;
            if (sendRc != IOResult.Disconnected) {
                recvRc = handleReceiveTask();
                if (recvRc != IOResult.NotBusy) {
                    return recvRc;
                }
            }
            return sendRc;
        }

        private IOResult handleSendTask() {
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
                        return IOResult.NotBusy;
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
                        return IOResult.Busy;
                    }
                } else {
                    if (nWritten < 0) {
                        if (tryHandleDisconnect()) {
                            LOG.info().$("socket peer disconnected when writing [fd=").$(fd).$(']').$();
                            return IOResult.Disconnected;
                        } else {
                            return IOResult.Busy;
                        }
                    }
                    // OS send buffer full, if nothing was written return not busy due to back pressure
                    return wroteSomething ? IOResult.Busy : IOResult.NotBusy;
                }
            }
        }

        private IOResult handleReceiveTask() {
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
                            return readSomething ? IOResult.Busy : IOResult.NotBusy;
                        }
                    } else {
                        if (nRead < 0) {
                            if (tryHandleDisconnect()) {
                                LOG.info().$("socket peer disconnected when reading [fd=").$(fd).$(']').$();
                                return IOResult.Disconnected;
                            } else {
                                return IOResult.Busy;
                            }
                        }
                        return readSomething ? IOResult.Busy : IOResult.NotBusy;
                    }
                }

                switch (receiveFrameType) {
                    case TableReplicationStreamHeaderSupport.FRAME_TYPE_SLAVE_COMMIT_READY:
                        int masterTableId = Unsafe.getUnsafe().getByte(receiveAddress + TableReplicationStreamHeaderSupport.OFFSET_MASTER_TABLE_ID);
                        if (!tryHandleSlaveCommitReady(masterTableId)) {
                            return IOResult.Busy;
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

        private boolean tryHandleSlaveCommitReady(int masterTableId) {
            long seq = connectionCallbackQueue.getProducerSeq().next();
            if (seq >= 0) {
                try {
                    MasterConnectionCallbackEvent event = connectionCallbackQueue.getEvent(seq);
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

    static class MasterConnectionCallbackEvent extends ConnectionCallbackEvent {
        final static byte SLAVE_READEY_TOCOMMIT_EVENT_TYPE = 2;
        private int tableId;

        void assignSlaveComitReady(long slaveId, int masterTableId) {
            assert eventType == ConnectionCallbackEvent.NO_EVENT_TYPE;
            eventType = SLAVE_READEY_TOCOMMIT_EVENT_TYPE;
            this.peerId = slaveId;
            tableId = masterTableId;
        }
    }
}
