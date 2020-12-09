package io.questdb.cairo.replication;

import java.util.concurrent.atomic.AtomicInteger;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionCallbackEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.FanOutSequencedQueue;
import io.questdb.cairo.replication.ReplicationPeerDetails.PeerConnection;
import io.questdb.cairo.replication.ReplicationPeerDetails.SequencedQueue;
import io.questdb.cairo.replication.ReplicationSlaveConnectionMultiplexer.SlaveConnectionWorkerEvent;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.IntObjHashMap;

public class ReplicationSlaveConnectionMultiplexer extends AbstractMultipleConnectionManager<SlaveConnectionWorkerEvent, ConnectionCallbackEvent> {
    private static final Log LOG = LogFactory.getLog(ReplicationSlaveConnectionMultiplexer.class);
    private final CairoConfiguration configuration;
    private final ReplicationSlaveCallbacks callbacks;

    public ReplicationSlaveConnectionMultiplexer(
            CairoConfiguration configuration,
            WorkerPool workerPool,
            int connectionCallbackQueueLen,
            int newConnectionQueueLen,
            ReplicationSlaveCallbacks callbacks
    ) {
        super(configuration.getFilesFacade(), workerPool, connectionCallbackQueueLen, newConnectionQueueLen, SlaveConnectionWorkerEvent::new, ConnectionCallbackEvent::new);
        this.configuration = configuration;
        this.callbacks = callbacks;
    }

    boolean tryAddSlaveWriter(int masterTableId, SlaveWriter slaveWriter) {
        long seq = connectionWorkerQueue.getProducerSeq().next();
        if (seq >= 0) {
            SlaveConnectionWorkerEvent event = connectionWorkerQueue.getEvent(seq);
            try {
                event.assignAddSlaveWriter(masterTableId, slaveWriter);
            } finally {
                connectionWorkerQueue.getProducerSeq().done(seq);
            }
            return true;
        }
        return false;
    }

    boolean tryRemoveSlaveWriter(int masterTableId, AtomicInteger ackCounter) {
        long seq = connectionWorkerQueue.getProducerSeq().next();
        if (seq >= 0) {
            SlaveConnectionWorkerEvent event = connectionWorkerQueue.getEvent(seq);
            try {
                event.assignRemoveSlaveWriter(masterTableId, ackCounter);
            } finally {
                connectionWorkerQueue.getProducerSeq().done(seq);
            }
            return true;
        }
        return false;
    }

    @Override
    boolean handleTasks() {
        boolean busy = false;
        long seq;
        while ((seq = connectionCallbackQueue.getConsumerSeq().next()) >= 0) {
            ConnectionCallbackEvent event = connectionCallbackQueue.getEvent(seq);
            try {
                long peerId = event.peerId;
                switch (event.eventType) {
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

    @Override
    ConnectionWorkerJob<SlaveConnectionWorkerEvent, ConnectionCallbackEvent> createConnectionWorkerJob(int nWorker, FanOutSequencedQueue<SlaveConnectionWorkerEvent> connectionWorkerQueue) {
        return new SlaveConnectionWorkerJob(nWorker, connectionWorkerQueue);
    }

    @Override
    ReplicationPeerDetails createNewReplicationPeerDetails(long peerId) {
        return new MasterPeerDetails(peerId, nWorkers, connectionWorkerJobs, configuration, connectionCallbackQueue);
    }

    interface ReplicationSlaveCallbacks {
        void onPeerDisconnected(long peerId, long fd);
    }

    private static class MasterPeerDetails extends ReplicationPeerDetails {
        MasterPeerDetails(
                long peerId,
                int nWorkers,
                ConnectionWorkerJob<?, ?>[] connectionWorkerJobs,
                CairoConfiguration configuration,
                SequencedQueue<ConnectionCallbackEvent> connectionCallbackQueue
        ) {
            super(peerId, nWorkers, connectionWorkerJobs, () -> {
                return new MasterConnection(configuration, connectionCallbackQueue);
            });
        }
    }

    static class SlaveConnectionWorkerEvent extends ConnectionWorkerEvent {
        final static byte ADD_SLAVE_WRITER_EVENT_TYPE = 2;
        final static byte REMOVE_SLAVE_WRITER_EVENT_TYPE = 3;
        private int masterTableId;
        private SlaveWriter slaveWriter;
        private AtomicInteger ackCounter;

        private void assignAddSlaveWriter(int masterTableId, SlaveWriter slaveWriter) {
            eventType = ADD_SLAVE_WRITER_EVENT_TYPE;
            nWorker = ALL_WORKERS;
            this.masterTableId = masterTableId;
            this.slaveWriter = slaveWriter;
        }

        private void assignRemoveSlaveWriter(int masterTableId, AtomicInteger ackCounter) {
            eventType = REMOVE_SLAVE_WRITER_EVENT_TYPE;
            nWorker = ALL_WORKERS;
            this.masterTableId = masterTableId;
            this.ackCounter = ackCounter;
        }
    }

    static class SlaveConnectionWorkerJob extends ConnectionWorkerJob<SlaveConnectionWorkerEvent, ConnectionCallbackEvent> {
        private final IntObjHashMap<SlaveWriter> slaveWriteByMasterTableId = new IntObjHashMap<>();

        protected SlaveConnectionWorkerJob(int nWorker, FanOutSequencedQueue<SlaveConnectionWorkerEvent> connectionWorkerQueue) {
            super(nWorker, connectionWorkerQueue);
        }

        @Override
        protected void handleConsumerEvent(SlaveConnectionWorkerEvent event) {
            switch (event.eventType) {
                case SlaveConnectionWorkerEvent.ADD_SLAVE_WRITER_EVENT_TYPE:
                    slaveWriteByMasterTableId.put(event.masterTableId, event.slaveWriter);
                    LOG.info().$("added slave writer [nWorker=").$(nWorker).$(", masterTableId=").$(event.masterTableId).$(']').$();
                    break;
                case SlaveConnectionWorkerEvent.REMOVE_SLAVE_WRITER_EVENT_TYPE:
                    slaveWriteByMasterTableId.remove(event.masterTableId);
                    int nAck = event.ackCounter.decrementAndGet();
                    LOG.info().$("removed slave writer [nWorker=").$(nWorker).$(", masterTableId=").$(event.masterTableId).$(", nAck=").$(nAck).$(']').$();
                    break;
            }
        }
    }

    private static class MasterConnection extends PeerConnection<ConnectionCallbackEvent> {
        private boolean diconnecting;
        private ReplicationStreamReceiver streamReceiver;

        MasterConnection(CairoConfiguration configuration, SequencedQueue<ConnectionCallbackEvent> connectionCallbackQueue) {
            super(configuration.getFilesFacade(), connectionCallbackQueue);
            streamReceiver = new ReplicationStreamReceiver(configuration);
        }

        @Override
        public PeerConnection<ConnectionCallbackEvent> of(long peerId, long fd, ConnectionWorkerJob<?, ?> workerJob) {
            this.peerId = peerId;
            this.fd = fd;
            this.workerId = workerJob.getWorkerId();
            diconnecting = false;
            streamReceiver.of(fd, ((SlaveConnectionWorkerJob) workerJob).slaveWriteByMasterTableId, () -> {
                diconnecting = true;
            });
            return this;
        }

        @Override
        public IOResult handleIO() {
            if (!diconnecting) {
                return streamReceiver.handleIO() ? IOResult.Busy : IOResult.NotBusy;
            }

            if (tryHandleDisconnect()) {
                return IOResult.Disconnected;
            }

            return IOResult.Busy;
        }

        @Override
        public void clear() {
            peerId = Long.MIN_VALUE;
            fd = -1;
            streamReceiver.clear();
        }

        @Override
        public void close() {
            clear();
            if (null != streamReceiver) {
                clear();
                streamReceiver.close();
                streamReceiver = null;
            }
        }

    }
}
