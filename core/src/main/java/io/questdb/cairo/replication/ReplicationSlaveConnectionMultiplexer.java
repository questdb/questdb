package io.questdb.cairo.replication;

import java.util.concurrent.atomic.AtomicInteger;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationPeerDetails.ConnectionWorkerJob;
import io.questdb.cairo.replication.ReplicationPeerDetails.FanOutSequencedQueue;
import io.questdb.cairo.replication.ReplicationPeerDetails.PeerConnection;
import io.questdb.cairo.replication.ReplicationSlaveConnectionMultiplexer.SlaveConnectionWorkerEvent;
import io.questdb.cairo.replication.ReplicationSlaveManager.SlaveWriter;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.IntObjHashMap;

public class ReplicationSlaveConnectionMultiplexer extends AbstractMultipleConnectionManager<SlaveConnectionWorkerEvent> {
    private static final Log LOG = LogFactory.getLog(ReplicationSlaveConnectionMultiplexer.class);
    private final CairoConfiguration configuration;

    public ReplicationSlaveConnectionMultiplexer(
            CairoConfiguration configuration,
            WorkerPool workerPool,
            int connectionCallbackQueueLen,
            int newConnectionQueueLen
    ) {
        super(configuration.getFilesFacade(), workerPool, connectionCallbackQueueLen, newConnectionQueueLen, SlaveConnectionWorkerEvent::new);
        this.configuration = configuration;
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
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    ConnectionWorkerJob<?> createConnectionWorkerJob(int nWorker, FanOutSequencedQueue<SlaveConnectionWorkerEvent> connectionWorkerQueue) {
        return new SlaveConnectionWorkerJob(nWorker, connectionWorkerQueue);
    }

    @Override
    ReplicationPeerDetails createNewReplicationPeerDetails(long peerId) {
        return new MasterPeerDetails(peerId, nWorkers, connectionWorkerJobs, configuration);
    }

    private static class MasterPeerDetails extends ReplicationPeerDetails {
        MasterPeerDetails(long peerId, int nWorkers, ConnectionWorkerJob<?>[] connectionWorkerJobs, CairoConfiguration configuration) {
            super(peerId, nWorkers, connectionWorkerJobs, () -> {
                return new MasterConnection(configuration);
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

    static class SlaveConnectionWorkerJob extends ConnectionWorkerJob<SlaveConnectionWorkerEvent> {
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

    // TODO
    private static class ToPeerEvent {
    }

    private static class MasterConnection implements PeerConnection<ToPeerEvent> {
        private long peerId = Long.MIN_VALUE;
        private long fd = -1;
        private int workerId;
        private ReplicationStreamReceiver streamReceiver;
        private boolean disconnected;

        MasterConnection(CairoConfiguration configuration) {
            streamReceiver = new ReplicationStreamReceiver(configuration);
        }

        @Override
        public PeerConnection<ToPeerEvent> of(long peerId, long fd, ConnectionWorkerJob<?> workerJob) {
            this.peerId = peerId;
            this.fd = fd;
            this.workerId = workerJob.getWorkerId();
            streamReceiver.of(fd, ((SlaveConnectionWorkerJob) workerJob).slaveWriteByMasterTableId);
            disconnected = false;
            return this;
        }

        @Override
        public long getFd() {
            return fd;
        }

        @Override
        public int getWorkerId() {
            return workerId;
        }

        @Override
        public boolean handleIO() {
            assert !disconnected;
            return streamReceiver.handleIO();
        }

        @Override
        public boolean isDisconnected() {
            return disconnected;
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
