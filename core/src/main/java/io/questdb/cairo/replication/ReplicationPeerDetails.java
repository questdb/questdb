package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SPSequence;
import io.questdb.mp.Sequence;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

abstract class ReplicationPeerDetails implements Closeable {
    private static final Log LOG = LogFactory.getLog(ReplicationPeerDetails.class);
    private long peerId = Long.MIN_VALUE;
    private int nWorkers;
    private final ConnectionWorkerJob[] connectionWorkerJobs;
    private final ObjectFactory<PeerConnection<?>> connectionFactory;
    private IntList nAssignedByWorkerId = new IntList();
    private ObjList<PeerConnection<?>> connections;
    private ObjList<PeerConnection<?>> connectionCache;

    interface PeerConnection<SENDEVT> extends Closeable {
        PeerConnection<SENDEVT> of(long peerId, long fd, int workerId);

        long getFd();

        int getWorkertId();

        SequencedQueue<SENDEVT> getConsumerQueue();

        boolean handleSendTask();

        boolean handleReceiveTask();

        boolean isDisconnected();

        void clear();
    }

    public static class SequencedQueue<T> {
        public static final <T> SequencedQueue<T> createSingleProducerSingleConsumerQueue(int queueLen, ObjectFactory<T> eventFactory) {
            Sequence producerSeq = new SPSequence(queueLen);
            Sequence consumerSeq = new SCSequence();
            RingQueue<T> queue = new RingQueue<>(eventFactory, queueLen);
            producerSeq.then(consumerSeq).then(producerSeq);
            return new SequencedQueue<T>(producerSeq, consumerSeq, queue);
        }

        public static final <T> SequencedQueue<T> createMultipleProducerSingleConsumerQueue(int queueLen, ObjectFactory<T> eventFactory) {
            Sequence producerSeq = new MPSequence(queueLen);
            Sequence consumerSeq = new SCSequence();
            RingQueue<T> queue = new RingQueue<>(eventFactory, queueLen);
            producerSeq.then(consumerSeq).then(producerSeq);
            return new SequencedQueue<T>(producerSeq, consumerSeq, queue);
        }

        private final Sequence producerSeq;
        private final Sequence consumerSeq;
        private final RingQueue<T> queue;

        SequencedQueue(Sequence producerSeq, Sequence consumerSeq, RingQueue<T> queue) {
            super();
            this.producerSeq = producerSeq;
            this.consumerSeq = consumerSeq;
            this.queue = queue;
        }

        public Sequence getProducerSeq() {
            return producerSeq;
        }

        public Sequence getConsumerSeq() {
            return consumerSeq;
        }

        public T getEvent(long seq) {
            return queue.get(seq);
        }
    }

    static class ConnectionWorkerEvent {
        private PeerConnection<?> newConnection;
    }

    static class ConnectionWorkerJob implements Job {
        private final SequencedQueue<ConnectionWorkerEvent> newConnectionQueue;
        private final ObjList<PeerConnection<?>> connections = new ObjList<>();
        private boolean busy;

        protected ConnectionWorkerJob(SequencedQueue<ConnectionWorkerEvent> consumerQueue) {
            super();
            this.newConnectionQueue = consumerQueue;
        }

        @Override
        public boolean run(int workerId) {
            busy = false;
            handleConsumerEvents();
            int nConnection = 0;
            while (nConnection < connections.size()) {
                PeerConnection<?> connection = connections.get(nConnection);
                if (connection.handleSendTask()) {
                    busy = true;
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
            return busy;
        }

        private void handleConsumerEvents() {
            long seq;
            while ((seq = newConnectionQueue.getConsumerSeq().next()) >= 0) {
                ConnectionWorkerEvent event = newConnectionQueue.getEvent(seq);
                try {
                    connections.add(event.newConnection);
                    busy = true;
                } finally {
                    event.newConnection = null;
                    newConnectionQueue.getConsumerSeq().done(seq);
                }
            }
        }
    }

    ReplicationPeerDetails(long peerId, int nWorkers, ConnectionWorkerJob[] connectionWorkerJobs, ObjectFactory<PeerConnection<?>> connectionFactory) {
        super();
        this.peerId = peerId;
        this.nWorkers = nWorkers;
        this.connectionWorkerJobs = connectionWorkerJobs;
        this.connectionFactory = connectionFactory;
        nAssignedByWorkerId = new IntList(nWorkers);
        for (int nWorker = 0; nWorker < nWorkers; nWorker++) {
            nAssignedByWorkerId.add(0);
        }
        connections = new ObjList<>();
        connectionCache = new ObjList<>();
    }

    boolean tryAddConnection(long fd) {
        int nMinAssigned = Integer.MAX_VALUE;
        int workerId = Integer.MAX_VALUE;
        for (int nWorker = 0; nWorker < nWorkers; nWorker++) {
            int nAssigned = nAssignedByWorkerId.getQuick(nWorker);
            if (nAssigned < nMinAssigned) {
                nMinAssigned = nAssigned;
                workerId = nWorker;
            }
        }

        SequencedQueue<ConnectionWorkerEvent> queue = connectionWorkerJobs[workerId].newConnectionQueue;
        long seq = queue.producerSeq.next();
        if (seq >= 0) {
            try {
                ConnectionWorkerEvent event = queue.getEvent(seq);
                PeerConnection<?> connection;
                if (connectionCache.size() > 0) {
                    int n = connectionCache.size() - 1;
                    connection = connectionCache.getQuick(n);
                    connectionCache.remove(n);
                } else {
                    connection = connectionFactory.newInstance();
                }
                connection.of(peerId, fd, workerId);
                event.newConnection = connection;
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

    void removeConnection(long fd) {
        for (int n = 0, sz = connections.size(); n < sz; n++) {
            PeerConnection<?> peerConnection = connections.get(n);
            if (peerConnection.getFd() == fd) {
                connections.remove(n);
                nAssignedByWorkerId.set(peerConnection.getWorkertId(), nAssignedByWorkerId.getQuick(peerConnection.getWorkertId()) - 1);
                peerConnection.clear();
                connectionCache.add(peerConnection);
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    <T extends PeerConnection<?>> T getConnection(int concurrencyId) {
        int connectionId = concurrencyId % connections.size();
        return (T) connections.getQuick(connectionId);
    }

    @Override
    public void close() {
        if (null != connections) {
            peerId = Long.MIN_VALUE;
            Misc.freeObjList(connectionCache);
            connectionCache = null;
            Misc.freeObjList(connections);
            connections = null;
            nAssignedByWorkerId.clear();
            nAssignedByWorkerId = null;
        }
    }
}
