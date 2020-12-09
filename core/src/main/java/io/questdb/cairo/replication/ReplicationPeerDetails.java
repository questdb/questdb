package io.questdb.cairo.replication;

import java.io.Closeable;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.FanOut;
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
    private final ConnectionWorkerJob<?>[] connectionWorkerJobs;
    private final ObjectFactory<PeerConnection> connectionFactory;
    private IntList nAssignedByWorkerId = new IntList();
    private ObjList<PeerConnection> connections;
    private ObjList<PeerConnection> connectionCache;

    interface PeerConnection extends Closeable {
        PeerConnection of(long peerId, long fd, ConnectionWorkerJob<?> workerJob);

        long getFd();

        int getWorkerId();

        boolean handleIO();

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

    public static class FanOutSequencedQueue<T> {
        public static final <T> FanOutSequencedQueue<T> createSingleProducerFanOutConsumerQueue(int queueLen, ObjectFactory<T> eventFactory, int nConsumers) {
            Sequence producerSeq = new SPSequence(queueLen);
            Sequence[] consumerSeqs = new Sequence[nConsumers];
            if (nConsumers > 1) {
                FanOut fanOut = new FanOut();
                for (int n = 0; n < nConsumers; n++) {
                    SCSequence consumerSeq = new SCSequence();
                    consumerSeqs[n] = consumerSeq;
                    fanOut.and(consumerSeq);
                }
                producerSeq.then(fanOut).then(producerSeq);
            } else {
                SCSequence consumerSeq = new SCSequence();
                consumerSeqs[0] = consumerSeq;
                producerSeq.then(consumerSeq).then(producerSeq);
            }
            RingQueue<T> queue = new RingQueue<>(eventFactory, queueLen);
            return new FanOutSequencedQueue<T>(producerSeq, consumerSeqs, queue);
        }

        private final Sequence producerSeq;
        private final Sequence[] consumerSeqs;
        private final RingQueue<T> queue;

        FanOutSequencedQueue(Sequence producerSeq, Sequence[] consumerSeqs, RingQueue<T> queue) {
            super();
            this.producerSeq = producerSeq;
            this.consumerSeqs = consumerSeqs;
            this.queue = queue;
        }

        public Sequence getProducerSeq() {
            return producerSeq;
        }

        public Sequence getConsumerSeq(int nConsumer) {
            return consumerSeqs[nConsumer];
        }

        public T getEvent(long seq) {
            return queue.get(seq);
        }
    }

    static class ConnectionWorkerEvent {
        final static byte ADD_NEW_CONNECTION_EVENT_TYPE = 1;
        final static int ALL_WORKERS = -1;
        protected byte eventType;
        protected int nWorker;
        PeerConnection newConnection;

        void assignAddNewConnection(int nWorker, PeerConnection newConnection) {
            eventType = ADD_NEW_CONNECTION_EVENT_TYPE;
            this.nWorker = nWorker;
            this.newConnection = newConnection;
        }
    }

    static abstract class ConnectionWorkerJob<EVT extends ConnectionWorkerEvent> implements Job {
        protected final int nWorker;
        private final FanOutSequencedQueue<EVT> connectionWorkerQueue;
        private final ObjList<PeerConnection> connections = new ObjList<>();
        private boolean busy;

        protected ConnectionWorkerJob(int nWorker, FanOutSequencedQueue<EVT> connectionWorkerQueue) {
            super();
            this.nWorker = nWorker;
            this.connectionWorkerQueue = connectionWorkerQueue;
        }

        @Override
        public boolean run(int workerId) {
            busy = false;
            handleConsumerEvents();
            int nConnection = 0;
            while (nConnection < connections.size()) {
                PeerConnection connection = connections.get(nConnection);
                if (connection.handleIO()) {
                    busy = true;
                }
                if (connection.isDisconnected()) {
                    // TODO:
                    connections.remove(nConnection);
                } else {
                    nConnection++;
                }
            }
            return busy;
        }

        public final int getWorkerId() {
            return nWorker;
        }

        private void handleConsumerEvents() {
            long seq;
            Sequence consumerSeq = connectionWorkerQueue.getConsumerSeq(nWorker);
            while ((seq = consumerSeq.next()) >= 0) {
                EVT event = connectionWorkerQueue.getEvent(seq);
                try {
                    if (nWorker == event.nWorker || event.nWorker == ConnectionWorkerEvent.ALL_WORKERS) {
                        if (event.eventType == ConnectionWorkerEvent.ADD_NEW_CONNECTION_EVENT_TYPE) {
                            connections.add(event.newConnection);
                            LOG.info().$("handling new connection [fd=").$(event.newConnection.getFd()).$(", nWorker=").$(nWorker).$(']').$();
                        } else {
                            handleConsumerEvent(event);
                        }
                        busy = true;
                    }
                } finally {
                    consumerSeq.done(seq);
                }
            }
        }

        protected abstract void handleConsumerEvent(EVT event);
    }

    ReplicationPeerDetails(long peerId, int nWorkers, ConnectionWorkerJob<?>[] connectionWorkerJobs, ObjectFactory<PeerConnection> connectionFactory) {
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

        FanOutSequencedQueue<? extends ConnectionWorkerEvent> queue = connectionWorkerJobs[workerId].connectionWorkerQueue;
        long seq = queue.producerSeq.next();
        if (seq >= 0) {
            try {
                ConnectionWorkerEvent event = queue.getEvent(seq);
                PeerConnection connection;
                if (connectionCache.size() > 0) {
                    int n = connectionCache.size() - 1;
                    connection = connectionCache.getQuick(n);
                    connectionCache.remove(n);
                } else {
                    connection = connectionFactory.newInstance();
                }
                connection.of(peerId, fd, connectionWorkerJobs[workerId]);
                event.assignAddNewConnection(workerId, connection);
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
            PeerConnection peerConnection = connections.get(n);
            if (peerConnection.getFd() == fd) {
                connections.remove(n);
                nAssignedByWorkerId.set(peerConnection.getWorkerId(), nAssignedByWorkerId.getQuick(peerConnection.getWorkerId()) - 1);
                peerConnection.clear();
                connectionCache.add(peerConnection);
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    <T extends PeerConnection> T getConnection(int concurrencyId) {
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
